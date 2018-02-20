#!/usr/bin/env python

import csv
import os
import dateutil.parser
import argparse
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi
from pyclowder.datasets import DatasetsApi
from pyclowder.collections import CollectionsApi

# Remove the streams but leave the sensors when parsing.

# ----------------------------------------------------------------------
# BEGIN CONFIGURATION
# ----------------------------------------------------------------------

# extra date information
timezone = '-06:00'

# should sensors be created if they don't exist ????
create_sensors = False

# ----------------------------------------------------------------------
# END CONFIGURATION
# ----------------------------------------------------------------------

# mappings from name used in file to id
mappings = {
    "Alkalinity, Total as CaCO3": "alkalinity-glenda",
    "Chlorophyll-a": "chlorophyll-a-glenda",
    "Nitrogen, Total Oxidized": "nitrogen-glenda",
    "Phosphorus, Total as P": "total-phosphorus-bulk-glenda",
    "Phosphorus, Total Dissolved": "total-phosphorus-filtrate-glenda",
    "Phosphorus, Dissolved Reactive": "phosphorus-dissolved-glenda",
    "Turbidity": "turbidity-glenda",
    "Ammonium": "ammonium-glenda",
    "Beam Attenuation (Seabird)": "beam-attenuation-seabird",
    "Chloride": "chloride-glenda",
    "Specific Conductance": "conductivity-glenda",
    "Hardness, Total as CaCO3": "total-hardness-glenda",
    "Irradiance (Seabird)": "irradiance-seabird",
    "Oxygen, Dissolved": "oxygen-dissolved-glenda",
    "Phosphorus, Elemental": "phosphorus-elemental-glenda",
    "Phosphorus, Orthophosphorus as P": "phosphorus-orthophosphorus-glenda",
    "Secchi Disc Transparency": "secchi-disc-transparency-glenda",
    "Silica, Dissolved as Si": "silica-dissolved-glenda",
    "Silicon, Elemental": "silicon-elemental-glenda",
    "Silica, Total": "silica-total-glenda",
    "Temperature": "temperature-glenda",
    "pH": "ph-glenda"
}

# List of timezones
timezone_infos = {
    "PST": -28800, "PDT": -25200,
    "MST": -25200, "MDT": -21600,
    "CST": -21600, "CDT": -18000,
    "EST": -18000, "EDT": -14400,
    "ADT": -10800
}

# ["SRF", "LEP", "TRM"] => EP
# mapping the depth_code to the old format.
depth_mapping = {
    'Surface': 'SRF',
    'Lower Epilimnion': 'LEP',
    'Mid Epilimnion': 'MEP',
    'Mid Hypolimnion': 'MHY',
    'Upper Hypolimnion': 'UHY',
    'Bottom Minus 10': 'B10',
    'Bottom Minus': 'B-',
    'Integrated, Spring': 'INT-SPR',
    'Integrated, Summer': 'INT-SUM',
    'Middle Depth': 'MID',
    'Thermocline': 'TRM',
    'Five Meter': '5M',
    'Ten Meter': '10M',
    'Twenty Meter': '20M',
    'Thirty Meter': '30M',
    'Forty Meter': '40M',
    'Synthetic Sample': 'SYN',
    '100': 'SRF',
    '150': 'MEP',
    '200': 'LEP',
    '300': 'TRM',
    '350': 'UHY',
    '400': 'MHY',
    '450': 'B10',
    '450.5': 'B10',
    '500': 'B1,B2',
    '505': 'B1,B2',
    '50': '50'
}


# ----------------------------------------------------------------------
# find/create the collection holding EPA data
# ----------------------------------------------------------------------
def get_collection_id(collections_client, space_id):
    result = collections_client.get_all_collections()
    if isinstance(result, list):
        for c in result:
            if c['collectionname'] == "EPA":
                return c['id']

    request = collections_client.create("EPA", "EPA ingested data", None, space_id)

    if request.status_code != 200:
        print("ERR  : Problem creating collection : [" + str(request.status_code) + "] - " + request.text)
        return None
    return request.json()['id']


# ----------------------------------------------------------------------
# find sensor by name
# ----------------------------------------------------------------------
def get_sensor(sensor_name, sensor_client):
    r = sensor_client.sensor_get_by_name(sensor_name)
    if (r.status_code == 200) and len(r.json()) > 0:
        sensors = r.json()
        return sensors[0]

    r = sensor_client.sensor_get_by_name(sensor_name + "M")
    if (r.status_code == 200) and len(r.json()) > 0:
        sensors = r.json()
        return sensors[0]

    # s['name'] + "M" == sensor_name:
    r = sensor_client.sensor_get_by_name(sensor_name.rstrip("M"))
    if (r.status_code == 200) and len(r.json()) > 0:
        sensors = r.json()
        return sensors[0]

    return None


# ----------------------------------------------------------------------
# parse the file
# ----------------------------------------------------------------------
def parse_file(sensor_name, collection_id, sensor, space_id, clowder, geodashboard, directory, download_date,
               dataset_client, stream_client, datapoint_client):
    # create an empty dataset & add it to collection
    dataset_name = sensor_name
    body = {"name": dataset_name,
            "description": "EPA ingested data, Download Date: %s" % download_date,
            "collection": [collection_id],
            "space": [space_id]}
    r = dataset_client.create_empty(body)
    dataset_id = r['id']
    source_url = clowder + "datasets/" + dataset_id
    # need geometry everywhere
    geometry = {"type": "Point",
                "coordinates": sensor['geometry']['coordinates']}

    # see if stream needs to be created
    if '_stream' not in sensor:
        # create stream and add as metadata to file
        properties = {"source": source_url, "type": "EPA"}
        if geodashboard is not None:
            properties['dashboard'] = "%s#detail/location/%s/" % (geodashboard, sensor['name'])
        stream = {"name": "EPA-" + sensor_name,
                  "type": "Feature",
                  "geometry": geometry,
                  "properties": properties,
                  "sensor_id": str(sensor['id'])}
        r = stream_client.stream_post_json(stream)
        sensor['_stream'] = r['id']

    metadata = {}
    sensor_folder_path = os.path.join(directory, sensor_name)
    # loop in the files for the same sensor in different years
    datafiles = [datafile for datafile in os.listdir(sensor_folder_path) if
                 os.path.isfile(os.path.join(sensor_folder_path, datafile)) and datafile.endswith('.csv')]
    for datafile in datafiles:
        file_name = os.path.join(sensor_folder_path, datafile)
        print "INFO : Parsing file : " + file_name
        # open spreadsheet
        spam_reader = csv.reader(open(file_name, 'rb'))

        # upload file to clowder and add to the dataset
        r = dataset_client.upload_file(dataset_id, file_name)
        file_id = r['id']
        print("DEBUG: Created file: ", file_id)

        # loop through the data
        header = None

        print("DEBUG : Parsing data, creating streams as needed.")
        datapoints_body = []
        for row in spam_reader:
            # check header
            if not header:
                if 'STATION_ID' not in row or 'DEPTH_CODE' not in row or 'QC_TYPE' not in row:
                    print("ERR  : invalid header detected")
                    return
                header = []
                for x in row:
                    header.append(str(x))
                continue
            # in the raw file, there is a additional space before the MONTH
            if row[header.index('QC_TYPE')] == 'routine field sample' and (
                    row[header.index('MONTH')].replace(" ", "") in ['March', 'April', 'May']
                    or (row[header.index('MONTH')].replace(" ", "") in ['August', 'Sep'] and row[
                    header.index('DEPTH_CODE')] in ['Surface', 'Mid Epilimnion', '100', '150'])):
                # add streams as metadata
                info = {
                    "sensor": clowder + "/api/geostreams/sensors/" + str(sensor['id']),
                    "stream": clowder + "/api/geostreams/streams/" + str(sensor['_stream']),
                    "datapoints": clowder + "/api/geostreams/datapoints?stream_id=" + str(sensor['_stream']),
                    "download_date": download_date
                }

                if geodashboard is not None:
                    info['dashboard'] = "%s#detail/location/%s/" % (geodashboard, sensor['name'])
                metadata[sensor['name']] = info

                # adding data to streams
                # basic info
                if row[header.index('STATION_ID')] != sensor_name:
                    print("Wrong station id " + sensor_name + "  " + row[header.index('STATION_ID')])
                    return
                properties = {"source": source_url,
                              "type": "EPA",
                              "DEPTH_CODE": depth_mapping.get(row[header.index('DEPTH_CODE')],
                                                              row[header.index('DEPTH_CODE')]),
                              "QC_TYPE": row[header.index('QC_TYPE')],
                              "SAMPLE_ID": row[header.index('SAMPLE_ID')]
                              }

                # column+1 is the name of field, column+2 is data
                for column in range(header.index('ANL_CODE_1'), len(header), 7):
                    if row[column] is not None and row[column] != "" and row[column + 2] != "no result reported":
                        if str(row[column + 1]) in mappings:
                            if row[column + 1] == "Phosphorus, Total as P":
                                if row[column + 4] == "Total/Bulk":
                                    properties[mappings[row[column + 1]]] = row[column + 2]
                                elif row[column + 4] == "Filtrate":
                                    properties[mappings["Phosphorus, Total Dissolved"]] = row[column + 2]
                                # if row[column + 4] == "":
                                #    properties[mappings["Phosphorus, Dissolved Reactive"]] = row[column + 2]
                                else:
                                    print("The mapping is" + row[column + 4])
                            elif row[column + 1] == "Silica, Total":
                                try:
                                    properties[mappings["Silica, Dissolved as Si"]] = \
                                        float(row[column + 2]) * 32.066 / 64.064
                                except ValueError:
                                    print("Couldn't parse Silica value: ", row[column + 2], " date: ",
                                          row[header.index('SAMPLING_DATE')])
                            else:
                                properties[mappings[row[column + 1]]] = row[column + 2]
                        else:
                            print "No mapping found for " + str(row[column + 1])
                            properties[row[column + 1]] = row[column + 2]
                # time format is different from the old raw file.
                date = dateutil.parser.parse(row[header.index('SAMPLING_DATE')] + ' ' + row[header.index('TIME_ZONE')],
                                             yearfirst=False, ignoretz=False, tzinfos=timezone_infos).isoformat()
                if date[0:4] < "2000":
                    properties.pop("chlorophyll-a-glenda", None)
                body = {"start_time": date, "end_time": date, "type": "Feature", "geometry": geometry,
                        "properties": properties}
                datapoints_body.append(body)

        r = datapoint_client.datapoint_create_bulk(datapoints_body, str(sensor['_stream']))

        if r.status_code != 200:
            print("ERR  : Could not add datapoint to stream : [" + str(r.status_code) + "] - " + r.text)
            return

        print("DEBUG : finish file.")
    # end of file loop

    # update dataset by adding metadata
    dataset_client.add_metadata(dataset_id, metadata)


# ----------------------------------------------------------------------
# MAIN Function
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', "--directory", help="directory of new data",
                        default=r'/Users/indiragp/Documents/GLM/GLENDA_new')
    parser.add_argument('-c', "--clowder", help="Clowder url",
                        default="http://localhost:9000")

    parser.add_argument('-b', "--dashboard", help="Geodashboard url",
                        default="http://localhost:8080/")

    parser.add_argument('-k', "--key", help="Clowder Api key",
                        default="r1ek3rs")

    parser.add_argument('-s', "--spaceid", help="space id",
                        default="57d85ee1e4b02aceeedcd183")

    parser.add_argument("--downloaddate", help="download date",
                        default="10-19-2016")
    parser.add_argument('-u', '--user', help="Clowder username")
    parser.add_argument('-p', '--password', help="Clowder password")

    args = parser.parse_args()

    directory = args.directory
    # clowder host (used to link the stream with the source file)
    clowder = args.clowder
    # geodashboard host (used to link from clowder to geodashboard)
    dashboard = args.dashboard
    # clowder key
    key = args.key
    # the space which datasets will be added to
    space_id = args.spaceid
    # the download date of the raw file. assume all files under sensor are downloaded by the same date.
    download_date = args.downloaddate
    user = args.user
    password = args.password

    dataset_client = DatasetsApi(host=clowder, key=key, username=user, password=password)
    sensor_client = SensorsApi(host=clowder, key=key, username=user, password=password)
    stream_client = StreamsApi(host=clowder, key=key, username=user, password=password)
    datapoint_client = DatapointsApi(host=clowder, key=key, username=user, password=password)
    collections_client = CollectionsApi(host=clowder, key=key, username=user, password=password)

    collection_id = get_collection_id(collections_client, space_id)
    if os.path.isdir(directory) and collection_id is not None:
        sensor_folders = [sensor_folder for sensor_folder in os.listdir(directory) if
                          os.path.isdir(os.path.join(directory, sensor_folder))]
        for sensor_folder in sensor_folders:
            # check Clowder connect & get all sensors, change the sensor name to the folder name
            sensor = get_sensor(sensor_folder, sensor_client)

            if sensor is None:
                print("ERR : not implemented")
            else:
                parse_file(sensor_folder, collection_id, sensor, space_id, clowder, dashboard, directory, download_date,
                           dataset_client, stream_client, datapoint_client)
                sensor_client.sensor_statistics_post(sensor["id"])


if __name__ == "__main__":
    main()
