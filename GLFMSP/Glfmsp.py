""" Parser for data from GLFMSP """

import argparse as argparse
import logging
import os

import xlrd
from pyclowder.datasets import DatasetsApi
from pygeotemporal.datapoints import DatapointsApi
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi

# mappings from name used in file to id
mappings = {
    "Mercury, ug/g": "mercury",
    "Mirex, ug/g": "mirex",
    "PBDE-tot, ug/g": "pbde",
    "Total DDT + DDD + DDE, ug/g": "ddt",
    "Total DDT + DDD + DDE, ug/g ww": "ddt",
    "Total Chlordane, ug/g": "chlordanes",
    "Total PCBs, ug/g": "pcb",
    "Total PCBs, ug/g ww": "pcb",
    "Toxaphene (Camphechlor), ug/g": "toxaphene",
    "Toxaphene (Camphechlor), ug/g ww": "toxaphene"
}

sensor_geo = {
    "grid0424": [42.41666667, -79.58333333, 0],
    "grid0904": [41.58333333, -82.91666667, 0],
    "grid1413": [44.08333333, -82.75, 0],
    "grid0710": [45.25, -83.01666667, 0],
    "grid2210": [42.58333333, -86.41666667, 0],
    "grid0906": [44.75, -87.08333333, 0],
    "grid0713": [43.41666667, -77.91666667, 0],
    "grid0623": [43.58333333, -76.25, 0],
    "grid1311": [46.91666667, -90.41666667, 0],
    "grid1028": [47.41666667, -87.58333333, 0]
}

sensor_name = {
    "grid0424": "Dunkirk",
    "grid0904": "Middle Bass Island",
    "grid1413": "Port Austin",
    "grid0710": "Rockport",
    "grid2210": "Saugatuck",
    "grid0906": "Sturgeon Bay",
    "grid0713": "North Hamlin",
    "grid0623": "Oswego",
    "grid1311": "Apostle Islands",
    "grid1028": "Keweenaw Point"
}

sensor_region = {
    "grid0424": "ER",
    "grid0904": "ER",
    "grid1413": "HU",
    "grid0710": "HU",
    "grid2210": "MI",
    "grid0906": "MI",
    "grid0713": "ON",
    "grid0623": "ON",
    "grid1311": "SU",
    "grid1028": "SU"
}

sensor_site = {
    "grid0424": "LE-Dunkirk",
    "grid0904": "LE-Middle Bass Island",
    "grid1413": "LH-Port Austin",
    "grid0710": "LH-Rockport",
    "grid2210": "LM-Saugatuck",
    "grid0906": "LM-Sturgeon Bay",
    "grid0713": "LO-North Hamlin",
    "grid0623": "LO-Oswego",
    "grid1311": "LS-Apostle Island",
    "grid1028": "LS-Keweenaw Point"
}

log = logging.getLogger("glfmsp")


def main():
    """ Great Lake Fish Monitoring Program parser Main function, using pyclowder2"""

    # Required command line arguments for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-l', '--location',
                    help="Clowder url, for example http://localhost:9000",
                    default="http://localhost:9000")
    ap.add_argument('-k', '--key', help="Clowder api key", default="r1ek3rs")
    ap.add_argument('-b', '--dashboard',
                    help="Geodashboard url, for example http://localhost:8080/",
                    default="http://localhost:8080/")
    ap.add_argument('-u', '--user', help="Clowder username")
    ap.add_argument('-p', '--password', help="Clowder password")
    ap.add_argument('-d', "--directory", help="Directory of new data",
                    default=r'/Users/indiragp/Documents/GLM/GLFMSP/02_2018')
    ap.add_argument('-s', "--spaceid", help="Space id",
                    default="56460c9de4b0941caa3e4e41")

    opts = ap.parse_args()

    # Exit if arguments are not provided
    if not opts.location and not opts.user and not opts.password:
        ap.print_usage()
        quit()

    host = opts.location
    key = opts.key
    user = opts.user
    password = opts.password
    dashboard = opts.dashboard
    sensor_folder_path = opts.directory
    space_id = opts.spaceid

    dataset_client = DatasetsApi(host=host, key=key, username=user, password=password)
    sensor_client = SensorsApi(host=host, key=key, username=user, password=password)
    stream_client = StreamsApi(host=host, key=key, username=user, password=password)
    datapoint_client = DatapointsApi(host=host, key=key, username=user, password=password)

    # create dataset
    dataset = {
        "name": "GLFMSP",
        "description": "Great Lakes Fish Monitoring and Surveillance Program - ingested data",
        "space": [space_id]
    }

    metadata = {
        "download_date": "03-30-2017",
        "dashboard": [],
        "sensors": [],
        "streams": []

    }
    response = dataset_client.create_empty(dataset)
    dataset_id = response['id']
    source_url = "%s/datasets/%s" % (host, dataset_id)

    # open the files and create streams and datapoints
    if os.path.isdir(sensor_folder_path):
        datafiles = [datafile for datafile in os.listdir(sensor_folder_path) if
                     os.path.isfile(os.path.join(sensor_folder_path, datafile)) and datafile.endswith('.xlsx')]

        # foreach file: upload to dataset
        # foreach sensor:
        #    create sensor, stream
        #    foreach file:
        #        create datapoints
        #    update sensor_statistics
        # add dataset metadata

        for datafile in datafiles:
            filefullname = os.path.join(sensor_folder_path, datafile)
            dataset_client.upload_file(dataset_id, filefullname)

        for s in sensor_name:
            # Create Sensors and Stream.
            sensor_json = sensor_client.sensor_create_json(s, sensor_geo[s][0], sensor_geo[s][1], sensor_geo[s][2],
                                                           sensor_name[s], sensor_region[s])
            sensor_json['properties']['type'] = {
                "id": "glfmsp",
                "title": "Contaminants"
            }
            response = sensor_client.sensor_post_json(sensor_json)
            sensor_id = response['id']
            print "INFO: Created Sensor: " + str(sensor_id)

            stream = {
                "name": "GLFMSP-%s" % s,
                "type": "Feature",
                "properties": {
                    "source": source_url,
                    "dashboard": "%s#detail/location/%s" % (dashboard, s)
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": sensor_geo[s]
                },
                "sensor_id": str(sensor_id)
            }

            response = stream_client.stream_post_json(stream)
            stream_id = response['id']
            print "INFO: Created Stream: " + str(stream_id)

            metadata["dashboard"].append(dashboard + "#detail/location/" + s + "/")
            metadata["sensors"].append(host + "/api/geostreams/sensors/" + str(sensor_id))
            metadata["streams"].append(host + "/api/geostreams/streams/" + str(stream_id))
            counter = 0
            for datafile in datafiles:
                filefullname = os.path.join(sensor_folder_path, datafile)
                print "INFO: Parsing file: " + filefullname

                wb = xlrd.open_workbook(filefullname)
                worksheet = wb.sheet_by_index(0)

                file_header = []  # The row where we stock the name of the column
                for col in range(worksheet.ncols):
                    file_header.append(worksheet.cell_value(0, col).replace(" ", "_"))

                for row in range(1, worksheet.nrows):
                    # Check if the name of the site is the site that we are trying to parse

                    if worksheet.cell_value(row, file_header.index('Site')) == sensor_site[s]:
                        properties = {"source": source_url}
                        # date like '1985-08-11T01:30:00-06:00'
                        date_year = str(int(worksheet.cell_value(row, file_header.index('Year'))))
                        date = date_year + '-08-15T00:00:00-06:00'

                        properties['species'] = worksheet.cell_value(row, file_header.index('Species'))
                        properties['sampleid'] = worksheet.cell_value(row, file_header.index('Summary_ID'))
                        param = worksheet.cell_value(row, file_header.index('Analyte')) + ', ' + \
                            worksheet.cell_value(row, file_header.index('Result_Units'))
                            # worksheet.cell_value(row, file_header.index('Site_Summary_Units'))

                        if param in mappings:
                            param = mappings[param]
                        else:
                            print "INFO: No mapping found for %s" % param
                            mappings[param] = param
                        properties[param] = worksheet.cell_value(row, file_header.index('Result'))
                        # worksheet.cell_value(row, file_header.index('Site_Mean'))
                        properties[param + '_var'] = worksheet.cell_value(row, file_header.index('Site_Variability'))
                        properties[param + '_var_type'] = worksheet.cell_value(row, file_header.index(
                            'Site_Variability_Type'))
                        properties['disclaimer'] = "The fish are collected over the course of several months each " \
                                                   "fall. The date, Oct. 15, was chosen for the time series since " + \
                                                   "it is the mid-point of the sampling season"
                        # properties['comments'] = worksheet.cell_value(row, file_header.index('CSRA Comment'))
                        datapoint = {
                            "start_time": date,
                            "end_time": date,
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": sensor_geo[s]
                            },
                            "properties": properties,
                            "stream_id": str(stream_id)
                        }
                        response = datapoint_client.datapoint_post(datapoint)
                        if response.status_code == 200:
                            counter += 1

            print "Info: Ingested %s datapoints for sensor %s with id %s" % (counter, sensor_name[s], sensor_id)
            sensor_client.sensor_statistics_post(sensor_id)

        dataset_client.add_metadata(dataset_id, metadata)


if __name__ == "__main__":
    main()
