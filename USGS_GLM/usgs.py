import yaml
import sys
import requests
import json
import copy
import csv
import pprint

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import date, datetime

from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi
from pygeotemporal.time_transformers import time2utc

# get configuration
config = yaml.load(open("config.yml", 'r'))

# instantiate API objects
sensorclient = SensorsApi(host=config['defaults']['host'], key=config['defaults']['key'])
streamclient = StreamsApi(host=config['defaults']['host'], key=config['defaults']['key'])
datapointclient = DatapointsApi(host=config['defaults']['host'], key=config['defaults']['key'])
sites_source = config['defaults']['sites_map']

for sitepair in sites_source:
    site = sitepair.keys()[0]
    statename = sitepair[site]
    start_date = config['defaults']['start_date']  # %Y-%m-%d this is just a string - indeterminate what time zone
    end_date = date.today().strftime('%Y-%m-%d')  # %Y-%m-%d this is just a string - indeterminate what time zone
    print "Getting sensor data for " + str(site)

    # get data from usgs, not used currently
    # sensor_url = config['defaults']['data_source_url'].replace("statename", statename) + "format=json&sites=" + site + "&parameterCd=" + \
    #              ','.join(config['defaults']['parameters_from_usgs'])
    # print(sensor_url)
    # r = requests.get(sensor_url)
    # r.raise_for_status()

    # get the sensor
    r = sensorclient.sensor_get_by_name("usgs" + site)
    sensors = r.json()
    if len(sensors) == 0:
        print "No sensor found, please add the sensor first."
        break

    sensor = sensors[0]
    longitude = sensor["geometry"]["coordinates"][1]
    latitude = sensor["geometry"]["coordinates"][0]

    # create stream
    stream_json = streamclient.stream_create_json_from_sensor(sensor)
    stream = streamclient.stream_post_json(stream_json)

    # get most recent datapoint in clowder for this sensor (used for start of update parsing)
    # start_time as 2017-08-04T13:15:00Z
    # latest_datapoint = datapointclient.datapoint_latest_get(sensor['id'], stream['id'])
    # print "latest_datapoint ", latest_datapoint

    if stream != None and stream['end_time'] is not None:
        latest_datapoint_datetime = parse(stream['end_time'])  # now in zulu as datetime
        start_date = latest_datapoint_datetime.strftime('%Y-%m-%d')  # time zone zulu eg. 2015-10-22 as string
    else:
        latest_datapoint_datetime = None

    # convert to datetimes
    start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date() - relativedelta(days=1)
    end_date = datetime.strptime(str(end_date), "%Y-%m-%d").date()

    step_start_date = copy.copy(start_date)

    datapoint_count = 0
    datapoint_post_count = 0
    # EACH LOOP IS FOR A YEAR OF DATA
    while step_start_date <= end_date:
        if step_start_date + relativedelta(years=1) >= end_date:
            step_end_date = end_date + relativedelta(days=1)
        else:
            step_end_date = step_start_date + relativedelta(years=1) - relativedelta(days=1)

        # get data from usgs
        source_url = config['defaults']['data_source_url'].replace("statename", statename) + "format=rdb&site_no=" + \
                  site
        rdb_url = source_url +'&begin_date=' + str(step_start_date) + '&end_date=' + str(step_end_date)
        print "getting data from usgs using: ", rdb_url
        # print rdb_url
        r = requests.get(rdb_url)
        r.raise_for_status()
        rdb_data = r.text

        step_start_date = step_start_date + relativedelta(years=1)

        if rdb_data is None or rdb_data[0:11] == "#  No sites":
            print "No data available from: " + str(step_start_date -
                                                   relativedelta(years=1)) + " to: " + str(step_end_date)
            continue

        # put data into a dictionary
        rows = []
        lineOfText = ""
        for i in range(len(rdb_data)):
            if rdb_data[i] != "\n":
                lineOfText += rdb_data[i]
            else:
                if len(lineOfText) > 0 and lineOfText[0] != "#":
                    rows.append(lineOfText)
                lineOfText = ""
        del rdb_data

        rows.pop(1)
        data = csv.DictReader(rows, delimiter='\t')

        # parse and post datapoints
        start_time_hold = []

        datapoints_bulk_list = []

        rows_total = len(list(data))

        data = csv.DictReader(rows, delimiter='\t')
        rows_count = 0
        for row in data:
            rows_count += 1
            datapoint_count += 1
            start_time = time2utc(row['datetime'] + " 00:00:00")
            if start_time in start_time_hold:
                print("Start time " + str(start_time) + " already has been posted for this sensor - not posting")
                continue
            start_time_hold.append(start_time)


            # if latest_datapoint_datetime is None or parse(start_time).strftime('%Y-%m-%d-%H-%M') >
            # latest_datapoint_datetime:
            if latest_datapoint_datetime is None or parse(start_time) > latest_datapoint_datetime:
                properties = {'source': source_url}
                for key in row.keys():
                    parameterkey = key.split('_', 1)[1] if len(key.split('_', 1)) >1 else 'notparam'
                    if parameterkey in config['map_parameters'] and row[key] \
                            not in [None, "", " ", "eqp", "ice", "***", "ICE", "Eqp", "Fld", "Ssn", "Dis", "Ice",
                                    "Bkw", "7.8_Eqp"]:
                        if key[-3:] == '_cd':
                            properties[config['map_parameters'][parameterkey]] = row[key]
                        else:
                            properties[config['map_parameters'][parameterkey]] = float(row[key])

                datapoint_json = {
                    'start_time': start_time,
                    'end_time': start_time,
                    'type': 'Feature',
                    'geometry': {
                        'type': "Point",
                        'coordinates': [
                            longitude,
                            latitude,
                            0
                        ]
                    },
                    'stream_id': str(stream['id']),
                    'sensor_id': str(sensor['id']),
                    'sensor_name': str(stream['name']),
                    'properties': properties
                }
                datapoints_bulk_list.append(datapoint_json)
                # create a new post request when datapoints is more than 1000
                if datapoint_count % 1000 == 0 or rows_total == rows_count:
                    print "+++++++++++++"
                    print datapoint_count, rows_count, rows_total
                    # pprint.pprint(datapoints_bulk_list)
                    print "posting datapoints starting at " + str(datapoint_count) + " for stream " \
                          + str(stream['id']) + " with number " + str(len(datapoints_bulk_list))
                    headers = {'Content-type': 'application/json'}
                    r = requests.post(str(config['defaults']['host']) + "/api/geostreams/datapoints/bulk?key="
                                      + str(config['defaults']['key']),
                                      data=json.dumps({"datapoints": datapoints_bulk_list,
                                                       "stream_id": str(stream['id'])}), headers=headers)
                    datapoints_bulk_list = []
                    if r.status_code != 200:
                        print("ERR  : Could not add datapoint to stream : [" + str(r.status_code) + "] - " + r.text)

                        # datapoint_response = datapointclient.datapoint_post(datapoint_json)

    print "Updating sensor with id " + str(sensor['id'])
    sensorclient.sensor_statistics_post(sensor['id'])

