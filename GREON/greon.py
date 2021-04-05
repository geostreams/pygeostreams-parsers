from pygeostreams.sensors import SensorsApi
from pygeostreams.streams import StreamsApi
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.cache import CacheApi
from pygeostreams.time_transformers import time2utc
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import smtplib
from email.mime.text import MIMEText
import yaml
import sys
import copy
import os
import csv
import logging
import subprocess
import json
from pprint import pprint
import time

timer = time.time()

# Append path to hucfinder as needed
# TODO add to config.yml
sys.path.append('../../..')
from huc_finder.huc import HucFinder
hucfinder = HucFinder('../../../huc_finder/huc-all.shp')

# get configuration
config = yaml.load(open("config.yml", 'r'), Loader=yaml.FullLoader)

# configure logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("fiona").setLevel(logging.WARNING)
logging.getLogger("geopandas").setLevel(logging.ERROR)
logging.getLogger("pyproj").setLevel(logging.ERROR)
logging.basicConfig(filename=config['log_filename'], level=logging.DEBUG)

host = config['host']
username = config['username']
password = config['password']

logging.info(" ")
logging.info("[greon.py] Starting GREON Parser for %s" % host)

binning_sensor_id_list = []

# Authenticate
sensorclient = SensorsApi(host=host, username=username, password=password)
streamclient = StreamsApi(host=host, username=username, password=password)
datapointclient = DatapointsApi(host=host, username=username, password=password)
cacheclient = CacheApi(host=host, username=username, password=password)

def create_sensor_stream(site):
    # get or create sensor
    geocode = config['geocodes'][site]
    huc_json = hucfinder.getHuc(lat=geocode[1], lon=geocode[0])
    logging.debug("[greon.py] Getting sensor")
    sensor_json = sensorclient.sensor_create_json(site,
                                                  geocode[0],
                                                  geocode[1],
                                                  0,
                                                  site,
                                                  huc_json['huc4']['code'],
                                                  huc=huc_json,
                                                  network='greon',
                                                  organization_id='greon',
                                                  title="Great Rivers Ecological Observation Network")

    sensor = sensorclient.sensor_post_json(sensor_json)

    latest_datapoint = sensor['max_end_time']

    logging.debug("[greon.py] Latest datapoint:" + str(latest_datapoint))

    # create or get stream - need to add _WQ to name of sensor for stream
    # then romove before returning
    sensor_name = copy.copy(sensor['name'])
    sensor['name'] = sensor_name + "_WQ"
    sensor['properties']["name"] = sensor_name + "_WQ"
    stream_json = streamclient.stream_create_json_from_sensor(sensor)
    stream_wq = streamclient.stream_post_json(stream_json)
    sensor['name'] = sensor_name
    sensor['properties']['name'] = sensor_name

    return sensor, stream_wq, geocode, latest_datapoint

for site in config['gltg-sites']:

    logging.debug("[greon.py] ------ Parsing %s" % site)

    sensor, stream_wq, geocode, latest_datapoint = create_sensor_stream(site)

    # create list of paths for site and data type
    file_list = []

    logging.debug("[greon.py] Getting data files for %s" % site)
    for path, subdirs, files in os.walk("%s/%s%s" % (config['loggernet_dir'], str(site), "/WQData")):
        # don't add files to list that are previous to latest_datapoint
        for name in files:
            if name[-3:] == "dat":
                file_list.append(os.path.join(path, name))
    logging.debug("[greon.py] Done getting data files for %s" % site)

    count = 0
    greon1_to_greon2 = False
    datapoints_bulk_list = []
    good_datapoint_count = 0
    rows_total = 0
    rows_count = 0

    while len(file_list) > 0:
        file_rows = open(file_list[0]).read().splitlines()
        file_rows.pop(0)
        file_rows.pop(1)
        file_rows.pop(1)
        data = csv.DictReader(file_rows)

        for row in data:
            rows_count += 1

            if site == "GREON-01" and row['TIMESTAMP'] > "2017-07-02 02:52:00" and not greon1_to_greon2:
                if greon1_to_greon2 is False:
                    logging.debug("[greon.py] Switching GREON-01 to GREON-02")
                    sensorclient.sensor_statistics_post(sensor['id'])
                    # sensor, stream_wq, geocode = create_sensor_stream("GREON-02")
                    sensor, stream_wq, geocode, latest_datapoint = create_sensor_stream("GREON-02")
                    greon1_to_greon2 = True
                continue

            if latest_datapoint is None or time2utc(row['TIMESTAMP']) > str(latest_datapoint):

                count += 1
                properties = {}
                for key in row.keys():
                    if key in config['map_parameters'] and row[key] not in ['NAN']:
                        properties[config['map_parameters'][key]] = float(row[key])
                if properties:

                    datapoint_json = {
                        'start_time': time2utc(row['TIMESTAMP']),
                        'end_time': time2utc(row['TIMESTAMP']),
                        'type': 'Feature',
                        'geometry': {
                            'type': "Point",
                            'coordinates': [
                                geocode[0],
                                geocode[1],
                                0
                            ]
                        },
                        'stream_id': stream_wq['id'],
                        'sensor_id': sensor['id'],
                        'sensor_name': str(stream_wq['name']),
                        'properties': properties
                    }

                    good_datapoint_count += 1

                    datapoints_bulk_list.append(datapoint_json)

                    if good_datapoint_count % 100 == 0 or len(file_list) == 1:
                        logging.debug("[greon.py] Posting datapoints up to: " + str(count)+" of sensor " + str(site))

                        response = datapointclient.datapoint_create_bulk(datapoints_bulk_list)
                        logging.debug("[greon.py] %s" % response)


                        datapoints_bulk_list = []
                        if str(sensor['id']) not in binning_sensor_id_list:
                            binning_sensor_id_list.append(str(sensor['id']))
                else:
                    logging.debug("[greon.py] Not posting data for row - all NANs ")
        file_list.pop(0)

    if len(datapoints_bulk_list) > 0:
        logging.debug("[greon.py] creating datapoints up to (end of sensor loop) of sensor " + str(site))
        response = datapointclient.datapoint_create_bulk(datapoints_bulk_list)
        logging.debug(response)
        datapoints_bulk_list = []
        if str(sensor['id']) not in binning_sensor_id_list:
            binning_sensor_id_list.append(str(sensor['id']))

    logging.debug("[greon.py] updating sensor with id " + str(sensor['id']))
    sensorclient.sensor_statistics_post(sensor['id'])

    sensor_for_update = sensorclient.sensor_get(sensor['id']) #.json()['sensor']

    logging.debug("[greon.py] Setting online status for sensor " + str(sensor['id']))

    if sensor_for_update["max_end_time"][:10] > str(date.today() - relativedelta(days=5)):
        if "online_status" not in sensor_for_update["properties"] or sensor_for_update["properties"][
            "online_status"] != "online":
            logging.debug("changing online status to online for " + str(site))
            sensor_for_update["properties"]["online_status"] = "online"
            response = sensorclient.update_sensor_metadata(sensor_for_update)

    else:
        if "online_status" not in sensor_for_update["properties"] or sensor_for_update["properties"][
            "online_status"] != "offline":
            logging.debug("changing online status to offline for " + str(site))
            sensor_for_update["properties"]["online_status"] = "offline"
            response = sensorclient.update_sensor_metadata(sensor_for_update)

    logging.debug("[greon.py] Done with sensor " + str(sensor['id']))

if len(binning_sensor_id_list) > 0:
    logging.info("[greon.py] Start binning")
    subprocess.Popen(
        ["python", config['binning_path'],
         "--host", host,
         "-u", username,
         "-p", password,
         "-s", " ".join(binning_sensor_id_list),
         "-l", config['log_filename'],
         "-w", str(config['binning_wait']),
         "-n", str(config['n_binning_threads'])
         ]
    ).wait()
else:
    logging.info("[greon.py] There are no new datapoints to parser - skipping binning")

logging.debug("[greon.py] Done Parsing GREON.  Total time %sm" % ((time.time()-timer)/60))
logging.debug(" ")
