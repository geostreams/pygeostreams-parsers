import yaml
import sys
from pprint import pprint
import requests
import copy
import csv
import logging
import time
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import date, datetime
import subprocess

from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi
from pygeotemporal.cache import CacheApi
from pygeotemporal.time_transformers import time2utc

timer = time.time()

# Append path to hucfinder as needed
# TODO add to config.yml
sys.path.append('../../..')
from huc_finder.huc import HucFinder
hucfinder = HucFinder('../../../huc_finder/huc-all.shp')

config = yaml.load(open("config.yml", 'r'), Loader=yaml.FullLoader)

# configure logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("fiona").setLevel(logging.WARNING)
logging.basicConfig(filename=config['log_filename'], level=logging.DEBUG)

host = config['host']
username = config['username']
password = config['password']

logging.info(" ")
logging.info("[usgs.py] Starting USGS Parser for %s" % host)

# Authenticate
sensorclient = SensorsApi(host=host, username=username, password=password)
streamclient = StreamsApi(host=host, username=username, password=password)
datapointclient = DatapointsApi(host=host, username=username, password=password)
cacheclient = CacheApi(host=host, username=username, password=password)

logging.debug("[usgs.py] Authentication complete for " + str(host))

sites_source = {'super_gauges': config['gltg-sites-supergauges'],
                        'regular_gauges': config['gltg-sites']}


binning_sensor_id_list = []

# run supergauges and regular gauges
for sub_source in sites_source:

    logging.debug("[usgs.py] ----- parsing %s ------" % sub_source)

    total_sensor_count = 0

    binning_sensor_id_list = []
    for site in sites_source[sub_source]:

        total_sensor_count += 1

        sensor = None
        stream = None
        sensor_usgs_json = None
        sensor_json = None

        logging.debug("[usgs.py] parsing site %s at sensor_count=%s" % (site, str(total_sensor_count)))

        start_date = config['start_date']  # %Y-%m-%d this is just a string - indeterminate what time zone
        end_date = date.today().strftime( '%Y-%m-%d')  # %Y-%m-%d this is just a string - indeterminate what time zone

        # get the sensor or create if needed
        sensor_for_site = sensorclient.sensor_get_by_name(site).json()['sensors']

        if len(sensor_for_site)  > 0:
            sensor = sensor_for_site[0]
            try:
                stream = streamclient.stream_get_by_name_json(site)['streams'][0]
            except:
                logging.debug("[usgs.py] Unable to get stream for %s" % site)
                continue

            logging.debug("[usgs.py] Getting data from USGS for %s" % site)

            if sensor['max_end_time'] == 'N/A':
                logging.debug("[usgs.py] Sensor %s has no max_end_time - trying updating statistics" % site)
                sensorclient.sensor_statistics_post(sensor['id'])
                sensor = sensorclient.sensor_get_by_name(site).json()['sensors'][0]

            if sensor['max_end_time'] != 'N/A':
                start_date = sensor['max_end_time'][:10]

            sensor_url = config['data_source_url'] + "format=json&sites=" + site + "&parameterCd=" + ','.join(
                config['parameters_from_usgs']) + "&startDT=" + start_date

            logging.debug('[usgs.py] %s' % sensor_url)
            # increase timeout to 10minutes
            r = requests.get(sensor_url, timeout=600)
            logging.debug(r.status_code)
            sensor_usgs_json = r.json()

        else:
            logging.debug("[usgs.py] Creating sensor_name=%s" % str(site))

            logging.debug("[usgs.py] getting data from USGS for %s" % site)
            sensor_url = config['data_source_url'] + "format=json&sites=" + site + "&parameterCd=" + ','.join(
                config['parameters_from_usgs'])

            try:
                r = requests.get(sensor_url, timeout=600)
            except RequestException as e:
                logging.debug("[usgs.py] ERROR getting data from USGS for %s with error s%" % (site, e))
                continue

            sensor_usgs_json = r.json()

            # create new sensor and stream
            logging.debug("[usgs.py] Getting huc")

            huc_json = hucfinder.getHuc(
                lat=float(sensor_usgs_json['value']['timeSeries'][0]['sourceInfo']["geoLocation"]["geogLocation"]["latitude"]),
                lon=float(sensor_usgs_json['value']['timeSeries'][0]['sourceInfo']["geoLocation"]["geogLocation"]["longitude"])
            )

            if 'huc4' not in huc_json:
                logging.debug("[usgs] problem getting huc for %s - skipping" % site)
                continue

            logging.debug("[usgs.py] creating sensor and stream")
            if sub_source == "super_gauges":
                source_id = "usgs-sg"
            else:
                source_id = "usgs"


            sensor_json = sensorclient.sensor_create_json(
                site,
                sensor_usgs_json['value']['timeSeries'][0]['sourceInfo']["geoLocation"]["geogLocation"]["longitude"],
                sensor_usgs_json['value']['timeSeries'][0]['sourceInfo']["geoLocation"]["geogLocation"]["latitude"],
                0,
                site,
                huc_json['huc4']['code'],
                huc=huc_json,
                network="usgs",
                organization_id=source_id,
                title="United States Geographical Survey",
            )

            sensor = sensorclient.sensor_post_json(sensor_json)

            # create or get stream
            stream_json = streamclient.stream_create_json_from_sensor(sensor)
            stream = streamclient.stream_post_json(stream_json)

        logging.debug("[usgs.py] Parsing to sensor id " + str(sensor['id']) + " stream id " + str(stream['id']))
        logging.debug("[usgs.py] Getting latest datapoint for stream id " + str(stream['id']))

        # get most recent datapoint for this sensor (used for start of update parsing)
        # start_time as 2017-08-04T13:15:00Z
        latest_datapoint = sensor['max_end_time']
        if latest_datapoint is None:
            logging.debug("[usgs.py] No previous datapoint")
        else:
            logging.debug("[usgs.py] latest_datapoint end_time " + latest_datapoint)
            logging.debug("[usgs.py] sensor end_time" + sensor['max_end_time'])

        if latest_datapoint is not None and latest_datapoint != "N/A":
            latest_datapoint_datetime = parse(latest_datapoint)  # now in zulu as datetime
            start_date = latest_datapoint_datetime.strftime('%Y-%m-%d')  # time zone zulu eg. 2015-10-22 as string
        else:
            latest_datapoint_datetime = None

        # convert to datetimes
        start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date() - relativedelta(days=1)
        end_date = datetime.strptime(str(end_date), "%Y-%m-%d").date()

        step_start_date = copy.copy(start_date)

        datapoint_count = 0

        # EACH LOOP IS FOR A YEAR OF DATA
        while step_start_date <= end_date:
            if step_start_date + relativedelta(years=1) >= end_date:
                step_end_date = end_date + relativedelta(days=1)
            else:
                step_end_date = step_start_date + relativedelta(years=1) - relativedelta(days=1)

            # get data from usgs
            rdb_url = config['data_source_url'] + "format=rdb&sites=" + site + "&parameterCd=" \
                      + ','.join(config['parameters_from_usgs']) + '&startDT=' + str(step_start_date) \
                      + '&endDT=' + str(step_end_date)
            logging.debug("[usgs.py] getting data from usgs using: " + str(rdb_url))

            try:
                r = requests.get(rdb_url)
                r.raise_for_status()
            except:
                logging.error("[usgs.py] Failed to get data from: " + str(rdb_url))
                logging.error("[usgs.py] Waiting 10 seconds and trying again")
                time.sleep(10)
                r = requests.get(rdb_url)
                r.raise_for_status()

            rdb_data = r.text
            step_start_date = step_start_date + relativedelta(years=1)

            if rdb_data is None or rdb_data[0:11] == "#  No sites":
                logging.debug(
                    "[usgs.py] No data available from: " + str(step_start_date - relativedelta(years=1)) + " to: " + str(
                        step_end_date))
                continue

            # put data into a dictionary
            rows = []
            lineOfText = ""
            for i in range(len(rdb_data)):
                if rdb_data[i] != "\n":
                    lineOfText += rdb_data[i]
                else:
                    if lineOfText[0] != "#":
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

                start_time = time2utc(row['datetime'] + ":00")
                if start_time in start_time_hold:
                    logging.debug(
                        "[usgs.py] Start time " + str(start_time) + " already has been posted for this sensor - not posting")
                    continue
                start_time_hold.append(start_time)

                if latest_datapoint_datetime is None or parse(start_time) > latest_datapoint_datetime:
                    properties = {}
                    for key in row.keys():
                        key_split = key.split("_", 1)
                        if len(key_split) < 2:
                            continue

                        if key_split[1] in config['map_parameters'] and row[key] not in [None, "", " ", "eqp",
                                                                                         "ice", "***", "ICE", "Eqp",
                                                                                         "Fld", "Ssn", "Dis", "Ice",
                                                                                         "Bkw", "Mnt", "DIS", "Rat",
                                                                                         "ZFL", "Dry","Tst"]:
                            if key[-3:] == '_cd':
                                properties[config['map_parameters'][key_split[1]]] = row[key]
                            else:
                                properties[config['map_parameters'][key_split[1]]] = float(row[key])

                    datapoint_json = {
                        'start_time': start_time,
                        'end_time': start_time,
                        'type': 'Feature',
                        'geometry': {
                            'type': "Point",
                            'coordinates': [

                                sensor['geometry']['coordinates'][0],
                                sensor['geometry']['coordinates'][1],
                                sensor['geometry']['coordinates'][2]
                            ]
                        },
                        'stream_id': stream['id'],
                        'sensor_id': sensor['id'],
                        'sensor_name': str(stream['name']),
                        'properties': properties
                    }

                    datapoint_count += 1

                    datapoints_bulk_list.append(datapoint_json)

                    if datapoint_count > 100 or rows_total == rows_count:
                        logging.debug("[usgs.py] posting datapoints for stream "
                                      + str(stream['id']) + " sensor " + str(sensor['id']))
                        r = datapointclient.datapoint_create_bulk(datapoints=datapoints_bulk_list)
                        if r.status_code != 200:
                            r1 = datapointclient.datapoint_create_bulk(datapoints_bulk_list[:len(datapoints_bulk_list)//2])
                            r2 = datapointclient.datapoint_create_bulk(datapoints_bulk_list[len(datapoints_bulk_list)//2:])

                            if r1.status_code != 200 or r2.status_code != 200:
                                logging.debug("[usgs.py] Failed to post  bulk of datapoints")
                        datapoints_bulk_list = []
                        logging.debug(r)
                        if r.status_code != 200:
                            logging.debug("[usgs.py] ERROR  : Could not add datapoint to stream : [" + str(
                                r.status_code) + "] - " + r.text)
                        if str(sensor['id']) not in binning_sensor_id_list:
                            logging.debug("usgs.py] Added sensor_id%s to binning_sensor_id_list" % str(sensor['id']))
                            binning_sensor_id_list.append(str(sensor['id']))
                        datapoint_count = 0

        logging.debug("[usgs.py] Updating sensor with id " + str(sensor['id']))
        sensorclient.sensor_statistics_post(sensor['id'])

        sensor_for_update = sensorclient.sensor_get(sensor['id'])

        logging.debug("[usgs.py] Setting online status for sensor " + str(sensor['id']))

        if sensor_for_update["max_end_time"][:10] > str(date.today() - relativedelta(days=5)):
            if "online_status" not in sensor_for_update["properties"] or \
                    sensor_for_update["properties"]["online_status"] != "online":
                logging.debug("Changing online status to online for " + str(site))
                sensor_for_update["properties"]["online_status"] = "online"
                response = sensorclient.update_sensor_metadata(sensor_for_update)
                logging.info(response.status_code)

        else:
            if "online_status" not in sensor_for_update["properties"] or \
                    sensor_for_update["properties"]["online_status"] != "offline":
                logging.debug("Changing online status to offline for " + str(site))
                sensor_for_update["properties"]["online_status"] = "offline"
                response = sensorclient.update_sensor_metadata(sensor_for_update)
        logging.debug("[usgs.py] Done parsing sensor_id " + str(sensor['id']))

    if len(binning_sensor_id_list) > 0:
        logging.info("[usgs.py] Start binning")
        binning_wait = str(config['binning_wait'])

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
        logging.info("[usgs.py] There are no new datapoints to parser - skipping binning")
        continue
    if sub_source == "regular_gauges":
        logging.info("[usgs.py] Waiting for 20m")
        time.sleep(1200)

logging.debug("[usgs.py] Finished parsing usgs at %sm" % str((time.time() - timer) / 60))