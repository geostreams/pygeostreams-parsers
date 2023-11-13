from pygeostreams.sensors import SensorsApi
from pygeostreams.streams import StreamsApi
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.cache import CacheApi
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import yaml
import sys
from pprint import pprint
import requests
import logging
import time
import json
import subprocess

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
logging.getLogger("geopandas").setLevel(logging.ERROR)
logging.getLogger("pyproj").setLevel(logging.ERROR)
logging.basicConfig(filename=config['log_filename'], level=logging.DEBUG)

host = config['host']
username = config['username']
password = config['password']

sensorclient = SensorsApi(host=host, username=username, password=password)
datapointclient = DatapointsApi(host=host, username=username, password=password)
streamclient = StreamsApi(host=host, username=username, password=password)
cacheclient = CacheApi(host=host, username=username, password=password)


# iwqis urls
sites_url = u"https://api2.iwqis.iowawis.org/v1/sites"
datapoints_url = u"https://api2.iwqis.iowawis.org/v1/hourly?site_uid=[UID]&begin=[BEGIN]"
param_url = u"https://api2.iwqis.iowawis.org/v1/params/[PARAM]"

# config for time
start_date_get_data = config['new_sensor_start_time']

# TODO add to config
# get all sites from iwqis
sites = requests.get(sites_url).json()
# or get a subset
#sites = config['ilnlrs_sites']

logging.debug(" [iwqis.py] Starting parser for host " + str(host))

binning_sensor_id_list = []

property_list = []
count_sites = 0
for site in sites:

    count_sites += 1

    stream = None
    sensor = None

    logging.debug("[iwqis.py] parsing iwqis site: %s" % str(site['uid']))

    sensor_for_site = sensorclient.sensor_get_by_name(site['uid']).json()['sensors']

    # get the sensor or create if needed
    if len(sensor_for_site) > 0:
        sensor = sensor_for_site[0]
        stream = streamclient.stream_get_by_name_json(site['uid'])['streams'][0]

    else:
        logging.debug("[iwqis.py] Creating sensor_name=%s" % str(site['uid']))

        # create new sensor and stream
        logging.debug("[iwqis.py] Getting huc")
        huc_json = hucfinder.getHuc(lat=site['latitude'], lon=site['longitude'])

        if 'huc4' not in huc_json:
            logging.debug("[iwqis] problem getting huc for %s - skipping" % site['uid'])
            continue

        logging.debug("[iwqis.py] creating sensor and stream")
        sensor_json = sensorclient.sensor_create_json(
            site['uid'],
            float(site['longitude']),
            float(site['latitude']),
            0,
            site['uid'],
            huc_json['huc4']['code'],
            huc=huc_json,
            network="iwqis",
            organization_id="iwqis",
            title="Iowa Water Quality Information System",
        )

        sensor = sensorclient.sensor_post_json(sensor_json)

        # create or get stream
        stream_json = streamclient.stream_create_json_from_sensor(sensor)
        stream = streamclient.stream_post_json(stream_json)

    if sensor['max_end_time'] in ["N/A",None]:
        logging.debug("[iwqis.py] New sensor: start time set to %s" % config['new_sensor_start_time'])
        latest_datapoint = config['new_sensor_start_time']
    elif len(sensor['max_end_time']) == 20:
        logging.debug("[iwqis.py] Preexisting sensor: start time set to %s" % sensor['max_end_time'])
        latest_datapoint = sensor['max_end_time']
    else:
        logging.debug("[iwqis.py] problem getting latest_datapoint - continuing")
        continue

    start_date_get_data = latest_datapoint

    datapoint_count = 0
    datapoints_bulk_list = []
    days_count = 0
    data_exists = True
    while True:
        # get datapoints from iwqis
        site_datapoints_url = datapoints_url.replace('[UID]', sensor['name']).replace('[BEGIN]', start_date_get_data)
        logging.debug(" [iwqis.py] Fetching data from " + str(site_datapoints_url))

        source_days = requests.get(site_datapoints_url).json()

        if len(source_days) == 0:
            logging.debug(" [iwqis.py] Posting datapoints up to " + str(datapoint_count))
            response = datapointclient.datapoint_create_bulk(datapoints_bulk_list)
            logging.debug('[iwqis.py] response status %s' % response)
            break

        start_date_get_data = datetime.strftime(datetime.strptime(source_days[-1]['day'].split("T")[0],'%Y-%m-%d') + relativedelta(days=1),'%Y-%m-%d')

        # loop through list of daily json data from iwqis - each day json object contains hourly values
        for source_day in source_days:
            days_count += 1

            # get parameter (nitrate etc.) info for day of data
            param = requests.get(param_url.replace("[PARAM]",source_day['param_uid'])).json()
            property_id = config['map_parameters'][param['uid'] + "-iwqis"]

            # loop through hourly data for single day: parse and post
            hour_count = 0
            for datapoint_source in source_day['data']:
                if datapoint_source is None:
                    continue
                properties = {}
                properties[property_id] = datapoint_source
                start_time =  datetime.strftime(parse(source_day['day']) + relativedelta(hours=hour_count), "%Y-%m-%dT%H:%M:%SZ")

                datapoint_json = {
                    'start_time': start_time,
                    'end_time': start_time,
                    'type': 'Feature',
                    'geometry': {
                        'type': "Point",
                        'coordinates': [
                            float(site['longitude']),
                            float(site['latitude']),
                            0
                        ]
                    },
                    'stream_id': stream['id'],
                    'sensor_id': sensor['id'],
                    'sensor_name': str(stream['name']),
                    'properties': properties
                }
                datapoints_bulk_list.append(datapoint_json)
                hour_count+=1
                datapoint_count += 1
            if len(datapoints_bulk_list) > 100 or days_count == len(source_days):
                response = datapointclient.datapoint_create_bulk(datapoints_bulk_list)
                # if datapoint_count % 5000 == 0:
                logging.debug(" [iwqis.py] Posted datapoints (end) up to " + str(datapoint_count))
                logging.debug( '[iwqis.py] response status %s' % response)
                datapoints_bulk_list =[]

                if str(sensor['id']) not in binning_sensor_id_list:
                    binning_sensor_id_list.append(str(sensor['id']))

    start_date_get_data = config['new_sensor_start_time']
    logging.debug("[iwqis.py] updating sensor " + str(sensor['id']))
    sensorclient.sensor_statistics_post(sensor['id'])

    sensor_for_update = sensorclient.sensor_get(sensor['id']).json()

    logging.debug(" [iwqis.py] Setting online status for sensor " + str(sensor['id']))

    if sensor_for_update["max_end_time"][:10] > str(date.today() - relativedelta(days=5)):
        # set to online
        if "online_status" not in sensor_for_update["properties"] or sensor_for_update["properties"]["online_status"] != "online":
            logging.debug("Changing online status to online for " + str(site['uid']))
            sensor_for_update["properties"]["online_status"] = "online"
            response = sensorclient.update_sensor_metadata(sensor_for_update)
    else:
        # set to offline
        if "online_status" not in sensor_for_update["properties"] or sensor_for_update["properties"]["online_status"] != "offline":
            logging.debug("Changing online status to offline for " + str(site['uid']))
            sensor_for_update["properties"]["online_status"] = "offline"
            response = sensorclient.update_sensor_metadata(sensor_for_update)

    logging.debug(" [iwqis.py] Done with sensor " + str(sensor['id']))

if len(binning_sensor_id_list) > 0:
    logging.info("[iwqis.py] Start binning")
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
    logging.info("[iwqis.py] There are no new datapoints to parser - skipping binning")

logging.debug(" [iwqis.py] Done Parsing IWQIS. Total time %sm" % ((time.time()-timer)/60))
logging.debug(" ")
