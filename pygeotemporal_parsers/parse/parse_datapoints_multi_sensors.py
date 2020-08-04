"""
    Parse Concatenated New Data for for multiple sensors given in the file
    (Creates streams if not present. Does not create new sensors or parameters)

    
    python parse_datapoints_multi_sensors.py -c /Users/aarajh/pygeotemporal-parsers/EPA_GLM/phytoplankton.yaml
"""

import os
import sys
import time
import copy
import yaml
import argparse
import csv
import pandas as pd
from datetime import datetime
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi
from pygeotemporal.client import GeostreamsClient
from pygeotemporal.cache import CacheApi
from datetime import date,datetime


def main():
    """Main Function"""

    # Required command line argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default='EPA_GLM/zooplankton.yaml',
                    required=False)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    print(os.getcwd())

    multi_config = yaml.load(open(opts.config, 'r'))

    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    local_path = multi_config['inputs']['file_path']
    datafile_file = str(local_path) + multi_config['inputs']['parse']
    timestamp = multi_config['inputs']['timestamp']
    ignore_columns = multi_config['inputs']['ignore_columns']
    source = multi_config['inputs']['source']
    sensor = multi_config['inputs']['sensor']
    param_mapping = multi_config['param_mapping']

    # config = multi_config['config']

    if not os.path.exists(datafile_file):
        print("Missing File to Parse.")
        return

    sensor_client = SensorsApi(host=url,
                                username=user, password=password)
    stream_client = StreamsApi(host=url,
                                username=user, password=password)
    datapoint_client = DatapointsApi(host=url,
                                username=user, password=password)
    parameters_client = GeostreamsClient(host=url, 
                                username=user, password=password)
    cache_client = CacheApi(host=url, username= user, password=password)


    raw_data = pd.read_csv(datafile_file, 
                            parse_dates=[timestamp], 
                            infer_datetime_format= True,
                            usecols= lambda x: x not in ignore_columns, 
                        )
    ## Preprocess data

    # Remove spaces in sensor name if any
    raw_data[sensor] = raw_data[sensor].str.replace(' ','')

    # Remove data points for sensors in skip_sensors list if provided
    if 'skip_sensors' in multi_config:
        skip_sensors = multi_config['skip_sensors']
        raw_data = raw_data[~raw_data[sensor].isin(skip_sensors)]

    sensors = raw_data[sensor].unique()

    print("Getting sensor/stream data from geostreams-api. ")
    sensors_data, streams_data = get_sensor_streams(sensors, 
                                                    sensor_client, 
                                                    stream_client, 
                                                    source)

    #  Parse Data
    print("Parsing Datafile: " + str(os.path.basename(datafile_file)))
    parse_data(raw_data, sensors_data, streams_data, datapoint_client, sensor, timestamp, param_mapping)

    ## Update the sensors
    print("Will update sensor stats. ")
    update_sensors_stats(sensor_client, sensors)

    print("Done!")

def get_sensor_streams(sensor_list, sensor_client, stream_client, source):
    sensors_data = {}
    streams_data = {}
    for sensor_name in sensor_list:
        # Get Sensor information for the Sensor Name
        try:
            sensor_raw = sensor_client.sensor_get_by_name(sensor_name)
            sensor_raw = sensor_raw.json()["sensors"][0]
            # sensor_raw.pop('parameters')
        except (IndexError, TypeError) as e:
            print ("Error getting the sensor info",sensor_name,". Are you sure it exists?\n", str(e))
            sys.exit(1)

        sensors_data[sensor_name] = sensor_raw
        
         # Get Stream information for the Sensor Name
        try:
            stream_raw = stream_client.stream_get_by_name_json(source + "-" + sensor_name)
            stream_raw = stream_raw['streams'][0]
        except (IndexError, TypeError):
            # Create stream if does not exists
            stream_json = stream_client.stream_create_json_from_sensor(sensor_raw)
            stream_json['name']=source + '-' + stream_json['name'] 
            stream_raw = stream_client.stream_post_json(stream_json)

        streams_data[sensor_name] = stream_raw 
        
    return(sensors_data, streams_data)


def update_sensors_stats(sensor_client, sensor_names):
    """Update all sensors stats after uploads"""

    for sensor_name in sensor_names:
        sensor_raw = sensor_client.sensor_get_by_name(sensor_name)
        sensor = sensor_raw.json()["sensors"]
        if len(sensor) == 1:
            sensor = sensor[0]
            sensor_id = sensor['id']
            print("Sensor found. Updating " + str(sensor_id))
            res = sensor_client.sensor_statistics_post(sensor_id)
            print( res.text )
        else:
            print("Sensor not found " + sensor_name)

    print("Sensor stats updated.")

def param_reader(root, data):
    for key, value in root.items():
        # Add check if value is some/any other type of object
        # Use try except? PROBABLY NOT 
        if isinstance(value, str) or isinstance(value, float) or isinstance(value, int):
            root[key] = data[value]
        else:
            root[key] = param_reader(value, data)
    return root

def get_date_by_season_year(season, year):
    if season == 'Spring':
        return date(year, 4, 1)
    if season == 'Summer':
        return date(year, 6, 1)
    else:
        return None

def parse_data(raw_data, sensors_data, streams_data, datapoint_client, sensor, timestamp, param_mapping):
    """Parse all the Data"""

    print("Number of Rows to Parse = " + str(len(raw_data.index)))

    for index, data in raw_data.iterrows():
        # Get date if available otherwise compute a random date based on season and year
        # (Specifically for phytoplankton as some datapoints donot have a date)
        try:
            date = data[timestamp].strftime('%Y-%m-%dT%H:%M:%SZ')
        except (ValueError):
            date = get_date_by_season_year(data['CRUISE'],data['YEAR']).strftime('%Y-%m-%dT%H:%M:%SZ')
        print (date)
        sensor_name = data[sensor]
        stream_id = streams_data[sensor_name]['id']
        sensor_id = sensors_data[sensor_name]['id']
        properties = copy.deepcopy(param_mapping)
        properties = param_reader(properties, data)
        datapoint = {
                'start_time': date,
                'end_time': date,
                'type': sensors_data[sensor_name]['geoType'],
                'geometry': sensors_data[sensor_name]['geometry'],
                 "properties": properties,
                 'stream_id': stream_id,
                 'sensor_id': sensor_id,
                 'sensor_name': str(sensor_name)
             }
        # Post Datapoint
        datapoint_post = datapoint_client.datapoint_post(datapoint)
        print("Created Datapoint %s for Sensor %s Stream %s for %s"
            % (str(datapoint_post.json()['id']), str(sensor_id),
            str(stream_id), str(date)))

    print("Parsing Done")

    return


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
