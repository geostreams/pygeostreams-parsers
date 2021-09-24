"""
    Parse CSV files containing nested parameters. 
    (Creates streams if not present. Does not create new sensors or parameters)

    
    python parse_datapoints_multi_sensors.py -c /pygeotemporal-parsers/SMARTFARM/soil_gas_flux.yml
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

    params = yaml.load(open(opts.config, 'r'))

    connection = params['connection']
    file_params = params['file']
    config = params['config']
    column_mappings = params['file']['column_mappings']
    if 'ignore_columns' not in file_params:
        ignore_columns = []
    else:
        ignore_columns = file_params['ignore_columns']


    if not os.path.exists(file_params['path']):
        print("Missing File to Parse.")
        return

    try:
        sensor_client = SensorsApi(host=connection['url'], username=connection['user'], password=connection['password'])
        stream_client = StreamsApi(host=connection['url'], username=connection['user'], password=connection['password'])
        datapoint_client = DatapointsApi(host=connection['url'], username=connection['user'], password=connection['password'])
        parameters_client = GeostreamsClient(host=connection['url'], username=connection['user'], password=connection['password'])
        cache_client = CacheApi(host=connection['url'], username=connection['user'], password=connection['password'])
    except:
        print("Unable to connect to geostreams api")
        return

    

    date_parser = lambda d: pd.to_datetime(d, errors='coerce')
    raw_data = pd.read_csv(file_params['path'], 
                            parse_dates=[column_mappings['timestamp']], 
                            infer_datetime_format= True,
                            date_parser=date_parser,
                            usecols= lambda x: x not in ignore_columns, 
                        )
    raw_data.dropna(how='all', axis=1, inplace=True)

    raw_data.rename({
                        column_mappings['timestamp']:'timestamp',
                    }, 
                    axis="columns", 
                    inplace=True
                    )

    ## Preprocess data
    if(config['sanitize']):
        raw_data.dropna(inplace=True)

    sensors = [config['sensor_name']]

    print("Getting sensor/stream data from geostreams-api. ")
    sensors_data, streams_data = get_sensor_streams(sensors, 
                                                    sensor_client, 
                                                    stream_client)


    #  Parse Data
    print("Parsing Datafile: " + str(os.path.basename(file_params['path'])))
    parse_data(raw_data, sensors_data, streams_data, config,  column_mappings['value'], datapoint_client)

    # ## Update the sensors
    print("Will update sensor stats. ")
    update_sensors_stats(sensor_client, sensors, True)

    print("Done!")

def get_sensor_streams(sensor_list, sensor_client, stream_client):
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
            stream_raw = stream_client.stream_get_by_name_json(sensor_name)
            stream_raw = stream_raw['streams'][0]
        except (IndexError, TypeError):
            # Create stream if does not exists
            stream_json = stream_client.stream_create_json_from_sensor(sensor_raw)
            stream_json['name']=stream_json['name'] 
            stream_raw = stream_client.stream_post_json(stream_json)

        streams_data[sensor_name] = stream_raw 
        
    return(sensors_data, streams_data)


def update_sensors_stats(sensor_client, sensor_names, printout=False):
    """Update all sensors stats after uploads"""

    for sensor_name in sensor_names:
        if '&' in sensor_name:
            sensor_name = str(sensor_name.replace('&', '%26'))

        sensor_raw = sensor_client.sensor_get_by_name(sensor_name)
        sensor = sensor_raw.json()["sensors"]
        if len(sensor) == 1:
            sensor = sensor[0]
            sensor_id = sensor['id']
            if printout is True:
                print("Sensor found. Updating " + str(sensor_id))
            print(sensor_id)
            sensor_client.sensor_statistics_post(sensor_id)
        else:
            print("Sensor not found " + sensor_name)

    if printout is True:
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

def parse_data(raw_data, sensors_data, streams_data, config, param_mapping, datapoint_client):
    """Parse all the Data"""

    print("Number of Rows to Parse = " + str(len(raw_data.index)))

    for index, data in raw_data.iterrows():
        date = data['timestamp'].strftime('%Y-%m-%dT%H:%M:%SZ')
        sensor_name = config['sensor_name']
        stream_id = streams_data[sensor_name]['id']
        sensor_id = sensors_data[sensor_name]['id']
        properties = {}
        # Get the Data Value for each parameter
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
        print(datapoint)
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
