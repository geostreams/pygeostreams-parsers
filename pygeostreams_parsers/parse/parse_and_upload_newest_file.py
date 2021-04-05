"""
    * Parse and Upload the Newest Locally-Present Data File *

    This Script will do the following:
        - Get the Newest File for parsing and upload
        - Get or Create Sensors and Streams
        - Get or Create a Collection
        - Get or Create a Dataset
        - Upload the Newest File to Clowder if necessary
        - Get or Create a File for parsing the new data
        - Parse the new Data
        - Update the Sensors Stats

"""

# Python Imports
import os
import yaml
import argparse
import csv
import glob
import pprint
import requests
from datetime import datetime

# Package Imports
from pyclowder.collections import CollectionsApi, get_datasets,\
    create_empty as create_empty_collection
from pyclowder.datasets import create_empty as create_empty_dataset, \
    get_file_list as file_list_dataset
from pygeostreams.sensors import SensorsApi
from pygeostreams.streams import StreamsApi
from pygeostreams.datapoints import DatapointsApi

# Module Imports
from parse_new_datapoints import parse_data, update_sensors_stats
from pygeostreams_parsers.sensors.create_sensors_and_streams \
    import create_sensors_and_streams


def main():
    """Main Function"""

    # Required command line argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/new_file_config.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    multi_config = yaml.load(open(opts.config, 'r'))

    # Variables from the config file
    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    key = multi_config['inputs']['key']
    file_path = multi_config['inputs']['file_path']
    file_type = multi_config['inputs']['file_type']
    timestamp = multi_config['inputs']['timestamp']
    parameters = multi_config['parameters']
    sensor_names = multi_config['sensors']
    config = multi_config['config']
    parse_file_name = os.path.join(file_path, multi_config['inputs']['parse'])
    header_timestamp = multi_config['header_timestamp']
    header_values = multi_config['header_to_parameters']

    # The Clients
    sensor_client = SensorsApi(host=url, key=key,
                               username=user, password=password)
    stream_client = StreamsApi(host=url, key=key,
                               username=user, password=password)
    datapoint_client = DatapointsApi(host=url, key=key,
                                     username=user, password=password)
    collections_client = CollectionsApi(host=url, key=key,
                                        username=user, password=password)

    # Get Newest Local File
    print("Will get newest local file to parse. ")
    newest_file = get_local_file(file_path, file_type, parse_file_name)

    datafile_name = os.path.basename(newest_file)
    print("Newest File is = " + datafile_name)

    if not os.path.exists(newest_file):
        print "Missing Newest File. "
        return

    # Get or Create File for Parsing
    print("Will get local file for parsing newest data. ")
    if os.path.exists(file_path):
        print("File for parsing is = " + parse_file_name)
        parse_file = open(parse_file_name, 'w+')
        # Remove old data
        parse_file.truncate()
    else:
        print "Missing File to Parse. "
        return

    # Get or Create Sensor
    print("Will get or create Sensors and Streams. ")
    create_sensors_and_streams(
        sensor_client, stream_client, sensor_names, config)

    # Get or Create Collection
    print("Will get or create Collection. ")
    collection_id = \
        get_or_create_collection(collections_client, url, key, config)

    # Get or Create Dataset
    print("Will get or create Dataset. ")
    dataset_id = \
        get_or_create_dataset(url, key, config, collection_id)

    # Upload File
    print("Will upload newest file to Clowder if it is not already present. ")
    upload_file(url, key, dataset_id, newest_file, datafile_name)

    if os.path.exists(newest_file):
        datafile = open(newest_file, 'rb')
    else:
        print "Missing Newest File. "
        return

    # Create to-be-parsed File
    print("Will update temporary to-be-parsed file. ")
    create_parse_file(stream_client, sensor_names, header_timestamp,
        datafile, parse_file, header_values)
    parse_file.close()

    # Parse Data
    print("Will parse data. ")
    parse_file = open(parse_file_name, 'r')
    parse_data(timestamp, config, sensor_names, parameters, parse_file,
               sensor_client, stream_client, datapoint_client)

    # Update the sensors
    print("Will update sensor stats. ")
    update_sensors_stats(sensor_client, sensor_names)

    # Ensure the files are closed properly regardless
    parse_file.close()
    datafile.close()
    print("Parsing Process Complete. ")


def get_local_file(file_path, extension, parse_file_name):
    """Get the Newest Local File to Parse"""

    # Get all Files
    all_files = filter(os.path.isfile, glob.glob(file_path + "/*" + extension))
    not_parse_file = filter(lambda x: not parse_file_name in x, all_files)

    # Sort the Files
    not_parse_file.sort(key=lambda x: os.path.getmtime(x))

    # Get the newest File for parsing
    newest_file = not_parse_file[len(not_parse_file) - 1]

    return newest_file


def get_or_create_collection(collections_client, url, key, config):
    """Get or Create a Collection for the General Sensor"""

    # Collection Info
    title = config['sensor']['properties']['name']
    description = config['sensor']['properties']['popupContent']

    # Get list of all Collections
    all_collections = collections_client.get_all_collections()

    if not len(all_collections):
        print('No Collections present - Creating Collection')
        collection_id = create_empty_collection(
            None, url + '/', key, title, description, None, None)
        print(pprint.pformat("Collection ID is = " + collection_id))
    else:
        print('Collections present - try to get Collection')
        collection = [collection for collection in all_collections
                         if collection['collectionname'] == title]
        if collection:
            collection_id = collection[0]['id']
            print(pprint.pformat("Collection ID is = " + collection_id))
        else:
            collection_id = None

    if not collection_id:
        print('Collection did not exist - Creating')
        collection_id = create_empty_collection(
            None, url + '/', key, title, description, None, None)
        print(pprint.pformat("Collection ID is = " + collection_id))

    return collection_id


def get_or_create_dataset(url, key, config, collection_id):
    """Get or Create a Dataset for the General Sensor"""

    # Dataset Info
    title = config['sensor']['properties']['name']
    description = config['sensor']['properties']['popupContent'] + ' - Daily'

    # Get list of all Datasets for the Collection ID
    all_datasets = get_datasets(None, url + '/', key, collection_id)

    if not len(all_datasets):
        print('No Datasets for the Collection present - Creating Dataset')
        dataset_id = create_empty_dataset(
            None, url + '/', key, title, description, collection_id)
        print(pprint.pformat("1 Dataset ID is = " + dataset_id))
    else:
        print('Datasets present - try to get Datasets')
        dataset = [dataset for dataset in all_datasets
                      if dataset['name'] == title]
        if dataset:
            dataset_id = dataset[0]['id']
            print(pprint.pformat("2 Dataset ID is = " + dataset_id))
        else:
            dataset_id = None

    if not dataset_id:
        print('Dataset did not exist  - Creating')
        dataset_id = create_empty_dataset(
            None, url + '/', key, title, description, collection_id)
        print(pprint.pformat("3 Dataset ID is = " + dataset_id))

    return dataset_id


def upload_file(url, key, dataset_id, datafile, filename):
    """Upload the Newest File to Clowder"""

    all_dataset_files = file_list_dataset(None, url + '/', key, dataset_id)
    existing_file_id = None

    for dataset_file in all_dataset_files:
        if dataset_file['filename'] == filename:
            print("File already present on Clowder. ")
            existing_file_id = dataset_file['id']
            print existing_file_id

    if existing_file_id == None:
        print("Uploading File to Clowder. ")
        r = requests.Session()
        response = r.post("%s/api/uploadToDataset/%s?key=%s" % (url, dataset_id, key),
                          files={'File': open(datafile, 'rb')})
        if response.status_code != 200:
            print('Problem uploading file  : [%d] - %s)' %
                  (response.status_code, response.text))
            return None
        file_id = str(response.json()['id'])
        print file_id

    return


def create_parse_file(stream_client, sensor_names, header_timestamp,
                      datafile, parse_file, header_values):
    """Add New Data to the Parsing File"""

    # All Stream End Times
    end_times = []

    for sensor_name in sensor_names:
        stream_details = stream_client.stream_get_by_name_json(sensor_name)
        end_time = stream_details[0]['end_time']
        if end_time:
            # Get in datetime format for comparison
            stream_end = (datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
                               .strftime('%Y-%m-%d %H:%M:%S'))
            end_times.append(stream_end)

    # Get latest Stream Time
    end_times.sort()
    if len(end_times) > 0:
        latest_time = end_times[len(end_times) - 1]
        latest_time = (datetime.strptime(latest_time, '%Y-%m-%d %H:%M:%S'))
    else:
        latest_time = \
            (datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))

    # Use CSV Dict Reader to interpret the CSV datafile
    reader = csv.DictReader(datafile)
    csv_keys = reader.fieldnames

    # Create Header
    header_value = ''
    for key in range(len(header_values)):
        header_value = \
            header_value + header_values[csv_keys[key]].strip() + ','
    parse_file.write(str(header_value[:-1]) + "\n")

    # Write to Parse File
    for row in reader:
        row_value = ''
        row_time = row[header_timestamp]
        for key in csv_keys:
            row_value = row_value + row[key].strip() + ','
        print row_value[:-1]
        # Get in datetime format for comparison
        row_end_time = (datetime.strptime(row_time, '%Y-%m-%d %H:%M:%S'))
        # Only want to write new data
        if row_end_time > latest_time:
            parse_file.write(str(row_value[:-1]) + "\n")

    return


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
