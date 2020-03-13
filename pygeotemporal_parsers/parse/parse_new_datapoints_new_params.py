"""
    Parse Concatenated New Data for Provided Sensor Names at a Location
    (Does not Create or Delete any Sensors or Streams)

    Will utilize defined new Parameter Names in the Config File
"""


import os
import time
import yaml
import argparse
import csv
from datetime import datetime
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi


def main():
    """Main Function"""

    # Required command line argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/multi_config.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    multi_config = yaml.load(open(opts.config, 'r'))

    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    local_path = multi_config['inputs']['file_path']
    datafile_file = str(local_path) + multi_config['inputs']['parse']
    timestamp = multi_config['inputs']['timestamp']
    parameters = multi_config['parameters']
    parameters_updated = multi_config['parameters_updated']
    sensor_names = multi_config['sensors']
    config = multi_config['config']

    if os.path.exists(datafile_file):
        datafile = open(datafile_file, 'r')
    else:
        print("Missing File to Parse.")
        return

    sensor_client = SensorsApi(host=url,
                               username=user, password=password)
    stream_client = StreamsApi(host=url,
                               username=user, password=password)
    datapoint_client = DatapointsApi(host=url,
                                     username=user, password=password)

    # Parse Data
    parse_data(timestamp, config, sensor_names, parameters, parameters_updated, datafile,
               sensor_client, stream_client, datapoint_client)

    # Update the sensors
    print("Will update sensor stats. ")
    update_sensors_stats(sensor_client, sensor_names)

    # Ensure the file is closed properly regardless
    datafile.close()


def update_sensors_stats(sensor_client, sensor_names):
    """Update all sensors stats after uploads"""

    for sensor_name in sensor_names:
        sensor_raw = sensor_client.sensor_get_by_name(sensor_name)
        sensor = sensor_raw.json()["sensors"]
        if len(sensor) == 1:
            sensor = sensor[0]
            sensor_id = sensor['id']
            print("Sensor found. Updating " + sensor_id)
            sensor_client.sensor_statistics_post(sensor_id)
        else:
            print("Sensor not found " + sensor_name)

    print("Sensor stats updated.")


def parse_data(timestamp, config, sensor_names, parameters, parameters_updated, datafile,
               sensor_client, stream_client, datapoint_client):
    """Parse all the Data"""

    # Use CSV Dict Reader to interpret the CSV datafile
    # First row is interpreted as the Dict Keys (i.e.: Header Row)
    reader = csv.DictReader(datafile)
    all_data = []
    for row in reader:
        all_data.append(row)
    csv_keys = reader.fieldnames

    # Save Coordinates for later usage (never changes)
    longitude = config['sensor']['geometry']['coordinates'][0]
    latitude = config['sensor']['geometry']['coordinates'][1]
    elevation = config['sensor']['geometry']['coordinates'][2]

    print("Parsing Datafile: " + str(datafile.name))
    print("Number of Rows to Parse = " + str(len(all_data)))

    for sensor_name in sensor_names:

        print('sensor_name is ' + sensor_name)

        # Get Sensor information for the Sensor Name
        sensor_raw = sensor_client.sensor_get_by_name(sensor_name)
        parse_sensor = sensor_raw.json()["sensors"]
        if len(parse_sensor) == 1:
            sensor = parse_sensor[0]
            sensor_id = sensor['id']
        else:
            continue

        # Get Stream information for the Sensor Name
        sensor_name = parse_sensor['name']
        stream_raw = stream_client.stream_get_by_name_json(sensor_name)
        parse_stream = stream_raw['streams'][0]
        stream_id = parse_stream['id']

        # Create Datapoints for each date for this Sensor Name
        for row in range(len(all_data)):

            # Get the Date in the proper format
            raw_date = all_data[row][timestamp]
            if '/' in raw_date:
                sensor_date_raw = (
                    datetime.strptime(raw_date, '%m/%d/%Y %H:%M:%S %p'))
                sensor_timestamp = time.mktime(sensor_date_raw.timetuple())
                sensor_date = (datetime.utcfromtimestamp(sensor_timestamp)
                               .strftime('%Y-%m-%dT%H:%M:%SZ'))
            else:
                sensor_date = (datetime.strptime(raw_date,'%Y-%m-%d %H:%M:%S')
                               .strftime('%Y-%m-%dT%H:%M:%SZ'))
            # Start Date == End Date
            start_time = sensor_date
            end_time = sensor_date
            properties = {}
            # Get the Data Value for each parameter
            for x in parameters:
                if x in csv_keys and parameters[x] == sensor_name:
                    data_value = all_data[row][x]
                    # Set Sensor Name Column Data
                    if data_value != '':
                        if parameters_updated is not None:
                            properties[parameters_updated[x]] = data_value
                        else:
                            properties[x] = data_value
                    else:
                        print("Data Field is blank - Not Parsing")
            # Create Datapoint
            datapoint = {
                'start_time': start_time,
                'end_time': end_time,
                'type': 'Feature',
                'geometry': {
                    'type': "Point",
                    'coordinates': [
                        longitude,
                        latitude,
                        elevation
                    ]
                },
                "properties": properties,
                'stream_id': str(stream_id),
                'sensor_id': str(sensor_id),
                'sensor_name': str(sensor_name)
            }

            # Post Datapoint
            datapoint_post = datapoint_client.datapoint_post(datapoint)

            print("Created Datapoint %s for Sensor %s Stream %s for %s"
                  % (str(datapoint_post.json()['id']), str(sensor_id),
                     str(stream_id), str(sensor_date)))

    # End of Loop

    print("Parsing Done")

    return


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
