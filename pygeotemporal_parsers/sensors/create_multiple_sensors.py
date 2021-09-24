"""Create Sensors and Streams"""


import argparse
import yaml
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
import pandas as pd


def main():
    """Main Function"""

    # Required argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/multi_config.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    params = yaml.load(open(opts.config, 'r'))

    connection = params['connection']
    file_params = params['file']
    column_mappings = params['file']['column_mappings']
    sensor_config = params['config']['sensor']

    print(connection)
    try:
        sensor_client = SensorsApi(host=connection['url'], username=connection['user'], password=connection['password'])
        stream_client = StreamsApi(host=connection['url'], username=connection['user'], password=connection['password'])
    except:
        print("Unable to establish connection")
        return
    raw_data = pd.read_csv(file_params['path'])
    raw_data.rename({
                        column_mappings['sensor']:'sensor',
                        column_mappings['latitude']:'latitude',
                        column_mappings['longitude']:'longitude'
                    }, 
                    axis="columns", 
                    inplace=True
                    )

    create_multiple_sensors(raw_data, sensor_config, sensor_client, stream_client)
    # # Create Sensors and Streams
    # create_sensors_and_streams(
    #     sensor_client, stream_client, sensor_names, config)

def create_multiple_sensors(raw_data, config, sensor_client, stream_client):
    """Create multiple sensors from a csv file"""
    print("Creating " + str(len(raw_data.index)) + " sensors.")
    for index, data in raw_data.iterrows():
        sensor_name = data['sensor']
        lat = data['latitude']
        long = data['longitude']
        sensor_json = sensor_client.sensor_create_json(
            data['sensor'],
            data['longitude'],
            data['latitude'],
            0,
            data['sensor'],
            region= config['properties']['region'],
            organization_id= config['properties']['type']['id'],
            title=config['properties']['type']['title']
        )
        sensor = sensor_client.sensor_post_json(sensor_json)
        print("Created Sensor " + str(sensor['id']))
        
        # Create the Stream
        stream_json = stream_client.stream_create_json_from_sensor(sensor)
        stream_details = stream_client.stream_post_json(stream_json)
        stream_id = stream_details['id']
        print("Created Stream  " + str(stream_id))
        print("")

    return

if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
