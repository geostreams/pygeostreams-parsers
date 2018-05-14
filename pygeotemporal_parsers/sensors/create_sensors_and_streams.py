"""Create Sensors and Streams"""


import argparse
import yaml
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi


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

    multi_config = yaml.load(open(opts.config, 'r'))

    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    key = multi_config['inputs']['key']
    sensor_names = multi_config['sensorscreate']
    config = multi_config['config']

    sensor_client = SensorsApi(host=url, key=key,
                               username=user, password=password)
    stream_client = StreamsApi(host=url, key=key,
                               username=user, password=password)

    # Create Sensors and Streams
    create_sensors_and_streams(
        sensor_client, stream_client, sensor_names, config)


def create_sensors_and_streams(
        sensor_client, stream_client, sensor_names, config):
    """Create all sensors and streams"""

    print("Creating Sensors and Streams")

    for sensor_name in sensor_names:

        # Create the Sensor
        sensor_json = sensor_client.sensor_create_json(
            sensor_name,
            config['sensor']['geometry']['coordinates'][0],
            config['sensor']['geometry']['coordinates'][1],
            config['sensor']['geometry']['coordinates'][2],
            sensor_name,
            config['sensor']['properties']['region'],
            config['sensor']['properties']['huc'],
            config['sensor']['properties']['type']['network'],
            config['sensor']['properties']['type']['id'],
            config['sensor']['properties']['type']['title']
        )
        sensor = sensor_client.sensor_post_json(sensor_json)
        print "Created Sensor " + str(sensor['id'])

        # Create the Stream
        stream_json = stream_client.stream_create_json_from_sensor(sensor)
        stream_details = stream_client.stream_post_json(stream_json)
        stream_id = stream_details['id']
        print "Created Stream  " + str(stream_id)

    return


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
