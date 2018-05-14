"""Delete Sensors and Streams"""


import yaml
import argparse
from pygeotemporal.sensors import SensorsApi


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
    sensor_names = multi_config['sensorsdelete']

    sensor_client = SensorsApi(host=url, key=key,
                              username=user, password=password)

    # Delete all existing Sensors, Streams, and Datapoints
    delete_sensors(sensor_client, sensor_names)


def delete_sensors(sensor_client, sensor_names):
    """
        Delete all sensors and attached datapoints/streams.
        Useful to reset instance for batch upload.

        This will only delete sensors that are defined in sensor_names.
    """

    print("DELETE ALL SENSORS, STREAMS, AND DATAPOINTS")

    for sensor_name in sensor_names:

        sensors = sensor_client.sensor_get_by_name(sensor_name).json()
        if len(sensors) > 0:
            for sensor in sensors:
                if sensor["name"] == sensor_name:
                    print("Deleting sensor " + sensor_name)
                    sensor_client.sensor_delete(sensor['id'])
        else:
            print("Sensor not found " + sensor_name)


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
