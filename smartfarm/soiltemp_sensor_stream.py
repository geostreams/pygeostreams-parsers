import sys


from pygeostreams.sensors import SensorsApi
from pygeostreams.cache import CacheApi
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.streams import StreamsApi

sys.path.append('..')

# instantiate API objects
sensors_client = SensorsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams", username="",
                            password="")
cache_client = CacheApi(host="https://smartfarm.ncsa.illinois.edu/geostreams", username="",
                            password="")
datapoints_client = DatapointsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams",
                                  username="", password="")
streams_client = StreamsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams", username="",
                            password="")


sensor_json = sensors_client.sensor_create_json("Reifsteck Site",
                                                  -88.290075,
                                                  40.0062722,
                                                  0,
                                                  "Reifsteck Site",
                                                  "Reifsteck Site",
                                                  organization_id='field',
                                                  title="Field Data")

#
sensor = sensors_client.sensor_post(sensor_json)
print(sensor.status_code)


# Sensor id represents the sensor of the newly created sensor. The next few lines helps create the corresponding stream for the sensor
sensor_id =
stream_json = streams_client.stream_create_json_from_sensor(sensors_client.sensor_get(sensor_id).json()["sensor"])
stream_details = streams_client.stream_post_json(stream_json)

r = sensors_client.sensor_statistics_post(sensor_id)
print(r.status_code)
