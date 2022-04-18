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

sensor = {
    "name": "Reifsteck footprint",
    "geoType": "Feature",
    "geometry": {
        "type": "Point",
        "coordinates": [
            -88.290075,
            40.0062722,
            0
        ]
    },
    "properties": {
        "name": "Reifsteck footprint",
        "popupContent": "Reifsteck footprint",
        "region": "Reifsteck footprint",
        "type": {
            "id": "field",
            "title": "Field Data"
        }
    },
    "min_start_time": "",
    "max_end_time": "",
    "parameters": [
    ]
}

r = sensors_client.sensor_post(sensor)
stream_json = streams_client.stream_create_json_from_sensor(sensors_client.sensor_get(254).json()["sensor"])
stream_details = streams_client.stream_post_json(stream_json)

r = sensors_client.sensor_statistics_post(254)
print(r.status_code)
