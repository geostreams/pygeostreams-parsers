import copy
import pandas as pd
import json

import time
import datetime

from pygeostreams.time_transformers import time2utc
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.sensors import SensorsApi
from pygeostreams.cache import CacheApi

# Change sensor id as needed to update that sensor's datapoints
sensor_id =

datapoints_client = DatapointsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams",
                                  username="", password="")
sensors_client = SensorsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams",
                                  username="", password="")
cache_client = CacheApi(host="https://smartfarm.ncsa.illinois.edu/geostreams",
                                  username="", password="")

df = pd.read_csv('CanopyHeight.Reifsteck.2020.csv')

# define what columns need to be ingested, always keep timestamp
cols = ["Date","Value (m)"]
data = df[df.columns.intersection(cols)]

# map column names to readable variable names
col_dict = {"Date": "timestamp","Value (m)": "canopy-height"}


data.columns = [col_dict.get(x, x) for x in data.columns]


def time_conversion(x):
    return x + " 00:00:00"


data['timestamp'] = data['timestamp'].apply(time_conversion)
print(data.head())

datapoint = {
    'start_time': "",
    'end_time': "",
    'type': 'Feature',
    'geometry': {
        'type': "Point",
        'coordinates': [
            -88.290075,
            40.0062722,
            0
        ]
    },
    'stream_id': sensor_id,
    'sensor_id': sensor_id,
    'sensor_name': "Reifsteck Site",
    'properties': {}
}
#
datapoints = data.to_json(orient='records')

#
dps = json.loads(datapoints)
datapoint_list = []

for d in dps:
    dp = copy.deepcopy(datapoint)
    time = time2utc(d.pop("timestamp"))
    dp["properties"] = d
    dp["start_time"] = dp["end_time"] = time
    r = datapoints_client.datapoint_post(dp)
    print(r.status_code)
    if r.status_code != 200:
        datapoint_list.append(dp)

with open('data.json', 'w') as f:
    json.dump(datapoint_list, f)


r = cache_client.bin(sensor_id)
r = sensors_client.sensor_statistics_post(sensor_id)
