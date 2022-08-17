import copy
import pandas as pd
import json

from datetime import datetime

from pygeostreams.time_transformers import time2utc
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.sensors import SensorsApi
from pygeostreams.cache import CacheApi

from tqdm import tqdm

# Change sensor id as needed to update that sensor's datapoints
sensor_id = 261

#
datapoints_client = DatapointsApi(host="", username="",
                            password="")
sensors_client = SensorsApi(host="", username="",
                            password="")
cache_client = CacheApi(host="", username="",
                            password="")
df = pd.read_csv('NDVI.Reifsteck.2020.csv')

# define what columns need to be ingested
cols = ["Datetime", "NDVI"]




data = df[df.columns.intersection(cols)]
# # map column names to readable variable names
col_dict = {"Datetime": "timestamp", "NDVI": "ndvi"}

data.columns = [col_dict.get(x, x) for x in data.columns]


# Cleaning Data
# Removing certain outlying data (hardcoded removal)
data.dropna(subset=['timestamp'], inplace= True)



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

datapoints = data.to_json(orient='records')

dps = json.loads(datapoints)
datapoint_list = []


for d in tqdm(dps):
    dp = copy.deepcopy(datapoint)
    time = time2utc(d.pop("timestamp"))
    dp["properties"] = d
    dp["start_time"] = dp["end_time"] = time
    r = datapoints_client.datapoint_post(dp)
    if r.status_code != 200:
        print(r.status_code)
    datapoint_list.append(dp)

with open('data.json', 'w') as f:
    json.dump(datapoint_list, f)


r = cache_client.bin(sensor_id)
r = sensors_client.sensor_statistics_post(sensor_id)