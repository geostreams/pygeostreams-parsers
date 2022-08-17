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
sensor_id = 261

datapoints_client = DatapointsApi(host="", username="",
                            password="")
sensors_client = SensorsApi(host="", username="",
                            password="")
cache_client = CacheApi(host="", username="",
                            password="")

data = pd.read_csv('SoilTemperature.Reifsteck.2020.csv')

# map column names to readable variable names
col_dict = {"TIMESTAMP": "timestamp",
            "TC_5cmP0": "5 cm",
            "TC_10cmP0": "10 cm",
            "TC_20cmP0": "20 cm",
            "TC_30cmP0": "30 cm",
            "TC_40cmP0": "40 cm",
            "TC_50cmP0": "50 cm",
            "TC_60cmP0": "60 cm",
            "TC_75cmP0": "75 cm",
            "TC_100cmP0": "100 cm"}
#
#
data.columns = [col_dict.get(x, x) for x in data.columns]

def time_conversion(x):
    return datetime.datetime.strptime(x,'%m/%d/%y %H:%M').strftime('%Y-%m-%d %H:%M:%S')


data["timestamp"] = data["timestamp"].apply(time_conversion)
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


dps = json.loads(datapoints)
datapoint_list = []

for d in dps:
    dp = copy.deepcopy(datapoint)
    time = time2utc(d.pop("timestamp"))
    d = {"soil_temperature": d}
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
