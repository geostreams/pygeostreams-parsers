import copy
import pandas as pd
import json

from datetime import datetime

from pygeostreams.time_transformers import time2utc
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.sensors import SensorsApi
from pygeostreams.cache import CacheApi

# Change sensor id as needed to update that sensor's datapoints
sensor_id = 261

#
datapoints_client = DatapointsApi(host="http://localhost:9001/geostreams",
                       username="mohanar2@illinois.edu", password="gmisCFvi4dQ9KBu")
sensors_client = SensorsApi(host="http://localhost:9001/geostreams",
                       username="mohanar2@illinois.edu", password="gmisCFvi4dQ9KBu")
cache_client = CacheApi(host="http://localhost:9001/geostreams",
                       username="mohanar2@illinois.edu", password="gmisCFvi4dQ9KBu")
df = pd.read_csv('SoilGasFlux.Reifsteck.2020.csv')

# define what columns need to be ingested
cols = ["Sampling date", "CH4 flux (mg/m2/day)"]




data = df[df.columns.intersection(cols)]
# # map column names to readable variable names
col_dict = {"Sampling date": "timestamp", "CH4 flux (mg/m2/day)": "ch4_flux"}

data.columns = [col_dict.get(x, x) for x in data.columns]


# Cleaning Data
# Removing certain outlying data (hardcoded removal)
# data = data[data['timestamp'] != '31-Dec--0001 00:00:00']


def timestamp_conversion(x):
    try:
        return datetime.strptime(x, "%m/%d/%Y").strftime("%Y-%m-%d %H:%M:%S")
    except:
        print("Error with conversion of this value - ", x)


data["timestamp"] = data["timestamp"].apply(timestamp_conversion)
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

datapoints = data.to_json(orient='records')

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