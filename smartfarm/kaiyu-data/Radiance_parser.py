import copy
import pandas as pd
import json

from datetime import datetime
import time

from pygeostreams.time_transformers import time2utc
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.sensors import SensorsApi
from pygeostreams.cache import CacheApi

from tqdm import tqdm

# Change sensor id as needed to update that sensor's datapoints
sensor_id = 261


datapoints_client = DatapointsApi(host="http://localhost:9000", username="",
                            password="")
sensors_client = SensorsApi(host="http://localhost:9000", username="",
                            password="")
cache_client = CacheApi(host="http://localhost:9000", username="",
                            password="")

data = pd.read_csv('Radiance.Reifsteck.2020.csv')
# data.dropna(subset=['Datetime'], inplace=True)
data = data[data['Datetime'] != 'NaT']

def midday_time_extractor(df):
    df['hr'] = df['Datetime'].apply(lambda x: datetime.strptime(x,'%m/%d/%Y %H:%M').time().hour)
    df['date'] = df['Datetime'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M').date())
    df = df[df['hr'] == 12]
    df=df.groupby(['date','hr']).first().reset_index()
    df=df.drop(['hr','date'],axis=1)
    return df

data = midday_time_extractor(data)
# map column names to readable variable names
col_dict = {"Datetime": "timestamp"}
data.columns = [col_dict.get(x, x) for x in data.columns]

def time_conversion(x):
    return datetime.strptime(x,'%m/%d/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')

print(data.dtypes)
data["timestamp"] = data["timestamp"].apply(time_conversion)


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

for d in tqdm(dps):
    dp = copy.deepcopy(datapoint)
    time = time2utc(d.pop("timestamp"))
    d = {"radiance": d}
    dp["properties"] = d
    dp["start_time"] = dp["end_time"] = time
    r = datapoints_client.datapoint_post(dp)
    # print(r.status_code)
    if r.status_code != 200:
        datapoint_list.append(dp)

with open('data.json', 'w') as f:
    json.dump(datapoint_list, f)


r = cache_client.bin(sensor_id)
r = sensors_client.sensor_statistics_post(sensor_id)