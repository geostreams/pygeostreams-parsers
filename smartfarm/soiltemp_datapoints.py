import copy
import pandas as pd
import json

from pygeostreams.time_transformers import time2utc
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.sensors import SensorsApi
from pygeostreams.cache import CacheApi

datapoints_client = DatapointsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams",
                                  username="", password="")
sensors_client = SensorsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams", username="",
                            password="")
cache_client = CacheApi(host="https://smartfarm.ncsa.illinois.edu/geostreams", username="",
                             password="")

df = pd.read_csv(r'smartflux.csv')
# define what columns need to be ingested
cols = ["timestamp", "TA_1_1_1", "TA_1_1_1_degree", "RH_1_1_1", "air_pressure", "air_pressure_kpa", "RN_1_1_1",
        "LWIN_1_1_1", "LWOUT_1_1_1", "SWIN_1_1_1", "SWOUT_1_1_1", "PPFD_1_1_1"]
data = df[df.columns.intersection(cols)]
# map column names to readable variable names
col_dict = {"TA_1_1_1": "surface_temperature_k",
            "TA_1_1_1_degree": "surface_temperature_c",
            "RH_1_1_1": "surface_humidity",
            "air_pressure": "air_pressure",
            "air_pressure_kpa": "air_pressure_kpa",
            "RN_1_1_1": "net_radiation",
            "LWIN_1_1_1": "longwave_incoming_down_radiation",
            "LWOUT_1_1_1": "longwave_outgoing_up_radiation",
            "SWIN_1_1_1": "shortwave_incoming_down_radiation",
            "SWOUT_1_1_1": "shortwave_outgoing_up_radiation",
            "PPFD_1_1_1": "photosynthetic_photon_flux_density"
            }

data.columns = [col_dict.get(x, x) for x in data.columns]

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
    'stream_id': 254,
    'sensor_id': 254,
    'sensor_name': "Reifsteck",
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


r = cache_client.bin(254)
r = sensors_client.sensor_statistics_post(254)
