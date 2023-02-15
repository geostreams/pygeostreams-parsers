import copy
import datetime
from urllib.request import HTTPBasicAuthHandler
from pygeostreams.sensors import SensorsApi
from pygeostreams.streams import StreamsApi
from pygeostreams.datapoints import DatapointsApi
from pygeostreams.cache import CacheApi
from pygeostreams.time_transformers import time2utc
import requests
import yaml
import csv
#Huc Finder
from huc_finder.huc import HucFinder
hucfinder = HucFinder('./huc_finder/huc-all.shp')

# get configuration
config = yaml.load(open("config.yml", 'r'), Loader=yaml.FullLoader)

#Mapping for storing Monitoring_ID with Stream_ID
monitor_id_stream_mapping = {}

host = config['host']
username = config['username']
password = config['password']


# Authenticate
sensorclient = SensorsApi(host=host, username=username, password=password)
streamclient = StreamsApi(host=host, username=username, password=password)
datapointclient = DatapointsApi(host=host, username=username, password=password)
cacheclient = CacheApi(host=host, username=username, password=password)


#Creating sensors
def create_sensor_stream(site,coordinates):
    # get or create sensor
    geocode = coordinates
    huc_json = hucfinder.getHuc(lat=geocode[0], lon=geocode[1])
    sensor_json = sensorclient.sensor_create_json(site,
                                                  float(geocode[1]),
                                                  float(geocode[0]),
                                                  0,
                                                  site,
                                                  huc_json['huc4']['code'],
                                                  huc=huc_json,
                                                  network='greon',
                                                  organization_id='greon',
                                                  title="Great Rivers Ecological Observation Network")
    sensor = sensorclient.sensor_post_json(sensor_json)
    latest_datapoint = sensor['max_end_time']
    # create or get stream - need to add _WQ to name of sensor for stream
    # then romove before returning
    sensor_name = copy.copy(sensor['name'])
    sensor['name'] = sensor_name + "_WQ"
    sensor['properties']["name"] = sensor_name + "_WQ"
    stream_json = streamclient.stream_create_json_from_sensor(sensor)
    stream_wq = streamclient.stream_post_json(stream_json) #
    sensor['name'] = sensor_name
    sensor['properties']['name'] = sensor_name
    return sensor, stream_wq, geocode, latest_datapoint

#Reading from station.csv
sensor_data={}
with open('station.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sensor_data[row['MonitoringLocationIdentifier']]=[row['LatitudeMeasure'],row['LongitudeMeasure']]


for site,coordinates in sensor_data.items():   
    sensor, stream_wq, geocode, latest_datapoint = create_sensor_stream(site,coordinates)
    monitor_id_stream_mapping[site] = [stream_wq['id'],sensor]
   
#Reading from resultphyschem.csv
rpc_data = []
with open('resultphyschem.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        #Creating the parameters array
        # Converting date and time to timestamp
        if(row["ActivityStartTime/Time"]==""):
             date_string = row["ActivityStartDate"]+" "+ "12:00:00"
        else:
             date_string = row["ActivityStartDate"]+" "+ row["ActivityStartTime/Time"]
       
        #timestamp = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S").timestamp()
        rpc_data.append([row['MonitoringLocationIdentifier'],date_string,[row['CharacteristicName'],row['ResultMeasureValue']]])
        
#Building dictionary
rpc_data_dict = {}

for parameters in rpc_data:
    if(parameters[0] not in rpc_data_dict):
        rpc_data_dict[parameters[0]]= {parameters[1]:{parameters[2][0]:parameters[2][1]}}
    else:
        get_time = rpc_data_dict[parameters[0]]
        if(parameters[1] not in get_time):
            rpc_data_dict[parameters[0]][parameters[1]]= {parameters[2][0]:parameters[2][1]}
        else:
            get_characteristics = get_time[parameters[1]]
            if(parameters[2][0] not in get_characteristics):
                get_characteristics[parameters[2][0]] = parameters[2][1]


#Pushing datapoints
for key,value in rpc_data_dict.items():
    monitor_id = key
    sensor_info ={'id':'','name':''}
    stream_id = monitor_id_stream_mapping[monitor_id][0]
    sensor_info = monitor_id_stream_mapping[monitor_id][1]
    get_timestamps = value
    for time in get_timestamps:
        #modify
        datapoints_bulk_list=[]
        datapoint_json = {
                        'start_time': time2utc(str(time)),
                        'end_time':  time2utc(str(time)),
                        'type': 'Feature',
                        'geometry': {
                            'type': "Point",
                            'coordinates': [sensor_data[monitor_id][1],
                            sensor_data[monitor_id][0],
                            0
                            ]
                        },
                        'stream_id':stream_id,
                        'sensor_id': sensor_info['id'],
                        'sensor_name': sensor_info['name'],
                        'properties': get_timestamps[time]
                    }
        datapoints_bulk_list.append(datapoint_json)
        response = datapointclient.datapoint_create_bulk(datapoints_bulk_list)

            

