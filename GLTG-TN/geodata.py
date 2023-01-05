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
hucfinder = HucFinder('../../../huc_finder/huc-all.shp')
# get configuration
config = yaml.load(open("config.yml", 'r'), Loader=yaml.FullLoader)



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
                                            
    print("---SENSOR JSON---", type(sensor_json))
    sensor = sensorclient.sensor_post_json(sensor_json)
    print("--SENSOR POST--",sensor)
   
    #sensor_id = (requests.post("http://localhost:9000/api/sensors", json=sensor_json ), 1000)
    #res = requests.post("http://localhost:9000/api/sensors", json=sensor_json )
   # print("---SENSOR CODE---", res.status_code)
    #print("---RES BODY--",res.content)
    sensor_from_geostreams = sensorclient.sensor_get(sensor_id['id'], 1000).json()
    
   
    latest_datapoint = sensor['max_end_time']

    # create or get stream - need to add _WQ to name of sensor for stream
    # then romove before returning
    sensor_name = copy.copy(sensor['name'])
    sensor['name'] = sensor_name + "_WQ"
    sensor['properties']["name"] = sensor_name + "_WQ"
    stream_json = streamclient.stream_create_json_from_sensor(sensor)
    stream_wq = streamclient.stream_post_json(stream_json)
    sensor['name'] = sensor_name
    sensor['properties']['name'] = sensor_name
    return sensor, stream_wq, geocode, latest_datapoint

#Reading from station
sensor_data={}
with open('station.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sensor_data[row['MonitoringLocationIdentifier']]=[row['LatitudeMeasure'],row['LongitudeMeasure']]
for site,coordinates in sensor_data.items():
    sensor, stream_wq, geocode, latest_datapoint = create_sensor_stream(site,coordinates)

