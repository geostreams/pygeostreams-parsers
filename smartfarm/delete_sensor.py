from pygeostreams.sensors import SensorsApi

sensor_id =

sensors_client = SensorsApi(host="https://smartfarm.ncsa.illinois.edu/geostreams",
                                  username="", password="")


r = sensors_client.sensor_delete(sensor_id)
print(r.status_code)