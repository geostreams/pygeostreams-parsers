import requests

sensor_id = 261
parameter = 'ch4_flux'

login = {
    "identifier": "mohanar2@illinois.edu",
    "password": "gmisCFvi4dQ9KBu"
}

# Get login token
url = "http://localhost:9001/geostreams/api/authenticate"
r = requests.post(url, headers={"Content-Encoding": "application/json"}, json=login)
auth_headers = {
    "x-auth-token": r.headers["x-auth-token"],
    "Content-Encoding": "application/json"
}

# Submit recalculate request
url = "http://localhost:9001/geostreams/api/cache?sensor_id=%s&parameter=%s" % (sensor_id, parameter)
print(url)
r = requests.post(url, headers=auth_headers)
print(r.status_code)
print(r.text)
