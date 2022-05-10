import requests

sensor_id =
login = {
    "identifier": "",
    "password": ""
}

# Get login token
url = "https://smartfarm.ncsa.illinois.edu/geostreams/api/authenticate"
r = requests.post(url, headers={"Content-Encoding": "application/json"}, json=login)
auth_headers = {
    "x-auth-token": r.headers["x-auth-token"],
    "Content-Encoding": "application/json"
}

# Submit recalculate request
url = "https://smartfarm.ncsa.illinois.edu/geostreams/api/cache?sensor_id=%s" % sensor_id
r = requests.post(url, headers=auth_headers)
print(r.status_code)
print(r.text)
