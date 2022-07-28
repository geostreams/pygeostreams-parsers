import json
import requests
import time
from datetime import datetime, timedelta


site = "cisco"
root = "https://decatur.watertechnologies.us/api/sites"

params = [
    "Av_Veloc", "Level", "Discharge", "Wiper",
    "NO3", "NO3N",
    "SQI",
    "RefA", "RefB", "RefC", "RefD"
    # not found
    # "NOx", "NOx -N",
]

hour_ago = datetime.now() - timedelta(hours=1)
hour_ago_unix = int(time.mktime(hour_ago.timetuple())) * 1000

for param in params:
    url = "%s/%s/readings/%s?window-start=%s" % (root, site, param, hour_ago_unix)
    req = requests.get(url)
    if req.status_code == 200:
        data = req.json()
        print(param)
        for d in data:
            ts = datetime.utcfromtimestamp(d['timestamp']/1000)
            print("%s: %s" % (ts.strftime('%Y-%m-%d %H:%M:%S'), d['value']))

