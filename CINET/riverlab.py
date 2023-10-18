import os
import time

import pandas as pd
import requests


def get_latest_datapoint():
    url = "https://app.extralab-system.com:8007/data/Monticello/lastpoint"
    r = requests.get(url)
    print(r.status_code)
    print(r.json())
    return r.json()


def load_append(raw_json):
    fname = "data.csv"
    if os.path.isfile(fname) and os.path.getsize(fname) > 0:
        current_data = pd.read_csv(fname)
        new_data = pd.DataFrame(raw_json, index=[0])
        merged = pd.concat([current_data, new_data])
        merged.drop_duplicates(inplace=True, keep=False, subset=['timedate'])
        merged.to_csv(fname, header=True, index=False, na_rep='NULL')
    else:
        print("*** File does not exist. Creating new one. ***")
        new_data = pd.DataFrame(raw_json, index=[0])
        new_data.to_csv(fname, header=True, index=False, na_rep='NULL')


if __name__ == '__main__':
    # usgs location close by https://waterdata.usgs.gov/monitoring-location/05572000
    (lat, lng) = 40.03086735, -88.58895419

    # TODO use a scheduler instead of while loop with time.sleep()
    # s = sched.scheduler(time.time, time.sleep)
    #
    #
    # def do_something(sc):
    #     print("Doing stuff...")
    #     raw_json = get_latest_datapoint()
    #     append_file(raw_json)
    #     s.enter(60, 1, do_something, (sc,))
    #
    #
    # s.enter(60, 1, do_something, (s,))
    # s.run()
    timedate = ""
    while True:
        print("Checking endpoint")
        raw_json = get_latest_datapoint()
        print("New datetime: " + raw_json["timedate"])
        print("Previous datetime: " + raw_json["timedate"])
        # TODO cast to datetime and compare those
        if raw_json["timedate"] != timedate:
            print("**** New Datapoint Found! ***")
            timedate = raw_json["timedate"]
            load_append(raw_json)
        time.sleep(30)
