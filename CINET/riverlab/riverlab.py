import os

import pandas as pd
import requests
import yaml


def get_latest_datapoint(url: str):
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        return None


def load_append(raw_json, fname):
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
    with open("/home/riverlab/cinetclowder.yaml", 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    time_date = ""
    raw_json = get_latest_datapoint(config["riverlab_endpoint"])
    if raw_json is not None and raw_json["timedate"] != time_date: # TODO cast to datetime and compare those
        time_date = raw_json["timedate"]
        load_append(raw_json, "/home/riverlab/data.csv")

    chem_json = get_latest_datapoint(config["chemistry_endpoint"])
    if raw_json is not None and raw_json["timedate"] != time_date: # TODO cast to datetime and compare those
        time_date = raw_json["timedate"]
        load_append(raw_json, "/home/riverlab/chemistry_data.csv")