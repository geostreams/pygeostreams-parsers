import os
import json
import requests
import time
from datetime import datetime
import pandas as pd


base_url = "https://zentracloud.com/api/v4/get_readings"
api_token = "Token a3f888af5643afaf3db5c9830987a918b33c6ad8"
device_information = [
    {"label": "CZO_ATF", "device_sn": "06-01888",
        "clowder_dsid": "630fd143e4b0787c2bfe7dc7"},
    {"label": "CZO_FF1", "device_sn": "06-01890",
        "clowder_dsid": "630fbbd9e4b0787c2bfe7bfd"},
    {"label": "CZO_FF2", "device_sn": "06-01896",
        "clowder_dsid": "630fc885e4b0787c2bfe7d13"},
    {"label": "CZO_RB1", "device_sn": "06-01883",
        "clowder_dsid": "630fb9d6e4b0787c2bfe7bb9"},
    {"label": "CZO_RB2", "device_sn": "06-01880",
        "clowder_dsid": "630fc7d4e4b0787c2bfe7cfa"},
    {"label": "SRFP2",   "device_sn": "06-01902",
        "clowder_dsid": "630fbd07e4b0787c2bfe7c13"},
]

def dump_results(measures, readings, output_filename):
    # Order measures by Port#
    measures_order = []
    curr_port = 1
    while curr_port is not None:
        port_empty = True
        for measure in measures:
            if measures[measure]['port_number'] == curr_port:
                measures_order.append(measure)
                port_empty = False
        curr_port = None if port_empty else curr_port + 1

    with open(output_filename, 'w') as out:
        # Write headers first
        h1 = [device['device_sn']]
        h2 = [f'# Records: {len(readings)}']
        h3 = ['Timestamps']
        for measure in measures_order:
            h1.append(f'Port{measures[measure]["port_number"]}')
            h2.append(measures[measure]['sensor_name'])
            h3.append(f'{measures[measure]["units"]} {measure}')
        out.write(','.join(h1)+'\n')
        out.write(','.join(h2)+'\n')
        out.write(','.join(h3)+'\n')
        for timestamp in sorted(readings.keys()):
            reading = readings[timestamp]
            vals = [timestamp]
            for measure in measures_order:
                if measure in reading:
                    vals.append(str(reading[measure]))
                else:
                    vals.append("None")
            out.write(','.join(vals)+'\n')

def append_results(measures, readings, output_filename):
    # TODO: Open local output_filename, append records, upload
    # Order measures by Port#
    measures_order = []
    curr_port = 1
    while curr_port is not None:
        port_empty = True
        for measure in measures:
            if measures[measure]['port_number'] == curr_port:
                measures_order.append(measure)
                port_empty = False
        curr_port = None if port_empty else curr_port + 1

    with open(output_filename, 'a') as out:
        for timestamp in sorted(readings.keys()):
            reading = readings[timestamp]
            vals = [timestamp]
            for measure in measures_order:
                if measure in reading:
                    vals.append(str(reading[measure]))
                else:
                    vals.append("None")
            out.write(','.join(vals)+'\n')

def upload_results(device, filename):
    # Upload file to Clowder dataset
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    key = "2498a45e-5e4d-4ecf-835c-0f6a2d0c3223"
    dataset_id = device["clowder_dsid"]

    print("Uploading %s" % filename)
    files = [('File', open(filename, 'rb'))]
    r = requests.post(f"{clowder_url}datasets/{dataset_id}/files",
                      files=files, headers={'X-API-key': key}, verify=False)
    new_file_id = r.json()['id']

    # Remove any older copies from dataset upon success
    if new_file_id is not None:
        file_list = requests.get(f"{clowder_url}datasets/{dataset_id}/files",
                                 headers={'X-API-key': key, 'Content-type': 'application/json'}, verify=False).json()
        for f in file_list:
            if f['filename'] == filename and f['id'] != new_file_id:
                requests.delete(f"{clowder_url}files/{f['id']}", headers={'X-API-key': key}, verify=False)


curr_time = datetime.now().strftime("%m-%d-%Y %H%M%S")
for device in device_information:
    if device['resume'] == False:
        continue
    print(f"--{device['label']}--")
    output_filename = f"{device['label']}.csv"
    if os.path.isfile(output_filename):
        # Determine last timestamp
        df = pd.read_csv(output_filename, header=2)
        earliest = df['Timestamps'].min()
        latest = df['Timestamps'].max()
        print(f"Found previous results from {earliest} to {latest}")

    # By default, this will start at most recent page of records and walk backwards
    params = {'device_sn': device['device_sn'],
              'per_page': 2000,
              'start_date': latest}
    print(next_url)
    response = requests.get(next_url, params=params,
                            headers={'content-type': 'application/json', 'Authorization': api_token})
    response.raise_for_status()
    content = json.loads(response.content)
    page_end = content['pagination']['page_end_date']
    next_url = content['pagination']['next_url']

    readings = {}
    measures = {}
    for measure in content['data']:
        for measure_entry in content['data'][measure]:
            measures[measure] = measure_entry['metadata']
            for reading in measure_entry['readings']:
                dt = reading['datetime']
                if dt not in readings:
                    readings[dt] = {}
                readings[dt][measure] = reading['value']

    append_results(measures, readings, output_filename)

    while next_url is not None:
        """
        Continue to fetch pages as long as necessary, really only needed when filling big gaps.
        Users are limited to a total of 60 calls per minute, and each device is limited to 1 call per minute. 
        One user can make 60 calls to 60 different devices in a 60 second period.
        """
        if page_end[:10] >= str(datetime.now())[:10]:
            print("All caught up.")
            upload_results(device, output_filename)
            break

        time.sleep(61)
        retries = 0
        print(next_url)
        try:
            response = requests.get(next_url,
                                    headers={'content-type': 'application/json', 'Authorization': api_token})
            response.raise_for_status()
        except Exception as e:
            retries += 1
            if retries > 5:
                print("Aborting calls to this device for now.")
                next_url = None
                continue
            print("Failed to connect, retrying in 60s...")
            continue
        content = json.loads(response.content)
        next_url = content['pagination']['next_url']

        for measure in content['data']:
            for measure_entry in content['data'][measure]:
                measures[measure] = measure_entry['metadata']
                for reading in measure_entry['readings']:
                    dt = reading['datetime']
                    if dt not in readings:
                        readings[dt] = {}
                    readings[dt][measure] = reading['value']

        append_results(measures, readings, output_filename)

    upload_results(device, output_filename)

print("All done.")
