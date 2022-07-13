import os
import requests
import yaml
import json
from datetime import datetime, timedelta
import pandas as pd

from pygeostreams.sensors import SensorsApi
from pygeostreams.streams import StreamsApi
from pygeostreams.datapoints import DatapointsApi


# Read ongoing DAT file, pull moving analysis window and run through analysis, email if bad.
# Parse into datapoints and upload file to Clowder, deleting current aggregate file.
# TODO: Create an annual aggregate file once per year.

# solar radiation cutoff for day/night
rad_threshold = 1
# Maintain a buffer of previous N days of datapoints for QC
moving_window_size = 90


# Return dict of {sensors/streams/datapoints} pygeostreams api clients
def create_api_clients(multi_config):
    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    return {
        "sensors": SensorsApi(host=url, username=user, password=password),
        "streams": StreamsApi(host=url, username=user, password=password),
        "datapoints": DatapointsApi(host=url, username=user, password=password)
    }


def convert_dates(x):
    if '/' in x:
        sensor_date_raw = (
            datetime.strptime(x, '%m/%d/%Y %H:%M:%S %p'))
        sensor_timestamp = timedelta.mktime(sensor_date_raw.timetuple())
        sensor_date = (datetime.utcfromtimestamp(sensor_timestamp)
                       .strftime('%Y-%m-%dT%H:%M:%SZ'))
    else:
        sensor_date = (datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                       .strftime('%Y-%m-%dT%H:%M:%SZ'))
    return sensor_date


# Turn row into a dict using headers as keys
def datapoint_from_row(line, headers):
    vals = line.replace("\n", "").split(",")
    dp = {}
    for i in range(len(headers)):
        try:
            dp[headers[i]] = float(vals[i].replace('"', ''))
        except:
            dp[headers[i]] = vals[i].replace('"', '')
    return dp


# Aggregate up standard deviations etc. from moving window
def calculate_analysis_buffer(analysis_buffer):
    # Restructure the buffer for pandas dataframe
    pandas_input = {}
    for day in analysis_buffer:
        for dp in day:
            dt = datetime.strptime(dp["TIMESTAMP"], '%Y-%m-%d %H:%M:%S')
            pandas_input[dt] = dp
    data = pd.DataFrame.from_dict(pandas_input, orient="index")

    stdev_window = 3

    # 3stdev
    meanvals = data.mean()
    stdvals = data.std()
    flagvals_highlims = meanvals + stdev_window * stdvals
    flagvals_lowlims = meanvals - stdev_window * stdvals

    # 3stdev-daynight
    rad = data['short_dn_Avg']
    night_indices = [i for i in range(len(rad)) if rad[i] < rad_threshold]
    day_indices = [i for i in range(len(rad)) if rad[i] >= rad_threshold]
    data_night = data.iloc[night_indices]
    data_day = data.iloc[day_indices]
    meanvals_day = data_day.mean()
    meanvals_night = data_night.mean()
    stdvals_day = data_day.std()
    stdvals_night = data_night.std()
    flagvals_highlims_day = meanvals_day + stdev_window * stdvals_day
    flagvals_highlims_night = meanvals_night + stdev_window * stdvals_night
    flagvals_lowlims_day = meanvals_day - stdev_window * stdvals_day
    flagvals_lowlims_night = meanvals_night - stdev_window * stdvals_night

    return {
        "3stdev": {
            "flag_high": flagvals_highlims,
            "flag_low": flagvals_lowlims
        },
        "3stdev-daynight": {
            "flag_high": {
                "day": flagvals_highlims_day,
                "night": flagvals_highlims_night,
            },
            "flag_low": {
                "day": flagvals_lowlims_day,
                "night": flagvals_lowlims_night,
            }
        }
    }


def validate_datapoint(datapoint, analysis_window, multi_config):
    # Run checks on data & send alert email if thresholds exceeded
    parameters = multi_config['parameters']
    result = {}

    for p in parameters:
        if p in datapoint.keys() and "qc_method" in parameters[p]:
            x = datapoint[p]
            qc = parameters[p]["qc_method"]

            if qc not in analysis_window.keys():
                print("No QC method found matching " + qc)
                continue
            if qc == "3stdev":
                low_limit = analysis_window[qc]["flag_low"][p]
                high_limit = analysis_window[qc]["flag_high"][p]
            elif qc == "3stdev-daynight":
                cycle = "day" if datapoint["short_dn_Avg"] >= rad_threshold else "night"
                low_limit = analysis_window[qc]["flag_low"][cycle][p]
                high_limit = analysis_window[qc]["flag_high"][cycle][p]

            if x < low_limit:
                result[p] = "FLAGGED - value %s is below threshold %s" % (x, low_limit)
            elif x > high_limit:
                result[p] = "FLAGGED - value %s is above threshold %s" % (x, high_limit)
            else:
                result[p] = "OK"
        else:
            result[p] = "NA"

    return result


def upload_datapoint(data, multi_config):
    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    sensor_client = SensorsApi(host=url, username=user, password=password)
    stream_client = StreamsApi(host=url, username=user, password=password)
    datapoint_client = DatapointsApi(host=url, username=user, password=password)
    sensor_names = multi_config['sensors']
    config = multi_config['config']
    longitude = config['sensor']['geometry']['coordinates'][0]
    latitude = config['sensor']['geometry']['coordinates'][1]
    elevation = config['sensor']['geometry']['coordinates'][2]

    data['TIMESTAMP'] = data['TIMESTAMP'].map(convert_dates)
    data['TIMESTAMP'] = data['TIMESTAMP'].astype(str)
    failed_datapoints = []

    for sensor_name in sensor_names:
        if '&' in sensor_name:
            sensor_name = str(sensor_name.replace('&', '%26'))

        # Get Sensor information for the Sensor Name
        sensor_raw = sensor_client.sensor_get_by_name(sensor_name)
        parse_sensor = sensor_raw.json()["sensors"][0]
        sensor_id = parse_sensor["id"]
        last_datapoint_date = parse_sensor["max_end_time"]
        parameters = parse_sensor["parameters"]
        parameters.append("TIMESTAMP")
        # Get Stream information for the Sensor Name
        stream_raw = stream_client.stream_get_by_name_json(sensor_name)
        parse_stream = stream_raw["streams"][0]
        stream_id = parse_stream['id']
        mask = (data['TIMESTAMP'] > last_datapoint_date)
        df = data.loc[mask]
        df.drop(columns=[col for col in df if col not in parameters], inplace=True)
        df['json'] = df.to_json(orient='records', lines=True).splitlines()
        if len(df.index) == 0:
            continue
        datapoints = []
        for index, value in (df['json']).items():
            value = json.loads(value)
            start_time = value.pop('TIMESTAMP', None)
            drop_keys = []
            for k, v in value.items():
                if v == "NAN" or v == "NaN":
                    drop_keys.append(k)
                    continue
                try:
                    value[k] = float(v)
                except ValueError:
                    drop_keys.append(k)
            for k in drop_keys:
                value.pop(k)
            end_time = start_time
            datapoint = {
                'start_time': start_time,
                'end_time': end_time,
                'type': 'Feature',
                'geometry': {
                    'type': "Point",
                    'coordinates': [
                        longitude,
                        latitude,
                        elevation
                    ]
                },
                "properties": value,
                'stream_id': stream_id,
                'sensor_id': sensor_id,
                'sensor_name': str(sensor_name)
            }
            # Post Datapoint
            # datapoint_post = datapoint_client.datapoint_post(datapoint)
            datapoints.append(datapoint)
            if len(datapoints) == 100:
                datapoint_post = datapoint_client.datapoint_create_bulk(datapoints)
                if datapoint_post.status_code != 200:
                    failed_datapoints.append(datapoints)
                datapoints = []
        if len(datapoints) > 0:
            datapoint_post = datapoint_client.datapoint_create_bulk(datapoints)
            if datapoint_post.status_code != 200:
                failed_datapoints.append(datapoints)
        sensor_client.sensor_statistics_post(sensor_id)
    # print(f"Done with {sensor_name}")
    if len(failed_datapoints) > 0:
        date = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        with open(f"/home/fluxtower/aggregate/gd/failed/{date}.json", 'w') as f:
            json.dump(failed_datapoints, f)


def process_new_data(src, multi_config, last_processed):
    found_last_processed = last_processed is None
    outfile = open("fluxTowerTest.csv", 'w')

    with open(src, "r") as datfile:
        # Store header info
        info = datfile.readline()
        head = datfile.readline()
        headers = head.replace("\n", "").replace('"', '').split(",")
        unit = datfile.readline()
        type = datfile.readline()

        analysis_buffer = []  # list of current_day lists
        analysis_summary = {}  # aggregate data about the analysis buffer window
        current_day = []  # list of datapoints on the date

        # Begin reading datapoints
        line = datfile.readline()
        dp = datapoint_from_row(line, headers)
        current_date = dp["TIMESTAMP"][:10]
        current_day.append(dp)
        print("First timestamp: " + dp["TIMESTAMP"])
        line = datfile.readline()
        first = True

        while line:
            dp = datapoint_from_row(line, headers)

            # Check if a new day has begun with this datapoint & accumulate
            line_date = dp["TIMESTAMP"][:10]
            if line_date != current_date:
                # print("Starting new date: "+line_date+" ("+str(len(current_day))+" records on prev day)")
                analysis_buffer.append(current_day)
                current_day = []
                current_date = line_date

                # Remove oldest day if we over-filled the window, then recalculate aggregates on full window only
                if len(analysis_buffer) > moving_window_size:
                    analysis_buffer.pop(0)
                if len(analysis_buffer) == moving_window_size:
                    print("calculating analysis buffer from " + current_date)
                    analysis_summary = calculate_analysis_buffer(analysis_buffer)

            current_day.append(dp)

            # Check whether datapoint needs QC + upload
            if not found_last_processed:
                if dp["TIMESTAMP"] == last_processed:
                    print("Found resume point: " + dp["TIMESTAMP"])
                    found_last_processed = True
                continue

            # If N days available, execute QC before uploading
            if len(analysis_buffer) == moving_window_size:
                qc_results = validate_datapoint(dp, analysis_summary, multi_config)
                dp["qc_status"] = qc_results

                if first:
                    outfile.write("TIMESTAMP," + ",".join(qc_results.keys()) + "\n")
                first = False
                outfile.write(dp["TIMESTAMP"] + "," + ",".join(qc_results.values()) + "\n")
            else:
                dp["qc_status"] = "not checked (insufficient data for analysis window)"
                outfile.write(dp["TIMESTAMP"] + "," + '"not checked (insufficient data for analysis window)"\n')

            # upload_datapoint(dp, multi_config)
            line = datfile.readline()

        outfile.close()
        print("Done.")


def main():
    # Pull geostreams configuration from config YML
    with open("/home/fluxtower/aggregate/gd/upload.yaml", 'r') as stream:
        try:
            multi_config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    data = pd.read_csv("/home/fluxtower/aggregate/data.csv", skiprows=[0, 2, 3], low_memory=False)
    upload_datapoint(data, multi_config)


if __name__ == "__main__":
    main()
