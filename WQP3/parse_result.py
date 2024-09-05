import os
import requests
import zipfile
import pandas as pd
from geopandas import GeoDataFrame
from geopandas.tools import sjoin
from shapely import wkt

geostreams_api = "http://localhost:9004/api/"  # https://greatlakestogulf.org/geostreams/api/
geostreams_user = "mburnet2@illinois.edu"
geostreams_password = "password"

state_ids = {
    "05": "AR",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "21": "KY",
    "22": "LA",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "39": "OH",
    "47": "TN",
    "55": "WI"
}
targets = [
    "Nitrogen",
    "Phosphorus",
    "Stream flow",
    "pH"
]
gapfills = {
    "Nitrogen": [
        "Nitrate + Nitrite",
        "Kjeldahl nitrogen",
        "Inorganic nitrogen (nitrate and nitrite) ***retired***use Nitrate + Nitrite"
    ]
}

huc_data = GeoDataFrame.from_file('../huc_finder/huc-all.shp')

# Fetch data from WQP
base_url = 'https://www.waterqualitydata.us/data/Result/search?mimeType=csv&zip=yes'
dl_headers = {'Content-Type': 'application/json', 'Accept': 'application/zip'}
for state_id in state_ids:
    # Physical query
    state_abbrev = state_ids[state_id]
    outzip = f"{state_abbrev}_all_1970.zip"
    outfile = outzip.replace("zip", "csv")
    if not os.path.exists(outfile):
        continue

        print(f"Downloading {outfile}")
        cmd = "curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/zip' "
        cmd += f'-d \'{"statecode":["US:{state_id}"],"siteType":["Stream"],"characteristicName":["Nitrogen","Phosphorus","pH","Stream flow"],"startDateLo":"01-01-1970","dataProfile":"resultPhysChem","providers":["NWIS","STORET"]}\''
        cmd += f"'https://www.waterqualitydata.us/data/Result/search?mimeType=csv&zip=yes' --output {outzip}"
        with zipfile.ZipFile(outzip, "r") as zip_ref:
            zip_ref.extractall()
        os.remove(outzip)
        os.rename("resultphyschem.csv", outfile)

# Get a token from geostreams
url = f"{geostreams_api}authenticate"
resp = requests.post(url, json={'identifier': geostreams_user, 'password': geostreams_password},
                     headers={'Content-Type': 'application/json'})
resp.raise_for_status()
token = resp.headers["x-auth-token"]
headers = {'X-Auth-Token': token, 'Content-type': 'application/json'}

all_targets = list(targets)
# for t in gapfills:
#     all_targets += gapfills[t]

def get_or_create_sensor(sensor_id, sensor_json):
    get_url = f"{geostreams_api}sensors?sensor_name={sensor_id}"
    post_url = f"{geostreams_api}sensors"
    response = requests.get(get_url)
    response.raise_for_status()
    results = response.json()['sensors']
    if len(results) > 0:
        print(f"Found existing sensor: {sensor_id}")
        return results[0]
    else:
        print(f"Creating sensor: {sensor_id}")
        sensor = requests.post(post_url, json=sensor_json, headers=headers)
        sensor.raise_for_status()
        # Get newly created sensor
        response = requests.get(get_url)
        response.raise_for_status()
        results = response.json()['sensors']
        return results[0]

def get_or_create_stream(stream_id, stream_json):
    get_url = f"{geostreams_api}streams?stream_name={stream_id}"
    post_url = f"{geostreams_api}streams"
    response = requests.get(get_url)
    response.raise_for_status()
    results = response.json()['streams']
    if len(results) > 0:
        print(f"Found existing stream: {stream_id}")
        return results[0]
    else:
        print(f"Creating stream: {stream_id}")
        stream = requests.post(post_url, json=stream_json, headers=headers)
        stream.raise_for_status()
        # Get newly created stream
        response = requests.get(get_url)
        response.raise_for_status()
        results = response.json()['streams']
        return results[0]

def post_bulk_datapoints(stream_id, datapoints):
    print(f"Posting {len(datapoints)} datapoints to {stream_id}")
    post_url = f"{geostreams_api}datapoints/bulk"
    response = requests.post(post_url, json=datapoints, headers={'Content-type': 'application/json'})
    response.raise_for_status()
    if response.status_code == 200:
        return response.json()

def get_or_create_parameter(parameter_name, parameter_json, categories=[]):
    # Fetch existing parameters
    param_url = f"{geostreams_api}parameters"
    response = requests.get(param_url)
    response.raise_for_status()
    parameters = response.json()['parameters']
    for p in parameters:
        if p['title'] == parameter_name or p['name'] == parameter_name:
            return p

    # Otherwise create it
    post_url = f"{geostreams_api}parameters"
    print(f"Creating parameter: {parameter_name}")
    param = requests.post(post_url, json={"parameter": parameter_json, "categories": categories}, headers=headers)
    param.raise_for_status()

    # Get newly created sensor
    response = requests.get(param_url)
    response.raise_for_status()
    parameters = response.json()['parameters']

    for p in parameters:
        if p['title'] == parameter_name or p['name'] == parameter_name:
            return p
    return None

def getHuc8(lat, lon):
    point = GeoDataFrame(pd.DataFrame({'id': [0]}), crs='epsg:4269',
                             geometry=[wkt.loads('POINT(' + str(lon) + ' ' + str(lat) + ')')])
    try:
        huc = sjoin(point, huc_data, how='inner', predicate='intersects')
        return huc['CAT'][0]
    except ValueError:
        return None


for state_id in state_ids:
    # Prepare observation data
    state_abbrev = state_ids[state_id]
    stations = {}
    alldata = {}
    params = {}

    for trait in ["chem", "phys", "all"]:
        data_file = f"{state_abbrev}_{trait}_1970.csv"
        if not os.path.exists(data_file):
            print(f"{data_file} does not exist")
            continue

        # Load & filter dataframe
        print(f"Scanning {data_file}")
        df = pd.read_csv(data_file, low_memory=False)
        df = df[df['CharacteristicName'].isin(all_targets)]
        # df = df[(df['ResultSampleFractionText'] == 'Total') |
        #          (df['ResultSampleFractionText'].isnull())]
        # df = df[(df['ResultMeasure/MeasureUnitCode'] == 'mg/l') |
        #         (df['ResultMeasure/MeasureUnitCode'] == 'mg/L') |
        #         (df['ResultMeasure/MeasureUnitCode'] == 'ug/L') |
        #         (df['ResultMeasure/MeasureUnitCode'] == 'std units') |
        #         (df['ResultMeasure/MeasureUnitCode'] == 'm3/sec')]

        for i, entry in df.iterrows():
            try:
                value = float(entry["ResultMeasureValue"])
            except:
                value = entry["ResultMeasureValue"]
            if value == '' or value is None or str(value) == 'nan':
                continue

            time = entry["ActivityStartDate"].rstrip()
            station = (entry["MonitoringLocationIdentifier"]
                       .replace("&", "and").replace("#", "-"))
            name = (str(entry["MonitoringLocationName"])
                    .replace("&", "and").replace("#", "-"))
            description = str(entry["SampleCollectionMethod/MethodDescriptionText"])
            organization = entry["OrganizationFormalName"]
            characteristic = entry["CharacteristicName"]
            unit = entry["ResultMeasure/MeasureUnitCode"]
            if str(unit) == 'nan':
                unit = ''
            if unit == 'ug/L':
                # Convert micrograms to milligrams
                value /= 1000
                unit = 'mg/L'
            measure = (f"{characteristic} {unit}".lower()
                       .replace(" ", "-").replace("/", "").replace("*", "").replace("+", ""))

            # Determine location
            latitude = float(entry["ActivityLocation/LatitudeMeasure"])
            longitude = float(entry["ActivityLocation/LongitudeMeasure"])
            if str(latitude) == 'nan' or str(longitude) == 'nan':
                continue
            try:
                huc8 = getHuc8(latitude, longitude)
            except:
                # TODO: Can we salvage these somehow?
                huc8 = "00000000"
                continue

            # if measure not in targets:
            #     for parent in gapfills:
            #         if measure in gapfills[parent]:
            #             measure = f"{parent} (gapfill)"
            #             break
            #     continue

            if station not in stations:
                stations[station] = {
                    "type": "Feature",
                    "name": station,
                    "properties": {
                        "name": station,
                        "region": huc8[:4],
                        "huc": {
                            "huc2": {"code": huc8[:2]},
                            "huc4": {"code": huc8[:4]},
                            "huc6": {"code": huc8[:6]},
                            "huc8": {"code": huc8}
                        },
                        "type": {
                            "id": "wqp",
                            "title": "Water Quality Portal"
                        },
                        "MonitoringLocationIdentifier": station,
                        "MonitoringLocationName": name,
                        "MonitoringLocationDescriptionText": description,
                        "HUC8": huc8,
                        "OrganizationFormalName": organization
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            longitude,
                            latitude
                        ]
                    }
                }
                alldata[station] = {}
            if measure not in alldata[station]:
                alldata[station][measure] = []
            if measure not in params:
                params[measure] = {
                    "characteristic": characteristic,
                    "unit": unit
                }

            # Check if we can add to existing record
            found_existing = False
            # if measure.endswith(" (gapfill)"):
            #     for existing_entry in alldata[station][measure]:
            #         if existing_entry["x"] == time:
            #             found_existing = True
            #             try:
            #                 existing_entry["y"] += value
            #             except TypeError: continue
            if not found_existing:
                alldata[station][measure].append({
                    "x": time,
                    "y": value
                })
    print(f"Done scanning {len(stations)} stations")

    # Create parameters
    found_params = []
    for station in stations:
        for measure in alldata[station]:
            if measure not in found_params:
                found_params.append(measure)
    for meas in found_params:
        get_or_create_parameter(meas, {
                'name': meas,
                'title': params[meas]['characteristic'],
                'unit': params[meas]["unit"],
                'search_view': True,
                'explore_view': True,
                'scale_names': None,
                'scale_colors': None,
            },
            categories=[{"name": "Water Quality", "detail_type": "time"}])

    small_skips = 0
    for station in stations:
        # Skip stations with < 5 years data
        years = []
        for measure in alldata[station]:
            for obs in alldata[station][measure]:
                obs_year = obs["x"].split("-")[0]
                if obs_year not in years:
                    years.append(obs_year)
        if len(set(years)) < 5:
            small_skips += 1
            continue

        # Create station if necessary
        sensor_id = get_or_create_sensor(station, stations[station])['id']
        stations[station]["properties"]["sensor_id"] = sensor_id

        """
        print(f"[{station}] Preparing gap-filled dataset")
        # Generate gap-filled versions where possible
        alldata_gapfill = {}
        for measure in alldata[station]:
            if measure.endswith(" (gapfill)"):
                continue  # These will be referenced by parent measure
            gapfill_measure = f"{measure} (gapfill)"
            alldata_gapfill[measure] = alldata[station][measure]
            if measure in gapfills:
                measure_dates = []
                for entry in alldata[station][measure]:
                    measure_dates.append(entry["x"])
                if gapfill_measure in alldata[station]:
                    for entry in alldata[station][gapfill_measure]:
                        if entry["x"] not in measure_dates:
                            if measure not in alldata_gapfill:
                                alldata_gapfill[measure] = []
                            alldata_gapfill[measure].append(entry)
    
        # Iterate over outputs sorted by time and create datapoints
        for measure in alldata_gapfill:
            observations = sorted(alldata_gapfill[measure], key=lambda x: x["x"])
        """

        properties = stations[station]["properties"]
        for measure in alldata[station]:
            observations = sorted(alldata[station][measure], key=lambda x: x["x"])
            print(f"...{measure}")

            stream_name = f"{station} - {measure}"
            stream_data = {
                "sensor_id": sensor_id,
                "name": stream_name,
                "type": "Feature",
                "geometry": stations[station]['geometry'],
                "properties": properties,
                "parameters": [measure]
            }
            stream = get_or_create_stream(stream_name, stream_data)
            stream_id = stream["id"]
            latest_datapoint = stream["end_time"]
            new_latest = latest_datapoint

            # TODO: If parameters didn't register properly...
            # update streams set params=CONCAT('{', split_part("name",' - ',2), '}')::text[];

            datapoints = []
            for observation in observations:
                if observation["x"] <= new_latest and new_latest != "N/A":
                    continue
                if str(observation["y"]) == "nan": continue
                new_latest = observation["x"]
                datapoints.append({
                    'start_time': observation["x"] + "T00:00:00Z",
                    'end_time': observation["x"] + "T00:00:00Z",
                    'type': 'Feature',
                    'geometry': stations[station]['geometry'],
                    'stream_id': stream_id,
                    'sensor_id': sensor_id,
                    'sensor_name': properties["MonitoringLocationName"],
                    "properties": {
                        measure: observation["y"]
                    }
                })
                if len(datapoints) > 200:
                    post_bulk_datapoints(stream_id, datapoints)
                    datapoints = []
            if len(datapoints) > 0:
                post_bulk_datapoints(stream_id, datapoints)

    print(f"...skipped {small_skips} stations with < 5 years data")

# Update cache/bins for all sensors
url = f"{geostreams_api}sensors"
resp = requests.get(url, headers=headers)
resp.raise_for_status()
sens = resp.json()['sensors']
for s in sens:
    url = f"{geostreams_api}cache?sensor_id={s["id"]}"
    resp = requests.post(url, headers=headers)
    resp.raise_for_status()
