import os
import requests
import pandas as pd

# Parsing a Result.csv file downloaded from:
base_url = "https://www.waterqualitydata.us/wqx3/Result/search?siteType=Stream&characteristicType=Nutrient&characteristicType=Physical&mimeType=csv&providers=NWIS&providers=STORET"
for state_id in ["17"]:
    state_url = f"{base_url}&statecode=US%3A{state_id}"
data_file = "IL_chem_1970.csv"

targets = [
    "Nitrogen",
    "Phosphorus"
]
gapfills = {
    "Nitrogen": [
        "Nitrate + Nitrite",
        "Kjeldahl nitrogen",
        "Inorganic nitrogen (nitrate and nitrite) ***retired***use Nitrate + Nitrite"
    ]
}
all_targets = list(targets)
for t in gapfills:
    all_targets += gapfills[t]


# Load & filter dataframe
df = pd.read_csv(data_file)
df = df[(df['ResultSampleFractionText'] == 'Total') | (df['ResultSampleFractionText'].str.len() == 0)]
df = df[(df['ActivityMediaName'] == 'Water') | (df['ActivityMediaName'].str.len() == 0)]
df = df[df['ResultMeasure/MeasureUnitCode'] == 'mg/L']
df = df[df['CharacteristicName'].isin(all_targets)]

# Prepare observation data
stations = {}
alldata = {}
for i, entry in df.iterrows():
    time = entry["ActivityStartDate"].rstrip()
    station = entry["MonitoringLocationIdentifier"]
    name = entry["MonitoringLocationName"]
    #type_name = entry["MonitoringLocationTypeName"]
    description = entry["SampleCollectionMethod/MethodDescriptionText"]
    #huc8 = entry["HUCEightDigitCode"]
    organization = entry["OrganizationFormalName"]
    measure = entry["CharacteristicName"]
    try:
        value = float(entry["ResultMeasureValue"])
    except:
        value = entry["ResultMeasureValue"]
    if value == '': continue

    if measure not in targets:
        for parent in gapfills:
            if measure in gapfills[parent]:
                measure = f"{parent} (gapfill)"
                break
        continue
    if station not in stations:
        # TODO: Create sensor
        stations[station] = {
            "type": "Feature",
            "properties": {
                "MonitoringLocationIdentifier": station,
                "MonitoringLocationName": name,
                #"MonitoringLocationTypeName": type_name,
                "MonitoringLocationDescriptionText": description,
                #"HUC8": huc8,
                "OrganizationFormalName": organization
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(entry["ActivityLocation/LongitudeMeasure"]),
                    float(entry["ActivityLocation/LatitudeMeasure"])
                ]
            }
        }
        stations[station]["properties"]["sensor_id"] = "N/A"
        alldata[station] = {}
    if measure not in alldata[station]:
        alldata[station][measure] = []

    # Check if we can add to existing record
    found_existing = False
    if measure.endswith(" (gapfill)"):
        for existing_entry in alldata[station][measure]:
            if existing_entry["x"] == time:
                found_existing = True
                try:
                    existing_entry["y"] += value
                except TypeError: continue
    if not found_existing:
        alldata[station][measure].append({
            "x": time,
            "y": value
        })

# Update station totals
for station in stations:
    years = []
    for measure in alldata[station]:
        for obs in alldata[station][measure]:
            obs_year = obs["x"].split("-")[0]
            if obs_year not in years:
                years.append(obs_year)
    stations[station]["properties"]["years"] = len(set(years))

# Generate gap-filled versions where possible
alldata_gapfill = {}
for station in stations:
    if stations[station]["properties"]["years"] < 5:
        continue

    alldata_gapfill = {}
    for measure in alldata[station]:
        if measure.endswith(" (gapfill)"):
            continue
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
        # TODO: Create stream
        properties = stations[station]["properties"]
        stream_data = {
            "sensor_id": properties["sensor_id"],
            "name": f"{properties["MonitoringLocationName"]} - {measure}",
            #"type": properties["MonitoringLocationTypeName"],
            "geometry": stations[station]['geometry'],
            "properties": properties
        }
        stream_id = "N/A"

        datapoints = []
        observations = sorted(alldata_gapfill[measure], key=lambda x: x["x"])
        for observation in observations:
            # TODO: Create datapoint
            datapoints.append({
                'start_time': observation["x"],
                'end_time': observation["x"],
                'type': 'Feature',
                'geometry': stations[station]['geometry'],
                'stream_id': stream_id,
                'sensor_id': properties["sensor_id"],
                'sensor_name': properties["MonitoringLocationName"],
                "properties": {
                    measure: observation["y"]
                }
            })
        # TODO: Post datapoints in bulk here
        print(datapoints[0])
