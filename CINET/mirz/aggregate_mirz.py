import os
import io
import csv
import json
import urllib3
import requests
import pandas as pd
from time import gmtime, strftime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


if __name__ == '__main__':
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    key = "2498a45e-5e4d-4ecf-835c-0f6a2d0c3223"
    dataset_id = "61571c894f0ca77f5a4badfa"
    target_groups = ["ILAG", "ILPR"]

    # Get list of files
    r = requests.get(f"{clowder_url}datasets/{dataset_id}/files", headers={'X-API-key': key}, verify=False)
    if r.status_code == 200:
        file_list = r.json()
    else:
        print("Error fetching file list: %s" % r)
        exit()

    output_filenames = {}
    for target_group in target_groups:
        # Load standardized column names if possible
        standard_cols = {}
        col_filename = "%s_columns.csv" % target_group
        if os.path.isfile(col_filename):
            print("Using column mappings from %s" % col_filename)
            with open(col_filename, 'r', encoding='utf-8-sig') as sc:
                reader = csv.reader(sc)
                headers = {}
                for row in reader:
                    # For each row, map all columns to the most recent available occurring name
                    if len(headers) == 0:
                        for i in range(len(row)):
                            headers[i] = row[i]
                        continue
                    for col_idx in headers:
                        if not pd.isna(row[col_idx]) and len(row[col_idx]) > 0:
                            latest_name = row[col_idx]
                    for col_idx in headers:
                        standard_cols[row[col_idx]] = latest_name

        with open("%s standardized.json" % target_group, 'w') as sc:
            sc.write(json.dumps(standard_cols, indent=2))

        full_data = None
        full_cols = {
            "column_sets": {},
            "files_by_set": {}
        }
        column_set = 1
        for f in file_list:
            if f['filename'].endswith(".xlsx") and f['filename'].startswith(target_group):
                print("Downloading %s" % f['filename'])
                r = requests.get(f"{clowder_url}files/{f['id']}/blob", headers={'X-API-key': key}, verify=False)
                if r.status_code == 200:
                    # Add data to the pile
                    data = pd.read_excel(io.BytesIO(r.content), header=1, skiprows=[2, 3])

                    # Make note of which columns were found
                    exists = None
                    columns = sorted(data.columns)
                    for i in full_cols["column_sets"]:
                        if full_cols["column_sets"][i] == columns:
                            exists = i
                    if exists is None:
                        full_cols["column_sets"][column_set] = columns
                        full_cols["files_by_set"][column_set] = [f['filename']]
                        column_set += 1
                    else:
                        full_cols["files_by_set"][exists].append(f['filename'])

                    # Rename columns if possible before concatenation
                    data.rename(columns=standard_cols, inplace=True)
                    if full_data is not None:
                        print(full_data.columns)
                        full_data = pd.concat([full_data, data])
                    else:
                        full_data = data
                else:
                    print("Error downloading file: %s" % f['id'])
                    exit()

        timestamp = strftime("%Y-%m-%d %H%M%S", gmtime())
        output_filename = 'MIRZ %s Aggregated (%s).csv' % (target_group, timestamp)
        full_data.to_csv(output_filename)
        print("Generated %s" % output_filename)
        output_filenames[target_group] = output_filename

        with open("MIRZ %s Aggregated Columns.json" % target_group, 'w') as out:
            out.write(json.dumps(full_cols, indent=2))

    exit()


    for group in output_filenames:
        # Upload file to Clowder dataset
        filename = output_filenames[group]
        print("Uploading %s" % filename)
        files = [('File', open(filename, 'rb'))]
        r = requests.post(f"{clowder_url}datasets/{dataset_id}/files",
                      files=files, headers={'X-API-key': key}, verify=False)
        if r.status_code == 200:
            new_file_id = r.json()['id']
        else:
            print("Could not upload file to Clowder.")
            print(r)
            new_file_id = None
        os.remove(filename)

        # Remove any older copies from dataset upon success
        if new_file_id is not None:
            file_list = requests.get(f"{clowder_url}datasets/{dataset_id}/files",
                        headers={'X-API-key': key, 'Content-type': 'application/json'}, verify=False).json()
            for f in file_list:
                if f['filename'].startswith("MIRZ %s Aggregated" % group) and f['id'] != new_file_id:
                    print("Deleting %s" % f['filename'])
                    requests.delete(f"{clowder_url}files/{f['id']}", headers={'X-API-key': key}, verify=False)
