import os
import shutil
import requests
from time import gmtime, strftime


if __name__ == '__main__':
    clowder_url = "http://cinet.ncsa.illinois.edu/clowder/api/"
    key = "2498a45e-5e4d-4ecf-835c-0f6a2d0c3223"
    dataset_id = "6319027ee4b0a58e7442ecbe"
    data_file = "fake_data.csv"
    data_pretty_name = "Fake Data File"

    # Rename data file with timestamp appended
    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    pretty_filename = data_file.replace(data_file, f"{data_pretty_name} ({timestamp}).{data_file.split('.')[-1]}")
    shutil.copyfile(data_file, pretty_filename)

    # Upload file to Clowder dataset
    print("Uploading %s" % pretty_filename)
    files = [('File', open(pretty_filename, 'rb'))]
    r = requests.post(f"{clowder_url}datasets/{dataset_id}/files",
                  files=files, headers={'X-API-key': key}, verify=False)
    if r.status_code == 200:
        new_file_id = r.json()['id']
    else:
        print("Could not upload file to Clowder.")
        print(r)
        new_file_id = None
    os.remove(pretty_filename)

    # Remove any older copies from dataset upon success
    if new_file_id is not None:
        file_list = requests.get(f"{clowder_url}datasets/{dataset_id}/files",
                    headers={'X-API-key': key, 'Content-type': 'application/json'}, verify=False).json()
        for f in file_list:
            if f['filename'].startswith(data_pretty_name) and f['id'] != new_file_id:
                print("Deleting %s" % f['filename'])
                requests.delete(f"{clowder_url}files/{f['id']}", headers={'X-API-key': key}, verify=False)
