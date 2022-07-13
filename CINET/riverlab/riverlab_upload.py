import os
import io
import yaml
import subprocess
import logging
import sys
import shutil
from time import gmtime, strftime

from pyclowder import files
from pyclowder.connectors import Connector


def email(content: str):
    body_str_encoded_to_byte = content.encode()
    subprocess.run([f"mail", f"-s Riverlab failure", "diegoac2@illinois.edu"], input=body_str_encoded_to_byte)


if __name__ == '__main__':
    """
    This script will:
    1. Open the config file and retrieve the Clowder url, key, dataset id, file id, and key
    2. Delete the current Riverlab file from Clowder
    3. Upload the most recent local data file to Clowder
    4. Replace the file id in the config file
    """
    config_file = sys.argv[1]
    with open(config_file, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    logging.basicConfig(filename='/home/riverlab/upload.log', filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.info("Upload file to Clowder initiated.")

    clow_host = config["clowder_host"]
    key = config["key"]
    file_id = config["file_id"]
    dataset_id = config["dataset_id"]
    data_file = config["data_file"]

    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    new_data_file = data_file.replace("data.csv", "Riverlab Data (%s).csv" % timestamp)
    shutil.copyfile(data_file, new_data_file)

    conn = Connector("", {})

    url = '%sapi/files/%s?key=%s' % (clow_host, file_id, key)
    try:
        res = conn.delete(url, verify=conn.ssl_verify if conn else True)
    except Exception as e:
        logging.error("Could not delete Riverlab's old data file.")
    new_file_id = files.upload_to_dataset(conn, clow_host, key, dataset_id, new_data_file)
    if new_file_id is not None:
        config["file_id"] = new_file_id
        with io.open(config_file, 'w', encoding='utf8') as outfile:
            yaml.dump(config, outfile, default_flow_style=False, allow_unicode=True)
        logging.info(f"Upload file to Clowder completed. New file id: {new_file_id}")
    else:
        logging.error("Could not upload dataset to Clowder.")
        email(f"{config_file} \n Failed to upload Riverlab file. \n Riverlab dataset now does not have a file.")

    os.remove(new_data_file)


    file_id = config["chemistry_file_id"]
    dataset_id = config["dataset_id"]
    data_file = config["chemistry_data_file"]

    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    new_data_file = data_file.replace("chemistry_data.csv", "Riverlab Chemistry Data (%s).csv" % timestamp)
    shutil.copyfile(data_file, new_data_file)

    conn = Connector("", {})

    url = '%sapi/files/%s?key=%s' % (clow_host, file_id, key)
    try:
        res = conn.delete(url, verify=conn.ssl_verify if conn else True)
    except Exception as e:
        logging.error("Could not delete Riverlab's old data file.")
    new_file_id = files.upload_to_dataset(conn, clow_host, key, dataset_id, new_data_file)
    if new_file_id is not None:
        config["file_id"] = new_file_id
        with io.open(config_file, 'w', encoding='utf8') as outfile:
            yaml.dump(config, outfile, default_flow_style=False, allow_unicode=True)
        logging.info(f"Upload file to Clowder completed. New file id: {new_file_id}")
    else:
        logging.error("Could not upload dataset to Clowder.")
        email(f"{config_file} \n Failed to upload Riverlab file. \n Riverlab dataset now does not have a file.")

    os.remove(new_data_file)
