import shutil
import os
import io
import yaml
import logging
import sys

from shutil import copyfile
from time import gmtime, strftime

from pyclowder import files
from pyclowder.connectors import Connector

"""
Copy DAT file to data.csv, upload to Clowder, try to delete previous
"""

if __name__ == '__main__':
    config_file = sys.argv[1]
    with open(config_file, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    logging.basicConfig(filename=config["log"], filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.info("Upload file to Clowder initiated.")

    dat_file = config["dat_file"]
    csv_file = config["csv_file"]

    # fetch the latest data file from loggernet
    copyfile(dat_file, csv_file)

    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    new_data_file = csv_file.replace("data.csv", "Flux Tower Data (%s).csv" % timestamp)
    shutil.copyfile(csv_file, new_data_file)

    clow_host = config["clowder_host"]
    key = config["key"]
    file_id = config["file_id"]
    dataset_id = config["dataset_id"]

    conn = Connector("", {})

    url = '%sapi/files/%s?key=%s' % (clow_host, file_id, key)
    try:
        res = conn.delete(url, verify=conn.ssl_verify if conn else True)
    except:
        pass

    #if res.status_code == 200:
    if True:
        new_file_id = files.upload_to_dataset(conn, clow_host, key, dataset_id, new_data_file)
        if new_file_id is not None:
            config["file_id"] = new_file_id
            with io.open(config_file, 'w', encoding='utf8') as outfile:
                yaml.dump(config, outfile, default_flow_style=False, allow_unicode=True)
            logging.info(f"Upload file to Clowder completed. New file id: {new_file_id}")
        else:
            logging.error("Could not upload dataset to Clowder.")
    else:
        logging.error("Could not delete old data file.")

    os.remove(new_data_file)
