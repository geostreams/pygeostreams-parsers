import os
import yaml
import logging
import shutil
import sys

from pyclowder import files
from pyclowder.connectors import Connector

"""
Read sftpflux source_files, upload to Clowder, move zip files and text (summary) files to /home/fluxtower
"""

if __name__ == '__main__':
    config_dir = sys.argv[1]
    with open(config_dir, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    logging.basicConfig(filename='/home/fluxtower/upload.log', filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.info("Upload file to Clowder initiated.")

    source_files = config["source_files"]
    zip_destination = config["zip_destination"]
    summary_destination = config["summary_destination"]
    clowder = config["clowder"]
    conn = Connector("", {})

    for file in os.listdir(source_files):
        if file.endswith(".zip"):
            zip_file = os.path.join(source_files, file)
            for dest in clowder:
                files.upload_to_dataset(conn, dest["clowder_host"], dest["key"], dest["zip_dataset_id"], zip_file)
            destination_zip_file = os.path.join(zip_destination, file)
            shutil.move(zip_file, destination_zip_file)
        elif file.endswith(".txt"):
            summary_file = os.path.join(source_files, file)
            for dest in clowder:
                files.upload_to_dataset(conn, dest["clowder_host"], dest["key"], dest["summary_dataset_id"], summary_file)
            destination_summary_file = os.path.join(summary_destination, file)
            shutil.move(summary_file, destination_summary_file)

    logging.info("Upload file to Clowder completed.")