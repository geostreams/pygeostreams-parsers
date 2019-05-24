"""
    * Create New Daily File *

    This Script will do the following:
        - Create a new Daily File with aggregated Year-to-Date data
        - Upload Yesterday's Daily File to the correct Clowder Dataset location
        - Move Yesterday's Daily File to the correct local folder

    It is intended for this script to be utilized once per day.

"""

# Python Imports
import os
import yaml
import argparse
import shutil
import requests
from datetime import datetime, timedelta


def main():
    """Main Function"""

    # Required command line argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/multi_config.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    # Config File
    multi_config = yaml.load(open(opts.config, 'r'))

    # Variables from the config file
    #   Local Path Information
    local_path = multi_config['inputs']['file_path']
    #   Create Concat File Name as AGGREGATE-YYYY-MM-DD
    current_datetime = datetime.now()
    current_date = current_datetime.strftime("%Y-%m-%d")
    file_set_name = (multi_config['inputs']['aggregate'] +
                     str(current_date) +
                     multi_config['inputs']['aggregate_file_type'])
    concat_file_name = os.path.join(local_path, file_set_name)
    #   Yesterday's Aggregate File
    previous_datetime = datetime.now() - timedelta(1)
    previous_date = datetime.strftime(previous_datetime, '%Y-%m-%d')
    previous_set_name = (multi_config['inputs']['aggregate'] +
                         str(previous_date) +
                         multi_config['inputs']['aggregate_file_type'])
    previous_file_name = os.path.join(local_path, previous_set_name)
    #   Aggregate Header File
    header_set_name = (multi_config['inputs']['aggregate_header'] +
                       multi_config['inputs']['aggregate_file_type'])
    header_file_name = os.path.join(local_path, header_set_name)
    #   Aggregate Folder Path
    aggregate_path = local_path + multi_config['inputs']['aggregate_path']
    aggregate_path_name = os.path.join(aggregate_path,
                                       os.path.basename(previous_file_name))
    #   For Clowder Uploads
    location = multi_config['inputs']['location']
    dataset_id = multi_config['inputs']['dataset_upload_id']
    key = multi_config['inputs']['key']

    # Check File Statuses
    previous_exists = os.path.isfile(previous_file_name)
    current_exists = os.path.isfile(concat_file_name)
    if not current_exists:
        if previous_exists and (
                current_datetime.year == previous_datetime.year):
            print ('Yesterday File exists. '
                   'Will create update Annual File. ')
            shutil.copy(previous_file_name, concat_file_name)

            print('Uploading File to Clowder. ')
            r = requests.Session()
            response = r.post("%s/api/uploadToDataset/%s?key=%s" %
                              (location, dataset_id, key),
                              files={'File': open(previous_file_name, 'rb')}
                              )
            if response.status_code != 200:
                print('Problem uploading file  : [%d] - %s)' %
                      (response.status_code, response.text))
                return None

            print ('Moving previous Annual File. ')
            shutil.move(previous_file_name, aggregate_path_name)
        else:
            print ('Yesterday File does not exist for this Year. '
                   'Creating empty Annual file with Header Only. ')
            shutil.copy(header_file_name, concat_file_name)
    else:
        print ('Current Annual file already created. ')

    print('Processing Complete. ')


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print('ERROR')
