"""
    Convert a provided JSON Data file to a CSV File,
    and add a new Column with mapped Variable Names
"""

import os
import argparse
import yaml
import pandas as panda


def main():
    """
        MAIN Function
    """

    # Required command line argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/multi_config_json_to_csv.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    # Get the Config File
    multi_config = yaml.load(open(opts.config, 'r'))

    # Main Directory
    main_dir = multi_config['inputs']['file_path']

    # Directory with the File to Convert
    json_file = os.path.join(main_dir, multi_config['inputs']['json_file'])
    if not os.path.isfile(json_file):
        print "ERROR: VALID json_file NOT SUPPLIED - EXITING"
        quit()

    # CSV File to Create
    conv_file = os.path.join(main_dir, multi_config['inputs']['conv_file'])
    if not os.path.isfile(conv_file):
        print "ERROR: VALID conv_file NOT SUPPLIED - EXITING"
        quit()

    # JSON Data ID to Map
    params_id = multi_config['inputs']['params_id']

    # JSON Data ID to Map
    conv_data = multi_config['inputs']['conv_data']

    # Variables Mapping
    variables = multi_config['variables']

    # Create a CSV File from the JSON File using Pandas
    panda.read_json(json_file).to_csv(conv_file)

    # Read the CSV File
    panda_csv = panda.read_csv(conv_file)

    # Setup Pandas DataFrame
    data_frame = panda.DataFrame(panda_csv)

    # Add a Column to the CSV File if the Original Column Exists
    if params_id in data_frame:
        data_frame[conv_data] = \
            data_frame[params_id].replace(variables, regex=True)

    # Write the data to the CSV File
    data_frame.to_csv(conv_file)

    # Done with Conversion
    print "DONE with Conversion"


if __name__ == "__main__":
    main()
