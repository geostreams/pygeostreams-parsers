"""
    * Get, Concat, Parse, and Add to Existing Daily File *

    This Script will do the following:
        - Get the files for parsing
        - Concat the new files and create a parsing file
        - Add the Aggregated data to the Existing Daily File
        - Create Sensors and Streams if they do not exist
        - Parse the new Data
        - Update the Sensors Stats

    It is intended for this script to be utilized once per hour each day.
"""

# Python Imports
import os
import yaml
import argparse
import shutil
from datetime import datetime

# Package Imports
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi

# Module Imports
from pygeotemporal_parsers.sensors.create_sensors_and_streams \
    import create_sensors_and_streams
from parse_new_datapoints import parse_data, update_sensors_stats
from concat_files import create_header, concat_files


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
    #   For Clowder Items
    url = multi_config['inputs']['location']
    key = multi_config['inputs']['key']
    #   For Aggregating New Data Files
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_set_name = (multi_config['inputs']['aggregate'] + str(current_time) +
                     multi_config['inputs']['aggregate_file_type'])
    concat_file_name = os.path.join(local_path, file_set_name)
    #   For Parsing
    downloads = local_path + multi_config['inputs']['downloads']
    new_files = multi_config['inputs']['new_files']
    new_file = local_path + multi_config['inputs']['new_files']
    parse_file_name = os.path.join(local_path, multi_config['inputs']['parse'])
    file_type = multi_config['inputs']['file_type']
    verify_files = multi_config['inputs']['file_key']
    timestamp = multi_config['inputs']['timestamp']
    verify_header = multi_config['inputs']['verify']
    total_header_rows = int(multi_config['inputs']['headers'])
    #   Assemble Daily File Name as AGGREGATE-YYYY-MM-DD
    current_datetime = datetime.now()
    current_date = current_datetime.strftime("%Y-%m-%d")
    file_set_name = (multi_config['inputs']['aggregate'] +
                     str(current_date) +
                     multi_config['inputs']['aggregate_file_type'])
    daily_file = os.path.join(local_path, file_set_name)
    #   General Sensor Config
    config = multi_config['config']
    #   For Parsing Items
    parameters = multi_config['parameters']
    sensor_names = multi_config['sensors']
    sensor_names_create = multi_config['sensorscreate']
    #   For Moving Hourly Files
    hourly_files = multi_config['inputs']['hourly_files']

    debug = False
    header_status = False

    # The Clients
    sensor_client = SensorsApi(host=url, key=key)
    stream_client = StreamsApi(host=url, key=key)
    datapoint_client = DatapointsApi(host=url, key=key)

    # GET NEW FILES start #

    # File to contain the list of new files (will create if does not exist)
    output_file = open(new_file, 'w')
    # Erase old lines in the file
    output_file.truncate()

    # Prepare the Parsing File (will create if does not exist)
    parse_file = open(parse_file_name, 'w')
    # Erase old lines in the file
    parse_file.truncate()
    parse_file.close()

    if not os.path.isdir(downloads):
        print "Missing Downloads Directory. "
        return

    source_files = os.listdir(downloads)

    # Add list of files to the proper list file (only one if run daily)
    num_new_files = 0
    for source_file in source_files:
        # Only Concatenate files with multi_config['inputs']['file_key']
        if verify_files in source_file:
            output_file.write(source_file + '\n')
            num_new_files += 1
    print "num_new_files here is = " + str(num_new_files)
    if num_new_files < 1:
        print "No new files to concat - exiting"
        return

    # Ensure the files are closed properly regardless
    output_file.close()

    # GET NEW FILES end #

    # CONCAT FILES start #

    print "INFO: file to write is: " + str(concat_file_name)
    print " "

    if os.path.isdir(downloads):

        # Create the header if it does not exist
        if header_status is False:
            header_status = \
                create_header(local_path, new_files, downloads, file_type,
                              concat_file_name, parse_file_name,
                              verify_header, total_header_rows)

            print "INFO: Header is = "
            for row in header_status:
                print row[:-1]
            print " "

        # Now to add the non-header data to the file and the daily file
        if header_status is not False:
            print "INFO: Concatenating the data"
            # Update Parsing File and Append to Daily File
            concat_files(debug, downloads, header_status, local_path,
                         new_files, file_type, daily_file,
                         parse_file_name, total_header_rows)

        # Next is to move the single files to their storage location
        if header_status is not False:
            move_hourly_files(downloads, local_path, new_files, hourly_files)

    else:
        print "ERROR: VALID downloads NOT SUPPLIED"
        print " "

    print ""
    print "DONE with Concatenation"

    # CONCAT FILES end #

    # CREATE SENSORS AND STREAMS start #

    # Create Sensors and Streams if they do not exist
    if sensor_names_create:
        create_sensors_and_streams(
            sensor_client, stream_client, sensor_names_create, config)

    # CREATE SENSORS AND STREAMS end #

    # PARSE FILES start #

    if os.path.exists(parse_file_name):
        datafile = open(parse_file_name, 'rb')
    else:
        print "Missing File to Parse. "
        return

    # Parse Data
    print("Will parse data. ")
    parse_file = open(parse_file_name, 'r')
    parse_data(timestamp, config, sensor_names, parameters, parse_file,
               sensor_client, stream_client, datapoint_client)

    # Update the sensors
    print("Will update sensor stats. ")
    update_sensors_stats(sensor_client, sensor_names)

    # Ensure the files are closed properly regardless
    parse_file.close()
    datafile.close()

    # Delete Extraneous File from daily parsing
    os.remove(concat_file_name)

    print("Parsing Process Complete. ")

    # PARSE FILES end #

    print("Processing Complete. ")


def move_hourly_files(data_folder, main_dir, new_files, hourly_files):
    """
        Move previously concatenated hourly files to a separate directory
    """

    # Current data directory with the files to move
    data_directory = os.path.join(main_dir, data_folder)
    print "INFO: Directory with new files: " + str(data_directory)

    # Current directory for the new files
    hourly_files_dir = os.path.join(main_dir, hourly_files)
    print "INFO: Directory for the new files: " + str(hourly_files_dir)

    new_files = open(os.path.join(main_dir, new_files), 'rb')
    new_file_list = new_files.read()
    new_files.close()

    print "Files to Move"
    print new_file_list

    # locate the data files
    datafiles = [datafile for datafile in os.listdir(data_directory) if
                 os.path.isfile(os.path.join(data_directory, datafile))
                 and datafile in new_file_list]

    # step through one file at a time
    for datafile in datafiles:
        print ("Moving " + datafile + " to " + hourly_files_dir)
        shutil.move(os.path.join(data_directory, datafile),
                    os.path.join(hourly_files_dir, datafile)
                    )

    print("Moving Hourly Files Complete. ")
    print(" ")


if __name__ == '__main__':

    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
