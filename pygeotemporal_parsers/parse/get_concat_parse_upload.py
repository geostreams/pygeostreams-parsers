"""
    * Parse and Upload the Newest Locally-Present Data File *

    This Script will do the following:
        - Get the files for parsing 
        - Concat the new files and create a parsing file
        - Create Sensors and Streams if they do not exist
        - Parse the new Data
        - Update the Sensors Stats
        - Upload newest Aggregate to the correct location

"""

# Python Imports
import os
import yaml
import argparse
from datetime import datetime

# Package Imports
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.datapoints import DatapointsApi
from pyclowder.datasets import get_file_list

# Module Imports
from pygeotemporal_parsers.sensors.create_sensors_and_streams \
    import create_sensors_and_streams
from parse_new_datapoints import parse_data, update_sensors_stats
from get_new_files import filter_files
from concat_files import create_header, concat_files
from parse_and_upload_newest_file import upload_file


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

    multi_config = yaml.load(open(opts.config, 'r'))

    # Variables from the config file
    config = multi_config['config']
    url = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    key = multi_config['inputs']['key']
    local_path = multi_config['inputs']['file_path']
    downloads = local_path + multi_config['inputs']['downloads']
    new_files = multi_config['inputs']['new_files']
    new_file = local_path + multi_config['inputs']['new_files']
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_set_name = (multi_config['inputs']['aggregate'] + str(current_time) + 
                     multi_config['inputs']['aggregate_file_type'])
    concat_file_name = os.path.join(local_path, file_set_name)
    parse_file_name = os.path.join(local_path, multi_config['inputs']['parse'])
    file_type = multi_config['inputs']['file_type']
    verify_header = multi_config['inputs']['verify']
    timestamp = multi_config['inputs']['timestamp']
    parameters = multi_config['parameters']
    sensor_names = multi_config['sensors']
    total_header_rows = int(multi_config['inputs']['headers'])
    sensor_names_create = multi_config['sensorscreate']
    
    debug = False
    header_status = False
    
    # The Clients
    sensor_client = SensorsApi(host=url, username=user, password=password)
    stream_client = StreamsApi(host=url, username=user, password=password)
    datapoint_client = DatapointsApi(host=url, username=user, password=password)

    # GET NEW FILES start #

    # The file that will contain the list of new files
    output_file = open(new_file, 'w')
    # Erase old lines in the file
    output_file.truncate()

    if not os.path.isdir(downloads):
        print "Missing Downloads Directory. "
        return

    source_files = get_file_list(None, url + '/', key, config['dataset_source_id'])

    # Filter out any previously parsed files
    num_new_files = 0
    for source_file in source_files:
        file_status = filter_files(url, key, downloads, source_file)
        if file_status is True:
            output_file.write(source_file['filename'] + '\n')
            num_new_files += 1
    print "num_new_files here is = " + str(num_new_files)
    if num_new_files < 1:
        print "No new files to parse - exiting"
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

        # Now to add the non-header data to the file
        if header_status is not False:
            print "INFO: Concatenating the data"
            concat_files(debug, downloads, header_status, local_path,
                         new_files, file_type, concat_file_name,
                         parse_file_name, total_header_rows)

    else:
        print "ERROR: VALID downloads NOT SUPPLIED"
        print " "

    print ""
    print "DONE with Concatenation"

    # CONCAT FILES end #

    # CREATE SENSORS AND STREAMS start #

    # Create Sensors and Streams if they do not exist
    create_sensors_and_streams(
        sensor_client, stream_client, sensor_names_create, config)

    # CREATE SENSORS AND STREAMS end #

    # PARSE FILES start #

    if os.path.exists(parse_file_name):
        datafile = open(parse_file_name, 'r')
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
    
    print("Parsing Process Complete. ")

    # PARSE FILES end #

    # UPLOAD NEW AGGREGATE FILE start #

    # Upload File
    print("Will upload newest file to Clowder if it is not already present. ")
    file_name_only = os.path.basename(concat_file_name)
    upload_file(
        url, key, config['dataset_upload_id'], concat_file_name, file_name_only)

    # UPLOAD NEW AGGREGATE FILE end #

    print("Processing Complete. ")


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
