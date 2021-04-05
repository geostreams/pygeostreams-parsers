"""
    Convert XLS Files to CSV Files
"""


import os
import argparse
import yaml
import xlrd
import string


def main():
    """
        MAIN Function
    """

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

    # Main Directory
    main_dir = multi_config['inputs']['file_path']

    # Directory with the files to be concatenated
    data_dir = os.path.join(main_dir, multi_config['inputs']['downloads'])

    # CSV with the list of files to be concatenated
    new_files = multi_config['inputs']['new_files']

    if not os.path.isfile(os.path.join(main_dir, new_files)):
        print "ERROR: VALID new_files NOT SUPPLIED - EXITING"
        quit()

    if os.path.isdir(data_dir):
        # Create CSV Files from the XLS Files
        convert_files(main_dir, new_files, data_dir)

    else:
        print "ERROR: VALID data_dir NOT SUPPLIED"
        print " "

    print ""
    print "DONE with Conversions"


def convert_files(main_dir, new_files, data_dir):
    """
        Convert each File
    """

    # The file that contains the list of new files to convert
    new_file_name = open(os.path.join(main_dir, new_files), 'rb')
    new_file_list = new_file_name.read()
    new_file_name.close()

    # locate the data files
    datafiles = [datafile for datafile in os.listdir(data_dir) if
                 os.path.isfile(os.path.join(data_dir, datafile)) and
                 datafile.endswith('.xls') and datafile in new_file_list]

    # The file that contains the list of new files to be updated
    new_files_updated = open(os.path.join(main_dir, new_files), 'w')
    # Erase old lines in the file
    new_files_updated.truncate()

    # step through one file at a time
    for datafile in datafiles:
        xls_file = xlrd.open_workbook(os.path.join(data_dir, datafile))
        first_sheet = xls_file.sheet_by_index(0)

        # Create the new file name
        new_file_name = os.path.splitext(datafile)[0] + '.csv'

        # Add the new file name to the file
        new_files_updated.write(new_file_name + '\n')

        # Update the name to include the full path
        new_file_name = os.path.join(data_dir, new_file_name)

        # Open the new CSV file for writing
        new_file_csv = open(new_file_name, 'w')

        # Will use to determine file location
        counter = 1

        # Write the data to the new CSV file
        for row in range(first_sheet.nrows):
            row_value = ''
            row_values = first_sheet.row_values(row)

            # Check the format of some values
            for value in row_values:
                # If is time (first item in the row)
                if (value == row_values[0]) and (isinstance(value, float)):
                    value = xlrd.xldate_as_datetime(value, xls_file.datemode)
                # If the value contains unicode
                if isinstance(value, unicode):
                    value = value.encode('utf8')
                # Add to row value
                row_value = str(row_value) + str(value).lstrip() + ','

            # Remove last comma
            row_value = row_value[:-1]

            # Ensure all characters are the proper type
            no_special_chars = ''.join(
                filter(lambda x: x in string.printable, row_value))
            no_special_chars = no_special_chars.replace(', ', ',')

            # Write to the CSV file
            new_file_csv.write(str(no_special_chars))
            if counter < len(range(first_sheet.nrows)):
                new_file_csv.write("\n")

            counter += 1

        # Close the new CSV file
        new_file_csv.close()

    # Ensure the files are closed properly regardless
    new_files_updated.close()

    return


if __name__ == "__main__":
    main()
