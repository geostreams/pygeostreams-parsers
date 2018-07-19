# PYGEOTEMPORAL PARSERS SCRIPTS

This is a set of scripts for Pygeotemporal Parsers.

## Table of Contents

* [General Information About the Scripts](#general_info)
    * [Script Descriptions](#descriptions)
    * [File Locations](#file_locations)
* [Setting Up for Usage](#setup)
* [The YAML Files](#yaml_files)
    * [About the `multi_config.yml` YAML File](#multi_yaml)
    * [About the `combo_multi_config.yml` YAML File](#combo_multi_yaml)
    * [About the `combo_multi_config_params.yml` YAML File](#combo_multi_params_yaml)
    * [About the `new_file_config.yml` YAML File](#new_file_yaml)
* [Running the Scripts](#run_scripts)

<a name="general_info"></a>
## General Information About the Scripts

<a name="descriptions"></a>
### Script Descriptions

Scripts with their Descriptions:
* `concat_files.py`
    * Concatenate all new files into two files:
        * Aggregate file
            * Current timestamp in the file name
            * Has appropriate Headers
        * File with one Header for parsing
            * Will erase contents first
            * Used by `parse_new_datapoints.py`
    * Will not delete files
* `convert_xls_to_csv.py`
    * Creates a CSV Files from provided XLS Files
    * Will not delete files
* `create_sensors_and_streams.py`
    * Create all defined Sensors and Streams if they do not exist
    * Does not create Datapoints
* `delete_sensors_and_streams.py`
    * Delete all defined Sensors and Streams
    * Also deletes associated Datapoints
* `get_new_files.py`
    * Get all files from Clowder not currently in the downloads directory
    * Will not duplicate files
    * Creates a file with a list of newly downloaded file names
        * Used by `concat-files.py`
        * Will erase the contents of an existing file with this list
* `get_concat_parse_upload.py`
    * Get the Newest Files for parsing
    * Concat the new files and create a parsing file via `concat-files.py`
    * Create Sensors and Streams if they do not exist via `create_sensors_and_streams.py`
    * Parse the new Data via `parse_new_datapoints.py`
    * Update the Sensors Stats via `parse_new_datapoints.py`
    * Upload newest Aggregate to the correct location via `parse_and_upload_newest_file.py`
* `get_convert_concat_parse_upload.py`
    * Get the Newest Files for parsing
    * Convert XLS Files to CSV Files via `convert_xls_to_csv.py`
    * Concat the new files and create a parsing file via `concat-files.py`
    * Create Sensors and Streams if they do not exist via `create_sensors_and_streams.py`
    * Parse the new Data via `parse_new_datapoints.py`
    * Update the Sensors Stats via `parse_new_datapoints.py`
    * Upload newest Aggregate to the correct location via `parse_and_upload_newest_file.py`
* `get_convert_concat_rename_parse_upload.py`
    * Get the Newest Files for parsing
    * Convert XLS Files to CSV Files via `convert_xls_to_csv.py`
    * Concat the new files and create a parsing file via `concat-files.py`
    * Create Sensors and Streams if they do not exist via `create_sensors_and_streams.py`
    * Parse the new Data with New Parameter Names via `parse_new_datapoints_new_params.py`
    * Update the Sensors Stats via `parse_new_datapoints_new_params.py`
    * Upload newest Aggregate to the correct location via `parse_and_upload_newest_file.py`
* `parse_and_upload_newest_file.py`
    * Get the Newest File for parsing and upload
    * Get or Create Sensors and Streams via `create_sensors_and_streams.py`
    * Get or Create a Collection
    * Get or Create a Dataset
    * Upload the Newest File to Clowder if necessary
    * Get or Create a File for parsing the new data
    * Parse the new Data via `parse_new_datapoints.py`
    * Update the Sensors Stats via `parse_new_datapoints.py`
* `parse_new_datapoints.py`
    * Parse all new data from the parsing file
    * Updates Sensor Statistics
* `parse_new_datapoints_new_params.py`
    * Parse all new data from the parsing file
    * Utilize new Parameter Names when parsing
    * Updates Sensor Statistics


<a name="file_locations"></a>
### File Locations

Config Files:
* All files are located in the `config` folder
* `pygeotemporal_parsers/config/combo_multi_config.yml`
* `pygeotemporal_parsers/config/combo_multi_config_params.yml`
* `pygeotemporal_parsers/config/multi_config.yml`
* `pygeotemporal_parsers/config/new_file_config.yml`

Parse Scripts:
* All scripts are located in the `parse` folder
* `pygeotemporal_parsers/parse/concat_files.py`
* `pygeotemporal_parsers/parse/convert_xls_to_csv.py`
* `pygeotemporal_parsers/parse/get_concat_parse_upload.py`
* `pygeotemporal_parsers/parse/get_convert_concat_parse_upload.py`
* `pygeotemporal_parsers/parse/get_convert_concat_rename_parse_upload.py`
* `pygeotemporal_parsers/parse/get_new_files.py`
* `pygeotemporal_parsers/parse/parse_and_upload_newest_file.py`
* `pygeotemporal_parsers/parse/parse_new_datapoints.py`
* `pygeotemporal_parsers/parse/parse_new_datapoints_new_params.py`

Sensors Scripts:
* All scripts are located in the `sensors` folder
* `pygeotemporal_parsers/sensors/create_sensors_and_streams.py`
* `pygeotemporal_parsers/sensors/delete_sensors_and_streams.py`

<a name="setup"></a>
## Setting Up for Usage

File to edit: `config/your_config.yml`
* `multi_config.yml` is provided as an example for these scripts:
    * `create_sensors_and_streams.py`
    * `delete_sensors_and_streams.py`
    * `get_new_files.py`
    * `concat-files.py`
    * `parse_new_datapoints.py`
* `combo_multi_config.yml` is provided as an example for `get_concat_parse_upload.py` and `get_concat_rename_parse_upload.py`
* `combo_multi_config_params.yml` is provided as an example for `get_convert_concat_rename_parse_upload.py`
* `new_file_config.yml` is provided as an example for `parse_and_upload_newest_file.py`

Note:
* Ensure all the directory paths are correct
* Ensure all the directory paths exist

Add this type of data as is necessary:
* Information for a Sensor
* Information for an associated Clowder Dataset
* Appropriate sensor names to create, destroy, and parse
* Mapping of parameters to sensors for what should be parsed from the file(s)

<a name="yaml_files"></a>
## The YAML Files

For each script, the necessary YAML items are listed.
Only the required items need to be included; Extra items are ignored by the scripts.
It is possible to have a single YAML file that works for multiple scripts.
Example YAML files in each section are designed to work for all associated scripts.

<a name="multi_yaml"></a>
### About the `multi_config.yml` YAML File

This section will provide information about all the parts of this style of YAML File.

These are the required YAML items for each of these scripts:

* `create_sensors_and_streams.py` ONLY uses these YAML items:
    * `['inputs']['location']`
    * `['inputs']['key']`
    * `['inputs']['user']`
    * `['inputs']['password']`
    * `['config'][ all subitems ]`
    * `['sensorscreate']`
* `delete_sensors_and_streams.py` ONLY uses these YAML items:
    * `['inputs']['location']`
    * `['inputs']['key']`
    * `['inputs']['user']`
    * `['inputs']['password']`
    * `['sensorsdelete']`
* `get_new_files.py` ONLY uses these YAML items:
    * `['inputs']['location']`
    * `['inputs']['key']`
    * `['inputs']['file_path']`
    * `['inputs']['downloads']`
    * `['inputs']['new_files']`
    * `['config'][ all subitems ]`
* `concat-files.py` ONLY uses these YAML items:
    * `['inputs']['file_path']`
    * `['inputs']['downloads']`
    * `['inputs']['new_files']`
    * `['inputs']['aggregate']`
    * `['inputs']['aggregate_file_type']`
    * `['inputs']['parse']`
    * `['inputs']['headers']`
    * `['inputs']['file_type']`
    * `['inputs']['verify']`
* `parse_new_datapoints.py` ONLY uses these YAML items:
    * `['inputs']['location']`
    * `['inputs']['key']`
    * `['inputs']['user']`
    * `['inputs']['password']`
    * `['inputs']['file_path']`
    * `['inputs']['parse']`
    * `['inputs']['timestamp']`
    * `['config'][ all subitems ]`
    * `['parameters']`
    * `['sensors']`

This is an example YAML file that can be utilized with variable descriptions:

```
inputs:                                         # General Inputs:
  location: "http://localhost:9000"             #   Clowder URL
  key: "clowder_key"                            #   Clowder Key
  user: "clowder_username"                      #   Clowder Username
  password: "clowder_password"                  #   Clowder Password
  file_path: "full-path/pygeotemporal_parsers/" #   Full file path to the pygeotemporal_parsers Directory
  timestamp: "timestamp"                        #   Keyword for the Time Column in the Raw Data files (RDF)
  headers: 4                                    #   Number of Header Rows in the Raw Data Files
  verify: "unique"                              #   Unique item in the Header shared for all associated RDF
  file_type: ".dat"                             #   File type to be parsed (.dat or .csv)
  new_files: "parse/new_files.csv"              #   Path after file_path for list of newly downloaded RDF
  aggregate: "parse/aggregate"                  #   Path after file_path for aggregate of newly downloaded RDF data
  aggregate_file_type: ".csv"                   #   Aggregate file type (.csv, .dat, etc.)
                                                #       Note: File == aggregate + datetime + aggregate_file_type
  parse: "parse/parse.csv"                      #   Path after file_path for the to-be-parsed file
  downloads: "parse/download_files"             #   Path for the location of the downloaded RDF
config:                                         # General Config Items: 
  dataset_source_id: "DatasetID"                #   String ID for the Dataset on Clowder where the RDF exist
  sensor:                                       #   Sensor Information:
      geometry:                                 #       Sensor Geometry
          type: Point                           #           Type of Geometry
          coordinates: [-88.00, 40.00, 0]       #           Coordinates
      properties:                               #       Properties
          region: "Region"                      #           Region
          type:                                 #           Type - Information
              id: "type_id"                     #           Type - ID
              title: "type_title"               #           Type - Title
              location: "location_name"         #           Type - Location Name
              network: ""                       #           Type - Network
          elevation:                            #           Elevation Information
              mean_sea_level: 0                 #           Elevation - Mean Sea Level 
              offset: 0                         #           Elevation - Any Offset
              offset_units: "meters"            #           Elevation - Offset Units
          name: ""                              #           Sensor Name
          popupContent: ""                      #           Sensor Popup Content
          huc: ""                               #           Sensor HUC Number
parameters:                                     # Parameters to Parse (Any Number):
  abc: "Instrument Name 01"                     #   Column_Name: "Sensor Name"
  def: "Instrument Name 02"                     #   Column_Name: "Sensor Name"
sensors:                                        # Sensors to Parse for the RDF (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
sensorscreate:                                  # Sensor Names to Create with Config Sensor Information (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
sensorsdelete:                                  # Sensor Names to Delete with Config Sensor Information (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
```

<a name="combo_multi_yaml"></a>
### About the `combo_multi_config.yml` YAML File

This section will provide information about all the parts of this style of YAML File.

These are the required YAML items for this script:

* `get_concat_parse_upload.py` ONLY uses these YAML items:
    * `['inputs'][ all subitems ]`
    * `['config'][ all subitems ]`
    * `['parameters']`
    * `['sensors']`
    * `['sensorscreate']`

This is an example YAML file that can be utilized with variable descriptions:

```
inputs:                                         # General Inputs:
  location: "http://localhost:9000"             #   Clowder URL
  key: "clowder_key"                            #   Clowder Key
  user: "clowder_username"                      #   Clowder Username
  password: "clowder_password"                  #   Clowder Password
  file_path: "full-path/pygeotemporal_parsers/" #   Full file path to the pygeotemporal_parsers Directory
  timestamp: "timestamp"                        #   Keyword for the Time Column in the Raw Data files (RDF)
  headers: 4                                    #   Number of Header Rows in the Raw Data Files
  verify: "unique"                              #   Unique item in the Header shared for all associated RDF
  file_type: ".dat"                             #   File type to be parsed (.dat or .csv)
  new_files: "parse/new_files.csv"              #   Path after file_path for list of newly downloaded RDF
  aggregate: "parse/aggregate"                  #   Path after file_path for aggregate of newly downloaded RDF data
  aggregate_file_type: ".csv"                   #   Aggregate file type (.csv, .dat, etc.)
                                                #       Note: File == aggregate + datetime + aggregate_file_type
  parse: "parse/parse.csv"                      #   Path after file_path for the to-be-parsed file
  downloads: "parse/download_files"             #   Path for the location of the downloaded RDF
config:                                         # General Config Items:
  dataset_source_id: "DatasetID"                #   String ID for the Dataset on Clowder where the RDF exist
  dataset_upload_id: "DatasetID"                #   String ID for the Dataset on Clowder for the new Aggregate upload
  sensor:                                       #   Sensor Information:
      geometry:                                 #       Sensor Geometry
          type: Point                           #           Type of Geometry
          coordinates: [-88.00, 40.00, 0]       #           Coordinates
      properties:                               #       Properties
          region: "Region"                      #           Region
          type:                                 #           Type - Information
              id: "type_id"                     #           Type - ID
              title: "type_title"               #           Type - Title
              location: "location_name"         #           Type - Location Name
              network: ""                       #           Type - Network
          elevation:                            #           Elevation Information
              mean_sea_level: 0                 #           Elevation - Mean Sea Level
              offset: 0                         #           Elevation - Any Offset
              offset_units: "meters"            #           Elevation - Offset Units
          name: ""                              #           Sensor Name
          popupContent: ""                      #           Sensor Popup Content
          huc: ""                               #           Sensor HUC Number
parameters:                                     # Parameters to Parse (Any Number):
  abc: "Instrument Name 01"                     #   Column_Name: "Sensor Name"
  def: "Instrument Name 02"                     #   Column_Name: "Sensor Name"
sensors:                                        # Sensors to Parse for the RDF (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
sensorscreate:                                  # Sensor Names to Create with Config Sensor Information (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
```

<a name="combo_multi_params_yaml"></a>
### About the `combo_multi_config_params.yml` YAML File

This section will provide information about all the parts of this style of YAML File.

These are the required YAML items for this script:

* `get_convert_concat_rename_parse_upload.py` ONLY uses these YAML items:
    * `['inputs'][ all subitems ]`
    * `['config'][ all subitems ]`
    * `['parameters']`
    * `['parameters_updated']`
    * `['sensors']`
    * `['sensorscreate']`

This is an example YAML file that can be utilized with variable descriptions:

```
inputs:                                         # General Inputs:
  location: "http://localhost:9000"             #   Clowder URL
  key: "clowder_key"                            #   Clowder Key
  user: "clowder_username"                      #   Clowder Username
  password: "clowder_password"                  #   Clowder Password
  file_path: "full-path/pygeotemporal_parsers/" #   Full file path to the pygeotemporal_parsers Directory
  timestamp: "timestamp"                        #   Keyword for the Time Column in the Raw Data files (RDF)
  headers: 4                                    #   Number of Header Rows in the Raw Data Files
  verify: "unique"                              #   Unique item in the Header shared for all associated RDF
  file_type: ".dat"                             #   File type to be parsed (.dat or .csv)
  new_files: "parse/new_files.csv"              #   Path after file_path for list of newly downloaded RDF
  aggregate: "parse/aggregate"                  #   Path after file_path for aggregate of newly downloaded RDF data
  aggregate_file_type: ".csv"                   #   Aggregate file type (.csv, .dat, etc.)
                                                #       Note: File == aggregate + datetime + aggregate_file_type
  parse: "parse/parse.csv"                      #   Path after file_path for the to-be-parsed file
  downloads: "parse/download_files"             #   Path for the location of the downloaded RDF
config:                                         # General Config Items:
  dataset_source_id: "DatasetID"                #   String ID for the Dataset on Clowder where the RDF exist
  dataset_upload_id: "DatasetID"                #   String ID for the Dataset on Clowder for the new Aggregate upload
  sensor:                                       #   Sensor Information:
      geometry:                                 #       Sensor Geometry
          type: Point                           #           Type of Geometry
          coordinates: [-88.00, 40.00, 0]       #           Coordinates
      properties:                               #       Properties
          region: "Region"                      #           Region
          type:                                 #           Type - Information
              id: "type_id"                     #           Type - ID
              title: "type_title"               #           Type - Title
              location: "location_name"         #           Type - Location Name
              network: ""                       #           Type - Network
          elevation:                            #           Elevation Information
              mean_sea_level: 0                 #           Elevation - Mean Sea Level
              offset: 0                         #           Elevation - Any Offset
              offset_units: "meters"            #           Elevation - Offset Units
          name: ""                              #           Sensor Name
          popupContent: ""                      #           Sensor Popup Content
          huc: ""                               #           Sensor HUC Number
parameters:                                     # Parameters to Parse (Any Number):
  abc: "Instrument Name 01"                     #   Column_Name: "Sensor Name"
  def: "Instrument Name 02"                     #   Column_Name: "Sensor Name"
parameters_updated:                             # Parameters as Should Appear when Parsed (Same Number as `parameters`):
  abc: "New-Name-01"                            #   Column_Name: "New-Paremeter_Name"
  def: "New-Name-01"                            #   Column_Name: "New-Paremeter_Name"
  ghi: "New-Name-02"                            #   Column_Name: "New-Paremeter_Name"
  jkl: "New-Name-02"                            #   Column_Name: "New-Paremeter_Name"
sensors:                                        # Sensors to Parse for the RDF (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
sensorscreate:                                  # Sensor Names to Create with Config Sensor Information (Any Number):
  - "Instrument Name 01"                        #   - "Sensor Name"
  - "Instrument Name 02"                        #   - "Sensor Name"
```

<a name="new_file_yaml"></a>
### About the `new_file_config.yml` YAML File

This section will provide information about all the parts of this style of YAML File.

These are the required YAML items for this script:

* `parse_and_upload_newest_file.py` ONLY uses these YAML items:
    * `['inputs'][ all subitems ]`
    * `['config'][ all subitems ]`
    * `['header_timestamp']`
    * `['header_to_parameters']`
    * `['parameters']`
    * `['sensors']`

This is an example YAML file that can be utilized with variable descriptions:

```
inputs:                                         # General Inputs:
  location: "http://localhost:9000"             #   Clowder URL
  key: "clowder_key"                            #   Clowder Key
  user: "clowder_username"                      #   Clowder Username
  password: "clowder_password"                  #   Clowder Password
  file_path: "full-dir-path-to-new-files"       #   Full file path to the Directory with the new file
  timestamp: "start_time"                       #   Keyword for the Time Column in to-be-parsed file
  file_type: ".dat"                             #   File type to be parsed (.dat or .csv)
  parse: "parse/parse.csv"                      #   Path after file_path for the to-be-parsed file
config:                                         # General Config Items:
  sensor:                                       #   Sensor Information:
      geometry:                                 #       Sensor Geometry
          type: Point                           #           Type of Geometry
          coordinates: [-88.00, 40.00, 0]       #           Coordinates
      properties:                               #       Properties
          region: "Region"                      #           Region
          type:                                 #           Type - Information
              id: "type_id"                     #           Type - ID
              title: "type_title"               #           Type - Title
              location: "location_name"         #           Type - Location Name
              network: ""                       #           Type - Network
          elevation:                            #           Elevation Information
              mean_sea_level: 0                 #           Elevation - Mean Sea Level
              offset: 0                         #           Elevation - Any Offset
              offset_units: "meters"            #           Elevation - Offset Units
          name: ""                              #           Sensor Name
          popupContent: ""                      #           Sensor Popup Content
          huc: ""                               #           Sensor HUC Number
header_timestamp: time_column_name              # Keyword for the Time Column in the RDF
header_to_parameters:                           # Mapping the RDF Columns to the to-be-parsed file
  ColumnName01: "parameter01"                   #   Column_Name: "Parameter_Name"
  ColumnName02: "parameter02"                   #   Column_Name: "Parameter_Name"
parameters:                                     # Parameters to Parse (Any Number):
  parameter01: "SensorName"                     #   Column_Name: "Sensor Name"
  parameter02: "SensorName"                     #   Column_Name: "Sensor Name"
sensors:                                        # Sensors to Parse and Get or Create for the RDF (Any Number):
  - "SensorName"                                #   - "Sensor Name"
```

<a name="run_scripts"></a>
## Running the Scripts

All scripts are run using the Command Line with one argument:
* `create_sensors_and_streams.py -c full-path-including/multi_config.yml`
* `delete_sensors_and_streams.py -c full-path-including/multi_config.yml`
* `get_new_files.py -c full-path-including/multi_config.yml`
* `concat-files.py -c full-path-including/multi_config.yml`
* `parse_new_datapoints.py -c full-path-including/multi_config.yml`
* `parse_new_datapoints_new_params.py -c full-path-including/combo_multi_config_params.yml`
* `get_concat_parse_upload.py -c full-path-including/combo_multi_config.yml`
* `get_convert_concat_parse_upload.py -c full-path-including/combo_multi_config.yml`
* `get_convert_concat_rename_parse_upload.py -c full-path-including/combo_multi_config_params.yml`
* `parse_and_upload_newest_file.py -c full-path-including/new_file_config.yml`
