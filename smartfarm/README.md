# SMARTFARM Data Parsers

This is a set of scripts to parse data and upload to sensor points in https://smartfarm.ncsa.illinois.edu. 

### Instructions to use the scripts.

* To use the scripts you will need to have an account in the geostream instance with the necessary privileges. 
  The user account can be created in the page `default_url+/signup`. Currently, master permission can be given by 
  modifying the respective column in the database. The account login details will be needed to run the Geostream APIs
* Ensure that you have clearly specified the sensor_id in the script before running the script

### Script Descriptions

* Scripts in current Directory
  * [bin_sensor.py](bin_sensor.py)
    * The script is used to submit a recalculate request for new data added to the sensor. You need to run this script,
     after uploading data to see the data in the site. 
  * [delete_sensor.py](delete_sensor.py)
    * The script is used to delete a certain sensor datapoint and all corresponding data and streams.
  * [soiltemp_sensor_stream.py](soiltemp_sensor_stream.py)
    * The script is used to create a sensor with a corresponding stream. Take note to specify the JSON describing the sensor metadata.
  * [soiltemp_datapoints.py](soiltemp_datapoints.py)
    * Outdated script to update data to sensor. 


* Scripts in [carl-data](carl-data) Directory. The scripts in directory pertain to parsing the data files compiled by Carl's group 
 in Box
  * [smartflux_reifteck_parser](carl-data/smartflux_reifteck_parser.py)
    * The script is used to parse: the CSV file currently named `smartflux_reifteck_2020_2021.csv`.
    * The parameters it adds to sensors are:
      1) Air Pressure (kPa)
      2) CO2 Flux (µmol+1s-1m-2)
      3) H20 Flux (mmol+1s-1m-2)
      4) Longwave Incoming Down Radiation (W/m^2)
      5) Longwave Outgoing Up Radiation (W/m^2)
      6) Mean Wind Speed (m+1s-1)
      7) Net Radiation (W/m^2)
      8) Photosynthetic Photon Flux Density (µmol/m^2/s)
      9) Shortwave Incoming Down Radiation (W/m^2)
      10) Shortwave Outgoing Up Radiation (W/m^2)
      11) Surface Humidity (%)
      12) Surface Temperature (C)
      13) Surface Temperature (K)


* Scripts in [kaiyu-data](kaiyu-data) Directory. The scripts in directory pertain to parsing the data files compiled by Kaiyu's group 
 in Box
  * [Canopy_Height_parser](kaiyu-data/Canopy_Height_parser.py)
    * The script is used to parse: the CSV file currently named `CanopyHeight.Reifsteck.2020.csv`. It adds the parameter Canopy Height (m)
  * [LeafAreaIndex_parser](kaiyu-data/Leaf_Area_Index_parser.py)
    * The script is used to parse: the CSV file currently named `LeafAreaIndex.Reifsteck.2020.csv`. It adds the parameter Leaf Area Index (m^2)
  * [SIF_parser](kaiyu-data/SIF_parser.py)
    * The script is used to parse: the CSV file currently named `SIF.Reifsteck.2020.csv`. It adds the parameter Solar Induced Fluorescence (mW m^2 nm-1 sr-1)
