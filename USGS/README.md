This is the USGS parser for both glm and gltg project.

To run the parser, change the config_<project>.yml to config.yml

For the sensor name, the glm is like "usgs04024000" while gltg is like "04024000".

Also, for glm, the region of sensor cannot get from source_url, so if you are creating a new sensor, 
please pay attention to properties["region"].