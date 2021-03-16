import pygeostreams.scheduler as scheduler

scheduler.scheduler("/home/yanzhao3/ENV-USGS/bin/python /home/clowder/parsers/USGS/usgs.py", 
    "Parse Only New Datapoints for USGS on seagrant-dev", "glm@list.illinois.edu", 3)
