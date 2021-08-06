# Riverlab Parser

Queries the Riverlab endpoint `https://app.extralab-system.com:8007/data/Monticello/lastpoint` every 30 seconds and if 
it finds a new datapoint based on timestamp, (`timedate` field) it adds it to the file. It uses `pandas` to load the
file from disk every time and merge in the new row.

`plot.py` includes simple code to plot the data in the CSV file using pandas and matplotlib.

## TODO
- Use a scheduler instead of while time.sleep loop
- Should we start new files after a certain number of rows?
- Post to the geostreams api