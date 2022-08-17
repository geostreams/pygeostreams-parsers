from sys import argv
import numpy as np
import pandas as pd

'''
Example: python NDVI_data_creator.py  2020-07-01 2020-07-31
'''

start_date = pd.to_datetime(argv[1])
end_date = pd.to_datetime(argv[2])
csv0 = pd.read_csv('Irradiance.Reifsteck.2020.csv')
csv1 = pd.read_csv('Radiance.Reifsteck.2020.csv')
Datetime = pd.to_datetime(csv0.Datetime,format='%m/%d/%Y %H:%M')
wavelength = np.array(csv0.columns[1:],np.float32)
mskRed = (wavelength>=650) & (wavelength<=660)
mskNIR = (wavelength>=770) & (wavelength<=780)
Red0 = csv0[csv0.columns[np.insert(mskRed,0,False)]].mean(1)
Red1 = csv1[csv1.columns[np.insert(mskRed,0,False)]].mean(1)
Red = Red1 / Red0 * np.pi
NIR0 = csv0[csv0.columns[np.insert(mskNIR,0,False)]].mean(1)
NIR1 = csv1[csv1.columns[np.insert(mskNIR,0,False)]].mean(1)
NIR = NIR1 / NIR0 * np.pi
data = (NIR-Red) / (NIR+Red)
plt.plot(Datetime, data, '.')
ndvi_df = pd.concat([Datetime, data.rename("NDVI")], axis=1)
print(ndvi_df.head())
ndvi_df.to_csv('NDVI.Reifsteck.2020.csv', index=False)
