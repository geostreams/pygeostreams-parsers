import pandas as pd
from matplotlib import pyplot as plt

fname = "data.csv"
fig, axs = plt.subplots(nrows=7, sharex=True)
fig.suptitle('Vertically stacked subplots')

df = pd.read_csv(fname, index_col="timedate", parse_dates=True)
print(df.dtypes)
df.cumsum()
plt.figure(figsize=(10, 15))

df[["Conductivity"]].plot(ax=axs[0], figsize=(10, 10))
df[["O2"]].plot(ax=axs[1])
df[["Pression EF", "Pression Pompe"]].plot(ax=axs[2])
df[["Probe Tank Temperature"]].plot(ax=axs[3])
df[["Pump Flow"]].plot(ax=axs[4])
df[["Turbidity"]].plot(ax=axs[5])
df[["pH"]].plot(ax=axs[6])

plt.show()
