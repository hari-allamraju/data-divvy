import pandas as pd
import os

print("Reading csv files")

files=[]
for r, d, f in os.walk("./data"):
    for file in f:
        if not '.zip' in file:
            files.append(os.path.join(r, file))

df = pd.concat([pd.read_csv(f) for f in files],sort=True)

froms=df[['from_station_id','from_station_name']]
froms.columns=['id','name']
tos=df[['to_station_id','to_station_name']]
tos.columns=['id','name']
all_stations = pd.concat([froms,tos],sort=True).drop_duplicates()

print (all_stations)



