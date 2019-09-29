import pandas as pd
import os
from sqlalchemy import create_engine

print("Reading csv files")

files=[]
for r, d, f in os.walk("./data"):
    for file in f:
        if (not '.zip' in file) and (not '.db' in file):
            files.append(os.path.join(r, file))

df = pd.concat([pd.read_csv(f) for f in files],sort=True)

df['start_time']=pd.to_datetime(df['start_time'])
df['end_time']=pd.to_datetime(df['end_time'])

froms=df[['from_station_id','from_station_name']]
froms.columns=['id','name']
tos=df[['to_station_id','to_station_name']]
tos.columns=['id','name']
all_stations = pd.concat([froms,tos],sort=True).drop_duplicates()

starts = df[['from_station_id','start_time']]
starts['action']=-1
starts.columns = ['id','time','action']
ends = df[['to_station_id','end_time']]
ends['action']=1
ends.columns = ['id','time','action']
availability=pd.concat([starts,ends],sort=True).groupby(['time','id']).sum().reset_index()
availability['date']=[x.date() for x in availability['time']]
availability['time']=[x.time() for x in availability['time']]
availability['isodayofweek']=[x.isoweekday() for x in availability['date']]
availability['hour']=[x.hour for x in availability['time']]
availability['minute']=[x.minute for x in availability['time']]

engine = create_engine("sqlite:///data/divy.db")

df.to_sql('data',engine,if_exists='replace')
all_stations.to_sql('stations',engine,if_exists='replace')
availability.to_sql('availability',engine,if_exists='replace')



