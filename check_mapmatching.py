
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2
os.chdir('D:/ViaSat/Catania')
cwd = os.getcwd()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

##########################################################
### Check mapmatching DB #################################
##########################################################

#### check how many TRIP ID we have ######################

# get all ID terminal of Viasat data
all_VIASAT_TRIP_IDs = pd.read_sql_query(
    ''' SELECT "TRIP_ID" 
        FROM public.mapmatching_2019 ''', conn_HAIG)

# make a list of all unique trips
all_TRIP_IDs = list(all_VIASAT_TRIP_IDs.TRIP_ID.unique())

print(len(all_VIASAT_TRIP_IDs))
print("trip number:", len(all_TRIP_IDs))

## get all terminals (unique number of vehicles)
idterm = list((all_VIASAT_TRIP_IDs.TRIP_ID.str.split('_', expand=True)[0]).unique())
print("vehicle number:", len(idterm))


## reload 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/all_ID_TRACKS_2019.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and matched idterms
all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(idterm))
print(len(all_ID_TRACKS_DIFF))
# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/all_ID_TRACKS_2019_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))

######################################
######################################
######################################
### check the size of a table ########
######################################

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.mapmatching_2019') )''', conn_HAIG)




################
### 2019 #######
################

## create index on the column (u,v) togethers in the table 'mapmatching_2017' ###
cur_HAIG.execute("""
CREATE INDEX UV_idx_2019 ON public.mapmatching_2019(u,v);
""")
conn_HAIG.commit()


## create index on the "TRIP_ID" column
cur_HAIG.execute("""
CREATE index trip_id_match2019_idx on public.mapmatching_2019("TRIP_ID");
""")
conn_HAIG.commit()


## create index on the "idtrace" column
cur_HAIG.execute("""
CREATE index trip_idrace_match2019_idx on public.mapmatching_2019("idtrace");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index mapmatching_2019_timedate_idx on public.mapmatching_2019(timedate);
""")
conn_HAIG.commit()




#### check size of the tables

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('mapmatching_2019') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('mapmatching_2017') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('dataraw') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('routecheck_2017') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('routecheck_2019') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public."OSM_edges"') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.idterm_portata') )''', conn_HAIG)

### check the size of the WHOLE DB "HAIG_Viasat_SA"
pd.read_sql_query('''
SELECT pg_size_pretty( pg_database_size('HAIG_Viasat_CT') )''', conn_HAIG)

