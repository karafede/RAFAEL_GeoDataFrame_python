
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2
os.chdir('D:/ViaSat/Salerno')
cwd = os.getcwd()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_SA()
cur_HAIG = conn_HAIG.cursor()


cur_HAIG.execute("""
ALTER TABLE public.dataraw ALTER COLUMN "id" TYPE bigint USING "id"::bigint
""")
conn_HAIG.commit()


## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "dataraw" add new_id serial PRIMARY KEY
     """)
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_new_id_idx on public.dataraw(new_id);
""")
conn_HAIG.commit()



cur_HAIG.execute("""
ALTER TABLE public.dataraw ALTER COLUMN "new_id" TYPE bigint USING "new_id"::bigint
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2017_temp_id_idx on public.routecheck_2017_temp("new_id");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
ALTER TABLE public.routecheck_2017_temp ALTER COLUMN "idterm" TYPE bigint USING "idterm" ::bigint
""")
conn_HAIG.commit()



cur_HAIG.execute("""
CREATE index routecheck_2017_temp_idterm_idx on public.routecheck_2017_temp("idterm");
""")
conn_HAIG.commit()






## check the "idterm"
all_idterm = pd.read_sql_query('''
                    SELECT idterm 
                    FROM public.obu''', conn_HAIG)
all_ID_TRACKS = list(all_idterm.idterm.unique())
len(all_ID_TRACKS)


## check the "id"
AAA = pd.read_sql_query('''
                    SELECT *
                    /* SELECT id, timedate, idterm */
                    FROM public.dataraw
                    WHERE vehtype = '2'
                    LIMIT 100000''', conn_HAIG)
print(len(AAA))

viasat_data = pd.read_sql_query('''
                        SELECT * 
                        FROM public.dataraw 
                        WHERE idterm = '4416566' ''', conn_HAIG)
viasat_data = viasat_data.sort_values('timedate')

## sort by 'id'
AAA = AAA.sort_values('idrequest')
AAA.head()
AAA.tail()

##############################################
### some operations on the DB ################


# filtering by date
date_filtering = pd.read_sql_query('''
                    SELECT * FROM public.dataraw WHERE timedate BETWEEN '2019-09-01 12:00:00'
                    AND '2019-09-01 13:00:00' ''', conn_HAIG)


# count number of vehicles by ID
counts_idterms = pd.read_sql_query('''
            SELECT idterm, vehtype, 
            count(idterm)
            FROM public.dataraw
            group by idterm, vehtype''', conn_HAIG)
## save data
# counts_idterms.to_csv('counts_idterms.csv')

count_types = pd.read_sql_query('''
            SELECT idterm, vehtype,
            count(*)
            FROM public.dataraw
            group by idterm, vehtype''', conn_HAIG)
count_types = (count_types.groupby(['vehtype']).count())['idterm']


# https://medium.com/@riccardoodone/the-love-hate-relationship-between-select-and-group-by-in-sql-4957b2a70229

# SELECT string_agg(title, ', ') as titles, genre, count(*) FROM films GROUP BY genre;

# idterm_aggregate_type = pd.read_sql_query('''
#             SELECT STRING_AGG(idterm::character varying, ', ') as idterms, vehtype,
#             count(*)
#             FROM public.dataraw
#             group by vehtype''', conn_HAIG)


counts_idterms_by_speed = pd.read_sql_query('''
            SELECT idterm, speed, 
            count(idterm)
            FROM public.dataraw
            group by idterm, speed ''', conn_HAIG)

###################################################################################
###################################################################################

# https://wiki.postgresql.org/wiki/What%27s_new_in_PostgreSQL_9.2#Index-only_scans
## routecheck table
routecheck_2017 = pd.read_sql_query('''
                    SELECT "idterm", "TRIP_ID", timedate
                    FROM public.routecheck_2017''', conn_HAIG)
routecheck_2017 = routecheck_2017.sort_values('timedate')

viasat_data = pd.read_sql_query('''
                        SELECT * 
                        FROM public.routecheck_2019 
                        WHERE "track_ID" = '2400053' ''', conn_HAIG)    ## vehtype = 2 (fleet)
viasat_data = viasat_data.sort_values('timedate')
# cur_HAIG.execute("DROP TABLE IF EXISTS routecheck_2017 CASCADE")
# conn_HAIG.commit()

routecheck_2019 = pd.read_sql_query('''
                    SELECT "track_ID", "TRIP_ID", timedate
                    FROM public.routecheck_2019
                    LIMIT 1000''', conn_HAIG)
routecheck_2019 = pd.read_sql_query('''
                    SELECT *
                    FROM public.routecheck_2019
                    LIMIT 1000''', conn_HAIG)
viasat_data = pd.read_sql_query('''
                        SELECT * 
                        FROM public.routecheck_2019 
                        WHERE "track_ID" = '3549237' ''', conn_HAIG)
viasat_data = viasat_data.sort_values('timedate')


routecheck_2019 = routecheck_2019.sort_values('timedate')
routecheck_2019 = pd.read_sql_query('''
                    SELECT  timedate
                    FROM public.routecheck_2019 ''', conn_HAIG)
min(routecheck_2019.timedate)
max(routecheck_2019.timedate)

'''
# match pattern of .csv files
viasat_filenames = ['VST_ENEA_SA_FCD_2017.csv',    # 83206797 lines
                    'VST_ENEA_SA_FCD_2019.csv',    # 106473285 lines
                    'VST_ENEA_SA_FCD_2019_2.csv']  # 79426790 lines
'''


## transform Geometry from text to LINESTRING
# wkb.loads(gdf_all_EDGES.geom, hex=True)
from shapely import wkb
import geopandas as gpd

def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)


viasat_data['geometry'] = viasat_data.apply(wkb_tranformation, axis=1)
viasat_data.drop(['geom'], axis=1, inplace= True)
viasat_data = gpd.GeoDataFrame(viasat_data)
viasat_data.plot()