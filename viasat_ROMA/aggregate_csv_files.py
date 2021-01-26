
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import codecs
import psycopg2
import db_connect
import datetime
from datetime import datetime
from datetime import date
import numpy as np


os.chdir('D:/ViaSat/VIASAT_RM')
cwd = os.getcwd()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_RM_2019()
cur_HAIG = conn_HAIG.cursor()


## create extension postgis on the database HAIG_Viasat_RM_2019  (only one time)

# cur_HAIG.execute("""
# CREATE EXTENSION postgis
# """)

# cur_HAIG.execute("""
#  CREATE EXTENSION postgis_topology
# """)

# conn_HAIG.commit()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_RM_2019')
connection = engine.connect()

# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS obu CASCADE")
# conn_HAIG.commit()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

######################################################################
## create OBU table ##################################################
######################################################################

static_csv = "VST_ENEA_ROMA_ANAG_20191209.csv"
static_data = pd.read_csv(static_csv, delimiter=';', encoding='latin-1', skiprows=1, header=None)
## assigna header
static_data.columns = ['idterm', 'devicetype', 'idvehcategory', 'brand', 'anno',
                       'portata', 'gender', 'age']
## remove row with empty columns
static_data["anno"] = (static_data["anno"].str.split("/", n = 3, expand = True)[2]).astype(str)
static_data['anno'] = static_data['anno'].astype(str).astype(int)
static_data['portata'] = static_data['portata'].astype('Int64')
static_data['age'] = static_data['age'].astype('Int64')
len(static_data)

#### create "OBU" table in the DB HAIG_Viasat_SA #####
## insert static_data into the DB HAIG_Viasat_SA
static_data.to_sql("obu", con=connection, schema="public", index=False)


##################################################
########## VIASAT dataraw ########################
##################################################

### upload Viasat data for ROMA 2019
viasat_filenames = ['VST_ENEA_ROMA_20191209.csv']

## erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS dataraw CASCADE")
# conn_HAIG.commit()


## loop over all the .csv file with the raw VIASAT data
for csv_file in viasat_filenames:
    print(csv_file)
    reader = csv.reader(codecs.open(csv_file))
    ## get length of the csv file
    lines = len(list(reader))
    print(lines)

    ## lines = 124171551
    # reader = csv.reader(codecs.open(csv_file))
    # for row in reader:
    #    print(row)


    slice = 100000  # slice of data to be insert into the DB during the loop
    ## calculate the neccessary number of iteration to carry out in order to upload all data into the DB
    iter = int(round(lines/slice, ndigits=0)) +1
    for i in range(0, iter):
        try:
            print(i)
            print(i, csv_file)
            # csv_file = viasat_filenames[0]
            # df = pd.read_csv(csv_file, header=None, delimiter=',' ,nrows=slice)
            if i == 0:
                df = pd.read_csv(csv_file, header=None, delimiter=';', skiprows=1, nrows=slice)
                df.columns = ['idrequest', 'idterm', 'timedate', 'latitude', 'longitude',
                              'speed', 'direction', 'grade', 'panel', 'event', 'vehtype',
                              'progressive']
                df['timedate'] = df['timedate'].map(lambda t: t[:-3])
                # df['id'] = pd.Series(range(i * slice, i * slice + slice))
                df['timedate'] = df['timedate'].astype('datetime64[ns]')
                ## upload into the DB
                df.to_sql("dataraw", con=connection, schema="public",
                          if_exists='append', index=False)
            else:
                df = pd.read_csv(csv_file, header=None, delimiter=';', skiprows=i * slice, nrows=slice)
                # df = pd.read_csv(csv_file, header=None ,delimiter=';', skiprows=i*slice ,nrows=slice, encoding='utf-16')
                ## define colum names
                df.columns = ['idrequest', 'idterm', 'timedate', 'latitude', 'longitude',
                              'speed', 'direction', 'grade', 'panel', 'event', 'vehtype',
                              'progressive']
                # df['id'] = pd.Series(range(i * slice, i * slice + slice))
                df['timedate'] = df['timedate'].astype('datetime64[ns]')
                ## upload into the DB
                df.to_sql("dataraw", con=connection, schema="public",
                                              if_exists='append', index = False)
                with open("last_file.txt", "w") as text_file:
                    text_file.write("last csv_file ID: %s" % (csv_file))
        except pd.errors.EmptyDataError:
            pass



###########################################################
### ADD a SEQUENTIAL ID to the dataraw table ##############
###########################################################

## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "dataraw" add id serial PRIMARY KEY
     """)
conn_HAIG.commit()

#### add an index to the "idterm"

cur_HAIG.execute("""
CREATE index dataraw_idterm_idx on public.dataraw(idterm);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_timedate_idx on public.dataraw(timedate);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_vehtype_idx on public.dataraw(vehtype);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_id_idx on public.dataraw("id");
""")
conn_HAIG.commit()


###########################################################
##### Check size DB and tables ############################

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.dataraw') )''', conn_HAIG)


###################################################################
###################################################################
###################################################################
#### create table with 'idterm', 'vehtype' and 'portata' ##########

idterm_vehtype_portata = pd.read_sql_query('''
                       WITH ids AS (SELECT idterm, vehtype
                                    FROM
                               dataraw)
                           select ids.idterm,
                                  ids.vehtype,
                                  obu.portata
                        FROM ids
                        LEFT JOIN obu ON ids.idterm = obu.idterm
                        ''', conn_HAIG)

## drop duplicates ###
idterm_vehtype_portata.drop_duplicates(['idterm'], inplace=True)
idterm_vehtype_portata.to_csv('D:/ENEA_CAS_WORK/ROMA_2019/idterm_vehtype_portata.csv')
## relaod .csv file
idterm_vehtype_portata = pd.read_csv('D:/ENEA_CAS_WORK/ROMA_2019/idterm_vehtype_portata.csv')
idterm_vehtype_portata = idterm_vehtype_portata[['idterm', 'vehtype', 'portata']]
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_RM_2019')
connection = engine.connect()
## populate DB
idterm_vehtype_portata.to_sql("idterm_portata", con=connection, schema="public",
          if_exists='append', index=False)



#################################################################################
#################################################################################
##################################################################################

### get unique list of dates in the DB (dataraw)

# make a list of unique dates (only dates not times!)
# select an unique table of dates postgresql
unique_DATES = pd.read_sql_query(
    '''SELECT DISTINCT all_dates.dates
        FROM ( SELECT dates.d AS dates
               FROM generate_series(
               (SELECT MIN(timedate) FROM public.dataraw),
               (SELECT MAX(timedate) FROM public.dataraw),
              '1 day'::interval) AS dates(d)
        ) AS all_dates
        INNER JOIN public.dataraw
	    ON all_dates.dates BETWEEN public.dataraw.timedate AND public.dataraw.timedate
        ORDER BY all_dates.dates ''', conn_HAIG)

# ADD a new field with only date (no time)
unique_DATES['just_date'] = unique_DATES['dates'].dt.date

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)


#### get all dates

from datetime import datetime
now1 = datetime.now()

all_DATES = pd.read_sql_query(
            ''' select
                date_trunc('day', timedate), 
                count(1)
                FROM public.dataraw
                group by 1
                /*limit 1000*/''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)

# ony get the "date" field ("%Y-%m-%d")
all_DATES['date'] = all_DATES['date_trunc'].apply(lambda x: x.strftime("%Y-%m-%d"))
# sort dte from old to most recent
all_DATES = all_DATES.sort_values('date')



#### get all idterms

from datetime import datetime
now1 = datetime.now()

all_idterms = pd.read_sql_query(
            ''' select idterm, vehtype, count(*)
                FROM public.dataraw
                WHERE dataraw.vehtype::bigint = 2
                group by idterm, vehtype
                /*limit 1000*/  ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)

all_idterms.drop_duplicates(['idterm'], inplace=True)

################################################################
################################################################
all_idterms.head()

# 4312817

### get data for a specific date

viasat_data = pd.read_sql_query(
            ''' select *
                FROM public.dataraw
                WHERE idterm = '4080125'
                /*limit 1000*/  ''', conn_HAIG)
viasat_data['timedate'] = viasat_data['timedate'].apply(lambda t: t.replace(second=0))

viasat_data['date'] = viasat_data['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
# sort dte from old to most recent
# viasat_data = viasat_data.sort_values('timedate')
# viasat_data.drop_duplicates(['date'], inplace=True)
viasat_data.to_csv('D:/ENEA_CAS_WORK/ROMA_2019/viasat_data_4080125_NOT_ordered.csv')



'''

## add geometry WGS84 4286
## add geometry WGS84 4286 (Salerno, Italy)
cur_HAIG.execute("""
alter table dataraw add column geom geometry(POINT,4326)
""")

cur_HAIG.execute("""
update dataraw set geom = st_setsrid(st_point(longitude,latitude),4326)
""")
routecheck_2017

conn_HAIG.commit()

'''


