
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


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')
connection = engine.connect()

# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS obu CASCADE")
# conn_HAIG.commit()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex


###########################
## create OBU table #######
###########################

static_csv = "VEM_ANAS_RAFAELCT.csv"
static_data = pd.read_csv(static_csv, delimiter=',', encoding='latin-1', header=None)
static_data.columns = ['idterm', 'devicetype', 'idvehcategory', 'brand', 'anno',
                       'portata', 'gender', 'age']
static_data['idterm'] = static_data['idterm'].astype('Int64')
static_data['anno'] = static_data['anno'].astype('Int64')
static_data['portata'] = static_data['portata'].astype('Int64')
len(static_data)

#### create "OBU" table in the DB HAIG_Viasat_SA #####
## insert static_data into the DB HAIG_Viasat_SA
static_data.to_sql("obu", con=connection, schema="public", index=False)


##################################################
########## VIASAT dataraw ########################
##################################################

# match pattern of .csv files
viasat_filenames = [# 'VST_ANAS_CT_20190201.csv',    # 30064082 lines
                    'VST_ANAS_CT_20190501.csv',    # 35181813 lines
                    'VST_ANAS_CT_20190801.csv',    # 26527801 lines
                    'VST_ANAS_CT_20191101.csv']    # 30986196 lines

## erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS dataraw CASCADE")
# conn_HAIG.commit()


## loop over all the .csv file with the raw VIASAT data
for csv_file in viasat_filenames:
    # print(csv_file)
# csv_file = viasat_filenames[0]
    file = open(csv_file)
    reader = csv.reader(file)
    ## get length of the csv file
    lines = len(list(reader))
    print(lines)

    slice = 100000  # slice of data to be insert into the DB during the loop
    ## calculate the neccessary number of iteration to carry out in order to upload all data into the DB
    iter = int(round(lines/slice, ndigits=0))   #+1
    for i in range(0, iter):
        print(i)
        print(i, csv_file)
        # csv_file = viasat_filenames[0]
        df = pd.read_csv(csv_file, header=None ,delimiter=';', skiprows=i*slice ,nrows=slice)
        ## define colum names
        df.columns = ['idrequest', 'idterm', 'timedate', 'latitude', 'longitude',
                      'speed', 'direction', 'grade', 'panel', 'event', 'vehtype',
                      'progressive']
        # df['id'] = pd.Series(range(i * slice, i * slice + slice))
        df['timedate'] = df['timedate'].astype('datetime64[ns]')
        ## upload into the DB
        df.to_sql("dataraw", con=connection, schema="public",
                                       if_exists='append', index = False)


###########################################################
##### Check size DB and tables ############################

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.dataraw') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.routecheck_2019') )''', conn_HAIG)

### check the size of the WHOLE DB "HAIG_Viasat_SA"
pd.read_sql_query('''
SELECT pg_size_pretty( pg_database_size('HAIG_Viasat_CT') )''', conn_HAIG)


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
idterm_vehtype_portata.to_csv('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/idterm_vehtype_portata.csv')
## relaod .csv file
idterm_vehtype_portata = pd.read_csv('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/idterm_vehtype_portata.csv')
idterm_vehtype_portata = idterm_vehtype_portata[['idterm', 'vehtype', 'portata']]
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')
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


################################################################
################################################################

### further operations on routecheck and mapmatching_2019 #####

### create index for "timedate'
cur_HAIG.execute("""
CREATE index routecheck_2019_timedate_idx on public.routecheck_2019(timedate);
""")
conn_HAIG.commit()


### create index for 'idterm'
cur_HAIG.execute("""
CREATE index routecheck_2019_idterm_idx on public.routecheck_2019(idterm);
""")
conn_HAIG.commit()


### create index for 'longitude'
cur_HAIG.execute("""
CREATE index routecheck_2019_lon_idx on public.routecheck_2019(longitude);
""")
conn_HAIG.commit()


### create index for 'latitutde'
cur_HAIG.execute("""
CREATE index routecheck_2019_lat_idx on public.routecheck_2019(latitude);
""")
conn_HAIG.commit()


##############################################################
### get unique list of dates in the DB (routecheck_2019) #####


BBB = pd.read_sql_query(
    '''SELECT timedate
        FROM public.routecheck_2019
        WHERE date(timedate) = '2019-02-08' ''', conn_HAIG)

CCC = pd.read_sql_query(
    '''SELECT *
        FROM public.routecheck_2019
        WHERE idterm = '2400048' ''', conn_HAIG)


# make a list of unique dates (only dates not times!)
# select an unique table of dates postgresql
unique_DATES = pd.read_sql_query(
    '''SELECT DISTINCT all_dates.dates
        FROM ( SELECT dates.d AS dates
               FROM generate_series(
               (SELECT MIN(timedate) FROM public.routecheck_2019),
               (SELECT MAX(timedate) FROM public.routecheck_2019),
              '1 day'::interval) AS dates(d)
        ) AS all_dates
        INNER JOIN public.routecheck_2019
	    ON all_dates.dates BETWEEN public.routecheck_2019.timedate AND public.routecheck_2019.timedate
        ORDER BY all_dates.dates ''', conn_HAIG)

# ADD a new field with only date (no time)
unique_DATES['just_date'] = unique_DATES['dates'].dt.date

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)
