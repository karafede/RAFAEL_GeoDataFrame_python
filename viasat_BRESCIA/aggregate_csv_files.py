
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import codecs
import psycopg2
os.chdir('D:/ViaSat/Brescia')
cwd = os.getcwd()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_BS()
cur_HAIG = conn_HAIG.cursor()


## create extension postgis on the database HAIG_Viasat_SA  (only one time)

# cur_HAIG.execute("""
# CREATE EXTENSION postgis
# """)

# cur_HAIG.execute("""
#  CREATE EXTENSION postgis_topology
# """)

# conn_HAIG.commit()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_BS')
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

static_csv = "VST_ENEA_BS_DATISTATICI_20201118_new.csv"
static_data = pd.read_csv(static_csv, delimiter=',', encoding='latin-1', header=None)
## There about 1000 linkes with wrong format (This line have been skipped)
static_data = static_data[pd.to_numeric(static_data[4], errors='coerce').notnull()]
## drop last column
del static_data[8]

## assigna header
static_data.columns = ['idterm', 'devicetype', 'idvehcategory', 'brand', 'anno',
                       'portata', 'gender', 'age']
## remove row with empty columns
static_data['anno'] = static_data['anno'].astype(str).astype(int)
static_data['anno'] = static_data['anno'].astype('Int64')
static_data['portata'] = static_data['portata'].astype('Int64')
len(static_data)

#### create "OBU" table in the DB HAIG_Viasat_SA #####
## insert static_data into the DB HAIG_Viasat_SA
static_data.to_sql("obu", con=connection, schema="public", index=False)


##################################################
########## VIASAT dataraw ########################
##################################################

### upload Viasat data for the month of November 2019
extension = 'csv'
os.chdir('D:/ViaSat/Brescia/marzo_2019')
viasat_filenames = glob.glob('*.{}'.format(extension))

## erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS dataraw CASCADE")
# conn_HAIG.commit()

# del viasat_filenames[0]
# del viasat_filenames[1]
# del viasat_filenames[2]
# del viasat_filenames[3]
# del viasat_filenames[4]
# del viasat_filenames[5]
#
# del viasat_filenames[0]
# del viasat_filenames[1]
# del viasat_filenames[2]
#
# del viasat_filenames[0]
# del viasat_filenames[1]
#
# del viasat_filenames[0]


## loop over all the .csv file with the raw VIASAT data
for csv_file in viasat_filenames:
    print(csv_file)
# csv_file = viasat_filenames[2]
    # file = open(csv_file)
    # reader = csv.reader(file)
    reader = csv.reader(codecs.open(csv_file, 'rU', 'utf-16'))
    ## get length of the csv file
    lines = len(list(reader))
    print(lines)

#    reader = csv.reader(codecs.open(csv_file, 'rU', 'utf-16'))
#    for row in reader:
#        print(row)

    slice = 100000  # slice of data to be insert into the DB during the loop
    ## calculate the neccessary number of iteration to carry out in order to upload all data into the DB
    iter = int(round(lines/slice, ndigits=0)) +1
    for i in range(0, iter):
        try:
            print(i)
            print(i, csv_file)
            # csv_file = viasat_filenames[0]
            # df = pd.read_csv(csv_file, header=None, delimiter=',' ,nrows=slice)
            df = pd.read_csv(csv_file, header=None ,delimiter=',', skiprows=i*slice ,nrows=slice, encoding='utf-16')
            ## define colum names
            df.columns = ['idrequest', 'idterm', 'timedate', 'latitude', 'longitude',
                          'speed', 'direction', 'grade', 'panel', 'event', 'vehtype',
                          'progressive', 'millisec', 'timedate_gps', 'distance']
            # df['id'] = pd.Series(range(i * slice, i * slice + slice))
            df['timedate'] = df['timedate'].astype('datetime64[ns]')
            df['timedate_gps'] = df['timedate_gps'].astype('datetime64[ns]')
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
idterm_vehtype_portata.to_csv('D:/ENEA_CAS_WORK/BRESCIA/idterm_vehtype_portata.csv')
## relaod .csv file
idterm_vehtype_portata = pd.read_csv('D:/ENEA_CAS_WORK/BRESCIA/idterm_vehtype_portata.csv')
idterm_vehtype_portata = idterm_vehtype_portata[['idterm', 'vehtype', 'portata']]
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_BS')
connection = engine.connect()
## populate DB
idterm_vehtype_portata.to_sql("idterm_portata", con=connection, schema="public",
          if_exists='append', index=False)


#################################################################################
#################################################################################
##################################################################################


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


BBB = pd.read_sql_query(
    '''SELECT *
        FROM public.dataraw
        WHERE date(timedate) = '2019-03-08'
        limit 1000 ''', conn_HAIG)

BBB = pd.read_sql_query(
    '''SELECT timedate
        FROM public.dataraw
        WHERE date(timedate) = '2019-03-08'
         limit 1000''', conn_HAIG)

################################################################
################################################################




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


