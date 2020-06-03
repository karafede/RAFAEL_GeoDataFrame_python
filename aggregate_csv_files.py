
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
os.chdir('C:/python/projects/giraffe/viasat_data/CSVs')
cwd = os.getcwd()


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

# erase existing table
cur_HAIG.execute("DROP TABLE IF EXISTS prova_viasat_files_csv CASCADE")
conn_HAIG.commit()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## creat empty table to host all VIASAT data
# dateTime timestamp NOT NULL,
# cur_HAIG.execute("""
#      CREATE  TABLE "prova_viasat_files_csv"(
#      idRequest integer,
#      deviceId integer  ,
#      dateTime timestamp ,
#      latitude numeric ,
#      longitude numeric ,
#      speedKmh integer ,
#      heading integer ,
#      accuracyDop integer ,
#      EngnineStatus integer ,
#      Type integer ,
#      Odometer integer)
#      """)
#
# conn_HAIG.commit()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_CT')

# match pattern of .csv files
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

# Combine all files in the list and export as unique CSV
#combine all files in the list
# combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
#export to csv
# combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')


connection = engine.connect()
for idx , csv_file in enumerate(all_filenames):
    df = pd.read_csv(csv_file, header=None)
    df.columns = ['idRequest', 'deviceId', 'dateTime', 'latitude', 'longitude', 'speedKmh',
                        'heading', 'accuracyDop', 'EngnineStatus', 'Type', 'Odometer']
    print(df)
    df.to_sql("prova_viasat_files_csv", con=connection, schema="public",
                                   if_exists='append', index = False)



## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "prova_viasat_files_csv" add id serial
     """)
conn_HAIG.commit()

## drop one column
cur_HAIG.execute("""
ALTER TABLE "prova_viasat_files_csv" DROP "idRequest"
     """)
conn_HAIG.commit()


## check the "id"
AAA = pd.read_sql_query('''
                    SELECT id 
                    FROM public.prova_viasat_files_csv''', conn_HAIG)


### rename the table in order to create a new one with columns in a different order
cur_HAIG.execute("""
ALTER TABLE "prova_viasat_files_csv" rename to "prova_viasat_files_csv_old"
     """)
conn_HAIG.commit()

## check the dateTime format
cur_HAIG.execute("""
ALTER TABLE prova_viasat_files_csv_old ALTER COLUMN "dateTime" TYPE timestamp USING "dateTime"::timestamp
     """)
conn_HAIG.commit()


## create empty table to host all VIASAT data...with the WANTED order of the columns
cur_HAIG.execute("""
     CREATE  TABLE "prova_viasat_files_csv"(
     id bigint,
     deviceId integer  ,
     dateTime timestamp ,
     latitude numeric ,
     longitude numeric ,
     speedKmh integer ,
     heading integer ,
     accuracyDop integer ,
     EngnineStatus integer ,
     Type integer ,
     Odometer integer)
     """)
conn_HAIG.commit()



# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

## create empty table to host all VIASAT data
cur_HAIG.execute("""
    INSERT into prova_viasat_files_csv (id, deviceid, datetime, latitude, longitude, speedKmh,
                        heading, accuracydop, engninestatus, type, odometer)
    SELECT id, "deviceId", "dateTime", latitude, longitude, "speedKmh",
                        heading, "accuracyDop", "EngnineStatus", "Type", "Odometer" FROM "prova_viasat_files_csv_old";
    """)
conn_HAIG.commit()


conn_HAIG.close()
cur_HAIG.close()