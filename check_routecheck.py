
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_CATANIA()
cur_HAIG = conn_HAIG.cursor()


##########################################################
### Check mapmatching DB #################################
##########################################################

#### check how many TRIP ID we have ######################

# get all ID terminal of Viasat data
idterm = pd.read_sql_query(
    ''' SELECT "idterm" 
        FROM public.routecheck ''', conn_HAIG)

# make a list of all unique trips
processed_idterms = list(idterm.idterm.unique())
## transform all elements of the list into integers
processed_idterms = list(map(int, processed_idterms))
print(len(processed_idterms))

## reload 'all_idterms' as list
with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/all_idterms.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and processed idterms

all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(processed_idterms))
print(len(all_ID_TRACKS_DIFF))

# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/all_idterms_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))

