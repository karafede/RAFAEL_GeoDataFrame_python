
import os
# check working directory
cwd = os.getcwd()
# change working directoy
os.chdir('D:\\ENEA_CAS_WORK\\Catania_RAFAEL')
cwd = os.getcwd()
cwd

import psycopg2
import db_connect
from sklearn.metrics import silhouette_score
from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist
import math
import pandas as pd
import csv
import datetime

#Connect to an existing database
conn=db_connect.connect_viasat()
cur = conn.cursor()

# erase existing table
cur.execute("DROP TABLE IF EXISTS viasat_py_temp CASCADE")
cur.execute("DROP TABLE IF EXISTS sequence_catania CASCADE")

# dateTime timestamp NOT NULL,
cur.execute("""
     CREATE  TABLE viasat_py_temp(
     idRequest integer,
     deviceId integer  ,
     dateTime timestamp ,
     Latitude numeric ,
     longitude numeric ,
     speedKmh integer ,
     heading integer ,
     accuracyDop integer ,
     EngnineStatus integer ,
     Type integer ,
     Odometer integer)
     """)


# create a table of bin, lon and lat
cur.execute("""
     CREATE  TABLE sequence_catania(
     idRequest integer ,
     longitude numeric ,
     Latitude numeric)
     """)


conn.commit()
sep = ","

with open('VST_ENEA_CT_20190411_150502.csv', 'r') as f:
    viasat_data = csv.reader(f, delimiter = ',')
    for col in viasat_data:
        # print(len(row))
        if len(col) == 11:
            print(col[2])
            print(col[0])
            idRequest=col[0]
            deviceId=col[1]
            datatime=col[2]
            Latitude=col[3]
            longitude=col[4]
            speedKmh=col[5]
            heading=col[6]
            accuracyDop=col[7]
            EngnineStatus=col[8]
            Type=col[9]
            Odometer=col[10]
            input = "(" + str(idRequest) + sep\
                    + str(deviceId) + sep\
                    + "'" + str(datatime) + "'" + sep\
                    + str(Latitude) + sep\
                    + str(longitude) + sep\
                    + str(speedKmh) + sep\
                    + str(heading) + sep\
                    + str(accuracyDop) + sep\
                    + str(EngnineStatus) + sep\
                    + str(Type) + sep\
                    + str(Odometer) + ")"
            # input = "(" + str(idRequest) + ")"
            cur.execute("INSERT INTO viasat_py_temp (idRequest, deviceId, dateTime, Latitude, longitude, speedKmh, "
                        "heading, accuracyDop, EngnineStatus, Type, Odometer)" + " VALUES " +input + "")

conn.commit()


with open('VST_ENEA_CT_20190411_150502.csv', 'r') as f:
    viasat_data = csv.reader(f, delimiter = ',')
    for col in viasat_data:
        # print(len(row))
        if len(col) == 11:
            print(col[2])
            print(col[0])
            idRequest=col[0]
            deviceId=col[1]
            datatime=col[2]
            Latitude=col[3]
            longitude=col[4]
            speedKmh=col[5]
            heading=col[6]
            accuracyDop=col[7]
            EngnineStatus=col[8]
            Type=col[9]
            Odometer=col[10]
            input = "(" + str(idRequest) + sep\
                    + str(longitude) + sep\
                    + str(Latitude) + ")"
            # input = "(" + str(idRequest) + ")"
            cur.execute("INSERT INTO sequence_catania (idRequest, longitude, Latitude)" + " VALUES " +input + "")

conn.commit()


# erase extensions postgis
cur.execute("DROP EXTENSION IF EXISTS postgis CASCADE")
cur.execute("DROP EXTENSION IF EXISTS postgis_topology CASCADE")

cur.execute("""
CREATE EXTENSION postgis
""")

cur.execute("""
CREATE EXTENSION postgis_topology
""")

# add geometry WGS84 4286 (Catania, Italy)
cur.execute("""
alter table viasat_py_temp add column geom geometry(POINT,4326)
""")

cur.execute("""
update viasat_py_temp set geom = st_setsrid(st_point(longitude,Latitude),4326)
""")

conn.commit()

#### aggregate mutiple .csv data?????????????


conn.close()
cur.close()
