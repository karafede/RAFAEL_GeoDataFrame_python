
import os
os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data')
os.getcwd()

import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import folium
import osmnx as ox
import networkx as nx
import math
import momepy
from funcs_network_FK import roads_type_folium
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import db_connect
import datetime
from datetime import datetime
from datetime import date
from datetime import datetime
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import sqlalchemy as sal
import geopy.distance
import momepy
from shapely import wkb



import multiprocessing as mp
from multiprocessing import Process, freeze_support, Manager
from time import sleep
from collections import deque
import contextlib
from multiprocessing import Manager
from multiprocessing import Pool

import dill as Pickle
from joblib import Parallel, delayed
from joblib.externals.loky import set_loky_pickler
set_loky_pickler('pickle')
from multiprocessing import Pool,RLock


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

#####################################################
####################################################

# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS accuracy_2019 CASCADE")
# conn_HAIG.commit()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_CT')

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)


## load EDGES from OSM
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, geom
                            FROM "OSM_edges" ''',conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace= True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)
## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)


#### get the list of all TRIP_ID from mapmatching
#### check how many TRIP ID we have #############


# # get all ID terminal of Viasat data
# all_VIASAT_TRIP_IDs = pd.read_sql_query(
#     ''' SELECT "TRIP_ID"
#         FROM public.mapmatching_2019 ''', conn_HAIG)
#
# # make a list of all unique trips
# all_TRIP_IDs = list(all_VIASAT_TRIP_IDs.TRIP_ID.unique())
# ### save and treat result in R #####
# with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/all_TRIP_IDs_2019.txt", "w") as file:
#     file.write(str(all_TRIP_IDs))


# ## reload 'all_TRIP_IDs' as list
# with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/all_TRIP_IDs_2019.txt", "r") as file:
#     all_TRIP_IDs = eval(file.readline())
# # print(len(all_TRIP_IDs))
#
#
#
# #### check how many TRIP ID we have ######################
#
# # get all ID terminal of Viasat data
# TRIP_IDs = pd.read_sql_query(
#     ''' SELECT "TRIP_ID"
#         FROM public.accuracy_2019 ''', conn_HAIG)
#
# # make a list of all unique trips
# TRIP_IDs = list(TRIP_IDs.TRIP_ID.unique())
#
# ## make difference between all idterm and matched idterms
# TRIP_IDs_DIFF = list(set(all_TRIP_IDs) - set(TRIP_IDs))
# print(len(TRIP_IDs_DIFF))
# # ## save 'all_ID_TRACKS' as list
# with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/all_TRIP_IDs_2019_new.txt", "w") as file:
#     file.write(str(TRIP_IDs_DIFF))


## reload 'all_TRIP_IDs' as list
with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/all_TRIP_IDs_2019_new.txt", "r") as file:
    all_TRIP_IDs = eval(file.readline())



def func(arg):
    last_trip_idx, TRIP_ID = arg

    print(TRIP_ID)
    trip = str(TRIP_ID)
    ## get u, v from each TRIP_ID
    selected_trip = pd.read_sql_query('''
                SELECT u, v, sequenza, "TRIP_ID"
                FROM public.mapmatching_2019 
                WHERE "TRIP_ID"::TEXT = '%s' ''' % trip, conn_HAIG)

    ### sort values by "sequenza"
    selected_trip = selected_trip.sort_values('sequenza')

    selected_trip = pd.merge(selected_trip, gdf_edges[['u', 'v', 'length']], on=['u', 'v'], how='left')
    selected_trip.drop_duplicates(['u', 'v'], inplace=True)
    ### find the travelled distance of the matched route
    sum_distance_mapmatching = sum(selected_trip.length)


    ## get progressive associate to each TRIP_ID
    routecheck_trip = pd.read_sql_query('''
                SELECT  "TRIP_ID", progressive
                FROM public.routecheck_2019 
                WHERE "TRIP_ID"::TEXT = '%s' ''' % trip, conn_HAIG)
    ## sum of progressive distance (true distance travelled by the vehicle)
    diff_progressive = routecheck_trip.progressive.diff()
    diff_progressive = diff_progressive.dropna()
    sum_progressive = sum(diff_progressive)  ## in meters

    ## calculate the accuracy of the matched route compared to the sum of the differences of the progressives (from Viasat data)
    ###  ACCURACY: ([length of the matched trajectory] / [length of the travelled distance (sum  delta progressives)])*100
    accuracy = int(int((sum_distance_mapmatching / sum_progressive) * 100))
    df_accuracy = pd.DataFrame({'accuracy': [accuracy], 'TRIP_ID': [trip]})

    #### Connect to database using a context manager and populate the DB ####
    connection = engine.connect()
    df_accuracy.to_sql("accuracy_2019", con=connection, schema="public",
                       if_exists='append')
    connection.close()



################################################
##### run all script using multiprocessing #####
################################################

## check how many processer we have available:
# print("available processors:", mp.cpu_count())

if __name__ == '__main__':
    # pool = mp.Pool(processes=mp.cpu_count()) ## use all available processors
    pool = mp.Pool(processes=55)     ## use 60 processors
    print("++++++++++++++++ POOL +++++++++++++++++", pool)
    results = pool.map(func, [(last_trip_idx, TRIP_ID) for last_trip_idx, TRIP_ID in enumerate(all_TRIP_IDs)])
    pool.close()
    pool.close()
    pool.join()
