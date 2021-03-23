
#####################################
### build route from routecheck #####
#####################################

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
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from PIL import Image


import multiprocessing as mp
from multiprocessing import Process, freeze_support, Manager
from time import sleep
from collections import deque
from multiprocessing.managers import BaseManager
import contextlib
from multiprocessing import Manager
from multiprocessing import Pool

import dill as Pickle
from joblib import Parallel, delayed
from joblib.externals.loky import set_loky_pickler
set_loky_pickler('pickle')
from multiprocessing import Pool,RLock


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

# idtrajectory;
# idterm;
# idtrace_o;
# idtrace_d;
# latitude;
# longitude;
# timedate;
# tripdistance_m;
# triptime_s;
# checkcode;
# breaktime_s;
# c_idtrajectory


# connect to new DB
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_CT')

### erase existing table...if exists....
# cur_HAIG.execute("DROP TABLE IF EXISTS route_2019 CASCADE")
# conn_HAIG.commit()


# get all terminals corresponding to 'cars' and 'fleet' (from routecheck_2019)
ID_vehicles = pd.read_sql_query('''
               SELECT idterm, vehtype
               FROM public.routecheck_2019
               /*WHERE vehtype = '1'*/
               ''', conn_HAIG)
# make an unique list
idterms = list(ID_vehicles.idterm.unique())
## save 'all_ID_TRACKS' as list
with open("idterms_2019.txt", "w") as file:
    file.write(str(idterms))

# ## reload all 'idterms' as list
with open("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/idterms_2019.txt", "r") as file:
   idterms = eval(file.readline())


# idterm = 4057114

def func(arg):
    last_idterm_idx, idterm = arg

# for last_track_idx, idterm in enumerate(idterms_cars):
    print(idterm)
    idterm = str(idterm)
    # print('VIASAT GPS track:', track_ID)
    viasat_data = pd.read_sql_query('''
                SELECT * FROM public.routecheck_2019 
                WHERE idterm = '%s' ''' % idterm, conn_HAIG)
    viasat_data = viasat_data.sort_values('timedate')
    ## add a field with the "NEXT timedate" in seconds
    viasat_data['next_totalseconds'] = viasat_data.totalseconds.shift(-1)
    viasat_data['next_timedate'] = viasat_data.timedate.shift(-1)
    viasat_data['next_totalseconds'] = viasat_data['next_totalseconds'].astype('Int64')
    viasat_data['next_totalseconds'] = viasat_data['next_totalseconds'].fillna(0)

    all_trips = list(viasat_data.idtrajectory.unique())
    for idx, idtrajectory in enumerate(all_trips):
        ### initialize an empty dataframe
        route_CATANIA = pd.DataFrame([])
        # print(idtrajectory)
        ## filter data by idterm and by idtrajectory (trip)
        data = viasat_data[viasat_data.idtrajectory == idtrajectory]
        idtrace_o = data[data.segment == min(data.segment)][['id']].iloc[0][0]
        idtrace_d = data[data.segment == max(data.segment)][['id']].iloc[0][0]
        latitude_o = data[data.segment == min(data.segment)][['latitude']].iloc[0][0]  ## at the ORIGIN
        longitude_o = data[data.segment == min(data.segment)][['longitude']].iloc[0][0]  ## at the ORIGIN
        latitude_d = data[data.segment == max(data.segment)][['latitude']].iloc[0][0]  ## at the DESTINATION
        longitude_d = data[data.segment == max(data.segment)][['longitude']].iloc[0][0]  ## at the DESTINATION
        timedate = str(data[data.segment == min(data.segment)][['timedate']].iloc[0][0])  ## at the ORIGIN
        ## trip distance in meters (sum of the increment of the "progressive"
        ## add a field with the "previous progressive"
        data['last_progressive'] = data.progressive.shift()  # <-------
        data['last_progressive'] = data['last_progressive'].astype('Int64')
        data['last_progressive'] = data['last_progressive'].fillna(0)
        ## compute increments of the distance (in meters)
        data['increment'] = data.progressive - data.last_progressive
        ## sum all the increments
        # tripdistance_m = sum(data['increment'])
        tripdistance_m = sum(data['increment'][1:len(data['increment'])])
        if tripdistance_m < 0:
            tripdistance_m = 0
        ## trip time in seconds (duration)
        time_o = data[data.segment == min(data.segment)][['path_time']].iloc[0][0]
        time_d = data[data.segment == max(data.segment)][['path_time']].iloc[0][0]
        triptime_s = time_d - time_o
        # time_o = data[data.segment == min(data.segment)][['totalseconds']].iloc[0][0]
        # time_d = data[data.segment == max(data.segment)][['totalseconds']].iloc[0][0]
        # triptime_s = time_d - time_o
        checkcode = data[data.segment == min(data.segment)][['anomaly']].iloc[0][0] ## at the ORIGIN
        ## intervallo di tempo tra un l'inizio di due viaggi successivi
        breaktime_s =  data[data.segment == max(data.segment)][['next_totalseconds']].iloc[0][0] -  \
                       data[data.segment == min(data.segment)][['totalseconds']].iloc[0][0]\
                       - triptime_s
        if breaktime_s < 0:
            breaktime_s = None
        ### build the final dataframe ("route" table)
        df_ROUTE = pd.DataFrame({'idtrajectory': [idtrajectory],
                                 'idterm': [idterm],
                                 'idtrace_o': [idtrace_o],
                                 'idtrace_d': [idtrace_d],
                                 'latitude_o': [latitude_o],
                                 'longitude_o': [longitude_o],
                                 'latitude_d': [latitude_d],
                                 'longitude_d': [longitude_d],
                                 'timedate_o': [timedate],
                                 'tripdistance_m': [tripdistance_m],
                                 'triptime_s': [triptime_s],
                                 'checkcode': [checkcode],
                                 'breaktime_s': [breaktime_s]})
        route_CATANIA = route_CATANIA.append(df_ROUTE)
        connection = engine.connect()
        route_CATANIA.to_sql("route_2019", con=connection, schema="public",
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
    results = pool.map(func, [(last_idterm_idx, idterm) for last_idterm_idx, idterm in enumerate(idterms)])
    pool.close()
    pool.close()
    pool.join()
