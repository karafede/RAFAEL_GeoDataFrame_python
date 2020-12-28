
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
import csv
from shapely import wkb


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data')
os.getcwd()

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')

## load EDGES from OSM
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, geom
                            FROM "OSM_edges" ''',conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace= True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)
## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)
# gdf_edges.plot()

#########################################################################
## generate a sottorete with mezzi pesanti for each day of the week #####
#########################################################################


from datetime import datetime
now1 = datetime.now()

#### ----------------------------- #################
### all DAYS of FEBRUARY and AUGUST ################

# MONTH = 'FEBRUARY_2019'
MONTH = 'AUGUST_2019'

viasat_data_pesanti = pd.read_sql_query('''
                               SELECT
                               mapmatching_2019.u, mapmatching_2019.v,
                               dataraw.idterm, dataraw.vehtype,
                               To_Char(mapmatching_2019.timedate, 'DAY') as dow
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                                      ON mapmatching_2019.idtrace = dataraw.id  
                                      WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '08'
                                      AND dataraw.vehtype::bigint = 2
                                      /*limit 1000*/
                         ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)


### load all VIASAT data for the month of FEBRUARY ('02')  or AUGUST ('08')---------- #########
### where all weekdays are selected af a PEAK HOURS 7-8; 17-18 (for FEBRUARY and AUGUST)

##############################################
### --- MORNING PEAK HOUR -----###############
##############################################

from datetime import datetime
now1 = datetime.now()

viasat_MORNING_PEAK = pd.read_sql_query('''
                        SELECT
                           mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate,  
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.idterm, dataraw.vehtype,
                               dataraw.speed,
                               To_Char(mapmatching_2019.timedate, 'DAY') as dow
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                               ON mapmatching_2019.idtrace = dataraw.id         
                               WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '08'
                               AND EXTRACT(ISODOW FROM mapmatching_2019.timedate) IN (1,2,3,4,5) /* days of the weeks Monday to Friday*/
                               AND (EXTRACT('hour' FROM mapmatching_2019.timedate) >= 7 and extract('hour' FROM mapmatching_2019.timedate) <= 8)
                               /*AND (EXTRACT('hour' FROM mapmatching_2019.timedate) >= 17 and extract('hour' FROM mapmatching_2019.timedate) <= 18)*/
                               /*limit 1000*/
                         ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)

## change working directory
os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/vulnerability_by_days')

## 1. get all idterm from veicoli pesanti
all_idterms_pesanti = list(viasat_data_pesanti.idterm.unique())
uv_pesanti = viasat_data_pesanti.drop_duplicates(['u', 'v'])
## reset index
uv_pesanti = uv_pesanti.reset_index(level=0)[['u','v']]

## 2. get all data for all vehicles (auto and veicoli pesanti) for a given DAY

## 1. get all idterm for all vehicles
all_idterms = list(viasat_MORNING_PEAK.idterm.unique())
len(all_idterms)
all_idterms = all_idterms[0:300]

######################################################################################
### get ORIGIN and DESTINATION for each idterm of all vehicles #######################
######################################################################################

## 2. get all ORIGIN and DESTINATION for all vehicles

### initialize an empty dataframe
all_catania_OD = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms):
    # print(idx)
    print(idterm)
    all_data = viasat_MORNING_PEAK[(viasat_MORNING_PEAK.idterm == idterm)]
    ## remove duplicates 'idtrajectories' (trips)
    all_trips = all_data.drop_duplicates(['idtrajectory'])
    ## make a list of all_trips
    all_trips = list(all_trips.idtrajectory.unique())
    for idx_a, idtrajectory in enumerate(all_trips):
        # print(idx_a)
        # print(idtrajectory)
        ## filter data by idterm and by idtrajectory (trip)
        data = viasat_MORNING_PEAK[(viasat_MORNING_PEAK.idterm == idterm) & (viasat_MORNING_PEAK.idtrajectory == idtrajectory)]
        ## sort data by "sequenza'
        data = data.sort_values('sequenza')
        ORIGIN = data[data.sequenza == min(data.sequenza)][['u']].iloc[0][0]
        DESTINATION = data[data.sequenza == max(data.sequenza)][['v']].iloc[0][0]
        data['ORIGIN'] = ORIGIN
        data['DESTINATION'] = DESTINATION
        all_catania_OD = all_catania_OD.append(data)
        all_catania_OD = all_catania_OD.drop_duplicates(['u', 'v', 'ORIGIN', 'DESTINATION'])
        ## reset index
        all_catania_OD = all_catania_OD.reset_index(level=0)[['u', 'v', 'ORIGIN', 'DESTINATION']]

## save data
all_catania_OD.to_csv('OD_catania_MORNING_PEAK_' + MONTH + '.csv')


## 4. filter all data with the (u,v) edges of veicoli pesanti
## filter with the edges for veicoli pesanti "viasat_data_pesanti_uv" and create a "sottorete"
sottorete = pd.merge(uv_pesanti, viasat_MORNING_PEAK[['u','v', 'speed']], on=['u', 'v'], how='left')
## drop all rows with NA values
sottorete = sottorete.dropna()

## 5. get "counts" from the "sottorete"
all_data = sottorete[['u','v']]
all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

# del viasat_data_all

### make average for "mean_speed" for the "sottorete"
sottorete_speed = sottorete[['u', 'v', 'speed']]
### get AVERAGE of travelled travelled "speed" for each edge
sottorete_speed = (sottorete_speed.groupby(['u', 'v']).mean()).reset_index()
sottorete_speed['speed'] = sottorete_speed.speed.astype('int')
## change names
sottorete_speed.columns = ["u", "v", "travel_speed"]

## merge with "all_counts_uv"
sottorete_speed = pd.merge(sottorete_speed, all_counts_uv, on=['u', 'v'], how='left')


## define the travelled_time on each edge
## merge with OSM edges (geometry)
sottorete_speed = pd.merge(sottorete_speed, gdf_edges, on=['u', 'v'], how='left')
sottorete_speed.drop_duplicates(['u', 'v'], inplace=True)
sottorete_speed['length(km)'] = sottorete_speed['length']/1000
sottorete_speed['travel_time'] = ((sottorete_speed['length(km)']) / (sottorete_speed['travel_speed'])) *3600 # seconds
sottorete_speed = gpd.GeoDataFrame(sottorete_speed)

## save data (for all vehicles)
sottorete_speed.to_file(filename= 'MORNING_PEAK_' + MONTH +'_sottorete_speed_counts_all_vehicles.geojson', driver='GeoJSON')
sottorete_speed.plot()



##############################################
### --- EVENING PEAK HOUR -----###############
##############################################


### load all VIASAT data for the month of FEBRUARY ('02')  or AUGUST ('08')---------- #########
### where all weekdays are selected af a PEAK HOURS 7-8; 17-18 (for FEBRUARY and AUGUST)

from datetime import datetime
now1 = datetime.now()

viasat_EVENING_PEAK = pd.read_sql_query('''
                        SELECT
                           mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate,  
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.idterm, dataraw.vehtype,
                               dataraw.speed,
                               To_Char(mapmatching_2019.timedate, 'DAY') as dow
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                               ON mapmatching_2019.idtrace = dataraw.id         
                               WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '08'
                               AND EXTRACT(ISODOW FROM mapmatching_2019.timedate) IN (1,2,3,4,5) /* days of the weeks Monday to Friday*/
                               /*AND (EXTRACT('hour' FROM mapmatching_2019.timedate) >= 7 and extract('hour' FROM mapmatching_2019.timedate) <= 8)*/
                               AND (EXTRACT('hour' FROM mapmatching_2019.timedate) >= 17 and extract('hour' FROM mapmatching_2019.timedate) <= 18)
                               /*limit 1000*/
                         ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)

## change working directory
os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/vulnerability_by_days')

## 1. get all idterm from veicoli pesanti
all_idterms_pesanti = list(viasat_data_pesanti.idterm.unique())
uv_pesanti = viasat_data_pesanti.drop_duplicates(['u', 'v'])
## reset index
uv_pesanti = uv_pesanti.reset_index(level=0)[['u','v']]

## 2. get all data for all vehicles (auto and veicoli pesanti) for a given DAY

## 1. get all idterm for all vehicles
all_idterms = list(viasat_EVENING_PEAK.idterm.unique())
len(all_idterms)
all_idterms = all_idterms[0:300]

######################################################################################
### get ORIGIN and DESTINATION for each idterm of all vehicles #######################
######################################################################################

## 2. get all ORIGIN and DESTINATION for all vehicles

### initialize an empty dataframe
all_catania_OD = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms):
    # print(idx)
    print(idterm)
    all_data = viasat_EVENING_PEAK[(viasat_EVENING_PEAK.idterm == idterm)]
    ## remove duplicates 'idtrajectories' (trips)
    all_trips = all_data.drop_duplicates(['idtrajectory'])
    ## make a list of all_trips
    all_trips = list(all_trips.idtrajectory.unique())
    for idx_a, idtrajectory in enumerate(all_trips):
        # print(idx_a)
        # print(idtrajectory)
        ## filter data by idterm and by idtrajectory (trip)
        data = viasat_EVENING_PEAK[(viasat_EVENING_PEAK.idterm == idterm) & (viasat_EVENING_PEAK.idtrajectory == idtrajectory)]
        ## sort data by "sequenza'
        data = data.sort_values('sequenza')
        ORIGIN = data[data.sequenza == min(data.sequenza)][['u']].iloc[0][0]
        DESTINATION = data[data.sequenza == max(data.sequenza)][['v']].iloc[0][0]
        data['ORIGIN'] = ORIGIN
        data['DESTINATION'] = DESTINATION
        all_catania_OD = all_catania_OD.append(data)
        all_catania_OD = all_catania_OD.drop_duplicates(['u', 'v', 'ORIGIN', 'DESTINATION'])
        ## reset index
        all_catania_OD = all_catania_OD.reset_index(level=0)[['u', 'v', 'ORIGIN', 'DESTINATION']]

## save data
all_catania_OD.to_csv('OD_catania_EVENING_PEAK_' + MONTH + '.csv')


## 4. filter all data with the (u,v) edges of veicoli pesanti
## filter with the edges for veicoli pesanti "viasat_data_pesanti_uv" and create a "sottorete"
sottorete = pd.merge(uv_pesanti, viasat_EVENING_PEAK[['u','v', 'speed']], on=['u', 'v'], how='left')
## drop all rows with NA values
sottorete = sottorete.dropna()

## 5. get "counts" from the "sottorete"
all_data = sottorete[['u','v']]
all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

# del viasat_data_all

### make average for "mean_speed" for the "sottorete"
sottorete_speed = sottorete[['u', 'v', 'speed']]
### get AVERAGE of travelled travelled "speed" for each edge
sottorete_speed = (sottorete_speed.groupby(['u', 'v']).mean()).reset_index()
sottorete_speed['speed'] = sottorete_speed.speed.astype('int')
## change names
sottorete_speed.columns = ["u", "v", "travel_speed"]

## merge with "all_counts_uv"
sottorete_speed = pd.merge(sottorete_speed, all_counts_uv, on=['u', 'v'], how='left')


## define the travelled_time on each edge
## merge with OSM edges (geometry)
sottorete_speed = pd.merge(sottorete_speed, gdf_edges, on=['u', 'v'], how='left')
sottorete_speed.drop_duplicates(['u', 'v'], inplace=True)
sottorete_speed['length(km)'] = sottorete_speed['length']/1000
sottorete_speed['travel_time'] = ((sottorete_speed['length(km)']) / (sottorete_speed['travel_speed'])) *3600 # seconds
sottorete_speed = gpd.GeoDataFrame(sottorete_speed)

## save data (for all vehicles)
sottorete_speed.to_file(filename= 'EVENING_PEAK_' + MONTH +'_sottorete_speed_counts_all_vehicles.geojson', driver='GeoJSON')
sottorete_speed.plot()



