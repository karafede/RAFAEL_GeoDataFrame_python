

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


# make a list of unique dates (only dates not times!)
# select an unique table of dates postgresql
unique_DATES = pd.read_sql_query(
    '''SELECT DISTINCT all_dates.dates
        FROM ( SELECT dates.d AS dates
               FROM generate_series(
               (SELECT MIN(timedate) FROM public.mapmatching_2019),
               (SELECT MAX(timedate) FROM public.mapmatching_2019),
              '1 day'::interval) AS dates(d)
        ) AS all_dates
        INNER JOIN public.mapmatching_2019
	    ON all_dates.dates BETWEEN public.mapmatching_2019.timedate AND public.mapmatching_2019.timedate
        ORDER BY all_dates.dates ''', conn_HAIG)

# ADD a new field with only date (no time)
unique_DATES['just_date'] = unique_DATES['dates'].dt.date
MONTHS = unique_DATES['dates'].dt.month

from datetime import datetime
now1 = datetime.now()

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)

##### import selected edges of the CATANIA "sottorete to get all idterms crossing that edges

#
# df = pd.read_csv("sottorete_catania.csv", delimiter=',')
# ## filter by one specific DATE
# df['timedate'] = df['timedate'].astype('datetime64[ns]')
# df['date'] = df['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))


# df.drop_duplicates(['idterm'], inplace=True)
# ## make a list of all IDterminals
### get all the "idterm" crossing the EDGED of the sottorete
# all_idterms = list(df.idterm.unique())


#### get all VIASAT data from map-matching (automobili e mezzi pesanti) on selected date
viasat_data_pesanti = pd.read_sql_query('''
                       SELECT  
                          mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.idterm, dataraw.vehtype
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                                      ON mapmatching_2019.idtrace = dataraw.id  
                                      /*WHERE date(mapmatching_2019.timedate) = '2019-02-25' AND*/
                                      WHERE dataraw.vehtype::bigint = 2
                                      ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)



## 1. get all idterm from veicoli pesanti and unique (u,v)
## 2. get all ORIGIN and DESTINATION for veicoli pesanti
## 3. get all data for all vehicles (auto and veicoli pesanti)
## 4. filter all data with the (u,v) edges of veicoli pesanti
## 5. get "counts" from all data

### get counts ("passaggi") across each EDGE
# all_data = viasat_data[['u','v']]
# all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

## 1. get all idterm from veicoli pesanti
all_idterms_pesanti = list(viasat_data_pesanti.idterm.unique())
uv_pesanti = viasat_data_pesanti.drop_duplicates(['u', 'v'])
## reset index
uv_pesanti = uv_pesanti.reset_index(level=0)[['u','v']]

## 5. get "counts" from the "sottorete"
all_data = viasat_data_pesanti[['u','v']]
all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

### make average for "mean_speed" for the "sottorete"
sottorete_speed = viasat_data_pesanti[['u', 'v', 'mean_speed']]
### get AVERAGE of travelled travelled "speed" for each edge
sottorete_speed = (sottorete_speed.groupby(['u', 'v']).mean()).reset_index()
sottorete_speed['mean_speed'] = sottorete_speed.mean_speed.astype('int')
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

## save data
sottorete_speed.to_file(filename='sottorete_speed_counts_veicoli_pesanti.geojson', driver='GeoJSON')
sottorete_speed.plot()


######################################################################################
### get ORIGIN and DESTINATION for each idterm #######################################
######################################################################################

## 2. get all ORIGIN and DESTINATION for veicoli pesanti

### initialize an empty dataframe
all_catania_OD = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_pesanti):
    print(idterm)
    all_data = viasat_data_pesanti[(viasat_data_pesanti.idterm == idterm)]
    ## remove duplicates 'idtrajectories' (trips)
    all_trips = all_data.drop_duplicates(['idtrajectory'])
    ## make a list of all_trips
    all_trips = list(all_trips.idtrajectory.unique())
    for idx_a, idtrajectory in enumerate(all_trips):
        print(idtrajectory)
        ## filter data by idterm and by idtrajectory (trip)
        data = viasat_data_pesanti[(viasat_data_pesanti.idterm == idterm) & (viasat_data_pesanti.idtrajectory == idtrajectory)]
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
all_catania_OD.to_csv('all_catania_OD.csv')
UVs = all_catania_OD.drop_duplicates(['u', 'v'])
ODs = all_catania_OD.drop_duplicates(['ORIGIN', 'DESTINATION'])

######################################################################################
######################################################################################
######################################################################################
######################################################################################

## 3. get all data for all vehicles (auto and veicoli pesanti)
viasat_data_all = pd.read_sql_query('''
                       SELECT  
                          mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.idterm, dataraw.vehtype
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                                      ON mapmatching_2019.idtrace = dataraw.id  
                                      /*WHERE date(mapmatching_2019.timedate) = '2019-02-25' AND*/
                                      /*WHERE dataraw.vehtype::bigint = 2*/
                                      ''', conn_HAIG)

'''

## 1. get all idterm for all vehicles
all_idterms = list(viasat_data_all.idterm.unique())
uv_all = viasat_data_all.drop_duplicates(['u', 'v'])
## reset index
uv_all = uv_all.reset_index(level=0)[['u','v']]


## 5. get "counts" from the "sottorete"
all_data = viasat_data_all[['u','v']]
all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

### make average for "mean_speed" for the "sottorete"
sottorete_speed = viasat_data_all[['u', 'v', 'mean_speed']]
### get AVERAGE of travelled travelled "speed" for each edge
sottorete_speed = (sottorete_speed.groupby(['u', 'v']).mean()).reset_index()
sottorete_speed['mean_speed'] = sottorete_speed.mean_speed.astype('int')
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

## save data
sottorete_speed.to_file(filename='sottorete_speed_counts_all_vehicles.geojson', driver='GeoJSON')
sottorete_speed.plot()

del all_data
del viasat_data_all


######################################################################################
### get ORIGIN and DESTINATION for each idterm of all vehicles #######################
######################################################################################

## 2. get all ORIGIN and DESTINATION for all vehicles

### initialize an empty dataframe
all_catania_OD = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms):
    print(idterm)
    all_data = viasat_data_all[(viasat_data_all.idterm == idterm)]
    ## remove duplicates 'idtrajectories' (trips)
    all_trips = all_data.drop_duplicates(['idtrajectory'])
    ## make a list of all_trips
    all_trips = list(all_trips.idtrajectory.unique())
    for idx_a, idtrajectory in enumerate(all_trips):
        print(idtrajectory)
        ## filter data by idterm and by idtrajectory (trip)
        data = viasat_data_all[(viasat_data_all.idterm == idterm) & (viasat_data_all.idtrajectory == idtrajectory)]
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
all_catania_OD.to_csv('all_catania_OD_all_vehicles.csv')


'''









## 4. filter all data with the (u,v) edges of veicoli pesanti
## filter with the edges for veicoli pesanti "viasat_data_pesanti_uv" and create a "sottorete"
# sottorete = pd.merge(uv_pesanti, viasat_data_all[['u','v', 'timedate', 'mean_speed']], on=['u', 'v'], how='left')
sottorete = pd.merge(uv_pesanti, viasat_data_all[['u','v', 'mean_speed']], on=['u', 'v'], how='left')
## drop all rows with NA values
sottorete = sottorete.dropna()

## 5. get "counts" from the "sottorete"
all_data = sottorete[['u','v']]
all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
# all_counts_uv = sottorete.groupby(['timedate', 'u', 'v']).size().reset_index().rename(columns={0:'counts'})


# del viasat_data_all

### make average for "mean_speed" for the "sottorete"
sottorete_speed = sottorete[['u', 'v', 'mean_speed']]
### get AVERAGE of travelled travelled "speed" for each edge
sottorete_speed = (sottorete_speed.groupby(['u', 'v']).mean()).reset_index()
# sottorete_speed['mean_speed'] = round(sottorete_speed['mean_speed'], 0)
sottorete_speed['mean_speed'] = sottorete_speed.mean_speed.astype('int')
## change names
sottorete_speed.columns = ["u", "v", "travel_speed"]

## merge with "all_counts_uv"
sottorete_speed = pd.merge(sottorete_speed, all_counts_uv, on=['u', 'v'], how='left')


## save as. csv file
# sottorete_speed.to_csv('sottorete_counts_timdedate.csv')

##########################################################
### get the FLUX #########################################
## load data processed from R  ###########################
# sottorete_FLUX = pd.read_csv("FLUX_sottorete_CATANIA.csv")
# sottorete_FLUX = sottorete_FLUX[['u', 'v', 'hour', 'flux', 'travel_speed']]
# sottorete_FLUX.drop_duplicates(['u', 'v'], inplace=True)


## define the travelled_time on each edge
## merge with OSM edges (geometry)
sottorete_speed = pd.merge(sottorete_speed, gdf_edges, on=['u', 'v'], how='left')
sottorete_speed.drop_duplicates(['u', 'v'], inplace=True)
sottorete_speed['length(km)'] = sottorete_speed['length']/1000
sottorete_speed['travel_time'] = ((sottorete_speed['length(km)']) / (sottorete_speed['travel_speed'])) *3600 # seconds
sottorete_speed = gpd.GeoDataFrame(sottorete_speed)

## save data
sottorete_speed.to_file(filename='sottorete_speed.geojson', driver='GeoJSON')
sottorete_speed.plot()


### generate a map for checking...
################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
################################################################################
# folium.GeoJson('sottorete_FLUX.geojson').add_to((my_map))
# my_map.save("sottorete_FLUX.html")