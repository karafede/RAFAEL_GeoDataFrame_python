
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


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

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


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

## get all the acccuracies from DB
accuracy_2019_all = pd.read_sql_query('''
               SELECT *
               FROM public.accuracy_2019 
               WHERE accuracy <= 70 
               AND accuracy > 10''' , conn_HAIG)

accuracy_2019_all = accuracy_2019_all.sort_values('accuracy')

## get a list of all TRIP_ID
trip_idx = list(accuracy_2019_all["TRIP_ID"])

# TRIP_ID = '3153187_conc_514'

###############################################################
###############################################################

## check LENGTHS of mapmaching path

### initialize an empty dataframe
all_mapped_length = pd.DataFrame([])

for idx, TRIP_ID in enumerate(trip_idx):
    print(TRIP_ID)
    trip = str(TRIP_ID)
    ## get u, v from each TRIP_ID
    selected_trip = pd.read_sql_query('''
                  SELECT u, v, sequenza, "TRIP_ID"
                  FROM public.mapmatching_2019 
                  WHERE "TRIP_ID"::TEXT = '%s' ''' % trip, conn_HAIG)


    selected_trip = pd.merge(selected_trip, gdf_edges[['u', 'v', 'length']], on=['u', 'v'], how='left')
    selected_trip.drop_duplicates(['u', 'v'], inplace=True)
    ### find the travelled distance of the matched route
    sum_distance_mapmatching = sum(selected_trip.length)
    sum_distance_mapmatching = int(sum_distance_mapmatching)
    print("============= DISTANCE MapMatching (meters):",  sum_distance_mapmatching)

    ## get progressive associate to each TRIP_ID
    routecheck_trip = pd.read_sql_query('''
                   SELECT  "TRIP_ID", progressive
                   FROM public.routecheck_2019 
                   WHERE "TRIP_ID"::TEXT = '%s' ''' % trip, conn_HAIG)
    ## sum of progressive distance (true distance travelled by the vehicle)
    diff_progressive = routecheck_trip.progressive.diff()
    diff_progressive = diff_progressive.dropna()
    sum_progressive = sum(diff_progressive)  ## in meters
    sum_progressive = int(sum_progressive)
    print("=============== length PROGRESSIVE Viasat (meters):",  sum_progressive)

    ## calculate the accuracy of the matched route compared to the sum of the differences of the progressives (from Viasat data)
    ###  ACCURACY: ([length of the matched trajectory] / [length of the travelled distance (sum  delta progressives)])*100
    if (sum_distance_mapmatching > 0 & sum_distance_mapmatching > 0):

        accuracy = int(int((sum_distance_mapmatching / sum_progressive) * 100))
        print("%%%%%======== ACCURACY (%):",  accuracy)

        ## build a triplet
        d = {'dist_matched (m)': [sum_distance_mapmatching],
             'dist_progressive (m)': [sum_progressive],
             'accuracy': [accuracy]}
        df = pd.DataFrame(data=d)
        all_mapped_length = all_mapped_length.append(df)

## save CSV data
all_mapped_length.to_csv('all_mapped_length_CATANIA.csv')
