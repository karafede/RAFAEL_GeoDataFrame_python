
import os
os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL')
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


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")



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
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/connect_HAIG_Viasat_CT')

## load EDGES from OSM
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, name, geom
                            FROM "OSM_edges" ''',conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace= True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)


gdf_nodes = pd.read_sql_query('''
                            SELECT *
                            FROM "OSM_edges" ''',conn_HAIG)
gdf_nodes['geometry'] = gdf_nodes.apply(wkb_tranformation, axis=1)
gdf_nodes.drop(['geom'], axis=1, inplace= True)
gdf_nodes = gpd.GeoDataFrame(gdf_nodes)


## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)
# gdf_edges.plot()

#### this is the mapmatching ONLY for the date of the 14 February 2019 (wednesday) #########

### set DAY to filter
DAY = '2019-02-14'

matched_data = pd.read_sql_query('''
                        WITH data AS(
                       SELECT  
                          mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.speed, dataraw.vehtype
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                                      ON mapmatching_2019.idtrace = dataraw.id  
                                       WHERE date(mapmatching_2019.timedate) = '2019-02-14' 
                                       /* WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '03'*/
                                       /* AND dataraw.vehtype::bigint = 2 */
                          )
                      SELECT u, v, vehtype, count(*) as counts,
                      date_part('hour', timedate) as hr
                      from data
                      group by u,v, hr, vehtype
                       ''', conn_HAIG)




## get counts ("passaggi") across each EDGE
all_counts_uv = matched_data

# compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
max_counts = max(all_counts_uv['counts'])
all_counts_uv['frequency'] = (all_counts_uv['counts']/max_counts)*100
all_counts_uv['frequency'] = round(all_counts_uv['frequency'], 0)
all_counts_uv['frequency'] = all_counts_uv.frequency.astype('int')

## rescale all data by an arbitrary number
all_counts_uv['scales'] = (all_counts_uv['counts']/max_counts) * 7


## Normalize to 1 and get loads
all_counts_uv["load(%)"] = round(all_counts_uv["counts"]/max(all_counts_uv["counts"]),4)*100


## merge edges for congestion with the road network to get the geometry
all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
# all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)

## sort by "frequency"
all_counts_uv.sort_values('frequency', ascending=True, inplace= True)
## add new column with the selected DAY
all_counts_uv['day'] = DAY
all_counts_uv = all_counts_uv[['u', 'v', 'vehtype', 'counts', 'hr', 'load(%)', 'length', 'name', 'geometry', 'day']]
all_counts_uv.to_csv('D:\\Federico\\FCDapp\\matched_routes_CATANIA_2019_02_14.csv')



