
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

from datetime import datetime
now1 = datetime.now()

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)

    #### get all VIASAT data from map-matching (automobili e mezzi pesanti) on selected date
    # viasat_data = pd.read_sql_query('''
    #                     SELECT
    #                        mapmatching_2019.u, mapmatching_2019.v,
    #                             mapmatching_2019.timedate, mapmatching_2019.mean_speed,
    #                             mapmatching_2019.idtrace, mapmatching_2019.sequenza,
    #                             mapmatching_2019.idtrajectory,
    #                             dataraw.idterm, dataraw.vehtype
    #                        FROM mapmatching_2019
    #                        LEFT JOIN dataraw
    #                                    ON mapmatching_2019.idtrace = dataraw.id
    #                                    WHERE date(mapmatching_2019.timedate) = %s
    #                                    /*AND dataraw.vehtype::bigint = 1*/
    #                                    ''', conn_HAIG, params=[DATE])

viasat_data = pd.read_sql_query('''
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
                                      WHERE dataraw.vehtype::bigint = 1
                                      ''', conn_HAIG)

### get counts for all edges ########
all_data = viasat_data[['u','v']]
all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

# AAA = viasat_data[viasat_data['u'] == 1416519821]

########################################################
##### build the map ####################################

all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
all_counts_uv = gpd.GeoDataFrame(all_counts_uv)
all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)
# all_counts_uv.plot()

## rescale all data by an arbitrary number
all_counts_uv["scales"] = (all_counts_uv.counts/max(all_counts_uv.counts)) * 7

################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################


folium.GeoJson(
all_counts_uv[['u','v', 'counts', 'scales', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'red',
        'color': 'red',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'counts']),
    ).add_to(my_map)

path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/plot_EGDES_all_dates/'
# my_map.save(path + "traffic_" + DATE + "_all_EDGES_counts_Catania.html")
# my_map.save(path + "MON_25_Feb_2019_HEAVY_traffic_counts_all_EDGES_all_Catania.html")
my_map.save(path + "CARS_traffic_counts_all_EDGES_all_Catania.html")


now2 = datetime.now()
print(now2 - now1)
