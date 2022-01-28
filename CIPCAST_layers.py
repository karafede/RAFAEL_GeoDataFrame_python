


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
from folium_stuff_FK_map_matching import plot_graph_folium_FK
from PIL import Image



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


os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data')
os.getcwd()
### load grafo with "cost" (travel times got from Viasat data)
file_graphml = 'CATANIA_VIASAT_cost.graphml'
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)
gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)


"""
list_MONTHS = [february', 'august']
list_DAYS = ['monday', 'tuesday', 'wednesday',
             'thursday', 'friday', 'saturday', 'sunday',
             'morning_peak', 'evening_peak']
"""

month = "august"
day = "evening_peak"


os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/CIPCAST')
os.getcwd()

## load vulnerability_AUGUST_2019
vulnerability = pd.read_sql_query('''
                            SELECT u, v, importance
                            FROM "vulnerability_EVENING_PEAK_AUGUST_2019" 
                            ''',conn_HAIG)

vulnerability = pd.DataFrame(vulnerability)

vulnerability['month'] = month
vulnerability['day'] = day

vulnerability['u'] = vulnerability.u.astype(np.int64)
vulnerability['v'] = vulnerability.v.astype(np.int64)
vulnerability = vulnerability.rename(columns={'importance': 'vulnerability'})

## merge with road network....
vulnerability = pd.merge(vulnerability, gdf_edges, on=['u', 'v'], how='left')
vulnerability = gpd.GeoDataFrame(vulnerability)
vulnerability.drop_duplicates(['u', 'v'], inplace=True)

# vulnerability_MONDAY_AUGUST_2019.plot()
# save as geojson file
vulnerability = vulnerability[['u', 'v', 'day', 'month', 'vulnerability', 'highway', 'name', 'ref', 'length', 'geometry']]
vulnerability['ref'] = vulnerability.ref.astype(str)
vulnerability['name'] = vulnerability.name.astype(str)
vulnerability['highway'] = vulnerability.highway.astype(str)
vulnerability = gpd.GeoDataFrame(vulnerability)
vulnerability.plot()
vulnerability.to_file(filename='vulnerability_' + day + "_" + month + '_2019.geojson', driver='GeoJSON')
