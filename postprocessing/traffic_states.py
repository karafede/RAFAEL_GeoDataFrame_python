
import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
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
# from funcs_network_FK import roads_type_folium
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
# import db_connect
import datetime
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK
import glob
from funcs_network_FK import cost_assignment
import statistics



#######################################################################
## count how many times each "travelled speed (km/h)"
#######################################################################

# load all map matching files
# match pattern of .GeoJson files
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing\\new_geojsons')
extension = 'geojson'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
#combine all files in the list
gdf_all_EDGES = pd.concat([gpd.read_file(f) for f in all_filenames])

# select speed and timedate
gdf_all_EDGES_speed = gdf_all_EDGES[['u', 'v', 'speed', 'timedate', 'length']]
gdf_all_EDGES_speed['speed'] = (gdf_all_EDGES_speed['speed'].ffill()+gdf_all_EDGES_speed['speed'].bfill())/2
# fill empty rows and round data
gdf_all_EDGES_speed = gdf_all_EDGES_speed.dropna(subset=['speed'])  # remove nan values
gdf_all_EDGES_speed['speed'] = round(gdf_all_EDGES_speed['speed'], 0)


## select only columns with 'u' and 'v' and speed
gdf_all_EDGES_speeds = gdf_all_EDGES_speed[['u', 'v', 'speed']]
df_all_EDGES_speeds = gdf_all_EDGES_speeds.groupby(gdf_all_EDGES_speeds.columns.tolist()).size().reset_index().rename(columns={0:'records_speeds'})

## select only columns 'u' and 'v'
gdf_all_EDGES_sel = gdf_all_EDGES_speed[['u', 'v']]
df_all_EDGES_sel = gdf_all_EDGES_sel.groupby(gdf_all_EDGES_sel.columns.tolist()).size().reset_index().rename(columns={0:'records_per_edge'})

# merge records_speeds and records 
MERGED_speed_records = pd.merge(df_all_EDGES_speeds, df_all_EDGES_sel, on=['u', 'v'], how='inner')

AAA = MERGED_speed_records[(MERGED_speed_records['u'] == 33590390) & (MERGED_speed_records['v'] == 2811352408)]
AAA["time_at_mean_speed(%)"] = round((AAA.records_speeds /AAA.records_per_edge)*100, 0)

MERGED_speed_records["time_at_mean_speed(%)"] = round((MERGED_speed_records.records_speeds /MERGED_speed_records.records_per_edge)*100, 0)
MERGED_speed_records[["time_at_mean_speed(%)"]] = MERGED_speed_records[["time_at_mean_speed(%)"]].astype(int)



####################################
####################################
## get highway type on each edge ###

# load the file assigning the type of highway to each (u,v) edge
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
edges_highways = pd.read_csv("edges_highways.csv")

MERGED_speed_records = pd.merge(MERGED_speed_records, edges_highways[['u','v','highway']], on=['u', 'v'], how='left')
MERGED_speed_records.drop_duplicates(['u', 'v', 'speed', 'records_speeds', 'records_per_edge'], inplace=True)

###############################################
## assign max speed to each type of highway ###
###############################################

# define input road types as list
road_type = ['motorway', 'motorway_link', 'secondary', 'primary', 'tertiary', 'residential', 'unclassified',
             'trunk', 'trunk_link', 'tertiary_link', 'secondary_link', 'service']

## define "MAX SPEED" for each category of road in "highway"
# make a dictionary for ech max speed
max_speed_dict = {
        "residential": 50,
        "secondary": 90,
        "primary": 70,
        "tertiary": 70,
        "unclassified": 60,
        "secondary_link": 55,
        "trunk": 90,
        "tertiary_link": 50,
        "primary_link": 90,
        "motorway_link": 100,
        "trunk_link": 70,
        "motorway": 130,
        "living_street": 50,
        "road": 30,
        "other": 30,
        "service": 30
    }


# prepare a base_map ###########################################################
edges_copy = MERGED_speed_records
edges_copy = edges_copy['highway'].str.replace(r"\(.*\)", "")
MERGED_speed_records.highway = edges_copy

all_data = pd.DataFrame([])
for road in road_type:
    print(road)
    if road in max_speed_dict.keys():
        print("yes")
        maxspeed = max_speed_dict.get(road)
        highway_type = MERGED_speed_records[(MERGED_speed_records.highway.isin([road]))]
        if len(highway_type) != 0:
            highway_type['maxspeed'] = maxspeed
            all_data = all_data.append(highway_type)

MERGED_speed_records = all_data



# add field "length"
MERGED_speed_records = pd.merge(MERGED_speed_records, gdf_all_EDGES[['u', 'v','length']], on=['u', 'v'], how='left')
MERGED_speed_records.drop_duplicates(['u', 'v', 'speed', 'records_speeds', 'records_per_edge'], inplace=True)
MERGED_speed_records.reset_index(inplace=True)

# calculate density (N vehicle/space)
MERGED_speed_records['density (vei/km)'] = MERGED_speed_records['records_per_edge']/(MERGED_speed_records['length']/1000)

# flux
# calculate travel demand (flux) (NULL SCENARIO, no penalty)
AAA = pd.DataFrame( gdf_all_EDGES[['u', 'v','timedate']])
## fill nans by values after non-nan values (for 'hour' and 'timedate')
AAA['timedate'] = AAA['timedate'].ffill()
AAA = AAA.dropna(subset=['timedate'])  # remove nan values

gdf_all_EDGES_speed['timedate'] = pd.to_datetime(df_flux['timedate'])
## use 15 minutes average
df_flux = df_flux.resample(rule='15Min', on='timedate').mean()
df_flux['records'] = df_flux['records'].ffill()  # 15 minutes flux
df_flux['records'] = df_flux['records'] / 0.25
BBB = pd.DataFrame(df_flux['records'])
TRAVEL_DEMAND_OD = BBB.records.mean()  # veichles/hour