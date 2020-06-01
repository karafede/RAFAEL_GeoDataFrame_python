
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
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import datetime
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK
from shapely.geometry import Point, LineString, MultiLineString
from shapely import geometry, ops
import glob
import db_connect
from shapely import wkb
import sqlalchemy as sal

'''
gdf_all_EDGES = gpd.read_file('C:/ENEA_CAS_WORK/Catania_RAFAEL/postprocessing/new_geojsons/all_EDGES_2019-04-15_Apr-16-2020_130_194.geojson')
AAA = pd.DataFrame(gdf_all_EDGES)
'''


conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_CT')

# get all map-matched data from the DB
gdf_all_EDGES = pd.read_sql_query(
    ''' SELECT *
        FROM public.mapmatching''', conn_HAIG)

## transform Geometry from text to LINESTRING
# wkb.loads(gdf_all_EDGES.geom, hex=True)
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

gdf_all_EDGES['geometry'] = gdf_all_EDGES.apply(wkb_tranformation, axis=1)
gdf_all_EDGES.drop(['geom'], axis=1, inplace= True)
gdf_all_EDGES = gpd.GeoDataFrame(gdf_all_EDGES)
gdf_all_EDGES.plot()


os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
## select only columns 'u' and 'v'
gdf_all_EDGES_sel = gdf_all_EDGES[['u', 'v']]
# time --> secs
# distance --> km
# speed --> km/h
gdf_all_EDGES_time = gdf_all_EDGES[['u', 'v', 'mean_speed']]

## fill nans by mean of before and after non-nan values (for 'time' and 'speed')
# gdf_all_EDGES_time['time'] = (gdf_all_EDGES_time['time'].ffill()+gdf_all_EDGES_time['time'].bfill())/2
# gdf_all_EDGES_time['speed'] = (gdf_all_EDGES_time['speed'].ffill()+gdf_all_EDGES_time['speed'].bfill())/2


#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

df_all_EDGES_sel = gdf_all_EDGES.groupby(gdf_all_EDGES_sel.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'records'})

# make a copy
df_all_EDGES_records = df_all_EDGES_sel
threshold = np.average(df_all_EDGES_records.records)


### add colors based on 'records'
vmin = min(df_all_EDGES_records.records)
vmax = max(df_all_EDGES_records.records)
# df_all_EDGES_records.iloc[-1] = np.nan
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.Reds)  # scales of reds
df_all_EDGES_records['color'] = df_all_EDGES_records['records'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_sel = df_all_EDGES_sel[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
clean_edges_matched_route = pd.merge(df_all_EDGES_sel, gdf_all_EDGES, on=['u', 'v'],how='left')
clean_edges_matched_route = gpd.GeoDataFrame(clean_edges_matched_route)
clean_edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)

# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
MERGED_clean_EDGES = pd.merge(clean_edges_matched_route, df_all_EDGES_records, on=['u', 'v'], how='inner')
# remove duplicates nodes
MERGED_clean_EDGES.drop_duplicates(['u', 'v'], inplace=True)
MERGED_clean_EDGES['records'] = round(MERGED_clean_EDGES['records'], 0)
MERGED_clean_EDGES['length(km)'] = MERGED_clean_EDGES['length']/1000
MERGED_clean_EDGES['length(km)'] = round(MERGED_clean_EDGES['length(km)'], 3)
# compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
max_records = max(MERGED_clean_EDGES['records'])
MERGED_clean_EDGES['frequency(%)'] = (MERGED_clean_EDGES['records']/max_records)*100
MERGED_clean_EDGES['frequency(%)'] = round(MERGED_clean_EDGES['frequency(%)'], 0)

df_MERGED_clean_EDGES = pd.DataFrame(MERGED_clean_EDGES)

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=14, tiles='cartodbpositron')
###################################################

# add colors to map
my_map = plot_graph_folium_FK(MERGED_clean_EDGES, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=2, edge_opacity=0.7)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    MERGED_clean_EDGES[['u','v', 'frequency(%)', 'records', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'length(km)', 'frequency(%)', 'records']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
##########################################

MERGED_clean_EDGES.to_file(filename='DB_FREQUENCIES_and_RECORDS_by_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_frequecy_all_EDGES_2019-04-15_May-26-2020.html")


#######################################################################
######### get the travelled TIME in each edge, when available #########
#######################################################################


### get AVERAGE of traveled "time" and travelled "speed" for each edge
df_all_EDGES_time = (gdf_all_EDGES_time.groupby(['u', 'v'], sort = False).mean()).reset_index()
df_all_EDGES_time.columns = ["u", "v", "travel_speed"]
df_all_EDGES_time = pd.merge(MERGED_clean_EDGES, df_all_EDGES_time, on=['u', 'v'], how='inner')
df_all_EDGES_time = pd.DataFrame(df_all_EDGES_time)

## get selected columns
df_all_EDGES_time = df_all_EDGES_time[["u", "v", "length(km)", "travel_speed"]]
## get travelled time (in seconds)
df_all_EDGES_time['travel_time'] = ((df_all_EDGES_time['length(km)']) / (df_all_EDGES_time['travel_speed'])) *3600 # seconds

# make a copy
df_all_timeEDGES = df_all_EDGES_time
# add colors based on 'time' (seconds)
vmin = min(df_all_timeEDGES.travel_time[df_all_timeEDGES.travel_time > 0])
vmax = max(df_all_timeEDGES.travel_time)
AVG = np.average(df_all_timeEDGES.travel_time)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.cool)  # scales of reds (or "coolwarm" , "bwr", °cool°)
df_all_timeEDGES['color'] = df_all_timeEDGES['travel_time'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_time = df_all_EDGES_time[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
times_edges_matched_route = pd.merge(df_all_EDGES_time, gdf_all_EDGES, on=['u', 'v'],how='left')
times_edges_matched_route = gpd.GeoDataFrame(times_edges_matched_route)
times_edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)


# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
TIME_EDGES = pd.merge(times_edges_matched_route, df_all_timeEDGES, on=['u', 'v'], how='inner')
# remove duplicates nodes
TIME_EDGES.drop_duplicates(['u', 'v'], inplace=True)
TIME_EDGES['travel_time'] = round(TIME_EDGES['travel_time'], 1)
TIME_EDGES['travel_speed'] = round(TIME_EDGES['travel_speed'], 0)

TIME_EDGES=TIME_EDGES.rename(columns = {'travel_time':'travel time (sec)'})
TIME_EDGES=TIME_EDGES.rename(columns = {'travel_speed':'travelled speed (km/h)'})

df_TIME_EDGES = pd.DataFrame(TIME_EDGES)
merged_data = pd.merge(df_TIME_EDGES, df_MERGED_clean_EDGES, on=['u', 'v',
                                                         'index', 'idtrajectory', 'idtrace', 'sequenza',
                                                         'mean_speed', 'timedate', 'totalseconds', 'TRIP_ID',
                                                         'track_ID', 'length', 'highway', 'name', 'ref',
                                                         'length(km)'], how = 'inner')
merged_data.drop(['color_x', 'color_y', 'geometry_y'], axis=1, inplace = True)
merged_data = merged_data.rename(columns={'geometry_x': 'geometry'})


#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################

# add colors to map
my_map = plot_graph_folium_FK(TIME_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    TIME_EDGES[['travel time (sec)', 'travelled speed (km/h)', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel time (sec)', 'travelled speed (km/h)', 'length(km)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

TIME_EDGES.to_file(filename='DB_TIME_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_travel_time_all_EDGES_2019-04-15_May-26-2020.html")

#######################################################################
######### get the travelled SPEED in each edge, when available ########
#######################################################################

### get average of traveled "time" and travelled "speed" for each edge
df_all_EDGES_speed = (gdf_all_EDGES_time.groupby(['u', 'v'], sort = False).mean()).reset_index()
df_all_EDGES_speed.columns = ["u", "v", "travel_speed"]
df_all_EDGES_speed = pd.merge(MERGED_clean_EDGES, df_all_EDGES_speed, on=['u', 'v'], how='inner')
df_all_EDGES_speed = pd.DataFrame(df_all_EDGES_speed)

## get selected columns
df_all_EDGES_speed = df_all_EDGES_speed[["u", "v", "length(km)", "travel_speed"]]
## get travelled time (in seconds)
df_all_EDGES_speed['travel_time'] = ((df_all_EDGES_speed['length(km)']) / (df_all_EDGES_speed['travel_speed'])) *3600 # seconds


# make a copy
df_all_speedEDGES = df_all_EDGES_speed
# add colors based on 'time' (seconds)
vmin = min(df_all_EDGES_speed.travel_speed[df_all_EDGES_speed.travel_speed > 0])
vmax = max(df_all_EDGES_speed.travel_speed)
AVG = np.average(df_all_EDGES_speed.travel_speed)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlGn)  # scales of reds (or "coolwarm" , "bwr")
df_all_EDGES_speed['color'] = df_all_EDGES_speed['travel_speed'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_speed = df_all_EDGES_speed[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
speeds_edges_matched_route = pd.merge(df_all_EDGES_speed, gdf_all_EDGES, on=['u', 'v'],how='left')
speeds_edges_matched_route = gpd.GeoDataFrame(times_edges_matched_route)
speeds_edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)


# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
SPEED_EDGES = pd.merge(speeds_edges_matched_route, df_all_speedEDGES, on=['u', 'v'], how='inner')
# remove duplicates nodes
SPEED_EDGES.drop_duplicates(['u', 'v'], inplace=True)
SPEED_EDGES['travel_time'] = round(SPEED_EDGES['travel_time'], 1)
SPEED_EDGES['travel_speed'] = round(SPEED_EDGES['travel_speed'], 0)

SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_time':'travel time (sec)'})
SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_speed':'travelled speed (km/h)'})

df_SPEED_EDGES = pd.DataFrame(SPEED_EDGES)
merged_data = pd.merge(merged_data, df_SPEED_EDGES, on=['u', 'v',
                                                         'index', 'idtrajectory', 'idtrace', 'sequenza',
                                                         'mean_speed', 'timedate', 'totalseconds', 'TRIP_ID',
                                                         'track_ID', 'length', 'highway', 'name', 'ref',
                                                         'length(km)','travelled speed (km/h)',
                                                         'travel time (sec)'], how = 'inner')
merged_data.drop(['color', 'geometry_y'], axis=1, inplace = True)
merged_data = merged_data.rename(columns={'geometry_x': 'geometry'})
## change names to be able to write on the DB
merged_data = merged_data.rename(columns={'length(km)': 'length_km'})
merged_data = merged_data.rename(columns={'travelled speed (km/h)': 'travelled speed_km_h'})
merged_data = merged_data.rename(columns={'travel time (sec)': 'travel_time_secs'})
merged_data = merged_data.rename(columns={'frequency(%)': 'frequency'})
merged_data = gpd.GeoDataFrame(merged_data)

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################


# add colors to map
my_map = plot_graph_folium_FK(SPEED_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    SPEED_EDGES[['travel time (sec)', 'travelled speed (km/h)', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel time (sec)', 'travelled speed (km/h)', 'length(km)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

SPEED_EDGES.to_file(filename='DB_SPEED_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_travel_speed_all_EDGES_2019-04-15_May-26-2020.html")


#########################################################
### insert into the DB  #################################
#########################################################

### Connect to a DB and populate the DB  ###
connection = engine.connect()
merged_data['geom'] = merged_data['geometry'].apply(wkb_hexer)
merged_data.drop('geometry', 1, inplace=True)
merged_data.to_sql("paths_postprocess_temp", con=connection, schema="public")
connection.close()

'''
# copy temporary table to a permanent table with the right GEOMETRY datatype
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_CT')
with engine.connect() as conn, conn.begin():
    sql = """create table paths_postprocess as (select * from paths_postprocess_temp)"""
    conn.execute(sql)

# Convert the `'geom'` column back to Geometry datatype, from text
with engine.connect() as conn, conn.begin():
    print(conn)
    sql = """ALTER TABLE public.paths_postprocess
                                  ALTER COLUMN geom TYPE Geometry(LINESTRING, 4326)
                                    USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)

'''
