
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
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK

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
MONTHS.drop_duplicates(keep='first', inplace=True)
MONTHS = list(MONTHS)

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)



from datetime import datetime
now1 = datetime.now()

#### get all VIASAT data from map-matching (automobili e mezzi pesanti) on selected date
viasat_data = pd.read_sql_query('''
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
                                      /*WHERE date(mapmatching_2019.timedate) = '2019-02-25'*/
                                      WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '08'
                                      /*AND dataraw.vehtype::bigint = 2*/
                          )
                       SELECT u, v, COUNT(*)
                       FROM  data
                       GROUP BY u, v
                       ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)


## get counts ("passaggi") across each EDGE
all_counts_uv = viasat_data

# compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
max_counts = max(all_counts_uv['count'])
all_counts_uv['frequency'] = (all_counts_uv['count']/max_counts)*100
all_counts_uv['frequency'] = round(all_counts_uv['frequency'], 0)
all_counts_uv['frequency'] = all_counts_uv.frequency.astype('int')


## merge edges for congestion with the road network to get the geometry
all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)

## sort by "frequency"
all_counts_uv.sort_values('frequency', ascending=True, inplace= True)



## get only frequency >= 50%
all_counts_uv = all_counts_uv[all_counts_uv.frequency >= 35]


## make a geodataframe
all_counts_uv = gpd.GeoDataFrame(all_counts_uv)


##### PLOT ######

## rescale all data by an arbitrary number
all_counts_uv["scales"] = round(((all_counts_uv.frequency/max(all_counts_uv.frequency)) * 3) + 0.1 ,1)
## rename the field 'frequency'
all_counts_uv = all_counts_uv.rename(columns={'frequency': 'frequency(%)'})

# add colors based on 'frequency'
vmin = min(all_counts_uv.scales)
vmax = max(all_counts_uv.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
all_counts_uv['color'] = all_counts_uv['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


# add colors to map
my_map = plot_graph_folium_FK(all_counts_uv, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=3.5, edge_opacity=0.5)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    all_counts_uv[['u','v', 'scales', 'frequency(%)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':0.6
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'frequency(%)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
# my_map.save(path + "criticality_Feb_May_Aug_Nov_2019_Catania_all_vehicles.html")
# my_map.save(path + "criticality_Feb_2019_Catania_all_vehicles.html")
my_map.save(path + "criticality_Aug_2019_Catania_all_vehicles.html")



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
###### loop to generate daily maps #####################################################


# subset database with only one specific date and one specific TRACK_ID)
# for idx, row in unique_DATES.iterrows():
#     DATE = row[1].strftime("%Y-%m-%d")
#     print(DATE)
#     #################################################################################
#     ### loop over the dates  ########################################################
#     viasat_data = pd.read_sql_query('''
#                            WITH data AS(
#                            SELECT
#                               mapmatching_2019.u, mapmatching_2019.v,
#                                    mapmatching_2019.timedate, mapmatching_2019.mean_speed,
#                                    mapmatching_2019.idtrace, mapmatching_2019.sequenza,
#                                    mapmatching_2019.idtrajectory,
#                                    dataraw.speed, dataraw.vehtype
#                               FROM mapmatching_2019
#                               LEFT JOIN dataraw
#                                           ON mapmatching_2019.idtrace = dataraw.id
#                                           /*WHERE date(mapmatching_2019.timedate) = '2019-02-25'*/
#                                           WHERE date(mapmatching_2019.timedate) = %s
#                                           /*WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '02'*/
#                                           AND dataraw.vehtype::bigint = 2
#                               )
#                            SELECT u, v, COUNT(*)
#                            FROM  data
#                            GROUP BY u, v
#                            ''', conn_HAIG, params={DATE})


## group by day of the week (DOW) for each month
for idx, row in enumerate(MONTHS):
    MONTH = str(row)
    print(MONTH)
    viasat_data_all = pd.read_sql_query('''
                            select u, v,
                            To_Char(timedate, 'DAY') as dow,
                            COUNT(*)
                            from mapmatching_2019 
                            WHERE EXTRACT(MONTH FROM timedate) = %s
                            group by u,v,dow
                             ''', conn_HAIG, params={MONTH})

    ## get unique list of day
    DAYS = list(viasat_data_all.dow.unique())
    for idx, day in enumerate(DAYS):
        DAY = str(day)
        print(DAY)
        ## filter by day
        viasat_data = viasat_data_all[viasat_data_all.dow == DAY]

        ## get counts ("passaggi") across each EDGE
        all_counts_uv = viasat_data

        # compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
        max_counts = max(all_counts_uv['count'])
        all_counts_uv['frequency'] = (all_counts_uv['count'] / max_counts) * 100
        all_counts_uv['frequency'] = round(all_counts_uv['frequency'], 0)
        all_counts_uv['frequency'] = all_counts_uv.frequency.astype('int')

        ## merge edges for congestion with the road network to get the geometry
        all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
        all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)

        ## sort by "frequency"
        all_counts_uv.sort_values('frequency', ascending=True, inplace=True)

        ## get only frequency >= 50%
        all_counts_uv = all_counts_uv[all_counts_uv.frequency >= 35]

        ## rescale all data by an arbitrary number
        all_counts_uv["scales"] = round(((all_counts_uv.frequency / max(all_counts_uv.frequency)) * 3) + 0.1, 1)
        ## rename the field 'frequency'
        all_counts_uv = all_counts_uv.rename(columns={'frequency': 'frequency(%)'})

        # add colors based on 'frequency'
        vmin = min(all_counts_uv.scales)
        vmax = max(all_counts_uv.scales)
        # Try to map values to colors in hex
        norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
        mapper = plt.cm.ScalarMappable(norm=norm,
                                       cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
        all_counts_uv['color'] = all_counts_uv['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

        ## make a geodataframe
        all_counts_uv = gpd.GeoDataFrame(all_counts_uv)
        ## setup projection (CRS)
        all_counts_uv.crs = 4326

        ## make a RECTANGLE to clip the viasat_data
        from shapely.geometry import Polygon

        lat_point_list = [37.625, 37.625, 37.426, 37.426, 37.625]
        lon_point_list = [14.86, 15.25, 15.25, 14.86, 14.86]

        polygon_geom = Polygon(zip(lon_point_list, lat_point_list))
        crs = {'init': 'epsg:4326'}
        polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])
        ## check projection (CRS)
        ## https://geopandas.org/projections.html
        # polygon.crs

        # polygon.plot()
        ## clip the data with the RECTANGLE
        all_counts_uv = gpd.clip(all_counts_uv, polygon)

        ### plot gedataframe with colors -----###
        ## add background map ###
        gdf = all_counts_uv
        import contextily as ctx

        # minx, miny, maxx, maxy = gdf.geometry.total_bounds
        # polygon.geometry.total_bounds

        ## reproject with mercator coordinates (this is the coordinate system of the basemap)
        gdf = gdf.to_crs(epsg=3857)
        # Plot the data within the RECTANGULAR extensions
        fig, ax = plt.subplots(figsize=(10, 10))
        polygon = polygon.to_crs(epsg=3857)
        polygon.plot(alpha=0,
                     color="white",
                     edgecolor="black",
                     ax=ax)
        gdf.plot(ax=ax, alpha=1, color=gdf['color'])
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)
        ax.set_axis_off()
        # plt.axis('equal')
        plt.show()

        # 'OpenStreetMap.Mapnik',
        # 'OpenTopoMap',
        #  'Stamen.Toner',
        #  'Stamen.TonerLite',
        #  'Stamen.Terrain',
        #  'Stamen.TerrainBackground',
        #  'Stamen.Watercolor',
        #  'NASAGIBS.ViirsEarthAtNight2012',
        #  'CartoDB.Positron',
        # 'CartoDB.Voyager'

        ### get full month name (from number to month)
        import calendar
        MONTH = int(MONTH)
        MONTH_name = calendar.month_name[MONTH]
        path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
        plt.savefig(path + 'criticality_' + DAY + '_' +
                    MONTH_name + '2019_Catania_all_vehicles.png')
        plt.close()


