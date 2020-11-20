
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


# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)

from datetime import datetime
now1 = datetime.now()

#### get all VIASAT data from map-matching (automobili e mezzi pesanti) on selected date
viasat_data = pd.read_sql_query('''
                       SELECT  
                          mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.speed, dataraw.vehtype
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                                      ON mapmatching_2019.idtrace = dataraw.id  
                                      /*WHERE date(mapmatching_2019.timedate) = '2019-02-25' AND*/
                                      /* WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '02'*/
                                      /*AND dataraw.vehtype::bigint = 2*/
                                      ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)



## get counts ("passaggi") across each EDGE
all_data = viasat_data[['u','v', 'timedate']]
## resample 'timedate' to 15 minutes interval
# df_flux = all_data.resample(rule='15Min', on='timedate').size()
# df_flux = all_data.groupby(['u','v'],sort=False).resample(rule='15Min', on='timedate').size().reset_index().rename(columns={0:'counts'})

### get "speed"
speed_uv = viasat_data[['u', 'v', 'timedate', 'speed']]
speed_uv['hour'] = speed_uv['timedate'].apply(lambda x: x.hour)
# speed_uv = speed_uv.sort_values(['u', 'v'])
# speed_uv = speed_uv[0:1000]
# speed_uv.to_csv('speed_uv.csv')

del viasat_data

## get a new field for the HOUR
all_data['hour'] = all_data['timedate'].apply(lambda x: x.hour)
# date (day)
all_data['date'] = all_data['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
all_data = all_data[['u','v', 'hour', 'date']]

## get the counts for each edge (u,v) pair and for each hour (this is a FLUX)
all_counts_uv = all_data.groupby(['u','v', 'hour', 'date'], sort=False).size().reset_index().rename(columns={0:'FLUX'})
# get the mean by hour
all_counts_uv = all_counts_uv.groupby(['u', 'v', 'hour'], sort=False).mean().reset_index()  ## FLUX
# all_counts_uv = all_counts_uv.sort_values(['u', 'v'])¶
## check which are the hours with the highest FLUX....
# all_counts_uv['hour'].hist()

## make a summary stat plot
all_counts_hourly = all_counts_uv.groupby(['hour'], sort=False).sum().reset_index()
all_counts_hourly = all_counts_hourly.sort_values('hour', ascending=True)
all_counts_hourly.plot.bar(x = 'hour', y = 'FLUX')


## get hour with maximum FLUX for each edge (u.v) pair (PEAK HOUR, ORA di PUNTA)
### https://stackoverflow.com/questions/15705630/get-the-rows-which-have-the-max-count-in-groups-using-groupby
max_FLUX_uv = all_counts_uv.sort_values('FLUX', ascending=False).drop_duplicates(['u','v'])
## to find the FLUX at "rete carica" consider the lower flux
min_FLUX_uv = all_counts_uv.sort_values('FLUX', ascending=True).drop_duplicates(['u','v'])
## plot distribution
# max_FLUX_uv['FLUX'].hist()
## check which are the hours with the highest FLUX....
# max_FLUX_uv['hour'].hist()

fig = plt.hist(max_FLUX_uv['FLUX'])
plt.title('Distribution of Fluxes at Peak Hours')
plt.xlabel("max flux (vehicles/hour)")
plt.ylabel("Frequency")
plt.savefig("Peak_Hour_FLUX.png")
plt.close()


fig = plt.hist(max_FLUX_uv['hour'], bins= 24)   # bins = 24, color = 'gold'
plt.title('Distribution of Peak Hours')
plt.xlabel("hour")
plt.ylabel("Frequency")
plt.savefig("Peak_hours.png")
plt.close()


### get AVERAGE of travelled travelled "speed" for each edge and each hour
speed_uv = (speed_uv.groupby(['u', 'v', 'hour']).mean()).reset_index()
speed_uv['speed'] = speed_uv.speed.astype('int')
## check SPEED distribution
# speed_uv['speed'].hist()


## max value
# all_counts_uv[all_counts_uv.counts ==max(all_counts_uv.counts)]

## get speed at the PEAK HOUR (merge "speed_uv" with "max_FLUX_uv")
flux_PEAK_HOUR = pd.merge(max_FLUX_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
flux_PEAK_HOUR = flux_PEAK_HOUR.rename(columns={'speed': 'speed_PHF'})
## check distributiion of SPEEDS
# flux_PEAK_HOUR['speed_PHF'].hist()

fig = plt.hist(flux_PEAK_HOUR['speed_PHF'])
plt.title('Distribution of speeds @ Peak Hours')
plt.xlabel("max flux (vehicles/hour)")
plt.ylabel("Frequency")
plt.savefig("speed_at_Peak_Hour.png")
plt.close()


## get a sottorete by filtering the FLUX (only consider higher fluxes).......
## get only fluxes >= 10 vehi/hour
flux_PEAK_HOUR = flux_PEAK_HOUR[flux_PEAK_HOUR.FLUX >= 10]
## check distributiion of SPEEDS
# flux_PEAK_HOUR['speed_PHF'].hist()
# flux_PEAK_HOUR['FLUX'].hist()
# flux_PEAK_HOUR['hour'].hist()

## get speed at RETE SCARICA (merge "speed_uv" with "min_FLUX_uv")
flux_RETE_SCARICA = pd.merge(min_FLUX_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
flux_RETE_SCARICA = flux_RETE_SCARICA.rename(columns={'speed': 'speed_RETE_SCARICA'})
# flux_RETE_SCARICA['speed_RETE_SCARICA'].hist()

## find edges (u,v) CONGESTED ("ratio" between SPEED at PEAK HOUR and SPEED at RETE SCARICA)
speed_PHF_and_SCARICA = pd.merge(flux_PEAK_HOUR[['u','v', 'speed_PHF']], flux_RETE_SCARICA[['u','v', 'speed_RETE_SCARICA']], on=['u', 'v'], how='left')
## remove rows with 0 values
speed_PHF_and_SCARICA = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.speed_PHF != 0]
speed_PHF_and_SCARICA = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.speed_RETE_SCARICA != 0]
speed_PHF_and_SCARICA['congestion_index'] = speed_PHF_and_SCARICA.speed_PHF / speed_PHF_and_SCARICA.speed_RETE_SCARICA
speed_PHF_and_SCARICA['congestion_index'] = abs(1 - speed_PHF_and_SCARICA['congestion_index'])


## speed_rete_scarica should be bigger than speed at PHF
### sort congested index to find the highest values
speed_PHF_and_SCARICA = speed_PHF_and_SCARICA.sort_values('congestion_index', ascending=False)
### check the length of the edge
# speed_PHF_and_SCARICA = pd.merge(speed_PHF_and_SCARICA, gdf_edges[['u', 'v', 'length']], on=['u', 'v'], how='left')

## get only congestion index < 1
speed_PHF_and_SCARICA = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.congestion_index <= 1]

## merge edges for congestion with the road network to get the geometry
speed_PHF_and_SCARICA = pd.merge(speed_PHF_and_SCARICA, gdf_edges, on=['u', 'v'], how='left')
speed_PHF_and_SCARICA.drop_duplicates(['u', 'v'], inplace=True)

## make unique list of "u" and "v"
all_uv = list(speed_PHF_and_SCARICA.u.unique())  + list(speed_PHF_and_SCARICA.v.unique())
all_uv = list(dict.fromkeys(all_uv))

## delete edges with 'length' larger than 15 meters
## check if each node is in the u or v column
## we only delete isolate edges and not interconnected edges
for idx, node in enumerate(all_uv):
    # print(idx, node)
    U = pd.Series(list(speed_PHF_and_SCARICA.u))
    V = pd.Series(list(speed_PHF_and_SCARICA.v))
    if not (node in U.tolist()) and (node in V.tolist()):
        print("\nThis node exists only in one edge==============================")
        ### find row to delete (with 'length'> 28 meters
        if (speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.values == node]['length'].iloc[0] <= 28):
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            row_to_delete = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.values == node].index
            speed_PHF_and_SCARICA = speed_PHF_and_SCARICA.drop(row_to_delete)


## sort by "length"
speed_PHF_and_SCARICA.sort_values('length', ascending=True, inplace= True)
## filter out all road shorter than 11 meters
# speed_PHF_and_SCARICA = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.length > 15]

## normalize "congestion index"
speed_PHF_and_SCARICA["congestion"] = round(((speed_PHF_and_SCARICA["congestion_index"]/max(speed_PHF_and_SCARICA["congestion_index"]))*1) +0, 2)

## get only congestion index > 0.5
# speed_PHF_and_SCARICA = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.congestion_index >= 0.5]


## make a geodataframe
speed_PHF_and_SCARICA = gpd.GeoDataFrame(speed_PHF_and_SCARICA)

## save data
# speed_PHF_and_SCARICA.to_file(filename='congestion_index_Feb_May_Agu_Nov_2019.geojson', driver='GeoJSON')
# speed_PHF_and_SCARICA.plot()


##### PLOT ######

## rescale all data by an arbitrary number
speed_PHF_and_SCARICA["scales"] = round(((speed_PHF_and_SCARICA.congestion_index/max(speed_PHF_and_SCARICA.congestion_index)) * 3) + 0.1 ,1)

# add colors based on 'congestion_index'
vmin = min(speed_PHF_and_SCARICA.scales)
vmax = max(speed_PHF_and_SCARICA.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
speed_PHF_and_SCARICA['color'] = speed_PHF_and_SCARICA['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

# get only congestion index < 1
# speed_PHF_and_SCARICA = speed_PHF_and_SCARICA[speed_PHF_and_SCARICA.congestion_index < 1]
## normalize "congestion index"
# speed_PHF_and_SCARICA["congestion"] = round(speed_PHF_and_SCARICA["congestion_index"]/max(speed_PHF_and_SCARICA["congestion_index"]),2)


# add colors to map
my_map = plot_graph_folium_FK(speed_PHF_and_SCARICA, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=4, edge_opacity=0.5)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    speed_PHF_and_SCARICA[['u','v', 'scales', 'congestion', 'length', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':0.6
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'congestion', 'length']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
# my_map.save(path + "congestion_Feb_May_Agu_Nov_2019_Catania_all_vehicles.html")
# my_map.save(path + "congestion_August_2019_Catania_all_vehicles.html")
# my_map.save(path + "congestion_February_2019_Catania_all_vehicles.html")
my_map.save(path + "congestion_Feb_May_Agu_Nov_2019_Catania_all_vehicles.html")
