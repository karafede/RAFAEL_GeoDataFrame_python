
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

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)

################################################################################
################################################################################
################################################################################


## divide data by directions througn the TANGENZIALE DI Catania
## (294034837, 6754556102) --> Taormina
## (476455543, 4064451884)  --> Catania

from datetime import datetime
now1 = datetime.now()

df =  pd.read_sql_query('''
                     WITH path AS(SELECT 
                            split_part("TRIP_ID"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (294034837, 6754556102),
                           (476455543, 4064451884))
                                 /*LIMIT 1000*/ )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id   
                            WHERE date(path.timedate) = '2019-11-24'
                            /*WHERE EXTRACT(MONTH FROM path.timedate) = '02'*/
                                 ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)

## get all the IDTERM  vehicles passing through the TANGENZIALE OVEST CATANIA
df_TAORMINA = df[(df['u'] == 294034837) & (df['v'] == 6754556102) ]   ## towards Taormina
df_CATANIA = df[(df['u'] == 476455543) & (df['v'] == 4064451884) ]    ## towards Catania

df_TAORMINA.drop_duplicates(['idterm'], inplace=True)
df_CATANIA.drop_duplicates(['idterm'], inplace=True)

# ## make a list of all IDterminals for the direction of Salerno and Avellino
all_idterms_TAORMINA = list(df_TAORMINA.idterm.unique())
all_idterms_CATANIA = list(df_CATANIA.idterm.unique())

del df

###############################################################################
## get MAP-MATCHING data from DB for a specific day of the month (2019 ########
###############################################################################

from datetime import datetime
now1 = datetime.now()

#### get all VIASAT data from map-matching (automobili e mezzi pesanti) on selected date
viasat_data = pd.read_sql_query('''
                    SELECT  
                       mapmatching_2019.u, mapmatching_2019.v,
                            mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                            mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                            mapmatching_2019.idtrajectory,
                            dataraw.idterm, dataraw.vehtype,
                            dataraw.speed
                       FROM mapmatching_2019
                       LEFT JOIN dataraw 
                                   ON mapmatching_2019.idtrace = dataraw.id  
                                   WHERE date(mapmatching_2019.timedate) = '2019-11-24'
                                   /*WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '02'*/
                                   /*AND dataraw.vehtype::bigint = 1*/
                    ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)



##########################################################
##### Get counts only for trips on SELECTED EDGES ########

### get all data only for the vehicles in the list
all_TAORMINA = viasat_data[viasat_data.idterm.isin(all_idterms_TAORMINA)]
all_CATANIA = viasat_data[viasat_data.idterm.isin(all_idterms_CATANIA)]

## get data with "sequenza" STARTING from the chosen nodes on the TANGENZIALE OVEST CATANIA for each idterm
pnt_sequenza_TAORMNINA = all_TAORMINA[(all_TAORMINA['u'] == 294034837) & (all_TAORMINA['v'] == 6754556102) ]
pnt_sequenza_CATANIA = all_CATANIA[(all_CATANIA['u'] == 476455543) & (all_CATANIA['v'] == 4064451884) ]

# del viasat_data


### initialize an empty dataframe
partial_TAORMINA = pd.DataFrame([])
partial_CATANIA = pd.DataFrame([])

##################
### TAORMINA #####
##################
for idx, idterm in enumerate(all_idterms_TAORMINA):
    print(idterm)
    idterm = int(idterm)
    ### get starting "sequenza"
    sequenza = pnt_sequenza_TAORMNINA[pnt_sequenza_TAORMNINA.idterm == idterm]['sequenza'].iloc[0]
    sub = all_TAORMINA[(all_TAORMINA.idterm == idterm) & (all_TAORMINA.sequenza >= sequenza)]
    partial_TAORMINA = partial_TAORMINA.append(sub)



##################
### CATANIA ######
##################
for idx, idterm in enumerate(all_idterms_CATANIA):
    print(idterm)
    idterm = int(idterm)
    # get starting "sequenza"
    sequenza = pnt_sequenza_CATANIA[pnt_sequenza_CATANIA.idterm == idterm]['sequenza'].iloc[0]
    sub = all_CATANIA[(all_CATANIA.idterm == idterm) & (all_CATANIA.sequenza >= sequenza)]
    partial_CATANIA = partial_CATANIA.append(sub)


####################################################
### further filtering with idtrajectory (by trip) ##
####################################################

partial_TAORMINA_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_TAORMINA):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_TAORMNINA[pnt_sequenza_TAORMNINA.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_TAORMINA[(partial_TAORMINA.idterm == idterm) & (partial_TAORMINA.idtrajectory == idtrajectory)]
    partial_TAORMINA_bis = partial_TAORMINA_bis.append(sub)


partial_CATANIA_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_CATANIA):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_CATANIA[pnt_sequenza_CATANIA.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_CATANIA[(partial_CATANIA.idterm == idterm) & (partial_CATANIA.idtrajectory == idtrajectory)]
    partial_CATANIA_bis = partial_CATANIA_bis.append(sub)

## rename partial data
partial_TAORMINA = partial_TAORMINA_bis
partial_CATANIA = partial_CATANIA_bis

## only select (u,v) from partial data
TAORMINA = partial_TAORMINA[['u','v']]
CATANIA = partial_CATANIA[['u','v']]


#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

### get counts for selected edges ###
counts_uv_TAORMINA = TAORMINA.groupby(TAORMINA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_CATANIA = CATANIA.groupby(CATANIA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

### get counts for all edges ########
# all_data = viasat_data[['u','v']]
# all_counts_uv = all_data.groupby(all_data.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

########################################################
##### build the map ####################################

## merge counts with OSM (Open Street Map) edges
counts_uv_TAORMINA = pd.merge(counts_uv_TAORMINA, gdf_edges, on=['u', 'v'], how='left')
counts_uv_TAORMINA = gpd.GeoDataFrame(counts_uv_TAORMINA)
counts_uv_TAORMINA.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_TAORMINA.plot()

counts_uv_CATANIA = pd.merge(counts_uv_CATANIA, gdf_edges, on=['u', 'v'], how='left')
counts_uv_CATANIA = gpd.GeoDataFrame(counts_uv_CATANIA)
counts_uv_CATANIA.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_CATANIA.plot()

# all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
# all_counts_uv = gpd.GeoDataFrame(all_counts_uv)
# all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)
# all_counts_uv.plot()


# counts_uv_TAORMINA["scales"] = (counts_uv_TAORMINA.counts/max(counts_uv_TAORMINA.counts)) * 7
# counts_uv_CATANIA["scales"] = (counts_uv_CATANIA.counts/max(counts_uv_CATANIA.counts)) * 7

# all_counts_uv["scales"] = (all_counts_uv.counts/max(all_counts_uv.counts)) * 7

# ################################################################################
# # create basemap CATANIA
# ave_LAT = 37.510284
# ave_LON = 15.092042
# my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
# #################################################################################
#
#
# ##### MAP for TAORMINA ######
#
# folium.GeoJson(
# counts_uv_TAORMINA[['u','v', 'counts', 'scales', 'geometry']].to_json(),
#     style_function=lambda x: {
#         'fillColor': 'red',
#         'color': 'red',
#         'weight':  x['properties']['scales'],
#         'fillOpacity': 1,
#         },
# highlight_function=lambda x: {'weight':3,
#         'color':'blue',
#         'fillOpacity':1
#     },
#     # fields to show
#     tooltip=folium.features.GeoJsonTooltip(
#         fields=['u', 'v', 'counts']),
#     ).add_to(my_map)
#
#
# ##### MAP for CATANIA ######
#
# folium.GeoJson(
# counts_uv_CATANIA[['u','v', 'counts', 'scales', 'geometry']].to_json(),
#     style_function=lambda x: {
#         'fillColor': 'blue',
#         'color': 'blue',
#         'weight':  x['properties']['scales'],
#         'fillOpacity': 1,
#         },
# highlight_function=lambda x: {'weight':3,
#         'color':'blue',
#         'fillOpacity':1
#     },
#     # fields to show
#     tooltip=folium.features.GeoJsonTooltip(
#         fields=['u', 'v', 'counts']),
#     ).add_to(my_map)

# my_map.save("traffic_auto_partial_counts_TAORMINA_CATANIA_24_NOVEMBER_2019.html")

########################################
########################################
########################################
# make a colored map  ##################
########################################
########################################
########################################

#######################################
## --- TAORMINA --- ###################

## rescale all data by an arbitrary number
# counts_uv_TAORMINA = counts_uv_TAORMINA[counts_uv_TAORMINA.counts > 10]
counts_uv_TAORMINA["scales"] = round(((counts_uv_TAORMINA.counts/max(counts_uv_TAORMINA.counts)) * 1) ,1)
# add colors based on 'counts'
vmin = min(counts_uv_TAORMINA.scales)
vmax = max(counts_uv_TAORMINA.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
counts_uv_TAORMINA['color'] = counts_uv_TAORMINA['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
counts_uv_TAORMINA['counts'] = round(counts_uv_TAORMINA['counts'], 1)

## Normalize to 1
counts_uv_TAORMINA["load (%)"] = round(counts_uv_TAORMINA["scales"]/max(counts_uv_TAORMINA["scales"]),2)*100
counts_uv_TAORMINA = counts_uv_TAORMINA[counts_uv_TAORMINA["load (%)"] > 5]


my_map = plot_graph_folium_FK(counts_uv_TAORMINA, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=3, edge_opacity=0.6)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    counts_uv_TAORMINA[['u','v', 'scales', 'load (%)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load (%)']
    ),
).add_to(my_map)


folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
my_map.save(path + "counts_TAORMINA_24_NOVEMBER_2019.html")

# del my_map

#######################################
## --- CATANIA --------- ##############

## rescale all data by an arbitrary number
# counts_uv_CATANIA = counts_uv_CATANIA[counts_uv_CATANIA.counts > 10]
counts_uv_CATANIA["scales"] = round(((counts_uv_CATANIA.counts/max(counts_uv_CATANIA.counts)) * 1)  ,1)
# add colors based on 'counts'
vmin = min(counts_uv_CATANIA.scales)
vmax = max(counts_uv_CATANIA.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
counts_uv_CATANIA['color'] = counts_uv_CATANIA['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
counts_uv_CATANIA['counts'] = round(counts_uv_CATANIA['counts'], 1)

## Normalize to 1
counts_uv_CATANIA["load (%)"] = round(counts_uv_CATANIA["scales"]/max(counts_uv_CATANIA["scales"]),2)*100
counts_uv_CATANIA = counts_uv_CATANIA[counts_uv_CATANIA["load (%)"] > 5]


my_map = plot_graph_folium_FK(counts_uv_CATANIA, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=3, edge_opacity=0.5)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    counts_uv_CATANIA[['u','v', 'scales', 'load (%)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load (%)']
    ),
).add_to(my_map)

my_map.save(path + "counts_CATANIA_24_NOVEMBER_2019.html")


################################################################################
################################################################################
################################################################################
################################################################################


### MAKE a map with percentace (%) flows from TAORMINA and to CATANIA
Catania_partial = pd.DataFrame(counts_uv_CATANIA)
Taormina_partial = pd.DataFrame(counts_uv_TAORMINA)

## mormalize separately and get colors
Catania_partial["scales"] = round(((Catania_partial.counts/max(Catania_partial.counts)))  ,1)
# add colors based on 'counts'
vmin = min(Catania_partial.scales)
vmax = max(Catania_partial.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
Catania_partial['color'] = Catania_partial['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
## Normalize to 1
Catania_partial["load (%)"] = round(Catania_partial["scales"]/max(Catania_partial["scales"]),2)*100


## mormalize separately and get colors
Taormina_partial["scales"] = round(((Taormina_partial.counts/max(Taormina_partial.counts)))  ,1)
# add colors based on 'counts'
vmin = min(Taormina_partial.scales)
vmax = max(Taormina_partial.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
Taormina_partial['color'] = Taormina_partial['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
## Normalize to 1
Taormina_partial["load (%)"] = round(Taormina_partial["scales"]/max(Taormina_partial["scales"]),2)*100


## merge CATANIA with TAORMINA counts and keep the max count....
Taormina_Catania = pd.concat([Catania_partial, Taormina_partial]).groupby(['u','v'], as_index=False)['load (%)'].max()
Taormina_Catania = pd.merge(Taormina_Catania, gdf_edges, on=['u', 'v'], how='left')
Taormina_Catania = gpd.GeoDataFrame(Taormina_Catania)
# Taormina_Catania.plot()
Taormina_Catania = Taormina_Catania[Taormina_Catania["load (%)"] > 5]

## get colors
Taormina_Catania["scales"] = round(((Taormina_Catania["load (%)"]/max(Taormina_Catania["load (%)"])) )  ,1)
# # add colors based on 'counts'
vmin = min(Taormina_Catania.scales)
vmax = max(Taormina_Catania.scales)
# # Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
Taormina_Catania['color'] = Taormina_Catania['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


my_map = plot_graph_folium_FK(Taormina_Catania, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=3, edge_opacity=0.5)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    Taormina_Catania[['u','v', 'load (%)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load (%)']
    ),
).add_to(my_map)

my_map.save(path + "counts_TAORMINA_and_CATANIA_24_NOVEMBER_2019.html")


###################################################
#### ---- ISOCHRONES ----------------##############
###################################################

## get the counts for each edge (u,v) pair and for each "TRAJECTORY" (TRIP)
# get the mean by u,v and "idtrajectory" of the SPEED
speed_uv = partial_TAORMINA[['u', 'v', 'idtrajectory', 'mean_speed']]
speed_uv = (speed_uv.groupby(['u', 'v', 'idtrajectory']).mean()).reset_index()
speed_uv = pd.merge(speed_uv, gdf_edges[['u','v','length']], on=['u', 'v'], how='left')
## calculate travel times on each edge for edge within each "idtrajectory"
speed_uv['travel_time'] = ((speed_uv.length/1000)/(speed_uv.mean_speed))*60  ## in minutes

## sum all travel times all nodes fo the same "idtrajectory"
traveltime_trajectory = (speed_uv[['idtrajectory','travel_time']].groupby(['idtrajectory']).sum()).reset_index()
## check which are the trajectories with the highest travel time....
traveltime_trajectory['travel_time'].hist()
traveltime_trajectory = traveltime_trajectory[traveltime_trajectory.travel_time < 120]
traveltime_trajectory['travel_time'].hist()


## merge with 'u' and 'v'
traveltime_trajectory = pd.merge(traveltime_trajectory, speed_uv[['u','v','idtrajectory']], on=['idtrajectory'], how='left')
traveltime_trajectory['travel_time'] = traveltime_trajectory['travel_time'].astype(np.int64)
## get geometry from the OSM network
traveltime_trajectory = pd.merge(traveltime_trajectory, gdf_edges, on=['u', 'v'], how='left')
traveltime_trajectory = gpd.GeoDataFrame(traveltime_trajectory)
# traveltime_trajectory.plot()


#######################################
## --- TAORMINA --- ###################

## make a categorization for different range of travel times
# [0,10] [10,18] [18,26] [30,40]......


# counts_uv_TAORMINA = counts_uv_TAORMINA[counts_uv_TAORMINA.counts > 11]
traveltime_trajectory["scales"] = round(((traveltime_trajectory.travel_time/max(traveltime_trajectory.travel_time)) * 1) ,1)
# add colors based on 'counts'
vmin = min(traveltime_trajectory.scales)
vmax = max(traveltime_trajectory.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
traveltime_trajectory['color'] = traveltime_trajectory['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
# counts_uv_TAORMINA['counts'] = round(counts_uv_TAORMINA['counts'], 1)


my_map = plot_graph_folium_FK(traveltime_trajectory, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=3, edge_opacity=0.5)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    traveltime_trajectory[['u','v', 'scales', 'travel_time', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':0.8
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'travel_time']
    ),
).add_to(my_map)


folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
my_map.save(path + "travel_time_TAORMINA_24_NOVEMBER_2019.html")


###################################################
###################################################
###################################################







##################
########################
#################################
###########################################
####################################################
##########################################################
##################################################################
########################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

