
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
## (488537136, 1767590558) --> Misterbianco
## (637681763, 370190911) --> AciCastello
## (293556912, 708283295) --> SS121A
## (293558366, 708685571) --> SS121B

from datetime import datetime
now1 = datetime.now()

df =  pd.read_sql_query('''
                     WITH path AS(SELECT 
                            split_part("TRIP_ID"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (294034837, 6754556102),
                           (476455543, 4064451884), (488537136, 1767590558),
                           (637681763, 370190911), (293556912, 708283295),
                            (293558366, 708685571))
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
df_Misterbianco = df[(df['u'] == 488537136) & (df['v'] == 1767590558) ]    ## towards Misterbianco
df_Acicastello = df[(df['u'] == 637681763) & (df['v'] == 370190911) ]    ## towards AciCastello
df_SS121A = df[(df['u'] == 293556912) & (df['v'] == 708283295) ]    ## around Misterbianco area
df_SS121B = df[(df['u'] == 293558366) & (df['v'] == 708685571) ]    ## around Misterbianco area

df_TAORMINA.drop_duplicates(['idterm'], inplace=True)
df_CATANIA.drop_duplicates(['idterm'], inplace=True)
df_Misterbianco.drop_duplicates(['idterm'], inplace=True)
df_SS121A.drop_duplicates(['idterm'], inplace=True)
df_SS121B.drop_duplicates(['idterm'], inplace=True)


# ## make a list of all IDterminals for the direction of Salerno and Avellino
all_idterms_TAORMINA = list(df_TAORMINA.idterm.unique())
all_idterms_CATANIA = list(df_CATANIA.idterm.unique())
all_idterms_Misterbianco = list(df_Misterbianco.idterm.unique())
all_idterms_Acicastello = list(df_Acicastello.idterm.unique())
all_idterms_SS121A = list(df_SS121A.idterm.unique())
all_idterms_SS121B = list(df_SS121B.idterm.unique())

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
all_Misterbianco = viasat_data[viasat_data.idterm.isin(all_idterms_Misterbianco)]
all_Acicastello = viasat_data[viasat_data.idterm.isin(all_idterms_Acicastello)]
all_SS121A = viasat_data[viasat_data.idterm.isin(all_idterms_SS121A)]
all_SS121B = viasat_data[viasat_data.idterm.isin(all_idterms_SS121B)]

## get data with "sequenza" STARTING from the chosen nodes on the TANGENZIALE OVEST CATANIA for each idterm
pnt_sequenza_TAORMNINA = all_TAORMINA[(all_TAORMINA['u'] == 294034837) & (all_TAORMINA['v'] == 6754556102) ]
pnt_sequenza_CATANIA = all_CATANIA[(all_CATANIA['u'] == 476455543) & (all_CATANIA['v'] == 4064451884) ]
pnt_sequenza_Misterbianco = all_Misterbianco[(all_Misterbianco['u'] == 488537136) & (all_Misterbianco['v'] == 1767590558) ]
pnt_sequenza_Acicastello = all_Acicastello[(all_Acicastello['u'] == 637681763) & (all_Acicastello['v'] == 370190911) ]
pnt_sequenza_SS121A = all_SS121A[(all_SS121A['u'] == 293556912) & (all_SS121A['v'] == 708283295) ]
pnt_sequenza_SS121B = all_SS121B[(all_SS121B['u'] == 293558366) & (all_SS121B['v'] == 708685571) ]

# del viasat_data

### initialize an empty dataframe
partial_TAORMINA = pd.DataFrame([])
partial_CATANIA = pd.DataFrame([])
partial_Misterbianco = pd.DataFrame([])
partial_Acicastello = pd.DataFrame([])
partial_SS121A = pd.DataFrame([])
partial_SS121B = pd.DataFrame([])

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



#######################
### Misterbianco ######
#######################
for idx, idterm in enumerate(all_idterms_Misterbianco):
    print(idterm)
    idterm = int(idterm)
    ### get starting "sequenza"
    sequenza = pnt_sequenza_Misterbianco[pnt_sequenza_Misterbianco.idterm == idterm]['sequenza'].iloc[0]
    sub = all_Misterbianco[(all_Misterbianco.idterm == idterm) & (all_Misterbianco.sequenza >= sequenza)]
    partial_Misterbianco = partial_Misterbianco.append(sub)


#######################
### Acicastello #######
#######################
for idx, idterm in enumerate(all_idterms_Acicastello):
    print(idterm)
    idterm = int(idterm)
    ### get starting "sequenza"
    sequenza = pnt_sequenza_Acicastello[pnt_sequenza_Acicastello.idterm == idterm]['sequenza'].iloc[0]
    sub = all_Acicastello[(all_Acicastello.idterm == idterm) & (all_Acicastello.sequenza >= sequenza)]
    partial_Acicastello = partial_Acicastello.append(sub)


##################
### SS121A #######
##################
for idx, idterm in enumerate(all_idterms_SS121A):
    print(idterm)
    idterm = int(idterm)
    ### get starting "sequenza"
    sequenza = pnt_sequenza_SS121A[pnt_sequenza_SS121A.idterm == idterm]['sequenza'].iloc[0]
    sub = all_SS121A[(all_SS121A.idterm == idterm) & (all_SS121A.sequenza >= sequenza)]
    partial_SS121A = partial_SS121A.append(sub)


##################
### SS121B #######
##################
for idx, idterm in enumerate(all_idterms_SS121B):
    print(idterm)
    idterm = int(idterm)
    ### get starting "sequenza"
    sequenza = pnt_sequenza_SS121B[pnt_sequenza_SS121B.idterm == idterm]['sequenza'].iloc[0]
    sub = all_SS121B[(all_SS121B.idterm == idterm) & (all_SS121B.sequenza >= sequenza)]
    partial_SS121B = partial_SS121B.append(sub)



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



partial_Misterbianco_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_Misterbianco):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_Misterbianco[pnt_sequenza_Misterbianco.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_Misterbianco[(partial_Misterbianco.idterm == idterm) & (partial_Misterbianco.idtrajectory == idtrajectory)]
    partial_Misterbianco_bis = partial_Misterbianco_bis.append(sub)


partial_Acicastello_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_Acicastello):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_Acicastello[pnt_sequenza_Acicastello.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_Acicastello[(partial_Acicastello.idterm == idterm) & (partial_Acicastello.idtrajectory == idtrajectory)]
    partial_Acicastello_bis = partial_Acicastello_bis.append(sub)



partial_SS121A_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_SS121A):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_SS121A[pnt_sequenza_SS121A.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_SS121A[(partial_SS121A.idterm == idterm) & (partial_SS121A.idtrajectory == idtrajectory)]
    partial_SS121A_bis = partial_SS121A_bis.append(sub)


partial_SS121B_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_SS121B):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_SS121B[pnt_sequenza_SS121B.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_SS121B[(partial_SS121B.idterm == idterm) & (partial_SS121B.idtrajectory == idtrajectory)]
    partial_SS121B_bis = partial_SS121B_bis.append(sub)


## rename partial data
partial_TAORMINA = partial_TAORMINA_bis
partial_CATANIA = partial_CATANIA_bis
partial_Misterbianco = partial_Misterbianco_bis
partial_Acicastello = partial_Acicastello_bis
partial_SS121A = partial_SS121A_bis
partial_SS121B = partial_SS121B_bis

## only select (u,v) from partial data
TAORMINA = partial_TAORMINA[['u','v']]
CATANIA = partial_CATANIA[['u','v']]
MISTERBIANCO = partial_Misterbianco[['u','v']]
ACICASTELLO = partial_Acicastello[['u','v']]
SS121A = partial_SS121A[['u','v']]
SS121B = partial_SS121B[['u','v']]

#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

### get counts for selected edges ###
counts_uv_TAORMINA = TAORMINA.groupby(TAORMINA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_CATANIA = CATANIA.groupby(CATANIA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_Misterbianco = MISTERBIANCO.groupby(MISTERBIANCO.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_Acicastello = ACICASTELLO.groupby(ACICASTELLO.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_SS121A = SS121A.groupby(SS121A.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_SS121B = SS121B.groupby(SS121B.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})


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


counts_uv_Misterbianco = pd.merge(counts_uv_Misterbianco, gdf_edges, on=['u', 'v'], how='left')
counts_uv_Misterbianco = gpd.GeoDataFrame(counts_uv_Misterbianco)
counts_uv_Misterbianco.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_Misterbianco.plot()

counts_uv_Acicastello = pd.merge(counts_uv_Acicastello, gdf_edges, on=['u', 'v'], how='left')
counts_uv_Acicastello = gpd.GeoDataFrame(counts_uv_Acicastello)
counts_uv_Acicastello.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_Acicastello.plot()

counts_uv_SS121A = pd.merge(counts_uv_SS121A, gdf_edges, on=['u', 'v'], how='left')
counts_uv_SS121A = gpd.GeoDataFrame(counts_uv_SS121A)
counts_uv_SS121A.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_SS121A.plot()

counts_uv_SS121B = pd.merge(counts_uv_SS121B, gdf_edges, on=['u', 'v'], how='left')
counts_uv_SS121B = gpd.GeoDataFrame(counts_uv_SS121B)
counts_uv_SS121B.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_SS121B.plot()



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

################################################################################
################################################################################
################################################################################
# make a colored map  ##########################################################
################################################################################
################################################################################
################################################################################

'''
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
'''

################################################################################
################################################################################
################################################################################
################################################################################

#### Color Map ##################################################################
#################################################################################
## ---> TAORMINA --- + ---> CATANIA  Tangenziale Ovest Catania###################
#################################################################################

### MAKE a map with percentace (%) flows from TAORMINA and to CATANIA
Catania_partial = pd.DataFrame(counts_uv_CATANIA)
Taormina_partial = pd.DataFrame(counts_uv_TAORMINA)
Misterbianco_partial = pd.DataFrame(counts_uv_Misterbianco)
Acicastello_partial = pd.DataFrame(counts_uv_Acicastello)
SS121A_partial = pd.DataFrame(counts_uv_SS121A)
SS121B_partial = pd.DataFrame(counts_uv_SS121B)

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


## mormalize separately and get colors
Misterbianco_partial["scales"] = round(((Misterbianco_partial.counts/max(Misterbianco_partial.counts)))  ,1)
# add colors based on 'counts'
vmin = min(Misterbianco_partial.scales)
vmax = max(Misterbianco_partial.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
Misterbianco_partial['color'] = Misterbianco_partial['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
## Normalize to 1
Misterbianco_partial["load (%)"] = round(Misterbianco_partial["scales"]/max(Misterbianco_partial["scales"]),2)*100


## mormalize separately and get colors
Acicastello_partial["scales"] = round(((Acicastello_partial.counts/max(Acicastello_partial.counts)))  ,1)
# add colors based on 'counts'
vmin = min(Acicastello_partial.scales)
vmax = max(Acicastello_partial.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
Acicastello_partial['color'] = Acicastello_partial['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
## Normalize to 1
Acicastello_partial["load (%)"] = round(Acicastello_partial["scales"]/max(Acicastello_partial["scales"]),2)*100



## mormalize separately and get colors
SS121A_partial["scales"] = round(((SS121A_partial.counts/max(SS121A_partial.counts)))  ,1)
# add colors based on 'counts'
vmin = min(SS121A_partial.scales)
vmax = max(SS121A_partial.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
SS121A_partial['color'] = SS121A_partial['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
## Normalize to 1
SS121A_partial["load (%)"] = round(SS121A_partial["scales"]/max(SS121A_partial["scales"]),2)*100


## mormalize separately and get colors
SS121B_partial["scales"] = round(((SS121B_partial.counts/max(SS121B_partial.counts)))  ,1)
# add colors based on 'counts'
vmin = min(SS121B_partial.scales)
vmax = max(SS121B_partial.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
SS121B_partial['color'] = SS121B_partial['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
## Normalize to 1
SS121B_partial["load (%)"] = round(SS121B_partial["scales"]/max(SS121B_partial["scales"]),2)*100



## merge CATANIA with TAORMINA, MISTERBIANCO, ACICASTELLO and SS121 counts and keep the max count....
merged_loads = pd.concat([Catania_partial, Taormina_partial,
                         Misterbianco_partial, Acicastello_partial,
                          SS121A_partial, SS121B_partial]).groupby(['u','v'], as_index=False)['load (%)'].max()
merged_loads = pd.merge(merged_loads, gdf_edges, on=['u', 'v'], how='left')
merged_loads = gpd.GeoDataFrame(merged_loads)
# Taormina_Catania.plot()
merged_loads = merged_loads[merged_loads["load (%)"] > 5]

## get colors
merged_loads["scales"] = round(((merged_loads["load (%)"]/max(merged_loads["load (%)"])) )  ,1)
# # add colors based on 'counts'
vmin = min(merged_loads.scales)
vmax = max(merged_loads.scales)
# # Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
merged_loads['color'] = merged_loads['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


my_map = plot_graph_folium_FK(merged_loads, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=5, edge_opacity=1)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    merged_loads[['u','v', 'load (%)', 'geometry']].to_json(),
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
my_map.save(path + "LOADS_TAORMINA_CATANIA_MISTERBI_ACICAS_SS121_24_NOVEMBER_2019.html")


######################################################################
######################## COLORBAR ####################################
######################################################################

import matplotlib as mpl
COLORS_by_records = pd.DataFrame(merged_loads.drop_duplicates(['load (%)', 'color']))[['load (%)', 'color']]
# sort by ascending order of the column records
COLORS_by_records = COLORS_by_records.sort_values(by=['load (%)'])
len(COLORS_by_records)
# keep same order...
color_list = COLORS_by_records.color.drop_duplicates().tolist()

fig, ax = plt.subplots(figsize=(8, 1))
fig.subplots_adjust(bottom=0.5)
# cmap = matplotlib.colors.ListedColormap(color_list)
cmap = mpl.cm.YlOrRd
MAX  = max(COLORS_by_records['load (%)'])
MIN  = min(COLORS_by_records['load (%)'])
# cmap.set_over(str(MAX + 5))
# cmap.set_under(str(MIN -5))

cmap.set_over('k')
cmap.set_under('white')

# make a sequence list of records
bounds = np.arange(MIN, MAX, 10).tolist()

norm = mpl.colors.BoundaryNorm(bounds, cmap.N)
cb2 = mpl.colorbar.ColorbarBase(ax, cmap=cmap,
                                norm=norm,
                                boundaries=[5] + bounds + [MAX+5],
                                extend='both',
                                ticks=bounds,
                                spacing='uniform',
                                orientation='horizontal')
cb2.set_label('load (%)')
fig.savefig('LEGEND_loads.png')

##################################################################
##################################################################
##################################################################
#### ---- ISOCHRONES by "trajectory" ---------------##############
## starting from Tangenziale Ovest Catania towards Taormina #####
##################################################################
##################################################################

###########################
## --> TAORMINA ###########
###########################


## get the counts for each edge (u,v) pair and for each "TRAJECTORY" (TRIP)
# get the mean by u,v and "idtrajectory" of the SPEED
speed_uv = partial_TAORMINA[['u', 'v', 'idtrajectory', 'speed']]
speed_uv = (speed_uv.groupby(['u', 'v', 'idtrajectory']).mean()).reset_index()
speed_uv = pd.merge(speed_uv, gdf_edges[['u','v','length']], on=['u', 'v'], how='left')
## calculate travel times on each edge for edge within each "idtrajectory"
speed_uv['travel_time'] = ((speed_uv.length/1000)/(speed_uv.speed))*60  ## in minutes

## sum all travel times all nodes fo the same "idtrajectory"
traveltime_trajectory = (speed_uv[['idtrajectory','travel_time']].groupby(['idtrajectory']).sum()).reset_index()
## check which are the trajectories with the highest travel time....
traveltime_trajectory['travel_time'].hist()
traveltime_trajectory = traveltime_trajectory[traveltime_trajectory.travel_time < 120]
traveltime_trajectory['travel_time'].hist()


## merge with 'u' and 'v'
traveltime_trajectory = pd.merge(traveltime_trajectory, speed_uv[['u','v','idtrajectory']], on=['idtrajectory'], how='left')
traveltime_trajectory['travel_time'] = traveltime_trajectory['travel_time'].astype(np.int64)


## make a categorization for different range of travel times (minutes)
# [0,10] [10,18] [18,26] [26,34] [34,42] [42,50], [50,57] [57,66] [66,74] [74,82]   # 10 CLASSES
## create a new field for LEVELS of SERVICE 'travel_time_range'
# traveltime_trajectory = traveltime_trajectory[0:1000]
traveltime_trajectory['travel_range'] = None
for i in range(len(traveltime_trajectory)):
    row = traveltime_trajectory.iloc[i]
    if (0 < row['travel_time'] <= 5):
        traveltime_trajectory.travel_range.iloc[i] = 5
    elif (5 < row['travel_time'] <= 10):
        traveltime_trajectory.travel_range.iloc[i] = 10
    elif (10 < row['travel_time'] <= 15):
        traveltime_trajectory.travel_range.iloc[i] = 15
    elif (15 < row['travel_time'] <= 20):
        traveltime_trajectory.travel_range.iloc[i] = 20
    elif (20 < row['travel_time'] <= 25):
        traveltime_trajectory.travel_range.iloc[i] = 25
    elif (25 < row['travel_time'] <= 30):
        traveltime_trajectory.travel_range.iloc[i] = 30
    elif (30 < row['travel_time'] <= 35):
        traveltime_trajectory.travel_range.iloc[i] = 35
    elif (35 < row['travel_time'] <= 40):
        traveltime_trajectory.travel_range.iloc[i] = 40
    elif (40 < row['travel_time'] <= 45):
        traveltime_trajectory.travel_range.iloc[i] = 45
    elif (45 < row['travel_time'] <= 50):
        traveltime_trajectory.travel_range.iloc[i] = 50
    elif (50 < row['travel_time'] <= 55):
        traveltime_trajectory.travel_range.iloc[i] = 55
    elif (55 < row['travel_time'] <= 60):
        traveltime_trajectory.travel_range.iloc[i] = 60
    elif (60 < row['travel_time'] <= 65):
        traveltime_trajectory.travel_range.iloc[i] = 65
    elif (65 < row['travel_time'] <= 70):
        traveltime_trajectory.travel_range.iloc[i] = 70
    elif (70 < row['travel_time'] <= 75):
        traveltime_trajectory.travel_range.iloc[i] = 75
    elif (75 < row['travel_time'] <= 80):
        traveltime_trajectory.travel_range.iloc[i] = 80
    elif (80 < row['travel_time'] <= 85):
        traveltime_trajectory.travel_range.iloc[i] = 85
    elif (85 < row['travel_time'] <= 90):
        traveltime_trajectory.travel_range.iloc[i] = 90


## get geometry from the OSM network
traveltime_trajectory = pd.merge(traveltime_trajectory, gdf_edges, on=['u', 'v'], how='left')
traveltime_trajectory = gpd.GeoDataFrame(traveltime_trajectory)
# traveltime_trajectory.plot()


### map ###

traveltime_trajectory["scales"] = traveltime_trajectory.travel_range/max(traveltime_trajectory.travel_range)
# add colors based on 'counts'
vmin = min(traveltime_trajectory.scales)
vmax = max(traveltime_trajectory.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.brg)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
traveltime_trajectory['color'] = traveltime_trajectory['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


my_map = plot_graph_folium_FK(traveltime_trajectory, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=2.5, edge_opacity=0.8)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    traveltime_trajectory[['u','v', 'scales', 'travel_range', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'yellow',
        'fillOpacity':0.8
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'travel_range']
    ),
).add_to(my_map)


folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
my_map.save(path + "travel_time_TAORMINA_24_NOVEMBER_2019.html")


###############################################################################
###############################################################################
###############################################################################
###############################################################################

###########################
## --> CATANIA ###########
###########################


## get the counts for each edge (u,v) pair and for each "TRAJECTORY" (TRIP)
# get the mean by u,v and "idtrajectory" of the SPEED
speed_uv = partial_CATANIA[['u', 'v', 'idtrajectory', 'speed']]
speed_uv = (speed_uv.groupby(['u', 'v', 'idtrajectory']).mean()).reset_index()
speed_uv = pd.merge(speed_uv, gdf_edges[['u','v','length']], on=['u', 'v'], how='left')
## calculate travel times on each edge for edge within each "idtrajectory"
speed_uv['travel_time'] = ((speed_uv.length/1000)/(speed_uv.speed))*60  ## in minutes

## sum all travel times all nodes fo the same "idtrajectory"
traveltime_trajectory = (speed_uv[['idtrajectory','travel_time']].groupby(['idtrajectory']).sum()).reset_index()
## check which are the trajectories with the highest travel time....
traveltime_trajectory['travel_time'].hist()
traveltime_trajectory = traveltime_trajectory[traveltime_trajectory.travel_time < 120]
traveltime_trajectory['travel_time'].hist()


## merge with 'u' and 'v'
traveltime_trajectory = pd.merge(traveltime_trajectory, speed_uv[['u','v','idtrajectory']], on=['idtrajectory'], how='left')
traveltime_trajectory['travel_time'] = traveltime_trajectory['travel_time'].astype(np.int64)


## make a categorization for different range of travel times (minutes)
# [0,10] [10,18] [18,26] [26,34] [34,42] [42,50], [50,57] [57,66] [66,74] [74,82]   # 10 CLASSES
## create a new field for LEVELS of SERVICE 'travel_time_range'
# traveltime_trajectory = traveltime_trajectory[0:1000]
traveltime_trajectory['travel_range'] = None
for i in range(len(traveltime_trajectory)):
    row = traveltime_trajectory.iloc[i]
    if (0 < row['travel_time'] <= 5):
        traveltime_trajectory.travel_range.iloc[i] = 5
    elif (5 < row['travel_time'] <= 10):
        traveltime_trajectory.travel_range.iloc[i] = 10
    elif (10 < row['travel_time'] <= 15):
        traveltime_trajectory.travel_range.iloc[i] = 15
    elif (15 < row['travel_time'] <= 20):
        traveltime_trajectory.travel_range.iloc[i] = 20
    elif (20 < row['travel_time'] <= 25):
        traveltime_trajectory.travel_range.iloc[i] = 25
    elif (25 < row['travel_time'] <= 30):
        traveltime_trajectory.travel_range.iloc[i] = 30
    elif (30 < row['travel_time'] <= 35):
        traveltime_trajectory.travel_range.iloc[i] = 35
    elif (35 < row['travel_time'] <= 40):
        traveltime_trajectory.travel_range.iloc[i] = 40
    elif (40 < row['travel_time'] <= 45):
        traveltime_trajectory.travel_range.iloc[i] = 45
    elif (45 < row['travel_time'] <= 50):
        traveltime_trajectory.travel_range.iloc[i] = 50
    elif (50 < row['travel_time'] <= 55):
        traveltime_trajectory.travel_range.iloc[i] = 55
    elif (55 < row['travel_time'] <= 60):
        traveltime_trajectory.travel_range.iloc[i] = 60
    elif (60 < row['travel_time'] <= 65):
        traveltime_trajectory.travel_range.iloc[i] = 65
    elif (65 < row['travel_time'] <= 70):
        traveltime_trajectory.travel_range.iloc[i] = 70
    elif (70 < row['travel_time'] <= 75):
        traveltime_trajectory.travel_range.iloc[i] = 75
    elif (75 < row['travel_time'] <= 80):
        traveltime_trajectory.travel_range.iloc[i] = 80
    elif (80 < row['travel_time'] <= 85):
        traveltime_trajectory.travel_range.iloc[i] = 85
    elif (85 < row['travel_time'] <= 90):
        traveltime_trajectory.travel_range.iloc[i] = 90


### remove "none" values
traveltime_trajectory = traveltime_trajectory.dropna(subset=['travel_range'])  # remove nan values

## get geometry from the OSM network
traveltime_trajectory = pd.merge(traveltime_trajectory, gdf_edges, on=['u', 'v'], how='left')
traveltime_trajectory = gpd.GeoDataFrame(traveltime_trajectory)
# traveltime_trajectory.plot()


### map ###

traveltime_trajectory["scales"] = traveltime_trajectory.travel_range/max(traveltime_trajectory.travel_range)
# add colors based on 'counts'
vmin = min(traveltime_trajectory.scales)
vmax = max(traveltime_trajectory.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.brg)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
traveltime_trajectory['color'] = traveltime_trajectory['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


my_map = plot_graph_folium_FK(traveltime_trajectory, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=2.5, edge_opacity=0.8)

style = {'fillColor': '#00000000', 'color': '#00000000'}

# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    traveltime_trajectory[['u','v', 'scales', 'travel_range', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'yellow',
        'fillOpacity':0.8
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'travel_range']
    ),
).add_to(my_map)


folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
my_map.save(path + "travel_time_CATANIA_24_NOVEMBER_2019.html")



######################################################################
######################## COLORBAR ####################################
######################################################################

import matplotlib as mpl
COLORS_by_records = pd.DataFrame(traveltime_trajectory.drop_duplicates(['travel_range', 'color']))[['travel_range', 'color']]
# sort by ascending order of the column records
COLORS_by_records = COLORS_by_records.sort_values(by=['travel_range'])
len(COLORS_by_records)
# keep same order...
color_list = COLORS_by_records.color.drop_duplicates().tolist()

fig, ax = plt.subplots(figsize=(8, 1))
fig.subplots_adjust(bottom=0.5)
cmap = mpl.cm.brg
MAX  = max(COLORS_by_records['travel_range'])
MIN  = min(COLORS_by_records['travel_range'])

cmap.set_over('k')
cmap.set_under('white')

# make a sequence list of records
bounds = np.arange(MIN, MAX, 5).tolist()

norm = mpl.colors.BoundaryNorm(bounds, cmap.N)
cb2 = mpl.colorbar.ColorbarBase(ax, cmap=cmap,
                                norm=norm,
                                boundaries=[5] + bounds + [MAX+5],
                                extend='both',
                                ticks=bounds,
                                spacing='uniform',
                                orientation='horizontal')
cb2.set_label('travel time (min)')
fig.savefig('LEGEND_ISOCHRONES.png')


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

