
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

## Viadotto Sordo
## (312617273, 6556214429) --> Catania
## (518162385, 6532608322) --> Siracusa

## Viadotto San Paolo
## "Viadotto San Paolo" u,v --> (841721621, 6758675255) --> Catania
## "Viadotto San Paoo" u,v --> (4096452579, 6758779932) --> Acireale


from datetime import datetime
now1 = datetime.now()

df =  pd.read_sql_query('''
                     WITH path AS(SELECT 
                            split_part("TRIP_ID"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (841721621, 6758675255),
                           (4096452579, 6758779932))
                                 /*LIMIT 10000 */)
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id   
                            WHERE date(path.timedate) = '2019-11-21'
                            /*WHERE EXTRACT(MONTH FROM path.timedate) = '02'*/
                                 ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)

## get all the IDTERM  vehicles passing through the TANGENZIALE OVEST CATANIA
df_CATANIA = df[(df['u'] == 841721621) & (df['v'] == 6758675255) ]   ## towards CATANIA
df_ACIREALE = df[(df['u'] == 4096452579) & (df['v'] == 6758779932) ]    ## towards ACIREALE

df_CATANIA.drop_duplicates(['idterm'], inplace=True)
df_ACIREALE.drop_duplicates(['idterm'], inplace=True)

# ## make a list of all IDterminals for the direction of Catania and Acireale
all_idterms_CATANIA = list(df_CATANIA.idterm.unique())
all_idterms_ACIREALE = list(df_ACIREALE.idterm.unique())


###############################################################################
## get MAP-MATCHING data from DB for a specific day of the month (2019) ########
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
                                   WHERE date(mapmatching_2019.timedate) = '2019-11-21'
                                   /*WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '02'*/
                                   /*AND dataraw.vehtype::bigint = 1*/
                                   /*LIMIT 10000*/
                    ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)



##########################################################
##### Get counts only for trips on SELECTED EDGES ########

### get all data only for the vehicles in the list
all_CATANIA = viasat_data[viasat_data.idterm.isin(all_idterms_CATANIA)]
all_ACIREALE = viasat_data[viasat_data.idterm.isin(all_idterms_ACIREALE)]


## get data with "sequenza" STARTING from the chosen nodes on the TANGENZIALE OVEST CATANIA for each idterm
pnt_sequenza_CATANIA = all_CATANIA[(all_CATANIA['u'] == 841721621) & (all_CATANIA['v'] == 6758675255) ]
pnt_sequenza_ACIREALE = all_ACIREALE[(all_ACIREALE['u'] == 4096452579) & (all_ACIREALE['v'] == 6758779932) ]


# del viasat_data

### initialize an empty dataframe
partial_CATANIA = pd.DataFrame([])
partial_ACIREALE = pd.DataFrame([])


##################
### ACIREALE #####
##################
for idx, idterm in enumerate(all_idterms_ACIREALE):
    print(idterm)
    idterm = int(idterm)
    ### get starting "sequenza" on the EDGE of Viadotto San Paolo
    sequenza = pnt_sequenza_ACIREALE[pnt_sequenza_ACIREALE.idterm == idterm]['sequenza'].iloc[0]
    sub = all_ACIREALE[(all_ACIREALE.idterm == idterm) & (all_ACIREALE.sequenza >= sequenza)]
    partial_ACIREALE = partial_ACIREALE.append(sub)



##################
### CATANIA ######
##################
for idx, idterm in enumerate(all_idterms_CATANIA):
    print(idterm)
    idterm = int(idterm)
    # get starting "sequenza" on the EDGE of Viadotto San Paolo
    sequenza = pnt_sequenza_CATANIA[pnt_sequenza_CATANIA.idterm == idterm]['sequenza'].iloc[0]
    sub = all_CATANIA[(all_CATANIA.idterm == idterm) & (all_CATANIA.sequenza >= sequenza)]
    partial_CATANIA = partial_CATANIA.append(sub)


####################################################
### further filtering with idtrajectory (by trip) ##
####################################################

partial_ACIREALE_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_ACIREALE):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_ACIREALE[pnt_sequenza_ACIREALE.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_ACIREALE[(partial_ACIREALE.idterm == idterm) & (partial_ACIREALE.idtrajectory == idtrajectory)]
    partial_ACIREALE_bis = partial_ACIREALE_bis.append(sub)


partial_CATANIA_bis = pd.DataFrame([])

for idx, idterm in enumerate(all_idterms_CATANIA):
    print(idterm)
    idterm = int(idterm)
    # get starting "idtrajectory"
    idtrajectory = pnt_sequenza_CATANIA[pnt_sequenza_CATANIA.idterm == idterm]['idtrajectory'].iloc[0]
    sub = partial_CATANIA[(partial_CATANIA.idterm == idterm) & (partial_CATANIA.idtrajectory == idtrajectory)]
    partial_CATANIA_bis = partial_CATANIA_bis.append(sub)


## rename partial data
partial_ACIREALE = partial_ACIREALE_bis
partial_CATANIA = partial_CATANIA_bis


## only select (u,v) from partial data
ACIREALE = partial_ACIREALE[['u','v']]
CATANIA = partial_CATANIA[['u','v']]


#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

### get counts for selected edges ###
counts_uv_ACIREALE = ACIREALE.groupby(ACIREALE.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_CATANIA = CATANIA.groupby(CATANIA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})


########################################################
##### build the map ####################################

## merge counts with OSM (Open Street Map) edges
counts_uv_ACIREALE = pd.merge(counts_uv_ACIREALE, gdf_edges, on=['u', 'v'], how='left')
counts_uv_ACIREALE = gpd.GeoDataFrame(counts_uv_ACIREALE)
counts_uv_ACIREALE.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_ACIREALE.plot()

counts_uv_CATANIA = pd.merge(counts_uv_CATANIA, gdf_edges, on=['u', 'v'], how='left')
counts_uv_CATANIA = gpd.GeoDataFrame(counts_uv_CATANIA)
counts_uv_CATANIA.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_CATANIA.plot()


counts_uv_ACIREALE["scales"] = (counts_uv_ACIREALE.counts/max(counts_uv_ACIREALE.counts)) * 7
counts_uv_CATANIA["scales"] = (counts_uv_CATANIA.counts/max(counts_uv_CATANIA.counts)) * 7

######################################################################################
######################################################################################
### SAVE .CSV file ###################################################################

## Normalize to 1 and get loads
counts_uv_ACIREALE["load(%)"] = round(counts_uv_ACIREALE["counts"]/max(counts_uv_ACIREALE["counts"]),4)*100
counts_uv_CATANIA["load(%)"] = round(counts_uv_CATANIA["counts"]/max(counts_uv_CATANIA["counts"]),4)*100
## save to .csv file
LOADS_ACIREALE_UV = pd.DataFrame(counts_uv_ACIREALE[['u','v','load(%)']])
LOADS_CATANIA_UV = pd.DataFrame(counts_uv_CATANIA[['u','v','load(%)']])

counts_uv = [LOADS_ACIREALE_UV, LOADS_CATANIA_UV]
counts_uv = pd.concat(counts_uv)
counts_uv.to_csv('LOADS_Viadotto_SanPaolo_CATANIA_21_NOVEMBER_2019.csv')


################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
counts_uv_ACIREALE[['u','v', 'counts', 'scales', 'load(%)', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'blue',
        'color': 'blue',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)


folium.GeoJson(
counts_uv_CATANIA[['u','v', 'counts', 'scales', 'load(%)', 'geometry']].to_json(),
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
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)


my_map.save("LOADS_Viadotto_SanPaolo_CATANIA_21_NOVEMBER_2019.html")


##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
##################################################################################################################
############# introduce an INTERRUPTION in the "Viadotto San Paolo" ##############################################
#############------------------both directions---------------------------------##################################################

penalty = 25000  # PENALTY time or DISRUPTION time (seconds of closure of the link (u,v))

## add a disruption time on the Viadotto San Paolo
### load grafo
file_graphml = 'CATANIA_VIASAT_cost.graphml'
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)

###########################################
######## ----> CATANIA ####################

for u, v, key, attr in grafo.edges(keys=True, data=True):
    # print(attr)
    # print(attr.get("VIASAT_cost"))
    if len(attr['VIASAT_cost']) > 0:
        attr['VIASAT_cost'] = float(attr.get("VIASAT_cost"))
        # print(attr)

for u, v, key, attr in grafo.edges(keys=True, data=True):
    zipped_CATANIA = zip([841721621], [6758675255]) ### Viadotto San Paolo ---> Catania (u,v)
    if (u, v) in zipped_CATANIA:
        print(u, v)
        print("gotta!=====================================================")
        attr['VIASAT_cost'] = float(attr['VIASAT_cost']) + penalty
        print(attr)
        # break
        grafo.add_edge(u, v, key, attr_dict=attr)


### initialize an empty dataframe
Catania_Viadotto_SanPaolo_OD = pd.DataFrame([])


all_trips = list(partial_CATANIA_bis.idtrajectory.unique())
for idx_a, idtrajectory in enumerate(all_trips):
    # print(idx_a)
    # print(idtrajectory)
    ## filter data by idterm and by idtrajectory (trip)
    data = all_CATANIA[all_CATANIA.idtrajectory == idtrajectory]
    ## sort data by "sequenza'
    data = data.sort_values('sequenza')
    ORIGIN = data[data.sequenza == min(data.sequenza)][['u']].iloc[0][0]
    DESTINATION = data[data.sequenza == max(data.sequenza)][['v']].iloc[0][0]
    data['ORIGIN'] = ORIGIN
    data['DESTINATION'] = DESTINATION
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.append(data)
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.drop_duplicates(['u', 'v', 'ORIGIN', 'DESTINATION'])
    ## reset index
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.reset_index(level=0)[['u', 'v', 'ORIGIN', 'DESTINATION']]

    
# loop ever each ORIGIN --> DESTINATION pair
all_EDGES_CATANIA = pd.DataFrame([])
O = list(Catania_Viadotto_SanPaolo_OD.ORIGIN.unique())
D = list(Catania_Viadotto_SanPaolo_OD.DESTINATION.unique())
zipped_OD = zip(O, D)
for (i, j) in zipped_OD:
    print(i,j)
    try:
        ## find shortest path based on the "cost" (time) + PENALTY
        try:
            # get shortest path again...but now with the PENALTY
            shortest_OD_path_VIASAT_penalty = nx.shortest_path(grafo, i, j,
                                                                    weight='VIASAT_cost')
            path_edges = list(zip(shortest_OD_path_VIASAT_penalty, shortest_OD_path_VIASAT_penalty[1:]))
            all_EDGES_CATANIA = all_EDGES_CATANIA.append(path_edges)
        except ValueError:
            print('Contradictory paths found:', 'negative weights?')
    except (nx.NodeNotFound, nx.exception.NetworkXNoPath):
        print('O-->D NodeNotFound', 'i:', i, 'j:', j)

## neams columns
all_EDGES_CATANIA.columns = ['u', 'v']


######################################
##### ---> ACIREALE ##################

file_graphml = 'CATANIA_VIASAT_cost.graphml'
grafo = ox.load_graphml(file_graphml)


for u, v, key, attr in grafo.edges(keys=True, data=True):
    # print(attr)
    # print(attr.get("VIASAT_cost"))
    if len(attr['VIASAT_cost']) > 0:
        attr['VIASAT_cost'] = float(attr.get("VIASAT_cost"))

for u, v, key, attr in grafo.edges(keys=True, data=True):
    zipped_ACIREALE = zip([4096452579], [6758779932]) ### Viadotto San Paolo ---> Acireale (u,v)
    if (u, v) in zipped_ACIREALE:
        print(u, v)
        print("gotta!=====================================================")
        attr['VIASAT_cost'] = float(attr['VIASAT_cost']) + penalty
        print(attr)
        # break
        grafo.add_edge(u, v, key, attr_dict=attr)

### initialize an empty dataframe
Acireale_Viadotto_SanPaolo_OD = pd.DataFrame([])

all_trips = list(partial_ACIREALE_bis.idtrajectory.unique())
for idx_a, idtrajectory in enumerate(all_trips):
    # print(idx_a)
    # print(idtrajectory)
    ## filter data by idterm and by idtrajectory (trip)
    data = all_ACIREALE[all_ACIREALE.idtrajectory == idtrajectory]
    ## sort data by "sequenza'
    data = data.sort_values('sequenza')
    ORIGIN = data[data.sequenza == min(data.sequenza)][['u']].iloc[0][0]
    DESTINATION = data[data.sequenza == max(data.sequenza)][['v']].iloc[0][0]
    data['ORIGIN'] = ORIGIN
    data['DESTINATION'] = DESTINATION
    Acireale_Viadotto_SanPaolo_OD = Acireale_Viadotto_SanPaolo_OD.append(data)
    Acireale_Viadotto_SanPaolo_OD = Acireale_Viadotto_SanPaolo_OD.drop_duplicates(['u', 'v', 'ORIGIN', 'DESTINATION'])
    ## reset index
    Acireale_Viadotto_SanPaolo_OD = Acireale_Viadotto_SanPaolo_OD.reset_index(level=0)[['u', 'v', 'ORIGIN', 'DESTINATION']]

# loop ever each ORIGIN --> DESTINATION pair
all_EDGES_ACIREALE = pd.DataFrame([])
O = list(Acireale_Viadotto_SanPaolo_OD.ORIGIN.unique())
D = list(Acireale_Viadotto_SanPaolo_OD.DESTINATION.unique())
zipped_OD = zip(O, D)
for (i, j) in zipped_OD:
    print(i, j)
    try:
        ## find shortest path based on the "cost" (time) + PENALTY
        try:
            # get shortest path again...but now with the PENALTY
            shortest_OD_path_VIASAT_penalty = nx.shortest_path(grafo, i, j,
                                                               weight='VIASAT_cost')
            path_edges = list(zip(shortest_OD_path_VIASAT_penalty, shortest_OD_path_VIASAT_penalty[1:]))
            all_EDGES_ACIREALE = all_EDGES_ACIREALE.append(path_edges)
        except ValueError:
            print('Contradictory paths found:', 'negative weights?')
    except (nx.NodeNotFound, nx.exception.NetworkXNoPath):
        print('O-->D NodeNotFound', 'i:', i, 'j:', j)

## neams columns
all_EDGES_ACIREALE.columns = ['u', 'v']

#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

### get counts for selected edges ###
counts_uv_disruption_CATANIA = all_EDGES_CATANIA.groupby(all_EDGES_CATANIA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_disruption_ACIREALE = all_EDGES_ACIREALE.groupby(all_EDGES_ACIREALE.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

########################################################
##### build the map ####################################

## merge counts with OSM (Open Street Map) edges
counts_uv_disruption_CATANIA = pd.merge(counts_uv_disruption_CATANIA, gdf_edges, on=['u', 'v'], how='left')
counts_uv_disruption_CATANIA = gpd.GeoDataFrame(counts_uv_disruption_CATANIA)
counts_uv_disruption_CATANIA.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_disruption_CATANIA.plot()


## merge counts with OSM (Open Street Map) edges
counts_uv_disruption_ACIREALE = pd.merge(counts_uv_disruption_ACIREALE, gdf_edges, on=['u', 'v'], how='left')
counts_uv_disruption_ACIREALE = gpd.GeoDataFrame(counts_uv_disruption_ACIREALE)
counts_uv_disruption_ACIREALE.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_disruption_ACIREALE.plot()

counts_uv_disruption_CATANIA["scales"] = (counts_uv_disruption_CATANIA.counts/max(counts_uv_disruption_CATANIA.counts)) * 7
counts_uv_disruption_ACIREALE["scales"] = (counts_uv_disruption_ACIREALE.counts/max(counts_uv_disruption_ACIREALE.counts)) * 7

######################################################################################
######################################################################################
### SAVE .CSV file ###################################################################

## Normalize to 1 and get loads
counts_uv_disruption_CATANIA["load(%)"] = round(counts_uv_disruption_CATANIA["counts"]/max(counts_uv_disruption_CATANIA["counts"]),4)*100
counts_uv_disruption_ACIREALE["load(%)"] = round(counts_uv_disruption_ACIREALE["counts"]/max(counts_uv_disruption_ACIREALE["counts"]),4)*100
## save to .csv file
LOADS_DISRUPTION_CATANIA_UV = pd.DataFrame(counts_uv_disruption_CATANIA[['u','v','load(%)']])
LOADS_DISRUPTION_ACIREALE_UV = pd.DataFrame(counts_uv_disruption_ACIREALE[['u','v','load(%)']])

counts_uv = [LOADS_DISRUPTION_CATANIA_UV,
             LOADS_DISRUPTION_ACIREALE_UV]
counts_uv = pd.concat(counts_uv)
counts_uv.to_csv('LOADS_DISRUPTION_Viadotto_SanPaolo_CATANIA_21_NOVEMBER_2019.csv')


################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
counts_uv_disruption_CATANIA[['u','v', 'counts', 'scales', 'load(%)', 'geometry']].to_json(),
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
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)


folium.GeoJson(
counts_uv_disruption_ACIREALE[['u','v', 'counts', 'scales', 'load(%)', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'blue',
        'color': 'blue',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)

my_map.save("LOADS_DISRUPTION_Viadotto_SanPaolo_CATANIA_21_NOVEMBER_2019.html")

## several checks ### -------------
# AAA = pd.DataFrame(counts_uv_disruption_CATANIA[counts_uv_disruption_CATANIA.u == 312617273])
# AAA = pd.DataFrame(counts_uv_disruption_CATANIA[counts_uv_disruption_CATANIA.u == 518162385])
# AAAA = pd.DataFrame(counts_uv[counts_uv.u == 312617273])



##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
## one direction by time #########################################################################

#### -------------------------------- ######
### get counts for all edges ###############
### -------------------------------- #######

#####################################################
#####################################################
#####################################################
######## ----> direzione CATANIA ####################

penalty = 25000  # PENALTY time or DISRUPTION time (seconds of closure of the link (u,v))
file_graphml = 'CATANIA_VIASAT_cost.graphml'
grafo = ox.load_graphml(file_graphml)


for u, v, key, attr in grafo.edges(keys=True, data=True):
    # print(attr)
    # print(attr.get("VIASAT_cost"))
    if len(attr['VIASAT_cost']) > 0:
        attr['VIASAT_cost'] = float(attr.get("VIASAT_cost"))
        # print(attr)

for u, v, key, attr in grafo.edges(keys=True, data=True):
    zipped_CATANIA = zip([841721621], [6758675255]) ### Viadotto San Paolo ---> Catania (u,v)
    if (u, v) in zipped_CATANIA:
        print(u, v)
        print("gotta!=====================================================")
        attr['VIASAT_cost'] = float(attr['VIASAT_cost']) + penalty
        print(attr)
        # break
        grafo.add_edge(u, v, key, attr_dict=attr)

### initialize an empty dataframe
Catania_Viadotto_SanPaolo_OD = pd.DataFrame([])


partials = [partial_CATANIA_bis,
             partial_ACIREALE_bis]
partials = pd.concat(partials)

all_trips = list(partials.idtrajectory.unique())
## nly loop over the idtrajectories crossing the Viadotto San Paolo
for idx_a, idtrajectory in enumerate(all_trips):
    # print(idx_a)
    # print(idtrajectory)
    ## filter data by idterm and by idtrajectory (trip)
    data = viasat_data[viasat_data.idtrajectory == idtrajectory]
    ## sort data by "sequenza'
    data = data.sort_values('sequenza')
    ORIGIN = data[data.sequenza == min(data.sequenza)][['u']].iloc[0][0]
    DESTINATION = data[data.sequenza == max(data.sequenza)][['v']].iloc[0][0]
    data['ORIGIN'] = ORIGIN
    data['DESTINATION'] = DESTINATION
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.append(data)
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.drop_duplicates(['u', 'v', 'ORIGIN', 'DESTINATION'])
    ## reset index
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.reset_index(level=0)[['u', 'v', 'ORIGIN', 'DESTINATION']]

# loop ever each ORIGIN --> DESTINATION pair
all_EDGES_CATANIA = pd.DataFrame([])
O = list(Catania_Viadotto_SanPaolo_OD.ORIGIN.unique())
D = list(Catania_Viadotto_SanPaolo_OD.DESTINATION.unique())
zipped_OD = zip(O, D)
for (i, j) in zipped_OD:
    print(i, j)
    try:
        ## find shortest path based on the "cost" (time) + PENALTY
        try:
            # get shortest path again...but now with the PENALTY
            shortest_OD_path_VIASAT_penalty = nx.shortest_path(grafo, i, j,
                                                               weight='VIASAT_cost')
            path_edges = list(zip(shortest_OD_path_VIASAT_penalty, shortest_OD_path_VIASAT_penalty[1:]))
            all_EDGES_CATANIA = all_EDGES_CATANIA.append(path_edges)
        except ValueError:
            print('Contradictory paths found:', 'negative weights?')
    except (nx.NodeNotFound, nx.exception.NetworkXNoPath):
        print('O-->D NodeNotFound', 'i:', i, 'j:', j)

## neams columns
all_EDGES_CATANIA.columns = ['u', 'v']

counts_uv_disruption_CATANIA = all_EDGES_CATANIA.groupby(all_EDGES_CATANIA.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

counts_uv_disruption_CATANIA = pd.merge(counts_uv_disruption_CATANIA, gdf_edges, on=['u', 'v'], how='left')
counts_uv_disruption_CATANIA = gpd.GeoDataFrame(counts_uv_disruption_CATANIA)
counts_uv_disruption_CATANIA.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_disruption_CATANIA.plot()

counts_uv_disruption_CATANIA["scales"] = (counts_uv_disruption_CATANIA.counts/max(counts_uv_disruption_CATANIA.counts)) * 7

## Normalize to 1 and get loads
counts_uv_disruption_CATANIA["load(%)"] = round(counts_uv_disruption_CATANIA["counts"]/max(counts_uv_disruption_CATANIA["counts"]),4)*100
## save csv
counts_uv_disruption_CATANIA.to_csv('DISRUPTION_Viadotto_SanPaplo_verso_CATANIA_21_NOVEMBER_2019.csv')

################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
counts_uv_disruption_CATANIA[['u','v', 'counts', 'scales', 'load(%)', 'geometry']].to_json(),
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
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)

my_map.save("DISRUPTION_Viadotto_SanPaolo_verso_CATANIA_21_NOVEMBER_2019.html")


#####################################################
######## ----> direzione ACIREALE ###################

penalty = 25000  # PENALTY time or DISRUPTION time (seconds of closure of the link (u,v))
file_graphml = 'CATANIA_VIASAT_cost.graphml'
grafo = ox.load_graphml(file_graphml)

for u, v, key, attr in grafo.edges(keys=True, data=True):
    # print(attr)
    # print(attr.get("VIASAT_cost"))
    if len(attr['VIASAT_cost']) > 0:
        attr['VIASAT_cost'] = float(attr.get("VIASAT_cost"))
        # print(attr)


for u, v, key, attr in grafo.edges(keys=True, data=True):
    zipped_ACIREALE = zip([4096452579], [6758779932])  ### Viadotto San Paolo ---> Acireale (u,v)
    if (u, v) in zipped_ACIREALE:
        print(u, v)
        print("gotta!=====================================================")
        attr['VIASAT_cost'] = float(attr['VIASAT_cost']) + penalty
        print(attr)
        # break
        grafo.add_edge(u, v, key, attr_dict=attr)

### initialize an empty dataframe
Catania_Viadotto_SanPaolo_OD = pd.DataFrame([])

partials = [partial_CATANIA_bis,
             partial_ACIREALE_bis]
partials = pd.concat(partials)

all_trips = list(partials.idtrajectory.unique())
## nly loop over the idtrajectories crossing the Viadotto San Paolo
for idx_a, idtrajectory in enumerate(all_trips):
    # print(idx_a)
    # print(idtrajectory)
    ## filter data by idterm and by idtrajectory (trip)
    data = viasat_data[viasat_data.idtrajectory == idtrajectory]
    ## sort data by "sequenza'
    data = data.sort_values('sequenza')
    ORIGIN = data[data.sequenza == min(data.sequenza)][['u']].iloc[0][0]
    DESTINATION = data[data.sequenza == max(data.sequenza)][['v']].iloc[0][0]
    data['ORIGIN'] = ORIGIN
    data['DESTINATION'] = DESTINATION
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.append(data)
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.drop_duplicates(['u', 'v', 'ORIGIN', 'DESTINATION'])
    ## reset index
    Catania_Viadotto_SanPaolo_OD = Catania_Viadotto_SanPaolo_OD.reset_index(level=0)[['u', 'v', 'ORIGIN', 'DESTINATION']]

# loop ever each ORIGIN --> DESTINATION pair
all_EDGES_ACIREALE = pd.DataFrame([])
O = list(Catania_Viadotto_SanPaolo_OD.ORIGIN.unique())
D = list(Catania_Viadotto_SanPaolo_OD.DESTINATION.unique())
zipped_OD = zip(O, D)
for (i, j) in zipped_OD:
    print(i, j)
    try:
        ## find shortest path based on the "cost" (time) + PENALTY
        try:
            # get shortest path again...but now with the PENALTY
            shortest_OD_path_VIASAT_penalty = nx.shortest_path(grafo, i, j,
                                                               weight='VIASAT_cost')
            path_edges = list(zip(shortest_OD_path_VIASAT_penalty, shortest_OD_path_VIASAT_penalty[1:]))
            all_EDGES_ACIREALE = all_EDGES_ACIREALE.append(path_edges)
        except ValueError:
            print('Contradictory paths found:', 'negative weights?')
    except (nx.NodeNotFound, nx.exception.NetworkXNoPath):
        print('O-->D NodeNotFound', 'i:', i, 'j:', j)

## neams columns
all_EDGES_ACIREALE.columns = ['u', 'v']

counts_uv_disruption_ACIREALE = all_EDGES_ACIREALE.groupby(all_EDGES_ACIREALE.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

counts_uv_disruption_ACIREALE = pd.merge(counts_uv_disruption_ACIREALE, gdf_edges, on=['u', 'v'], how='left')
counts_uv_disruption_ACIREALE = gpd.GeoDataFrame(counts_uv_disruption_ACIREALE)
counts_uv_disruption_ACIREALE.drop_duplicates(['u', 'v'], inplace=True)
# counts_uv_disruption_ACIREALE.plot()

counts_uv_disruption_ACIREALE["scales"] = (counts_uv_disruption_ACIREALE.counts/max(counts_uv_disruption_ACIREALE.counts)) * 7

## Normalize to 1 and get loads
counts_uv_disruption_ACIREALE["load(%)"] = round(counts_uv_disruption_ACIREALE["counts"]/max(counts_uv_disruption_ACIREALE["counts"]),4)*100
## save csv
counts_uv_disruption_ACIREALE.to_csv('DISRUPTION_Viadotto_SanPaolo_verso_ACIREALE_21_NOVEMBER_2019.csv')

################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
counts_uv_disruption_ACIREALE[['u','v', 'counts', 'scales', 'load(%)', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'blue',
        'color': 'blue',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)

my_map.save("DISRUPTION_Viadotto_SanPaolo_verso_ACIREALE_21_NOVEMBER_2019.html")


