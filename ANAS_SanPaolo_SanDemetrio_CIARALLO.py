

import os
os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL')
os.getcwd()

from math import radians, cos, sin, asin, sqrt
import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from collections import OrderedDict
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
from sqlalchemy import exc
from sqlalchemy.pool import NullPool
import sqlalchemy as sal
import geopy.distance
import warnings
import geopy.distance
import csv
from shapely import wkb
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK
from PIL import Image
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data')
os.getcwd()


# connect to Catania DB
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()


### load grafo with "cost" (travel times got from Viasat data)
file_graphml = 'CATANIA_VIASAT_cost.graphml'
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)
gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

for u, v, key, attr in grafo.edges(keys=True, data=True):
    # print(attr)
    # print(attr.get("VIASAT_cost"))
    if len(attr['VIASAT_cost']) > 0:
        attr['VIASAT_cost'] = float(attr.get("VIASAT_cost"))


## Viadotto San Paolo
### these are the OpenStretMap node of the roadnetwork
## "Viadotto San Paolo" u,v --> (841721621, 6758675255) --> Catania
## "Viadotto San Paoo" u,v --> (4096452579, 6758779932) --> Acireale (Messina)

###-------> SIMULATING DISRUPTION of VIADOTTO SAN PAOLO after an EARTHQUAKE ---- ########
### list of tuples indicating DISRUPTED EDGES including viadotto San Paolo

nearest_node_first_SANPAOLO = 4096452579   ## nodo tra rampa immissione tangenziale da via Etnea

### these are the OpenStretMap node of the roadnetwork
disruptions = [(309299130, 309299849), (1901929906, 309299871), (4072618994, 841721800),
               (1363946984, 1363946943), (476455543, 4064451884), (305783473, 610320390),
               (488537136, 1767590558), (637681763, 370190911),  ## Lorenzo Bolano
               (298299791, 298300329), (567758808, 310536545),
               (33589436, 254098470), (275916438, 343415692), (270879950, 879521951),
               (529318415, 2804761144), (2068701503, 1284917043), (1385222216, 410772674),
               (841721621, 6758675255), (4096452579, 6758779932), ## viadotto San Paolo
               (4096452584,	4096452579), (6758675255,315727256),   ## viadotto San Paolo
               (309299849, 309299130), (309299871, 371203941), (309299871, 1901929904),
               (841721800, 4072618994), (841721800, 841721729),
               (1363946984, 1363946990), (1363946984, 1363947064),
               (4064451884, 371641452), (4064451884, 518162385),
               (610320390, 4256813812), (610320390, 839604230),(488537136, 837153063),(298299791, 735994580),
               (310536545, 327445474), (369539604, 327445477),(265617677, 330970040), (310536545, 327445474),
               (33589436, 665017784), (305411588, 33589436),  ## via Etnea (in the city Center)
               (275916438, 292628356), (585163331, 275916438), (292628349, 275916438),
               (270879950, 255260622), (5654907028, 270879950),
               (2804761144, 317200805), (2804761144, 4058436906), (317200805, 2804761144), (529318415, 2804761144), (4058436906, 2804761144),
               (1284917043, 2068701503), (1284917043, 844864323), (844864323, 1284917043),
               (1385222216, 410772686), (1385222216, 1385222342), (1385222216, 1385222465), (410772674, 1385222216),
               (292898762, 6750351577), (416782575, 6582460908), (2951082981, 292898762), (6750351578, 416782575),  ### galleria San Demetrio
               (529146972, 1766060186), (529146972, 746030606), (746030606, 529146972), (1766060186, 529146972),
               (748732021, 253935547),
               (878748193, 878747885),
               (939867727, 878748175), (878748175, 939867727), (878748175, 3104823429), (878748175, 3104820703)]

## make a dataframe for list of tuples.
df_disruptions = pd.DataFrame(disruptions, columns =['u', 'v'])

### ----> add penaty to the grafo
penalty = 864000  # PENALTY time (10 days) or DISRUPTION time (seconds of closure of the link (u,v))

for u, v, key, attr in grafo.edges(keys=True, data=True):
    zipped = zip(list(df_disruptions.u), list(df_disruptions.v))
    if (u, v) in zipped:
        print(u, v)
        # print(attr)
        print("gotta!=====================================================")
        attr['VIASAT_cost'] = float(attr['VIASAT_cost']) + penalty
        print(attr)
        # print(attr.get("VIASAT_cost"))
        # break
        grafo.add_edge(u, v, key, attr_dict=attr)

########################################################################################
########## BUILD EMERGENCY PATHS #######################################################
########################################################################################

############ ////////////////////////////////////////// ###############################
##--> path Vigili del fuoco NORD -------> Monte Kà Tira A (entry point)
# path_VVF_NORD_MONTEKA = [nearest_node_first_VVF_NORD, nearest_node_first_MONTE_KA]
path_VVF_NORD_MONTEKA = [325677885, 911416281]

#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_VVF_NORD_MONTEKA, path_VVF_NORD_MONTEKA[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_VVF_NORD_MONTEKA = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_VVF_NORD_MONTEKA.to_csv('VVF_NORD_MONTEKA.csv')

VVF_NORD_MONTEKA = edges_matched_route_VVF_NORD_MONTEKA[['geometry']]
VVF_NORD_MONTEKA.columns = ['geometry']
VVF_NORD_MONTEKA = gpd.GeoDataFrame(VVF_NORD_MONTEKA)
VVF_NORD_MONTEKA.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
VVF_NORD_MONTEKA.to_file(filename='VVF_NORD_MONTEKA.geojson', driver='GeoJSON')
folium.GeoJson('VVF_NORD_MONTEKA.geojson').add_to((my_map))
my_map.save("VVF_NORD_MONTEKA.html")




############ ////////////////////////////////////////// ###############################
##--> path Monte Kà Tira (entry point) -------> Viadotto San Paolo (entry point)
# path_MONTEKA_SANPAOLO = [nearest_node_first_MONTE_KA, nearest_node_first_SANPAOLO]
path_MONTEKA_SANPAOLO = [911416281, 4096452579]

#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_MONTEKA_SANPAOLO, path_MONTEKA_SANPAOLO[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_MONTEKA_SANPAOLO = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_MONTEKA_SANPAOLO.to_csv('MONTEKA_SANPAOLO.csv')

MONTEKA_SANPAOLO = edges_matched_route_MONTEKA_SANPAOLO[['geometry']]
MONTEKA_SANPAOLO.columns = ['geometry']
MONTEKA_SANPAOLO = gpd.GeoDataFrame(MONTEKA_SANPAOLO)
MONTEKA_SANPAOLO.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
MONTEKA_SANPAOLO.to_file(filename='MONTEKA_SANPAOLO.geojson', driver='GeoJSON')
folium.GeoJson('MONTEKA_SANPAOLO.geojson').add_to((my_map))
my_map.save("MONTEKA_SANPAOLO.html")


############ ////////////////////////////////////////// ###############################
#######------------------------------------------------################################
##--> path Vigili del fuoco SUD -------> Compo sportivo Nesina (entry point)
# path_VVF_SUD_NESINA = [nearest_node_first_VVF_SUD, nearest_node_first_NESINA]
path_VVF_SUD_NESINA = [529151967, 479288949]

#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_VVF_SUD_NESINA, path_VVF_SUD_NESINA[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_VVF_SUD_NESINA = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_VVF_SUD_NESINA.to_csv('VVF_SUD_NESINA.csv')

VVF_SUD_NESINA = edges_matched_route_VVF_SUD_NESINA[['geometry']]
VVF_SUD_NESINA.columns = ['geometry']
VVF_SUD_NESINA = gpd.GeoDataFrame(VVF_SUD_NESINA)
VVF_SUD_NESINA.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
VVF_SUD_NESINA.to_file(filename='VVF_SUD_NESINA.geojson', driver='GeoJSON')
folium.GeoJson('VVF_SUD_NESINA.geojson').add_to((my_map))
my_map.save("VVF_SUD_NESINA.html")


############ ////////////////////////////////////////// ###############################
#######------------------------------------------------################################
##--> path Campo sportivo Nesina (entry point) ------->  Viadotto San Paolo
# path_NESINA_SANPAOLO = [nearest_node_first_NESINA, nearest_node_first_SANPAOLO]
path_NESINA_SANPAOLO = [479288949, 4096452579]

#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_NESINA_SANPAOLO, path_NESINA_SANPAOLO[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_NESINA_SANPAOLO = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_NESINA_SANPAOLO.to_csv('NESINA_SANPAOLO.csv')

NESINA_SANPAOLO = edges_matched_route_NESINA_SANPAOLO[['geometry']]
NESINA_SANPAOLO.columns = ['geometry']
NESINA_SANPAOLO = gpd.GeoDataFrame(NESINA_SANPAOLO)
NESINA_SANPAOLO.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
NESINA_SANPAOLO.to_file(filename='NESINA_SANPAOLO.geojson', driver='GeoJSON')
folium.GeoJson('NESINA_SANPAOLO.geojson').add_to((my_map))
my_map.save("NESINA_SANPAOLO.html")



############ ////////////////////////////////////////// ###############################
#######------------------------------------------------################################
##--> path Vigili Del Fuoco Distaccamento Catania Sud  ------->  Interporto (Entry Point)
path_VVF_SUD_INTERPORTO = [529151967, 518184285]

#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_VVF_SUD_INTERPORTO, path_VVF_SUD_INTERPORTO[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_VVF_SUD_INTERPORTO = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_VVF_SUD_INTERPORTO.to_csv('VVF_SUD_INTERPORTO.csv')

VVF_SUD_INTERPORTO = edges_matched_route_VVF_SUD_INTERPORTO[['geometry']]
VVF_SUD_INTERPORTO.columns = ['geometry']
VVF_SUD_INTERPORTO = gpd.GeoDataFrame(VVF_SUD_INTERPORTO)
VVF_SUD_INTERPORTO.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
VVF_SUD_INTERPORTO.to_file(filename='VVF_SUD_INTERPORTO.geojson', driver='GeoJSON')
folium.GeoJson('VVF_SUD_INTERPORTO.geojson').add_to((my_map))
my_map.save("VVF_SUD_INTERPORTO.html")




############ ////////////////////////////////////////// ###############################
#######------------------------------------------------################################
##--> path Vigili Del Fuoco Distaccamento Lentini  ------->  Sicula Trasporti Stadium (Entry Point)
path_VVF_LENTINI_STADIUM = [940680432, 939867608]

#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_VVF_LENTINI_STADIUM, path_VVF_LENTINI_STADIUM[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_VVF_LENTINI_STADIUM = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_VVF_LENTINI_STADIUM.to_csv('VVF_LENTINI_STADIUM.csv')

VVF_LENTINI_STADIUM = edges_matched_route_VVF_LENTINI_STADIUM[['geometry']]
VVF_LENTINI_STADIUM.columns = ['geometry']
VVF_LENTINI_STADIUM = gpd.GeoDataFrame(VVF_LENTINI_STADIUM)
VVF_LENTINI_STADIUM.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
VVF_LENTINI_STADIUM.to_file(filename='VVF_LENTINI_STADIUM.geojson', driver='GeoJSON')
folium.GeoJson('VVF_LENTINI_STADIUM.geojson').add_to((my_map))
my_map.save("VVF_LENTINI_STADIUM.html")



############ ////////////////////////////////////////// ###############################
#######------------------------------------------------################################
##--> path Vigili Del Fuoco Distaccamento Catania Sud  ------->   Galleria San Demetrio
path_VVF_SUD_SanDemetrio = [529151967, 416782575]   ## 416782575, 6582460908
#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_VVF_SUD_SanDemetrio, path_VVF_SUD_SanDemetrio[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_VVF_SUD_SanDemetrio = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_VVF_SUD_SanDemetrio.to_csv('VVF_SUD_SanDemetrio.csv')

VVF_SUD_SanDemetrio = edges_matched_route_VVF_SUD_SanDemetrio[['geometry']]
VVF_SUD_SanDemetrio.columns = ['geometry']
VVF_SUD_SanDemetrio = gpd.GeoDataFrame(VVF_SUD_SanDemetrio)
VVF_SUD_SanDemetrio.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
VVF_SUD_SanDemetrio.to_file(filename='VVF_SUD_SanDemetrio.geojson', driver='GeoJSON')
folium.GeoJson('VVF_SUD_SanDemetrio.geojson').add_to((my_map))
my_map.save("VVF_SUD_SanDemetrio.html")




############ ////////////////////////////////////////// ###############################
#######------------------------------------------------################################
##--> path Vigili Del Fuoco Distaccamento Lentini  ------->   Galleria San Demetrio
path_VVF_LENTINI_SanDemetrio = [940680432, 416782575]   ## 416782575, 6582460908
#### build matched route with all max_prob_node  #####
matched_route = []
all_matched_edges = []
for origin, destination in zip(path_VVF_LENTINI_SanDemetrio, path_VVF_LENTINI_SanDemetrio[1:]):
    try:
        print(origin, destination)
        # use full complete graph to build the final path
        route = nx.dijkstra_path(grafo, origin, destination, weight='VIASAT_cost')
        # route = nx.shortest_path(grafo, origin, destination, weight='VIASAT_cost')
        path_edges = list(zip(route, route[1:]))
        # print(path_edges)
        all_matched_edges.append(path_edges)
        matched_route.append(route)
    except nx.NetworkXNoPath:
        print('No path', 'u:', origin, 'v:', destination)

if len(all_matched_edges) > 0:
    # isolate edges in the grafo from 'all_matched_edges'
    df_nodes = []
    for i in range(len(all_matched_edges)):
        # print(all_matched_edges[i])
        route = all_matched_edges[i]
        for nodes in route:
            # print('nodes:',nodes)
            df_nodes.append(nodes)

    df_nodes = pd.DataFrame(df_nodes)
    df_nodes.columns = ['u', 'v']

edges_matched_route_VVF_LENTINI_SanDemetrio = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
## save csv file
edges_matched_route_VVF_LENTINI_SanDemetrio.to_csv('VVF_LENTINI_SanDemetrio.csv')

VVF_LENTINI_SanDemetrio = edges_matched_route_VVF_LENTINI_SanDemetrio[['geometry']]
VVF_LENTINI_SanDemetrio.columns = ['geometry']
VVF_LENTINI_SanDemetrio = gpd.GeoDataFrame(VVF_LENTINI_SanDemetrio)
VVF_LENTINI_SanDemetrio.plot()
################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
# save first as geojson file
VVF_LENTINI_SanDemetrio.to_file(filename='VVF_LENTINI_SanDemetrio.geojson', driver='GeoJSON')
folium.GeoJson('VVF_LENTINI_SanDemetrio.geojson').add_to((my_map))
my_map.save("VVF_LENTINI_SanDemetrio.html")




############ \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ ###############################
############ ////////////////////////////////////////// ###############################
############ \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ ###############################
############ ////////////////////////////////////////// ###############################
#### ----->  VULNERABILITY --------- ##################################################

"""
vulnerability_all_AUGUST_2019 = pd.read_sql_query('''
                       SELECT *
                           FROM "vulnerability_all_OD_AUGUST_2019" ''', conn_HAIG)
"""

vulnerability_all_FEBRUARY_2019 = pd.read_sql_query('''
                       SELECT *
                           FROM "vulnerability_all_OD_FEBRUARY_2019_NEW" ''', conn_HAIG)

# vulnerabilty_2019 = vulnerability_all_AUGUST_2019
vulnerabilty_2019 = vulnerability_all_FEBRUARY_2019


vulnerabilty_2019['u'] = vulnerabilty_2019.u.astype(np.int64)
vulnerabilty_2019['v'] = vulnerabilty_2019.v.astype(np.int64)

########################################################
##### build the map ####################################

# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, gdf_edges, on=['u', 'v'], how='left')
vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_MONTEKA_SANPAOLO, on=['u', 'v'], how='left')
# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_NESINA_SANPAOLO, on=['u', 'v'], how='left')
# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_VVF_SUD_SanDemetrio, on=['u', 'v'], how='left')
# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_VVF_LENTINI_SanDemetrio, on=['u', 'v'], how='left')

## get only rows with 'geometry != NaN
vulnerabilty_2019 = vulnerabilty_2019[vulnerabilty_2019['geometry'].notna()]
vulnerabilty_2019 = gpd.GeoDataFrame(vulnerabilty_2019)
vulnerabilty_2019.drop_duplicates(['u', 'v'], inplace=True)
# vulnerabilty_2019.plot()


## rescale all data by an arbitrary number
vulnerabilty_2019["scales"] = round(((vulnerabilty_2019.importance/max(vulnerabilty_2019.importance)) * 3) + 0.1 ,4)

# add colors based on 'importance' (vehicles*hours)
vmin = min(vulnerabilty_2019.scales)   # -0.4
vmax = max(vulnerabilty_2019.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
vulnerabilty_2019['color'] = vulnerabilty_2019['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
vulnerabilty_2019['importance'] = round(vulnerabilty_2019['importance'], 0)

## Normalize to 1
vulnerabilty_2019["vulnerability"] = round(vulnerabilty_2019["importance"]/max(vulnerabilty_2019["importance"]), 4)


################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
vulnerabilty_2019[['u','v', 'importance', 'scales', 'vulnerability', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'black',
        'color': 'black',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'vulnerability']),
    ).add_to(my_map)


########################################
# make a colored map  ##################
########################################

my_map = plot_graph_folium_FK(vulnerabilty_2019, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=4, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    vulnerabilty_2019[['u','v', 'importance', 'scales', 'vulnerability', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'vulnerability']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'

# my_map.save(path + "vulnerability_NESINA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_NESINA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_MONTEKA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_MONTEKA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_VVF_SUD_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_VVF_SUD_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_VVF_LENTINI_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_VVF_LENTINI_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.html")


# save first as geojson file
vulnerabilty_2019 = vulnerabilty_2019[['u', 'v', 'CELL', 'vulnerability', 'geometry']]
# vulnerabilty_2019.to_file(filename='vulnerability_MONTEKA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# vulnerabilty_2019.to_file(filename='vulnerability_MONTEKA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# vulnerabilty_2019.to_file(filename='vulnerability_NESINA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
vulnerabilty_2019.to_file(filename='vulnerability_NESINA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# vulnerabilty_2019.to_file(filename='vulnerability_VVF_SUD_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# vulnerabilty_2019.to_file(filename='vulnerability_VVF_SUD_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# vulnerabilty_2019.to_file(filename='vulnerability_VVF_LENTINI_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# vulnerabilty_2019.to_file(filename='vulnerability_VVF_LENTINI_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')






############ \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ ###############################
############ ////////////////////////////////////////// ###############################

## make maps with hexagonal CELL
#################################

os.getcwd()


"""
vulnerability_all_AUGUST_2019 = pd.read_sql_query('''
                       SELECT *
                           FROM "vulnerability_all_OD_AUGUST_2019" ''', conn_HAIG)
"""

vulnerability_all_FEBRUARY_2019 = pd.read_sql_query('''
                       SELECT *
                           FROM "vulnerability_all_OD_FEBRUARY_2019_NEW" ''', conn_HAIG)

# vulnerabilty_2019 = vulnerability_all_AUGUST_2019
vulnerabilty_2019 = vulnerability_all_FEBRUARY_2019

vulnerabilty_2019['u'] = vulnerabilty_2019.u.astype(np.int64)
vulnerabilty_2019['v'] = vulnerabilty_2019.v.astype(np.int64)


### load all hexagonal cells (hex_grid)
hex_grid = gpd.read_file("hex_grid_400m_FEBRUARY.geojson")
# hex_grid = gpd.read_file("hex_grid_400m_AUGUST.geojson")


hex_grid['CELL'] = hex_grid.index
# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_MONTEKA_SANPAOLO, on=['u', 'v'], how='left')
# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_NESINA_SANPAOLO, on=['u', 'v'], how='left')
# vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_VVF_SUD_SanDemetrio, on=['u', 'v'], how='left')
vulnerabilty_2019 = pd.merge(vulnerabilty_2019, edges_matched_route_VVF_LENTINI_SanDemetrio, on=['u', 'v'], how='left')


## get only rows with 'geometry != NaN
vulnerabilty_2019 = vulnerabilty_2019[vulnerabilty_2019['geometry'].notna()]
vulnerabilty_2019 = gpd.GeoDataFrame(vulnerabilty_2019)
vulnerabilty_2019.drop_duplicates(['u', 'v'], inplace=True)


CELLS_vulnerabilty_2019 = pd.merge(vulnerabilty_2019[['CELL', 'importance']], hex_grid, on=['CELL'], how='inner')
CELLS_vulnerabilty_2019 = gpd.GeoDataFrame(CELLS_vulnerabilty_2019)
# CELLS_vulnerabilty_2019.plot()

## rescale all data by an arbitrary number
CELLS_vulnerabilty_2019["scales"] = round(((CELLS_vulnerabilty_2019.importance/max(CELLS_vulnerabilty_2019.importance)) * 3) + 0.1 ,4)
CELLS_vulnerabilty_2019["vulnerability"] = round(CELLS_vulnerabilty_2019["importance"]/max(CELLS_vulnerabilty_2019["importance"]), 4)


## make a color map for the link importance (vehicles*hours) of each element in each hexagonal cell
# add colors based on 'importance' (vehicles*hours)
vmin = min(CELLS_vulnerabilty_2019.scales)   # -0.4
vmax = max(CELLS_vulnerabilty_2019.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black
CELLS_vulnerabilty_2019['colors'] = CELLS_vulnerabilty_2019['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
CELLS_vulnerabilty_2019['importance'] = round(CELLS_vulnerabilty_2019['importance'], 0)

################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################
#################################################################################

CELLS_vulnerabilty_2019 = CELLS_vulnerabilty_2019.rename(columns = {'importance':'importance (vei*hour)'})

folium.GeoJson(
CELLS_vulnerabilty_2019[['CELL', 'importance (vei*hour)', 'colors', 'scales', 'vulnerability',
                             'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': x['properties']['colors'],
        'color': x['properties']['colors'],
        'weight':  0.8,
        'fillOpacity': 0.1,
        },
highlight_function=lambda x: {'weight':1,
        'color':'blue',
        'fillOpacity':0.1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['CELL', 'vulnerability']),
    ).add_to(my_map)


folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'

# my_map.save(path + "vulnerability_CELL_NESINA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_CELL_NESINA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_CELL_MONTEKA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_CELL_MONTEKA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_CELL_VVF_SUD_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_CELL_VVF_SUD_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.html")
# my_map.save(path + "vulnerability_CELL_VVF_LENTINI_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.html")
my_map.save(path + "vulnerability_CELL_VVF_LENTINI_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.html")



# save first as geojson file
CELLS_vulnerabilty_2019 = CELLS_vulnerabilty_2019[['CELL', 'vulnerability', 'geometry']]
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_MONTEKA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_MONTEKA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_NESINA_SANPAOLO_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_NESINA_SANPAOLO_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_VVF_SUD_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_VVF_SUD_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
# CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_VVF_LENTINI_SanDemetrio_FEBRUARY_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')
CELLS_vulnerabilty_2019.to_file(filename='vulnerability_CELL_VVF_LENTINI_SanDemetrio_AUGUST_DISRUPTION_SAN_PAOLO.geojson', driver='GeoJSON')











############ -------------------------------- ##########################
########################################################################
########################################################################
####### FIND NEAREST NODES and EDGES ###################################
########################################################################
########### --------------------------------- ##########################


'''
###---> get nearest node @ Vigili Del Fuoco Distaccamento Cittadino Catania Nord  (lat: 37.54418693580304, lon: 15.047406661379835)
lat0 = float(37.54418693580304)
lon0 = float(15.047406661379835)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_VVF_NORD = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Vigili Del Fuoco Distaccamento Cittadino Catania Nord: ", u0, v0, "first node ---> ", nearest_node_first_VVF_NORD)
## edge Vigili Del Fuoco Distaccamento Cittadino Catania Nord:  325677885 839609163 first node --->  325677885


###---> get nearest node @ Vigili Del Fuoco Distaccamento Catania Sud  (lat: 37.45053484829888, lon: 15.048439101472548)
lat0 = float(37.45053484829888)
lon0 = float(15.048439101472548)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_VVF_SUD = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Vigili Del Fuoco Distaccamento Cittadino Catania Sud: ", u0, v0, "first node ---> ", nearest_node_first_VVF_SUD)
## edge Vigili Del Fuoco Distaccamento Cittadino Catania Sud:  4041146072 529151967 first node --->  529151967


###---> get nearest node @ Monte Kà Tira A  (lat: 37,5620464376290, lon: 15,1028828307122)
lat0 = float(37.5620464376290)
lon0 = float(15.1028828307122)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_MONTE_KA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge MONTE KA TIRA: ", u0, v0, "first node ---> ", nearest_node_first_MONTE_KA)
## edge MONTE KA TIRA:  911406283 911416281 first node --->  911416281


###---> get nearest node @ Campo sportivo Nesina  (lon: 15.04598038	lat: 37.51700187)
lat0 = float(37.51700187)
lon0 = float(15.04598038)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_NESINA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Campo sportivo Nesina: ", u0, v0, "first node ---> ", nearest_node_first_NESINA)
## edge Campo sportivo Nesina:  479288949 4064452213 first node --->  479288949



###---> get nearest node @ Viadotto San Paolo  (lon: 15.073568	lat: 37.549831)
lat0 = float(37.549831)
lon0 = float(15.073568)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_SANPAOLO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Viadotto San Paolo: ", u0, v0, "first node ---> ", nearest_node_first_SANPAOLO)
## edge Viadotto San Paolo:  841721621 6758675255 first node --->  6758675255


###############################################################################################
###############################################################################################
###############################################################################################
#### DISRUPTIONS on the EDGES along EMERGENCY PATHS ###########################################


###---> get nearest node @ Via Barriera del Bosco  (lon: 15.07919837530,	lat: 37.54454377390 )
lat0 = float(37.54454377390)
lon0 = float(15.07919837530)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_BARRIERA_BOSCO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Barriera del Bosco: ", u0, v0, "first node ---> ", nearest_node_first_BARRIERA_BOSCO)
## Via Barriera del Bosco:  309299130 309299849 first node --->  309299130 (also 309299849, 309299130)



###---> get nearest node @ Via Leucatia  (lon: 15.08162300000,	lat:37.53970300000)
lat0 = float(37.53970300000)
lon0 = float(15.08162300000)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_LEUCATIA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Leucatia: ", u0, v0, "first node ---> ", nearest_node_first_LEUCATIA)
## edge Via Leucatia:  1901929906 309299871  first node --->  309299871 (also 309299871, 371203941)
## also (309299871, 1901929904)



###---> get nearest node @ Via Pietra dell'Ova  (lon: 15.09049322350	lat: 37.5406459159)
lat0 = float(37.5406459159)
lon0 = float(15.09049322350)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_PIETRA_DELLOVA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Pietra dell'Ova: ", u0, v0, "first node ---> ", nearest_node_first_PIETRA_DELLOVA)
## edge Via Pietra dell'Ova:  4072618994 841721800   first node --->  841721800
## also (841721800, 4072618994) and (841721800, 841721729)



###---> get nearest node @ Via Pietro Novelli  (lon: 15.08552448740	lat:37.5384952460)
lat0 = float(37.5384952460)
lon0 = float(15.08552448740)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_PIETRO_NOVELLI = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Pietro Novelli: ", u0, v0, "first node ---> ", nearest_node_first_PIETRO_NOVELLI)
## edge Via Pietro Novelli:  1363946984 1363946943 first node --->  1363946984
## also (1363946984, 1363946990) and (1363946984, 1363947064)



###---> get nearest node @ E45  (lon: 15.01936964550	37.512480602500)
lat0 = float(37.512480602500)
lon0 = float(15.01936964550)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_firs_E45 = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge E45: ", u0, v0, "first node ---> ", nearest_node_firs_E45)
## edge E45:  476455543 4064451884   first node --->  4064451884
##also (4064451884, 371641452) and (4064451884, 518162385)


###---> get nearest node Corso Carlo Marx  (lon: 15.02385100310	lat:37.51466377020)
lat0 = float(37.51466377020)
lon0 = float(15.02385100310)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_firs_MARX = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Corso Carlo Marx: ", u0, v0, "first node ---> ", nearest_node_firs_MARX)
## Corso Carlo Marx:  305783473 610320390   first node --->  610320390
## also (610320390, 4256813812) and (610320390, 839604230)


###---> get nearest node Via Lorenzo Bolano  (lon: 15.057941790	lat: 37.5186293616)
lat0 = float(37.5186293616)
lon0 = float(15.057941790)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_BOLANO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Lorenzo Bolano: ", u0, v0, "first node ---> ", nearest_node_first_BOLANO)
## edge Via Lorenzo Bolano:  488537136 1767590558 first node --->  488537136 (also 488537136, 837153063)


###---> get nearest node Via Giovanni Battista  (lon: 15.052184987	lat: 37.54852251)
lat0 = float(37.54852251)
lon0 = float(15.052184987)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_BATTISTA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Giovanni Battista: ", u0, v0, "first node ---> ", nearest_node_first_BATTISTA)
## edge Via Giovanni Battista:  298299791 298300329 first node --->  298299791
## also (298299791, 735994580)


###---> get nearest node Via Ulisse  (lon: 15.106049484	lat: 37.53176146)
lat0 = float(37.53176146)
lon0 = float(15.106049484)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_ULISSE = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Ulisse: ", u0, v0, "first node ---> ", nearest_node_first_ULISSE)
## edge Via Ulisse:  567758808 310536545 first node --->  310536545
## also (310536545, 327445474) (369539604, 327445477) (265617677, 330970040) (310536545, 327445474)




###---> get nearest node Via Etnea  (lon: 15.086192222	lat:37.50739554)
lat0 = float(37.50739554)
lon0 = float(15.086192222)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_ETNEA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Etnea: ", u0, v0, "first node ---> ", nearest_node_first_ETNEA)
## edge Via Etnea:  33589436 254098470 first node --->  33589436
## also (33589436, 665017784), (305411588, 33589436)



###---> get nearest node SS114  (lon: 15.089414816	lat: 37.50079821)
lat0 = float(37.50079821)
lon0 = float(15.089414816)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_SS114 = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge SS114: ", u0, v0, "first node ---> ", nearest_node_first_SS114)
## edge  SS114:  275916438 343415692 first node --->  275916438
## also (275916438, 292628356), (585163331, 275916438), (292628349, 275916438)


###---> get nearest node Via alla RENA  (lon: 15.075816563	lat: 37.48036824)
lat0 = float(37.48036824)
lon0 = float(15.075816563)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_RENA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge RENA: ", u0, v0, "first node ---> ", nearest_node_first_RENA)
## RENA:  270879950 879521951 first node --->  270879950
## also (270879950, 255260622), (5654907028, 270879950)



###---> get nearest node Via Zia Liisa  (lon: 15.069131137	lat: 37.48401178)
lat0 = float(37.48401178)
lon0 = float(15.069131137)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_LIISA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Zia Liisa: ", u0, v0, "first node ---> ", nearest_node_first_LIISA)
## edge Via Zia Liisa:  529318415 2804761144 first node --->  2804761144
## also (2804761144, 317200805), (2804761144, 4058436906), (317200805, 2804761144), (529318415, 2804761144), (4058436906, 2804761144)



###---> get nearest node Via Madonna Lacrime  (lon: 15.096785	lat: 37.552916)
lat0 = float(37.552916)
lon0 = float(15.096785)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_LACRIME = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via Madonna Lacrime 89: ", u0, v0, "first node ---> ", nearest_node_first_LACRIME)
## edge Via Madonna Lacrime 89:  2068701503 1284917043 first node --->  1284917043
## also (1284917043, 2068701503), (1284917043, 844864323), (844864323, 1284917043)


###---> get nearest node Via del Santuario  (lon: 15.12533	lat: 37.582704)
lat0 = float(37.582704)
lon0 = float(15.12533)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_first_SANTUARIO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Via del Santuario: ", u0, v0, "first node ---> ", nearest_node_first_SANTUARIO)
## edge Via del Santuario:  1385222216 410772674 first node --->  1385222216
### also (1385222216, 410772686), (1385222216, 1385222342), (1385222216, 1385222465), (410772674, 1385222216)



###---> get nearest node entrata tangenziale catania (Viadotto San Paolo) da via Etnea  (lat: 37.54702551307489, lon :15.066897512475823 )
lat0 = float(37.54702551307489)
lon0 = float(15.066897512475823)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_Tang_San_Paolo = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge Tang_San_Paolo: ", u0, v0, "first node ---> ", nearest_node_Tang_San_Paolo)
## edge Tang_San_Paolo:  312410905 4096452579 first node --->  312410905



###---> get nearest node Vigili Del Fuoco Distaccamento Lentini  (lat: 14.98827115804	lon: 37.30034341828)
lat0 = float(37.30034341828)
lon0 = float(14.98827115804)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_VVF_SUD_LENTINI = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge TVVF_SUD_LENTINI: ", u0, v0, "first node ---> ", nearest_node_VVF_SUD_LENTINI)
## edge TVVF_SUD_LENTINI:  940679848 940680432 first node --->  940680432


###---> get nearest node INTERPORTO (lat: 15.0476664552137	lon: 37.4443115326635)
lat0 = float(37.4443115326635)
lon0 = float(15.0476664552137)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_INTERPORTO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_INTERPORTO: ", u0, v0, "first node ---> ", nearest_node_INTERPORTO)
## edge nearest_node_INTERPORTO:  593906277 518184285 first node --->  518184285


###---> get nearest node Sicula Trasporti Stadium (lat: 14.9965172	37.28917854)
lat0 = float(37.28917854)
lon0 = float(14.9965172)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_SICULA_TRASPORTI = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_SICULA_TRASPORTI: ", u0, v0, "first node ---> ", nearest_node_SICULA_TRASPORTI)
## edge nearest_node_SICULA_TRASPORTI:  878748331 939867608 first node --->  939867608


###---> get nearest node GALLERIA SAN DEMETRIO  (lat: 15.046953	37.36077) (37.360928027717996, 15.04742161503255
lat0 = float(37.360928027717996)
lon0 = float(15.04742161503255)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_SAN_DEMETRIO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_SAN_DEMETRIO: ", u0, v0, "first node ---> ", nearest_node_SAN_DEMETRIO)
## edge edge nearest_node_SAN_DEMETRIO:  292898762 6750351577 first node --->  6750351577
## edge nearest_node_SAN_DEMETRIO:  416782575 6582460908 first node --->  416782575



###---> get nearest node SP69ii (lat: 15.020345883	37.4053669732) 
lat0 = float(37.4053669732)
lon0 = float(15.020345883)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_SP69ii = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_SP69ii: ", u0, v0, "first node ---> ", nearest_node_SP69ii)
## edge nearest_node_SP69ii:  529146972 1766060186 first node --->  1766060186



###---> get nearest node SS114 (lat: 15.065080051	37.4027033428) 
lat0 = float(37.4027033428)
lon0 = float(15.065080051)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_SP114 = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_SP114: ", u0, v0, "first node ---> ", nearest_node_SP114)
## edge nearest_node_SP114:  748732021 253935547 first node --->  253935547


###---> get nearest node via Murganzio (lat: 14.99789700	37.285847000) 
lat0 = float(37.285847000)
lon0 = float(14.99789700)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_MURGANZIO = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_MURGANZIO: ", u0, v0, "first node ---> ", nearest_node_MURGANZIO)
## edge nearest_node_MURGANZIO:  878748193 878747885 first node --->  878748193




###---> get nearest node via Etnea (lat: 14.999859	37.291259) 
lat0 = float(37.291259)
lon0 = float(14.999859)
point0 = (lat0, lon0)
geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
nearest_node_ETNEA = min((u0, v0),
                         key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
print("edge nearest_node_ETNEA: ", u0, v0, "first node ---> ", nearest_node_ETNEA)
## edge nearest_node_ETNEA:  939867727 878748175 first node --->  878748175

'''




'''

#########################################
###### EASSY PLOT #######################
#########################################

data = {'u':  [u0],
        'v': [v0],
        'geometry': [geom]}
nearest_node_ETNEA = gpd.GeoDataFrame(data)
nearest_node_ETNEA.plot()

################################################################################
# create basemap CATANIA
ave_LAT = 37.510284
ave_LON = 15.092042
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
nearest_node_ETNEA[['u','v', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': '#00000000',
        'color': '#00000000',
        'weight':  x['properties']['u'],
        'fillOpacity': 0,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':0
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v']),
    ).add_to(my_map)


my_map.save("selected_edge_PIANO_EMERGENZA_Ciarallo.html")


'''