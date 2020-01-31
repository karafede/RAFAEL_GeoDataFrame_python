
import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL')

from datetime import datetime
import psycopg2
import db_connect
from sklearn.metrics import silhouette_score
from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist
import math
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import LinearRing
from shapely.geometry import MultiLineString
import csv
import datetime
import folium
import osmnx as ox
import networkx as nx
import matplotlib.cm as cm
import matplotlib.colors as colors
from itertools import chain
from colour import Color
from funcs_network_FK import roads_type_folium
import hashlib
from pyproj import Proj, transform
from scipy import spatial
import math
import queue

# set of neighbors (viasat measurments) of a node in the graph

file_graphml = 'Catania__Italy_cost.graphml'
# viasat_data = "viasat_max_data.csv"
viasat_data = "viasat_max_data_short.csv"
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)

# make a geodataframe from the grapho
gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

fields = ["longitude", "latitude"]
viasat = pd.read_csv(viasat_data, usecols=fields)

# create an index column
viasat["ID"] = viasat.index

######################################################

# build a geodataframe with VIASAT data
geometry = [Point(xy) for xy in zip(viasat.longitude, viasat.latitude)]
# viasat = viasat.drop(['longitude', 'latitude'], axis=1)
crs = {'init': 'epsg:4326'}
viasat_gdf = GeoDataFrame(viasat, crs=crs, geometry=geometry)
# viasat_gdf.plot()

# Buffer the points by some units (unit is kilometer)
buffer = viasat_gdf.buffer(0.0005)  #50 meters # this is a geoseries
# buffer.plot()
# make a dataframe
buffer_viasat = pd.DataFrame(buffer)
buffer_viasat.columns = ['geometry']
type(buffer_viasat)
# transform a geoseries into a geodataframe
# https://gis.stackexchange.com/questions/266098/how-to-convert-a-geoserie-to-a-geodataframe-with-geopandas

## circumscript the area of the track (buffer zone)
# union = buffer.unary_union
# envelope = union.envelope
# rectangle_viasat = gpd.GeoDataFrame(geometry=gpd.GeoSeries(envelope))
# rectangle_viasat.plot()

# geodataframe with edges
type(gdf_edges)
# gdf_edges.plot()


from datetime import datetime

buff = []
index_edges = []
index_buff = []
edge = []
DISTANCES = []
now1 = datetime.now()

for index1, streets in gdf_edges.iterrows():
    for index2, via_buff in buffer_viasat.iterrows():
        if streets['geometry'].intersects(via_buff['geometry']) is True:
            print("OK=======================OK")
            index_edges.append(index1)
            index_buff.append(index2)
            STREET = streets.u, streets.v, index2
            # get distance between Viasat measurement and edge
            distance = (Point(viasat[['longitude', 'latitude']].iloc[index2]).distance(streets.geometry))*100000 # roughly meter conversion
            print("distance track-edge: ", distance, " meters")
            edge.append(STREET)
            distance = distance, index2
            DISTANCES.append(distance)
            # list all buffers in sequence
            buff.append(via_buff.name)
now2 = datetime.now()
print(now2 - now1)

# 1 = 100 km
# 0.1 = 10 km
# 0.01 = 1 km
# 0.001 = 100m
# 0.0001 = 10m
# 0.00001 = 1m


## filter gdf_edges based on index_edges (edges in the buffer)
## near neighbour edges (near the viasat measurements)
nn_gdf_edges = gdf_edges[gdf_edges.index.isin(index_edges)]
## plot selects edges
nn_gdf_edges.plot()

'''
#############################
# plot in a Folium map ######
#############################

# make main map
place_country = "Catania, Italy"
road_type = "motorway, motorway_link, secondary, primary, tertiary"
file_graphml = 'Catania__Italy_cost.graphml'
my_map = roads_type_folium(file_graphml, road_type, place_country)

# add neighbour edges crossed by the buffer of the viasat data
points = []
for i in range(len(nn_gdf_edges)):
    nn_edges = ox.make_folium_polyline(edge=nn_gdf_edges.iloc[i], edge_color="yellow", edge_width=1,
                                            edge_opacity=1, popup_attribute=None)
    points.append(nn_edges.locations)
folium.PolyLine(points, color="yellow", weight=2, opacity=1).add_to(my_map)

# add buffered viasat polygons
# save first as geojson file
buffer.to_file(filename='buffer_viasat.geojson', driver='GeoJSON')
folium.GeoJson('buffer_viasat.geojson').add_to((my_map))
my_map.save("near_neighbours_Catania.html")

'''

##########################################################
########## VALHALL ALGORITHM MAP MATCHING  ###############
##########################################################

from math import radians, cos, sin, asin, sqrt

# sort edges and associated buffer (first buffer is the Number 43)
df_edges = pd.DataFrame(edge)
df_edges.columns = ['u', 'v', 'buffer_ID']
df_edges.sort_values(by=['buffer_ID'], inplace=True)

# sort df by u and v
# df_edges.sort_values(['u','v'],ascending=False, inplace=True)

# make a dictionary: for each buffer/track/measurement (key) assign u and v
ID_TRACK = list(df_edges.buffer_ID.unique())
df_edges_dict = {}
keys = ID_TRACK
for track in keys:
        df_edges_dict[track] = df_edges[['u', 'v']][df_edges['buffer_ID']==track ].values.tolist()
print(df_edges_dict)


# nodes associated to tracks
nodes_u = list(df_edges.u.unique())
u_dict = {}
keys = nodes_u
for u in keys:
        u_dict[u] = df_edges[df_edges['u']==u ].values.tolist()
print(u_dict)
# u_track = u_dict.get(u)[0][2]


# define distance between GPS track (viasat measurements) and node
def great_circle_track_node(u):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    u_track = u_dict.get(u)[0][2]
    coords_track = viasat[viasat.ID == u_track].values.tolist()
    lon_track = coords_track[0][1]
    lat_track = coords_track[0][0]
    coords_u = gdf_nodes[gdf_nodes.index == u][['x', 'y']].values.tolist()
    lon_u = coords_u[0][0]
    lat_u = coords_u[0][1]
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon_track, lat_track, lon_u, lat_u])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r # Kilometers


# define distance between two GPS tracks (viasat measurements)
def great_circle_track(u, v):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    u_track = u_dict.get(u)[0][2]
    # v_track = u_dict.get(v)[0][2]
    v_track = u_track+1
    coords_track_u = viasat[viasat.ID == u_track].values.tolist()
    lon_track_u = coords_track_u[0][1]
    lat_track_u = coords_track_u[0][0]
    coords_track_v = viasat[viasat.ID == v_track].values.tolist()
    if len(coords_track_v) > 0:
        lon_track_v = coords_track_v[0][1]
        lat_track_v = coords_track_v[0][0]
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon_track_u, lat_track_u, lon_track_v, lat_track_v])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371 # Radius of earth in kilometers. Use 3956 for miles
        return c * r # Kilometers
    return 0


# "sigma" has been calculated ad the standard deviation of all the distances between viasat measurements and nodes
# SIGMA_Z = 1.4826*np.median(DISTANCES) # meters
SIGMA_Z = 1.4826*np.median([x[0] for x in DISTANCES]) # meters
SIGMA_Z = SIGMA_Z/1000 #Kilometers
print(SIGMA_Z)


###############################
### emission probability ######
###############################

# Gaussian distribution of all NODES close to Viasat measurements.
def emission_prob(u):
    c = 1 / (SIGMA_Z * math.sqrt(2 * math.pi))
    return c * math.exp(-0.5*(great_circle_track_node(u)/SIGMA_Z)**2)


prob = []
for u in u_dict:
    emiss_prob = emission_prob(u)
    prob.append(emiss_prob)
    # print(prob)
print("max_probability: ", max(prob))
print("min_probability: ", min(prob))


#################################
### Transition probability ######
#################################

# transition probability (probability that the distance u-->v is from the mesasurements's distances at nodes u and v
def transition_prob(u, v):
    BETA = 1
    c = 1 / BETA
    # Calculating route distance is expensive.
    # We will discuss how to reduce the number of calls to this function later.
    # distance on the route
    delta = abs(nx.shortest_path_length(grafo, u, v, weight='length')/1000 -
                great_circle_track(u, v))  # in Kilometers
    return c * math.exp(-delta)


# calculate BETA
deltaB = []
for u in u_dict:
    for v in [item[1] for item in u_dict.get(u)]:
        LEN_ROUTE = nx.shortest_path_length(grafo, u, v, weight='length') / 1000  # in Km
        print("Len_Route", LEN_ROUTE)  # in Km
        # distance on the sphere (cartesian distance)
        DIST = great_circle_track(u, v)  # in Km
        if DIST != None:
            delta = abs(DIST - LEN_ROUTE)
            print(DIST, "=============================")  # in Km
            print("DELTA: ", delta)  # in Km
            deltaB.append(delta)
    BETA = (1 / math.log(2)) * np.median(deltaB)
    print("BETA: ", BETA)


trans_prob = []
for u in u_dict:
    for v in [item[1] for item in u_dict.get(u)]:
        t_prob = transition_prob(u, v)
        print(t_prob)
        trans_prob.append(t_prob)
print("max_transition_prob: ", max(trans_prob))
print("min_transition_prob: ", min(trans_prob))


# build ADJACENCY LIST (all possible paths between nodes from u ---> v) (list all paths in between)
# df_edges adjacent list with GPS tracks ordered by priority of appearance
adjacency_list = {}
all_routes = dict()
df_edges.sort_values(by=['buffer_ID'], inplace=True)
track_list = list(df_edges.buffer_ID.unique())
for track in track_list:
    print(track)
    # filter dataframe
    df = df_edges[df_edges['buffer_ID'] == track][['u', 'v']]
    u_list = list(df.u.unique())
    for u in u_list:
        print(u)
        df2 = df[df.u == u][['u', 'v']]
        adjacency_list[u] = df2.values.tolist()


##########################
# VITERBI algorithm ######
##########################

del u
del v

# s = 3987101865
s = 4277112580
t = 2941239107
# t = 1836387039
# t = 1836387053
t = 891536279
t = 2941259032


def viterbi_search(adjacency_list, s, t):
    # Initialize joint probability for each node
    joint_prob = {}
    for u in adjacency_list:
        joint_prob[u] = 0
    # predecessor = {}
    matched_edges = []
    q = list()

    if adjacency_list.get(s) is not None:
        q.append(s)
    else:
        next_edge = int(df_edges[df_edges['v'] == s]['buffer_ID'] + 1)
        s = df_edges[df_edges['buffer_ID'] == next_edge].iloc[0]['u']
        q.append(s)

    # joint_prob[s] = emission_prob(s)
    joint_prob[s] = 1
    # predecessor[s] = None
    u = s
    # pred = []
    for v in [x[1] for x in adjacency_list.get(u)]:
        if adjacency_list.get(v) is not None:
            # print(v)
            q.append(v)

    if adjacency_list.get(t) is not None:
        q.append(t)

    while len(q) !=0:
        u = q.pop()

        for v in [x[1] for x in adjacency_list.get(u)]:
            # print(v)
            if adjacency_list.get(v) is not None:
                print(v)
                # pred.append(v)
                # new_prob = joint_prob[u] * transition_prob(u, v) * emission_prob(v)
                # new_prob = joint_prob[u] * transition_prob(u, v) * 1
                # if u == t:
                #     v = u
                #     new_prob = transition_prob(u, t) * 1
                    # new_prob_t = transition_prob(u, v) * 1
                    # joint_prob[u] = new_prob_t
                new_prob = transition_prob(u, v) * 1
                print("new_prob:", u, v, transition_prob(u, v))
                if joint_prob[v] < new_prob:
                    joint_prob[v] = new_prob
                    # pred = list(predecessor)
                    # predecessor[v] = pred.pop()
                    # print("predecessor:", predecessor)
                    print("joint_prob:", joint_prob)
                    print("u,v:", u, v)
                    edge = (u, v)
                    # print(edge)
                    matched_edges.append(edge)
    return joint_prob, matched_edges # predecessor


# # build the matched path and route
# def construct_path(predecessor):
#     matched_route = []
#     type(predecessor)
#     for x in predecessor:
#         # print(x)
#         matched_route.append(x)
#     # print(matched_route)
#     path_matched_route = list(zip(matched_route, matched_route[1:]))
#     print("matched_path:", path_matched_route)
#     print("mathced_route:", matched_route)
#     # print (x, ':', predecessor[x])
#     # matched_route = [x, predecessor[x]]
#     # print(matched_route)

VITERBI_probs = viterbi_search(adjacency_list, s, t)
print(VITERBI_probs)


# print("joint_porob: ", VITERBI_probs[0])
# print("predecessor: ", VITERBI_probs[1])

# path
# VITERBI_path = construct_path(VITERBI_probs[1])






#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################


