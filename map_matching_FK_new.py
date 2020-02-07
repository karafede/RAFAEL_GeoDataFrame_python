
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
from osgeo import ogr



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

# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')

for i in range(len(viasat)):
    folium.CircleMarker(location=[viasat.latitude.iloc[i], viasat.longitude.iloc[i]],
                        popup=viasat.ID.iloc[i],
                        radius=6,
                        color="black",
                        fill=True,
                        fill_color="black",
                        fill_opacity=1).add_to(my_map)
my_map.save("matched_route.html")


######################################################

# build a geodataframe with VIASAT data
geometry = [Point(xy) for xy in zip(viasat.longitude, viasat.latitude)]
# viasat = viasat.drop(['longitude', 'latitude'], axis=1)
crs = {'init': 'epsg:4326'}
viasat_gdf = GeoDataFrame(viasat, crs=crs, geometry=geometry)
# viasat_gdf.plot()

# Buffer the points by some units (unit is kilometer)
buffer = viasat_gdf.buffer(0.00025)  #50 meters # this is a geoseries
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


# add buffered viasat polygons
# save first as geojson file
buffer.to_file(filename='buffer_viasat.geojson', driver='GeoJSON')
# folium.GeoJson('buffer_viasat.geojson').add_to((my_map))
my_map.save("matched_route.html")


'''
#### add ANAS catania Network #####################################
####################################################################
# load shp file
path = 'C:/ENEA_CAS_WORK/Catania_RAFAEL/files_ANAS/'
stralcio_ANAS_shape = gpd.read_file(path + "stralcio_grafo_ANAS.shp")
postazioni_ANAS_shape = gpd.read_file((path + "postazioni_area_catania.shp"))


my_map= folium.Map([37.53988692816245, 15.044971594798902], zoom_start=11, tiles='cartodbpositron')
# save first as geojson file
stralcio_ANAS_shape.to_file(filename='ANAS_stralcio.geojson', driver='GeoJSON')
postazioni_ANAS_shape.to_file(filename='postazioni_ANAS.geojson', driver='GeoJSON')
folium.GeoJson('ANAS_stralcio.geojson').add_to((my_map))
folium.GeoJson('postazioni_ANAS.geojson').add_to((my_map))
my_map.save("ANAS_stralcio_map.html")

####################################################################
####################################################################
'''

# 1 = 100 km
# 0.1 = 10 km
# 0.01 = 1 km
# 0.001 = 100m
# 0.0001 = 10m
# 0.00001 = 1m

############################################################
###====================================================#####
############################################################

from datetime import datetime

buff = []
index_edges = []
index_buff = []
edge = []
DISTANCES = []
now1 = datetime.now()


for streets in gdf_edges.itertuples(index=True):
    for via_buff in buffer_viasat.itertuples(index=True):
        if streets.geometry.intersects(via_buff.geometry) is True:
            print("OK=======================OK")
            index_edges.append(streets.Index)
            index_buff.append(via_buff.Index)
            STREET = streets.u, streets.v, via_buff.Index
            # get distance between Viasat measurement and edge
            distance = (Point(viasat[['longitude', 'latitude']].iloc[via_buff.Index]).distance(streets.geometry))*100000 # roughly meter conversion
            print("distance track-edge: ", distance, " meters")
            edge.append(STREET)
            distance = distance, via_buff.Index
            DISTANCES.append(distance)
            # list all buffers in sequence
            # buff.append(via_buff.name)
now2 = datetime.now()
print(now2 - now1)


############################################################
###====================================================#####
############################################################

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

# # remove edges that do not have (u,v) pairs on the grapho
# idx_rows_to_remove = []
# for i in range(len(df_edges)):
#     if int(df_edges[['u']].iloc[i]) in df_edges['v'].values:
#         print("OK")
#         print(i)
#     else:
#         print("============================================")
#         print(i)
#         idx = df_edges.iloc[i].name
#         idx_rows_to_remove.append(idx)
# df_edges = df_edges.drop(idx_rows_to_remove, axis='rows')
# len(df_edges)


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
        u_dict[u] = df_edges[df_edges['u']==u].values.tolist()
print(u_dict)

nodes_v = list(df_edges.v.unique())
v_dict = {}
keys = nodes_v
for v in keys:
        v_dict[v] = df_edges[df_edges['v']==v].values.tolist()
print(v_dict)

# join two dictionaries
nodes_dict = {**u_dict, **v_dict}



# define distance between GPS track (viasat measurements) and node
def great_circle_track_node(u):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # u_track = u_dict.get(u)[0][2]
    u_track = nodes_dict.get(u)[0][2]
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
    # u_track = u_dict.get(u)[0][2]
    u_track = nodes_dict.get(u)[0][2]
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
    return 1 * math.exp(-0.5*(great_circle_track_node(u)/SIGMA_Z)**2)


prob = []
# for u in u_dict:
for u in nodes_dict:
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
    shortest_path = nx.shortest_path_length(grafo, u, v, weight='length')/1000
    # print('## u:',u, '## v:', v, '## delta = direct_distance - shortest distance (km):', delta, '## shortest_path (km):',shortest_path)
    # print('## u:', u, '## v:', v, '## shortest_path (km):', shortest_path)
    return c * math.exp(-delta)



# calculate BETA
deltaB = []
# for u in u_dict:
#     for v in [item[1] for item in u_dict.get(u)]:
for u in nodes_dict:
    for v in [item[1] for item in nodes_dict.get(u)]:
        LEN_ROUTE = nx.shortest_path_length(grafo, u, v, weight='length') / 1000  # in Km
        # print("Len_Route", LEN_ROUTE)  # in Km
        # distance on the sphere (cartesian distance)
        DIST = great_circle_track(u, v)  # in Km
        if DIST != None:
            delta = abs(DIST - LEN_ROUTE)
            # print(DIST, "=============================")  # in Km
            # print("DELTA: ", delta)  # in Km
            deltaB.append(delta)
    BETA = (1 / math.log(2)) * np.median(deltaB)
    # print("BETA: ", BETA)


trans_prob = []
# for u in u_dict:
#     for v, track in [(item[1], item[2]) for item in u_dict.get(u)]:
for u in nodes_dict:
    for v, track in [(item[1], item[2]) for item in nodes_dict.get(u)]:
        # print(v)
        print('track:', track)
        t_prob = transition_prob(u, v)
        # print(t_prob)
        trans_prob.append(t_prob)
print("## max_transition_prob: ", max(trans_prob))
print("## min_transition_prob: ", min(trans_prob))


# define the adjaceny list
adjacency_list = {}
for i in range(len(df_edges_dict)):
    track = df_edges_dict.get(i)
    unique_list = set(x for l in track for x in l)
    adjacency_list[i] = unique_list

# if two lists of the adjacency list are identical, then only take the last one...
result = {}
for key,value in adjacency_list.items():
    if value not in result.values():
        result[key] = value

adjacency_list = result

# track_list = list(df_edges.buffer_ID.unique())
track_list = list(set(adjacency_list))

max_prob_node = []
for i in range(len(track_list)):
    print(track_list[i])
    if track_list[i] == max(track_list):
        break

# for track in track_list:
#     print(track)
#     if track+1 == len(track_list):
#         break

    # track = 1
    trans_prob = {}
    emiss_prob = {}
    shortest_path = {}

    # for u in adjacency_list[track]:
    #     for v in adjacency_list[track+1]:
    # for i in range(len(track_list)):

    for u in adjacency_list[track_list[i]]:
        for v in adjacency_list[track_list[i+1]]:

            if u != v:
                print(u,v)
            try:
                if u != v:
                    trans_prob[u] = transition_prob(u, v)
                    shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                    print('#u:', u, '#v:', v, 'shortest_path:', nx.shortest_path_length(grafo, u, v, weight='length') / 1000)
                    emiss_prob[u] = emission_prob(u)
            except nx.NetworkXNoPath:
                     print('No path', 'u:', u, 'v:', v, )

    MAX_trans_key = max(trans_prob, key=trans_prob.get)
    # MAX_emiss_key = max(emiss_prob, key=emiss_prob.get)
    MAX_trans_value = trans_prob.get(MAX_trans_key)
    # MAX_emiss_value = emiss_prob.get(MAX_emiss_key)
    if MAX_trans_value !=0:
        # MAX_prob = max(MAX_trans_value, MAX_emiss_value)
        print("max_prob_NODE:", MAX_trans_key)
        max_prob_node.append(MAX_trans_key)


        # 'MAX_trans_key' must be also in the next set of nodes where there is the next track.
        # if this is not the case, then 'MAX_trans_key' is not valid!!!
        if MAX_trans_key not in adjacency_list[track_list[i+1]]:
            adjacency_list[track_list[i]].remove(MAX_trans_key)
            max_prob_node.remove(MAX_trans_key)
            # and start calculation again
            trans_prob = {}
            emiss_prob = {}
            shortest_path = {}
            for u in adjacency_list[track_list[i]]:
                for v in adjacency_list[track_list[i+1]]:
                    if u != v:
                        print(u, v)
                    try:
                        if u != v:
                            trans_prob[u] = transition_prob(u, v)
                            shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                            print('#u:', u, '#v:', v, 'shortest_path:',
                                  nx.shortest_path_length(grafo, u, v, weight='length') / 1000)
                            emiss_prob[u] = emission_prob(u)
                    except nx.NetworkXNoPath:
                        print('No path', 'u:', u, 'v:', v, )
            MAX_trans_key = max(trans_prob, key=trans_prob.get)
            MAX_trans_value = trans_prob.get(MAX_trans_key)
            if MAX_trans_value != 0:
                print("max_prob_NODE:", MAX_trans_key)
                max_prob_node.append(MAX_trans_key)


'''
# build ADJACENCY LIST (all possible paths between nodes from u ---> v) (list all paths in between)
# df_edges adjacent list with GPS tracks ordered by priority of appearance
adjacency_list = {}
df_edges.sort_values(by=['buffer_ID'], inplace=True)
track_list = list(df_edges.buffer_ID.unique())
for track in track_list:
    print(track)
    # filter dataframe
    df1 = df_edges[df_edges['buffer_ID'] == track][['u', 'v']]
    df2 = df_edges[df_edges['buffer_ID'] == track+1][['u', 'v']]
    df_end = df_edges[df_edges['buffer_ID'] == track-1][['u', 'v']]
    # u_list1 = list(df1.u.unique())
    # u_list2 = list(df2.u.unique())
    u_list1 = df1.values.tolist()
    flattened_u_list1 = [val for sublist in u_list1 for val in sublist]
    u_list2 = df2.values.tolist()
    flattened_u_list2 = [val for sublist in u_list2 for val in sublist]
    # u_list_end = df_end.values.tolist()
    # flattened_u_list_end = [val for sublist in u_list_end for val in sublist]
    # if track != len(track_list) - 1:
    pairs = [(x, y) for x in flattened_u_list1 for y in flattened_u_list2]
    # else:
    #     pairs = [(x, y) for x in flattened_u_list1 for y in flattened_u_list_end]
    # remove duplicates tuples
    pairs = list(set(pairs))
    # print('pairs:', pairs)
    adjacency_list[track] = pairs

# remove empty list
adjacency_list = {k: v for k, v in adjacency_list.items() if v and v[0]}


## find node with the best Joint probability for map-matching

matched_edges = []
max_prob_node = []
new = []
for track in range(len(adjacency_list)):
    print(track)
    joint_prob = {}
    route = adjacency_list[track]
    # route = adjacency_list[5]
    for u,v in route:
        joint_prob[u] = emission_prob(u)
        # joint_prob[u] = 1
        joint_prob[v] = 0
        try:
            # new_prob = transition_prob(u, v) * 1
            # new_prob = joint_prob[u] * transition_prob(u, v) * 1
            new_prob = joint_prob[u] * transition_prob(u, v) * emission_prob(v)
            # new_prob = transition_prob(u, v) * emission_prob(v)
            shortest_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
            # print("new_prob:",'## u:', u, '## v:', v, transition_prob(u, v))
        except nx.NetworkXNoPath:
            print('No path', 'u:', u, 'v:',v,)
            new_prob = 0
            shortest_path = None
        if joint_prob[v] < new_prob:
            joint_prob[v] = new_prob
            print("joint_prob:", joint_prob)
            print('u:', u, 'v:',v, "new_prob:", new_prob, '## shortest_path (km):', shortest_path)
            edge = (u, v)
            matched_edges.append(edge)
        elif track == 0 and shortest_path !=0:
            try:
                print('no prob available')
                # print(u, v)
                # print('u:', u, 'v:', v, "new_prob:", transition_prob(u, v) * emission_prob(v), '## shortest_path (km):', shortest_path)
                NEW_PROB = transition_prob(u, v) * emission_prob(v)
                new.append(NEW_PROB)
                new_prob = max(new)
                joint_prob[v] = new_prob
            except nx.NetworkXNoPath:
                # print('No path', 'u:', u, 'v:', v, )
                new_prob = 0
                shortest_path = None
    # get the node with the larger probability
    MAX_prob_key = max(joint_prob, key=joint_prob.get)
    # MAX_prob_value = joint_prob.get(MAX_prob_key)
    print("max_prob_NODE:", MAX_prob_key)
    max_prob_node.append(MAX_prob_key)


    # remove element with prob == 1
    # joint_prob = {k: v for k, v in joint_prob.items() if v != 1}
'''

# get unique values (ordered)
from collections import OrderedDict
# max_prob_node = list(set(max_prob_node))
max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

# build matched route
matched_route = []
all_matched_edges = []
for origin, destination in zip(max_prob_node, max_prob_node[1:]):
    # print(origin, destination)
    route = nx.shortest_path(grafo, origin, destination, weight='length')
    path_edges = list(zip(route, route[1:]))
    # print(path_edges)
    all_matched_edges.append(path_edges)
    matched_route.append(route)


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

# filter gdf_edges with df_nodes
keys = list(df_nodes.columns.values)
index_gdf_edges = gdf_edges.set_index(keys).index
index_df_nodes = df_nodes.set_index(keys).index
egdes_matched_route = gdf_edges[index_gdf_edges.isin(index_df_nodes)]

# quick plot
egdes_matched_route.plot()

## add plot in Folium map
# save first as geojson file
egdes_matched_route.geometry.to_file(filename='matched_route.geojson', driver='GeoJSON')
folium.GeoJson('matched_route.geojson').add_to((my_map))
my_map.save("matched_route.html")



'''

##########################
# VITERBI algorithm ######
##########################

if 'u' in globals():
    del u

if 'v' in globals():
    del v

# s = 3987101865
s = 4277112580
# t = 2941239107
# t = 1836387039
t = 1836387053
# t = 891536279
# t = 2941259032


# list all nodes in the adjacency list between s --> t
q = []
# if adjacency_list.get(s) is not None:
for node in adjacency_list.keys():
    print(node)
    adjacency_list[node]
    # make a list of all u and v together
    for v in [x[1] for x in adjacency_list.get(s)]:
    # if adjacency_list.get(v) is not None:
        for path in adjacency_list.get(v):
            for y in path:
                q.append(y)
# else:
#     # jump to the next edge....
#     next_edge = int(df_edges[df_edges['v'] == s]['buffer_ID'] + 1)
#     s = df_edges[df_edges['buffer_ID'] == next_edge].iloc[0]['u']
#     for v in [x[1] for x in adjacency_list.get(s)]:
#         if adjacency_list.get(v) is not None:
#             for path in adjacency_list.get(v):
#                 for y in path:
#                     q.append(y)



def viterbi_search(adjacency_list, s, t):
    # Initialize joint probability for each node
    joint_prob = {}
    for u in adjacency_list:
        joint_prob[u] = 0
    matched_edges = []
    joint_prob[s] = 1
    u = s
    while len(q) !=0:
        u = q.pop()
        for v in [x[1] for x in adjacency_list.get(u)]:
            if adjacency_list.get(v) is not None:
                # new_prob = joint_prob[u] * transition_prob(u, v) * emission_prob(v)
                # new_prob = joint_prob[u] * transition_prob(u, v) * 1
                new_prob = transition_prob(u, v) * 1
                print("new_prob:",'## u:', u, '## v:', v, transition_prob(u, v))
                if joint_prob[v] < new_prob:
                    joint_prob[v] = new_prob
                    # print("joint_prob:", joint_prob)
                    # print("u,v:", u, v)
                    edge = (u, v)
                    matched_edges.append(edge)
    print("## probabilities @nodes:", joint_prob)
    print("## matched edges:", matched_edges)
    return joint_prob, matched_edges


VITERBI_probs = viterbi_search(adjacency_list, s, t)
# print("probabilities:", VITERBI_probs[0])
# print("matched edges:", VITERBI_probs[1])

'''

# def viterbi_search(adjacency_list, s, t):
#     # Initialize joint probability for each node
#     joint_prob = {}
#     for u in adjacency_list:
#         joint_prob[u] = 0
#     # predecessor = {}
#     matched_edges = []
#     q = list()
#
#     if adjacency_list.get(s) is not None:
#         q.append(s)
#     else:
#         next_edge = int(df_edges[df_edges['v'] == s]['buffer_ID'] + 1)
#         s = df_edges[df_edges['buffer_ID'] == next_edge].iloc[0]['u']
#         q.append(s)
#
#     # joint_prob[s] = emission_prob(s)
#     joint_prob[s] = 1
#     # predecessor[s] = None
#     u = s
#     # pred = []
#     for v in [x[1] for x in adjacency_list.get(u)]:
#         if adjacency_list.get(v) is not None:
#             # print(v)
#             q.append(v)
#
#     # if adjacency_list.get(t) is not None:
#     #     q.append(t)
#
#     while len(q) !=0:
#         u = q.pop()
#         # if u == t: break
#         for v in [x[1] for x in adjacency_list.get(u)]:
#             # print(v)
#             if adjacency_list.get(v) is not None:
#                 # print(v)
#                 # pred.append(v)
#                 # new_prob = joint_prob[u] * transition_prob(u, v) * emission_prob(v)
#                 # new_prob = joint_prob[u] * transition_prob(u, v) * 1
#                 new_prob = transition_prob(u, v) * 1
#                 print("new_prob:", u, v, transition_prob(u, v))
#                 if joint_prob[v] < new_prob:
#                     joint_prob[v] = new_prob
#                     # pred = list(predecessor)
#                     # predecessor[v] = pred.pop()
#                     # print("predecessor:", predecessor)
#                     print("joint_prob:", joint_prob)
#                     print("u,v:", u, v)
#                     edge = (u, v)
#                     # print(edge)
#                     matched_edges.append(edge)
#     return joint_prob, matched_edges # predecessor



#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################


