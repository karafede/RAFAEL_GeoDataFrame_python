
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

# set of neighbors (viasat measurments) of a node in the graph

file_graphml = 'Catania__Italy_cost.graphml'
viasat_data = "viasat_max_data.csv"
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)

# make a geodataframe from the grapho
# gdf_edges = ox.graph_to_gdfs(grafo, nodes=False, fill_edge_geometry=True)
gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

fields = ["longitude", "latitude"]
viasat = pd.read_csv(viasat_data, usecols=fields)

# create an index column
viasat["ID"] = viasat.index

######################################################

# extract all lat, lon in WGS84 from the grafo
lat84 = (gdf_nodes.y).values.tolist()
lon84 = (gdf_nodes.x).values.tolist()
# define UMT33 projection (from degree to meters)
myProj = Proj("+proj=utm +zone=33, +north +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
# convert lon, lat into meters (as list)
lon_grafo, lat_grafo = myProj(lon84, lat84)
A = list(zip(lon_grafo, lat_grafo))  # NODES
kdtree = spatial.cKDTree(A)  # algorithm to optimize the calculation of nearest neighbor nodes

# VIASAT data
lon_VIASAT = (viasat.longitude).values.tolist()
lat_VIASAT = (viasat.latitude).values.tolist()
# convert lon, lat into meters (as list)
lon_VIASAT, lat_VIASAT = myProj(lon_VIASAT, lat_VIASAT)

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

# intesect polygons with linestring......
# https://gis.stackexchange.com/questions/269441/intersecting-linestrings-with-polygons-in-python

from datetime import datetime

buff = []
index_edges = []
index_buff = []
edge = []
now1 = datetime.now()

for index1, streets in gdf_edges.iterrows():
    for index2, via_buff in buffer_viasat.iterrows():
        if streets['geometry'].intersects(via_buff['geometry']) is True:
            print("OK=======================OK")
            index_edges.append(index1)
            index_buff.append(index2)
            STREET = streets.u, streets.v, index2
            distance = (Point(viasat[['longitude', 'latitude']].iloc[index2]).distance(streets.geometry))*100000 # rough meter conversion
            print(distance)  # km??
            edge.append(STREET)
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

# u.measurement
class adj_list_u:
    def __init__(self, rowf):
        self.u = int(rowf[0])
        self.measurement = int(rowf[1])

node_u = []
for index, rowf in df_edges.iterrows():
    print(rowf)
    node_u.append(  adj_list_u( rowf[['u', 'buffer_ID']] ) )

# v.measurement
class adj_list_v:
    def __init__(self, rowf):
        self.v = int(rowf[0])
        self.measurement = int(rowf[1])

node_v = []
for index, rowf in df_edges.iterrows():
    print(rowf)
    node_v.append(  adj_list_v( rowf[['v', 'buffer_ID']] ) )


 # u_measurement = int(df_edges[df_edges['u'] == u]['buffer_ID'])

def haversine_measurement_node(node_measurement, node):
    # u_measurement = u.measurement
    # u = node_u
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # u_measurement = int(df_edges[df_edges['u'] == u]['buffer_ID'])
    coords_measurements = viasat[viasat.ID == node_measurement].values.tolist()
    lon_measurement_u = coords_measurements[0][1]
    lat_measurement_u = coords_measurements[0][0]
    if node == u:
        coords_u = gdf_nodes[gdf_nodes.index == node.u][['x', 'y']].values.tolist()
        lon_u = coords_u[0][0]
        lat_u = coords_u[0][1]
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon_measurement_u, lat_measurement_u, lon_u, lat_u])
    elif node == v:
        coords_v = gdf_nodes[gdf_nodes.index == node.v][['x', 'y']].values.tolist()
        lon_v = coords_v[0][0]
        lat_v = coords_v[0][1]
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon_measurement_u, lat_measurement_u, lon_v, lat_v])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r # Kilometers


def haversine_measurement(u_measurement, v_measurement):
    # u_measurement = u.measurement
    # v_measurement = v.measurement
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    coords_measurements_u = viasat[viasat.ID == u_measurement].values.tolist()
    lon_measurement_u = coords_measurements_u[0][1]
    lat_measurement_u = coords_measurements_u[0][0]
    coords_measurements_v = viasat[viasat.ID == v_measurement].values.tolist()
    lon_measurement_v = coords_measurements_v[0][1]
    lat_measurement_v = coords_measurements_v[0][0]
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon_measurement_u, lat_measurement_u, lon_measurement_v, lat_measurement_v])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r  # Kilometers


# calcualtion of "sigma" for the Gaussian distribution that will be defined next
# https://stackoverflow.com/questions/26368533/distance-of-a-gps-point-to-a-polyline
# "sigma" has been calculated ad the standard deviation of all the hrvesine distances betweeen meaurments and nodes)
dist_degrees = []
for i in range(len(df_edges)):
    u = node_u[i]
    u_measurement = u.measurement
    DIST_DEGREES = haversine_measurement_node(u_measurement, u)
    print(DIST_DEGREES)
    dist_degrees.append(DIST_DEGREES)
# calculate the standard deviation of the distance between two points in degrees
SIGMA_Z = np.std(dist_degrees)

u = node_u[i]
v = node_v[i]

###############################
### emission probability ######
###############################
# A gaussian distribution of all NODES that are closer a node stays to its measurement
def emission_prob(u):
    c = 1 / (SIGMA_Z * math.sqrt(2 * math.pi))
    return c * math.exp(-haversine_measurement_node(u.measurement, u)**2)


prob = []
for i in range(len(df_edges)):
    u = node_u[i]
    emiss_prob = emission_prob(u)
    print(emiss_prob)
    prob.append(emiss_prob)
print("max_probability: ", max(prob))

#####################################################################
### ----------------------------------------------------#############
# why "cost is a string???
# transform "cost" into float
for u,v,key,attr in grafo.edges(keys=True,data=True):
    print(attr["cost"])
    print(type(attr["cost"]))
    attr['cost'] = float(attr['cost'])
    print(type(attr["cost"]))

u = node_u[0]
v = node_v[10]

nx.shortest_path_length(grafo, u.u, v.v, weight='cost') # why "cost is a string???
nx.shortest_path_length(grafo, u.u, v.v, weight='length')

### ----------------------------------------------------#############
#####################################################################

u = node_u[0]
v = node_v[10]

# transition probability (probability that the distance u-->v is frm the mesasurements's distances at nodes u and v
# A empirical distribution
def transition_prob(u, v):
    BETA = 1
    c = 1 / BETA
    # Calculating route distance is expensive.
    # We will discuss how to reduce the number of calls to this function later.
    delta = abs(nx.shortest_path_length(grafo, u.u, v.v, weight='length')/1000 -
                haversine_measurement(u.measurement, v.measurement))  # in Kilometers
    return c * math.exp(-delta)

trans_prob = []
for i in range(len(df_edges)):
    u = node_u[i]
    v = node_v[i]
    t_prob = transition_prob(u, v)
    print(t_prob)
    trans_prob.append(t_prob)
print("max_transition_prob: ", max(trans_prob))


##################################
### Probability of a path ########
##################################

# define a path as list of edges
route = [810075284, 4277112580, 4191850164, 3987101865, 2941239107]
path = list(zip(route,route[1:]))
ox.plot_graph_route(grafo, route, route_color='green', fig_height=12, fig_width=12)

def path_prob(path):
    assert path
    u, v = path[0]
    for i in range(len(df_edges)):
        if node_u[i].u == u and node_v[i].v == v:
            print("got it!:", i, node_u[i].u)
            u = node_u[i]
    joint_prob = emission_prob(u)
    for u, v in path:
        print(u,v)
        for i in range(len(df_edges)):
            if node_u[i].u == u and node_v[i].v == v:
                u = node_u[i]
                v = node_v[i]
                print(u,v)
        joint_prob *= transition_prob(u, v) * emission_prob(v)
    return joint_prob


####################################################################################
####################################################################################
####################################################################################
####################################################################################
####################################################################################
####################################################################################

# remove row in df_edges where u in never in v
df_edges.sort_values(['u','v'],ascending=False, inplace=True)


idx_rows_to_remove = []
for i in range(len(df_edges)):
    if int(df_edges[['u']].iloc[i]) in df_edges['v'].values:
        print("OK")
        print(i)
    else:
        print("============================================")
        print(i)
        idx = df_edges.iloc[i].name
        idx_rows_to_remove.append(idx)
df_edges = df_edges.drop(idx_rows_to_remove, axis='rows')


df1 = pd.DataFrame(df_edges['buffer_ID'].value_counts().values, index=df_edges['buffer_ID'].value_counts().index, columns=['count'])
# sort df1  by index
df1=df1.sort_index()
ADJ_LIST = []

# build ADJACENCY LIST
# df_edges adjacent list with GPS tracks ordered by priority of appearance
for idx, row in df1.iterrows():
        track_freq = row['count']
        if idx+1 != len(df1):
            track_freq_next = int(df1[df1.index == idx+1]['count'])
            print(idx, track_freq)
            print(idx+1, track_freq_next)
        # subset dataframe according to index
        # idx = 0
        A = df_edges[df_edges['buffer_ID'] == idx][['u', 'v']]
        A = list(A['u'])
        print("A: ", A)
        if idx + 1 != len(df1):
            B = df_edges[df_edges['buffer_ID'] == idx+1][['u', 'v']]
            B = list(B['u'])
            print("B: ", B)
    ## make a list of all u and v together
        pairs = [(x,y) for x in A for y in B]
        print('pairs:', pairs)
        for u, v in pairs:
            if idx + 1 != len(df1):
                print('u {} --> v {}'.format(u, v))
                if u == v:
                    paths = []
                    print("path: NONE")
                else:
                    paths = nx.shortest_path(grafo, u, v, weight='length')
                    print("path:", paths)

# nx.shortest_path_length(grafo, u, v, weight='length') / 1000 (in Km)


ADJ_LIST.append(pairs)
print('ADJ_LIST:', ADJ_LIST)




# def maximum_path_prob(adjacency_list, s, t):
#     return max((path_prob(path), path)
#                for path in all_paths(adjacency_list, s, t),
#                key=lambda prob, path: prob)





maxdist = 50 # meters
adjacent_list = []
distances = []

# find the nodes within a fixed distance
for i in range(len(viasat)):
    B = [lon_VIASAT[i], lat_VIASAT[i]]  # viasat data
    # print(B)
    ix_list = kdtree.query_ball_point(B, maxdist) # for each VIASAT point find the nodes within a fixed distance (return indices)
    print(ix_list)
    adjacent_list.append(ix_list)
    for ix in ix_list:
        print(ix)
        dist = math.sqrt((lon_VIASAT[i] - lon_grafo[ix]) ** 2 + (lat_VIASAT[i] - lat_grafo[ix]) ** 2)
        print("distance: ",dist)
        distances.append(dist)


# adjacent_list = pd.DataFrame(adjacent_list)
viasat['neigh_nodes'] = adjacent_list

# remove empty strings
adjacent_list = list(filter(None, adjacent_list))

######################################################################
######################################################################

# buffer approach
# https://stackoverflow.com/questions/41524081/determine-if-a-two-dimensional-point-falls-within-the-the-unit-circle-python-3

# https://gis.stackexchange.com/questions/314949/creating-square-buffers-around-points-using-shapely

# https://gis.stackexchange.com/questions/314949/creating-square-buffers-around-points-using-shapely


'''
############## create stopo-to-stop file with distance
      def create_stopToStopDistance(self, maxdist):
          # read stops
          print('loading stops....')
          inProj = Proj(init='epsg:4326')
          outProj = Proj(init='epsg:32633')
          outfile = open(self.path+"stoptostop.txt", 'w')
          outfile.write("fromstop_id, fromstop_name, tostop_id, tostop_name, meters" + "\n")
          print('loading stops....')
          stops = self.path + "stops.txt";
          fields = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
          dtype_dic = {'stop_id': str, 'stop_name': str, 'stop_lat': "float64", 'stop_lon': "float64"}
          stp = pd.read_csv(stops, usecols=fields, dtype=dtype_dic)
          stp['stop_id'] = stp['stop_id'].astype(str)
          stp['stop_name'] = stp['stop_name'].astype(str)
          stp['stop_lat'] = stp['stop_lat'].astype("float64")
          stp['stop_lon'] = stp['stop_lon'].astype("float64")
          liststoopcoo = stp.values.tolist()
          lat84 = stp['stop_lat'].values.tolist()
          lon84 = stp['stop_lon'].values.tolist()
          myProj = Proj("+proj=utm +zone=33, +north +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
          lon, lat = myProj(lon84, lat84)
          A = list(zip(lon, lat))   # NODES
          kdtree = spatial.cKDTree(A)  # algorithm to optimize the calculation o nearest nodes
          for i in range(len(lon)):
              B = [lon[i], lat[i]]
              ix_list = kdtree.query_ball_point(B, maxdist)  # find the nodes within a fixed distance
              for ix in ix_list:
                  dist = math.sqrt((lon[i] - lon[ix]) ** 2 + (lat[i] - lat[ix]) ** 2)
                  # st=print(liststoopcoo[i][0],liststoopcoo[i][1],liststoopcoo[ix][0], liststoopcoo[ix][1], int(dist))
                  outfile.write(liststoopcoo[i][0] + "," + liststoopcoo[i][1] + "," + liststoopcoo[ix][0] + "," +
                                liststoopcoo[ix][1] + "," + str(int(dist)) + "\n")
              print(liststoopcoo[i][0], '-', liststoopcoo[i][1])
          outfile.close()
'''

