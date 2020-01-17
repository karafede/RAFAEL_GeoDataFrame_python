
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
viasat = viasat.drop(['longitude', 'latitude'], axis=1)
crs = {'init': 'epsg:4326'}
viasat_gdf = GeoDataFrame(viasat, crs=crs, geometry=geometry)
viasat_gdf.plot()

# Buffer the points by some units (unit is kilometer)
buffer = viasat_gdf.buffer(0.0005)  #50 meters # this is a geoseries
buffer.plot()
# make a dataframe
buffer_viasat = pd.DataFrame(buffer)
buffer_viasat.columns = ['geometry']
type(buffer_viasat)
# transform a geoseries into a geodataframe
# https://gis.stackexchange.com/questions/266098/how-to-convert-a-geoserie-to-a-geodataframe-with-geopandas

## circumscript the area of the track (buffer zone)
union = buffer.unary_union
envelope = union.envelope
rectangle_viasat = gpd.GeoDataFrame(geometry=gpd.GeoSeries(envelope))
# rectangle_viasat.plot()

# geodataframe with edges
type(gdf_edges)
gdf_edges.plot()

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
            edge.append(STREET)
            # list all buffers in sequence
            buff.append(via_buff.name)
now2 = datetime.now()
print(now2 - now1)
        # else:
        #     print("NO==NO")


# sort edges and associated buffer (first buffer is the Number 43)
df = pd.DataFrame(edge)
df.columns = ['u', 'v', 'buffer_ID']
df.sort_values(by=['buffer_ID'], inplace=True)
print(df)

# for each buffer_ID get the VIASAT measurement

# build the class...
# u.measurement

# set_of_edges = set(edge)


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

# add the rectangle that circusmcript the area of the viasat data
# rectangle_viasat.to_file(filename='rectangle_viasat.geojson', driver='GeoJSON')
# folium.GeoJson('rectangle_viasat.geojson').add_to((my_map))

# https://shapely.readthedocs.io/en/stable/manual.html
# https://gis.stackexchange.com/questions/127878/line-vs-polygon-intersection-coordinates


####################################################################################
####################################################################################


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

