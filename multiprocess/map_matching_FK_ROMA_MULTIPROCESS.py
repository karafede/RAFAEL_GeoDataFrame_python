
import os
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()

from math import radians, cos, sin, asin, sqrt
from funcs_mapmatching import great_circle_track_node, great_circle_track
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
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'


import multiprocessing as mp
from multiprocessing import Process, freeze_support, Manager
from time import sleep
from collections import deque
from multiprocessing.managers import BaseManager
import contextlib
from multiprocessing import Manager
from multiprocessing import Pool

import dill as Pickle
from joblib import Parallel, delayed
from joblib.externals.loky import set_loky_pickler
set_loky_pickler('pickle')
from multiprocessing import Pool,RLock

# today date
today = date.today()
today = today.strftime("%b-%d-%Y")


os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()
## load grafo
file_graphml = 'Roma__Italy_70km.graphml'
grafo_ALL = ox.load_graphml(file_graphml)
## ox.plot_graph(grafo_ALL)
# gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)
gdf_nodes_ALL, gdf_edges_ALL = ox.graph_to_gdfs(grafo_ALL)

'''
AAA = gdf_edges_ALL.drop_duplicates(['u', 'v'])
AAA = pd.DataFrame(AAA)
len(AAA)
sum(AAA.length)
'''

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()

# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS mapmatching_all CASCADE")
# cur_HAIG.execute("DROP TABLE IF EXISTS accuracy CASCADE")
# conn_HAIG.commit()

# Function to generate WKB hex
## I use this funxtion when I want to insert data into a DB (no need to plot)
def wkb_hexer(line):
    return line.wkb_hex


# Create an SQL connection engine to the output DB
# engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_RM_2019')
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_ROMA', poolclass=NullPool)


'''
## import OSM network into the DB 'HAIG_Viasat_RM_2019'
###  to a DB and populate the DB  ###
connection = engine.connect()
gdf_edges_ALL['geom'] = gdf_edges_ALL['geometry'].apply(wkb_hexer)
gdf_edges_ALL.drop('geometry', 1, inplace=True)
gdf_edges_ALL.to_sql("edges", con=connection, schema="net",
                   if_exists='append')

gdf_nodes_ALL['geom'] = gdf_nodes_ALL['geometry'].apply(wkb_hexer)
gdf_nodes_ALL.drop('geometry', 1, inplace=True)
gdf_nodes_ALL.to_sql("nodes", con=connection, schema="net",
                   if_exists='append')

##### "edges": convert "geometry" field as LINESTRING
with engine.connect() as conn, conn.begin():
    print(conn)
    sql = """ALTER TABLE net.edges
    ALTER COLUMN geom TYPE Geometry(LINESTRING, 4326)
     USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)


##### "nodes": convert "geometry" field as POINTS
with engine.connect() as conn, conn.begin():
    print(conn)
    sql = """ALTER TABLE net.nodes
    ALTER COLUMN geom TYPE Geometry(POINT, 4326)
    USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)

## create index on the column (u,v) togethers in the table 'edges' ###
## Multicolumn Indexes ####

cur_HAIG.execute("""
CREATE INDEX edges_UV_idx ON net.edges(u,v);
""")
conn_HAIG.commit()

conn_HAIG.close()
cur_HAIG.close()
'''


"""
## get all ID terminal of Viasat data  (from routecheck)
all_VIASAT_IDterminals = pd.read_sql_query(
     ''' SELECT "idterm"
         FROM public.routecheck''', conn_HAIG)
## make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())
## save 'all_ID_TRACKS' as list
with open("D:\\ENEA_CAS_WORK\\ROMA_2019\\all_ID_TRACKS_2019.txt", "w") as file:
     file.write(str(all_ID_TRACKS))
"""


"""
## get all terminals corresponding to 'fleet' (from routecheck_2019)
viasat_fleet = pd.read_sql_query('''
              SELECT idterm, vehtype
              FROM public.routecheck
              WHERE vehtype = '2' ''', conn_HAIG)
## make an unique list
idterms_fleet = list(viasat_fleet.idterm.unique())
## save 'all_ID_TRACKS' as list
with open("D:\\ENEA_CAS_WORK\\ROMA_2019\\idterms_fleet.txt", "w") as file:
     file.write(str(idterms_fleet))

"""

## reload 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_2019.txt", "r") as file:
     all_ID_TRACKS = eval(file.readline())
# with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_2019_new.txt", "r") as file:
#    all_ID_TRACKS = eval(file.readline())



## reload 'idterms_fleet' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/idterms_fleet.txt", "r") as file:
    idterms_fleet = eval(file.readline())


# track_ID = '4378843'
# 5922087

# ####################################################################################
# ### create basemap (Roma)
# import folium
#
# ave_LAT = 41.888009265234906
# ave_LON = 12.500281904062206
#
# my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
# ####################################################################################


## read each TRIP from each idterm (TRACK_ID or idtrajectory)


def func(arg):
    last_track_idx, track_ID = arg
    track_ID = str(track_ID)
    print("idterm:", track_ID)
    viasat_data = pd.read_sql_query('''
                SELECT * FROM public.routecheck 
                 /*WHERE date(routecheck.timedate) = '2019-10-09' AND */    
                WHERE "idterm" = '%s' ''' % track_ID, conn_HAIG)
    ### FILTERING #############################################
    # viasat_data = viasat_data[viasat_data.anomaly != "IQc345d"]
    viasat_data = viasat_data[viasat_data.anomaly != "EQc3456"]
    viasat_data = viasat_data[viasat_data.anomaly != "EQc3T5d"]
    if int(track_ID) not in idterms_fleet:
        # print("+++++++ vehtype = car ++++++++++++++++")
        viasat_data = viasat_data[viasat_data.anomaly != "IQ2C4V6"]
    # list all TRIPS for a each idterm
    all_TRIPS = list(viasat_data.TRIP_ID.unique())
    for idx_trip, trip in enumerate(all_TRIPS):
        # TRIP_ID = trip
        viasat = viasat_data[viasat_data.TRIP_ID == trip]
        viasat = viasat.sort_values('timedate')
        viasat.reset_index(drop=True, inplace=True)

        if len(viasat) > 5:
            ## introduce a dynamic buffer
            # dx = max(viasat.longitude) - min(viasat.longitude)
            # dy = max(viasat.latitude) - min(viasat.latitude)
            # if dx < 0.007:
            #     buffer_diam = 0.00020
            # else:
            #     buffer_diam = 0.00008

            buffer_diam = 0.00008   ## best choiche...so far..

            # get extent of viasat data
            ext = 0.030
            ## top-right corner
            p1 = Point(np.min(viasat.longitude)-ext, np.min(viasat.latitude)-ext)
            ## bottom-right corner
            p2 = Point(np.max(viasat.longitude)+ext,np.min(viasat.latitude)-ext)
            ## bottom-left corner
            p3 = Point(np.max(viasat.longitude)+ext, np.max(viasat.latitude)+ext)
            ## top-left corner
            p4 = Point(np.min(viasat.longitude)-ext,np.max(viasat.latitude)+ext)

            # Initialize a test GeoDataFrame where geometry is a list of points
            viasat_extent = gpd.GeoDataFrame([['box', p1],
                                   ['box', p2],
                                   ['box', p3],
                                   ['box', p4]],
                                 columns = ['shape_id', 'geometry'],
                                 geometry='geometry')

            # Extract the coordinates from the Point object
            viasat_extent['geometry'] = viasat_extent['geometry'].apply(lambda x: x.coords[0])
            # Group by shape ID
            #  1. Get all of the coordinates for that ID as a list
            #  2. Convert that list to a Polygon
            viasat_extent = viasat_extent.groupby('shape_id')['geometry'].apply(lambda x: Polygon(x.tolist())).reset_index()
            # Declare the result as a new a GeoDataFrame
            viasat_extent = gpd.GeoDataFrame(viasat_extent, geometry = 'geometry')
            # viasat_extent.plot()

            # # get graph only within the extension of the rectangular polygon
            # # filter some features from the OSM graph
            # filter = (
            #     '["highway"!~"living_street|abandoned|steps|construction|service|pedestrian|'
            #     'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path|footway"]')
            # grafo = ox.graph_from_polygon(viasat_extent.geometry[0], custom_filter=filter)
            #
            # ## make a geo-dataframe from the grapho
            # gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

            # reset indices
            viasat.reset_index(drop=True, inplace=True)

            # create an index column
            viasat["ID"] = viasat.index
            ## sum of progressive distance (true distance travelled by the vehicle)
            diff_progressive = viasat.progressive.diff()
            diff_progressive = diff_progressive.dropna()
            sum_progressive = sum(diff_progressive)  ## in meters

            # for polygon extent of VIASAT data, find intersecting nodes then induce a subgraph
            for polygon in viasat_extent['geometry']:
                intersecting_nodes = gdf_nodes_ALL[gdf_nodes_ALL.intersects(polygon)].index
                grafo = grafo_ALL.subgraph(intersecting_nodes)
                # fig, ax = ox.plot_graph(grafo)
            ## get geodataframes for edges and nodes from the subgraph
            if len(grafo) > 0:
                gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

                # build a geodataframe with VIASAT data
                geometry = [Point(xy) for xy in zip(viasat.longitude, viasat.latitude)]
                crs = {'init': 'epsg:4326'}
                viasat_gdf = GeoDataFrame(viasat, crs=crs, geometry=geometry)

                # Buffer the points by some units (unit is kilometer)
                buffer = viasat_gdf.buffer(buffer_diam)  # 25 meters # this is a geoseries  (0.00010)
                # make a dataframe
                buffer_viasat = pd.DataFrame(buffer)
                buffer_viasat.columns = ['geometry']

                # 1 = 100 km
                # 0.1 = 10 km
                # 0.01 = 1 km
                # 0.001 = 100m
                # 0.0001 = 10m
                # 0.00001 = 1m

                ############################################################
                ###====================================================#####
                ############################################################

                # from datetime import datetime

                # find all edges intersect by the buffers defined above
                # index_edges = []
                # index_buff = []
                edge = []
                DISTANCES = []
                # now1 = datetime.now()
                DISTANCES_dict = {}

                for streets in gdf_edges.itertuples(index=True):
                    for via_buff in buffer_viasat.itertuples(index=True):
                        if streets.geometry.intersects(via_buff.geometry) is True:
                            # print("OK=======================OK")
                            # index_edges.append(streets.Index)
                            # index_buff.append(via_buff.Index)
                            STREET = streets.u, streets.v, via_buff.Index
                            # get distance between Viasat measurement and edge
                            distance = (Point(viasat[['longitude', 'latitude']].iloc[via_buff.Index]).distance(streets.geometry))*100000 # roughly meter conversion
                            # print("distance track-edge: ", distance, " meters")
                            edge.append(STREET)
                            dist = distance
                            distance = distance, via_buff.Index
                            DISTANCES.append(distance)
                            DISTANCES_dict[streets.u] = dist
                # now2 = datetime.now()
                # print(now2 - now1)

                ##########################################################
                ########## VALHALL ALGORITHM MAP MATCHING  ###############
                ##########################################################

                # sort edges and associated buffer
                if len(edge) > 0:
                    df_edges = pd.DataFrame(edge)
                    df_edges.columns = ['u', 'v', 'buffer_ID']
                    df_edges.sort_values(by=['buffer_ID'], inplace=True)

                    ## count the number of edges in each buffer
                    COUNTS_buffer = df_edges.groupby(df_edges[['buffer_ID']].columns.tolist(),
                                                     sort=False).size().reset_index().rename(
                        columns={0: 'counts'})
                    buffer_to_remove = list(((COUNTS_buffer[COUNTS_buffer.counts >= 4]).buffer_ID))
                    df_edges = df_edges[~df_edges.buffer_ID.isin(buffer_to_remove)]

                    ## merge "df_edges" with "viasat" to get the "id"
                    EDGES = df_edges
                    EDGES = EDGES.rename(columns={'buffer_ID': 'ID'})
                    KKK = pd.merge(EDGES, viasat, on=['ID'],how='left')

                    # sort df by u and v
                    # df_edges.sort_values(['u','v'],ascending=False, inplace=True)

                    # make a dictionary: for each buffer/track/measurement (key) assign u and v
                    ID_TRACK = list(df_edges.buffer_ID.unique())
                    df_edges_dict = {}
                    keys = ID_TRACK
                    for track in keys:
                            df_edges_dict[track] = df_edges[['u', 'v']][df_edges['buffer_ID']==track ].values.tolist()


                    # nodes associated to tracks
                    nodes_u = list(df_edges.u.unique())
                    u_dict = {}
                    keys = nodes_u
                    for u in keys:
                            u_dict[u] = df_edges[df_edges['u']==u].values.tolist()

                    nodes_v = list(df_edges.v.unique())
                    v_dict = {}
                    keys = nodes_v
                    for v in keys:
                            v_dict[v] = df_edges[df_edges['v']==v].values.tolist()

                    ## join two dictionaries
                    nodes_dict = {**u_dict, **v_dict}

                    # define distance between GPS track (viasat measurements) and node
                    # def great_circle_track_node(u):
                    #     """
                    #     Calculate the great circle distance between two points
                    #     on the earth (specified in decimal degrees)
                    #     """
                    #     # u_track = u_dict.get(u)[0][2]
                    #     u_track = nodes_dict.get(u)[0][2]
                    #     coords_track = viasat[viasat.ID == u_track].values.tolist()
                    #     lon_track = coords_track[0][2]
                    #     lat_track = coords_track[0][3]
                    #     coords_u = gdf_nodes[gdf_nodes.index == u][['x', 'y']].values.tolist()
                    #     lon_u = coords_u[0][0]
                    #     lat_u = coords_u[0][1]
                    #     # convert decimal degrees to radians
                    #     lon1, lat1, lon2, lat2 = map(radians, [lon_track, lat_track, lon_u, lat_u])
                    #
                    #     # haversine formula
                    #     dlon = lon2 - lon1
                    #     dlat = lat2 - lat1
                    #     a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                    #     c = 2 * asin(sqrt(a))
                    #     r = 6371 # Radius of earth in kilometers. Use 3956 for miles
                    #     return c * r # Kilometers


                    # define distance between two GPS tracks (viasat measurements)
                    # def great_circle_track(u):
                    #     # Calculate the great circle distance between two points from viasat data (progressive)
                    #     u_track = nodes_dict.get(u)[0][2]
                    #     v_track = u_track+1
                    #     if v_track <= max(viasat.ID):
                    #         distance =int((viasat[viasat['ID'] == v_track]).progressive) - int((viasat[viasat['ID'] == u_track]).progressive)
                    #         distance = distance/1000 # in Km
                    #     else:
                    #         distance = 0
                    #     return distance


                    # "sigma" has been calculated as the standard deviation of all the distances between viasat measurements and nodes
                    # SIGMA_Z = 1.4826*np.median(DISTANCES) # meters
                    SIGMA_Z = 1.4826*np.median([x[0] for x in DISTANCES]) # meters
                    # SIGMA_Z = SIGMA_Z/1000 # Kilometers


                    ###############################
                    ### emission probability ######
                    ###############################

                    # Gaussian distribution of all NODES close to Viasat measurements.
                    def emission_prob(u):
                        # c = 1 / (SIGMA_Z * math.sqrt(2 * math.pi))
                        return 1 * math.exp(-0.5 * (great_circle_track_node(u, nodes_dict, viasat, gdf_nodes) / SIGMA_Z) ** 2)
                        # return great_circle_track_node(u,nodes_dict, viasat, gdf_nodes) / SIGMA_Z  # !!! this is only a DISTANCE!!

                    # prob = []
                    # for u in nodes_dict:
                    #     emiss_prob = emission_prob(u)
                    #     prob.append(emiss_prob)

                    #################################
                    ### Transition probability ######
                    #################################

                    # transition probability (probability that the distance u-->v is from the mesasurements's distances at nodes u and v
                    def transition_prob(u, v):
                        BETA = 1
                        c = 1 / BETA
                        delta = abs(nx.shortest_path_length(grafo, u, v, weight='length')/1000 -
                                    great_circle_track(u, nodes_dict, viasat))  # in Kilometers
                        return c * math.exp(-delta)


                    # trans_prob = []
                    # for u in nodes_dict:
                    #     for v, track in [(item[1], item[2]) for item in nodes_dict.get(u)]:
                    #         t_prob = transition_prob(u, v)
                    #         trans_prob.append(t_prob)


                    ####################################################
                    ####################################################

                    # define the adjaceny list
                    adjacency_list = {}
                    for key in df_edges_dict.keys():
                        track = df_edges_dict.get(key)
                        unique_list = set(x for l in track for x in l)
                        adjacency_list[key] = unique_list

                    # if two lists of the adjacency list are identical, then take only the last one...
                    result = {}
                    for key,value in adjacency_list.items():
                        if value not in result.values():
                            result[key] = value
                        adjacency_list = result

                    # get all keys names from the adjacency list
                    from operator import itemgetter
                    def getList(dict):
                        return list(map(itemgetter(0), dict.items()))
                    track_list = getList(adjacency_list)

                    # exception...
                    if len(track_list)==2:
                        track_list = [1]

                    #######################
                    ### MAP MATCHING ######
                    #######################

                    # Inititate empty dictionaries to store distances between points and times (secs) between points
                    distance_between_points = {}
                    # speed_between_points = {}
                    # time_track = {}
                    # HOUR_track = {}
                    # timedate_track = {}

                    if len(track_list) > 1:
                        max_prob_node = []
                        for i in range(len(track_list)):
                            if track_list[i] == max(track_list):
                                break

                            trans_prob = {}
                            emiss_prob = {}
                            shortest_path = {}
                            SHORT_PATH = []

                            for u in adjacency_list[track_list[i]]:
                                for v in adjacency_list[track_list[i+1]]:
                                    # distance travelled from one point to the next one (in km)
                                    distance_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).progressive) - int(
                                        (viasat[viasat['ID'] == track_list[i]]).progressive)
                                    distance_VIASAT = abs(distance_VIASAT) / 1000  # in Km
                                    # add distance to a dictionary in function of edge "u"
                                    distance_between_points[u] = distance_VIASAT
                                    # time spent to travel from one point to te next one (in seconds)
                                    # time_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).path_time) - int(
                                    #     (viasat[viasat['ID'] == track_list[i]]).path_time)
                                    # # add time to a dictionary in function of edge "u"
                                    # time_track[u] = time_VIASAT
                                    # HOUR_track[u] = int((viasat[viasat['ID'] == track_list[i]]).hour)
                                    # timedate_track[u] = ((viasat[viasat['ID'] == track_list[i]]).timedate).to_string()[4:23]

                                    # mean speed between two points (tracks)
                                    # if int((viasat[viasat['ID'] == track_list[i + 1]]).speed) == 0:
                                    #     speed_VIASAT = int((viasat[viasat['ID'] == track_list[i]]).speed)
                                    # elif int((viasat[viasat['ID'] == track_list[i]]).speed) == 0:
                                    #     speed_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).speed)
                                    # else:
                                    #     speed_VIASAT = (int(
                                    #         (viasat[viasat['ID'] == track_list[i + 1]]).speed) + int(
                                    #         (viasat[viasat['ID'] == track_list[i]]).speed)) / 2
                                    #
                                    # # add speed to a dictionary in function of edge "u"
                                    # speed_between_points[u] = speed_VIASAT
                                    if u != v:
                                        try:
                                            if u != v:
                                                shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                short_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                SHORT_PATH.append(short_path)
                                                if shortest_path[u] <= distance_VIASAT:
                                                    trans_prob[u] = transition_prob(u, v)
                                                    emiss_prob[u] = emission_prob(u)
                                        except nx.NetworkXNoPath:
                                            pass


                            if len(trans_prob) != 0:
                                MAX_trans_key = max(trans_prob, key=trans_prob.get)
                                MAX_emiss_key = max(emiss_prob, key=emiss_prob.get)
                                MAX_trans_value = trans_prob.get(MAX_trans_key)
                            else:
                                MAX_trans_key = 0
                                MAX_trans_value = 0
                            if MAX_trans_value !=0:
                                max_prob_node.append(MAX_trans_key)


                                while MAX_trans_key not in adjacency_list[track_list[i + 1]]:
                                    # do not execute this part of the code if we only have 3 GPS tracks
                                    if 1 <= len(track_list) <= 3:
                                        break
                                    adjacency_list[track_list[i]].remove(MAX_trans_key)
                                    max_prob_node.remove(MAX_trans_key)
                                    # and start calculation again
                                    trans_prob = {}
                                    emiss_prob = {}
                                    shortest_path = {}
                                    SHORT_PATH = []

                                    for u in adjacency_list[track_list[i]]:
                                        for v in adjacency_list[track_list[i + 1]]:
                                            # distance travelled from one point to the next one (in km)
                                            distance_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).progressive) - int(
                                                (viasat[viasat['ID'] == track_list[i]]).progressive)
                                            distance_VIASAT = abs(distance_VIASAT) / 1000  # in Km
                                            # add distance to a dictionary in function of edge "u"
                                            distance_between_points[u] = distance_VIASAT
                                            # time spent to travel from one point to te next one (in seconds)
                                            # time_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).path_time) - int(
                                            #     (viasat[viasat['ID'] == track_list[i]]).path_time)
                                            # # add time to a dictionary in function of edge "u"
                                            # time_track[u] = time_VIASAT
                                            # HOUR_track[u] = int((viasat[viasat['ID'] == track_list[i]]).hour)
                                            # timedate_track[u]=((viasat[viasat['ID'] == track_list[i]]).timedate).to_string()[4:23]

                                            # mean speed between two points (tracks)
                                            # if int((viasat[viasat['ID'] == track_list[i + 1]]).speed) == 0:
                                            #     speed_VIASAT = int((viasat[viasat['ID'] == track_list[i]]).speed)
                                            # elif int((viasat[viasat['ID'] == track_list[i]]).speed) == 0:
                                            #     speed_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).speed)
                                            # else:
                                            #     speed_VIASAT = (int(
                                            #         (viasat[viasat['ID'] == track_list[i + 1]]).speed) + int(
                                            #         (viasat[viasat['ID'] == track_list[i]]).speed)) / 2
                                            #
                                            # # add speed to a dictionary in function of edge "u"
                                            # speed_between_points[u] = speed_VIASAT
                                            if u != v:
                                                try:
                                                    if u != v:
                                                        shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                        short_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                        SHORT_PATH.append(short_path)
                                                        # check if the distance between 'track_list[i+1]' and 'track_list[i]' is less than shortest_path[u]
                                                        if shortest_path[u] <= distance_VIASAT:
                                                            trans_prob[u] = transition_prob(u, v)
                                                            emiss_prob[u] = emission_prob(u)
                                                except nx.NetworkXNoPath:
                                                    pass
                                    if len(trans_prob) != 0:
                                        MAX_trans_key = max(trans_prob, key=trans_prob.get)
                                        MAX_emiss_key = max(emiss_prob, key=emiss_prob.get)
                                        MAX_trans_value = trans_prob.get(MAX_trans_key)
                                    if MAX_trans_value != 0:
                                        # compare distance: node-GPS track with node-edge
                                        if MAX_emiss_key in DISTANCES_dict.keys():
                                            # if MAX_trans_key != MAX_emiss_key and DISTANCES_dict[MAX_emiss_key] > emiss_prob[MAX_emiss_key]:
                                            #    max_prob_node.append(MAX_emiss_key)
                                            # else:
                                            max_prob_node.append(MAX_trans_key)
                                        else:
                                            if MAX_trans_key != MAX_emiss_key:
                                                lat = float(viasat.latitude[viasat.ID == track_list[i]])
                                                lon = float(viasat.longitude[viasat.ID == track_list[i]])
                                                max_prob_node.append(MAX_emiss_key)
                                            else:
                                                max_prob_node.append(MAX_trans_key)
                                    if MAX_trans_key not in adjacency_list[track_list[i + 1]]:
                                        break
                            # check if the first GPS track is assigned to a node
                            if track_list[i]==0:
                                if MAX_trans_key==0 or MAX_emiss_key== 0:
                                    # get nearest node to the GPS track along the same edge
                                    lat = float(viasat.latitude[viasat.ID == track_list[i]])
                                    lon = float(viasat.longitude[viasat.ID == track_list[i]])
                                    point = (lat, lon)
                                    geom, u, v = ox.get_nearest_edge(grafo, point)
                                    nearest_node = min((u, v), key=lambda n: ox.great_circle_vec(lat, lon, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                                    max_prob_node.append(nearest_node)
                            # check if there is a node with the minimum path to the the next node
                            if MAX_trans_key in shortest_path.keys() and MAX_trans_key in max_prob_node:
                                if shortest_path[MAX_trans_key] < min(SHORT_PATH):
                                    print("find the next shortest path")
                                else:
                                    new_node = min(shortest_path, key=shortest_path.get)
                                    max_prob_node.remove(MAX_trans_key)
                                    max_prob_node.append(new_node)

                            if len(max_prob_node) != 0:
                                distance_to_track_i = ox.great_circle_vec(lat1 = grafo.nodes[max_prob_node[-1]]['y'],
                                                                          lng1 = grafo.nodes[max_prob_node[-1]]['x'],
                                                                          lat2=float(viasat.latitude[
                                                                                         viasat.ID == track_list[i]]),
                                                                          lng2=float(viasat.longitude[
                                                                                         viasat.ID == track_list[
                                                                                             i]])) / 1000

                                distance_to_track_i1 = ox.great_circle_vec(lat1 = grafo.nodes[max_prob_node[-1]]['y'],
                                                                           lng1 = grafo.nodes[max_prob_node[-1]]['x'],
                                                                           lat2=float(viasat.latitude[
                                                                                          viasat.ID == track_list[
                                                                                              i + 1]]),
                                                                           lng2=float(viasat.longitude[
                                                                                          viasat.ID == track_list[
                                                                                              i + 1]])) / 1000
                                dists = [distance_to_track_i, distance_to_track_i1]
                                lat1 = float(viasat.latitude[viasat.ID == track_list[i + 1]])
                                lon1 = float(viasat.longitude[viasat.ID == track_list[i + 1]])
                                lat0 = float(viasat.latitude[viasat.ID == track_list[i]])
                                lon0 = float(viasat.longitude[viasat.ID == track_list[i]])
                                point1 = (lat1, lon1)
                                point0 = (lat0, lon0)
                            ####/////////////////////////////////////////########################################
                                if np.mean(dists) <= (distance_VIASAT/2):
                                    geom0, u0, v0 = ox.get_nearest_edge(grafo, point0)
                                    geom1, u1, v1 = ox.get_nearest_edge(grafo, point1)
                                    nn0 = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                                    max_prob_node.append(nn0)
                                    nn1 = min((u1, v1), key=lambda n: ox.great_circle_vec(lat1, lon1, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                                    max_prob_node.append(nn1)
                    elif len(track_list)==1:
                        for KEY in adjacency_list.keys():
                            max_prob_node = list(adjacency_list[KEY])  # only one edge

                    ###################################################################################
                    ###################################################################################
                    ###### BUILD the PATH #############################################################
                    ###################################################################################
                    ###################################################################################

                    # get unique values (ordered) - remove duplicates
                    max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

                    ##### get last element of the "adjacency_list" (dictionary)
                    last_key_nodes = list(adjacency_list.keys())[-1]
                    last_nodes = list(adjacency_list[last_key_nodes])  ## get both of them!
                    max_prob_node.extend(last_nodes)

                    ### check that the nodes are on the same direction!!!!! ####
                    ## remove nodes that are not on the same directions..........
                    NODE_TO_REMOVE = []
                    for i in range(len(max_prob_node)-2):
                        if (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:,[0]]) and (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [1]]):
                                node_to_remove = ([max_prob_node[1:(len(max_prob_node) - 1)][i]])[0]
                                NODE_TO_REMOVE.append(node_to_remove)
                    ## remove node from the max_prob_node list
                    if len(NODE_TO_REMOVE) != 0:
                        max_prob_node = [i for i in max_prob_node if i not in NODE_TO_REMOVE]


                    ## check that the first GPS track point is near the first node of the 'max_prob_node'
                    i = 0
                    lat0 = float(viasat.latitude[viasat.ID == track_list[i]])
                    lon0 = float(viasat.longitude[viasat.ID == track_list[i]])
                    point0 = (lat0, lon0)
                    geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
                    nearest_node_first = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                    if nearest_node_first in max_prob_node:
                        if max_prob_node[0] != nearest_node_first:
                            idx = max_prob_node.index(nearest_node_first)
                            ## move 'nearest_node_first' at the first place
                            max_prob_node.insert(0, max_prob_node.pop(idx))

                    # append the very first node to the max_prob_node list
                    # max_prob_node = [u0] + max_prob_node

                    # ORIGIN = max_prob_node[0]
                    # DESTINATION = max_prob_node[-1]

                    ## check that there is continuity between the first and second pair of node (within two consecutive buffers)
                    EDGES_BUFFER = df_edges.drop_duplicates('buffer_ID')
                    ## get the buffer number in which there is the u0
                    if len(EDGES_BUFFER[EDGES_BUFFER.u == u0]) > 0:
                        n_buffer = EDGES_BUFFER[EDGES_BUFFER.u == u0]['buffer_ID'].iloc[0]
                        # check if the consecutive bugger exists...
                        if (n_buffer + 1) in list(EDGES_BUFFER['buffer_ID']):
                            ## append the very first node to the max_prob_node list
                            max_prob_node = [u0] + max_prob_node


                    ## remove duplicates
                    max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

                    ### make a Dataframe with the list of the max_prob_nodes
                    DF_max_prob_node = pd.DataFrame(max_prob_node)
                    DF_max_prob_node = DF_max_prob_node.rename(columns={0: 'u'})
                    ## merge 'max_prob_node' with edges
                    ## make a list of all consecutive u,v from EDGES_BUFFER...
                    node_EDGES = EDGES_BUFFER[['u', 'v']].values.tolist()
                    node_EDGES = [val for sublist in node_EDGES for val in sublist]
                    ## remove duplicates
                    node_EDGES = list(OrderedDict.fromkeys(node_EDGES))
                    ## make a dataframe
                    DF_node_EDGES = pd.DataFrame(node_EDGES)
                    DF_node_EDGES = DF_node_EDGES.rename(columns={0: 'u'})

                    # filter gdf_edges with df_nodes
                    keys = list(DF_max_prob_node.columns.values)
                    index_edges = DF_node_EDGES.set_index(keys).index
                    index_df_nodes = DF_max_prob_node.set_index(keys).index
                    max_prob_node = DF_node_EDGES[index_edges.isin(index_df_nodes)]

                    max_prob_node = list(max_prob_node['u'])

                    ## attach the FIRST NODE of the EDGES initialy found
                    ## make a list of all the indices of the VIASAT DATA
                    VIASAT_INDICES = list(viasat.ID)
                    first_index = VIASAT_INDICES[0]
                    last_index = VIASAT_INDICES[-1]
                    lat0 = float(viasat.latitude[viasat.ID == first_index])
                    lon0 = float(viasat.longitude[viasat.ID == first_index])
                    point0 = (lat0, lon0)
                    lat_last = float(viasat.latitude[viasat.ID == last_index])
                    lon_last = float(viasat.longitude[viasat.ID == last_index])
                    point_last = (lat_last, lon_last)
                    ## get the node with the minimu, distance from the track correspondingg to the BUFFER number..
                    geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
                    very_first_node = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'],
                                                                                      grafo.nodes[n]['x']))
                    geom_last, u_last, v_last = ox.get_nearest_edge(grafo, point_last)
                    very_last_node = min((u_last, v_last),
                                         key=lambda n: ox.great_circle_vec(lat_last, lon_last, grafo.nodes[n]['y'],
                                                                           grafo.nodes[n]['x']))
                    # ox.get_nearest_node(grafo, point0)
                    ## append the 'very_first_node' and the 'very_last_node'
                    max_prob_node = [very_first_node] + max_prob_node + [very_last_node]

                    ## remove duplicates
                    max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

                    #### build matched route with all max_prob_node  #####
                    matched_route = []
                    all_matched_edges = []
                    for origin, destination in zip(max_prob_node, max_prob_node[1:]):
                        try:
                            # use full complete graph to build the final path
                            route = nx.dijkstra_path(grafo, origin, destination, weight='length')
                            path_edges = list(zip(route, route[1:]))
                            all_matched_edges.append(path_edges)
                            matched_route.append(route)
                        except nx.NetworkXNoPath:
                            pass

                    ##########///////////////////////////////////////////////////// ##########################################
                    # if more than 2 element of the matched_route[2] are in matched_route[3], then delete matched_route[3]
                    len_matched_edges = len(all_matched_edges)
                    if len_matched_edges >1:
                        list1 = all_matched_edges[len_matched_edges-2]
                        list2 = all_matched_edges[len_matched_edges-1]
                        common_nodes_last=[elem for elem in list1 if elem in list2]
                        if len(common_nodes_last) >= 2:
                            all_matched_edges.remove(list2)
                    ##########///////////////////////////////////////////////////// ##########################################

                    if len(all_matched_edges) > 1:
                        # isolate edges in the grafo from 'all_matched_edges'
                        df_nodes = []
                        for i in range(len(all_matched_edges)):
                            route = all_matched_edges[i]
                            for nodes in route:
                                df_nodes.append(nodes)

                        df_nodes = pd.DataFrame(df_nodes)
                        df_nodes.columns = ['u', 'v']

                        ## merge ordered list of nodes with edges from grafo
                        edges_matched_route = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
                        edges_matched_route = gpd.GeoDataFrame(edges_matched_route)
                        edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)

                        # filter 'edges_matched_route' (remove key = 1)
                        filter_edge = edges_matched_route[edges_matched_route.key != 0]
                        if len(filter_edge) !=0:
                            selected_edges = edges_matched_route[edges_matched_route.u.isin(list(pd.to_numeric(filter_edge.u)))]
                            # get the with row with key == 0 (to be then removed
                            idx_edge = list(selected_edges[selected_edges.key == 0].index)
                            # filter row in 'edges_matched_route' with index == idx_edge
                            edges_matched_route = edges_matched_route[~edges_matched_route.index.isin(idx_edge)]
                        # select edges with 'key' == 1
                        if len(filter_edge) > 1:
                            selected_edges = edges_matched_route[edges_matched_route.u.isin(list(pd.to_numeric(filter_edge.u)))]
                            if len(selected_edges) == 1:
                                idx_edge = list(selected_edges[selected_edges.key == 1].index)
                                # filter row in 'edges_matched_route' with index == idx_edge
                                edges_matched_route = edges_matched_route[~edges_matched_route.index.isin(idx_edge)]

                        ##################################################################################
                        ######## Build the final table as from Gaetano ###################################
                        ##################################################################################

                        ## merge with adjacency list to assign corresponding tracks to each edge.....
                        DF_edges_matched_route = pd.DataFrame(edges_matched_route)
                        ## consider using KKK to get the 'idtrace'
                        KKK = KKK.rename(columns={'ID': 'buffer_ID'})
                        HHH = pd.merge(DF_edges_matched_route, df_edges, on=['u', 'v'], how='left')
                        HHH['buffer_ID'] = HHH['buffer_ID'].bfill()
                        HHH.drop_duplicates(['u', 'v'], inplace=True)
                        HHH['buffer_ID'] = HHH['buffer_ID'].ffill()
                        KKK_new = KKK[['u', 'v', 'buffer_ID', 'id', 'progressive', 'totalseconds', 'path_time', 'speed',
                                       'timedate', 'TRIP_ID', 'idtrajectory', 'idterm', 'anomaly']]
                        edges_matched_route_GV = pd.merge(HHH, KKK_new, on=['u', 'v', 'buffer_ID'], how='left')
                        # edges_matched_route_GV['id'] = edges_matched_route_GV['id'].bfill()
                        edges_matched_route_GV['id'] = edges_matched_route_GV['id'].ffill()
                        edges_matched_route_GV['id'] = edges_matched_route_GV['id'].bfill()
                        try:
                            edges_matched_route_GV['id'] = edges_matched_route_GV.id.astype('int')
                        except ValueError:
                             pass

                        edges_matched_route_GV['idtrajectory'] = edges_matched_route_GV['idtrajectory'].ffill()
                        edges_matched_route_GV['idtrajectory'] = edges_matched_route_GV['idtrajectory'].bfill()
                        try:
                            edges_matched_route_GV['idtrajectory'] = edges_matched_route_GV.idtrajectory.astype('int')
                        except ValueError:
                            pass

                        edges_matched_route_GV['totalseconds'] = edges_matched_route_GV['totalseconds'].ffill()
                        edges_matched_route_GV['totalseconds'] = edges_matched_route_GV['totalseconds'].bfill()
                        edges_matched_route_GV['totalseconds'] = edges_matched_route_GV.totalseconds.astype('int')

                        edges_matched_route_GV['timedate'] = edges_matched_route_GV['timedate'].ffill()
                        edges_matched_route_GV['timedate'] = edges_matched_route_GV['timedate'].bfill()

                        # compute the difference between last and first time within the same "progressive" value
                        edges_matched_route_GV['progressive'] = edges_matched_route_GV['progressive'].ffill()
                        edges_matched_route_GV['progressive'] = edges_matched_route_GV['progressive'].bfill()
                        edges_matched_route_GV['progressive'] = edges_matched_route_GV.progressive.astype('int')

                        edges_matched_route_GV = edges_matched_route_GV.rename(columns={'id': 'idtrace'})
                        edges_matched_route_GV['sequenza'] = edges_matched_route_GV.index

                        # last = edges_matched_route_GV.groupby('progressive').nth(-1)
                        first = edges_matched_route_GV.groupby('progressive').nth(0)
                        first_time = first.set_index('idtrace')
                        first_progressive = first.reset_index(level=0)
                        diff_time = first_time.totalseconds.diff()
                        diff_progressive = first_progressive.progressive.diff()
                        # shift
                        diff_time = diff_time.shift(-1)
                        diff_time = pd.DataFrame(diff_time)
                        diff_time['idtrace'] = diff_time.index
                        # reset index
                        diff_time.reset_index(drop=True, inplace=True)
                        diff_progressive = diff_progressive.shift(-1)

                        # concatenate diff_time with diff_progressive
                        df_speed = pd.concat([diff_time, diff_progressive], axis=1)
                        if len(df_speed) == 1:
                            edges_matched_route_GV['mean_speed'] = edges_matched_route_GV[edges_matched_route_GV.speed.notnull()]['speed']
                        # df_diff = df_diff.bfill(axis='rows')
                        else:
                            df_speed['mean_speed'] = (df_speed.progressive/1000)/(df_speed.totalseconds/3600)
                            # add last instant speed
                            df_speed["mean_speed"].iloc[len(df_speed)-1] = first.speed.iloc[len(first)-1]
                        # merge df_speed with main dataframe "edges_matched_route_GV" using "idtrace" as common field
                        edges_matched_route_GV = pd.merge(edges_matched_route_GV, df_speed, on=['idtrace'], how='left')
                        edges_matched_route_GV.drop(['totalseconds_y'], axis=1, inplace= True)
                        edges_matched_route_GV = edges_matched_route_GV.rename(columns={'totalseconds_x': 'totalseconds'})

                        edges_matched_route_GV['mean_speed'] = edges_matched_route_GV['mean_speed'].ffill()
                        edges_matched_route_GV['mean_speed'] = edges_matched_route_GV['mean_speed'].bfill()
                        edges_matched_route_GV['mean_speed'] = edges_matched_route_GV.mean_speed.astype('int')
                        edges_matched_route_GV['TRIP_ID'] = edges_matched_route_GV['TRIP_ID'].ffill()
                        edges_matched_route_GV['TRIP_ID'] = edges_matched_route_GV['TRIP_ID'].bfill()
                        edges_matched_route_GV['idterm'] = edges_matched_route_GV['idterm'].ffill()
                        edges_matched_route_GV['idterm'] = edges_matched_route_GV['idterm'].bfill()
                        ## remove rows with negative "mean_speed"...for now....
                        edges_matched_route_GV = edges_matched_route_GV[edges_matched_route_GV['mean_speed'] > 0]
                        edges_matched_route_GV = edges_matched_route_GV[edges_matched_route_GV['mean_speed'] < 190]
                        edges_matched_route_GV = gpd.GeoDataFrame(edges_matched_route_GV)

                        if len(edges_matched_route_GV) > 0:
                            ## populate a DB
                            try:
                                final_map_matching_table_GV = edges_matched_route_GV[['idtrajectory',
                                                                                      'u', 'v', 'idterm',
                                                                                      'idtrace', 'sequenza',
                                                                                      'mean_speed',
                                                                                      'timedate',
                                                                                      'TRIP_ID', 'length']]

                                final_map_matching_table_GV = gpd.GeoDataFrame(final_map_matching_table_GV)

                                ### Connect to a DB and populate the DB  ###
                                connection = engine.connect()
                                ## final_map_matching_table_GV['geom'] = final_map_matching_table_GV['geometry'].apply(wkb_hexer)
                                ## final_map_matching_table_GV.drop('geometry', 1, inplace=True)

                                final_map_matching_table_GV.to_sql("mapmatching_all", con=connection, schema="public",
                                                 if_exists='append')

                                #################################################################
                                #################################################################
                                ### find the travelled distance of the matched route
                                sum_distance_mapmatching = sum(final_map_matching_table_GV.length)
                                ## calculate the accuracy of the matched route compared to the sum of the differences of the progressives (from Viasat data)
                                ###  ACCURACY: ([length of the matched trajectory] / [length of the travelled distance (sum  delta progressives)])*100
                                try:
                                    accuracy = int(int((sum_distance_mapmatching / sum_progressive) * 100))
                                    df_accuracy = pd.DataFrame({'accuracy': [accuracy], 'TRIP_ID': [trip]})
                                    df_accuracy.to_sql("accuracy", con=connection, schema="public",
                                                     if_exists='append')
                                except ZeroDivisionError:
                                    pass
                                connection.close()

                            except KeyError:
                                pass
    return

################################################
##### run all script using multiprocessing #####
################################################

## check how many processer we have available:
# print("available processors:", mp.cpu_count())

if __name__ == '__main__':
    # pool = mp.Pool(processes=mp.cpu_count()) ## use all available processors
    pool = mp.Pool(processes=45)     ## use 60 processors
    print("++++++++++++++++ POOL +++++++++++++++++", pool)
    results = pool.map(func, [(last_track_idx, track_ID) for last_track_idx, track_ID in enumerate(all_ID_TRACKS)])
    pool.close()
    pool.close()
    pool.join()

    conn_HAIG.close()
    cur_HAIG.close()
