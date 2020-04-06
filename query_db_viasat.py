
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 2020

@author: fkaragul

"""

import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL')

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



def viasat_map_data(file_graphml, road_type, place_country):
    # open db from PostgreSQL
    #Connect to an existing database
    conn=db_connect.connect_viasat()
    cur = conn.cursor()

    # select few fields
    sample_data = pd.read_sql_query(
    ''' SELECT deviceId, longitude,Latitude, datetime
        FROM public.viasat_py_temp''', conn)

    all_VIASAT_data = pd.read_sql_query(
    ''' SELECT *
        FROM public.viasat_py_temp''', conn)

    #################################################################
    ######### COLUMN NAMES ##########################################
    #################################################################
    # If you just want the COLUMN NAMES
    cur.execute("SELECT * FROM public.viasat_py_temp LIMIT 0")
    colnames_VIASAT = [desc[0] for desc in cur.description]
    type(colnames_VIASAT)
    ################################################################
    ################################################################

    # filtering by date
    sample_data_timerange = pd.read_sql_query(
    ''' SELECT *
        FROM public.viasat_py_temp
        WHERE datetime BETWEEN '2019-04-11 12:24:27' AND '2019-04-11 12:40:00' ''', conn)


    '''
    print("Print each row and it's columns values")
    cur.execute("SELECT * FROM public.viasat_py_temp")
    table_by_records=cur.fetchall()
    for row in table_by_records:
           print("deviceId = ", row[0], )
           print("datatime = ", row[1])
           print("Latitude  = ", row[2],
           print("longitude = ", row[3]),"\n")
    '''

    # count number of vehicle-points by ID
    number_vehi_ID = pd.read_sql_query(
    '''SELECT deviceid, count(deviceid)
        FROM public.viasat_py_temp
        group by deviceid''', conn)

    # find the vehicle with more GPS track-points
    # number_vehi_ID.sort_values(by=['count'], inplace=True)
    max_points =  max(number_vehi_ID['count'])
    track_ID = (number_vehi_ID[number_vehi_ID['count'] == max_points].deviceid).astype(str).tolist()[0]

    # count number of vehicle-points by speed
    speed_vehi_ID = pd.read_sql_query(
    '''SELECT deviceid, speedkmh, count(deviceid)
        FROM public.viasat_py_temp
        group by deviceid, speedkmh''', conn)


    # filter VIASAT data only for the vehicle ID with the larger number of points
    track_ID = '5902695'
    track = pd.read_sql_query(
    ''' SELECT *
        FROM public.viasat_py_temp
        WHERE deviceid = '%s' ''' % track_ID, conn)
    # save data
    track.to_csv('viasat_max_data.csv')



    '''
    # make a geopandas-dataframe
    geometry = [Point(xy) for xy in zip(track.longitude, track.latitude)]
    track = track.drop(['longitude', 'latitude', "geom"], axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = GeoDataFrame(track, crs=crs, geometry=geometry)
    gdf.plot()
    '''

    '''
    # create basemap
    ave_LAT=37.53988692816245
    ave_LON=15.044971594798902
    my_map = folium.Map([ave_LAT, ave_LON], zoom_start = 11, tiles='cartodbpositron')
    my_map.save("viasat_max_data.html")
    
    for i in range(len(track)):
        folium.CircleMarker(location=[track.latitude.iloc[i], track.longitude.iloc[i]],
                                                     popup=track.deviceid.iloc[i],
                                                     radius=6,
                                                     color="black",
                                                     fill=True,
                                                     fill_color="black",
                                                     fill_opacity=1).add_to(my_map)
    my_map.save("viasat_max_data.html")
    '''

    # base map with the road from OSM
    my_map = roads_type_folium(file_graphml, road_type, place_country)


    # function to generate random colors for each deviceid
    memory = {}
    def id_to_random_color(number):
        if not number in memory:
            numByte = str.encode(number)
            hashObj = hashlib.sha1(numByte).digest()
            r, g, b = hashObj[-1] / 255.0, hashObj[-2] / 255.0, hashObj[-3] / 255.0
            memory[number]= (r, g, b, 1.0)
            return r, g, b, 1.0
        else:
            return memory[number]


    # make deviceid as string
    all_VIASAT_data['deviceid']= all_VIASAT_data['deviceid'].astype(str)

    COLOR = []
    for i in range(len(all_VIASAT_data)):
        BBB = id_to_random_color(all_VIASAT_data['deviceid'].iloc[i])
        CCC = Color(hsl=BBB[0:3])
        col = "%s" % CCC
        # print(col)
        COLOR.append(col)
    COLOR = pd.DataFrame(COLOR)
    all_VIASAT_data['color'] = COLOR

    # get unique ID for each vehicle
    list_ID = all_VIASAT_data.drop_duplicates(['deviceid'])[['deviceid']]
    for idx, rowid in list_ID.iterrows():
        # filter data by deviceid
        track = all_VIASAT_data[all_VIASAT_data['deviceid'] == rowid["deviceid"]]
        track.apply(lambda row:folium.CircleMarker(location=[row["latitude"], row["longitude"]],
                                                     popup=row["deviceid"],
                                                     radius=3,
                                                     color=row["color"],
                                                     fill=True,
                                                     fill_color=row["color"],
                                                     fill_opacity=1).add_to(my_map), axis=1)
    folium.LayerControl().add_to(my_map)
    my_map.save("viasat_data.html")

    conn.close()
    cur.close()
    # return my_map
