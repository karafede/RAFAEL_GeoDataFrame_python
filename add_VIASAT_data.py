

import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL')
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

# today date
# today = date.today()
# today = today.strftime("%b-%d-%Y")

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

def viasat_map_data(file_graphml, road_type, place_country):

           # connect to PGadmnin SQL server
        conn = db_connect.connect_viasat()
        cur = conn.cursor()

        # get all ID terminal of Viasat data
        all_VIASAT_IDterminals = pd.read_sql_query(
            ''' SELECT *
                FROM public.obu''', conn)

        # make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
        all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())
        len(all_VIASAT_IDterminals)

        # make a list of unique dates (only dates not times!)
        # select an unique table of dates postgresql
        unique_DATES = pd.read_sql_query(
            '''SELECT DISTINCT all_dates.dates
                FROM ( SELECT dates.d AS dates
                       FROM generate_series(
                       (SELECT MIN(timedate) FROM public.dataraw),
                       (SELECT MAX(timedate) FROM public.dataraw),
                      '1 day'::interval) AS dates(d)
                ) AS all_dates
                INNER JOIN public.dataraw
                ON all_dates.dates BETWEEN public.dataraw.timedate AND public.dataraw.timedate
                ORDER BY all_dates.dates ''', conn)

        # ADD a new field with only date (no time)
        unique_DATES['just_date'] = unique_DATES['dates'].dt.date

        # #############################################################################################
        # # create basemap
        # ave_LAT = 37.53988692816245
        # ave_LON = 15.044971594798902
        # my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
        # #############################################################################################

        DATE = '2019-04-15'
        # track_ID = '2839293'

        # track_ID = '2508141'
        # track_ID = "2678884"

        track_ID = '2507530'

        # subset database with only one specific date and one specific TRACK_ID)
        viasat_data = pd.read_sql_query('''
                        SELECT * FROM public.dataraw 
                        WHERE date(timedate) = %s 
                        AND idterm = %s ''', conn, params=(DATE, track_ID))
        # remove duplicate GPS tracks (@ same position)
        viasat_data.drop_duplicates(['latitude', 'longitude'], inplace=True)

        # remove viasat data with 'progressive' == 0
        viasat_data = viasat_data[viasat_data['progressive'] != 0]
        # remove viasat data with 'speed' == 0
        viasat_data = viasat_data[viasat_data['speed'] != 0]
        # select only rows with different reading of the odometer (vehicle is moving on..)
        # viasat_data.drop_duplicates(['progressive'], inplace= True)
        # remove viasat data with 'panel' == 0  (when the car does not move, the engine is OFF)
        viasat_data = viasat_data[viasat_data['panel'] != 0]
        # remove data with "speed" ==0  and "odometer" != 0 AT THE SAME TIME!
        viasat_data = viasat_data[~((viasat_data['progressive'] != 0) & (viasat_data['speed'] == 0))]
        # select only VIASAT point with accuracy ("grade") between 1 and 22
        viasat_data = viasat_data[(1 <= viasat_data['grade']) & (viasat_data['grade'] <= 15)]
        # viasat_data = viasat_data[viasat_data['direction'] != 0]
        if len(viasat_data) == 0:
            print('============> no VIASAT data for that day ==========')

        ########################################################################################
        ########################################################################################
        ########################################################################################

        if len(viasat_data) > 2:
            fields = ["longitude", "latitude", "progressive", "timedate", "speed"]
            # viasat = pd.read_csv(viasat_data, usecols=fields)
            viasat = viasat_data[fields]

            # transform "datetime" into seconds
            # separate date from time
            # transform object "datetime" into  datetime format
            viasat['timedate'] = viasat['timedate'].astype('datetime64[ns]')
            # date
            viasat['date'] = viasat['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
            # hour
            viasat['hour'] = viasat['timedate'].apply(lambda x: x.hour)
            # minute
            viasat['minute'] = viasat['timedate'].apply(lambda x: x.minute)
            # seconds
            viasat['seconds'] = viasat['timedate'].apply(lambda x: x.second)
            # make one field with time in seconds
            viasat['path_time'] = viasat['hour'] * 3600 + viasat['minute'] * 60 + viasat['seconds']
            viasat = viasat.reset_index()
            viasat['path_time'] = viasat['path_time'] - viasat['path_time'][0]
            viasat = viasat[["longitude", "latitude", "progressive", "path_time", "speed"]]

            ## get extent of viasat data
            ext = 0.025
            ## top-right corner
            p1 = Point(np.min(viasat.longitude) - ext, np.min(viasat.latitude) - ext)
            ## bottom-right corner
            p2 = Point(np.max(viasat.longitude) + ext, np.min(viasat.latitude) - ext)
            ## bottom-left corner
            p3 = Point(np.max(viasat.longitude) + ext, np.max(viasat.latitude) + ext)
            ## top-left corner
            p4 = Point(np.min(viasat.longitude) - ext, np.max(viasat.latitude) + ext)

            # Initialize a test GeoDataFrame where geometry is a list of points
            viasat_extent = gpd.GeoDataFrame([['box', p1],
                                              ['box', p2],
                                              ['box', p3],
                                              ['box', p4]],
                                             columns=['shape_id', 'geometry'],
                                             geometry='geometry')

            # Extract the coordinates from the Point object
            viasat_extent['geometry'] = viasat_extent['geometry'].apply(lambda x: x.coords[0])
            # Group by shape ID
            #  1. Get all of the coordinates for that ID as a list
            #  2. Convert that list to a Polygon
            viasat_extent = viasat_extent.groupby('shape_id')['geometry'].apply(
                lambda x: Polygon(x.tolist())).reset_index()
            # Declare the result as a new a GeoDataFrame
            viasat_extent = gpd.GeoDataFrame(viasat_extent, geometry='geometry')
            # viasat_extent.plot()

            # get graph only within the extension of the rectangular polygon
            # filter some features from the OSM graph
            filter = (
                '["highway"!~"living_street|abandoned|footway|pedestrian|raceway|cycleway|steps|construction|'
                'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path"]')
            grafo = ox.graph_from_polygon(viasat_extent.geometry[0], custom_filter=filter)

            # ox.plot_graph(grafo)
            ox.save_graphml(grafo, filename='partial_OSM.graphml')

        # reset indices
        viasat.reset_index(drop=True, inplace=True)

        # create an index column
        viasat["ID"] = viasat.index

        ######################################################

        # load base map with the road from OSM with colors for each edge nd node
        my_map = roads_type_folium(file_graphml, road_type, place_country)

        # add VIASAT GPS track on the base map (defined above)
        for i in range(len(viasat)):
            folium.CircleMarker(location=[viasat.latitude.iloc[i], viasat.longitude.iloc[i]],
                                popup= (track_ID + '_' + str(viasat.ID.iloc[i])),
                                radius=5,
                                color="black",
                                fill=True,
                                fill_color="black",
                                fill_opacity=1).add_to(my_map)
        # my_map.save("matched_route_21032020.html")
        my_map.save(track_ID + "_VIASAT_partial_" + DATE + ".html")
