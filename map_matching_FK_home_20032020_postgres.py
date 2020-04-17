
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
today = date.today()
today = today.strftime("%b-%d-%Y")

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to PGadmnin SQL server
conn = db_connect.connect_viasat()
cur = conn.cursor()

# viasat_data = pd.read_sql_query('''
#              SELECT * FROM public.dataraw
#          WHERE date(timedate) = '2019-04-15'
#                     AND idterm = '2839293' ''', conn)
# viasat_data.drop_duplicates(['latitude', 'longitude'], inplace=True)
# viasat_data.drop_duplicates(['progressive'], inplace=True)
# # remove viasat data with 'panel' == 0  (when the car does not move, the engine is OFF)
# viasat_data = viasat_data[viasat_data['panel'] != 0]
# viasat_data.to_csv('viasat_data_2839293.csv')


all_EDGES = pd.DataFrame([])

# get all ID terminal of Viasat data
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.obu''', conn)

# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())
# len(all_VIASAT_IDterminals)

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

# track_ID = '2507511'

# track_ID = '2507530'
# track_ID = "2678884"

# DATE = '2019-04-11'
# track_ID = '3188580'


# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)
    ################################################################################
    # create basemap
    ave_LAT = 37.53988692816245
    ave_LON = 15.044971594798902
    my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
    ################################################################################################
    ## to be used when the query stop. Start from the last saved index
    last_track_idx = 324+68+10+115
    for last_track_idx, track_ID in enumerate(all_ID_TRACKS[last_track_idx:len(all_ID_TRACKS)]):
    #for last_track_idx, track_ID in enumerate(all_ID_TRACKS):
        track_ID = str(track_ID)
        print('VIASAT GPS track:', track_ID)
        viasat_data = pd.read_sql_query('''
                    SELECT * FROM public.dataraw 
                    WHERE date(timedate) = %s 
                    AND idterm = %s ''', conn, params=(DATE, track_ID))
        # remove duplicate GPS tracks (@ same position)
        viasat_data.drop_duplicates(['latitude', 'longitude'], inplace=True)

        ## slice VIASAT data by TRACK_ID each time "panel == 0" . Make separate datasets each time we meet "panel == 0
        # slice_list = []
        # slice = pd.DataFrame([])
        # for idx, row in viasat_data.iterrows():
        #         if row.panel == 1:
        #             # print(row.panel)
        #             slice = slice.append(row)
        #             viasat_data = slice
        #         if row.panel == 0:
        #             # print(slice)
        #             slice = pd.DataFrame([])
        #             continue
        #             # goes back to top of loop

        # remove viasat data with 'progressive' == 0
        viasat_data = viasat_data[viasat_data['progressive'] != 0]
        # remove viasat data with 'speed' == 0
        # viasat_data = viasat_data[viasat_data['speed'] != 0]
        # select only rows with different reading of the odometer (vehicle is moving on..)
        # viasat_data.drop_duplicates(['progressive'], inplace= True)
        # remove viasat data with 'panel' == 0  (when the car does not move, the engine is OFF)
        # viasat_data = viasat_data[viasat_data['panel'] != 0]
        # remove data with "speed" ==0  and "odometer" != 0 AT THE SAME TIME!
        viasat_data = viasat_data[~((viasat_data['progressive'] != 0) & (viasat_data['speed'] == 0))]
        # select only VIASAT point with accuracy ("grade") between 1 and 22
        viasat_data = viasat_data[(1 <= viasat_data['grade']) & (viasat_data['grade'] <= 15)]
        # viasat_data = viasat_data[viasat_data['direction'] != 0]
        if len(viasat_data) == 0:
            print('============> no VIASAT data for that day ==========')

        # viasat_data = viasat_data[1:(len(viasat_data)-1)]
########################################################################################
########################################################################################
########################################################################################

# load grafo
# file_graphml = 'Catania__Italy_cost.graphml'
# grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)
# gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

        if len(viasat_data) > 5  and len(viasat_data) < 90:
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
            viasat = viasat[["longitude", "latitude", "progressive", "path_time", "speed", "hour", "timedate"]]
            ## get only VIASAT data where difference between two consecutive points is > 3600 secx (1 hour)
            ## this is to define the TRIP after a 'long' STOP time
            viasat1 = viasat
            diff_time = viasat.path_time.diff()
            diff_time = diff_time.fillna(0)
            row = []
            for i in range(len(diff_time)):
                if diff_time.iloc[i] >= 3600:
                    row.append(i)
            # get next element of the list row
            # row_next = [x+1 for x in row]
            if len(row)>0:
                row.append("end")
                for idx, j in enumerate(row):
                    print(j)
                    print(idx)
                    if j == row[0]:
                        lista = [i for i in range(0,j)]
                    if (idx > 0 and j != 'end'):
                        lista = [i for i in range(row[idx - 1], row[idx])]
                    if j == "end":
                        lista = [i for i in range(row[idx-1], len(viasat))]
                        print(lista)
                        viasat = viasat1.iloc[lista, :]

                    if len(viasat_data) > 5:
                        ## introduce a dynamic buffer
                        dx = max(viasat.longitude) - min(viasat.longitude)
                        dy = max(viasat.latitude) - min(viasat.latitude)
                        if dx < 0.007:
                            buffer_diam = 0.00020
                        else:
                            buffer_diam = 0.00005

                        buffer_diam = 0.00005

                        ## get extent of viasat data
                        ext = 0.025
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

                        # get graph only within the extension of the rectangular polygon
                        # filter some features from the OSM graph
                        # filter = (
                        #     '["highway"!~"living_street|abandoned|footway|pedestrian|raceway|cycleway|steps|construction|'
                        #     'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path|secondary_link|'
                        #     'tertiary_link"]')
                        filter = (
                            '["highway"!~"living_street|abandoned|steps|construction|'
                            'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path"]')
                        grafo = ox.graph_from_polygon(viasat_extent.geometry[0], custom_filter=filter)

                        # ox.plot_graph(grafo)
                        # ox.save_graphml(grafo, filename='partial_OSM.graphml')


                        # make a geo-dataframe from the grapho
                        gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

                        # reset indices
                        viasat.reset_index(drop=True, inplace=True)

                        # create an index column
                        viasat["ID"] = viasat.index

                        ######################################################

                        # add VIASAT GPS track on the base map (defined above)
                        for i in range(len(viasat)):
                            folium.CircleMarker(location=[viasat.latitude.iloc[i], viasat.longitude.iloc[i]],
                                                popup= (track_ID + '_' + str(viasat.ID.iloc[i])),
                                                radius=1,
                                                color="black",
                                                fill=True,
                                                fill_color="black",
                                                fill_opacity=1).add_to(my_map)
                        # my_map.save("matched_route_21032020.html")
                        my_map.save("matched_route_VIASAT_" + DATE + '_' + today + ".html")

                        ######################################################

                        # build a geodataframe with VIASAT data
                        geometry = [Point(xy) for xy in zip(viasat.longitude, viasat.latitude)]
                        # viasat = viasat.drop(['longitude', 'latitude'], axis=1)
                        crs = {'init': 'epsg:4326'}
                        viasat_gdf = GeoDataFrame(viasat, crs=crs, geometry=geometry)
                        # viasat_gdf.plot()

                        # Buffer the points by some units (unit is kilometer)
                        buffer = viasat_gdf.buffer(buffer_diam)  # 25 meters # this is a geoseries  (0.00010)
                        # buffer.plot()
                        # make a dataframe
                        buffer_viasat = pd.DataFrame(buffer)
                        buffer_viasat.columns = ['geometry']


                        # geodataframe with edges
                        type(gdf_edges)
                        # gdf_edges.plot()

                        '''
                        ## circumscript the area of the track (buffer zone)
                        union = buffer.unary_union
                        envelope = union.envelope
                        rectangle_viasat = gpd.GeoDataFrame(geometry=gpd.GeoSeries(envelope))
                        rectangle_viasat.plot()
                        
                        # grafo = ox.load_graphml(file_graphml)
                        grafo = ox.graph_from_polygon(rectangle_viasat.geometry[0])
                        ox.plot_graph(grafo)
                        '''

                        # add buffered viasat polygons
                        # save first as geojson file
                        buffer.to_file(filename='buffer_viasat.geojson', driver='GeoJSON')
                        # folium.GeoJson('buffer_viasat.geojson').add_to((my_map))
                        # my_map.save("matched_route.html")

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

                        # find all edges intersect by the buffers defined above
                        buff = []
                        index_edges = []
                        index_nodes = []
                        index_buff = []
                        edge = []
                        DISTANCES = []
                        now1 = datetime.now()
                        DISTANCES_dict = {}

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
                                    dist = distance
                                    distance = distance, via_buff.Index
                                    DISTANCES.append(distance)
                                    DISTANCES_dict[streets.u] = dist
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
                        # nn_gdf_edges.plot()

                        '''
                        # make an unique lists of all nodes inside the nn_gdf_edges
                        all_nodes = list(pd.concat([gdf_edges['u'], gdf_edges['v']]).unique())
                        nn_list_nodes = list(pd.concat([nn_gdf_edges['u'], nn_gdf_edges['v']]).unique())
                        
                        # all_nodes minus nn_list_nodes
                        set1 = set(all_nodes)
                        set2 = set(nn_list_nodes)
                        set_difference = set1.difference(set2)
                        subtracted_list = list(set_difference)
                        
                        # create a reduced graph with onle the near neighbour nodes
                        grafo.remove_nodes_from(subtracted_list)
                        ox.plot_graph(grafo)
                        '''


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
                        if len(edge) > 0:
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
                            def great_circle_track(u):
                                # Calculate the great circle distance between two points from viasat data (progressive)
                                u_track = nodes_dict.get(u)[0][2]
                                v_track = u_track+1
                                if v_track <= max(viasat.ID):
                                    distance =int((viasat[viasat['ID'] == v_track]).progressive) - int((viasat[viasat['ID'] == u_track]).progressive)
                                    distance = distance/1000 # in Km
                                else:
                                    distance = 0
                                return distance


                            # "sigma" has been calculated ad the standard deviation of all the distances between viasat measurements and nodes
                            # SIGMA_Z = 1.4826*np.median(DISTANCES) # meters
                            SIGMA_Z = 1.4826*np.median([x[0] for x in DISTANCES]) # meters
                            SIGMA_Z = SIGMA_Z/1000 # Kilometers
                            print(SIGMA_Z)


                            ###############################
                            ### emission probability ######
                            ###############################

                            # Gaussian distribution of all NODES close to Viasat measurements.
                            def emission_prob(u):
                                c = 1 / (SIGMA_Z * math.sqrt(2 * math.pi))
                                # return 1 * math.exp(-0.5*(great_circle_track_node(u)/SIGMA_Z)**2)
                                return great_circle_track_node(u)/SIGMA_Z  # !!! this is only a DISTANCE!!


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
                                            great_circle_track(u))  # in Kilometers
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
                                    DIST = great_circle_track(u)  # in Km
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


                            ####################################################
                            ####################################################

                            # define the adjaceny list
                            adjacency_list = {}
                            for key in df_edges_dict.keys():
                                print(key)
                                track = df_edges_dict.get(key)
                                print(track)
                                unique_list = set(x for l in track for x in l)
                                adjacency_list[key] = unique_list

                            # if two lists of the adjacency list are identical, then take only the last one...
                            result = {}
                            for key,value in adjacency_list.items():
                                if value not in result.values():
                                    result[key] = value
                            # if len(result) == 1:
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
                            speed_between_points = {}
                            time_track = {}
                            HOUR_track = {}
                            timedate_track = {}

                            if len(track_list) > 1:
                                max_prob_node = []
                                for i in range(len(track_list)):
                                    print(track_list[i])
                                    if track_list[i] == max(track_list):
                                        break

                                    trans_prob = {}
                                    emiss_prob = {}
                                    shortest_path = {}
                                    SHORT_PATH = []

                                    # i = 0
                                    for u in adjacency_list[track_list[i]]:
                                        for v in adjacency_list[track_list[i+1]]:
                                            # distance travelled from one point to the next one (in km)
                                            distance_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).progressive) - int(
                                                (viasat[viasat['ID'] == track_list[i]]).progressive)
                                            distance_VIASAT = abs(distance_VIASAT) / 1000  # in Km
                                            # add distance to a dictionary in function of edge "u"
                                            distance_between_points[u] = distance_VIASAT
                                            # time spent to travel from one point to te next one (in seconds)
                                            time_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).path_time) - int(
                                                (viasat[viasat['ID'] == track_list[i]]).path_time)
                                            # add time to a dictionary in function of edge "u"
                                            time_track[u] = time_VIASAT
                                            HOUR_track[u] = int((viasat[viasat['ID'] == track_list[i]]).hour)
                                            timedate_track[u] = ((viasat[viasat['ID'] == track_list[i]]).timedate).to_string()[4:23]
                                            # print(((viasat[viasat['ID'] == track_list[i]]).timedate[2]).strftime(
                                            #     "%Y-%m-%d %H:%M:%S"), "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

                                            # mean speed between two points (tracks)
                                            if int((viasat[viasat['ID'] == track_list[i + 1]]).speed) == 0:
                                                speed_VIASAT = int((viasat[viasat['ID'] == track_list[i]]).speed)
                                            elif int((viasat[viasat['ID'] == track_list[i]]).speed) == 0:
                                                speed_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).speed)
                                            else:
                                                speed_VIASAT = (int(
                                                    (viasat[viasat['ID'] == track_list[i + 1]]).speed) + int(
                                                    (viasat[viasat['ID'] == track_list[i]]).speed)) / 2

                                            # add speed to a dictionary in function of edge "u"
                                            speed_between_points[u] = speed_VIASAT
                                            # print(u, v, distance_VIASAT)
                                            if u != v:
                                                print(u,v)
                                            try:
                                                if u != v:
                                                    shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                    short_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                    SHORT_PATH.append(short_path)
                                                    if shortest_path[u] <= distance_VIASAT:
                                                        print('===== KEEP ========')
                                                        print('#u:', u, '#v:', v, 'shortest_path:',
                                                              nx.shortest_path_length(grafo, u, v, weight='length') / 1000, 'distance_VIASAT:', distance_VIASAT)
                                                        trans_prob[u] = transition_prob(u, v)
                                                        emiss_prob[u] = emission_prob(u)
                                            except nx.NetworkXNoPath:
                                                     print('No path', 'u:', u, 'v:', v, )
                                    if len(trans_prob) != 0:
                                        MAX_trans_key = max(trans_prob, key=trans_prob.get)
                                        MAX_emiss_key = min(emiss_prob, key=emiss_prob.get)
                                        MAX_trans_value = trans_prob.get(MAX_trans_key)
                                        MAX_emiss_value = emiss_prob.get(MAX_emiss_key)
                                    else:
                                        MAX_trans_key = 0
                                        MAX_trans_value = 0
                                    if MAX_trans_value !=0:
                                        # MAX_prob = max(MAX_trans_value, MAX_emiss_value)
                                        print("max_prob_NODE:", MAX_trans_key)
                                        max_prob_node.append(MAX_trans_key)


                                        while MAX_trans_key not in adjacency_list[track_list[i + 1]]:
                                            # if MAX_trans_key != MAX_emiss_key:
                                            #     break
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
                                                    time_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).path_time) - int(
                                                        (viasat[viasat['ID'] == track_list[i]]).path_time)
                                                    # add time to a dictionary in function of edge "u"
                                                    time_track[u] = time_VIASAT
                                                    HOUR_track[u] = int((viasat[viasat['ID'] == track_list[i]]).hour)
                                                    timedate_track[u]=((viasat[viasat['ID'] == track_list[i]]).timedate).to_string()[4:23]

                                                    # mean speed between two points (tracks)
                                                    if int((viasat[viasat['ID'] == track_list[i + 1]]).speed) == 0:
                                                        speed_VIASAT = int((viasat[viasat['ID'] == track_list[i]]).speed)
                                                    elif int((viasat[viasat['ID'] == track_list[i]]).speed) == 0:
                                                        speed_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).speed)
                                                    else:
                                                        speed_VIASAT = (int(
                                                            (viasat[viasat['ID'] == track_list[i + 1]]).speed) + int(
                                                            (viasat[viasat['ID'] == track_list[i]]).speed)) / 2

                                                    # add speed to a dictionary in function of edge "u"
                                                    speed_between_points[u] = speed_VIASAT
                                                    # print(u, v, distance_VIASAT)
                                                    if u != v:
                                                        print(u, v)
                                                    try:
                                                        if u != v:
                                                            shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                            short_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                                            SHORT_PATH.append(short_path)
                                                            # print('#u:', u, '#v:', v, 'shortest_path:', nx.shortest_path_length(grafo, u, v, weight='length') / 1000)
                                                            # check if the distance between 'track_list[i+1]' and 'track_list[i]' is less than shortest_path[u]
                                                            if shortest_path[u] <= distance_VIASAT:
                                                                print('===== KEEP ========')
                                                                print('#u:', u, '#v:', v, 'shortest_path:',
                                                                      nx.shortest_path_length(grafo, u, v, weight='length') / 1000, 'distance_VIASAT:', distance_VIASAT)
                                                                trans_prob[u] = transition_prob(u, v)
                                                                emiss_prob[u] = emission_prob(u)
                                                    except nx.NetworkXNoPath:
                                                        print('No path', 'u:', u, 'v:', v, )
                                            if len(trans_prob) != 0:
                                                MAX_trans_key = max(trans_prob, key=trans_prob.get)
                                                MAX_emiss_key = max(emiss_prob, key=emiss_prob.get)
                                                MAX_trans_value = trans_prob.get(MAX_trans_key)
                                                MAX_emiss_value = emiss_prob.get(MAX_emiss_key)
                                            if MAX_trans_value != 0:
                                                print("max_prob_NODE:", MAX_trans_key)
                                                # compare distance: node-GPS track with node-edge
                                                if MAX_emiss_key in DISTANCES_dict.keys():
                                                    if MAX_trans_key != MAX_emiss_key and DISTANCES_dict[MAX_emiss_key] > emiss_prob[MAX_emiss_key]:
                                                        # max_prob_node.append(MAX_trans_key)
                                                        max_prob_node.append(MAX_emiss_key)
                                                    else:
                                                        max_prob_node.append(MAX_trans_key)
                                                else:
                                                    if MAX_trans_key != MAX_emiss_key:
                                                        # max_prob_node.append(MAX_trans_key)
                                                        # distance between Max_key and the GPS track (in Km)
                                                        lat = float(viasat.latitude[viasat.ID == track_list[i]])
                                                        lon = float(viasat.longitude[viasat.ID == track_list[i]])
                                                        distance_MAX_key_to_track = ox.great_circle_vec(lat1 = grafo.nodes[MAX_emiss_key]['y'],
                                                                 lng1 = grafo.nodes[MAX_emiss_key]['x'],
                                                                 lat2=lat,
                                                                 lng2=lon)/1000
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
                                            # nearest_node = ox.get_nearest_node(grafo, point)
                                            print("use nearest node to the GPS track")
                                            # distance between nearest node and the GPS track (in Km)
                                            distance_nearest_to_track = ox.great_circle_vec(lat1 = grafo.nodes[nearest_node]['y'],
                                                                                    lng1 = grafo.nodes[nearest_node]['x'],
                                                                                    lat2=lat,
                                                                                    lng2=lon)/1000
                                            max_prob_node.append(nearest_node)
                                    # check if there is a node with the minimum path the the next node
                                    if MAX_trans_key in shortest_path.keys() and MAX_trans_key in max_prob_node:
                                        if shortest_path[MAX_trans_key] < min(SHORT_PATH):
                                            print("find the next shortest path")
                                        else:
                                            new_node = min(shortest_path, key=shortest_path.get)
                                            max_prob_node.remove(MAX_trans_key)
                                            max_prob_node.append(new_node)
                                    ##### WORK in progress................................###############################
                                    ####/////////////////////////////////////////########################################
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
                                        mean_dist = np.mean(dists)
                                        lat1 = float(viasat.latitude[viasat.ID == track_list[i + 1]])
                                        lon1 = float(viasat.longitude[viasat.ID == track_list[i + 1]])
                                        lat0 = float(viasat.latitude[viasat.ID == track_list[i]])
                                        lon0 = float(viasat.longitude[viasat.ID == track_list[i]])
                                        point1 = (lat1, lon1)
                                        point0 = (lat0, lon0)
                                    ####/////////////////////////////////////////########################################
                                        if np.mean(dists) <= (distance_VIASAT/2):
                                            nearest_node = ox.get_nearest_node(grafo, point1, return_dist=True)
                                            geom0, u0, v0 = ox.get_nearest_edge(grafo, point0)
                                            geom1, u1, v1 = ox.get_nearest_edge(grafo, point1)
                                            nn0 = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                                            max_prob_node.append(nn0)
                                            nn1 = min((u1, v1), key=lambda n: ox.great_circle_vec(lat1, lon1, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                                            max_prob_node.append(nn1)
                                            # remove first element of the list
                                            # max_prob_node.pop(0)
                            elif len(track_list)==1:
                                # max_prob_node = list(adjacency_list[0]) # only one edge
                                for KEY in adjacency_list.keys():
                                    print(KEY)
                                    max_prob_node = list(adjacency_list[KEY])  # only one edge

                            ###################################################################################
                            ###################################################################################
                            ###### BUILD the PATH #############################################################
                            ###################################################################################
                            ###################################################################################

                            # get unique values (ordered) - remove duplicates
                            from collections import OrderedDict
                            max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

                            print(i)
                            ### attach the last destination node (v) to the matched list of nodes
                            if len(track_list) > 1:
                                if track_list[i] == max(track_list):
                                    last_node = v
                                    max_prob_node.append(last_node)


                            ### check that the nodes are on the same direction!!!!! ####
                            # remove nodes that are not on the same directions..........
                            NODE_TO_REMOVE = []
                            for i in range(len(max_prob_node)-2):
                                # u, v
                                # if (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [0]]) or (
                                #         ([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [1]]):
                                if (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:,[0]]) and (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [1]]):
                                        print( ([max_prob_node[1:(len(max_prob_node) - 1)][i]]), "---> OUT..!")
                                        node_to_remove = ([max_prob_node[1:(len(max_prob_node) - 1)][i]])[0]
                                        NODE_TO_REMOVE.append(node_to_remove)
                            # remove node from the max_prob_node list
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
                                    # move 'nearest_node_first' at the first place
                                    max_prob_node.insert(0, max_prob_node.pop(idx))

                            # append the very first node to the max_prob_node list
                            max_prob_node = [u0] + max_prob_node

                            ORIGIN = max_prob_node[0]
                            DESTINATION = max_prob_node[-1]

                            # build matched route
                            matched_route = []
                            all_matched_edges = []
                            for origin, destination in zip(max_prob_node, max_prob_node[1:]):
                                try:
                                    # print(origin, destination)
                                    # use full complete graph to build the final path
                                    route = nx.shortest_path(grafo, origin, destination, weight='length')
                                    path_edges = list(zip(route, route[1:]))
                                    # print(path_edges)
                                    all_matched_edges.append(path_edges)
                                    matched_route.append(route)
                                except nx.NetworkXNoPath:
                                    print('No path', 'u:', origin, 'v:', destination)


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
                                edges_matched_route = gdf_edges[index_gdf_edges.isin(index_df_nodes)]


                                # filter 'edges_matched_route' (remove key = 1)
                                filter_edge = edges_matched_route[edges_matched_route.key != 0]
                                if len(filter_edge) !=0:
                                    # selected_edges = edges_matched_route[edges_matched_route.u == int(pd.to_numeric(filter_edge.u))]
                                    selected_edges = edges_matched_route[edges_matched_route.u.isin(list(pd.to_numeric(filter_edge.u)))]
                                    # get the with row with key == 0 (to be then removed
                                    # idx_edge = int((selected_edges[selected_edges.key == 0].index).values)
                                    idx_edge = list(selected_edges[selected_edges.key == 0].index)
                                    # filter row in 'edges_matched_route' with index == idx_edge
                                    # edges_matched_route = edges_matched_route[edges_matched_route.index != idx_edge]
                                    edges_matched_route = edges_matched_route[~edges_matched_route.index.isin(idx_edge)]
                                # select edges with 'key' == 1


                                if len(filter_edge) > 1:
                                    # selected_edges = edges_matched_route[edges_matched_route.u == int(pd.to_numeric(filter_edge.u))]
                                    selected_edges = edges_matched_route[edges_matched_route.u.isin(list(pd.to_numeric(filter_edge.u)))]
                                    if len(selected_edges) == 1:
                                        # idx_edge = int((selected_edges[selected_edges.key == 1].index).values)
                                        idx_edge = list(selected_edges[selected_edges.key == 1].index)
                                        # filter row in 'edges_matched_route' with index == idx_edge
                                        # edges_matched_route = edges_matched_route[edges_matched_route.index != idx_edge]
                                        edges_matched_route = edges_matched_route[~edges_matched_route.index.isin(idx_edge)]

                                # make a dataframe from the dictionaries "time_track" and "distance_between_points"
                                time_dist_speed_edges = pd.DataFrame.from_dict(time_track, orient='index').reset_index()
                                distance_edges = pd.DataFrame.from_dict(distance_between_points, orient='index').reset_index()
                                speed_edges = pd.DataFrame.from_dict(speed_between_points, orient='index').reset_index()
                                hour_edges = pd.DataFrame.from_dict(HOUR_track, orient='index').reset_index()
                                timedate_edges = pd.DataFrame.from_dict(timedate_track, orient='index').reset_index()

                                try:
                                    time_dist_speed_edges['distance'] = distance_edges[0]
                                    time_dist_speed_edges['speed'] = speed_edges[0]
                                    time_dist_speed_edges['hour'] = hour_edges[0]
                                    time_dist_speed_edges['timedate'] = timedate_edges[0]
                                    time_dist_speed_edges.columns = ['u', 'time', 'distance', 'speed', 'hour', 'timedate']
                                    # merge "time_track and distances" with the "edges_matched_route"
                                    edges_matched_route = pd.merge(edges_matched_route, time_dist_speed_edges, on=['u'], how='left')
                                    # !!!both 'u' and 'v' must be in the "time_dist_speed_edges" dataframe['u']!!!!
                                    list_time_dist_speed_edges = list(time_dist_speed_edges['u'])
                                    boolean_filter = edges_matched_route[['u', 'v']].isin(list_time_dist_speed_edges)
                                    edges_matched_route_bool = edges_matched_route[
                                        (boolean_filter['u'] == True) & (boolean_filter['v'] == True)]
                                    edges_matched_route.loc[
                                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'time']=np.nan
                                    edges_matched_route.loc[
                                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'distance']=np.nan
                                    edges_matched_route.loc[
                                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'speed'] = np.nan
                                    edges_matched_route.loc[
                                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'hour'] = np.nan
                                    edges_matched_route.loc[
                                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'timedate'] = np.nan

                                    ## quick plot
                                    # edges_matched_route.plot()
                                    ## append all EDGES in an unique dataframe
                                    edges_matched_route['track_ID'] = track_ID
                                    edges_matched_route['DESTINATION'] = DESTINATION
                                    edges_matched_route['ORIGIN'] = ORIGIN

                                    ########################################################
                                    ######## roundabouts ###################################
                                    # AAA = pd.DataFrame(edges_matched_route)
                                    # BBB = AAA[['u', 'v']]
                                    # # find element with inverse diagonal (from bottom-left to top-right)
                                    # # select only roundabout
                                    # BBB = AAA[AAA.junction == 'roundabout']
                                    # if not BBB.empty:
                                    #     # get the rows with u, v having same 7 digits
                                    #     A = (BBB['u'].astype(str).str[:7])
                                    #     B = pd.DataFrame(A.value_counts()).reset_index()
                                    #     max_idx = B['index'][B['u'] == max(B['u'])]
                                    #     A = pd.DataFrame(A).reset_index()
                                    #     selected_idxs = A['index'][A['u'] != max_idx[0]]
                                    #     # remove selected idx from edges_matched_route
                                    #     edges_matched_route.drop(selected_idxs, inplace=True)
                                    #########################################################
                                    #########################################################

                                    all_EDGES = all_EDGES.append(edges_matched_route)
                                    ## save data
                                    # all_EDGES.to_csv('all_EDGES.csv')
                                    with open('all_EDGES_' + DATE + '_' + today + '.geojson', 'w') as f:
                                        f.write(all_EDGES.to_json())

                                    ## add plot in Folium map
                                    # save first as geojson file
                                    edges_matched_route.geometry.to_file(filename='matched_route_' + DATE + '_' + today + '.geojson',
                                                                         driver='GeoJSON')
                                    folium.GeoJson('matched_route_' + DATE + '_' + today + '.geojson').add_to((my_map))
                                    my_map.save("matched_route_VIASAT_" + DATE + '_' + today + ".html")
                                    # save last track ID and index (to be used in case the query stops)
                                    with open("last_track_ID.txt", "w") as text_file:
                                        text_file.write("last track ID and last track index: %s %s" % (track_ID, last_track_idx))
                                    # path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/outputs_catania_28022020/'
                                    # my_map.save(path + track_ID + "_" + DATE + "_matched_route.html")
                                    # path_cloud = 'C:/Users/Federico/ownCloud/Catania_RAFAEL/outputs_catania_28022020/'
                                    # my_map.save(path_cloud + track_ID + "_" + DATE + "_matched_route.html")
                                except KeyError:
                                    print("no distance_edges")

#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################

## close connection to DATABASE
conn.close()
cur.close()