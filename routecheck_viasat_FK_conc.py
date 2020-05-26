
import os
os.chdir('D:\\ENEA_CAS_WORK\\Catania_RAFAEL')
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

# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

conn = db_connect.connect_EcoTripCT()
cur = conn.cursor()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')


## create extension postgis on the database HAIG_Viasat_CT  (only one time)
# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS routecheck CASCADE")
# conn_HAIG.commit()

# cur_HAIG.execute("""
# CREATE EXTENSION postgis
# """)
# cur_HAIG.execute("""
# CREATE EXTENSION postgis_topology
# """)
# conn_HAIG.commit()


# get all ID terminal of Viasat data
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.obu''', conn)

# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())

# DATE = '2019-04-15'
# track_ID = '2507511'
# track_ID = '2509123'

# all_ID_TRACKS = ['2507511']
# track_ID = '2509151'
# track_ID = '2509642'
# track_ID = '2521275'
# track_ID = '2678929'

## to be used when the query stop. Start from the last saved index
for last_track_idx, track_ID in enumerate(all_ID_TRACKS):
    print(track_ID)
    track_ID = str(track_ID)
    # print('VIASAT GPS track:', track_ID)
    viasat_data = pd.read_sql_query('''
                SELECT * FROM public.dataraw 
                WHERE idterm = '%s' ''' % track_ID, conn)
    # remove duplicate GPS tracks (@ same position)
    viasat_data.drop_duplicates(['latitude', 'longitude'], inplace=True)
    # sort data by timedate:
    viasat_data['timedate'] = viasat_data['timedate'].astype('datetime64[ns]')

    # remove viasat data with 'progressive' == 0
    # viasat_data = viasat_data[viasat_data['progressive'] != 0]
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

########################################################################################
########################################################################################
########################################################################################

    # if len(viasat_data) > 5  and len(viasat_data) < 90:
    if (len(viasat_data) > 5) and sum(viasat_data.progressive)> 0 and \
            ((viasat_data.progressive == 0).sum()) < 0.2*(len(viasat_data)):  # <----
        fields = ["id", "longitude", "latitude", "progressive", 'panel', 'grade', "timedate", "speed"]
        # viasat = pd.read_csv(viasat_data, usecols=fields)
        viasat = viasat_data[fields]
        ## add a field for "anomalies"
        viasat['anomaly'] = '0123456'
        # transform "datetime" into seconds
        # separate date from time
        # transform object "datetime" into  datetime format
        viasat['timedate'] = viasat['timedate'].astype('datetime64[ns]')
        base_time = datetime(1970, 1, 1)
        viasat['totalseconds'] = pd.to_datetime(viasat['timedate'], format='% M:% S.% f')
        viasat['totalseconds'] = pd.to_datetime(viasat['totalseconds'], format='% M:% S.% f') - base_time
        viasat['totalseconds'] = viasat['totalseconds'].dt.total_seconds()
        viasat['totalseconds'] = viasat.totalseconds.astype('int')
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
        # make difference in totalaseconds from the start of the first trip of each TRACK_ID (need this to compute trips)
        viasat['path_time'] = viasat['totalseconds'] - viasat['totalseconds'][0]
        viasat = viasat[["id", "longitude", "latitude", "progressive", "path_time", "totalseconds",
                         "panel", "grade", "speed", "hour", "timedate", "anomaly"]]
        viasat['last_totalseconds'] = viasat.totalseconds.shift()   # <-------
        viasat['last_progressive'] = viasat.progressive.shift()  # <-------
        ## set nan values to -1
        viasat.last_totalseconds = viasat.last_totalseconds.fillna(-1)   # <-------
        viasat.last_progressive = viasat.last_progressive.fillna(-1)  # <-------
        ## get only VIASAT data where difference between two consecutive points is > 600 secx (10 minutes)
        ## this is to define the TRIP after a 'long' STOP time
        viasat1 = viasat
        ## compute difference in seconds between two consecutive tracks
        diff_time = viasat.path_time.diff()
        diff_time = diff_time.fillna(0)
        VIASAT_TRIPS_by_ID = pd.DataFrame([])
        row = []
        ## define a list with the starting indices of each new TRIP
        for i in range(len(diff_time)):
            if diff_time.iloc[i] >= 600:
                row.append(i)
        # get next element of the list row
        # row_next = [x+1 for x in row]
        # row_previous = [x-1 for x in row]
        if len(row)>0:
            row.append("end")
            # split Viasat data by TRIP (for a given ID..."idterm")
            for idx, j in enumerate(row):
                print(j)
                print(idx)
                # assign an unique TRIP ID
                TRIP_ID = str(track_ID) + "_" + str(idx)
                print(TRIP_ID)

                if j == row[0]:  # first TRIP
                    lista = [i for i in range(0,j)]
                    print(lista)
                    ## get  subset of VIasat data for each list:
                    viasat = viasat1.iloc[lista, :]
                    viasat['TRIP_ID'] = TRIP_ID
                    viasat['idtrajectory'] = viasat.id.iloc[0]
                    print(viasat)
                    VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID.append(viasat)

                if (idx > 0 and j != 'end'):   # intermediate TRIPS
                    lista = [i for i in range(row[idx - 1], row[idx])]
                    print(lista)
                    ## get  subset of VIasat data for each list:
                    viasat = viasat1.iloc[lista, :]
                    viasat['TRIP_ID'] = TRIP_ID
                    viasat['idtrajectory'] = viasat.id.iloc[0]
                    print(viasat)
                    VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID.append(viasat)

                if j == "end":  # last trip for that ID
                    # lista = [i for i in range(row[idx-1], len(viasat))]
                    lista = [i for i in range(row[idx-1], len(viasat1))]
                    print(lista)
                    ## get  subset of VIasat data for each list:
                    viasat = viasat1.iloc[lista, :]
                    viasat['TRIP_ID'] = TRIP_ID
                    viasat['idtrajectory'] = viasat.id.iloc[0]
                    print(viasat)
                    ## append all TRIPS by ID
                    VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID.append(viasat)


                    ##############################################
                    ### get CONCCATENATED TRIPS ##################
                    ##############################################

                    ## get indices where diff_progressive < 0
                    diff_progressive = VIASAT_TRIPS_by_ID.progressive.diff()
                    diff_progressive = diff_progressive.fillna(0)
                    row_progr = []
                    ## define a list with the starting indices of each new TRIP
                    for i in range(len(diff_progressive)):
                        if diff_progressive.iloc[i] < 0:
                            row_progr.append(i)
                    # split Viasat data by TRIP (for a given ID..."idterm")
                    for idx, j in enumerate(row_progr):
                        print(idx)
                        print(j)
                        # assign an unique TRIP ID
                        TRIP_ID_conc = str(track_ID) + "_conc_" + str(idx)
                        idtrajectory = str(VIASAT_TRIPS_by_ID['id'].iloc[j])

                        if idx < len(row_progr)-1:  # intermediate TRIPS
                            lista_conc = [i for i in range(row_progr[idx], row_progr[idx+1])]
                            print(lista_conc)
                            ## get  subset of VIasat data for each list:
                            VIASAT_TRIPS_by_ID["TRIP_ID"].iloc[lista_conc] = TRIP_ID_conc
                            VIASAT_TRIPS_by_ID["idtrajectory"].iloc[lista_conc] = idtrajectory

                #############################################
                ### add anomaly codes from Carlo Liberto ####
                #############################################

                    # final_TRIPS = pd.DataFrame([])
                    # get unique trips
                    all_TRIPS = list(VIASAT_TRIPS_by_ID.TRIP_ID.unique())
                    VIASAT_TRIPS_by_ID['last_panel'] = VIASAT_TRIPS_by_ID.panel.shift()
                    VIASAT_TRIPS_by_ID['last_lon'] = VIASAT_TRIPS_by_ID.longitude.shift()
                    VIASAT_TRIPS_by_ID['last_lat'] = VIASAT_TRIPS_by_ID.latitude.shift()
                    VIASAT_TRIPS_by_ID['last_totalseconds'] = VIASAT_TRIPS_by_ID.totalseconds.shift()
                    ## set nan values to -1
                    VIASAT_TRIPS_by_ID.last_panel= VIASAT_TRIPS_by_ID.last_panel.fillna(-1)
                    VIASAT_TRIPS_by_ID.last_lon = VIASAT_TRIPS_by_ID.last_lon.fillna(-1)
                    VIASAT_TRIPS_by_ID.last_lat = VIASAT_TRIPS_by_ID.last_lat.fillna(-1)
                    VIASAT_TRIPS_by_ID['last_panel'] = VIASAT_TRIPS_by_ID.last_panel.astype('int')
                    ## get TRIP sizes
                    # TRIP_sizes = VIASAT_TRIPS_by_ID.groupby(["TRIP_ID"]).size()
                    for idx_trip, trip in enumerate(all_TRIPS):
                        VIASAT_TRIP = VIASAT_TRIPS_by_ID[VIASAT_TRIPS_by_ID.TRIP_ID == trip]
                        VIASAT_TRIP.reset_index(drop=True, inplace=True)
                        print(VIASAT_TRIP)
                        timeDiff = VIASAT_TRIP.totalseconds.iloc[0] - VIASAT_TRIP.last_totalseconds.iloc[0]
                        for idx_row, row in VIASAT_TRIP.iterrows():
                            coords_1 = (row.latitude, row.longitude)
                            coords_2 = (row.last_lat, row.last_lon)
                            lDist = (geopy.distance.geodesic(coords_1, coords_2).km)*1000  # in meters
                            ####### PANEL ###################################################
                            if (row.panel == 1 and row.last_panel == 1):  # errore on-on
                                s = (list(row.anomaly))
                                s[0] = "E"
                                s = "".join(s)
                                VIASAT_TRIP.at[len(VIASAT_TRIP)-1, "anomaly"] = s
                                # set the intermediates anomaly to "I"
                                s = (list(row.anomaly))
                                s[0] = "I"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                s = (list(row.anomaly))
                                s[0] = "S"
                                s = "".join(s)
                                VIASAT_TRIP.at[0, "anomaly"] = s
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                            elif (row.panel == 0 and row.last_panel == -1):
                                s = (list(row.anomaly))
                                s[0] = "E"
                                s = "".join(s)
                                # VIASAT_TRIP.at[0, "anomaly"] = s
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)
                            elif (row.panel == 0 and row.last_panel == 0):  # off-off
                                s = (list(row.anomaly))
                                s[0] = "E"
                                s = "".join(s)
                                # VIASAT_TRIP.at[0, "anomaly"] = s
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                s = (list(row.anomaly))
                                s[0] = "E"
                                s = "".join(s)
                                VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                            elif (row.panel == 0 and row.last_panel == 1):  # off-off
                                s = (list(row.anomaly))
                                s[0] = "E"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                            elif (row.panel == 1 and row.last_panel == -1):
                                s = (list(row.anomaly))
                                s[0] = "I"
                                s = "".join(s)
                                # VIASAT_TRIP.at[0, "anomaly"] = s
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                print(VIASAT_TRIP)
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)
                            elif (row.panel == 1 and row.last_panel == 0):
                                s = (list(row.anomaly))
                                s[0] = "E"
                                s = "".join(s)
                                VIASAT_TRIP.at[len(VIASAT_TRIP)-1, "anomaly"] = s
                                s = (list(row.anomaly))
                                s[0] = "S"
                                s = "".join(s)
                                # VIASAT_TRIP.at[0, "anomaly"] = s
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                print(VIASAT_TRIP)
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                      ####### TRAVEL time > 10 min ###############################################

                            if (row.last_panel ==1 and row.panel ==1 and timeDiff > 10*60):
                                s = list(VIASAT_TRIP.iloc[0].anomaly)
                                s[0] = "S"
                                s[4] = "T"
                                s = "".join(s)
                                VIASAT_TRIP.at[0, "anomaly"] = s
                                s = list(VIASAT_TRIP.iloc[len(VIASAT_TRIP) - 1].anomaly)
                                s[0] = "E"
                                s[4] = "T"
                                s = "".join(s)
                                VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                            if (row.grade <= 15):
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[1] = "Q"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            elif (row.grade > 15):
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[1] = "q"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            if (lDist > 0 and VIASAT_TRIP["anomaly"].iloc[idx_row] != "S"):
                                if (row.progressive/lDist < 0.9):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[2] = "c"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                elif (row.progressive/lDist > 10 and row.progressive > 2200):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[3] = "C"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            if (timeDiff > 0 and 3.6 * 1000 * row.progressive / timeDiff > 250):
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[5] = "V"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            if (row.panel !=1 and row.progressive > 10000):
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[0] = "S"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[6] = "D"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[0] = "E"
                                s = "".join(s)
                                VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                            elif (row.panel != 0 and row.progressive <= 0):
                                s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                s[6] = "d"
                                s = "".join(s)
                                VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                        ### add all TRIPS together ##################
                        # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                        ### remove columns and add terminal ID
                        VIASAT_TRIP['track_ID'] = track_ID
                        VIASAT_TRIP['segment'] = VIASAT_TRIP.index
                        VIASAT_TRIP.drop(['last_panel', 'last_lon', 'last_lat',    # <------
                                          'last_totalseconds', 'last_progressive'], axis=1,
                                         inplace=True)

                        #### Connect to database using a context manager and populate the DB ####
                        connection = engine.connect()
                        VIASAT_TRIP.to_sql("routecheck_temp_concat", con=connection, schema="public",
                                           if_exists='append')
                        connection.close()



##################################################################
##################################################################
##################################################################
#### add geometry once the DB has been completry populated #######
##################################################################
##################################################################

'''
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

# copy temporary table to a permanent table with the right GEOMETRY datatype
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_CT')
with engine.connect() as conn, conn.begin():
    sql = """create table routecheck as (select * from routecheck_temp)"""
    conn.execute(sql)


# add geometry WGS84 4286 (Catania, Italy)
cur_HAIG.execute("""
alter table routecheck add column geom geometry(POINT,4326)
""")

cur_HAIG.execute("""
update routecheck set geom = st_setsrid(st_point(longitude,latitude),4326)
""")

conn_HAIG.commit()
conn_HAIG.close()
cur_HAIG.close()
'''