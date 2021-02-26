
import os
os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data')
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
import momepy
import csv
from shapely import wkb


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")


########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)


## load EDGES from OSM
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, geom
                            FROM "OSM_edges" ''',conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace= True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)
## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)
# gdf_edges.plot()

####################################################
####################################################

all_Viasat = pd.read_sql_query('''
                        SELECT
                        routecheck_2019.idterm
                        FROM routecheck_2019
                                       ''', conn_HAIG)
N = len(all_Viasat)
### get frequencies of the GPS accuracies
grade_upto_15 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade < 15                                      
                                      ''', conn_HAIG)
G_upto_15 = (len(grade_upto_15) / N ) *100


grade_8 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 8                                      
                                      ''', conn_HAIG)
G_8 = (len(grade_8) / N ) *100


grade_9 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 9                                      
                                      ''', conn_HAIG)
G_9 = (len(grade_9) / N ) *100


grade_10 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 10                                      
                                      ''', conn_HAIG)
G_10 = (len(grade_10) / N ) *100

grade_11 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 11                                      
                                      ''', conn_HAIG)
G_11 = (len(grade_11) / N ) *100


grade_12 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 12                                      
                                      ''', conn_HAIG)
G_12 = (len(grade_12) / N ) *100


grade_13 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 13                                      
                                      ''', conn_HAIG)
G_13 = (len(grade_13) / N ) *100


grade_14 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 14                                      
                                      ''', conn_HAIG)
G_14 = (len(grade_14) / N ) *100


grade_15 = pd.read_sql_query('''
                       SELECT  
                          routecheck_2019.grade
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.grade = 15                                      
                                      ''', conn_HAIG)
G_15 = (len(grade_15) / N ) *100



## Distanza euclidea maggiore della distanza percorsa
## select how many records we have with "anomaly" contains "c"
viasat_anomaly_c = pd.read_sql_query('''
                       SELECT
                          idterm, anomaly
                          FROM routecheck_2019
                          WHERE  anomaly LIKE '%c%' ''', conn_HAIG)
### hom many ......
anomaly_c = (len(viasat_anomaly_c) / N ) *100


## Distanza percorsa maggiore di 10 volte quella euclidea
## select how many records we have with "anomaly" contains "C"
viasat_anomaly_C = pd.read_sql_query('''
                       SELECT
                       idterm, anomaly
                       FROM routecheck_2019
                       WHERE  anomaly LIKE '%C%' ''', conn_HAIG)
### hom many ......
anomaly_C = (len(viasat_anomaly_C) / N ) *100

## Intervallo di tempo fra due tracce consecutive superiore a 10 minuti
## select how many records we have with "anomaly" START with "E"
viasat_anomaly_E = pd.read_sql_query('''
                        SELECT
                        idterm, anomaly
                        FROM routecheck_2019
                        WHERE  anomaly ILIKE 'E%' ''', conn_HAIG)
### hom many ......
anomaly_E = (len(viasat_anomaly_E) / N ) *100



## Intervallo di tempo fra due tracce consecutive superiore a 10 minuti
## select how many records we have with "anomaly" tha contains "T"
viasat_anomaly_T = pd.read_sql_query('''
                        SELECT
                        idterm, anomaly
                        FROM routecheck_2019
                       WHERE  anomaly LIKE '%T%' ''', conn_HAIG)
### hom many ......
anomaly_T = (len(viasat_anomaly_T) / N ) *100


## Velocità istantanea superiore a 250 Km/h
## select how many records we have with "anomaly" that contains "V"
viasat_anomaly_V = pd.read_sql_query('''
                       SELECT
                       idterm, anomaly
                       FROM routecheck_2019
                      WHERE  anomaly LIKE '%V%' ''', conn_HAIG)
### hom many ......
anomaly_V = (len(viasat_anomaly_V) / N ) *100


## percentuale di viaggi concatenati (con dati GPS diversi)
## select how many records we have with "anomaly" that contains "d"
viasat_anomaly_d = pd.read_sql_query('''
                       SELECT
                          idterm, anomaly
                         FROM routecheck_2019
                         WHERE  anomaly LIKE '%d%' ''', conn_HAIG)
### hom many ......
anomaly_d = (len(viasat_anomaly_d) / N ) *100


#### incremento tra due progressive > 10km
viasat_anomaly_D = pd.read_sql_query('''
                       SELECT
                          idterm, anomaly
                         FROM routecheck_2019
                         WHERE  anomaly LIKE '%D%' ''', conn_HAIG)
### hom many ......
anomaly_D = (len(viasat_anomaly_D) / N ) *100


viasat_anomaly_VCD = pd.read_sql_query('''
                       SELECT
                          idterm, anomaly
                         FROM routecheck_2019
                         WHERE  anomaly ILIKE 'VCD'
                          limit 1000''', conn_HAIG)

viasat_anomaly_VCD = pd.read_sql_query('''
                       SELECT
                          idterm, anomaly
                         FROM routecheck_2019
                         WHERE  anomaly ILIKE '%V%' and 
                         anomaly ILIKE '%C%' and
                         anomaly ILIKE '%D%' and 
                         anomaly ILIKE '%c%'                       
                         ''', conn_HAIG)

### hom many ......
viasat_anomaly_VCD = (len(viasat_anomaly_VCD) / N ) *100



## select al TRIPS
all_TRIP_IDs = pd.read_sql_query('''
                       SELECT
                          idterm, "TRIP_ID"
                          FROM routecheck_2019
                          ''', conn_HAIG)
all_TRIP_IDs = len(list(all_TRIP_IDs.TRIP_ID.unique()))



grade = pd.read_sql_query('''
                        SELECT grade, COUNT(*)
                        from routecheck_2019 
                        WHERE routecheck_2019.grade < 15   
                        group by grade
                         ''', conn_HAIG)

####################################################################
####################################################################
####################################################################
####################################################################


from datetime import datetime
now1 = datetime.now()


#### get max instant speed from veicoli pesanti
max_speed = pd.read_sql_query('''
                       SELECT  
                          max(routecheck_2019.speed)
                          FROM routecheck_2019                    
                          WHERE routecheck_2019.speed < 160                
                          AND routecheck_2019.vehtype::bigint = 2                          
                                      ''', conn_HAIG)


now2 = datetime.now()
print(now2 - now1)

