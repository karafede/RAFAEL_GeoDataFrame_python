
#### from Gaetano Valenti
#### modified by Federico Karagulian

import os
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
import kmeans_clusters
import dbscan_clusters
import db_connect


## Connect to an existing database
conn_HAIG = db_connect.connect_HAIG_CATANIA()
cur_HAIG = conn_HAIG.cursor()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_CATANIA')

cur_HAIG.execute("UPDATE residenze SET residence = null;")
#Query the database to obtain the list of idterm with residence
cur_HAIG.execute("SELECT idterm FROM nights_py order by idterm;")
records = cur_HAIG.fetchall()
idTerminale=[]
for row in records:
    idTerminale.append(str(row[0]))

# idTerm = '3217835'

for idTerm in idTerminale:
    ## Query the database and obtain data as Python objects
    ## calculate minimum distance (LIMIT 1) between the "residence point" and the "nighttime parking point"
    cur_HAIG.execute("SELECT DISTINCT ON (mindist) pippo.id, pippo.idterm, cast(pippo.mindist as integer) as mindist "
    "FROM (SELECT  residenze.id, residenze.idterm, residenze.n_points, residenze.avgparkingtime_s, residenze.geom, ST_Distance(ST_Transform(residenze.geom, 32632), ST_Transform(nights_py.geom,32632)) as mindist "
    "FROM residenze inner join nights_py on nights_py.idterm= residenze.idterm "
    "where residenze.idterm="+idTerm+")pippo Order By mindist ASC LIMIT 1")
    records = cur_HAIG.fetchall()
    #print(records.shape)
    id=str(records[0][0])
    idterm=str(records[0][1])
    dist=str(records[0][2])
    print(id, idterm, dist)
    cur_HAIG.execute("UPDATE residenze SET residence ="+dist+" WHERE id="+id+" and idterm="+idterm+";")
# Make the changes to the database persistent
conn_HAIG.commit()
# Close communication with the database
cur_HAIG.close()
conn_HAIG.close()
