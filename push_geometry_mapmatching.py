

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

#############################################################
#############################################################

# make a copy of routecheck into routecheck_temp

conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

## Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')

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


########################################################################################
########## DATABASE OPERATIONS after generation of map-matching trajectories############
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS mapmatching_temp CASCADE")
# cur_HAIG.execute("DROP TABLE IF EXISTS mapmatching CASCADE")
# conn_HAIG.commit()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## copy temporary table to a permanent table with the right GEOMETRY datatype
## Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')
with engine.connect() as conn, conn.begin():
    sql = """create table mapmatching as (select * from mapmatching_temp)"""
    conn.execute(sql)

## Convert the `'geom'` column back to Geometry datatype, from text
with engine.connect() as conn, conn.begin():
    print(conn)
    sql = """ALTER TABLE public.mapmatching
                                  ALTER COLUMN geom TYPE Geometry(LINESTRING, 4326)
                                    USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)
