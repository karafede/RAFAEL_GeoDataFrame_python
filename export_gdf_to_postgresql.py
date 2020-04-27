
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
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import sqlalchemy as sal

os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing\\new_geojsons')
gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15_Apr-17-2020_130_194_68_10.geojson")
# gdf_all_EDGES.plot()

conn = db_connect.connect_octo2015()
cur = conn.cursor()

# https://stackoverflow.com/questions/38361336/write-geodataframe-into-sql-database


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

# Convert `'geom'` column in GeoDataFrame `gdf` to hex
    # Note that following this step, the GeoDataFrame is just a regular DataFrame
    # because it does not have a geometry column anymore. Also note that
    # it is assumed the `'geom'` column is correctly datatyped.
gdf_all_EDGES['geom'] = gdf_all_EDGES['geometry'].apply(wkb_hexer)
gdf_all_EDGES.drop('geometry', 1, inplace=True)

# Create SQL connection engine
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/Octo2015')


# ## create extension postgis on the database Octo2015
# cur.execute("""
# CREATE EXTENSION postgis
# """)
#
# cur.execute("""
# CREATE EXTENSION postgis_topology
# """)
#
# conn.commit()
# conn.close()
# cur.close()

# Connect to database using a context manager
with engine.connect() as conn, conn.begin():
    # Note use of regular Pandas `to_sql()` method.
    gdf_all_EDGES.to_sql("map_matching_temp", con=conn, schema="public",
               if_exists='append', index=False)

# copy temporary table to a permanent table with the right GEOMETRY datatype

# with engine.connect() as conn, conn.begin():
#     sql = """create table map_matching as (select * from public.map_matching_temp)"""
#     conn.execute(sql)

# Convert the `'geom'` column back to Geometry datatype, from text
with engine.connect() as conn, conn.begin():
    sql = """ALTER TABLE public.map_matching
               ALTER COLUMN geom TYPE Geometry(LINESTRING, 4326)
                 USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)
