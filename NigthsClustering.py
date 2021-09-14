
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

timedate = []
timeins = []
longitude =[]
latitude =[]
panel =[]
deltaT=[]
idTerminale=[]
sep=","
#Query to create db_residencePY
cur_HAIG.execute("DROP TABLE IF EXISTS public.nights_py CASCADE")
cur_HAIG.execute("CREATE  TABLE public.nights_py "
"(id bigserial primary key, "
" idTerm integer  NOT NULL, "
" n_points smallint NOT NULL,"
" n_nights integer  NOT NULL,"
" geom geometry(Point,4326))");

#Query the database to obtain the list of idterm
cur_HAIG.execute("SELECT idterm FROM idterm_portata order by idterm;")
records = cur_HAIG.fetchall()
for row in records:
    idTerminale.append(str(row[0]))

for idTerm in idTerminale:
    # Query the database and obtain data as Python objects
    '''
    query=("SELECT ST_X(ST_Transform(dataraw.geom, 32632)), ST_Y(ST_Transform(dataraw.geom, 32632)), " 
          "CASE WHEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL)=0) " 
          "AND (dataraw.timedate+('2 hour')::INTERVAL)<(date_trunc('day', dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL) + '03:00:00')) "
          "THEN 1 "
          "WHEN (DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL)>0) "
          "AND ((date_trunc('day', dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL) + '03:00:00')>((dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL))) "
          "THEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))-1) "
          "ELSE DATE(dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL) "
          "END as nights "
          "FROM route "
          "inner join dataraw on dataraw.id=route.idtrace_d "
          "where route.idterm="+idTerm+" and dataraw.panel=2 and breaktime_s>4*3600 and "
          "((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL)=0 "
          "AND (dataraw.timedate+('2 hour')::INTERVAL)<(date_trunc('day', dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL) + '03:00:00')) "
          "OR (DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL)>0)) "
          "order by dataraw.timedate")
    '''
    ### transform coordinates degress (4326) into cartographical coodinates (32632) (meters) to make calculations...
    query=("SELECT  ST_X(ST_Transform(dataraw.geom, 32632)), ST_Y(ST_Transform(dataraw.geom, 32632)), "
          "CASE "
          "WHEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))=0 "
          "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))<3 "
          "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))>=3 )) "
          "THEN 1 "
          "WHEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))>0 " 
          "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))>=3 "
          "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))<3 )) "
          "THEN (DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))-1 "
          "WHEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))>0 "
          "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))>=3 "
          "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))>=3 )) "
          "THEN (DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))  "
          "WHEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))>0 "
          "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))<3 "
          "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))<3 )) "
          "THEN (DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL)) "
          "WHEN ((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))>0 "
          "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))<3 "
          "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))>=3 )) "
          "THEN (DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))+1  "
          "END as nights "
          "FROM route "
          "inner join dataraw on dataraw.id=route.idtrace_d "
          "where route.idterm="+idTerm+" and breaktime_s>4*3600 and "
          "( "
          "((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))=0 "
		  "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))<3 "
		  "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))>=3 )) "
          "OR "
          "((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))>1) "
	      "OR "
	      "((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))=1 "
	      "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))>=3 "
	      "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))>=3 )) "
          "OR "
	      "((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))=1 "
	      "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))<3 "
	      "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))>=3 )) "
          "OR "
	     "((DATE (dataraw.timedate + (route.breaktime_s * ('1 second')::INTERVAL)+('2 hour')::INTERVAL) - DATE(dataraw.timedate+('2 hour')::INTERVAL))=1 "
	      "AND (EXTRACT(hour from(dataraw.timedate+('2 hour')::INTERVAL))<3 "
	      "AND  EXTRACT(hour from (dataraw.timedate + route.breaktime_s * ('1 second')::INTERVAL+('2 hour')::INTERVAL))<3 )) "
          ") "           
          "order by dataraw.timedate")
    cur_HAIG.execute(query)
    records = cur_HAIG.fetchall()
    if(len(records)<3): continue
    lon=[]
    lat=[]
    nights=[]
    for row in records:
        lon.append(row[0])
        lat.append(row[1])
        nights.append(float(row[2]))

    labels=dbscan_clusters.dbscan(lon,lat,120)
    labels_bis = list( dict.fromkeys(labels) ) # labels non duplicati
    input_best=""
    nn_max=0
    for lab in labels_bis:
        indices=[index for index, value in enumerate(labels) if value == lab]
        x = [lon[i] for i in indices]
        y = [lat[i] for i in indices]
        n_n= [nights[i] for i in indices]
        frequenza=len(x)
        xm= sum(x)/float(frequenza)
        ym= sum(y)/float(frequenza)
        n_nights=int(sum(n_n))
        ### transform back cartographical coordinates (32632) (meters) into degree coodinates (4326) (for projections into a GIS
        input = "(" + str(idTerm) + sep + str(frequenza) + sep + str(n_nights)+ sep + "ST_Transform(st_setsrid(st_makepoint(" + str(xm) + "," + str(ym) + "), 32632), 4326))";
        if (nn_max<n_nights):
            input_best=input
            nn_max=n_nights
            print(idTerm, xm, ym, frequenza, n_nights)
    if  input_best:
        cur_HAIG.execute("INSERT INTO public.nights_py (idTerm, n_points, n_nights, geom)" + " VALUES " + input_best + "");

cur_HAIG.execute("CREATE INDEX nights_idterm ON public.nights_py USING btree (idTerm);");

# Make the changes to the database persistent
conn_HAIG.commit()
# Close communication with the database
cur_HAIG.close()
conn_HAIG.close()
#exec(open("NightsClustering1.py").read());