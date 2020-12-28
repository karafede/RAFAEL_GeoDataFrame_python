

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
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK

# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

os.chdir('D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data')
os.getcwd()


### function to get consecutive edges...
def consec_sort(lst):
    def key(x):
        nonlocal index
        if index <= lower_index:
            index += 1
            return -1
        return abs(x[0] - lst[index - 1][1])
    for lower_index in range(len(lst) - 2):
        index = 0
        lst = sorted(lst, key=key)
    return lst


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
## I need this because I generate maps
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_CT')

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


# make a list of unique dates (only dates not times!)
# select an unique table of dates postgresql
unique_DATES = pd.read_sql_query(
    '''SELECT DISTINCT all_dates.dates
        FROM ( SELECT dates.d AS dates
               FROM generate_series(
               (SELECT MIN(timedate) FROM public.mapmatching_2019),
               (SELECT MAX(timedate) FROM public.mapmatching_2019),
              '1 day'::interval) AS dates(d)
        ) AS all_dates
        INNER JOIN public.mapmatching_2019
	    ON all_dates.dates BETWEEN public.mapmatching_2019.timedate AND public.mapmatching_2019.timedate
        ORDER BY all_dates.dates ''', conn_HAIG)

# ADD a new field with only date (no time)
unique_DATES['just_date'] = unique_DATES['dates'].dt.date
MONTHS = unique_DATES['dates'].dt.month
MONTHS.drop_duplicates(keep='first', inplace=True)
MONTHS = list(MONTHS)


# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)

from datetime import datetime
now1 = datetime.now()

#### get all VIASAT data from map-matching (automobili e mezzi pesanti) on selected date
viasat_data = pd.read_sql_query('''
                       SELECT  
                          mapmatching_2019.u, mapmatching_2019.v,
                               mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                               mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                               mapmatching_2019.idtrajectory,
                               dataraw.speed, dataraw.vehtype
                          FROM mapmatching_2019
                          LEFT JOIN dataraw 
                                      ON mapmatching_2019.idtrace = dataraw.id  
                                      /*WHERE date(mapmatching_2019.timedate) = '2019-02-25'*/
                                      /*WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '02'*/
                                      /*AND dataraw.vehtype::bigint = 2*/
                                      ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)



## get counts ("passaggi") across each EDGE
all_data = viasat_data[['u','v', 'timedate']]
## resample 'timedate' to 15 minutes interval
# df_flux = all_data.resample(rule='15Min', on='timedate').size()
# df_flux = all_data.groupby(['u','v'],sort=False).resample(rule='15Min', on='timedate').size().reset_index().rename(columns={0:'counts'})

### get "speed"
speed_uv = viasat_data[['u', 'v', 'timedate', 'speed']]
speed_uv['hour'] = speed_uv['timedate'].apply(lambda x: x.hour)

### get AVERAGE of travelled travelled "speed" for each edge and each hour
speed_uv = (speed_uv.groupby(['u', 'v', 'hour']).mean()).reset_index()
speed_uv['speed'] = speed_uv.speed.astype('int')

del viasat_data

## get a new field for the HOUR
all_data['hour'] = all_data['timedate'].apply(lambda x: x.hour)
# date (day)
all_data['date'] = all_data['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
all_data = all_data[['u','v', 'hour', 'date']]


## get the counts for each edge (u,v) pair and for each hour (this is a FLUX)
all_counts_uv = all_data.groupby(['u','v', 'hour', 'date'], sort=False).size().reset_index().rename(columns={0:'FLUX'})
# get the mean by hour
all_counts_uv = all_counts_uv.groupby(['u', 'v', 'hour'], sort=False).mean().reset_index()  ## FLUX

## get hour with maximum FLUX for each edge (u.v) pair (PEAK HOUR, ORA di PUNTA)
### https://stackoverflow.com/questions/15705630/get-the-rows-which-have-the-max-count-in-groups-using-groupby
max_FLUX_uv = all_counts_uv.sort_values('FLUX', ascending=False).drop_duplicates(['u','v'])
max_FLUX_uv = max_FLUX_uv.rename(columns={'FLUX': 'max_FLUX'})


## to find the FLUX at "rete carica" consider the lower flux
min_FLUX_uv = all_counts_uv.sort_values('FLUX', ascending=True).drop_duplicates(['u','v'])
min_FLUX_uv = min_FLUX_uv.rename(columns={'FLUX': 'min_FLUX'})
## calculate the VOLATILIY of FLUX
min_max_FLUX_uv = pd.merge(max_FLUX_uv[['u','v', 'max_FLUX']], min_FLUX_uv[['u','v', 'min_FLUX']], on=['u', 'v'], how='left')
min_max_FLUX_uv['volatility'] = (min_max_FLUX_uv.max_FLUX - min_max_FLUX_uv.min_FLUX)/2


## get speed at the PEAK HOUR (merge "speed_uv" with "max_FLUX_uv")
flux_PEAK_HOUR = pd.merge(max_FLUX_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
flux_PEAK_HOUR = flux_PEAK_HOUR.rename(columns={'speed': 'speed_PHF'})
## get DENSITY at PEAK HOUR
## density  ("flux/speed")  number of vehicles/km
flux_PEAK_HOUR = flux_PEAK_HOUR[flux_PEAK_HOUR.speed_PHF > 0]
flux_PEAK_HOUR['PHF_density'] = flux_PEAK_HOUR.max_FLUX / flux_PEAK_HOUR.speed_PHF


## get speed at RETE SCARICA (merge "speed_uv" with "min_FLUX_uv")
flux_RETE_SCARICA = pd.merge(min_FLUX_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
flux_RETE_SCARICA = flux_RETE_SCARICA.rename(columns={'speed': 'speed_RETE_SCARICA'})


## merge FLUX and speed by 'u','v'' and 'hour' (get a AVERAGE of the FLUX)
all_counts_uv = pd.merge(all_counts_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
all_counts_uv = all_counts_uv[all_counts_uv.speed > 0]
## conpute DENSITY  ("flux/speed")  number of vehicles/km (it is an AVERAGE value)
all_counts_uv['density'] = all_counts_uv.FLUX / all_counts_uv.speed

## get a sottorete by filtering the FLUX (only consider higher fluxes).......
## get only fluxes >= 10 vehi/hour
all_counts_uv = all_counts_uv[all_counts_uv.FLUX >= 10]


#### --------------------------------------------------------------------- ######
### for each u,v pair make hourly averages and get the CAPACITY DROP (CD)  ######
## make unique list of "u"

all_u = list(all_counts_uv.u.unique())
U = pd.Series(list(all_counts_uv.u))

# node = 265617693

rows_CD = pd.DataFrame([])
for idx, node in enumerate(all_u):
    print(idx, node)
    if (node in U.tolist()):
        slected_counts_uv = all_counts_uv[all_counts_uv.u == node]
        # slected_counts_uv = slected_counts_uv.groupby(['hour']).mean().reset_index()
        if (len(slected_counts_uv) > 1):
            # slected_counts_uv.plot.scatter(x='density', y='FLUX')
            ## get the density at maximum FLUX and the one just after....for the CAPACITY DROP
            counts_CD = slected_counts_uv.sort_values('FLUX', ascending=False)[0:2]
            # if (counts_CD.density.iloc[0] < counts_CD.density.iloc[1]):
            print("=== CAPACITY DROP OK OK OK =======")
            slected_counts_uv['cap_drop'] = ((counts_CD.FLUX.iloc[0] - counts_CD.FLUX.iloc[1]))
            rows_CD = rows_CD.append(slected_counts_uv)

####################################################################################################################



## merge edges for congestion with the road network to get the geometry
AAA = pd.merge(rows_CD, gdf_edges, on=['u', 'v'], how='left')
AAA.drop_duplicates(['u', 'v'], inplace=True)
## make a geodataframe
AAA = gpd.GeoDataFrame(AAA)
AAA.plot()


## build a dataframe with all variables to use for the calculation of the RESILIENCE
RS_uv = pd.merge(rows_CD, min_max_FLUX_uv, on=['u', 'v'], how='left')
RS_uv.dropna(inplace= True)
RS_uv = pd.merge(RS_uv, flux_PEAK_HOUR[['u', 'v', 'speed_PHF', 'PHF_density']], on=['u', 'v'], how='left')
RS_uv = pd.merge(RS_uv, flux_RETE_SCARICA[['u', 'v']], on=['u', 'v'], how='left')
RS_uv.dropna(inplace= True)

### make the "FUNDAMENTAL MODEL of TRAFFIC FLOW"  (flux vs densty)
# RS_uv.plot.scatter(x = 'density', y = 'FLUX')

### group by edge...and make an average of all other variables (over all the HOURS)
RS_uv = RS_uv[['u','v','FLUX','volatility','speed','max_FLUX', 'density',
               'speed_PHF','cap_drop', 'PHF_density']].groupby(['u', 'v']).mean().reset_index()

##### --------------- ####
## compute RESISTANCE ####
RS_uv['RESISTANCE'] = ((RS_uv.FLUX + RS_uv.volatility)/RS_uv.speed) / ((RS_uv.max_FLUX + RS_uv.volatility)/RS_uv.speed_PHF)
# RS_uv = RS_uv[RS_uv.max_FLUX != RS_uv.cap_drop]
RS_uv = RS_uv[RS_uv.max_FLUX > RS_uv.cap_drop]

### make consecutive edges...
lista_uv = list(RS_uv[['u', 'v']].itertuples(index=False, name=None))
output = consec_sort(lista_uv)
consecutive_edges = pd.DataFrame(output)
consecutive_edges.columns = ['u', 'v']
RS_uv = pd.merge(consecutive_edges, RS_uv,  on=['u', 'v'], how='left')
RS_uv = RS_uv.drop_duplicates(['u', 'v'])
## compute "FLUX_in in"
RS_uv['FLUX_in'] = RS_uv.FLUX.shift()
## make column of "FLUX_out"
RS_uv['FLUX_out'] = RS_uv['FLUX']
## remove rows with NA values
RS_uv.dropna(subset = ['FLUX_out'], inplace= True)
RS_uv.dropna(subset = ['FLUX_in'], inplace= True)

##### --------------- ####
## compute RECOVERY ######
RS_uv['RECOVERY'] = ((RS_uv.FLUX + (RS_uv.FLUX_in - RS_uv.FLUX_out)) / RS_uv.speed) / ((RS_uv.max_FLUX - RS_uv.cap_drop)/ RS_uv.speed_PHF )


## merge edges for congestion with the road network to get the geometry
AAA = pd.merge(RS_uv, gdf_edges, on=['u', 'v'], how='left')
AAA.drop_duplicates(['u', 'v'], inplace=True)
## make a geodataframe
AAA = gpd.GeoDataFrame(AAA)
AAA.plot()

# i = 0

## compute RESILIENCE INDEX
LPIR = pd.DataFrame([])
for i in range(len(RS_uv)):
    print(i)
    if (RS_uv.density.iloc[i] <= RS_uv.PHF_density.iloc[i]):
        delta = 1
    elif (RS_uv.density.iloc[i] > RS_uv.PHF_density.iloc[i]):
        delta = 0
    row = RS_uv.iloc[[i]]
    print('=== delta ===:', delta)
    row['resilience'] = row.RESISTANCE * delta + row.RECOVERY * (1 - delta)
    LPIR = LPIR.append(row)


LPIR = LPIR.sort_values('resilience', ascending=False)
LPIR_RESILIENT = LPIR[LPIR.resilience <= 1]
### make the inverse...for the colormap...
LPIR_RESILIENT.resilience = 1- LPIR_RESILIENT.resilience
LPIR_not_RESILIENT = LPIR[LPIR.resilience > 1]


## merge edges for congestion with the road network to get the geometry
LPIR_RESILIENT = pd.merge(LPIR_RESILIENT, gdf_edges, on=['u', 'v'], how='left')
LPIR_RESILIENT.drop_duplicates(['u', 'v'], inplace=True)
# LPIR_RESILIENT = LPIR_RESILIENT.sort_values('length', ascending=False)
LPIR_RESILIENT = gpd.GeoDataFrame(LPIR_RESILIENT)
LPIR_RESILIENT.plot()


# ## make unique list of "u" and "v"
# all_uv = list(LPIR_RESILIENT.u.unique())  + list(LPIR_RESILIENT.v.unique())
# all_uv = list(dict.fromkeys(all_uv))
#
# ## delete edges with 'length' larger than 15 meters
# ## check if each node is in the u or v column
# ## we only delete isolate edges and not interconnected edges
# for idx, node in enumerate(all_uv):
#     # print(idx, node)
#     U = pd.Series(list(LPIR_RESILIENT.u))
#     V = pd.Series(list(LPIR_RESILIENT.v))
#     if not (node in U.tolist()) and (node in V.tolist()):
#         print("\nThis node exists only in one edge==============================")
#         ### find row to delete (with 'length'> 28 meters
#         if (LPIR_RESILIENT[LPIR_RESILIENT.values == node]['length'].iloc[0] <= 30):
#             print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
#             row_to_delete = LPIR_RESILIENT[LPIR_RESILIENT.values == node].index
#             LPIR_RESILIENT = LPIR_RESILIENT.drop(row_to_delete)


##### PLOT ######

## rescale all data by an arbitrary number
LPIR_RESILIENT["scales"] = round(((LPIR_RESILIENT.resilience/max(LPIR_RESILIENT.resilience)) * 3) + 0.1 ,1)
## rescale all data by an arbitrary number
LPIR_RESILIENT["resilience"] = round(LPIR_RESILIENT.resilience,1)


# add colors based on 'congestion_index'
vmin = min(LPIR_RESILIENT.scales)
vmax = max(LPIR_RESILIENT.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlGnBu)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
LPIR_RESILIENT['color'] = LPIR_RESILIENT['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


# add colors to map
my_map = plot_graph_folium_FK(LPIR_RESILIENT, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=3.5, edge_opacity=0.5)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    LPIR_RESILIENT[['u','v', 'scales', 'resilience', 'length', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':0.6
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'resilience', 'length']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
my_map.save(path + "RESILIENCE_CATANIA.Feb_May_Aug_Nov_2019.html")


# Total LPIR on a road
# section is thus the average overall considered time interval in the specified
# time period. The interpretation of LPIR is as follows. If LPIR < 1 then the
# link can resist a significant drop in level of service and thus remains
# uncongested. It is therefore resilient and robust. However, a link that does
# suffer a drop in level of service but can recover quickly should also be
# considered resilient, although it may not be robust. It is not necessarily the
# case that LPIR > 1 means that the link is always non-resilient.

## for LPIR < 1.
## i.e LPIR = 0.3 compared to LPIR = 0.2, means that the link with LPIR = 0.3
# can recover LESS QUICKLY than the link with LPIT = 0.2 (which RECOVER FASTER).



########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
###### loop to generate MONTHLY maps #####################################################

for idx, row in enumerate(MONTHS):
    MESE = str(row)
    print(MESE)
    #################################################################################
    ### loop over the months  ########################################################
    viasat_data = pd.read_sql_query('''
                           SELECT  
                              mapmatching_2019.u, mapmatching_2019.v,
                                   mapmatching_2019.timedate, 
                                   mapmatching_2019.idtrace,
                                   dataraw.speed
                              FROM mapmatching_2019
                              LEFT JOIN dataraw 
                                          ON mapmatching_2019.idtrace = dataraw.id                
                                          WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = %s
                                          /*limit 10000*/
                                          ''', conn_HAIG, params={MESE})


    ## get counts ("passaggi") across each EDGE
    all_data = viasat_data[['u', 'v', 'timedate']]
    ### get "speed"
    speed_uv = viasat_data[['u', 'v', 'timedate', 'speed']]
    speed_uv['hour'] = speed_uv['timedate'].apply(lambda x: x.hour)

    ### get AVERAGE of travelled travelled "speed" for each edge and each hour
    speed_uv = (speed_uv.groupby(['u', 'v', 'hour']).mean()).reset_index()
    speed_uv['speed'] = speed_uv.speed.astype('int')

    del viasat_data

    ## get a new field for the HOUR
    all_data['hour'] = all_data['timedate'].apply(lambda x: x.hour)
    # date (day)
    all_data['date'] = all_data['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
    all_data = all_data[['u', 'v', 'hour', 'date']]

    ## get the counts for each edge (u,v) pair and for each hour (this is a FLUX)
    all_counts_uv = all_data.groupby(['u', 'v', 'hour', 'date'], sort=False).size().reset_index().rename(
        columns={0: 'FLUX'})
    # get the mean by hour
    all_counts_uv = all_counts_uv.groupby(['u', 'v', 'hour'], sort=False).mean().reset_index()  ## FLUX

    ## get hour with maximum FLUX for each edge (u.v) pair (PEAK HOUR, ORA di PUNTA)
    ### https://stackoverflow.com/questions/15705630/get-the-rows-which-have-the-max-count-in-groups-using-groupby
    max_FLUX_uv = all_counts_uv.sort_values('FLUX', ascending=False).drop_duplicates(['u', 'v'])
    max_FLUX_uv = max_FLUX_uv.rename(columns={'FLUX': 'max_FLUX'})

    ## to find the FLUX at "rete carica" consider the lower flux
    min_FLUX_uv = all_counts_uv.sort_values('FLUX', ascending=True).drop_duplicates(['u', 'v'])
    min_FLUX_uv = min_FLUX_uv.rename(columns={'FLUX': 'min_FLUX'})
    ## calculate the VOLATILIY of FLUX
    min_max_FLUX_uv = pd.merge(max_FLUX_uv[['u', 'v', 'max_FLUX']], min_FLUX_uv[['u', 'v', 'min_FLUX']], on=['u', 'v'],
                               how='left')
    min_max_FLUX_uv['volatility'] = (min_max_FLUX_uv.max_FLUX - min_max_FLUX_uv.min_FLUX) / 2

    ## get speed at the PEAK HOUR (merge "speed_uv" with "max_FLUX_uv")
    flux_PEAK_HOUR = pd.merge(max_FLUX_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
    flux_PEAK_HOUR = flux_PEAK_HOUR.rename(columns={'speed': 'speed_PHF'})
    ## get DENSITY at PEAK HOUR
    ## density  ("flux/speed")  number of vehicles/km
    flux_PEAK_HOUR = flux_PEAK_HOUR[flux_PEAK_HOUR.speed_PHF > 0]
    flux_PEAK_HOUR['PHF_density'] = flux_PEAK_HOUR.max_FLUX / flux_PEAK_HOUR.speed_PHF

    ## get speed at RETE SCARICA (merge "speed_uv" with "min_FLUX_uv")
    flux_RETE_SCARICA = pd.merge(min_FLUX_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
    flux_RETE_SCARICA = flux_RETE_SCARICA.rename(columns={'speed': 'speed_RETE_SCARICA'})

    ## merge FLUX and speed by 'u','v'' and 'hour' (get a AVERAGE of the FLUX)
    all_counts_uv = pd.merge(all_counts_uv, speed_uv, on=['u', 'v', 'hour'], how='left')
    all_counts_uv = all_counts_uv[all_counts_uv.speed > 0]
    ## conpute DENSITY  ("flux/speed")  number of vehicles/km (it is an AVERAGE value)
    all_counts_uv['density'] = all_counts_uv.FLUX / all_counts_uv.speed

    del speed_uv

    ## get a sottorete by filtering the FLUX (only consider higher fluxes).......
    ## get only fluxes >= 10 vehi/hour
    all_counts_uv = all_counts_uv[all_counts_uv.FLUX >= 10]

    #### --------------------------------------------------------------------- ######
    ### for each u,v pair make hourly averages and get the CAPACITY DROP (CD)  ######
    ## make unique list of "u"

    all_u = list(all_counts_uv.u.unique())
    U = pd.Series(list(all_counts_uv.u))

    rows_CD = pd.DataFrame([])
    for idx, node in enumerate(all_u):
        print(idx, node)
        if (node in U.tolist()):
            slected_counts_uv = all_counts_uv[all_counts_uv.u == node]
            # slected_counts_uv = slected_counts_uv.groupby(['hour']).mean().reset_index()
            if (len(slected_counts_uv) > 1):
                # slected_counts_uv.plot.scatter(x='density', y='FLUX')
                ## get the density at maximum FLUX and the one just after....for the CAPACITY DROP
                counts_CD = slected_counts_uv.sort_values('FLUX', ascending=False)[0:2]
                # if (counts_CD.density.iloc[0] < counts_CD.density.iloc[1]):
                print("=== CAPACITY DROP OK OK OK =======")
                slected_counts_uv['cap_drop'] = ((counts_CD.FLUX.iloc[0] - counts_CD.FLUX.iloc[1]))
                rows_CD = rows_CD.append(slected_counts_uv)

    ####################################################################################################################

    del all_counts_uv

    ## build a dataframe with all variables to use for the calculation of the RESILIENCE
    RS_uv = pd.merge(rows_CD, min_max_FLUX_uv, on=['u', 'v'], how='left')
    RS_uv.dropna(inplace=True)
    RS_uv = pd.merge(RS_uv, flux_PEAK_HOUR[['u', 'v', 'speed_PHF', 'PHF_density']], on=['u', 'v'], how='left')
    RS_uv = pd.merge(RS_uv, flux_RETE_SCARICA[['u', 'v']], on=['u', 'v'], how='left')
    RS_uv.dropna(inplace=True)

    del flux_RETE_SCARICA
    del flux_PEAK_HOUR
    del rows_CD

    ### group by edge...and make an average of all other variables (over all the HOURS)
    RS_uv = RS_uv[['u', 'v', 'FLUX', 'volatility', 'speed', 'max_FLUX', 'density',
                   'speed_PHF', 'cap_drop', 'PHF_density']].groupby(['u', 'v']).mean().reset_index()

    ##### --------------- ####
    ## compute RESISTANCE ####
    RS_uv['RESISTANCE'] = ((RS_uv.FLUX + RS_uv.volatility) / RS_uv.speed) / (
                (RS_uv.max_FLUX + RS_uv.volatility) / RS_uv.speed_PHF)
    # RS_uv = RS_uv[RS_uv.max_FLUX != RS_uv.cap_drop]
    RS_uv = RS_uv[RS_uv.max_FLUX > RS_uv.cap_drop]

    ### make consecutive edges...
    lista_uv = list(RS_uv[['u', 'v']].itertuples(index=False, name=None))
    output = consec_sort(lista_uv)
    consecutive_edges = pd.DataFrame(output)
    consecutive_edges.columns = ['u', 'v']
    RS_uv = pd.merge(consecutive_edges, RS_uv, on=['u', 'v'], how='left')
    RS_uv = RS_uv.drop_duplicates(['u', 'v'])
    ## compute "FLUX_in in"
    RS_uv['FLUX_in'] = RS_uv.FLUX.shift()
    ## make column of "FLUX_out"
    RS_uv['FLUX_out'] = RS_uv['FLUX']
    ## remove rows with NA values
    RS_uv.dropna(subset=['FLUX_out'], inplace=True)
    RS_uv.dropna(subset=['FLUX_in'], inplace=True)

    ##### --------------- ####
    ## compute RECOVERY ######
    RS_uv['RECOVERY'] = ((RS_uv.FLUX + (RS_uv.FLUX_in - RS_uv.FLUX_out)) / RS_uv.speed) / (
                (RS_uv.max_FLUX - RS_uv.cap_drop) / RS_uv.speed_PHF)

    ## compute RESILIENCE INDEX
    LPIR = pd.DataFrame([])
    for i in range(len(RS_uv)):
        print(i)
        if (RS_uv.density.iloc[i] <= RS_uv.PHF_density.iloc[i]):
            delta = 1
        elif (RS_uv.density.iloc[i] > RS_uv.PHF_density.iloc[i]):
            delta = 0
        row = RS_uv.iloc[[i]]
        print('=== delta ===:', delta)
        row['resilience'] = row.RESISTANCE * delta + row.RECOVERY * (1 - delta)
        LPIR = LPIR.append(row)

    del RS_uv

    LPIR = LPIR.sort_values('resilience', ascending=False)
    LPIR_RESILIENT = LPIR[LPIR.resilience <= 1]
    ### make the inverse...for the colormap...
    LPIR_RESILIENT.resilience = 1 - LPIR_RESILIENT.resilience
    LPIR_not_RESILIENT = LPIR[LPIR.resilience > 1]

    ## merge edges for congestion with the road network to get the geometry
    LPIR_RESILIENT = pd.merge(LPIR_RESILIENT, gdf_edges, on=['u', 'v'], how='left')
    LPIR_RESILIENT.drop_duplicates(['u', 'v'], inplace=True)
    # LPIR_RESILIENT = LPIR_RESILIENT.sort_values('length', ascending=False)
    LPIR_RESILIENT = gpd.GeoDataFrame(LPIR_RESILIENT)

    ###########################
    ##### PLOT ################

    ## rescale all data by an arbitrary number
    LPIR_RESILIENT["scales"] = round(((LPIR_RESILIENT.resilience/max(LPIR_RESILIENT.resilience)) * 3) + 0.1 ,1)
    ## rescale all data by an arbitrary number
    LPIR_RESILIENT["resilience"] = round(LPIR_RESILIENT.resilience,1)

    # add colors based on 'congestion_index'
    vmin = min(LPIR_RESILIENT.scales)
    vmax = max(LPIR_RESILIENT.scales)
    # Try to map values to colors in hex
    norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlGnBu)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
    LPIR_RESILIENT['color'] = LPIR_RESILIENT['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

    ## setup projection (CRS)
    LPIR_RESILIENT.crs = 4326

    ## make a RECTANGLE to clip the viasat_data
    from shapely.geometry import Polygon

    lat_point_list = [37.625, 37.625, 37.426, 37.426, 37.625]
    lon_point_list = [14.86, 15.25, 15.25, 14.86, 14.86]

    polygon_geom = Polygon(zip(lon_point_list, lat_point_list))
    crs = {'init': 'epsg:4326'}
    polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])
    ## check projection (CRS)
    # polygon.crs

    # polygon.plot()
    ## clip the data with the RECTANGLE
    LPIR_RESILIENT = gpd.clip(LPIR_RESILIENT, polygon)

    ### plot gedataframe with colors -----###
    ## add background map ###
    gdf = LPIR_RESILIENT
    import contextily as ctx

    del LPIR_RESILIENT

    # minx, miny, maxx, maxy = gdf.geometry.total_bounds
    # polygon.geometry.total_bounds

    ## reproject with mercator coordinates (this is the coordinate system of the basemap)
    gdf = gdf.to_crs(epsg=3857)
    # Plot the data within the RECTANGULAR extensions
    fig, ax = plt.subplots(figsize=(10, 10))
    polygon = polygon.to_crs(epsg=3857)
    polygon.plot(alpha=0,
                 color="white",
                 edgecolor="black",
                 ax=ax)
    gdf.plot(ax=ax, alpha=1, color=gdf['color'])
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)
    ax.set_axis_off()
    plt.show()

    ### get full month name (from number to month)
    import calendar
    MESE = int(MESE)
    MONTH_name = calendar.month_name[MESE]
    path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
    plt.savefig(path + 'resilience_' +
                MONTH_name + '_2019' + '_Catania_all_vehicles.png')
    plt.close()
