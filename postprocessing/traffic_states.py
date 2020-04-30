
import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
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
# from funcs_network_FK import roads_type_folium
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
# import db_connect
import datetime
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK
import glob
from funcs_network_FK import cost_assignment
import statistics



#######################################################################
## count how many times each "travelled speed (km/h)"
#######################################################################

# load all map matching files
# match pattern of .GeoJson files
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing\\new_geojsons')
extension = 'geojson'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
#combine all files in the list
gdf_all_EDGES = pd.concat([gpd.read_file(f) for f in all_filenames])


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



### load all map-matching files
### match pattern of .GeoJson files
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing\\new_geojsons')
extension = 'geojson'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
#combine all files in the list
gdf_all_EDGES = pd.concat([gpd.read_file(f) for f in all_filenames])



## initialize an empty dataframe to get sorted data by consecutive edges...
ordered_gdf_all_EDGES = pd.DataFrame([])

## sort by "timedate" for each "track_ID" (and for origin and destination...)
# list all "track_ID"
all_ID_TRACKS = list(gdf_all_EDGES.track_ID.unique())
for track_idx, track_ID in enumerate(all_ID_TRACKS):
    track_ID = str(track_ID)
    # print('VIASAT GPS track:', track_ID)
    BBB = gdf_all_EDGES[gdf_all_EDGES.track_ID == track_ID]
    BBB.reset_index(inplace=True)

    ## loop over each trip (ORIGIN--->
    ODs = BBB.drop_duplicates(['ORIGIN', 'DESTINATION'])
    O = list(ODs.ORIGIN)
    D = list(ODs.DESTINATION)
    zipped_OD = zip(O, D)
    # loop ever each ORIGIN --> DESTINATION pair
    for (O, D) in zipped_OD:
        # print(O, D)
        BBB_OD = BBB[(BBB.ORIGIN == O) & (BBB.DESTINATION == D)]
        BBB_OD.drop_duplicates(['id'], inplace=True)
        lista = list(BBB_OD[['u', 'v']].itertuples(index=False, name=None))
        output = consec_sort(lista)
        # BBB_OD.plot()

        df_output = pd.DataFrame(output)
        df_output.columns = ['u', 'v']
        # merge df_output with BBB
        MERGED = pd.merge(df_output, BBB_OD,  on=['u', 'v'], how='left')
        # re-convert to a geodataframe
        MERGED = gpd.GeoDataFrame(MERGED)
        # MERGED.plot()
        MERGED.drop_duplicates(['id'], inplace=True)
        MERGED.reset_index(inplace=True)
        # UUU = pd.DataFrame(MERGED)

        ## append ordered geodataframe to a new geodataframe
        ordered_gdf_all_EDGES = ordered_gdf_all_EDGES.append(MERGED)
        ordered_gdf_all_EDGES = ordered_gdf_all_EDGES.drop(['level_0'], axis=1)
        ordered_gdf_all_EDGES.reset_index(inplace=True)

        ## chek the structure...
        # PPP = pd.DataFrame(ordered_gdf_all_EDGES)

# rename as before.....
gdf_all_EDGES = ordered_gdf_all_EDGES

AAA = pd.DataFrame(gdf_all_EDGES)

# select speed and timedate
gdf_all_EDGES_speed = gdf_all_EDGES[['u', 'v', 'speed', 'timedate', 'length']]
gdf_all_EDGES_speed['speed'] = (gdf_all_EDGES_speed['speed'].ffill()+gdf_all_EDGES_speed['speed'].bfill())/2
# fill empty rows and round data
gdf_all_EDGES_speed = gdf_all_EDGES_speed.dropna(subset=['speed'])  # remove nan values
gdf_all_EDGES_speed['speed'] = round(gdf_all_EDGES_speed['speed'], 0)


## select only columns with 'u' and 'v' and speed
gdf_all_EDGES_speeds = gdf_all_EDGES_speed[['u', 'v', 'speed']]
df_all_EDGES_speeds = gdf_all_EDGES_speeds.groupby(gdf_all_EDGES_speeds.columns.tolist()).size().reset_index().rename(columns={0:'records_speeds'})

## select only columns 'u' and 'v'
gdf_all_EDGES_sel = gdf_all_EDGES_speed[['u', 'v']]
df_all_EDGES_sel = gdf_all_EDGES_sel.groupby(gdf_all_EDGES_sel.columns.tolist()).size().reset_index().rename(columns={0:'records_per_edge'})

## select only columns 'timedate' (I NEED THIS to CALCULATE the DENSITY)
gdf_all_EDGES_timedate = gdf_all_EDGES[['u','v','timedate']]
## drop nan values in the 'timedate' field (these are the timedates where the veicle is crossing the edge)
gdf_all_EDGES_timedate = gdf_all_EDGES_timedate.dropna(subset=['timedate'])  # remove nan values
df_all_EDGES_timedate = gdf_all_EDGES_timedate.groupby(gdf_all_EDGES_timedate.columns.tolist()).size().reset_index().rename(columns={0:'instant_records_by_edge'})
# make the mean for the same edge
df_all_EDGES_timedate_MEAN = df_all_EDGES_timedate.groupby(['u','v']).mean()
df_all_EDGES_timedate_MEAN = df_all_EDGES_timedate_MEAN.reset_index(level=['u', 'v'])
df_all_EDGES_timedate_MEAN['instant_records_by_edge'] = round(df_all_EDGES_timedate_MEAN['instant_records_by_edge'], 1).astype(np.int64)

# merge records_speeds and records
MERGED_speed_records = pd.merge(df_all_EDGES_speeds, df_all_EDGES_sel, on=['u', 'v'], how='inner')

# merge records_speeds and records
MERGED_speed_records = pd.merge(MERGED_speed_records, df_all_EDGES_timedate_MEAN, on=['u', 'v'], how='inner')

# calculate the percentage of time at mean speed in each edge
MERGED_speed_records["time_at_mean_speed(%)"] = round((MERGED_speed_records.records_speeds /MERGED_speed_records.records_per_edge)*100, 0)
MERGED_speed_records[["time_at_mean_speed(%)"]] = MERGED_speed_records[["time_at_mean_speed(%)"]].astype(int)

####################################
####################################
## get highway type on each edge ###

# load the file assigning the type of highway to each (u,v) edge
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
edges_highways = pd.read_csv("edges_highways.csv")

MERGED_speed_records = pd.merge(MERGED_speed_records, edges_highways[['u','v','highway']], on=['u', 'v'], how='left')
MERGED_speed_records.drop_duplicates(['u', 'v', 'speed', 'records_speeds', 'records_per_edge', 'instant_records_by_edge'], inplace=True)

###############################################
## assign max speed to each type of highway ###
###############################################

# define input road types as list
road_type = ['motorway', 'motorway_link', 'secondary', 'primary', 'tertiary', 'residential', 'unclassified',
             'trunk', 'trunk_link', 'tertiary_link', 'secondary_link', 'service']

## define "MAX SPEED" for each category of road in "highway"
# make a dictionary for ech max speed
max_speed_dict = {
        "residential": 50,
        "secondary": 90,
        "primary": 70,
        "tertiary": 70,
        "unclassified": 60,
        "secondary_link": 55,
        "trunk": 90,
        "tertiary_link": 50,
        "primary_link": 90,
        "motorway_link": 100,
        "trunk_link": 70,
        "motorway": 130,
        "living_street": 50,
        "road": 30,
        "other": 30,
        "service": 30
    }


## assign max speed depending from the type of road
edges_copy = MERGED_speed_records
edges_copy = edges_copy['highway'].str.replace(r"\(.*\)", "")
MERGED_speed_records.highway = edges_copy

all_data = pd.DataFrame([])
for road in road_type:
    print(road)
    if road in max_speed_dict.keys():
        print("yes")
        maxspeed = max_speed_dict.get(road)
        highway_type = MERGED_speed_records[(MERGED_speed_records.highway.isin([road]))]
        if len(highway_type) != 0:
            highway_type['maxspeed'] = maxspeed
            all_data = all_data.append(highway_type)

MERGED_speed_records = all_data



# add field "length"
MERGED_speed_records = pd.merge(MERGED_speed_records, gdf_all_EDGES[['u', 'v','length']], on=['u', 'v'], how='left')
MERGED_speed_records.drop_duplicates(['u', 'v', 'speed', 'records_speeds', 'records_per_edge', 'instant_records_by_edge'], inplace=True)
MERGED_speed_records.reset_index(inplace=True)

# calculate density (N vehicle/space) calculated @ each speed level
MERGED_speed_records['density (vei/km)'] = MERGED_speed_records['instant_records_by_edge']/(MERGED_speed_records['length']/1000)
# MERGED_speed_records['density (vei/km)'] = MERGED_speed_records['records_speeds']/(MERGED_speed_records['length']/1000)

# https://didattica-2000.archived.uniroma2.it//TTC/deposito/05_APP_CAP1_DEFLUSSO_ININTERROTTO_2010_COMPLETO.pdf

###############################################
########### flux ##############################

# calculate travel demand (flux) (NULL SCENARIO, no penalty)
TEMP = pd.DataFrame( gdf_all_EDGES[['u', 'v','timedate']])
## fill nans by values after non-nan values (for 'hour' and 'timedate')
TEMP['timedate'] = TEMP['timedate'].ffill()
TEMP = TEMP.dropna(subset=['timedate'])  # remove nan values


## merge TEMP with records
df_flux = pd.merge(TEMP, df_all_EDGES_sel, on=['u', 'v'], how='left')
df_flux['timedate'] = pd.to_datetime(df_flux['timedate'])

## use 15 minutes average per u,v pair...
df_flux = df_flux.groupby(['u','v']).resample(rule='15Min', on='timedate').mean()

## fill empty times
df_flux['records_per_edge'] = df_flux['records_per_edge'].ffill()  # 15 minutes flux
df_flux['u'] = df_flux['u'].ffill()
df_flux['v'] = df_flux['v'].ffill()
df_flux['u'] = df_flux['u'].astype(np.int64)
df_flux['v'] = df_flux['v'].astype(np.int64)

## compute hourly flux
df_flux['records_per_edge'] = df_flux['records_per_edge'] / 0.25

# make averages by u--v pair (edges)
# drop the "messy" index column...
df_flux.reset_index(drop=True, inplace=True)
df_flux = pd.DataFrame(df_flux)
df_flux = df_flux.groupby(['u','v']).mean()
df_flux = df_flux.reset_index(level=['u', 'v'])

df_flux=df_flux.rename(columns = {'records_per_edge':'flux (vei/hour)'})
max(df_flux['flux (vei/hour)'])

## merge all data together
MERGED_speed_records = pd.merge(MERGED_speed_records, df_flux, on=['u', 'v'], how='left')

## merge with the geodataframe to make a geo-dataframe
len(MERGED_speed_records)
MERGED_speed_records_geo = pd.merge(gdf_all_EDGES[['u','v','geometry']], MERGED_speed_records, on=['u', 'v'], how='left')
# MERGED_speed_records_geo.drop_duplicates(['u', 'v', 'speed', 'records_speeds', 'records_per_edge', 'instant_records_by_edge'], inplace=True)
MERGED_speed_records_geo.drop_duplicates(['u', 'v'], inplace=True)
## fil nan values
MERGED_speed_records_geo['flux (vei/hour)'] = (MERGED_speed_records_geo['flux (vei/hour)'].ffill()+MERGED_speed_records_geo['flux (vei/hour)'].bfill())/2
## remove nan values
MERGED_speed_records_geo = MERGED_speed_records_geo.dropna(subset=['flux (vei/hour)'])  # remove nan values
MERGED_speed_records_geo.plot()

HHH = pd.DataFrame(MERGED_speed_records_geo)

#############################################################
###### plot TRAFFIC FLUXES ##################################
#############################################################

### add colors based on 'records'
vmin = min(MERGED_speed_records_geo['flux (vei/hour)'])
vmax = max(MERGED_speed_records_geo['flux (vei/hour)'])
## map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.Reds)  # scales of reds
MERGED_speed_records_geo['color'] = MERGED_speed_records_geo['flux (vei/hour)'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))


#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=14, tiles='cartodbpositron')
###################################################

# add colors to map
my_map = plot_graph_folium_FK(MERGED_speed_records_geo, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=2, edge_opacity=0.7)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    MERGED_speed_records_geo[['u','v', 'flux (vei/hour)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'flux (vei/hour)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
##########################################

MERGED_speed_records_geo.to_file(filename='FLUX_by_EDGES.geojson', driver='GeoJSON')
my_map.save("FLUX_by_EDGES_2019-04-15_Apr-27-2020.html")


HHH = pd.DataFrame(MERGED_speed_records_geo)


########################################################################
########################################################################
# build the TRAFFIC STATES LEVELS (Levels Of Service - LOS) ############
########################################################################


###################################################################
### URBAN TRAFFIC #################################################
###################################################################

geo_df = MERGED_speed_records_geo
## fill missing rows
geo_df['density (vei/km)'] = geo_df['density (vei/km)'].ffill()
geo_df['maxspeed'] = geo_df['maxspeed'].ffill()
geo_df['speed'] = geo_df['speed'].ffill()
KKK = pd.DataFrame(geo_df)
# geo_df = geo_df[geo_df.maxspeed.isin([50, 90])]
geo_df.reset_index(inplace=True)
# create a new field 'LOS"
geo_df['LOS'] = None
for i in range(len(geo_df)):
    row = geo_df.iloc[i]
    ### for URBAN traffic ==================================================
    if (row['time_at_mean_speed(%)'] > 85) and (row['maxspeed'] == 50) or (row['maxspeed'] == 90) and \
            (row['speed'] > 42.2 ) and (row['density (vei/km)'] < 7 ):
        print("OK=============================== Libero ============OK")
        geo_df.LOS.iloc[i] = "A; Libero"
    if (row['time_at_mean_speed(%)'] > 85) and (row['maxspeed'] == 90 or (row['maxspeed'] == 50)) and \
            (row['speed'] > 76.5) and (row['density (vei/km)'] < 7):
        print("OK=============================== Libero ==========OK")
        # print(i)
        geo_df.LOS.iloc[i] = "A; Libero"
    if (row['time_at_mean_speed(%)'] < 85) and (row['time_at_mean_speed(%)'] > 67) and (row['maxspeed'] == 50) or\
            (row['maxspeed'] == 90)and \
            (row['speed'] < 42.2) and (row['speed'] > 33.5) and (row['density (vei/km)'] < 11) \
            and (row['density (vei/km)']> 7):
        print("OK===========================================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "B; Libero"
    if (row['time_at_mean_speed(%)'] < 85) and (row['time_at_mean_speed(%)'] > 67) and (row['maxspeed'] == 90) or\
            (row['maxspeed'] == 50) and \
            ( row['speed'] < 76.5) and (row['speed'] > 60.3) and (row['density (vei/km)'] < 11) and\
            (row['density (vei/km)']>7 ):
        print("OK========================Libero ====OK")
        # print(i)
        geo_df.LOS.iloc[i] = "B; Libero"
    if (row['time_at_mean_speed(%)'] < 67) and (row['time_at_mean_speed(%)'] > 50) and (row['maxspeed'] == 50) or\
            (row['maxspeed'] == 90) and \
            (row['speed'] < 33.5) and (row['speed'] > 25) and (row['density (vei/km)'] < 17) and\
            (row['density (vei/km)'] > 11):
        print("OK========== Libero =================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "C; Stabile"
    if (row['time_at_mean_speed(%)'] < 67) and (row['time_at_mean_speed(%)'] > 50 ) and (row['maxspeed'] == 90) or \
            ((row['maxspeed'] == 50)) and \
            (45 < row['speed'] < 60.3) and (row['speed'] > 45) and (row['density (vei/km)'] < 17) and \
            (row['density (vei/km)']> 11):
        print("OK================================================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "C; Stabile"
    if (40 < row['time_at_mean_speed(%)'] < 50) and (row['time_at_mean_speed(%)'] > 40) and (row['maxspeed'] == 50) or\
            (row['maxspeed'] == 90)and \
            (row['speed'] < 25) and (row['speed'] > 20) and (row['density (vei/km)'] < 22) and\
            (row['density (vei/km)'] > 17):
        print("OK====================== Stabile ===========OK")
        # print(i)
        geo_df.LOS.iloc[i] = "D; Congestionato"
    if (row['time_at_mean_speed(%)'] < 50) and(row['time_at_mean_speed(%)'] > 40) and (row['maxspeed'] == 90) or\
            (row['maxspeed'] == 50) and \
            (row['speed'] < 45) and (row['speed'] > 36) and (row['density (vei/km)'] < 22) and \
            (row['density (vei/km)']> 17):
        print("OK====================== Congestionato =====OK")
        # print(i)
        geo_df.LOS.iloc[i] = "D; Congestionato"
    if (row['time_at_mean_speed(%)'] < 40) and (row['time_at_mean_speed(%)']> 30) and (row['maxspeed'] == 50) or \
            (row['maxspeed'] == 90) and \
            (row['speed'] < 20) and (row['speed'] > 15) and (row['density (vei/km)'] < 28) and\
            (row['density (vei/km)']> 22):
        print("OK====================== Congestionato =======OK")
        # print(i)
        geo_df.LOS.iloc[i] = "E; Saturato"
    if (row['time_at_mean_speed(%)'] < 40) and (row['time_at_mean_speed(%)'] > 30) and (row['maxspeed'] == 90) or \
            (row['maxspeed'] == 50) and \
            (row['speed'] < 36) and (row['speed'] > 27) and (row['density (vei/km)'] < 28) and \
            (row['density (vei/km)']> 22):
        print("OK====================== SATURATO ===OK")
        # print(i)
        geo_df.LOS.iloc[i] = "E; Saturato"
    if (row['time_at_mean_speed(%)'] < 30) and (row['maxspeed'] == 50) or (row['maxspeed'] == 90) and \
            (row['speed'] < 15) and  (row['density (vei/km)'] > 28):
        print("OK====================== Saturato ===OK")
        # print(i)
        geo_df.LOS.iloc[i] = "F; Saturato"
    if (row['time_at_mean_speed(%)'] < 30) and (row['maxspeed'] == 90) or (row['maxspeed'] == 50) and \
            (row['speed'] < 27) and (row['density (vei/km)'] > 28):
        print("OK====================== Saturato ===OK")
        # print(i)
        geo_df.LOS.iloc[i] = "F; Saturato"

    ### for motorways ================================================
    if (row['time_at_mean_speed(%)'] <= 35) and (row['speed'] > 90):
        print("OK=============================== Libero ============OK")
        geo_df.LOS.iloc[i] = "A; Libero"
    if (row['time_at_mean_speed(%)'] < 50) and (row['time_at_mean_speed(%)'] > 35) and\
            (row['speed'] < 90) and (row['speed'] > 80):
        print("OK===========================================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "B; Libero"
    if (row['time_at_mean_speed(%)'] < 65) and (row['time_at_mean_speed(%)'] > 50) and \
            (row['speed'] < 80) and (row['speed'] > 70):
        print("OK========== Libero =================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "C; Stabile"
    if (40 < row['time_at_mean_speed(%)'] < 80) and (row['time_at_mean_speed(%)'] > 65) and \
            (row['speed'] < 70) and (row['speed'] > 60):
        print("OK====================== Stabile ===========OK")
        # print(i)
        geo_df.LOS.iloc[i] = "D; Congestionato"
    if (row['time_at_mean_speed(%)'] > 80) and (row['speed'] < 35) :
        print("OK====================== Congestionato =======OK")
        # print(i)
        geo_df.LOS.iloc[i] = "E; Saturato"

traffic_states = pd.DataFrame(geo_df)
traffic_states = traffic_states.sort_values('LOS')








########################################################
##### MOTORWAY TRAFFIC #################################
########################################################

geo_df = MERGED_speed_records_geo
geo_df = geo_df[geo_df.maxspeed.isin([130, 100])]  # motorway and motorway_link
geo_df.reset_index(inplace=True)
geo_df = pd.DataFrame(geo_df)
# create a new field 'LOS"
geo_df['LOS'] = None
for i in range(len(geo_df)):
    row = geo_df.iloc[i]
    if (row['time_at_mean_speed(%)'] <= 35) and (row['speed'] > 90):
        print("OK=============================== Libero ============OK")
        geo_df.LOS.iloc[i] = "A; Libero"
    if (row['time_at_mean_speed(%)'] < 50) and (row['time_at_mean_speed(%)'] > 35) and\
            (row['speed'] < 90) and (row['speed'] > 80):
        print("OK===========================================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "B; Libero"
    if (row['time_at_mean_speed(%)'] < 65) and (row['time_at_mean_speed(%)'] > 50) and \
            (row['speed'] < 80) and (row['speed'] > 70):
        print("OK========== Libero =================OK")
        # print(i)
        geo_df.LOS.iloc[i] = "C; Stabile"

    if (40 < row['time_at_mean_speed(%)'] < 80) and (row['time_at_mean_speed(%)'] > 65) and \
            (row['speed'] < 70) and (row['speed'] > 60):
        print("OK====================== Stabile ===========OK")
        # print(i)
        geo_df.LOS.iloc[i] = "D; Congestionato"

    if (row['time_at_mean_speed(%)'] > 80) and (row['speed'] < 35) :
        print("OK====================== Congestionato =======OK")
        # print(i)
        geo_df.LOS.iloc[i] = "E; Saturato"


GGG = pd.DataFrame(geo_df)
GGG = GGG.sort_values('LOS')





