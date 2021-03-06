
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
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import datetime
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK
from shapely.geometry import Point, LineString, MultiLineString
from shapely import geometry, ops
import glob

## reload data (to be used later on...)
# gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15_Apr-30-2020.geojson")
# gdf_all_EDGES = gpd.read_file("all_EDGES_10032020.geojson")  # LARGE file
# gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15.geojson")
# gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15_Apr-07-2020.geojson")
# gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15_Apr-11-2020.geojson")

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

# rename as beofore.....
gdf_all_EDGES = ordered_gdf_all_EDGES


## make a .csv file that assigns to each (u,v) pair, the type of road ("highway")
edges = []
import json
for file in all_filenames:
    # with open("all_EDGES_2019-04-15_Apr-15-2020_0_130.geojson") as f:
    with open(file) as f:
        data = json.load(f)
    for feature in data['features']:
        print(feature['properties'])
        edge = [feature['properties']['u'], feature['properties']['v'], feature['properties']['highway']]
        edges.append((edge))
edges_highways = pd.DataFrame(edges,  columns=['u', 'v', 'highway'])
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
edges_highways.to_csv('edges_highways.csv')


os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
## select only columns 'u' and 'v'
gdf_all_EDGES_sel = gdf_all_EDGES[['u', 'v']]
# time --> secs
# distance --> km
# speed --> km/h
# gdf_all_EDGES_time = gdf_all_EDGES[['u', 'v', 'time', 'distance', 'speed', 'hour', 'timedate']]
gdf_all_EDGES_time = gdf_all_EDGES[['u', 'v', 'time', 'distance', 'speed']]

## fill nans by mean of before and after non-nan values (for 'time' and 'speed')
gdf_all_EDGES_time['time'] = (gdf_all_EDGES_time['time'].ffill()+gdf_all_EDGES_time['time'].bfill())/2
gdf_all_EDGES_time['speed'] = (gdf_all_EDGES_time['speed'].ffill()+gdf_all_EDGES_time['speed'].bfill())/2

# AAA = pd.DataFrame(gdf_all_EDGES_time)
# AAA.dropna(subset = ['hour'], inplace= True)

###################
#### GROUP BY #####
###################

#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

df_all_EDGES_sel = gdf_all_EDGES.groupby(gdf_all_EDGES_sel.columns.tolist()).size().reset_index().rename(columns={0:'records'})

# make a copy
df_all_EDGES_records = df_all_EDGES_sel
threshold = np.average(df_all_EDGES_records.records)

### select only columns with records > N
# df_all_EDGES_sel = df_all_EDGES_sel[df_all_EDGES_sel.records >= 15]
# df_all_EDGES_sel = df_all_EDGES_sel[df_all_EDGES_sel.records >= round(threshold,0) + 1]

### add colors based on 'records'
vmin = min(df_all_EDGES_records.records)
vmax = max(df_all_EDGES_records.records)
# df_all_EDGES_records.iloc[-1] = np.nan
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.Reds)  # scales of reds
df_all_EDGES_records['color'] = df_all_EDGES_records['records'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
# records = df_all_EDGES_sel[['records']]

df_all_EDGES_sel = df_all_EDGES_sel[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
keys = list(df_all_EDGES_sel.columns.values)
index_recover_all_EDGES = gdf_all_EDGES.set_index(keys).index
index_df_all_EDGES_sel = df_all_EDGES_sel.set_index(keys).index
clean_edges_matched_route = gdf_all_EDGES[index_recover_all_EDGES.isin(index_df_all_EDGES_sel)]

# get same color name according to the same 'u' 'v' pair
clean_edges_matched_route[['u', 'v']].head()
# merge records and colors into the geodataframe
MERGED_clean_EDGES = pd.merge(clean_edges_matched_route, df_all_EDGES_records, on=['u', 'v'], how='inner')
# remove duplicates nodes
MERGED_clean_EDGES.drop_duplicates(['u', 'v'], inplace=True)
MERGED_clean_EDGES['records'] = round(MERGED_clean_EDGES['records'], 0)
MERGED_clean_EDGES['length(km)'] = MERGED_clean_EDGES['length']/1000
MERGED_clean_EDGES['length(km)'] = round(MERGED_clean_EDGES['length(km)'], 3)
# compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
max_records = max(MERGED_clean_EDGES['records'])
MERGED_clean_EDGES['frequency(%)'] = (MERGED_clean_EDGES['records']/max_records)*100
MERGED_clean_EDGES['frequency(%)'] = round(MERGED_clean_EDGES['frequency(%)'], 0)

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=14, tiles='cartodbpositron')
###################################################

# add colors to map
my_map = plot_graph_folium_FK(MERGED_clean_EDGES, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=2, edge_opacity=0.7)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    MERGED_clean_EDGES[['u','v', 'frequency(%)', 'records', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'length(km)', 'frequency(%)', 'records']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
##########################################

MERGED_clean_EDGES.to_file(filename='FREQUENCIES_and_RECORDS_by_EDGES.geojson', driver='GeoJSON')
# my_map.save("clean_matched_route_frequecy_all_EDGES_10032020.html")
my_map.save("clean_matched_route_frequecy_all_EDGES_2019-04-15_Apr-27-2020.html")


### compute the average number of trips between the SAME ORIGIN and DESTINATION
gdf_all_EDGES_ODs = gdf_all_EDGES[['ORIGIN', 'DESTINATION']]
df_all_EDGES_ODs = gdf_all_EDGES.groupby(gdf_all_EDGES_ODs.columns.tolist()).size().reset_index().rename(columns={0:'N_trips'})
edge_with_more_trips = df_all_EDGES_ODs[['ORIGIN','DESTINATION']][ df_all_EDGES_ODs.N_trips == max(df_all_EDGES_ODs.N_trips)]

#######################################################################
######### get the travelled TIME in each edge, when available #########
#######################################################################


# LENGHTS = pd.DataFrame(gdf_all_EDGES.length)
# SUMMARY_times = pd.DataFrame(gdf_all_EDGES_time.time)  # time is in seconds
# SUMMARY_times = SUMMARY_times.dropna(subset=['time'])
# SUMMARY_times.reset_index(inplace=True)

# get maximum edge length
L = []
for i in range(len(gdf_all_EDGES)):
    l = gdf_all_EDGES.iloc[i].length
    L.append(l)
max_length = max(L) #in meters
# Out[89]: 11050.723 meters
# minimum speed: 30kh/h ---> 120 sec for each km of road
max_possible_time = (1/60)*3600*max_length/1000

# gdf_all_EDGES_time = gdf_all_EDGES_time[gdf_all_EDGES_time.time < max_possible_time]
# AAA = pd.DataFrame(gdf_all_EDGES_time)

### get AVERAGE of traveled "time" and travelled "speed" for each edge
df_all_EDGES_time = (gdf_all_EDGES_time.groupby(['u', 'v']).mean()).reset_index()
df_all_EDGES_time.columns = ["u", "v", "travel_time", "travel_distance", "travel_speed"]
df_all_EDGES_time = pd.merge(MERGED_clean_EDGES, df_all_EDGES_time, on=['u', 'v'], how='inner')
df_all_EDGES_time = pd.DataFrame(df_all_EDGES_time)
sorted_length = df_all_EDGES_time.sort_values('length')
df_all_EDGES_time = df_all_EDGES_time[["u", "v", "travel_time", "travel_distance", "length(km)", "travel_speed"]]
### merge with the above "df_all_EDGES_sel" referred to the counts counts
# df_all_EDGES_time = pd.merge(df_all_EDGES_time, df_all_EDGES_sel, on=['u', 'v'], how='inner')
### drop NaN values
df_all_EDGES_time = df_all_EDGES_time.dropna(subset=['travel_time'])
df_all_EDGES_time['travel_time'] = ((df_all_EDGES_time['length(km)']) / (df_all_EDGES_time['travel_speed'])) *3600 # seconds

# sort values by travelled time
# sorted_values = df_all_EDGES_time.sort_values('travel_time')
# df_all_EDGES_time = df_all_EDGES_time[df_all_EDGES_time.travel_time < 1500] #(1000 sec == 16 minutes)
# sorted_values = df_all_EDGES_time.sort_values('travel_time')

# make a copy
df_all_timeEDGES = df_all_EDGES_time
# add colors based on 'time' (seconds)
vmin = min(df_all_timeEDGES.travel_time[df_all_timeEDGES.travel_time > 0])
vmax = max(df_all_timeEDGES.travel_time)
AVG = np.average(df_all_timeEDGES.travel_time)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.cool)  # scales of reds (or "coolwarm" , "bwr", °cool°)
df_all_timeEDGES['color'] = df_all_timeEDGES['travel_time'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_time = df_all_EDGES_time[['u','v']]


# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
keys = list(df_all_EDGES_time.columns.values)
index_recover_all_EDGES = gdf_all_EDGES.set_index(keys).index
index_df_all_EDGES_time = df_all_EDGES_time.set_index(keys).index

times_edges_matched_route = gdf_all_EDGES[index_recover_all_EDGES.isin(index_df_all_EDGES_time)]

# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
TIME_EDGES = pd.merge(times_edges_matched_route, df_all_timeEDGES, on=['u', 'v'], how='inner')
# remove duplicates nodes
TIME_EDGES.drop_duplicates(['u', 'v'], inplace=True)
TIME_EDGES['travel_time'] = round(TIME_EDGES['travel_time'], 1)
# TIME_EDGES['travel_time'] = TIME_EDGES['travel_time']/60  # minutes
# TIME_EDGES['travel_time'] = round(TIME_EDGES['travel_time'], 3)
# TIME_EDGES['travel_distance'] = (TIME_EDGES['travel_speed']) * (TIME_EDGES['travel_time']/60)  # (km/h)
# TIME_EDGES['travel_distance'] = round(abs(TIME_EDGES['travel_distance']), 2)

TIME_EDGES['length(km)'] = TIME_EDGES['length']/1000
TIME_EDGES['length(km)'] = round(TIME_EDGES['length(km)'], 3)

# TIME_EDGES['travel_distance'] = round(abs(TIME_EDGES["length(km)"]), 2)
# TIME_EDGES['travel_time'] = round((TIME_EDGES['length(km)'])/(TIME_EDGES['travel_speed']), 0)
TIME_EDGES['travel_speed'] = round(TIME_EDGES['travel_speed'], 0)

TIME_EDGES=TIME_EDGES.rename(columns = {'travel_time':'travel time (sec)'})
TIME_EDGES=TIME_EDGES.rename(columns = {'travel_distance':'travelled distance (km)'})
TIME_EDGES=TIME_EDGES.rename(columns = {'travel_speed':'travelled speed (km/h)'})


#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################

# add colors to map
my_map = plot_graph_folium_FK(TIME_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    TIME_EDGES[['travel time (sec)', 'travelled speed (km/h)', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel time (sec)', 'travelled speed (km/h)', 'length(km)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

TIME_EDGES.to_file(filename='TIME_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_travel_time_all_EDGES_2019-04-15_Apr-27-2020.html")

#######################################################################
######### get the travelled SPEED in each edge, when available ########
#######################################################################

### get average of traveled "time" and travelled "speed" for each edge
df_all_EDGES_time = (gdf_all_EDGES_time.groupby(['u', 'v']).mean()).reset_index()
df_all_EDGES_time.columns = ["u", "v", "travel_time", "travel_distance", "travel_speed"]
df_all_EDGES_time = pd.merge(MERGED_clean_EDGES, df_all_EDGES_time, on=['u', 'v'], how='inner')
df_all_EDGES_time = pd.DataFrame(df_all_EDGES_time)
df_all_EDGES_time = df_all_EDGES_time[["u", "v", "travel_time", "travel_distance", "length(km)", "travel_speed"]]

### merge with the above "df_all_EDGES_sel" referred to the counts counts
# df_all_EDGES_time = pd.merge(df_all_EDGES_time, df_all_EDGES_sel, on=['u', 'v'], how='inner')
### drop NaN values
df_all_EDGES_speed = df_all_EDGES_time.dropna(subset=['travel_speed'])
df_all_EDGES_speed['travel_time'] = ((df_all_EDGES_speed['length(km)']) / (df_all_EDGES_speed['travel_speed'])) *3600 # seconds

# sort values by travelled time
sorted_values = df_all_EDGES_speed.sort_values('travel_speed')

# make a copy
df_all_speedEDGES = df_all_EDGES_speed
# add colors based on 'time' (seconds)
vmin = min(df_all_EDGES_speed.travel_speed[df_all_EDGES_speed.travel_speed > 0])
vmax = max(df_all_EDGES_speed.travel_speed)
AVG = np.average(df_all_EDGES_speed.travel_speed)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlGn)  # scales of reds (or "coolwarm" , "bwr")
df_all_EDGES_speed['color'] = df_all_EDGES_speed['travel_speed'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_speed = df_all_EDGES_speed[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
keys = list(df_all_EDGES_speed.columns.values)
index_recover_all_EDGES = gdf_all_EDGES.set_index(keys).index
index_df_all_EDGES_speed = df_all_EDGES_speed.set_index(keys).index

speeds_edges_matched_route = gdf_all_EDGES[index_recover_all_EDGES.isin(index_df_all_EDGES_speed)]

# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
SPEED_EDGES = pd.merge(speeds_edges_matched_route, df_all_speedEDGES, on=['u', 'v'], how='inner')
# remove duplicates nodes
SPEED_EDGES.drop_duplicates(['u', 'v'], inplace=True)
SPEED_EDGES['travel_time'] = round(SPEED_EDGES['travel_time'], 1)
# SPEED_EDGES['travel_time'] = round(SPEED_EDGES['travel_time'], 0)
# SPEED_EDGES['travel_time'] = SPEED_EDGES['travel_time']/60
# SPEED_EDGES['travel_time'] = round(SPEED_EDGES['travel_time'], 3)
# SPEED_EDGES['travel_distance'] = (SPEED_EDGES['travel_speed']) * (SPEED_EDGES['travel_time']/60)  # (km/h)
# SPEED_EDGES['travel_distance'] = round(abs(SPEED_EDGES['travel_distance']), 2)
SPEED_EDGES['travel_speed'] = round(SPEED_EDGES['travel_speed'], 0)

SPEED_EDGES['length(km)'] = SPEED_EDGES['length']/1000
SPEED_EDGES['length(km)'] = round(SPEED_EDGES['length(km)'], 3)

SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_time':'travel time (sec)'})
SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_distance':'travelled distance (km)'})
SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_speed':'travelled speed (km/h)'})


#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################


# add colors to map
my_map = plot_graph_folium_FK(SPEED_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    SPEED_EDGES[['travel time (sec)', 'travelled speed (km/h)', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel time (sec)', 'travelled speed (km/h)', 'length(km)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

# SPEED_EDGES.to_file(filename='SPEED_EDGES.geojson', driver='GeoJSON')
# my_map.save("clean_matched_route_travel_time.html")
my_map.save("clean_matched_route_travel_speed_all_EDGES_2019-04-15_Apr-27-2020.html")



'''
# add "time" to travel each edge (if found) as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    MERGED_clean_EDGES[['time', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'orange',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['time']
    ),
).add_to(my_map)
'''



######################################################################
######################## COLORBAR ####################################
######################################################################

import matplotlib as mpl
COLORS_by_records = pd.DataFrame( MERGED_clean_EDGES.drop_duplicates(['frequency(%)', 'color']))[['frequency(%)', 'color']]
# sort by ascending order of the column records
COLORS_by_records = COLORS_by_records.sort_values(by=['frequency(%)'])
len(COLORS_by_records)
# keep same order...
color_list = COLORS_by_records.color.drop_duplicates().tolist()
# display colorbar based on hex colors:

fig, ax = plt.subplots(figsize=(8, 1))
fig.subplots_adjust(bottom=0.5)
# cmap = matplotlib.colors.ListedColormap(color_list)
cmap = mpl.cm.Reds
MAX  = max(COLORS_by_records['frequency(%)'])
MIN  = min(COLORS_by_records['frequency(%)'])
cmap.set_over(str(MAX + 5))
cmap.set_under(str(MIN -5))

cmap.set_over('k')
cmap.set_under('white')

# make a sequence list of records
bounds = np.arange(MIN, MAX, 10).tolist()

norm = mpl.colors.BoundaryNorm(bounds, cmap.N)
cb2 = mpl.colorbar.ColorbarBase(ax, cmap=cmap,
                                norm=norm,
                                boundaries=[5] + bounds + [MAX+5],
                                extend='both',
                                ticks=bounds,
                                spacing='uniform',
                                orientation='horizontal')
cb2.set_label('travel frequency (%)')
# fig.show()
# save colorbar (map-matching frequency)
fig.savefig('colorbar_map_matched.png')

merc = os.path.join('colorbar_map_matched.png')
# overlay colorbar to my_map
folium.raster_layers.ImageOverlay(merc, bounds = [[37.822617, 15.734203], [37.768644,15.391770]], interactive=True, opacity=1).add_to(my_map)
# re-save map

# my_map.save("clean_matched_route_frequecy.html")
# my_map.save("clean_matched_route_frequecy_all_EDGES_10032020.html")
my_map.save("clean_matched_route_frequecy_all_EDGES_2019-04-15_Apr-03-2020.html")
################################################################
################################################################




'''
geoms = []
# get all the paths accross the same edge (u,v)
for i in range(len(MERGED_clean_EDGES)):
    U = MERGED_clean_EDGES.u.iloc[i]
    V = MERGED_clean_EDGES.v.iloc[i]
    print('u:', U, 'v:', V, '================================================')
    BBB = gdf_all_EDGES[(gdf_all_EDGES['u'] == U) & (gdf_all_EDGES['v'] == V)]
    # get all the "story of the track_ID vehicles
    ID_list = list(BBB.track_ID)
    # filter gdf_all_EDGES based on a list of index
    all_paths = gdf_all_EDGES[gdf_all_EDGES.track_ID.isin(ID_list)]
    # all_paths.plot()

    # make an unique linestring
    LINE = []
    # combine them into a multi-linestring
    for j in range(len(all_paths)):
        line = all_paths.geometry.iloc[j]
        LINE.append(line)

    multi_line = geometry.MultiLineString(LINE)
    # merge the lines
    merged_line = ops.linemerge(multi_line)
    geoms.append(merged_line)

# newdata = gpd.GeoDataFrame(MERGED_clean_EDGES, geometry=geoms)  # this file is too BIG!!!
# newdata.geometry.to_file(filename='newdata.geojson', driver='GeoJSON')
'''


#########################################################################
##### ORIGINS and DESTINATIONS accross the same edge (u,v) ##############
#########################################################################

import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
os.getcwd()

# load grafo
file_graphml = 'Catania__Italy_cost.graphml'
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)

# make an empty dataframe to report all ORIGINS from which travels started and that crossed a given edge (u,v)
all_ORIGINS_df = pd.DataFrame([])
all_DESTINATIONS_df = pd.DataFrame([])

# get all the path accross the same edge (u,v)
for i in range(len(MERGED_clean_EDGES)):
    U = MERGED_clean_EDGES.u.iloc[i]
    V = MERGED_clean_EDGES.v.iloc[i]
    # print('u:', U, 'v:', V, '================================================')
    BBB = gdf_all_EDGES[(gdf_all_EDGES['u'] == U) & (gdf_all_EDGES['v'] == V)]
    # get all the "story of the track_ID vehicles
    ID_list = list(BBB.track_ID)
    # filter gdf_all_EDGES based on a list of index
    all_paths = gdf_all_EDGES[gdf_all_EDGES.track_ID.isin(ID_list)]
    # all_paths.plot()

    # make an unique list of ORIGIN and DESTINATION nodes
    ORIGINS = list(all_paths.ORIGIN.unique())
    DESTINATIONS = list(all_paths.DESTINATION.unique())

    df_ORIGINS_lon = []
    df_ORIGINS_lat = []
    df_DESTINATIONS_lon = []
    df_DESTINATIONS_lat = []
    df = pd.DataFrame([])

    # get the latitutde and longitute from the grafo of all the Catania region
    for NODE_O in ORIGINS:
        try:
            lon_o = grafo.nodes[NODE_O]['x']
            lat_o = grafo.nodes[NODE_O]['y']
        except KeyError:
            pass
        df_ORIGINS_lon.append(lon_o)
        df_ORIGINS_lat.append(lat_o)

        df_lon_o = df.append(df_ORIGINS_lon, True)
        df_lat_o = df.append(df_ORIGINS_lat, True)
        df_lon_o.columns = ['LON_ORIGIN']
        df_lat_o.columns = ['LAT_ORIGIN']

        ORIGINS_coord = pd.concat([df_lon_o, df_lat_o], axis=1)
        ORIGINS_coord['u'] = U
        ORIGINS_coord['v'] = V
        all_ORIGINS_df = all_ORIGINS_df.append(ORIGINS_coord)


    for NODE_D in DESTINATIONS:
        try:
            lon_d = grafo.nodes[NODE_D]['x']
            lat_d = grafo.nodes[NODE_D]['y']
        except KeyError:
            pass
        df_DESTINATIONS_lon.append(lon_d)
        df_DESTINATIONS_lat.append((lat_d))
        df_lon_d = df.append(df_DESTINATIONS_lon, True)
        df_lat_d = df.append(df_DESTINATIONS_lat, True)
        df_lon_d.columns = ['LON_DESTINATION']
        df_lat_d.columns = ['LAT_DESTINATION']


        # bind the dataframes (keep track of U and V)
        DESTINATIONS_coord = pd.concat([df_lon_d, df_lat_d], axis=1)
        DESTINATIONS_coord['u'] = U
        DESTINATIONS_coord['v'] = V

        all_DESTINATIONS_df = all_DESTINATIONS_df.append(DESTINATIONS_coord)


# remove duplicates
all_ORIGINS_df.drop_duplicates(['LON_ORIGIN', 'LAT_ORIGIN'], inplace=True)
#  make a geodataframe from lat, lon
geometry = [Point(xy) for xy in zip(all_ORIGINS_df.LON_ORIGIN, all_ORIGINS_df.LAT_ORIGIN)]
crs = {'init': 'epsg:4326'}
all_ORIGINS_gdf = GeoDataFrame(all_ORIGINS_df, crs=crs, geometry=geometry)
# save first as geojson file
all_ORIGINS_gdf.geometry.to_file(filename='all_PATHS_gdf.geojson', driver='GeoJSON')
# all_ORIGINS_gdf.plot()

all_DESTINATIONS_df.drop_duplicates(['LON_DESTINATION', 'LAT_DESTINATION'], inplace=True)
#  make a geodataframe from lat, lon
geometry = [Point(xy) for xy in zip(all_DESTINATIONS_df.LON_DESTINATION, all_DESTINATIONS_df.LAT_DESTINATION)]
crs = {'init': 'epsg:4326'}
all_DESTINATIONS_gdf = GeoDataFrame(all_DESTINATIONS_df, crs=crs, geometry=geometry)
# save first as geojson file
all_DESTINATIONS_gdf.geometry.to_file(filename='all_PATHS_gdf.geojson', driver='GeoJSON')
# all_DESTINATIONS_gdf.plot()


for idx, row in all_ORIGINS_df.iterrows():
    folium.CircleMarker(location=[row["LAT_ORIGIN"], row["LON_ORIGIN"]],
                                                # popup=row["deviceid"],
                                                radius=0.5,
                                                color="blue",
                                                # fill=True,
                                                # fill_color="black",
                                                fill_opacity=0.1).add_to(my_map)

for idx, row in all_DESTINATIONS_df.iterrows():
    folium.CircleMarker(location=[row["LAT_DESTINATION"], row["LON_DESTINATION"]],
                                                # popup=row["deviceid"],
                                                radius=0.5,
                                                color="red",
                                                # fill=True,
                                                # fill_color="blue",
                                                fill_opacity=0.1).add_to(my_map)

my_map.save("clean_matched_route_OD.html")


