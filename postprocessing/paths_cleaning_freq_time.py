
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


## reload data (to be used later on...)
# gdf_all_EDGES = gpd.read_file("all_EDGES.geojson")

# gdf_all_EDGES = gpd.read_file("all_EDGES_09032020.geojson")
# gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15.geojson")
# gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-15_Mar-27-2020.geojson")
gdf_all_EDGES = gpd.read_file("all_EDGES_2019-04-16_Mar-30-2020.geojson")

## select only columns 'u' and 'v'
gdf_all_EDGES_sel = gdf_all_EDGES[['u', 'v']]
# time --> secs
# distance --> km
# speed --> km/h
gdf_all_EDGES_time = gdf_all_EDGES[['u', 'v', 'time', 'distance', 'speed']]

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
# select only columns with records > N
# df_all_EDGES_sel = df_all_EDGES_sel[df_all_EDGES_sel.records >= 15]
df_all_EDGES_sel = df_all_EDGES_sel[df_all_EDGES_sel.records >= round(threshold,0) + 1]
# add colors based on 'records'
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

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################

# add colors to map
my_map = plot_graph_folium_FK(MERGED_clean_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=0.7)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    MERGED_clean_EDGES[['u','v', 'records', 'length', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'length', 'records']
    ),
).add_to(my_map)

my_map.save("clean_matched_route_frequecy.html")


#######################################################################
######### get the time travelled in each edge, when available #########
#######################################################################

### get average of traveled "time" and travelled "speed" for each edge
df_all_EDGES_time = (gdf_all_EDGES_time.groupby(['u', 'v']).mean()).reset_index()
df_all_EDGES_time.columns = ["u", "v", "travel_time", "travel_distance", "travel_speed", ]
### merge with the above "df_all_EDGES_sel" referred to the counts counts
# df_all_EDGES_time = pd.merge(df_all_EDGES_time, df_all_EDGES_sel, on=['u', 'v'], how='inner')
### drop NaN values
df_all_EDGES_time = df_all_EDGES_time.dropna(subset=['travel_time'])

# sort values by travelled time
sorted_values = df_all_EDGES_time.sort_values('travel_time')
df_all_EDGES_time = df_all_EDGES_time[df_all_EDGES_time.travel_time < 1500] #(1000 sec == 16 minutes)
sorted_values = df_all_EDGES_time.sort_values('travel_time')

# make a copy
df_all_timeEDGES = df_all_EDGES_time
# add colors based on 'time' (seconds)
vmin = min(df_all_timeEDGES.travel_time[df_all_timeEDGES.travel_time > 0])
vmax = max(df_all_timeEDGES.travel_time)
AVG = np.average(df_all_timeEDGES.travel_time)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.cool)  # scales of reds (or "coolwarm" , "bwr")
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
TIME_EDGES['travel_time'] = round(TIME_EDGES['travel_time'], 0)
TIME_EDGES['travel_distance'] = round(abs(TIME_EDGES['travel_distance']), 2)
TIME_EDGES['travel_speed'] = round(TIME_EDGES['travel_speed'], 0)


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
    TIME_EDGES[['travel_time', 'travel_speed', 'travel_distance', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel_time', 'travel_speed', 'travel_distance']
    ),
).add_to(my_map)

TIME_EDGES.to_file(filename='TIME_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_travel_time.html")



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
COLORS_by_records = pd.DataFrame( MERGED_clean_EDGES.drop_duplicates(['records', 'color']))[['records', 'color']]
# sort by ascending order of the column records
COLORS_by_records = COLORS_by_records.sort_values(by=['records'])
len(COLORS_by_records)
# keep same order...
color_list = COLORS_by_records.color.drop_duplicates().tolist()
# display colorbar based on hex colors:

fig, ax = plt.subplots(figsize=(6, 1))
fig.subplots_adjust(bottom=0.5)
# cmap = matplotlib.colors.ListedColormap(color_list)
cmap = mpl.cm.Reds
MAX  = max(COLORS_by_records.records)
MIN  = min(COLORS_by_records.records)
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
cb2.set_label('travel frequency (a.u.)')
# fig.show()
# save colorbar (map-matching frequency)
fig.savefig('colorbar_map_matched.png')

merc = os.path.join('colorbar_map_matched.png')
# overlay colorbar to my_map
folium.raster_layers.ImageOverlay(merc, bounds = [[37.822617, 15.734203], [37.768644,15.391770]], interactive=True, opacity=1).add_to(my_map)
# re-save map
my_map.save("clean_matched_route_frequecy.html")

################################################################
################################################################


'''
###################################
##### ORIGINS and DESTINATIONS ####
###################################

# laad grafo
# file_graphml = 'Catania__Italy_cost.graphml'
# grafo = ox.load_graphml(file_graphml)


# make an empty dataframe to report all ORIGINS from which travels started and that crossed a given edge (u,v)
all_ORIGINS_df = pd.DataFrame([])
all_DESTINATIONS_df = pd.DataFrame([])

# get all the path accross the same edge (u,v)
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
all_ORIGINS_gdf.plot()

all_DESTINATIONS_df.drop_duplicates(['LON_DESTINATION', 'LAT_DESTINATION'], inplace=True)
#  make a geodataframe from lat, lon
geometry = [Point(xy) for xy in zip(all_DESTINATIONS_df.LON_DESTINATION, all_DESTINATIONS_df.LAT_DESTINATION)]
crs = {'init': 'epsg:4326'}
all_DESTINATIONS_gdf = GeoDataFrame(all_DESTINATIONS_df, crs=crs, geometry=geometry)
# save first as geojson file
all_DESTINATIONS_gdf.geometry.to_file(filename='all_PATHS_gdf.geojson', driver='GeoJSON')
all_DESTINATIONS_gdf.plot()


for idx, row in all_ORIGINS_df.iterrows():
    folium.CircleMarker(location=[row["LAT_ORIGIN"], row["LON_ORIGIN"]],
                                                # popup=row["deviceid"],
                                                radius=0.5,
                                                color="black",
                                                # fill=True,
                                                # fill_color="black",
                                                fill_opacity=0.1).add_to(my_map)

for idx, row in all_DESTINATIONS_df.iterrows():
    folium.CircleMarker(location=[row["LAT_DESTINATION"], row["LON_DESTINATION"]],
                                                # popup=row["deviceid"],
                                                radius=0.5,
                                                color="blue",
                                                # fill=True,
                                                # fill_color="blue",
                                                fill_opacity=0.1).add_to(my_map)

my_map.save("clean_matched_route.html")

'''

