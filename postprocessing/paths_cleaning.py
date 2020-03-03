
import os
os.chdir('D:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
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


## reload data (to be used later on...)
gdf_all_EDGES = gpd.read_file("all_EDGES.geojson")


## select only columns 'u' and 'v'
gdf_all_EDGES_sel = gdf_all_EDGES[['u', 'v']]
## count how many times an edge ('u', 'v') occur in the geodataframe
df_all_EDGES_sel = gdf_all_EDGES.groupby(gdf_all_EDGES_sel.columns.tolist()).size().reset_index().rename(columns={0:'records'})


df_all_EDGES_records = gdf_all_EDGES.groupby(gdf_all_EDGES_sel.columns.tolist()).size().reset_index().rename(columns={0:'records'})
# select only columns with records > N
df_all_EDGES_sel = df_all_EDGES_sel[df_all_EDGES_sel.records >= 15]
# add colors based on 'records'
vmin = min(df_all_EDGES_records.records)
vmax = max(df_all_EDGES_records.records)
# df_all_EDGES_records.iloc[-1] = np.nan
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.Reds)  # scales of reds
df_all_EDGES_records['color'] = df_all_EDGES_records['records'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))
records = df_all_EDGES_sel[['records']]

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


'''
AAA = MERGED_clean_EDGES[MERGED_clean_EDGES['u'] == 33589436]
AAA.u
AAA.v
AAA.records
AAA.color
'''

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################

'''
clean_edges_matched_route.geometry.to_file(filename='clean_matched_route.geojson', driver='GeoJSON')
folium.GeoJson('clean_matched_route.geojson').add_to((my_map))
my_map.save("clean_matched_route.html")
'''

# add colors to map
my_map = plot_graph_folium_FK(MERGED_clean_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=4, edge_opacity=1)  # tiles='cartodbpositron'
my_map.save("clean_matched_route.html")


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
fig.show()
# save colorbar (map-matching frequency)
fig.savefig('colorbar_map_matched.png')


merc = os.path.join('colorbar_map_matched.png')
# overlay colorbar to my_map
folium.raster_layers.ImageOverlay(merc, bounds = [[37.822617, 15.734203], [37.768644,15.391770]], interactive=True, opacity=1).add_to(my_map)
# re-save map
my_map.save("clean_matched_route.html")


####################################################
####################################################
