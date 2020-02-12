
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
from matplotlib import cm



#### add ANAS catania Network #####################################
####################################################################

# load shp file

path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/files_ANAS/'
stralcio_ANAS_shape = gpd.read_file(path + "stralcio_grafo_ANAS.shp")
postazioni_ANAS_shape = gpd.read_file((path + "postazioni_area_catania.shp"))

my_map= folium.Map([37.53988692816245, 15.044971594798902], zoom_start=11, tiles='cartodbpositron')
# save first as geojson file
stralcio_ANAS_shape.to_file(filename='ANAS_stralcio.geojson', driver='GeoJSON')
postazioni_ANAS_shape.to_file(filename='postazioni_ANAS.geojson', driver='GeoJSON')
# add ANAS roads
folium.GeoJson('ANAS_stralcio.geojson').add_to((my_map))

'''
for i in range(len(postazioni_ANAS_shape)):
    folium.CircleMarker(location=[postazioni_ANAS_shape.LATITUDINE.iloc[i], postazioni_ANAS_shape.LONGITUDIN.iloc[i]],
                        popup=postazioni_ANAS_shape.STRADA.iloc[i],
                        radius=5,
                        color="black",
                        fill=True,
                        fill_color="black",
                        fill_opacity=1).add_to(my_map)

#folium.GeoJson('postazioni_ANAS.geojson').add_to((my_map))
my_map.save("ANAS_stralcio_map.html")
'''

####################################################################
####################################################################
# load ANAS total traffic counts data (Vehicles / hour)
ANAS_VEI_COUNTS = pd.read_csv('ANAS_summary_counts.csv')
ANAS_VEI_FLUX = pd.read_csv('ANAS_summary_peak_fluxes.csv')
# ANAS_VEI_FLUX[["August_2019","February_2019", "May_2019", "November_2019"]] = ANAS_VEI_FLUX[["August_2019","February_2019", "May_2019", "November_2019"]].fillna(0.0).astype(int)

# rgb tuple to hexadecimal conversion
def rgb2hex(rgb):
    rgb = [hex(int(256*x)) for x in rgb]
    r, g, b = [str(x)[2:] for x in rgb]
    return "#{}{}{}".format(r, g, b)

# Defines the color mapping from speeds to rgba
color_mapper = cm.ScalarMappable(cmap=cm.Reds)
# rgb_values = color_mapper.to_rgba(ANAS_VEI_FLUX['August_2019'])[:, 0:3] # keep rgb (first 3 columns)
# rgb_values = color_mapper.to_rgba(ANAS_VEI_FLUX['February_2019'])[:, 0:3] # keep rgb (first 3 columns)
# rgb_values = color_mapper.to_rgba(ANAS_VEI_FLUX['May_2019'])[:, 0:3] # keep rgb (first 3 columns)
rgb_values = color_mapper.to_rgba(ANAS_VEI_FLUX['November_2019'])[:, 0:3] # keep rgb (first 3 columns)
colors = [rgb2hex(rgb) for rgb in rgb_values]


ANAS_VEI_FLUX['marker_color'] = pd.cut(ANAS_VEI_FLUX['November_2019'], bins=13, labels=['lightgray', 'gray', 'blue', 'darkblue', 'green', 'yellow', 'lawngreen', 'orange', 'forestgreen', 'aqua', 'darkgray', 'plum', 'red'])
# ANAS_VEI_FLUX['marker_color'] = colors


for index, row in ANAS_VEI_FLUX.iterrows():
    folium.CircleMarker([row['LATITUDE'], row['LONGITUDE']],
                        popup=(row['STRADA'] + " " + "vehicles/hour: " + str(row['August_2019'])),
                        radius=9,
                        color=row['marker_color'],
                        fill_color=row['marker_color'],
                        fill=True,
                        fill_opacity=1).add_to(my_map)

#folium.GeoJson('postazioni_ANAS.geojson').add_to((my_map))
my_map.save("ANAS_stralcio_map.html")


