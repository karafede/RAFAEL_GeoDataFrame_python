
import os
os.chdir('D:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm
import matplotlib.colors as colors
import re
import folium
from itertools import chain
from osgeo import ogr
import geopandas as gpd
from funcs_network_FK import roads_type_folium
from funcs_network_FK import cost_assignment


# filter out some attributes
filter = ('["highway"!~"living_street|abandoned|footway|service|pedestrian|raceway|cycleway|steps|construction|'
          'service|bus_guideway|corridor|path|escape|rest_area|proposed"]')

# filter = (
#     '["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|'
#     'raceway|cycleway|steps|construction|primary|secondary|tertiary"]')

# import grapho (graphml)
G = ox.graph_from_address("Catania, Italy",
                          distance=60000,
                          network_type='drive', custom_filter=filter)


# graph_from_polygon

## save street network as ESRI shapefile (includes NODES and EDGES)
ox.save_graphml(G, filename = 'prova_Catania_street.graphml')
ox.plot_graph(G)

all_types =[]
for u, v, key, attr in G.edges(keys=True, data=True):
    print(attr["highway"])
    if attr['highway'] not in all_types:
        all_types.append(attr["highway"])
    



##############################################################
##############################################################
####### ROMA NETWORK #########################################


road_type = "motorway, motorway_link, secondary, primary, tertiary, residential, unclassified, living_street, trunk, trunk_link"
roads = road_type.replace(', ', '|')
filter = '["highway"~' + '"' + roads + '"' + "]"
distance = 20000 # distance from the center of the map (in meters)

G = ox.graph_from_address("ROMA, Italy",
                          distance=20000,
                          network_type='drive', custom_filter=filter)
ox.save_graphml(G, filename = 'ROMA_street.graphml')
ox.plot_graph(G)

place_country = "Roma, Italy"
file_graphml = 'ROMA_street.graphml'
cost_assignment(file_graphml, place_country)
file_graphml = 'Roma__Italy_cost.graphml'
my_map = roads_type_folium(file_graphml, road_type, place_country)
