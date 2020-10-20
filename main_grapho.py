
############################
######### RUNS #############
############################

from funcs_network_FK import graph
from funcs_network_FK import cost_assignment
from funcs_network_FK import roads_type_folium
from funcs_network_FK import centrality
# from query_db_viasat import viasat_map_data
from add_VIASAT_data import viasat_map_data
import osmnx as ox

# input name of City and Country
place_country = "Catania, Italy"
# place_country = "Fisciano, Italy"
# filter = ('["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|' \
#          'raceway|cycleway|steps|construction|primary|tertiary"]')

# define input road types as list
# road_type = ['motorway', 'motorway_link', 'secondary', 'primary', 'tertiary', 'residential', 'unclassified',
#              'trunk', 'trunk_link', 'tertiary_link', 'secondary_link', 'service']
road_type = ['motorway', 'motorway_link', 'secondary', 'primary', 'tertiary', 'residential',
             'unclassified', 'trunk', 'trunk_link', 'tertiary_link', 'secondary_link', 'service']

# roads = road_type.replace(', ', '|')
# filter = '["highway"~' + '"' + roads + '"' + "]"
distance = 60000 # distance from the center of the map (in meters)
# distance = 70000 # distance from the center of the map (in meters)

# make grapho, save .graphml, save shapefile (node and edges) and get statistics (basic and extended)
###########################################
#### download OSM graph from the network ##
###########################################
network_city = graph(place_country, distance) # filter

# file_graphml = 'Catania__Italy_60km.graphml'
file_graphml = 'Catania__Italy_for_CENTRALITY.graphml'
# file_graphml = 'Fisciano__Italy.graphml'
grafo = ox.load_graphml(file_graphml)
# ox.plot_graph(grafo)
gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)
gdf_edges.plot()

# assign weight and cost (==time) to the grapho
# file_graphml = 'Catania__Italy.graphml'
file_graphml = 'Catania__Italy_60km.graphml'
# file_graphml = 'Fisciano__Italy.graphml'
# file_graphml = 'partial_OSM.graphml'
cost_assignment(file_graphml, place_country)

'''
# plot all the network on the map with folium
# load file graphml
Catania = ox.load_graphml('partial_OSM.graphml')
Catania = ox.plot_graph_folium(Catania, graph_map=None, popup_attribute=None, tiles='cartodbpositron', zoom=10,
                  fit_bounds=True, edge_width=1, edge_opacity=1)
Catania.save("partial_OSM.html")
'''

### select road type and make a map (to be used as base map for the viasat data)
# file_graphml = 'Catania__Italy_cost.graphml'
file_graphml = 'Catania__Italy_60km.graphml'
# file_graphml = 'Fisciano__Italy.graphml'
my_map = roads_type_folium(file_graphml, road_type, place_country)

# edge centrality (make a map) (bc = betweenness centrality; cc = closeness centrality)
centrality(file_graphml, place_country, bc=True, cc=False)  # road_type

# OSM map & viasat data (make a map)
# !!! use the _cost.graphml
viasat_data = viasat_map_data(file_graphml, road_type, place_country)


