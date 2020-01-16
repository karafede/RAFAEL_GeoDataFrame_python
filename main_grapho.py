
############################
######### RUNS #############
############################

from funcs_network_FK import graph
from funcs_network_FK import cost_assignment
from funcs_network_FK import roads_type_folium
from funcs_network_FK import centrality
from query_db_viasat import viasat_map_data

# input name of City and Country
place_country = "Catania, Italy"
# filter = ('["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|' \
#          'raceway|cycleway|steps|construction|primary|tertiary"]')

# define input road types as list
# road_type = "motorway, motorway_link"
# road_type = "motorway, motorway_link, secondary"
road_type = "motorway, motorway_link, secondary, primary, tertiary"
# road_type = "motorway, motorway_link, secondary, primary, tertiary, residential"

roads = road_type.replace(', ', '|')
filter = '["highway"~' + '"' + roads + '"' + "]"
distance = 60000 # distance from the center of the map (in meters)

# make grapho, save .graphml, save shapefile (node and edges) and get statistics (basic and extended)
network_city = graph(place_country, distance, filter)

# assign weight and cost (==time) to the grapho
file_graphml = 'Catania__Italy.graphml'
cost_assignment(file_graphml, place_country)

# select road type and make a map (it returns a my_map, to be used as base map for the viasat data)
# !!! use the _cost.graphml
file_graphml = 'Catania__Italy_cost.graphml'
my_map = roads_type_folium(file_graphml, road_type, place_country)

# viasat data (make a map)
# !!! use the _cost.graphml
viasat_data = viasat_map_data(file_graphml, road_type, place_country)

# edge centrality (make a map) (bc = betweenness centrality; cc = closeness centrality)
centrality(file_graphml, road_type, place_country, bc=True, cc=False)


