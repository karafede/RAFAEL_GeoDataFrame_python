
############################
######### RUNS #############
############################

from funcs_network_FK import graph
from funcs_network_FK import cost_assignment
from funcs_network_FK import roads_type_folium


# input name of City and Country
place_country = "Catania, Italy"
# distance from the center of the map (in meters)
distance = 20000

# load grapho, save .graphml, save shapefile (node and edges) and get statistics (basic and extended)
network_city = graph(place_country, distance)

# assign weight and cost (time) to the grapho
file_graphml = 'Catania__Italy.graphml'
cost_assignment(file_graphml, place_country)

# select road type and save on a html folium map
# define input road types as list
# road_type = "motorway, motorway_link"
road_type = "motorway, motorway_link, secondary, primary, tertiary"
roads_type_folium(file_graphml, road_type, place_country)
