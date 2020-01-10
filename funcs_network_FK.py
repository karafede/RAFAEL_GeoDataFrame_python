
import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL')
import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm
import re
import folium
from itertools import chain


# check version of modules osmn and networkx
# print(nx.__version__)  # version 2.3 ONLY!
# print(ox.__version__)  # version 1.0

class grapho:
    def __init__(self, G, G_shp, edges, stats, extended_stats):
        self.G = G
        self.G_shp = G_shp
        self.edges = edges
        self.stats = stats
        self.extended_stats = extended_stats

def graph(place_country, distance):
    # filter out some attributes
    filter = ('["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|'
              'raceway|cycleway|steps|construction"]')
    # import grapho (graphml)
    G = ox.graph_from_address(str(place_country),
                              distance=distance,
                              network_type='drive',
                              custom_filter=filter)
    # import shapefile
    G_shp = ox.gdf_from_place(place_country)
    # import shapefile with edges and nodes
    G_projected = ox.project_graph(G)
    # save street network as ESRI shapefile (includes NODES and EDGES)
    name_place_country = re.sub('[/," ",:]', '_', place_country)
    ox.save_graph_shapefile(G_projected, filename='network_' + name_place_country + '-shape')
    ox.save_graphml(G, filename = name_place_country + '.graphml')
    ox.save_gdf_shapefile(G_shp)
    ox.plot_graph(G)
    # export edges and nodes
    edges = G.edges(keys=True, data=True)
    # get stats and extended stats
    stats = ox.basic_stats(G)
    extended_stats = ox.extended_stats(G)
    return grapho(G, G_shp, edges, stats, extended_stats)




'''
basic_stats = ox.basic_stats(B)
print(basic_stats['circuity_avg'])
# ans: [1.1167206203103612]
# In this street network, the streets are 12% more circuitous than the straight-lines paths would be.

extended_stats = ox.extended_stats(B)
print(extended_stats['pagerank_max_node'])
'''

# assign weight and cost (time) to the grapho
# weight/cost assignment
def cost_assignment(file_graphml, place_country):
    # these numbers are the speeds on different type of road
    grafo = ox.load_graphml(file_graphml)
    way_dict = {
        "residential": [30, 50, 10],
        "secondary": [40, 90, 30],
        "primary": [50, 70, 20],
        "tertiary": [35, 70, 10],
        "unclassified": [40, 60, 10],
        "secondary_link": [40, 55, 30],
        "trunk": [70, 90, 40],
        "tertiary_link": [35, 50, 30],
        "primary_link": [50, 90, 40],
        "motorway_link": [80, 100, 30],
        "trunk_link": [42, 70, 40],
        "motorway": [110, 130, 40],
        "living_street": [20, 50, 30],
        "road": [30, 30, 30],
        "other": [30, 30, 30]
    }
    # weight/cost assignment
    # u and v are the start and ending point of each edge (== arco).
    for u, v, key, attr in grafo.edges(keys=True, data=True):
        print(attr["highway"])
        # select first way type from list
        if type(attr["highway"]) is list:
            # verify if the attribute field is a list (it might happen)
            attr["highway"] = str(attr["highway"][0])  # first element of the list
            print(attr["highway"], '=================')
        # verify if the attribute exists, the way type in the dictionary
        if attr["highway"] not in way_dict.keys():
            speedlist = way_dict.get("other")
            speed = speedlist[0] * 1000 / 3600
            # create a new attribute time == "cost" in the field "highway"
            attr['cost'] = attr.get("length") / speed
            print(attr.get("highway"), speedlist[0], attr.get("cost"), '^^^^^^^^^^^')
            # add the "attr_dict" to the edge file
            grafo.add_edge(u, v, key, attr_dict=attr)
            continue

        if 'maxspeed' in set(attr.keys()) and len(attr.get("maxspeed")) < 4:
            if type(attr.get("maxspeed")) is list:
                speedList = [int(i) for i in attr.get("maxspeed")]
                speed = np.mean(speedList) * 1000 / 3600
                attr['cost'] = attr.get("length") / speed
                print(attr.get("highway"), attr.get("maxspeed"), attr.get("cost"), '========')
            else:
                speed = float(attr.get("maxspeed")) * 1000 / 3600
                attr['cost'] = attr.get("length") / speed
                print(attr.get("highway"), attr.get("maxspeed"), attr.get("cost"), '°°°°°°°°°')
            grafo.add_edge(u, v, key, attr_dict=attr)
        else:  # read speed from way class dictionary
            speedlist = way_dict.get(attr["highway"])
            speed = speedlist[0] * 1000 / 3600
            attr['cost'] = attr.get("length") / speed
            print(attr.get("highway"), speedlist[0], attr.get("cost"), '-----------')
            grafo.add_edge(u, v, key, attr_dict=attr)
    # save shp file AGAIN street network as ESRI shapefile (includes NODES and EDGES and new attributes)
    name_place_country = re.sub('[/," ",:]', '_', place_country)
    ox.save_graphml(grafo, filename=name_place_country + "cost" + '.graphml')
    ox.save_graph_shapefile(grafo, filename='network_' + name_place_country + '-shape')


# select road type and save on a html folium map
def roads_type_folium(file_graphml, road_type, place_country):
    # load grapho
    grafo = ox.load_graphml(file_graphml)
    # adding a new column of edge color to gdf of the graph edges
    gdf_edges = ox.graph_to_gdfs(grafo, nodes=False, fill_edge_geometry=True)
    road_type = road_type.replace(' ', '')
    # road = gdf_edges[(gdf_edges.highway.isin( list(road_type.split (",")) ))]

    # make a dictionary for ech color
    road_color_dict = {
        "secondary": "red",
        "primary": "green",
        "tertiary": "blue",
        "motorway_link": "yellow",
        "motorway": "black"
    }

    points = []
    # prepare a base_map ###########################################################
    gen_network = gdf_edges[(gdf_edges.highway.isin(["secondary"]))]
    for i in range(len(gen_network)):
        gen_poly = ox.make_folium_polyline(edge=gen_network.iloc[i], edge_color="black", edge_width=1,
                                           edge_opacity=1, popup_attribute=None)
        points.append(gen_poly.locations)

        gen_poly_unlisted = list(chain.from_iterable(points))
        ave_lat = sum(p[0] for p in gen_poly_unlisted) / len(gen_poly_unlisted)
        ave_lon = sum(p[1] for p in gen_poly_unlisted) / len(gen_poly_unlisted)
    my_map = folium.Map(location=[ave_lat, ave_lon], zoom_start=12)
    # my_map.save("Catania_motorway.html")
    ##################################################################################

    road_type = list(road_type.split(","))
    for road in road_type:
        if road in road_color_dict.keys():
            color_road = road_color_dict.get(road)
        motorway = gdf_edges[(gdf_edges.highway.isin([road]))]
        points = []
        for i in range(len(motorway)):
            motorway_poly = ox.make_folium_polyline(edge=motorway.iloc[i], edge_color="black", edge_width=1,
                                            edge_opacity=1, popup_attribute=None)
            points.append(motorway_poly.locations)
        folium.PolyLine(points, color=color_road, weight=4, opacity=1).add_to(my_map)
        name_place_country = re.sub('[/," ",:]', '_', place_country)
        roadtype = ' '.join([str(elem) for elem in road_type])
        roads = re.sub('[/," ",:]', '_', roadtype)
        my_map.save(name_place_country + "_" + roads + ".html")

# road_type = "motorway, motorway_link"
# road_type = "secondary"
# place_country = "Catania, Italy"
# file_graphml = 'Catania__Italy.graphml'