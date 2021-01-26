
import os
os.chdir('D:\\ENEA_CAS_WORK\\Catania_RAFAEL')
import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm
import matplotlib.colors as colors
import re
import folium
from itertools import chain
from folium_stuff_FK import make_folium_polyline_FK
from folium_stuff_FK import plot_graph_folium_FK
from osgeo import ogr
import geopandas as gpd

# check version of modules osmn and networkx
# print(nx.__version__)  # version 2.3 ONLY!
# print(ox.__version__)  # version 1.0

##################################################
###### load and save grapho ######################
##################################################
# class grapho:
#     def __init__(self, G, G_shp, edges):  #stats, extended_stats
#         self.G = G
#         self.G_shp = G_shp
#         self.edges = edges
#         # self.stats = stats
#         # self.extended_stats = extended_stats

def graph(place_country, distance):  # filter
    # filter out some attributes
    # filter = ('["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|'
    #           'raceway|cycleway|steps|construction"]')
    # filter = (
    #     '["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|'
    #     'raceway|cycleway|steps|construction|primary|secondary|tertiary"]')
    # filter = (
    #     '["highway"!~"living_street|abandoned|footway|pedestrian|raceway|cycleway|steps|construction|'
    #     'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path"]')
    # filter = (
    #     '["highway"!~"living_street|abandoned|steps|construction|'
    #     'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path"]')
    filter = (
        '["highway"!~"living_street|abandoned|steps|construction|service|pedestrian|'
        'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path|footway"]')
    # filter = filter

    # import grapho (graphml)
    G = ox.graph_from_address(str(place_country),
                              distance=distance,
                              network_type='drive', custom_filter=filter)

    # import shapefile
    # G_shp = ox.gdf_from_place(place_country)
    # import shapefile with edges and nodes
    # as the data is in WGS84 format, we might first want to reproject our data into metric system
    # so that our map looks better with the projection of the graph data in UTM format
    # G_projected = ox.project_graph(G)
    # save street network as ESRI shapefile (includes NODES and EDGES)
    name_place_country = re.sub('[/," ",:]', '_', place_country)
    # ox.save_graph_shapefile(G_projected, filename='network_' + name_place_country + '-shape')
    ox.save_graphml(G, filename = name_place_country + '.graphml')
    # ox.save_gdf_shapefile(G_shp)

    # ox.plot_graph(G)

    # export edges and nodes
    # edges = G.edges(keys=True, data=True)
    # get stats and extended stats
    # stats = ox.basic_stats(G_projected)
    # extended_stats = ox.extended_stats(G_projected, ecc=True, bc=True, cc=True) # it takes very long time...
    # extended_stats = ox.extended_stats(G_projected)
    # return grapho(G, G_shp, edges) #stats, extended_stats

####################################################
# assign weight and cost (time) to the grapho ######
# weight/cost assignment ###########################
####################################################
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
    ox.save_graphml(grafo, filename=name_place_country + "_cost" + '.graphml')  # when I save, the field "cost" becomes a string...wrong!
    # ox.save_graphml(grafo, filename=name_place_country + '.graphml')
    # ox.save_graph_shapefile(grafo, filename='network_' + name_place_country + '-shape')


#####################################################
# select road type and save on a html folium map ####
#####################################################
def roads_type_folium(file_graphml, road_type, place_country):
    # load grapho
    grafo = ox.load_graphml(file_graphml)
    # ox.plot_graph(grafo)
    # adding a new column of edge color to gdf of the graph edges
    gdf_edges = ox.graph_to_gdfs(grafo, nodes=False, fill_edge_geometry=True)
    gdf_nodes = ox.graph_to_gdfs(grafo, edges=False)
    # road_type = road_type.replace(' ', '')
    # road_type = ['motorway', 'motorway_link', 'secondary', 'primary', 'tertiary', 'residential', 'unclassified'
    #     , 'trunk', 'trunk_link', 'tertiary_link', 'secondary_link']
    # road = gdf_edges[(gdf_edges.highway.isin( list(road_type.split (",")) ))]

    # ox.get_edge_colors_by_attr....
    # make a dictionary for ech color
    # road_color_dict = {
    #     "secondary": "red",
    #     "primary": "green",
    #     "tertiary": "blue",
    #     "motorway_link": "yellow",
    #     "motorway": "black",
    #     "trunk": "orange",
    #     "trunk_link": "orange",
    #     "residential": "orange",
    #     "unclassified": "brown",
    #     "tertiary_link": "orange",
    #     "secondary_link": "orange",
    #     "service": "orange"
    # }

    road_color_dict = {
        "secondary": "lightgrey",
        "primary": "lightgrey",
        "tertiary": "lightgrey",
        "motorway_link": "black",
        "motorway": "black",
        "trunk": "black",
        "trunk_link": "black",
        "residential": "lightgrey",
        "unclassified": "lightgrey",
        "tertiary_link": "lightgrey",
        "secondary_link": "lightgrey",
        "service": "lightgrey"
    }
    points = []
    # prepare a base_map ###########################################################
    gdf_edges_copy = gdf_edges
    gdf_edges_copy = gdf_edges_copy['highway'].str.replace(r"\(.*\)", "")
    gdf_edges.highway = gdf_edges_copy

    gen_network = gdf_edges[gdf_edges.highway.isin(road_type)]
    # gen_network = gdf_edges[(gdf_edges.highway.isin([road_type.split(",")[0]]))]
    # gen_network = gdf_edges[(gdf_edges.highway.isin(["secondary"]))]
    # calculate Average Latitude and average Longitude
    for i in range(len(gen_network)):
        gen_poly = ox.make_folium_polyline(edge=gen_network.iloc[i], edge_color="black", edge_width=1,
                                           edge_opacity=1, popup_attribute=None)
        points.append(gen_poly.locations)
        gen_poly_unlisted = list(chain.from_iterable(points))
        ave_lat = sum(p[0] for p in gen_poly_unlisted) / len(gen_poly_unlisted)
        ave_lon = sum(p[1] for p in gen_poly_unlisted) / len(gen_poly_unlisted)
    my_map = folium.Map(location=[ave_lat, ave_lon], zoom_start=13, tiles='cartodbpositron') # tiles='cartodbpositron'
    # my_map.save("Catania_motorway.html")
    # my_map.save("Catania_partial.html")
    ##################################################################################

    # add nodes to my_map
    for i in range(len(gdf_nodes)):
        folium.CircleMarker(location=[gdf_nodes.y.iloc[i], gdf_nodes.x.iloc[i]],
                            popup=gdf_nodes.osmid.iloc[i],
                            radius=2,
                            color="red",
                            fill=True,
                            fill_color="yellow",
                            fill_opacity=0.6).add_to(my_map)

    # add edges
    # road_type = list(road_type.split(","))
    for road in road_type:
        print(road)
        if road in road_color_dict.keys():
            print("yes")
            color_road = road_color_dict.get(road)
        motorway = gdf_edges[(gdf_edges.highway.isin([road]))]
        points = []
        if len(motorway)!=0:
            for i in range(len(motorway)):
                motorway_poly = ox.make_folium_polyline(edge=motorway.iloc[i], edge_color="black", edge_width=1,
                                                edge_opacity=1, popup_attribute=None)
                points.append(motorway_poly.locations)
            folium.PolyLine(points, color=color_road, weight=2, opacity=1).add_to(my_map)
            name_place_country = re.sub('[/," ",:]', '_', place_country)
            # roadtype = ' '.join([str(elem) for elem in road_type])
            # roads = re.sub('[/," ",:]', '_', roadtype)
    # my_map.save(name_place_country + "_" + "partial" + ".html")
    # my_map.save("Catania_partial.html")
    my_map.save("Fisciano_partial.html")
    return my_map


######################
# edge centrality ####
######################
# Betweenness centrality was devised as a general measure of centrality
# BETWEENNESS CENTRALITY: Compute the shortest-path betweenness centrality for nodes

def centrality(file_graphml, place_country, bc=False, cc=False): #road_type
    # load grapho
    grafo = ox.load_graphml(file_graphml)
    #### replace "length" values with "cost" values #####
    # for u, v, key, attr in grafo.edges(keys=True, data=True):
    #     # print(attr)
    #     print(attr["length"])
    #     attr['length'] = attr.get("cost")
    #     # grafo.add_edge(u, v, key, attr_dict=attr)
    #     grafo.add_edge(u, v, key)
    # ox.extended_stats(grafo, bc=True)
    if cc:
        c_name = "close_centrality"
        edge_centrality = nx.closeness_centrality(nx.line_graph(grafo))
    if bc:
        c_name = "btw_centrality"
        edge_centrality = nx.betweenness_centrality(nx.line_graph(grafo), weight='pippo')  # not working!!!!!
    ev = [edge_centrality[edge + (0,)] for edge in grafo.edges()]
    # color scale converted to list of colors for graph edges
    norm = colors.Normalize(vmin=min(ev)*0.8, vmax=max(ev))
    # cividis, viridis, YlGn  (good colormaps
    # 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds',
    #             'YlOrBr', 'YlOrRd', 'OrRd', 'PuRd', 'RdPu', 'BuPu',
    #             'GnBu', 'PuBu', 'YlGnBu', 'PuBuGn', 'BuGn', 'YlGn',
    #             'viridis', 'plasma', 'inferno', 'magma', 'cividis']
    cmap = cm.ScalarMappable(norm=norm, cmap=cm.YlGn)
    ec = [cmap.to_rgba(cl) for cl in ev]
    fig, ax = ox.plot_graph(grafo, bgcolor='k', axis_off=True, node_size=0, node_color='w',
                            node_edgecolor='gray', node_zorder=2,
                            edge_color=ec, edge_linewidth=1.5, edge_alpha=1)

    gdf_edges = ox.graph_to_gdfs(grafo, nodes=False, fill_edge_geometry=True)
    gdf_edges['edge_color'] = ec

    my_map = plot_graph_folium_FK(gdf_edges, graph_map=None, popup_attribute=None,
                            zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1) #tiles='cartodbpositron'
    name_place_country = re.sub('[/," ",:]', '_', place_country)
    # road_type = road_type.replace(' ', '')
    # road_type = list(road_type.split(","))
    # roadtype = ' '.join([str(elem) for elem in road_type])
    # roads = re.sub('[/," ",:]', '_', roadtype)
    # my_map.save(c_name + "_" + roads + "_" + name_place_country + ".html")
    my_map.save(c_name + "_" + name_place_country + ".html")

#############################################################
#############################################################


# road_type = "motorway, motorway_link"
# # road_type = "secondary"
# # road_type = "motorway, motorway_link, secondary, primary, tertiary"
# place_country = "Catania, Italy"
# # file_graphml = 'Catania__Italy.graphml'  # to be used when run cost assignment and shortest path calculation (no saving!!)
# file_graphml = 'Catania__Italy_cost.graphml' # to be used when run folium map classification and centrality
# distance = 20000
# bc=False
# cc=True