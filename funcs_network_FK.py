
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
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
# check version of modules osmn and networkx
# print(nx.__version__)  # version 2.3 ONLY!
# print(ox.__version__)  # version 1.0

from heapq import heappush, heappop
from itertools import count
import warnings

from networkx.utils import py_random_state
from networkx.utils.decorators import not_implemented_for


'''
G=nx.Graph()
G.add_edge('a','b',cost=0.1)
G.add_edge('b','c',cost=1.5)
G.add_edge('a','c',cost=1.0)
G.add_edge('c','d',cost=22)
bet1=nx.betweenness_centrality(G, weight='cost', endpoints=False)
bet2=nx.betweenness_centrality(G, weight=None, endpoints=False)
print(bet1, bet2)
print(bet1, bet2)
'''

def _single_source_shortest_path_basic(G, s):
    S = []
    P = {}
    for v in G:
        P[v] = []
    sigma = dict.fromkeys(G, 0.0)  # sigma[v]=0 for v in G
    D = {}
    sigma[s] = 1.0
    D[s] = 0
    Q = [s]
    while Q:  # use BFS to find shortest paths
        v = Q.pop(0)
        S.append(v)
        Dv = D[v]
        sigmav = sigma[v]
        for w in G[v]:
            if w not in D:
                Q.append(w)
                D[w] = Dv + 1
            if D[w] == Dv + 1:  # this is a shortest path, count paths
                sigma[w] += sigmav
                P[w].append(v)  # predecessors
    return S, P, sigma


def _single_source_dijkstra_path_basic(G, s, weight):
    # modified from Eppstein
    S = []
    P = {}
    for v in G:
        P[v] = []
    sigma = dict.fromkeys(G, 0.0)  # sigma[v]=0 for v in G
    D = {}
    sigma[s] = 1.0
    push = heappush
    pop = heappop
    seen = {s: 0}
    c = count()
    Q = []  # use Q as heap with (distance,node id) tuples
    push(Q, (0, next(c), s, s))
    while Q:
        (dist, _, pred, v) = pop(Q)
        if v in D:
            continue  # already searched this node.
        sigma[v] += sigma[pred]  # count paths
        S.append(v)
        D[v] = dist
        for w, edgedata in G[v].items():
            vw_dist = dist + edgedata.get(weight, 1)
            if w not in D and (w not in seen or vw_dist < seen[w]):
                seen[w] = vw_dist
                push(Q, (vw_dist, next(c), v, w))
                sigma[w] = 0.0
                P[w] = [v]
            elif vw_dist == seen[w]:  # handle equal paths
                sigma[w] += sigma[v]
                P[w].append(v)
    return S, P, sigma


def _accumulate_basic(betweenness, S, P, sigma, s):
    delta = dict.fromkeys(S, 0)
    while S:
        w = S.pop()
        coeff = (1 + delta[w]) / sigma[w]
        for v in P[w]:
            delta[v] += sigma[v] * coeff
        if w != s:
            betweenness[w] += delta[w]
    return betweenness


def _accumulate_endpoints(betweenness, S, P, sigma, s):
    betweenness[s] += len(S) - 1
    delta = dict.fromkeys(S, 0)
    while S:
        w = S.pop()
        coeff = (1 + delta[w]) / sigma[w]
        for v in P[w]:
            delta[v] += sigma[v] * coeff
        if w != s:
            betweenness[w] += delta[w] + 1
    return betweenness


def _accumulate_edges(betweenness, S, P, sigma, s):
    delta = dict.fromkeys(S, 0)
    while S:
        w = S.pop()
        coeff = (1 + delta[w]) / sigma[w]
        for v in P[w]:
            c = sigma[v] * coeff
            if (v, w) not in betweenness:
                betweenness[(w, v)] += c
            else:
                betweenness[(v, w)] += c
            delta[v] += c
        if w != s:
            betweenness[w] += delta[w]
    return betweenness


def _rescale(betweenness, n, normalized, directed=False, k=None, endpoints=False):
    if normalized:
        if endpoints:
            if n < 2:
                scale = None  # no normalization
            else:
                # Scale factor should include endpoint nodes
                scale = 1 / (n * (n - 1))
        elif n <= 2:
            scale = None  # no normalization b=0 for all nodes
        else:
            scale = 1 / ((n - 1) * (n - 2))
    else:  # rescale by 2 for undirected graphs
        if not directed:
            scale = 0.5
        else:
            scale = None
    if scale is not None:
        if k is not None:
            scale = scale * n / k
        for v in betweenness:
            betweenness[v] *= scale
    return betweenness


def _rescale_e(betweenness, n, normalized, directed=False, k=None):
    if normalized:
        if n <= 1:
            scale = None  # no normalization b=0 for all nodes
        else:
            scale = 1 / (n * (n - 1))
    else:  # rescale by 2 for undirected graphs
        if not directed:
            scale = 0.5
        else:
            scale = None
    if scale is not None:
        if k is not None:
            scale = scale * n / k
        for v in betweenness:
            betweenness[v] *= scale
    return betweenness

#### ---------------------- ########
#### betweenness_centrality ########
#### ---------------------- ########

def betweenness_centrality_NEW(
    G, k=None, normalized=True, weight=None, endpoints=False, seed=None):
    betweenness = dict.fromkeys(G, 0.0)  # b[v]=0 for v in G
    if k is None:
        nodes = G
    else:
        nodes = seed.sample(G.nodes(), k)
    for s in nodes:
        # single source shortest paths
        if weight is None:  # use BFS
            S, P, sigma = _single_source_shortest_path_basic(G, s)
        else:  # use Dijkstra's algorithm
            S, P, sigma = _single_source_dijkstra_path_basic(G, s, weight)
        # accumulation
        if endpoints:
            betweenness = _accumulate_endpoints(betweenness, S, P, sigma, s)
        else:
            betweenness = _accumulate_basic(betweenness, S, P, sigma, s)
    # rescaling
    betweenness = _rescale(
        betweenness,
        len(G),
        normalized=normalized,
        directed=G.is_directed(),
        k=k,
        endpoints=endpoints,
    )
    return betweenness



### Compute betweenness centrality for edges
def edge_betweenness_centrality(G, k=None, normalized=True, weight=None, seed=None):
    betweenness = dict.fromkeys(G, 0.0)  # b[v]=0 for v in G
    # b[e]=0 for e in G.edges()
    betweenness.update(dict.fromkeys(G.edges(), 0.0))
    if k is None:
        nodes = G
    else:
        nodes = seed.sample(G.nodes(), k)
    for s in nodes:
        # single source shortest paths
        if weight is None:  # use BFS
            S, P, sigma = _single_source_shortest_path_basic(G, s)
        else:  # use Dijkstra's algorithm
            S, P, sigma = _single_source_dijkstra_path_basic(G, s, weight)
        # accumulation
        betweenness = _accumulate_edges(betweenness, S, P, sigma, s)
    # rescaling
    for n in G:  # remove nodes to only return edges
        del betweenness[n]
    betweenness = _rescale_e(
        betweenness, len(G), normalized=normalized, directed=G.is_directed()
    )
    return betweenness



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
    #### use this to calculate CENTRALITY
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

    #### use this for MAP-MATCHING
    filter = (
        '["highway"!~"living_street|abandoned|steps|construction|service|pedestrian|'
        'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path|footway"]')

    ## for "sottorete"
    # filter = (
    #     '["highway"!~"living_street|abandoned|steps|construction|service|pedestrian|'
    #     'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path|footway|'
    #     'tertiary|residential|tertiary_link|service|secondary_link|unclassified"]')
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

    ### ox.get_edge_colors_by_attr....
    # make a dictionary for ech color
    road_color_dict = {
        "secondary": "red",
        "primary": "green",
        "tertiary": "blue",
        "motorway_link": "yellow",
        "motorway": "black",
        "trunk": "orange",
        "trunk_link": "orange",
        "residential": "orange",
        "unclassified": "brown",
        "tertiary_link": "orange",
        "secondary_link": "orange",
        "service": "orange"
     }

    # road_color_dict = {
    #    "secondary": "lightgrey",
    #    "primary": "lightgrey",
    #    "tertiary": "lightgrey",
    #    "motorway_link": "black",
    #    "motorway": "black",
    #    "trunk": "black",
    #    "trunk_link": "black",
    #    "residential": "lightgrey",
    #    "unclassified": "lightgrey",
    #    "tertiary_link": "lightgrey",
    #    "secondary_link": "lightgrey",
    #    "service": "lightgrey"
    #}
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
    ##################################################################################

    # add nodes to my_map
    # for i in range(len(gdf_nodes)):
    #     folium.CircleMarker(location=[gdf_nodes.y.iloc[i], gdf_nodes.x.iloc[i]],
    #                         popup=gdf_nodes.osmid.iloc[i],
    #                         radius=2,
    #                         color="red",
    #                         fill=True,
    #                         fill_color="yellow",
    #                         fill_opacity=0.6).add_to(my_map)

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
    my_map.save("Fisciano_roads.html")
    return my_map


######################
# edge centrality ####
######################
# Betweenness centrality was devised as a general measure of centrality
# BETWEENNESS CENTRALITY: Compute the shortest-path betweenness centrality for nodes
## https://github.com/gboeing/networkx/blob/master/networkx/algorithms/centrality/betweenness.py
## https://stackoverflow.com/questions/56028683/how-to-add-edge-length-as-a-weight-in-betweeness-centrality-using-osmnx-networkx


def centrality(file_graphml, place_country, bc=False, cc=False): #road_type
    # load grapho

    os.chdir('D:\\ENEA_CAS_WORK\\Catania_RAFAEL\\viasat_data')
    file_graphml = 'Catania_VIASAT_Italy_for_CENTRALITY_cost.graphml'

    grafo = ox.load_graphml(file_graphml)
    G = ox.load_graphml(file_graphml)
    # ox.plot_graph(grafo)
    ### replace "length" values with "cost" values #####
    for u, v, key, attr in grafo.edges(keys=True, data=True):
        print(attr)
    #     print(attr["length"])
    #     print(attr["cost"])
    #     attr['length'] = attr.get("VIASAT_cost")
    #     grafo.add_edge(u, v, key, attr_dict=attr)

    # make attr "cost" as float!!! (units of "cost" are SECONDS!)
    for u, v, key, attr in grafo.edges(keys=True, data=True):
        print(attr)
        print(attr.get("cost"))
        if len(attr['cost']) > 0:
            attr['cost'] = float(attr.get("cost"))
            attr['VIASAT_cost'] = float(attr.get("VIASAT_cost"))
    ## geth a simple 'Graph' (only nodes)
    grafo = nx.Graph(grafo)

    if cc:
        c_name = "close_centrality"
        edge_centrality = nx.closeness_centrality(nx.line_graph(grafo))
        ## make a dataframe
        DF_edge_centrality = pd.Series(edge_centrality).reset_index()
        DF_edge_centrality.columns = ['u', 'v', 'centrality']
        ## save centrality into a .csv file
        DF_edge_centrality.to_csv("D:\\ENEA_CAS_WORK\\Catania_RAFAEL\\viasat_data\\close_centrality_Catania_AUGUST.csv")  ## only one node..

        ########################################################
        ##### build the map ####################################

        gdf_edges = ox.graph_to_gdfs(G, nodes=False, fill_edge_geometry=True)
        DF_edge_centrality = pd.merge(DF_edge_centrality, gdf_edges, on=['u', 'v'], how='left')
        ## remove line with na value
        DF_edge_centrality = DF_edge_centrality[DF_edge_centrality['geometry'].notna()]
        DF_edge_centrality = gpd.GeoDataFrame(DF_edge_centrality)
        DF_edge_centrality.drop_duplicates(['u', 'v'], inplace=True)
        # DF_edge_centrality.plot()
    if bc:
        c_name = "btw_centrality"
        edge_centrality = nx.betweenness_centrality(grafo, weight= 'VIASAT_cost', endpoints=False)  ## weight = None; weight = 'cost'
        # ev = [edge_centrality[edge + (0,)] for edge in grafo.edges()]  ### this is a list
        # ev = [edge_centrality[edge] for edge in grafo.edges()]   ### this is a list

        ## https://stackoverflow.com/questions/44012099/creating-a-dataframe-from-a-dict-where-keys-are-tuples
        ## make a dataframe
        DF_edge_centrality = pd.Series(edge_centrality).reset_index()
        DF_edge_centrality.columns = ['u', 'centrality']
        ## save centrality into a .csv file
        DF_edge_centrality.to_csv("D:\\ENEA_CAS_WORK\\Catania_RAFAEL\\viasat_data\\btw_centrality_Catania_AUGUST_VIASAT_cost.csv")  ## only one node..

        ########################################################
        ##### build the map ####################################

        gdf_edges = ox.graph_to_gdfs(G, nodes=False, fill_edge_geometry=True)
        DF_edge_centrality = pd.merge(DF_edge_centrality, gdf_edges, on=['u'], how='left')
        ## remove line with na value
        DF_edge_centrality = DF_edge_centrality[DF_edge_centrality['geometry'].notna()]
        DF_edge_centrality = gpd.GeoDataFrame(DF_edge_centrality)
        DF_edge_centrality.drop_duplicates(['u', 'v'], inplace=True)
        # DF_edge_centrality.plot()

    ## rescale all data by an arbitrary number
    DF_edge_centrality["scales"] = round(((DF_edge_centrality.centrality / max(DF_edge_centrality.centrality)) * 3) + 0.1, 1)

    # add colors based on 'centrality'
    vmin = min(DF_edge_centrality.scales)
    vmax = max(DF_edge_centrality.scales)
    # Try to map values to colors in hex
    norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlOrRd)  # scales of Reds (or "coolwarm" , "bwr", °cool°)  gist_yarg --> grey to black, YlOrRd
    DF_edge_centrality['color'] = DF_edge_centrality['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

    ## Normalize to 1
    DF_edge_centrality["centrality"] = round(DF_edge_centrality["scales"] / max(DF_edge_centrality["scales"]), 1)

    ################################################################################
    # create basemap CATANIA
    ave_LAT = 37.510284
    ave_LON = 15.092042
    my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
    #################################################################################

    ########################################
    # make a colored map  ##################
    ########################################
    from folium_stuff_FK_map_matching import plot_graph_folium_FK
    my_map = plot_graph_folium_FK(DF_edge_centrality, graph_map=None, popup_attribute=None,
                                  zoom=15, fit_bounds=True, edge_width=3, edge_opacity=0.5)
    style = {'fillColor': '#00000000', 'color': '#00000000'}
    folium.GeoJson(
        # data to plot
        DF_edge_centrality[['u', 'v', 'centrality', 'scales', 'geometry']].to_json(),
        show=True,
        style_function=lambda x: style,
        highlight_function=lambda x: {'weight': 3,
                                      'color': 'blue',
                                      'fillOpacity': 0.6
                                      },
        # fields to show
        tooltip=folium.features.GeoJsonTooltip(
            fields=['u', 'v', 'centrality']
        ),
    ).add_to(my_map)
    folium.TileLayer('cartodbdark_matter').add_to(my_map)
    folium.LayerControl().add_to(my_map)
    path = 'D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/'
    name_place_country = re.sub('[/," ",:]', '_', place_country)
    my_map.save(path + c_name + "_" + name_place_country + ".html")


    #################################################################################
    #################################################################################
    #################################################################################
    #################################################################################
    #################################################################################
    #################################################################################
    #################################################################################
    #################################################################################



# road_type = "motorway, motorway_link"
# # road_type = "secondary"
# # road_type = "motorway, motorway_link, secondary, primary, tertiary"
# place_country = "Catania, Italy"
# # file_graphml = 'Catania__Italy.graphml'  # to be used when run cost assignment and shortest path calculation (no saving!!)
# file_graphml = 'Catania__Italy_cost.graphml' # to be used when run folium map classification and centrality
# distance = 20000
# bc=False
# cc=True