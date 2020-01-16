#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  23 2019

@author: fkaragul
"""
# https://geoffboeing.com/2016/11/osmnx-python-street-networks/
# https://medium.com/@bobhaffner/osmnx-intro-and-routing-1fd744ba23d8

import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm
import matplotlib.colors as colors
import folium
from itertools import chain
from colour import Color

import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL')
# os.chdir('C:\\python\\projects\\giraffe\\viasat_data\\reti_VALENTI\\gv_net')
os.getcwd()

#load Graph
Catania = ox.load_graphml('Catania__Italy.graphml')
ox.plot_graph(Catania)


# way class: mean, max, min  (make a DICTIONARY) these are the "keys"
# these numbers are the speeds on different type of road
way_dict={
        "residential" : [ 30 , 50 , 10 ],
        "secondary" :   [ 40 , 90 , 30 ],
        "primary" :     [ 50 , 70 , 20 ],
        "tertiary" :    [ 35 , 70 , 10 ],
        "unclassified" :[ 40 , 60 , 10 ],
        "secondary_link": [ 40 , 55 , 30 ],
        "trunk" :       [ 70 , 90 , 40 ],
        "tertiary_link": [ 35 , 50 , 30 ],
        "primary_link" : [ 50 , 90 , 40 ],
        "motorway_link": [ 80 , 100 , 30 ],
        "trunk_link" :   [ 42 , 70 , 40 ],
        "motorway" :     [ 110 , 130 , 40 ],
        "living_street": [ 20 , 50 , 30 ],
        "road" :         [ 30 , 30 , 30 ],
        "other" :         [ 30 , 30 , 30 ]
        }


print(type(way_dict))
# execute some operation to create a "highway" field if this does not exist in the "edge" file
edge_file = Catania.edges(keys=True,data=True)
print(type(edge_file))

# weight/cost assignment
# u and v are the start and ending point of each edge (== arco).
for u,v,key,attr in Catania.edges(keys=True,data=True):
    print(attr["highway"])
    # select first way type from list
    if type(attr["highway"]) is list:
        # verify if the attribute field is a list (it might happen)
       attr["highway"]=str(attr["highway"][0])  # first element of the list
       print(attr["highway"],'=================')
    # verify if the attribute exists, the way type in the dictionary
    if attr["highway"] not in way_dict.keys():
       speedlist=way_dict.get("other")
       speed=speedlist[0]*1000/3600
       # create a new attribute time == "cost" in the field "highway"
       attr['cost']=attr.get("length")/speed
       print(attr.get("highway"), speedlist[0], attr.get("cost"),'^^^^^^^^^^^')
    # add the "attr_dict" to the edge file
       Catania.add_edge(u,v,key,attr_dict=attr)
       continue

    if 'maxspeed' in set(attr.keys()) and len(attr.get("maxspeed"))<4:
        if type(attr.get("maxspeed")) is list:
            speedList = [int(i) for i in attr.get("maxspeed")]
            speed=np.mean(speedList)*1000/3600
            attr['cost']=attr.get("length")/speed
            print(attr.get("highway"), attr.get("maxspeed"), attr.get("cost"),'========')
        else:
            speed=float(attr.get("maxspeed"))*1000/3600
            attr['cost']=attr.get("length")/speed
            print(attr.get("highway"), attr.get("maxspeed"), attr.get("cost"),'°°°°°°°°°')
        Catania.add_edge(u,v,key,attr_dict=attr)
    else:#read speed from way class dictionary
        speedlist = way_dict.get(attr["highway"])
        speed=speedlist[0]*1000/3600
        attr['cost']=attr.get("length")/speed
        print(attr.get("highway"), speedlist[0], attr.get("cost"),'-----------')
        Catania.add_edge(u,v,key,attr_dict=attr)

# highlight only motorway
# ec = ['r' if data['highway']== "motorway" else 'b' for u, v, key, data in Catania.edges(keys=True, data=True)]
# ox.plot_graph(Catania, node_size=0, edge_color=ec)

# adding a new column of edge color to gdf of the graph edges
gdf_edges = ox.graph_to_gdfs(Catania, nodes=False,  fill_edge_geometry=True)
gdf_nodes = ox.graph_to_gdfs(Catania, edges = False)
# gdf_edges['edge_color'] = ec
# road_type = "motorway"
road_type = "motorway, motorway_link"
# road_type = "motorway, motorway_link, secondary, primary, tertiary"
# road_type = "secondary"
# road_type = "motorway, motorway_link"
road_type = road_type.replace(' ', '')

# make a dictionary for ech color
road_color_dict={
        "secondary" :    "red",
        "primary" :      "green",
        "tertiary" :     "blue",
        "motorway_link": "yellow",
        "motorway" :     "black"
        }

points = []
# prepare a base_map ###########################################################
gen_network = gdf_edges[(gdf_edges.highway.isin(["motorway"]))]
for i in range(len(gen_network)):
    gen_poly = ox.make_folium_polyline(edge=gen_network.iloc[i], edge_color="black", edge_width=1,
                                       edge_opacity=1, popup_attribute=None)
    points.append(gen_poly.locations)

    gen_poly_unlisted = list(chain.from_iterable(points))
    ave_lat = sum(p[0] for p in gen_poly_unlisted) / len(gen_poly_unlisted)
    ave_lon = sum(p[1] for p in gen_poly_unlisted) / len(gen_poly_unlisted)
my_map = folium.Map(location=[ave_lat, ave_lon], zoom_start=12)
##################################################################################

# add nodes to my_map
for i in range(len(gdf_nodes)):
    folium.CircleMarker(location=[gdf_nodes.y.iloc[i], gdf_nodes.x.iloc[i]],
                                                 popup=gdf_nodes.osmid.iloc[i],
                                                 radius=5,
                                                 color="red",
                                                 fill=True,
                                                 fill_color="yellow",
                                                 fill_opacity=0.6).add_to(my_map)


# add edges
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
    folium.PolyLine(points, color=color_road, weight=5, opacity=1).add_to(my_map)
    my_map.save("Catania_motorway.html")

'''
# plot all the networl on the map
AAA = ox.plot_graph_folium(Catania, graph_map=None, popup_attribute=None, tiles='cartodbpositron', zoom=1,
                  fit_bounds=True, edge_width=3, edge_opacity=1)
AAA.save("BBB.html")
'''

#######################################################
#######################################################
#######################################################

import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm
import matplotlib.colors as colors
import folium
from itertools import chain
from colour import Color
from folium_stuff_FK import make_folium_polyline_FK
from folium_stuff_FK import plot_graph_folium_FK
from folium_stuff_FK import graph_to_gdfs_FK


import os
os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL')
os.getcwd()

# # node closeness centrality
file_graphml = 'Catania__Italy_cost.graphml'
grafo = ox.load_graphml(file_graphml)
# node_centrality = nx.closeness_centrality(grafo)
# df = pd.DataFrame(data=pd.Series(node_centrality).sort_values(), columns=['cc'])
# df['colors'] = ox.get_colors(n=len(df), cmap='inferno', start=0.2)
# df = df.reindex(grafo.nodes())
# nc = df['colors'].tolist()
# fig, ax = ox.plot_graph(grafo, bgcolor='k', node_size=30, node_color=nc, node_edgecolor='none', node_zorder=2,
#                         edge_color='#555555', edge_linewidth=1.5, edge_alpha=1)

### edge centrality
### OSMnx automatically uses edge lengths as the weight when calculating betweenness centrality. ###
# convert MultiDiGraph into simple Graph
# grafo = nx.DiGraph(grafo)
# edge_centrality = nx.closeness_centrality(nx.line_graph(grafo))
edge_centrality = nx.betweenness_centrality(nx.line_graph(grafo), weight='length')
edge_centrality = nx.betweenness_centrality(nx.line_graph(grafo))
ev = [edge_centrality[edge + (0,)] for edge in grafo.edges()]
# ev = [edge_centrality[edge] for edge in grafo.edges()]
# color scale converted to list of colors for graph edges
norm = colors.Normalize(vmin=min(ev)*0.8, vmax=max(ev))
### color scales
# inferno, cividis, viridis, YlGn  (good colormaps
# 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds',
#             'YlOrBr', 'YlOrRd', 'OrRd', 'PuRd', 'RdPu', 'BuPu',
#             'GnBu', 'PuBu', 'YlGnBu', 'PuBuGn', 'BuGn', 'YlGn',
#             'viridis', 'plasma', 'inferno', 'magma', 'cividis']
cmap = cm.ScalarMappable(norm=norm, cmap=cm.YlGn)
ec = [cmap.to_rgba(cl) for cl in ev]


# row = gdf_edges.iloc[3]
# row = row["edge_color"][0:3]
# # C = Color(hsl=(0.865006, 0.316822, 0.226055))
# row = Color(hsl=row)
# row = "%s" % row

# https://pypi.org/project/colour/

# color the edges in the original graph with closeness centralities in the line graph
fig, ax = ox.plot_graph(grafo, bgcolor='k', axis_off=True, node_size=0, node_color='w',
                        node_edgecolor='gray', node_zorder=2,
                        edge_color=ec, edge_linewidth=1.5, edge_alpha=1)

grafo_edges = grafo.edges(keys=True,data=True)
# grafo_edges = grafo.edges(data=True)
gdf_edges = ox.graph_to_gdfs(grafo, nodes=False, fill_edge_geometry=True)
# gdf_edges = graph_to_gdfs_FK(grafo, nodes=False, fill_edge_geometry=True)
gdf_edges['edge_color'] = ec
# gdf_edges.crs = {'init' :'epsg:4326'}
# gdf_edges.plot()

# for i in range(len(gdf_edges)):
#     make_folium_polyline_FK(edge = gdf_edges.iloc[i],
#                             edge_width = 4, edge_opacity = 1, popup_attribute=None).add_to(m)

AAA = plot_graph_folium_FK(gdf_edges, graph_map=None, popup_attribute=None,
                        tiles='cartodbpositron', zoom=1, fit_bounds=True, edge_width=4, edge_opacity=1)
AAA.save("prova_centrality.html")


for u, v, key, attr in grafo.edges(keys=True, data=True):
    print(attr)
    print(attr["length"])
    attr['length'] = attr.get("cost")
    # grafo.add_edge(u, v, key, attr_dict=attr)
    grafo.add_edge(u, v, key)

ox.extended_stats(grafo, bc=True)
ox.extended_stats(grafo, ecc=True, bc=True, cc=True)
#######################################################
#######################################################
#######################################################


from_n=np.random.choice(Catania.nodes)
to_n=np.random.choice(Catania.nodes)
# calculate the shortest path between two points...meters )this is a list of nodes based on the shortest time
route = nx.shortest_path(Catania,from_n,to_n, weight='cost')
# calculate the length of the route (this is a time)
lr = nx.shortest_path_length(Catania, from_n,to_n, weight='cost')
print(lr)
route = nx.shortest_path(Catania,from_n,to_n,weight='length')
# create pairs of edge for the shortest route
path_edges = list(zip(route,route[1:]))
len(path_edges)

lunghezza=[]

for l in path_edges:
  lunghezza.append(Catania [l[0]] [l[1]] [0]['length'])  # get only the length for each arch between 2 path edges, [0] it the key = 0
print("km:{0:.3f} h:{1:.3f} vm:{2:.0f}".format(sum(lunghezza)/1000, lr/3600, sum(lunghezza)/1000/lr*3600))  # units == km

route1 = nx.dijkstra_path(Catania,from_n,to_n,weight='cost')
lr1 = nx.dijkstra_path_length(Catania, from_n,to_n,weight='cost')

#route2 = nx.astar_path(Bracciano,from_n,to_n,weight='length')
#lr2=nx.astar_path_length(Bracciano, from_n,to_n,weight='length')

ox.plot_graph_route(Catania, route, route_color='green', fig_height=12, fig_width=12)

# make an interactive map
path = ox.plot_route_folium(Catania, route, route_color='green')
path.save('Catania_min_path.html')
# print(len(route), type(Bracciano))

# save shp file AGAIN street network as ESRI shapefile (includes NODES and EDGES and new attributes)
ox.save_graph_shapefile(Catania, filename='networkCatania-shape')

#print(way_dict["residential"][2])

# get way_dictionary with speed osmnx
way_dict={}
for u,v,key,attr in Catania.edges(keys=True,data=True):
    if 'maxspeed' in set(attr.keys()) and type(attr.get("highway")) is not list:
        way=attr.get("highway")
        speed=None
        if type(attr.get("maxspeed")) is not list:
           speed=int(attr.get("maxspeed"))
        else:
            speedList = [int(i) for i in attr.get("maxspeed")]
            speed=np.mean(speedList)
        if way in way_dict.keys():
           lista=[]
           lista=way_dict.get(way)
           lista.append(speed)
           way_dict[way]=lista
        else:
            lista=[]
            lista.append(speed)
            way_dict[way]=lista
        print(attr.get("highway"), attr.get("maxspeed"))
way_dict['other']=[ 20 , 20 , 20 ]
for i in way_dict.keys():
    print(i,': [',int(np.mean(way_dict.get(i))), ',', int(np.max(way_dict.get(i))),',', int(np.min(way_dict.get(i))),']')     

print('num_nodi',Catania.number_of_nodes())
print('num_archi',Catania.number_of_edges())

#print((attr_edge))

# print edges
edges = ox.graph_to_gdfs(Catania, nodes=False, edges=True)

for i in edges['highway']:
    if i is not list:
        print(edges['highway'])

# calculate basic and extended network stats, merge them together, and display
stats = ox.basic_stats(Catania)
type(Catania)
extended_stats = ox.extended_stats(Catania, ecc=True, bc=True, cc=True)


# get a color for each node
def get_color_list(n, color_map='plasma', start=0, end=1):
    return [cm.get_cmap(color_map)(x) for x in np.linspace(start, end, n)]

def get_node_colors_by_stat(Catania, data, start=0, end=1):
    df = pd.DataFrame(data=pd.Series(data).sort_values(), columns=['value'])
    df['colors'] = get_color_list(len(df), start=start, end=end)
    df = df.reindex(Catania.nodes())
    return df['colors'].tolist()

nc = get_node_colors_by_stat(Catania, data=extended_stats['betweenness_centrality'])
fig, ax = ox.plot_graph(Catania, node_color=nc, node_edgecolor='gray', node_size=20, node_zorder=2)


