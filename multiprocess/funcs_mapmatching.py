
from math import radians, cos, sin, asin, sqrt
import networkx as nx
import math

##  Define distance between GPS track (viasat measurements) and node
def great_circle_track_node(u,nodes_dict, viasat, gdf_nodes):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # u_track = u_dict.get(u)[0][2]
    u_track = nodes_dict.get(u)[0][2]
    coords_track = viasat[viasat.ID == u_track].values.tolist()
    lon_track = coords_track[0][2]
    lat_track = coords_track[0][3]
    coords_u = gdf_nodes[gdf_nodes.index == u][['x', 'y']].values.tolist()
    lon_u = coords_u[0][0]
    lat_u = coords_u[0][1]
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon_track, lat_track, lon_u, lat_u])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r  # Kilometers

## define distance between two GPS tracks (viasat measurements)
def great_circle_track(u, nodes_dict, viasat):
    # Calculate the great circle distance between two points from viasat data (progressive)
    u_track = nodes_dict.get(u)[0][2]
    v_track = u_track + 1
    if v_track <= max(viasat.ID):
        distance = int((viasat[viasat['ID'] == v_track]).progressive) - int(
            (viasat[viasat['ID'] == u_track]).progressive)
        distance = distance / 1000  # in Km
    else:
        distance = 0
    return distance


## Gaussian distribution of all NODES close to Viasat measurements.
def emission_prob(u, SIGMA_Z):
    c = 1 / (SIGMA_Z * math.sqrt(2 * math.pi))
    return 1 * math.exp(-0.5*(great_circle_track_node(u)/SIGMA_Z)**2)
    # return great_circle_track_node(u)/SIGMA_Z


## Transition probability (probability that the distance u-->v is from the mesasurements's distances at nodes u and v
def transition_prob(u, v, grafo):
    BETA = 1
    c = 1 / BETA
    # Calculating route distance is expensive.
    # We will discuss how to reduce the number of calls to this function later.
    # distance on the route
    delta = abs(nx.shortest_path_length(grafo, u, v, weight='length') / 1000 -
                great_circle_track(u))  # in Kilometers
    return c * math.exp(-delta)
