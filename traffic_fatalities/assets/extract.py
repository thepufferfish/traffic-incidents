import os
import requests
import pandas as pd
# import numpy as np
# import networkx as nx
import osmnx as ox
# import matplotlib.pyplot as plt
# import base64
# from io import BytesIO
from dagster import asset, multi_asset, AssetExecutionContext, AssetIn, AssetOut, MaterializeResult, MetadataValue, Output
from traffic_fatalities.partitions import nodes_partitions_def
from traffic_fatalities.utils import get_bounding_box


@multi_asset(
    outs={
        "osm_nodes": AssetOut(),
        "osm_edges": AssetOut()
    }
)
def fetch_openstreetmaps(context: AssetExecutionContext):
    G = ox.graph_from_place('Sacramento, California, USA', network_type='drive')
    # fig, ax = ox.plot_graph(G, show=False, close=False)
    # buffer = BytesIO()
    # plt.savefig(buffer, format='png')
    # plt.close()
    # image_data = base64.b64encode(buffer.getvalue())
    # md_content = f'![img](data:image/png;base64,{image_data.decode()})'

    # yield MaterializeResult(metadata={'map': MetadataValue.md(md_content)})  

    nodes, edges = ox.graph_to_gdfs(G)
    node_ids = nodes.index.tolist()
    node_ids = [str(x) for x in node_ids]
    context.instance.add_dynamic_partitions(nodes_partitions_def.name, partition_keys=node_ids)
    yield Output(nodes, output_name="osm_nodes")
    yield Output(edges, output_name="osm_edges")

@asset(
    ins={"osm_nodes": AssetIn()},
    partitions_def=nodes_partitions_def
)
def fetch_satellite_images(context: AssetExecutionContext, osm_nodes):
    mapbox_api_key = os.environ['MAPBOX_API_KEY']
    node_id = context.partition_key
    node = osm_nodes.loc[int(node_id)]
    lat = node['y']
    lon = node['x']
    north, south, east, west = get_bounding_box(lat, lon, box_size_meters=92)
    width = 640
    height = 640
    url = f"https://api.mapbox.com/styles/v1/mapbox/satellite-v9/static/[{west},{south},{east},{north}]/{width}x{height}?access_token={mapbox_api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        with open(f'data/images/satellite/{node_id}.png', 'wb') as f:
            f.write(response.content)

@asset
def fetch_incidents_data(context: AssetExecutionContext):
    df = pd.read_csv(f'data/incidents/Motor_Vehicle_Accident_Deaths.csv')
    return df