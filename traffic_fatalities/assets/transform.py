import pandas as pd
import numpy as np
import osmnx as ox
from thefuzz import fuzz
from osmnx.distance import great_circle
from scipy.spatial import cKDTree
from dagster import asset, AssetExecutionContext, AssetIn, MaterializeResult, MetadataValue, Output

EARTH_RADIUS = 6371000  # meters
DEGREE_TO_METERS = 111320  # Average distance for 1 degree latitude in meters
DISTANCE_THRESHOLD = 60 # meters

def lon_distance_threshold(lat, distance_in_meters):
    lat_rad = np.radians(lat)
    return distance_in_meters / (EARTH_RADIUS * np.cos(lat_rad) * (np.pi / 180))

@asset(
    ins={
        'fetch_tims_data': AssetIn(),
        'consolidated_nodes': AssetIn()
    }
)
def match_incidents_to_nodes(context: AssetExecutionContext, fetch_tims_data, consolidated_nodes):
    df = fetch_tims_data
    # drop locations without geocoding
    df = df.loc[~np.isnan(df['POINT_X'])]
    # drop roads that contain in street numbers (not intersections)
    df = df.loc[~df['PRIMARY_RD'].str.contains(r'\b[0-9]{3,}\b') & ~df['SECONDARY_RD'].str.contains(r'\b[0-9]{3,}\b')]
    # convert feet to meters and apply distance threshold
    df = df.loc[df['DISTANCE'] <= (DISTANCE_THRESHOLD / 0.3048)]
    # some streets have directions in parentheses e.g. (N)
    df['PRIMARY_RD'] = df['PRIMARY_RD'].str.replace(r'\s*\([NSEW]\)\s*', '')
    df['SECONDARY_RD'] = df['SECONDARY_RD'].str.replace(r'\s*\([NSEW]\)\s*', '')
    nodes = consolidated_nodes
    nodes_tree = cKDTree(nodes[['lat', 'lon']].values)
    match_df = pd.DataFrame()
    for index, row in df.iterrows():
        context.log.debug(f'Matching {index} to nodes')
        lat = row['POINT_Y']
        lon = row['POINT_X']
        lat_threshold = DISTANCE_THRESHOLD / DEGREE_TO_METERS # give fudge factor then check actual distances
        lon_threshold = lon_distance_threshold(lat, DISTANCE_THRESHOLD)
        nearby_nodes_idx = nodes_tree.query_ball_point(
            [lat, lon], 
            r=2*np.sqrt(lat_threshold**2 + lon_threshold**2)
        )
        for node_idx in nearby_nodes_idx:
            node = nodes.iloc[node_idx]
            node_lat = node['lat']
            node_lon = node['lon']
            distance = great_circle(lat, lon, node_lat, node_lon)
            if distance < DISTANCE_THRESHOLD:
                match_df = pd.concat([
                    match_df,
                    pd.DataFrame({'CASE_ID': [row['CASE_ID']], 'osmid': [node_idx], 'distance': [distance]})
                ])
    match_df = match_df.reset_index()
    match_df.to_csv(f'data/incidents/incidents_nodes_crosswalk.csv')
    return Output(
        value=match_df,
        metadata={
            'num_matched': len(match_df['CASE_ID'].unique()),
            'distance_stats': MetadataValue.md(match_df['distance'].describe().to_markdown()),
            'num_matches_stats': MetadataValue.md(match_df.groupby('CASE_ID').size().describe().to_markdown()),
            'preview': MetadataValue.md(match_df.head(20).to_markdown())
        }
    )