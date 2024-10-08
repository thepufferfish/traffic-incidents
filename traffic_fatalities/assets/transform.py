import re
import pandas as pd
import numpy as np
import osmnx as ox
from thefuzz import fuzz
from osmnx.distance import great_circle
from scipy.spatial import cKDTree
from dagster import asset, AssetExecutionContext, AssetIn, asset_check, AssetCheckExecutionContext, AssetCheckResult, MaterializeResult, MetadataValue, Output
from traffic_fatalities.partitions import consolidation_tolerances_partitions_def
from traffic_fatalities.utils import normalize_street_name

EARTH_RADIUS = 6371000  # meters
DEGREE_TO_METERS = 111320  # Average distance for 1 degree latitude in meters
DISTANCE_THRESHOLD = 60 # meters

def lon_distance_threshold(lat, distance_in_meters):
    lat_rad = np.radians(lat)
    return distance_in_meters / (EARTH_RADIUS * np.cos(lat_rad) * (np.pi / 180))

def consolidate_graph(context: AssetExecutionContext, osm_graph):
    # consolidates intersections inside centroids with radius of tolerance in meters
    tolerance = int(context.partition_key)
    G_proj = ox.project_graph(osm_graph)
    G_slim = ox.consolidate_intersections(G_proj, tolerance=tolerance)
    nodes, edges = ox.graph_to_gdfs(G_slim)
    nodes.to_csv(f'data/consolidated_nodes.csv')
    edges.to_csv(f'data/consolidated_edges.csv')
    yield Output(nodes, output_name="consolidated_nodes")
    yield Output(edges, output_name="consolidated_edges")

@asset(
    ins={
        'fetch_tims_data': AssetIn(),
        'consolidated_nodes': AssetIn()
    },
    partitions_def=consolidation_tolerances_partitions_def
)
def map_incidents_to_nodes(context: AssetExecutionContext, fetch_tims_data, consolidated_nodes):
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

@asset_check(
    asset=map_incidents_to_nodes,
    additional_ins={
        'consolidated_edges': AssetIn(),
        'fetch_tims_data': AssetIn()
    }
)
def check_incidents_matches(context: AssetCheckExecutionContext, map_incidents_to_nodes, consolidated_edges, fetch_tims_data):
    metadata = {}
    for key, value in map_incidents_to_nodes.items():
        df = pd.merge(
            value,
            fetch_tims_data[['CASE_ID', 'PRIMARY_RD', 'SECONDARY_RD', 'DISTANCE']],
            on=['CASE_ID'],
            how='left'
        )
        # some edge osmids are a list, and lists of complementary segments are sorted oppositely
        # so I need to sort them all the same way
        edges = consolidated_edges[key].reset_index(level=['u', 'v'])
        edges['osmid'] = edges['osmid'].apply(lambda x: x.sort() if type(x) == 'list' else x)
        edges['osmid_flat'] = edges['osmid'].apply(lambda x: str('-'.join(x)) if type(x) == 'list' else str(x))
        edges = edges.explode('name')
        edges['name'] = edges['name'].apply(lambda x: normalize_street_name(str(x)))
        edges = edges[['u', 'v', 'osmid', 'osmid_flat', 'name']]
        edges = pd.merge(
            edges,
            edges,
            left_on=['u', 'v', 'osmid_flat'],
            right_on=['v', 'u', 'osmid_flat'],
            how='outer',
            suffixes=['_u', '_v']
        )
        df = df.merge(
            edges,
            left_on=['osmid'],
            right_on=['u_u'],
            how='left',
            suffixes=['_node', '_edge']
        )
        df['primary_score'] = df.apply(
            lambda x: max(fuzz.ratio(x['PRIMARY_RD'], x['name_u']), fuzz.ratio(x['PRIMARY_RD'], x['name_v'])),
            axis=1
        )
        df['secondary_score'] = df.apply(
            lambda x: max(fuzz.ratio(x['SECONDARY_RD'], x['name_u']), fuzz.ratio(x['SECONDARY_RD'], x['name_v'])),
            axis=1
        )
        df['primary_match'] = df.apply(
            lambda x: x['primary_score'] == 100 if re.search(r'[0-9]', x['PRIMARY_RD']) else x['primary_score'] >= 75,
            axis=1
        )
        df['secondary_match'] = df.apply(
            lambda x: x['secondary_score'] == 100 if re.search(r'[0-9]', x['SECONDARY_RD']) else x['secondary_score'] >= 75,
            axis=1
        )
        df_grouped = df.groupby(['CASE_ID', 'osmid'])
        df_agg = df_grouped.agg({
            'primary_match': 'any',
            'secondary_match': 'any',
            'primary_score': 'max',
            'secondary_score': 'max'
        }).reset_index()
        df_agg['good_match'] = df_agg['primary_match'] & df_agg['secondary_match']
        good_matches = df_agg.loc[df_agg['good_match'],]
        bad_matches = df_agg.loc[~df_agg['good_match'],]
        bad_matches = bad_matches.loc[~bad_matches['CASE_ID'].isin(good_matches.CASE_ID)]
        df_good = good_matches.merge(
            df,
            on=['CASE_ID', 'osmid'],
            how='left'
        ).sort_values(['CASE_ID', 'osmid'])
        df_bad = bad_matches.merge(
            df,
            on=['CASE_ID', 'osmid'],
            how='left'
        ).sort_values(['CASE_ID', 'osmid'])
        df_bad = df_bad.loc[~df_bad['PRIMARY_RD'].str.contains(r'[0-9]{1,2}') & ~df_bad['SECONDARY_RD'].str.contains(r'[0-9]{1,2}')]
        metadata[key] = {
            'num_matches': df['CASE_ID'].nunique(),
            'num_good_matches': df_good['CASE_ID'].nunique(),
            'num_bad_matches': bad_matches['CASE_ID'].nunique(),
            'pct_good_matches': df_good['CASE_ID'].nunique() / df['CASE_ID'].nunique(),
            'pct_bad_matches': df_bad['CASE_ID'].nunique() / df['CASE_ID'].nunique(),#,
            #'bad_matches': df_bad.sort_values(['primary_score_x', 'secondary_score_x', 'CASE_ID', 'osmid'], ascending=False).head(100).to_markdown()
        }
    return AssetCheckResult(
        passed=True,
        metadata=metadata
    )
    

@asset(
    ins={
        'osm_graph': AssetIn(),
        'consolidated_nodes': AssetIn()
    }
)
def map_consolidated_nodes_to_images(context: AssetExecutionContext, osm_graph, consolidated_nodes):
    image_ids = ox.nearest_nodes(osm_graph, consolidated_nodes['lon'], consolidated_nodes['lat'], return_dist=False)
    mapping = pd.DataFrame({
        'osmid': consolidated_nodes['osmid'].values,
        'image_id': image_ids
    })
    return Output(
        value=mapping,
        metadata={
            'preview': MetadataValue.md(mapping.head().to_markdown())
        }
    )