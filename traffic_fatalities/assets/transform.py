import os
# import requests
import pandas as pd
import numpy as np
import googlemaps
from geopy.distance import geodesic
# import numpy as np
# import networkx as nx
# import matplotlib.pyplot as plt
# import base64
# from io import BytesIO
from dagster import asset, multi_asset, AssetExecutionContext, AssetIn, AssetOut, MaterializeResult, MetadataValue, Output
from traffic_fatalities.utils import get_county_name, get_city_name, clean_address, detect_highway, detect_intersection, detect_exact_address, extract_exact_address, extract_cross_streets, get_address_for_geocoding, find_closest_node

DISTANCE_THRESHOLD = 200

@asset(
    ins={'fetch_incidents_data': AssetIn()}
)
def prep_incident_addresses(context: AssetExecutionContext, fetch_incidents_data):
    df = fetch_incidents_data
    df['County'] = df['IncidentAddress'].apply(lambda x: get_county_name(str(x)))
    df['City'] = df['IncidentAddress'].apply(lambda x: get_city_name(str(x)))
    df = df.loc[df['County'] == 'Sacramento']
    df['CleanAddress'] = df['IncidentAddress'].apply(lambda x: clean_address(str(x)))
    df['IsHighway'] = df['CleanAddress'].apply(lambda x: detect_highway(str(x)))
    df['IsIntersection'] = df['CleanAddress'].apply(lambda x: detect_intersection(str(x))) & ~df['IsHighway']
    df['IsExactAddress'] = df['CleanAddress'].apply(lambda x: detect_exact_address(str(x))) & ~df['IsIntersection'] & ~df['IsHighway']
    df['ExactAddress'] = [extract_exact_address(x) if y else None for x, y in zip(df['CleanAddress'], df['IsExactAddress'])]
    df_intersections = pd.DataFrame([extract_cross_streets(x) if y else (None, None) for x, y in zip(df['CleanAddress'], df['IsIntersection'])], columns=['CrossStreets', 'IntersectionDistance'])
    df = pd.concat([df.reset_index(drop=True), df_intersections], axis=1)
    df['IsIntersection'] = df['IsIntersection'] & ((df['IntersectionDistance'] <= DISTANCE_THRESHOLD) | np.isnan(df['IntersectionDistance']))
    df = get_address_for_geocoding(df)
    df.to_csv(f'data/incidents/prepped_incidents.csv')
    return df

@asset(
    ins={'prep_incident_addresses': AssetIn()}
)
def geocode_incidents(context: AssetExecutionContext, prep_incident_addresses):
    df = prep_incident_addresses
    api_key = os.environ['GOOGLE_API_KEY']
    gmaps = googlemaps.Client(key=api_key)
    results_df = pd.DataFrame()
    for index, row in df.iterrows():
        if row['IsIntersection'] or row['IsExactAddress']:
            address = row['GeocodeAddress']
            result = gmaps.geocode(address)
            result_df = pd.json_normalize(result)
            row_df = df.loc[[index]]
            row_df = pd.concat([row_df] * int(result_df.shape[0])).reset_index(drop=True)
            result_df = pd.concat([row_df, result_df], axis=1)
        else:
            result_df = df.loc[[index]].reset_index(drop=True)
        results_df = pd.concat([results_df, result_df])
    results_df.reset_index(drop=True, inplace=True)
    results_df.to_csv(f'data/incidents/geocoded_incidents.csv')
    return results_df

@asset(
    ins={'geocode_incidents': AssetIn(), 'osm_nodes': AssetIn()}
)
def match_incidents_to_nodes(context: AssetExecutionContext, geocode_incidents, osm_nodes):
    df = geocode_incidents
    nodes = osm_nodes
    match_df = pd.DataFrame()
    for index, row in df.iterrows():
        lat = row['geometry.location.lat']
        lon = row['geometry.location.lng']
        if isinstance(row['types'], (list, str)) and ('intersection' in row['types'] or 'premise' in row['types']):
            for osmid, node in nodes.iterrows():
                node_lat = node['y']
                node_lon = node['x']
                distance = geodesic((lat, lon), (node_lat, node_lon)).feet
                if distance < DISTANCE_THRESHOLD:
                    match_df = pd.concat([
                        match_df,
                        pd.DataFrame({'CaseNum': [row['CaseNum']], 'osmid': [osmid], 'distance': [distance]})
                    ])

    match_df.to_csv(f'data/incidents/incidents_nodes_crosswalk.csv')

    return match_df