from dagster import define_asset_job
from traffic_fatalities.assets.extract import consolidate_graph
from traffic_fatalities.assets.transform import match_incidents_to_nodes


consolidation_job = define_asset_job(name='consolidate_graph_job', selection=[consolidate_graph])
match_incidents_job = define_asset_job(name='match_incidents_job', selection=[match_incidents_to_nodes])