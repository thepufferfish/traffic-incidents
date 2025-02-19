from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

from traffic_fatalities.assets import extract, transform
from traffic_fatalities.jobs import consolidation_job, match_incidents_job

all_assets = load_assets_from_modules([extract, transform])
all_asset_checks = load_asset_checks_from_modules([transform])
all_jobs = [consolidation_job, match_incidents_job]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs
)
