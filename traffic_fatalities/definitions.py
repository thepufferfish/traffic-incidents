from dagster import Definitions, load_assets_from_modules

from traffic_fatalities.assets import extract, transform

all_assets = load_assets_from_modules([extract, transform])

defs = Definitions(
    assets=all_assets,
)
