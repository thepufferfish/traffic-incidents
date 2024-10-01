from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

from traffic_fatalities.assets import extract, transform

all_assets = load_assets_from_modules([extract, transform])
all_asset_checks = load_asset_checks_from_modules([transform])

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks
)
