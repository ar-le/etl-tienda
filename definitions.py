from dagster import AssetSelection, Definitions, define_asset_job, load_from_defs_folder, load_assets_from_modules
from assets import extract, transform, load
from resources.extract_path import GetExtractPathResource
from resources.hive_resource import HiveResource

# Constantes de configuración
HIVE_IP = '192.168.18.139'
HIVE_USERNAME = 'hadoop-ariel'
HIVE_DATABASE = 'shop'


all_assets = load_assets_from_modules([extract, transform, load])

full_etl = define_asset_job(
    "full_etl",
    selection=AssetSelection.all(),
)



defs = Definitions(
    assets=all_assets,
    resources={
        'extract_path': GetExtractPathResource(
            campaigns='../data/campaigns.csv',
            customers='../data/customers.csv',
            products='../data/products.csv',
            transactions='../data/transactions.csv'
        ),
        'hive_resource': HiveResource(
            hive_ip=HIVE_IP,
            username=HIVE_USERNAME,
            database=HIVE_DATABASE
        )
    },
    jobs=[full_etl]
)