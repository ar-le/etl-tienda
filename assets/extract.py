import pandas as pd
from dagster import asset, AssetExecutionContext
from resources.extract_path import GetExtractPathResource


@asset(group_name="extract")
def campaigns(extract_path: GetExtractPathResource) -> pd.DataFrame:
    """Carga el DataFrame de campañas desde la ruta."""
    return pd.read_csv(extract_path.get_campaigns())


@asset(group_name="extract")
def customers(extract_path: GetExtractPathResource) -> pd.DataFrame:
    """Carga el DataFrame de clientes desde la ruta."""
    return pd.read_csv(extract_path.get_customers())


@asset(group_name="extract")
def products(extract_path: GetExtractPathResource) -> pd.DataFrame:
    """Carga el DataFrame de productos desde la ruta."""
    return pd.read_csv(extract_path.get_products())


@asset(group_name="extract")
def transactions(extract_path: GetExtractPathResource) -> pd.DataFrame:
    """Carga el DataFrame de transacciones desde la ruta."""
    return pd.read_csv(extract_path.get_transactions())

