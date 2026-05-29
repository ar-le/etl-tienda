import os
import pandas as pd
from dagster import asset, AssetExecutionContext
from resources.hive_resource import HiveResource
from sqlalchemy import text

import subprocess


def upload_to_hdfs_and_refresh(context, df: pd.DataFrame, table_name: str, hive_res: HiveResource):
    """
    Sube el archivo a una ruta temporal en HDFS y ejecuta LOAD DATA en Hive.
    Formato: PARQUET en lugar de ORC
    """
    local_file = f"temp_{table_name}.parquet"
    hdfs_temp_folder = f"/tmp/dagster_uploads/{table_name}"
    hdfs_temp_file = f"{hdfs_temp_folder}/data.parquet"

    
    try:
        for col in df.columns:
            if df[col].dtype == 'bool':
                # Cast
                df[col] = df[col].astype(bool)

        # 1. Guardar PARQUET localmente (en lugar de ORC)
        context.log.info(f"Generando PARQUET local para {table_name}...")
        df.to_parquet(local_file, engine='pyarrow', compression='snappy')

        # 2. Crear el directorio en HDFS (MKDIRS)
        mkdir_url = f"http://{hive_res.hive_ip}:9870/webhdfs/v1{hdfs_temp_folder}?op=MKDIRS&user.name={hive_res.username}"
        subprocess.run(["curl", "-s", "--show-error", "-X", "PUT", mkdir_url], check=True, capture_output=True)

        # 3. Paso A: Obtener la URL de redirección (CREATE)
        context.log.info(f"Solicitando redirección a NameNode para {table_name}...")
        get_url_cmd = [
            "curl", "-s", "--show-error", "-i", "-X", "PUT",
            f"http://{hive_res.hive_ip}:9870/webhdfs/v1{hdfs_temp_file}?op=CREATE&user.name={hive_res.username}&overwrite=true"
        ]
        
        result = subprocess.run(get_url_cmd, capture_output=True, text=True, check=True)
        
        # 4. Extraer la URL de la lista de líneas
        location_url = None
        lineas = result.stdout.splitlines()
        
        for linea in lineas:
            if linea.lower().startswith("location:"):
                location_url = linea.split(":", 1)[1].strip()
                break
        
        if not location_url:
            context.log.error(f"Contenido recibido de curl: {result.stdout}")
            raise Exception(f"No se encontró 'Location' en la respuesta de HDFS para {table_name}.")

        # 5. Paso B: Subida real usando la URL obtenida
        context.log.info(f"Subiendo bytes al DataNode...")
        upload_cmd = ["curl", "-s",  "--show-error", "-i", "-X", "PUT", "-T", local_file, location_url]
        subprocess.run(upload_cmd, check=True, capture_output=True)

        # 6. Refresh en Hive - PARQUET en lugar de ORC
        engine = hive_res.get_engine()
        load_query = text(f"LOAD DATA INPATH '{hdfs_temp_file}' OVERWRITE INTO TABLE {table_name}")
        
        with engine.begin() as connection:
            context.log.info(f"Ejecutando LOAD DATA en Hive para {table_name}...")
            connection.execute(load_query)
        
        context.log.info(f"¡Proceso completado para {table_name}!")

    except Exception as e:
        context.log.error(f"Error cargando {table_name}: {str(e)}")
        raise e
    finally:
        if os.path.exists(local_file):
            os.remove(local_file)

# --- ASSETS DE CARGA ---

@asset(group_name="load")
def load_dim_customers(context: AssetExecutionContext, dim_customers: pd.DataFrame, hive_resource: HiveResource) -> None:
    """Guarda la dimensión de clientes en Hive vía HDFS."""
    upload_to_hdfs_and_refresh(context, dim_customers, "customers", hive_resource)

@asset(group_name="load")
def load_dim_products(context: AssetExecutionContext, dim_products: pd.DataFrame, hive_resource: HiveResource) -> None:
    """Guarda la dimensión de productos en Hive vía HDFS."""
    upload_to_hdfs_and_refresh(context, dim_products, "products", hive_resource)

@asset(group_name="load")
def load_dim_campaigns(context: AssetExecutionContext, dim_campaigns: pd.DataFrame, hive_resource: HiveResource) -> None:
    """Guarda la dimensión de campañas en Hive vía HDFS."""
    upload_to_hdfs_and_refresh(context, dim_campaigns, "campaigns", hive_resource)

@asset(group_name="load")
def load_dim_time(context: AssetExecutionContext, dim_time: pd.DataFrame, hive_resource: HiveResource) -> None:
    """Guarda la dimensión de tiempo en Hive vía HDFS."""
    upload_to_hdfs_and_refresh(context, dim_time, "hours_minutes", hive_resource)

@asset(group_name="load")
def load_dim_dates(context: AssetExecutionContext, dim_dates: pd.DataFrame, hive_resource: HiveResource) -> None:
    """Guarda la dimensión de fechas en Hive vía HDFS."""
    upload_to_hdfs_and_refresh(context, dim_dates, "dates", hive_resource)

@asset(group_name="load")
def load_fact_transactions(context: AssetExecutionContext, fact_transactions: pd.DataFrame, hive_resource: HiveResource) -> None:
    """Guarda la tabla de hechos en Hive vía HDFS."""
    upload_to_hdfs_and_refresh(context, fact_transactions, "transactions", hive_resource)