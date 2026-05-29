import pandas as pd
from dagster import asset, AssetExecutionContext
from datetime import datetime


def add_sequential_id(df: pd.DataFrame, position: int = 0) -> pd.DataFrame:
    """
    Añade una columna de ID secuencial a un DataFrame.
    
    Args:
        df: DataFrame al que añadir el ID
        position: Posición donde insertar el ID (0 = inicio, -1 = final)
    
    Returns:
        DataFrame con la columna 'id' añadida
    """
    df['id'] = range(1, len(df) + 1)
    if position == 0:
        # Mover 'id' al principio
        cols = ['id'] + [col for col in df.columns if col != 'id']
        df = df[cols]
    return df


@asset(group_name="transform")
def dim_time(context: AssetExecutionContext) -> pd.DataFrame:
    """Dimensión de tiempo con todas las horas del día (00:00 a 23:59)"""
    times = []
    for hour in range(24):
        for minute in range(60):
            times.append({
                'hour': hour,
                'minute': minute
            })
    
    df = pd.DataFrame(times)
    context.log.info(f"Generando dimensión de tiempo con {len(df)} filas...")
    context.log.debug(f"Primeras filas de dim_time:\n{df.head()}")
    df = add_sequential_id(df)
    return df[['id', 'hour', 'minute']]


@asset(group_name="transform")
def dim_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Dimensión de clientes con IDs secuenciales y canales expandidos."""
    
    df = customers.copy()
    df = df.rename(columns={
        'loyalty_tier': 'tier'
    })
    
    # 1. Convertir age a birth_year
    current_year = datetime.now().year
    df['birth_year'] = current_year - df['age']
    
    # 2. Añadir columna is_active
    df['is_active'] = True
    
    # 3. Transformar acquisition_channel en columnas booleanas (One-Hot Encoding)
    # Definimos la lista exacta de canales que espera nuestra tabla de Hive
    channels = ['referral', 'organic', 'paid_search', 'social', 'email']
    
    for channel in channels:
        column_name = f'acquisition_channel_{channel}'
        # Comparamos y convertimos a bool explícitamente
        df[column_name] = df['acquisition_channel'].str.lower() == channel.replace('_', ' ')
    
    # 4. Añadir ID secuencial
    df = add_sequential_id(df)

    bool_cols = [
        'is_active',
        'acquisition_channel_referral',
        'acquisition_channel_organic',
        'acquisition_channel_paid_search',
        'acquisition_channel_social',
        'acquisition_channel_email'
    ]
    for col in bool_cols:
        df[col] = df[col].astype('boolean')
    
    # 5. Seleccionar las columnas finales en el orden exacto de la tabla Hive
    return df[[
        'id', 
        'birth_year', 
        'gender', 
        'tier', 
        'country', 
        'signup_date', 
        'acquisition_channel_referral',
        'acquisition_channel_organic',
        'acquisition_channel_paid_search',
        'acquisition_channel_social',
        'acquisition_channel_email',
        'is_active'
    ]]

@asset(group_name="transform")
def dim_products(products: pd.DataFrame) -> pd.DataFrame:
    """Dimensión de productos con IDs secuenciales."""
    df = products.copy()
    df = add_sequential_id(df)
    return df[['id', 'brand', 'category', 'base_price', 'launch_date']]


@asset(group_name="transform")
def dim_campaigns(campaigns: pd.DataFrame) -> pd.DataFrame:
    """Dimensión de campañas con IDs secuenciales y campos transformados."""
    df = campaigns.copy()
    df = df.rename(columns={'target_segment': 'target'})
    df = add_sequential_id(df)
    return df[['id', 'objective', 'target', 'channel']]


@asset(group_name="transform")
def dim_dates(context: AssetExecutionContext,transactions: pd.DataFrame) -> pd.DataFrame:
    """Extrae las fechas únicas de transacciones con día, mes, año y estación."""
    #Pasar a formto datetime (lo lee como string)
    ts_column = pd.to_datetime(transactions['timestamp'])
    # Extraer fechas únicas
    unique_dates = pd.Series(ts_column.dt.date.unique()).sort_values()
    
    df = pd.DataFrame({'date': unique_dates})
    df['date'] = pd.to_datetime(df['date'])
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    
    def get_season(month):
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'
    
    df['season'] = df['month'].apply(get_season)
    df.reset_index(drop=True, inplace=True)
    df = add_sequential_id(df)

    #debug
    context.log.info(f"Generando dimensión de fechas con {len(df)} filas...")
    context.log.debug(f"Primeras filas de dim_dates:\n{df.head()}")

    return df[['id', 'day', 'month', 'year', 'date', 'season']]


@asset(group_name="transform")
def fact_transactions(
    transactions: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    campaigns: pd.DataFrame,
    dim_customers: pd.DataFrame,
    dim_products: pd.DataFrame,
    dim_campaigns: pd.DataFrame,
    dim_dates: pd.DataFrame,
    dim_time: pd.DataFrame
) -> pd.DataFrame:
    """Tabla de hechos de transacciones con claves foráneas remapeadas."""
    # Crear mappings: old_id -> new_id (basado en orden de las filas)
    customer_mapping = dict(zip(customers['customer_id'], dim_customers['id']))
    product_mapping = dict(zip(products['product_id'], dim_products['id']))
    campaign_mapping = dict(zip(campaigns['campaign_id'], dim_campaigns['id']))
    
    # Crear mapping de fechas: date -> date_id
    date_mapping = dict(zip(pd.to_datetime(dim_dates['date']).dt.date, dim_dates['id']))
    
    # Crear mapping de tiempo: (hour, minute) -> time_id
    time_mapping = dict(zip(zip(dim_time['hour'], dim_time['minute']), dim_time['id']))
    
    df = transactions.copy()
    
    # Convertir timestamp a datetime (llega como string))
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Remapear IDs usando los mappings
    df['customer_id'] = df['customer_id'].map(customer_mapping)
    df['product_id'] = df['product_id'].map(product_mapping)
    df['campaign_id'] = df['campaign_id'].map(campaign_mapping)
    
    # Extraer date_id y time_id del timestamp
    df['date_id'] = df['timestamp'].dt.date.map(date_mapping)
    df['time_id'] = df['timestamp'].apply(lambda x: time_mapping.get((x.hour, x.minute)))
     
    # Renombrar columnas
    df = df.rename(columns={
        'discount_applied': 'discount',
        'gross_revenue': 'revenue',
        'refund_flag': 'is_refund'
    })

    df['is_refund'] = df['is_refund'].astype('boolean')
    
    # Generar nuevos IDs para transacciones
    df = add_sequential_id(df)
    
    # Seleccionar y ordenar columnas finales
    return df[['id', 'date_id', 'time_id', 'customer_id', 'product_id', 'campaign_id', 'quantity', 'discount', 'revenue', 'is_refund']]