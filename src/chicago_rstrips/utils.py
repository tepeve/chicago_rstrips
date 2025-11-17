from pathlib import Path
import pandas as pd

# ============================================================
## Funciones para manejar rutas de directorios
# ============================================================

def get_project_root():
    """Obtiene el directorio raíz del proyecto."""
    return Path(__file__).parent.parent.parent

def get_data_dir():
    """Obtiene el directorio de datos, creándolo si no existe."""
    data_dir = get_project_root() / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir

def get_raw_data_dir():
    """Obtiene el directorio de datos raw, creándolo si no existe."""
    raw_dir = get_data_dir() / "raw"
    raw_dir.mkdir(exist_ok=True)
    return raw_dir

def get_processed_data_dir():
    """Obtiene el directorio de datos procesados, creándolo si no existe."""
    processed_dir = get_data_dir() / "processed"
    processed_dir.mkdir(exist_ok=True)
    return processed_dir

def get_outputs_dir():
    """Obtiene el directorio de datos, creándolo si no existe."""
    outputs_dir = get_project_root() / "outputs"
    outputs_dir.mkdir(exist_ok=True)
    return outputs_dir

def get_features_dir():
    """Obtiene el directorio de features, creándolo si no existe."""
    features_dir = get_data_dir() / "features"
    features_dir.mkdir(exist_ok=True)
    return features_dir

def get_geospatial_features_dir():
    """Obtiene el directorio de features geoespaciales, creándolo si no existe."""
    geo_dir = get_features_dir() / "geospatial"
    geo_dir.mkdir(exist_ok=True)
    return geo_dir

# ============================================================
# Funciones para transformar tipos de datos en DataFrames
# ============================================================

def transform_dataframe_types(df, type_mapping):
    """
    Transforma los tipos de datos de un DataFrame según un mapeo especificado.
    
    Args:
        df: DataFrame a transformar
        type_mapping: Dict con {columna: tipo_destino}
    
    Returns:
        DataFrame con tipos transformados
    """
    df_copy = df.copy()
    
    for col, dtype in type_mapping.items():
        if col not in df_copy.columns:
            print(f"Advertencia: Columna '{col}' no existe en el DataFrame")
            continue
            
        try:
            if dtype == 'datetime64[ns]':
                df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
            elif dtype == 'Int64':
                # Convertir a numérico, redondear si tiene decimales, luego a Int64
                numeric_values = pd.to_numeric(df_copy[col], errors='coerce')
                # Redondear floats antes de convertir a Int64
                df_copy[col] = numeric_values.round().astype('Int64')
            elif dtype == 'float64':
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
            elif dtype == 'string':
                df_copy[col] = df_copy[col].astype('string')
            elif dtype == 'boolean':
                # Manejar booleanos correctamente
                df_copy[col] = df_copy[col].map({
                    'true': True, 'false': False, 
                    True: True, False: False,
                    'True': True, 'False': False,
                    '1': True, '0': False,
                    1: True, 0: False
                })
                df_copy[col] = df_copy[col].astype('boolean')
            else:
                df_copy[col] = df_copy[col].astype(dtype)
        except Exception as e:
            print(f"Advertencia: No se pudo convertir columna '{col}' a {dtype}: {e}")
            # Intentar conversión más permisiva
            if dtype in ['Int64', 'float64']:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                if dtype == 'Int64':
                    df_copy[col] = df_copy[col].round().astype('Int64')
            elif dtype == 'datetime64[ns]':
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
    
    return df_copy

# ============================================================
# Funciones para consultas geoespaciales
# ============================================================

def get_weather_stations():
    """
    Obtener todas las estaciones meteorológicas desde PostgreSQL.
    
    Returns:
        pd.DataFrame: DataFrame con información de estaciones
    """
    from chicago_rstrips.load_dim_static_tables import get_engine
    
    engine = get_engine()
    try:
        df = pd.read_sql(
            "SELECT * FROM features.dim_weather_stations ORDER BY station_id", 
            engine
        )
        return df
    finally:
        engine.dispose()


def get_stations_with_zones():
    """
    Obtener estaciones con sus zonas de Voronoi (usando la vista).
    
    Returns:
        pd.DataFrame: DataFrame con estaciones y sus zonas
    """
    from chicago_rstrips.load_dim_static_tables import get_engine
    
    engine = get_engine()
    try:
        df = pd.read_sql("SELECT * FROM features.vw_stations_with_zones", engine)
        return df
    finally:
        engine.dispose()


def find_zone_for_point(longitude, latitude):
    """
    Encontrar la zona de Voronoi más cercana a un punto dado.
    
    Args:
        longitude (float): Longitud del punto
        latitude (float): Latitud del punto
    
    Returns:
        str: station_id de la zona más cercana, o None si no se encuentra
    
    Example:
        >>> zone = find_zone_for_point(-87.6298, 41.8781)
        >>> print(zone)
        'Estacion_A'
    """
    from chicago_rstrips.load_dim_static_tables import get_engine
    
    engine = get_engine()
    
    query = text("""
        SELECT 
            vz.station_id,
            ws.longitude,
            ws.latitude,
            SQRT(
                POWER(:lon - ws.longitude, 2) + 
                POWER(:lat - ws.latitude, 2)
            ) AS distance
        FROM features.dim_voronoi_zones vz
        JOIN features.dim_weather_stations ws ON vz.station_id = ws.station_id
        ORDER BY distance
        LIMIT 1
    """)
    
    try:
        result = pd.read_sql(query, engine, params={'lon': longitude, 'lat': latitude})
        return result.iloc[0]['station_id'] if not result.empty else None
    finally:
        engine.dispose()
