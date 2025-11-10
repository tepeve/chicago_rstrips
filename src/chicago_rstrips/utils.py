from pathlib import Path

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
# Funciones para consultas geoespaciales
# ============================================================

def get_weather_stations():
    """
    Obtener todas las estaciones meteorológicas desde PostgreSQL.
    
    Returns:
        pd.DataFrame: DataFrame con información de estaciones
    """
    from chicago_rstrips.load_geospatial_features import get_engine
    
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
    from chicago_rstrips.load_geospatial_features import get_engine
    
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
    from chicago_rstrips.load_geospatial_features import get_engine
    
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
