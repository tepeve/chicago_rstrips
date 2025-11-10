import pandas as pd
from pathlib import Path
from chicago_rstrips.utils import get_data_dir
from chicago_rstrips.db_loader import get_engine, run_ddl, load_dataframe_to_postgres


def save_weather_stations(points_gdf, engine):
    """
    Persistir estaciones meteorológicas en PostgreSQL.
    
    Args:
        points_gdf (GeoDataFrame): GeoDataFrame con columnas: station_id, lon, lat, geometry
        engine: SQLAlchemy engine
    """
    # Convertir a WGS84 si no lo está
    if points_gdf.crs.to_epsg() != 4326:
        points_gdf = points_gdf.to_crs(epsg=4326)
    
    # Preparar DataFrame para PostgreSQL
    df = pd.DataFrame({
        'station_id': points_gdf['station_id'],
        'station_name': points_gdf['station_id'],
        'longitude': points_gdf.geometry.x,
        'latitude': points_gdf.geometry.y,
        'geometry_wkt': points_gdf.geometry.to_wkt(),
        'crs': 'EPSG:4326'
    })
    
    # Cargar usando función centralizada
    load_dataframe_to_postgres(
        df,
        table_name='dim_weather_stations',
        schema='features',
        engine=engine,
        if_exists='append'
    )
    
    # Guardar backup en parquet
    features_dir = get_data_dir() / "features" / "geospatial"
    features_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(features_dir / "weather_stations.parquet", index=False)
    print(f"✓ Backup guardado en {features_dir / 'weather_stations.parquet'}")


def save_voronoi_zones(voronoi_gdf, engine):
    """
    Persistir zonas de Voronoi en PostgreSQL.
    
    Args:
        voronoi_gdf (GeoDataFrame): GeoDataFrame con columnas: station_id, geometry
        engine: SQLAlchemy engine
    """
    # Convertir a WGS84
    if voronoi_gdf.crs.to_epsg() != 4326:
        voronoi_gdf = voronoi_gdf.to_crs(epsg=4326)
    
    # Calcular área en km² (proyectar temporalmente a UTM)
    voronoi_utm = voronoi_gdf.to_crs(epsg=26916)  # UTM Zone 16N
    areas_km2 = voronoi_utm.geometry.area / 1_000_000  # m² a km²
    
    # Preparar DataFrame
    df = pd.DataFrame({
        'station_id': voronoi_gdf['station_id'],
        'geometry_wkt': voronoi_gdf.geometry.to_wkt(),
        'area_km2': areas_km2.round(2),
        'crs': 'EPSG:4326'
    })
    
    # Cargar usando función centralizada
    load_dataframe_to_postgres(
        df,
        table_name='dim_voronoi_zones',
        schema='features',
        engine=engine,
        if_exists='append'
    )
    
    # Guardar backup en parquet
    features_dir = get_data_dir() / "features" / "geospatial"
    df.to_parquet(features_dir / "voronoi_zones.parquet", index=False)
    print(f"✓ Backup guardado en {features_dir / 'voronoi_zones.parquet'}")


def save_city_boundary(city_gdf, engine):
    """
    Persistir límite de la ciudad en PostgreSQL.
    
    Args:
        city_gdf (GeoDataFrame): GeoDataFrame con el polígono de Chicago
        engine: SQLAlchemy engine
    """
    # Convertir a WGS84
    if city_gdf.crs.to_epsg() != 4326:
        city_gdf = city_gdf.to_crs(epsg=4326)
    
    # Calcular área
    city_utm = city_gdf.to_crs(epsg=26916)
    area_km2 = city_utm.geometry.area.sum() / 1_000_000
    
    # Preparar DataFrame
    df = pd.DataFrame({
        'city_name': ['Chicago'],
        'geometry_wkt': [city_gdf.geometry.unary_union.wkt],
        'area_km2': [round(area_km2, 2)],
        'crs': ['EPSG:4326'],
        'source_url': ['https://data.cityofchicago.org/resource/qqq8-j68g.geojson']
    })
    
    # Cargar usando función centralizada
    load_dataframe_to_postgres(
        df,
        table_name='ref_city_boundary',
        schema='features',
        engine=engine,
        if_exists='append'
    )
    
    # Guardar backup
    features_dir = get_data_dir() / "features" / "geospatial"
    df.to_parquet(features_dir / "chicago_city_boundary.parquet", index=False)
    print(f"✓ Backup guardado en {features_dir / 'chicago_city_boundary.parquet'}")


def load_all_geospatial_features(points_gdf=None, voronoi_gdf=None, city_gdf=None):
    """
    Función principal para cargar todas las features geoespaciales.
    
    Si no se pasan los GeoDataFrames, los genera usando chicago_voronoi_zones.
    
    Args:
        points_gdf: GeoDataFrame de estaciones meteorológicas (opcional)
        voronoi_gdf: GeoDataFrame de zonas de Voronoi (opcional)
        city_gdf: GeoDataFrame del límite de Chicago (opcional)
    """
    # Si no se pasan los GeoDataFrames, generarlos
    if points_gdf is None or voronoi_gdf is None or city_gdf is None:
        print("📦 Generando features geoespaciales...")
        from chicago_rstrips.chicago_voronoi_zones import generate_all_geospatial_features
        features = generate_all_geospatial_features()
        city_gdf = features['city']
        points_gdf = features['stations']
        voronoi_gdf = features['zones']
    
    engine = get_engine()
    
    try:
        # 1. Crear schema Y TABLAS con DDL completo desde archivo SQL
        print("\n" + "="*60)
        print("CREANDO SCHEMA Y TABLAS DESDE ARCHIVO SQL")
        print("="*60)
        run_ddl(engine, "create_features_schema.sql")
        
        # 2. Cargar datos (ahora las tablas tienen PKs, FKs, constraints, etc.)
        print("\n" + "="*60)
        print("CARGANDO DATOS A POSTGRESQL")
        print("="*60)
        save_weather_stations(points_gdf, engine)
        save_voronoi_zones(voronoi_gdf, engine)
        save_city_boundary(city_gdf, engine)
        
        print("\n✅ Todas las features geoespaciales fueron cargadas exitosamente")
        
    except Exception as e:
        print(f"❌ Error al cargar features: {e}")
        raise
    finally:
        engine.dispose()


# Para testing independiente
if __name__ == "__main__":
    # Ahora NO necesita importar los GeoDataFrames específicos
    # La función se encarga de generarlos si no se pasan
    load_all_geospatial_features()