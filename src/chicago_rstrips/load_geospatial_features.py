import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from chicago_rstrips.utils import get_data_dir
from chicago_rstrips.config import (
    POSTGRES_LOCAL_USER,
    POSTGRES_LOCAL_PASSWORD,
    POSTGRES_LOCAL_HOST,
    POSTGRES_LOCAL_PORT,
    POSTGRES_LOCAL_DB
)


def get_engine():
    """Crear engine de SQLAlchemy con connection string de PostgreSQL."""
    db_url = (
        f"postgresql+psycopg2://{POSTGRES_LOCAL_USER}:{POSTGRES_LOCAL_PASSWORD}"
        f"@{POSTGRES_LOCAL_HOST}:{POSTGRES_LOCAL_PORT}/{POSTGRES_LOCAL_DB}"
    )
    return create_engine(db_url)


def create_features_schema_and_tables(engine):
    """
    Crea el schema 'features' y todas las tablas leyendo desde archivo SQL.
    """
    # Obtener la ruta al archivo SQL
    sql_file = Path(__file__).parent.parent.parent / "sql" / "create_features_schema.sql"
    
    if not sql_file.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo SQL: {sql_file}\n"
            f"Asegúrate de haber creado el archivo en: sql/create_features_schema.sql"
        )
    
    print(f"Leyendo DDL desde: {sql_file}")
    
    # Leer el contenido del archivo SQL
    with open(sql_file, 'r', encoding='utf-8') as f:
        ddl_statements = f.read()
    
    # Ejecutar las sentencias DDL
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl_statements)
    
    print("Schema 'features' y tablas creadas/recreadas desde archivo SQL")


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
        'station_name': points_gdf['station_id'],  # O un nombre más descriptivo
        'longitude': points_gdf.geometry.x,
        'latitude': points_gdf.geometry.y,
        'geometry_wkt': points_gdf.geometry.to_wkt(),  # Formato WKT
        'crs': 'EPSG:4326'
    })
    
    df.to_sql(
        name='dim_weather_stations',
        schema='features',
        con=engine,
        if_exists='append',  # ← CAMBIO: append en lugar de replace
        index=False,
        method='multi'
    )
    print(f"✓ {len(df)} estaciones meteorológicas guardadas en features.dim_weather_stations")
    
    # Guardar también en parquet
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
    
    # CAMBIO: Usar 'append'
    df.to_sql(
        name='dim_voronoi_zones',
        schema='features',
        con=engine,
        if_exists='append',  # ← CAMBIO
        index=False,
        method='multi'
    )
    print(f"✓ {len(df)} zonas de Voronoi guardadas en features.dim_voronoi_zones")
    
    # Guardar backup en parquet
    features_dir = get_data_dir() / "features" / "geospatial"
    df.to_parquet(features_dir / "voronoi_zones.parquet", index=False)
    print(f"Backup guardado en {features_dir / 'voronoi_zones.parquet'}")


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
        'geometry_wkt': [city_gdf.geometry.unary_union.wkt],  # Unir todos los polígonos
        'area_km2': [round(area_km2, 2)],
        'crs': ['EPSG:4326'],
        'source_url': ['https://data.cityofchicago.org/resource/qqq8-j68g.geojson']
    })
    
    # CAMBIO: Usar 'append'
    df.to_sql(
        name='ref_city_boundary',
        schema='features',
        con=engine,
        if_exists='append',  # ← CAMBIO
        index=False
    )
    print(f"✓ Límite de Chicago guardado en features.ref_city_boundary")
    
    # Guardar backup
    features_dir = get_data_dir() / "features" / "geospatial"
    df.to_parquet(features_dir / "chicago_city_boundary.parquet", index=False)
    print(f"✓ Backup guardado en {features_dir / 'chicago_city_boundary.parquet'}")


def load_all_geospatial_features(points_gdf, voronoi_gdf, city_gdf):
    """
    Función principal para cargar todas las features geoespaciales.
    
    Args:
        points_gdf: GeoDataFrame de estaciones meteorológicas
        voronoi_gdf: GeoDataFrame de zonas de Voronoi
        city_gdf: GeoDataFrame del límite de Chicago
    """
    engine = get_engine()
    
    try:
        # 1. Crear schema Y TABLAS con DDL completo desde archivo SQL
        print("\n" + "="*60)
        print("CREANDO SCHEMA Y TABLAS DESDE ARCHIVO SQL")
        print("="*60)
        create_features_schema_and_tables(engine)
        
        # 2. Cargar datos (ahora las tablas tienen PKs, FKs, constraints, etc.)
        print("\n" + "="*60)
        print("CARGANDO DATOS A POSTGRESQL")
        print("="*60)
        save_weather_stations(points_gdf, engine)
        save_voronoi_zones(voronoi_gdf, engine)
        save_city_boundary(city_gdf, engine)
        
        # 3. Verificar la carga con la función helper
        print("\n" + "="*60)
        print("ESTADÍSTICAS DE FEATURES CARGADAS")
        print("="*60)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM features.get_features_stats()"))
            for row in result:
                print(f"  • {row[0]}: {row[1]} registros (última actualización: {row[2]})")
        
        print("\n✅ Todas las features geoespaciales fueron cargadas exitosamente")
        
    except Exception as e:
        print(f"❌ Error al cargar features: {e}")
        raise
    finally:
        engine.dispose()


# Para testing independiente
if __name__ == "__main__":
    from chicago_rstrips.chicago_voronoi_zones import (
        city_poly_gdf,
        points_gdf,
        voronoi_con_id
    )
    
    load_all_geospatial_features(
        points_gdf=points_gdf,
        voronoi_gdf=voronoi_con_id,
        city_gdf=city_poly_gdf
    )