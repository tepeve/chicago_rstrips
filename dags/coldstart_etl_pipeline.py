from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pendulum
import pandas as pd

from chicago_rstrips.config import START_DATE, COLD_START_END_DATE


from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir


default_args = {"owner": "tepeve", 
                 "depends_on_past": False,
                 "start_date": pendulum.parse(START_DATE),
                 # "end_date": pendulum.parse(COLD_START_END_DATE),
                 "retries":     1,
                 "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="coldstart_etl_pipeline",
    schedule=None,
    catchup=True,
    tags=["setup", "features", "static", "one-time"],
    description="Carga features estáticas y primeros lotes de datos desde cero.",
    default_args=default_args
)
def coldstart_etl_pipeline():
    """
    DAG que orquesta la carga de features estáticas:
    Ejecuta DDLs para schemas y tablas estáticas y asegurar que existan las tablas de staging y dims existan.
    Genera los DataFrames de features en memoria.
    Carga los DataFrames a PostgreSQL usando el loader en db_loader.py.
    Guarda una visualización de las zonas Voronoi generadas.
    Extrae el primer lote datos de Trips y Locations a Parquet.
    Extrae el primer lote datos de Traffic y Regions a Parquet.
    Carga los 4 Parquets a PostgreSQL.
    Verifica la carga y genera un reporte.   
    """

    @task
    def setup_ddl():
        from chicago_rstrips.db_loader import get_engine, run_ddl
        print("Ejecutando DDLs...")
        engine = get_engine()
        try:
            # Asegura que los schemas existan
            run_ddl(engine, "create_schemas.sql") 
            run_ddl(engine, "create_dim_static_tables.sql")
            run_ddl(engine, "create_staging_tables.sql")
            run_ddl(engine, "create_dim_dynamic_tables.sql")
        finally:
            engine.dispose()
        print("✓ DDLs ejecutados.")

    @task
    def generate_city():
        from chicago_rstrips.create_location_static_features import generate_city_boundary
        print("Generando mapa de ciudad...")
        return generate_city_boundary()

    @task
    def generate_stations():
        from chicago_rstrips.create_location_static_features import generate_weather_stations
        print("Generando estaciones de clima...")
        return generate_weather_stations()

    @task
    # Vamos a pasar los df de city_boundary y weather_stations como parámetros de entrada como XComArgs
    def generate_voronoi(city_df, stations_df):
        from chicago_rstrips.create_location_static_features import generate_voronoi_zones
        print("Generando zonas Voronoi...")
        return generate_voronoi_zones(city_df, stations_df)

    @task
    def load_city(df):
        print("Cargando límite de ciudad...")
        from chicago_rstrips.db_loader import load_dataframe_to_postgres
        load_dataframe_to_postgres(
            df,
            table_name='chicago_city_boundary',
            schema='dim_spatial',
            if_exists='replace' 
        )

    @task
    def load_stations(df):
        from chicago_rstrips.db_loader import load_dataframe_to_postgres        
        print("Cargando estaciones...")
        load_dataframe_to_postgres(
            df,
            table_name='weather_stations_points',
            schema='dim_spatial',
            if_exists='replace'
        )

    @task
    def load_voronoi(df):
        from chicago_rstrips.db_loader import load_dataframe_to_postgres
        print("Cargando zonas Voronoi...")
        load_dataframe_to_postgres(
            df,
            table_name='weather_voronoi_zones',
            schema='dim_spatial',
            if_exists='replace'
        )

    @task
    def extract_trips(batch_id: str) -> dict:
        from chicago_rstrips.extract_trips_data import extract_trips_data
        from chicago_rstrips.utils import get_raw_data_dir        
        print("Extrayendo datos de trips desde Socrata API...")     
        # Esta función ya guarda 'raw_trips_data.parquet' y 'trips_locations.parquet'

        output_filename = f"raw_trips_data__{batch_id}.parquet"
        locations_filename = f"trips_locations__{batch_id}.parquet"
        
        
        trips_path_str = extract_trips_data(
            output_filename=output_filename,
            build_locations=True, 
            locations_strategy="rebuild",
            locations_filename=locations_filename,
            batch_id=batch_id,
            start_timestamp=START_DATE,
            end_timestamp=COLD_START_END_DATE
        )
        
        if not trips_path_str:
            raise ValueError("No se pudo extraer datos de trips")
        
        # Reconstruimos los paths para pasarlos explícitamente
        raw_dir = get_raw_data_dir()
        return {
            'trips_path': trips_path_str,
            'locations_path': str(raw_dir / locations_filename)
        }

    @task
    def extract_traffic(batch_id: str) -> dict:
        from chicago_rstrips.extract_traffic_data import extract_traffic_data
        from chicago_rstrips.utils import get_raw_data_dir 

        output_filename = f"stg_raw_traffic___{batch_id}.parquet"
        regions_filename = f"traffic_regions___{batch_id}.parquet"


        print("Extrayendo datos de tráfico desde Socrata API...")
        traffic_path_str = extract_traffic_data(
            output_filename=output_filename,
            build_regions=True, 
            regions_strategy="rebuild",
            traffic_regions_filename=regions_filename,
            batch_id=batch_id,
            start_timestamp=START_DATE,
            end_timestamp=COLD_START_END_DATE       
        )
        
        if not traffic_path_str:
            raise ValueError("No se pudo extraer datos de tráfico")

        raw_dir = get_raw_data_dir()
        return {
            'traffic_path': traffic_path_str,
            'regions_path': str(raw_dir / regions_filename)
        }
    
    @task
    def extract_weather(batch_id: str) -> dict:
        from chicago_rstrips.extract_weather_data import extract_weather_data
        print("Extrayendo datos de clima desde Visual Crossing Weather API...")

        output_filename = f"stg_raw_weather___{batch_id}.parquet"

        weather_path_str = extract_weather_data(
            output_filename=output_filename,
            batch_id=batch_id,
            start_timestamp=START_DATE,
            end_timestamp=COLD_START_END_DATE       
        )
        
        if not weather_path_str:
            raise ValueError("No se pudo extraer datos de clima")
        return {
            'weather_path': weather_path_str
        }

    # --- Primeras cargas de trips, traffic y weather ---

    @task
    def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
        """Combina los diccionarios de paths de trips, traffic y weather."""
        return {**trips_paths, **traffic_paths, **weather_paths}


    @task
    def load_trips(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando trips a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['trips_path'],
            table_name="stg_raw_trips",
            schema="staging",
            if_exists="replace" 
        )

    @task
    def load_locations(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando dimensión de ubicaciones a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['locations_path'],
            table_name="trips_locations",
            schema="dim_spatial",
            if_exists="replace" 
        )

    @task
    def load_traffic(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando datos de tránsito a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['traffic_path'],
            table_name="stg_raw_traffic",
            schema="staging",
            if_exists="replace" 
        )

    @task
    def load_traffic_regions(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando dimensión de regiones de tránsito a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['regions_path'],
            table_name="traffic_regions",
            schema="dim_spatial",
            if_exists="replace"
        )

    @task
    def load_weather(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando datos de clima a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['weather_path'],
            table_name="stg_raw_weather",
            schema="staging",
            if_exists="replace"
        )
    
    @task
    def visualize_task(city_df, stations_df, voronoi_df):
        from chicago_rstrips.create_location_static_features import save_weather_stations_visualization        
        print("Guardando visualización...")
        save_weather_stations_visualization(city_df, stations_df, voronoi_df)

    @task
    def verify_load(paths: dict):
        """Verifica integridad de la carga."""
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        
        print("🔍 Verificando integridad de la carga...")
        engine = get_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_trips")) # <-- "stg_raw_trips"
                trips_count = result.fetchone()[0]
                print(f"✓ Trips en staging: {trips_count}")
                
                if trips_count == 0:
                    raise ValueError("No se cargaron trips!")
                
                return { 'trips_count': trips_count }
                
        finally:
            engine.dispose()


    # --- FLUJO DEL DAG ---
    
    # Pasamos el run_id de Airflow como el batch_id
    run_id_str = "{{ run_id }}"
    
    ddl_task = setup_ddl()
    
    # Generación de features estáticas
    city_df = generate_city()
    stations_df = generate_stations()
    voronoi_df = generate_voronoi(city_df, stations_df)

    # Cargas de features estáticas (dependen de DDL y generación)
    load_city_op = load_city(city_df)
    load_stations_op = load_stations(stations_df)
    load_voronoi_op = load_voronoi(voronoi_df)
    
    # DDL debe ejecutarse antes de cargar datos estáticos
    ddl_task >> [load_city_op, load_stations_op, load_voronoi_op]
    
    # Visualización (solo depende de los datos generados, no de las cargas)
    visualize_op = visualize_task(city_df, stations_df, voronoi_df)

    # --- Extracción de datos dinámicos (pueden empezar después del DDL) ---
    trips_paths = extract_trips(batch_id=run_id_str)
    traffic_paths = extract_traffic(batch_id=run_id_str)
    weather_paths = extract_weather(batch_id=run_id_str)
    
    # Las extracciones deben esperar a que el DDL esté listo
    ddl_task >> [trips_paths, traffic_paths, weather_paths]

    # --- Cargas de datos dinámicos (dependen de las extracciones) ---
    trips_loaded = load_trips(trips_paths)
    locations_loaded = load_locations(trips_paths)
    traffic_loaded = load_traffic(traffic_paths)
    regions_loaded = load_traffic_regions(traffic_paths)
    weather_loaded = load_weather(weather_paths)

    # --- Verificación final ---
    all_paths = combine_paths(trips_paths, traffic_paths, weather_paths)
    
    # combine_paths debe ejecutarse después de las extracciones
    [trips_paths, traffic_paths, weather_paths] >> all_paths
    
    # Verificación depende de que TODAS las cargas terminen
    verification = verify_load(all_paths)
    [trips_loaded, locations_loaded, traffic_loaded, regions_loaded, weather_loaded] >> verification

coldstart_etl_pipeline()