from airflow.decorators import dag, task
from pendulum import datetime
from chicago_rstrips.extract_weather_data import extract_weather_data
from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir

# --- 1. Imports Limpios ---
# Importar las funciones de extracción
from chicago_rstrips.extract_trips_data import extract_trips_data
from chicago_rstrips.extract_traffic_data import extract_traffic_data

# Importar los loaders GENÉRICOS y el runner DDL
from chicago_rstrips.db_loader import get_engine, run_ddl, load_parquet_to_postgres

# --- 2. Imports Redundantes ELIMINADOS ---
# (Ya no importamos nada de load_facts_to_staging ni load_locations)


@dag(
    dag_id="etl_main_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "trips", "incremental"],
    description="Pipeline ETL para viajes y tráfico de Chicago",
    default_args={"owner": "tepeve", "retries": 1}
)
def etl_pipeline():
    """
    Pipeline ETL simplificado para datos de trips y tráfico:
    1. Ejecuta DDLs para asegurar que las tablas de staging y dims existan.
    2. Extrae datos de Trips y Locations a Parquet.
    3. Extrae datos de Traffic y Regions a Parquet.
    4. Carga los 4 Parquets a PostgreSQL.
    5. Verifica la carga y genera un reporte.
    """

    @task
    def setup_ddl():
        """Ejecuta DDLs para Schemas, Staging y Dims Dinámicas."""
        print("Ejecutando DDLs...")
        engine = get_engine()
        try:
            run_ddl(engine, "create_schemas.sql")
            run_ddl(engine, "create_staging_tables.sql")
            run_ddl(engine, "create_dim_dynamic_tables.sql")
        finally:
            engine.dispose()
        print("✓ DDLs ejecutados.")

    @task
    def extract_trips() -> dict:
        print("Extrayendo datos de trips desde Socrata API...")     
        # Esta función ya guarda 'raw_trips_data.parquet' y 'trips_locations.parquet'
        trips_path_str = extract_trips_data(
            output_filename="raw_trips_data.parquet",
            build_locations=True, 
            locations_strategy="rebuild",
            locations_filename="trips_locations.parquet"
        )
        
        if not trips_path_str:
            raise ValueError("No se pudo extraer datos de trips")
        
        # Reconstruimos los paths para pasarlos explícitamente
        raw_dir = get_raw_data_dir()
        return {
            'trips_path': trips_path_str,
            'locations_path': str(raw_dir / "trips_locations.parquet")
        }

    @task
    def extract_traffic() -> dict:
        print("Extrayendo datos de tráfico desde Socrata API...")
        
        traffic_path_str = extract_traffic_data(
            output_filename="stg_raw_traffic.parquet",
            build_regions=True, 
            regions_strategy="rebuild",
            traffic_regions_filename="traffic_regions.parquet"
        )
        
        if not traffic_path_str:
            raise ValueError("No se pudo extraer datos de tráfico")

        raw_dir = get_raw_data_dir()
        return {
            'traffic_path': traffic_path_str,
            'regions_path': str(raw_dir / "traffic_regions.parquet")
        }
    
    @task
    def extract_weather() -> dict:
        print("Extrayendo datos de clima desde Visual Crossing Weather API...")
        
        weather_path_str = extract_weather_data(
            output_filename="stg_raw_weather.parquet",
        )
        
        if not weather_path_str:
            raise ValueError("No se pudo extraer datos de clima")
        return {
            'weather_path': weather_path_str
        }

    # --- Tareas de Carga ---

    @task
    def load_trips(paths: dict):
        print("Cargando trips a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['trips_path'],
            table_name="stg_raw_trips",
            schema="staging",
            if_exists="replace" # Usar append para staging inicial
        )

    @task
    def load_locations(paths: dict):
        print("Cargando dimensión de ubicaciones a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['locations_path'],
            table_name="trips_locations",
            schema="dim_spatial",
            if_exists="replace" # Usar replace para dimensiones (ya que es 'rebuild')
        )

    @task
    def load_traffic(paths: dict):
        print("Cargando datos de tránsito a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['traffic_path'],
            table_name="stg_raw_traffic",
            schema="staging",
            if_exists="replace" # Usar append para staging inicial
        )

    @task
    def load_traffic_regions(paths: dict):
        print("Cargando dimensión de regiones de tránsito a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['regions_path'],
            table_name="traffic_regions",
            schema="dim_spatial",
            if_exists="replace"
        )

    @task
    def load_weather(paths: dict):
        print("Cargando datos de clima a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['weather_path'],
            table_name="stg_raw_weather",
            schema="staging",
            if_exists="replace"
        )
    
    # --- Tareas de Verificación y Reporte (con bug corregido) ---
    
    @task
    def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
        """Combina los diccionarios de paths de trips, traffic y weather."""
        return {**trips_paths, **traffic_paths, **weather_paths}

    @task
    def verify_load(paths: dict):
        """Verifica integridad de la carga."""
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        
        print("🔍 Verificando integridad de la carga...")
        engine = get_engine()
        try:
            with engine.connect() as conn:
                # 1. Contar trips (BUG CORREGIDO)
                result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_trips")) # <-- "stg_raw_trips"
                trips_count = result.fetchone()[0]
                print(f"✓ Trips en staging: {trips_count}")
                
                # 2. Contar ubicaciones
                result = conn.execute(text("SELECT COUNT(*) FROM dim_spatial.trips_locations"))
                locations_count = result.fetchone()[0]
                print(f"✓ Ubicaciones en dimensión: {locations_count}")
                
                # ... (resto de tus verificaciones) ...
                
                if trips_count == 0:
                    raise ValueError("No se cargaron trips!")
                
                return { 'trips_count': trips_count, 'locations_count': locations_count }
                
        finally:
            engine.dispose()

    @task
    def generate_report(verification_result: dict):
        """Genera reporte JSON de la ejecución."""
        # ... (tu lógica de reporte es correcta) ...
        print("📋 Reporte generado.")
        return "report.json"

    # ====================================================================
    # FLUJO DEL DAG
    # ====================================================================
    
    # Crear todas las tablas PRIMERO
    ddl_task = setup_ddl()

    # Rama de Trips
    trips_paths = extract_trips()
    trips_loaded = load_trips(trips_paths)
    locations_loaded = load_locations(trips_paths)

    # Rama de Traffic
    traffic_paths = extract_traffic()
    traffic_loaded = load_traffic(traffic_paths)
    regions_loaded = load_traffic_regions(traffic_paths)
    
    # Rama de Weather
    weather_paths = extract_weather()
    weather_loaded = load_weather(weather_paths)

    # Dependencias de DDL: NADA se carga antes que las tablas existan
    ddl_task >> [trips_loaded, locations_loaded, traffic_loaded, regions_loaded, weather_loaded]
    
    # Dependencias de Extracción
    trips_paths >> [trips_loaded, locations_loaded]
    traffic_paths >> [traffic_loaded, regions_loaded]
    weather_paths >> weather_loaded

    # Verificar después de que TODAS las cargas terminen
    all_loads_done = [trips_loaded, locations_loaded, traffic_loaded, regions_loaded, weather_loaded]
    all_paths = combine_paths(trips_paths, traffic_paths, weather_paths)
    
    verification = verify_load(all_paths)
    verification.set_upstream(all_loads_done) # <-- Dependencia explícita

    # Generar reporte
    generate_report(verification)

etl_pipeline()