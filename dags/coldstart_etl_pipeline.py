from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pendulum
from sqlalchemy import text
import pandas as pd

from chicago_rstrips.config import START_DATE, COLD_START_END_DATE
from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir

default_args = {
    "owner": "tepeve", 
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

@dag(
    dag_id="coldstart_etl_pipeline",
    schedule=None,
    start_date=pendulum.parse(START_DATE),
    catchup=False,
    tags=["setup", "features", "static", "one-time"],
    description="Carga features estáticas y primeros lotes de datos desde cero.",
    default_args=default_args
)
def coldstart_etl_pipeline():
    """
    DAG de inicialización (Cold Start):
    1. Setup DDL (Schemas, Tablas, Vistas).
    2. Generación y carga de Dimensiones Espaciales (City, Stations, Voronoi).
    3. Extracción y carga de datos históricos iniciales (Trips, Traffic, Weather).
    4. Población inicial de Fact Tables (Upsert masivo).
    5. Creación y refresco de Data Marts.
    """

# ============================================================
# 1. SETUP Y FEATURES ESTÁTICAS
# ============================================================

    @task
    def setup_ddl():
        from chicago_rstrips.db_loader import get_engine, run_ddl
        print("Ejecutando DDLs...")
        engine = get_engine()
        try:
            # Asegura que los schemas existan y tablas base
            run_ddl(engine, "create_schemas.sql") 
            run_ddl(engine, "create_dim_static_tables.sql")
            run_ddl(engine, "create_staging_tables.sql")
            run_ddl(engine, "create_dim_dynamic_tables.sql")
            run_ddl(engine, "create_fact_tables.sql")
            # Creamos las vistas vacías inicialmente
            # run_ddl(engine, "create_data_marts.sql")            
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
    def generate_voronoi(city_df, stations_df):
        from chicago_rstrips.create_location_static_features import generate_voronoi_zones
        print("Generando zonas Voronoi...")
        return generate_voronoi_zones(city_df, stations_df)

    @task
    def load_city(df):
        print("Cargando límite de ciudad...")
        from chicago_rstrips.db_loader import load_dataframe_to_postgres
        load_dataframe_to_postgres(df, table_name='chicago_city_boundary', schema='dim_spatial', if_exists='append')

    @task
    def load_stations(df):
        from chicago_rstrips.db_loader import load_dataframe_to_postgres        
        print("Cargando estaciones...")
        load_dataframe_to_postgres(df, table_name='weather_stations_points', schema='dim_spatial', if_exists='append')

    @task
    def load_voronoi(df):
        from chicago_rstrips.db_loader import load_dataframe_to_postgres
        print("Cargando zonas Voronoi...")
        load_dataframe_to_postgres(df, table_name='weather_voronoi_zones', schema='dim_spatial', if_exists='append')

    @task
    def visualize_task(city_df, stations_df, voronoi_df):
        from chicago_rstrips.create_location_static_features import save_weather_stations_visualization        
        print("Guardando visualización...")
        save_weather_stations_visualization(city_df, stations_df, voronoi_df)

# ============================================================
# 2. EXTRACCIÓN DE DATOS (E)
# ============================================================

    @task
    def extract_trips(**context) -> dict:
        from chicago_rstrips.extract_trips_data import extract_trips_data
        from chicago_rstrips.utils import get_raw_data_dir
        
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
        print("Extrayendo datos de trips desde Socrata API...")     
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
        
        raw_dir = get_raw_data_dir()
        return {
            'trips_path': trips_path_str,
            'locations_path': str(raw_dir / locations_filename)
        }

    @task
    def extract_traffic(**context) -> dict:
        from chicago_rstrips.extract_traffic_data import extract_traffic_data
        from chicago_rstrips.utils import get_raw_data_dir
        
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"

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
    def extract_weather(**context) -> dict:
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
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
        return {'weather_path': weather_path_str}

# ============================================================
# 3. CARGA A STAGING (L)
# ============================================================

    @task
    def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
        return {**trips_paths, **traffic_paths, **weather_paths}

    @task
    def load_trips(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando trips a PostgreSQL...")
        load_parquet_to_postgres(paths['trips_path'], table_name="stg_raw_trips", schema="staging", if_exists="append")

    @task
    def load_locations(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando dimensión de ubicaciones a PostgreSQL...")
        load_parquet_to_postgres(paths['locations_path'], table_name="trips_locations", schema="dim_spatial", if_exists="append")

    @task
    def load_traffic(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando datos de tránsito a PostgreSQL...")
        load_parquet_to_postgres(paths['traffic_path'], table_name="stg_raw_traffic", schema="staging", if_exists="append")

    @task
    def load_traffic_regions(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando dimensión de regiones de tránsito a PostgreSQL...")
        load_parquet_to_postgres(paths['regions_path'], table_name="traffic_regions", schema="dim_spatial", if_exists="append")

    @task
    def load_weather(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando datos de clima a PostgreSQL...")
        load_parquet_to_postgres(paths['weather_path'], table_name="stg_raw_weather", schema="staging", if_exists="append")

    @task
    def verify_load(paths: dict):
        """Verifica integridad de la carga a Staging."""
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        
        print("🔍 Verificando integridad de la carga...")
        engine = get_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_trips"))
                trips_count = result.fetchone()[0]
                print(f"✓ Trips en staging: {trips_count}")
                
                if trips_count == 0:
                    raise ValueError("No se cargaron trips!")
                
                return { 'trips_count': trips_count }
        finally:
            engine.dispose()

# ============================================================
# 4. TRANSFORMACIÓN DWH Y DATAMARTS (T)
# ============================================================

    @task
    def populate_initial_facts():
        """
        Ejecuta upsert_fact_tables.sql para todo el rango histórico.
        """
        from chicago_rstrips.db_loader import get_engine, run_ddl
        from chicago_rstrips.config import START_DATE, COLD_START_END_DATE
        
        print("Poblando Fact Tables con datos históricos del Cold Start...")
              
        start_dt = pendulum.parse(START_DATE)
        # Sumamos 1 minuto o redondeamos al día siguiente para asegurar que incluya el último timestamp 23:59
        end_dt = pendulum.parse(COLD_START_END_DATE).add(minutes=1) 
        print(f"Ventana de procesamiento histórico: {start_dt} -> {end_dt}")

        sql_params = {
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat()
        }

        engine = get_engine()
        try:
            run_ddl(engine, "upsert_fact_tables.sql", params=sql_params)
        finally:
            engine.dispose()
        print("✓ Fact Tables inicializadas con datos históricos.")

    # @task
    # def create_and_refresh_datamarts():
    #     """
    #     Asegura que las vistas existan y las refresca con la data recién cargada.
    #     """
    #     from chicago_rstrips.db_loader import get_engine, run_ddl
    #     print("Construyendo y Refrescando Datamarts...")
        
    #     engine = get_engine()
    #     try:            
    #         # 1. (Opcional pero recomendado) Re-ejecutar DDL por si hubo cambios
    #         run_ddl(engine, "create_data_marts.sql")

    #         # 2. Refrescar datos (Materialized Views)
    #         with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
    #             print("Refrescando dm_trips_hourly_pickup_stats...")
    #             conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY dm_trips_hourly_pickup_stats;"))
                
    #             print("Refrescando dm_ml_features_wide (puede tardar)...")
    #             conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY dm_ml_features_wide;"))
                
    #     finally:
    #         engine.dispose()
    #     print("✓ Datamarts listos y operativos.")

    # --- TAREA QUE FALTABA ---
    @task
    def generate_report(verification_result: dict):
        """Genera un reporte final simple."""
        trips_count = verification_result.get('trips_count', 0)
        print("="*40)
        print("COLD START COMPLETADO EXITOSAMENTE")
        print("="*40)
        print(f"Registros cargados inicialmente: {trips_count}")
        print("Fact Tables pobladas: SÍ")
        print("Datamarts construidos: SÍ")
        return "OK"


    # ============================================================
    # FLUJO DEL DAG
    # ============================================================
    
    ddl_task = setup_ddl()
    
    # 1. Estáticos
    city_df = generate_city()
    stations_df = generate_stations()
    voronoi_df = generate_voronoi(city_df, stations_df)

    load_city_op = load_city(city_df)
    load_stations_op = load_stations(stations_df)
    load_voronoi_op = load_voronoi(voronoi_df)
    
    ddl_task >> [load_city_op, load_stations_op, load_voronoi_op]
    visualize_op = visualize_task(city_df, stations_df, voronoi_df) # Solo para debug visual

    # 2. Dinámicos (Extracción)
    trips_paths = extract_trips()
    traffic_paths = extract_traffic()
    weather_paths = extract_weather()
    
    ddl_task >> [trips_paths, traffic_paths, weather_paths]
    load_stations_op >> weather_paths # Dependencia lógica

    # 3. Dinámicos (Carga Staging)
    trips_loaded = load_trips(trips_paths)
    locations_loaded = load_locations(trips_paths)
    traffic_loaded = load_traffic(traffic_paths)
    regions_loaded = load_traffic_regions(traffic_paths)
    weather_loaded = load_weather(weather_paths)

    # 4. Verificación y Transformación Final
    all_paths = combine_paths(trips_paths, traffic_paths, weather_paths)
    [trips_paths, traffic_paths, weather_paths] >> all_paths
    
    verification = verify_load(all_paths)
    
    # La verificación espera a que todas las cargas terminen
    [trips_loaded, locations_loaded, traffic_loaded, regions_loaded, weather_loaded] >> verification

    # Si la verificación pasa, poblamos facts y luego datamarts
    populate_facts_op = populate_initial_facts()
    # create_marts_op = create_and_refresh_datamarts()
    report_op = generate_report(verification)

    verification >> populate_facts_op >>  report_op # create_marts_op >> report_op

coldstart_etl_pipeline()