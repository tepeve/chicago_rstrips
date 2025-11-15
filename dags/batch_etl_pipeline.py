from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta
import pendulum
import pandas as pd

from chicago_rstrips.config import END_DATE, COLD_START_END_DATE


from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir

batch_start_date = pendulum.parse(COLD_START_END_DATE).add(days=1).start_of('day')
batch_end_date = pendulum.parse(END_DATE).end_of('day')

default_args = {"owner": "tepeve", 
                 "depends_on_past": False,
                 "start_date": batch_start_date,
                 "end_date": batch_end_date, 
                 "retries":     1,
                 "retry_delay": timedelta(minutes=5)}

@dag(
    dag_id="batch_etl_pipeline",
    schedule_interval= "@daily",
    catchup=True,
    tags=["etl", "trips", "traffic","incremental"],
    description="Pipeline ETL diario para viajes, tráfico y clima de Chicago",
    default_args= default_args
) 
def etl_pipeline():
    """
    Pipeline ETL simplificado para datos de trips, tráfico y clima:
    1. Ejecuta DDLs para asegurar que las tablas de staging y dims existan.
    2. Extrae datos de Trips y Locations a Parquet.
    3. Extrae datos de Traffic y Regions a Parquet.
    4. Carga los 4 Parquets a PostgreSQL.
    5. Verifica la carga y genera un reporte.
    """
    wait_coldstart = ExternalTaskSensor(
        task_id="wait_for_coldstart_etl_pipeline",
        external_dag_id="coldstart_etl_pipeline",
        external_task_id=None,  # Espera a que termine el DAG completo
        # execution_delta=timedelta(days=-4),  # Ajusta según la diferencia real
        mode="reschedule",  # Libera el worker mientras espera
        poke_interval=60,
        timeout=60*60*2,  # 2 horas
        allowed_states=["success"],
        failed_states=["failed"],
        dag=None,
    )

    @task
    def extract_trips(batch_id) -> dict:
        from chicago_rstrips.extract_trips_data import extract_trips_data
        from chicago_rstrips.utils import get_raw_data_dir           
        print("Extrayendo datos de trips desde Socrata API...")     

        output_filename = f"raw_trips_data__{batch_id}.parquet"
        locations_filename = f"trips_locations__{batch_id}.parquet"
                
        from airflow.utils.context import get_current_context
        context = get_current_context()
        # Convertir a formato esperado: "2025-09-04T00:00:00.000"
        start_timestamp = context['data_interval_start'].strftime("%Y-%m-%dT%H:%M:%S.000")
        # Para el end: restar 1 segundo para que sea 23:59:59 del día anterior
        end_timestamp = (context['data_interval_end'].subtract(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.000")
        trips_path_str = extract_trips_data(
            output_filename=output_filename,
            build_locations=True, 
            locations_strategy="incremental",
            locations_filename=locations_filename,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            batch_id=batch_id            
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
    def extract_traffic(batch_id) -> dict:
        from chicago_rstrips.extract_traffic_data import extract_traffic_data
        from chicago_rstrips.utils import get_raw_data_dir         
        print("Extrayendo datos de tráfico desde Socrata API...")

        output_filename = f"stg_raw_traffic___{batch_id}.parquet"
        regions_filename = f"traffic_regions___{batch_id}.parquet"
        
        from airflow.utils.context import get_current_context
        context = get_current_context()
        # Convertir a formato esperado: "2025-09-04T00:00:00.000"
        start_timestamp = context['data_interval_start'].strftime("%Y-%m-%dT%H:%M:%S.000")
        # Para el end: restar 1 segundo para que sea 23:59:59 del día anterior
        end_timestamp = (context['data_interval_end'].subtract(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.000")
        traffic_path_str = extract_traffic_data(
            output_filename=output_filename,
            build_regions=True, 
            regions_strategy="incremental",
            traffic_regions_filename=regions_filename,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,          
            batch_id=batch_id
        )
        
        if not traffic_path_str:
            raise ValueError("No se pudo extraer datos de tráfico")

        raw_dir = get_raw_data_dir()
        return {
            'traffic_path': traffic_path_str,
            'regions_path': str(raw_dir / regions_filename)
        }
    
    @task
    def extract_weather(batch_id) -> dict:      
        from chicago_rstrips.extract_weather_data import extract_weather_data
        output_filename = f"stg_raw_weather___{batch_id}.parquet"
        print("Extrayendo datos de clima desde Visual Crossing Weather API...")
        from airflow.utils.context import get_current_context
        context = get_current_context()
        # Para weather, usar el día de ejecución (data_interval_start)
        # data_interval_end - 1 día para obtener el día correcto
        start_timestamp = context['data_interval_start'].to_date_string()
        end_timestamp = context['data_interval_end'].subtract(days=1).to_date_string()
        
        weather_path_str = extract_weather_data(
            output_filename=output_filename,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,           
            batch_id=batch_id
        )
        
        if not weather_path_str:
            raise ValueError("No se pudo extraer datos de clima")
        return {
            'weather_path': weather_path_str
        }
 
    @task
    def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
        """Combina los diccionarios de paths de trips, traffic y weather."""
        return {**trips_paths, **traffic_paths, **weather_paths}    


    # --- Tareas de Carga ---

    @task
    def load_trips(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando trips a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['trips_path'],
            table_name="stg_raw_trips",
            schema="staging",
            if_exists="append" 
        )

    @task
    def load_locations(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando dimensión de ubicaciones a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['locations_path'],
            table_name="trips_locations",
            schema="dim_spatial",
            if_exists="append" 
        )

    @task
    def load_traffic(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres        
        print("Cargando datos de tránsito a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['traffic_path'],
            table_name="stg_raw_traffic",
            schema="staging",
            if_exists="append" 
        )

    @task
    def load_traffic_regions(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres        
        print("Cargando dimensión de regiones de tránsito a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['regions_path'],
            table_name="traffic_regions",
            schema="dim_spatial",
            if_exists="append"
        )

    @task
    def load_weather(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando datos de clima a PostgreSQL...")
        load_parquet_to_postgres(
            parquet_path=paths['weather_path'],
            table_name="stg_raw_weather",
            schema="staging",
            if_exists="append"
        )
    
    # --- Tareas de Verificación y Reporte (con bug corregido) ---
    
    # @task
    # def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
    #     """Combina los diccionarios de paths de trips, traffic y weather."""
    #     return {**trips_paths, **traffic_paths, **weather_paths}

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
    run_id_str = "{{ run_id }}"

    # Rama de Trips
    trips_paths = extract_trips(batch_id=run_id_str)
    traffic_paths = extract_traffic(batch_id=run_id_str)
    weather_paths = extract_weather(batch_id=run_id_str)

    # NADA se carga antes que las tablas sean creadas por coldstart_etl_pipeline
    wait_coldstart >> [trips_paths, traffic_paths, weather_paths]

    # Loadeo de datos
    # Rama de Trips
    trips_loaded = load_trips(trips_paths)
    locations_loaded = load_locations(trips_paths)
    
    # Rama de Traffic
    traffic_loaded = load_traffic(traffic_paths)
    regions_loaded = load_traffic_regions(traffic_paths)
     # Rama de Weather
    weather_loaded = load_weather(weather_paths)

    #  combinamos los paths una vez que todas las extracciones han terminado
    all_paths = combine_paths(trips_paths, traffic_paths, weather_paths)
    
    # Agrupamos todas las tareas de carga
    all_loads_done = [
        trips_loaded, 
        locations_loaded, 
        traffic_loaded, 
        regions_loaded, 
        weather_loaded
    ]


    # La verificación espera a que todas las cargas terminen
    verification = verify_load(all_paths)
    verification.set_upstream(all_loads_done)

    # --- 4. Reporte Final ---
    report = generate_report(verification)

    # # Dependencias de Extracción
    # trips_paths >> [trips_loaded, locations_loaded]
    # traffic_paths >> [traffic_loaded, regions_loaded]
    # weather_paths >> weather_loaded

etl_pipeline()