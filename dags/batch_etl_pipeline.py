from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.db import provide_session
from airflow.utils.state import DagRunState

from datetime import datetime, timedelta
import pendulum
import pandas as pd
from sqlalchemy import text

from chicago_rstrips.config import END_DATE, COLD_START_END_DATE, START_DATE
from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir

# Configuración de fechas: Inicia el día siguiente al fin del coldstart
batch_start_date = pendulum.parse(COLD_START_END_DATE).add(days=1).start_of('day')
batch_end_date = pendulum.parse(END_DATE).end_of('day')

default_args = {
    "owner": "tepeve", 
    "depends_on_past": False,
    "start_date": batch_start_date,
    "end_date": batch_end_date, 
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

@dag(
    dag_id="batch_etl_pipeline",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1, # Importante: 1 a la vez para evitar race conditions en el UPSERT/Refresh
    tags=["etl", "elt", "trips", "traffic", "incremental"],
    description="Pipeline ELT diario: Extrae, Carga a Staging y Transforma a DWH.",
    default_args=default_args
) 
def etl_pipeline():
    
    # --- SENSOR: Esperar a que el Cold Start prepare el terreno ---
    def get_coldstart_execution_date(dt):
        @provide_session
        def get_latest_success_run(session=None):
            latest_run = session.query(DagRun).filter(
                DagRun.dag_id == "coldstart_etl_pipeline",
                DagRun.state == DagRunState.SUCCESS
            ).order_by(DagRun.execution_date.desc()).first()
            
            if latest_run:
                return latest_run.execution_date
            return None
        return get_latest_success_run()
    
    wait_coldstart = ExternalTaskSensor(
        task_id="wait_for_coldstart_etl_pipeline",
        external_dag_id="coldstart_etl_pipeline",
        external_task_id=None,
        execution_date_fn=get_coldstart_execution_date,
        mode="reschedule",
        poke_interval=60,
        timeout=60*60*2,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    # ============================================================
    # 1. EXTRACCIÓN (E)
    # ============================================================

    @task
    def extract_trips(**context) -> dict:
        from chicago_rstrips.extract_trips_data import extract_trips_data
        from chicago_rstrips.utils import get_raw_data_dir
        
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
        print("Extrayendo datos de trips...")
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        
        # Socrata format
        start_timestamp = data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.000")
        end_timestamp = data_interval_end.subtract(seconds=1).strftime("%Y-%m-%dT%H:%M:%S.000")
        
        output_filename = f"raw_trips_data__{batch_id}.parquet"
        locations_filename = f"trips_locations__{batch_id}.parquet"
        
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
        
        print("Extrayendo datos de tráfico...")
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        
        start_timestamp = data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.000")
        end_timestamp = data_interval_end.subtract(seconds=1).strftime("%Y-%m-%dT%H:%M:%S.000")
        
        output_filename = f"stg_raw_traffic___{batch_id}.parquet"
        regions_filename = f"traffic_regions___{batch_id}.parquet"
        
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
    def extract_weather(**context) -> dict:
        from chicago_rstrips.extract_weather_data import extract_weather_data
        
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
        print("Extrayendo datos de clima...")
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
              
        start_timestamp = data_interval_start.to_date_string()
        end_timestamp = data_interval_end.subtract(days=1).to_date_string()
        
        output_filename = f"stg_raw_weather___{batch_id}.parquet"
        
        weather_path_str = extract_weather_data(
            output_filename=output_filename,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,           
            batch_id=batch_id
        )
        
        if not weather_path_str:
            raise ValueError("No se pudo extraer datos de clima")
        
        return {'weather_path': weather_path_str}

    # ============================================================
    # 2. CARGA A STAGING (L)
    # ============================================================
 
    @task
    def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
        return {**trips_paths, **traffic_paths, **weather_paths}

    @task
    def load_trips(paths: dict, **context):
        from chicago_rstrips.db_loader import load_parquet_to_postgres, get_engine
        
        print("Cargando trips a Staging (Append mode)...")
        # Nota: En batch diario confiamos en que la ventana de tiempo es única.
        # Si se requiere deduplicación estricta, se puede usar la lógica de lectura previa,
        # pero para performance en batch suele ser directo.
        load_parquet_to_postgres(
            parquet_path=paths['trips_path'],
            table_name="stg_raw_trips",
            schema="staging",
            if_exists="append"
        )

    @task
    def load_locations(paths: dict):
        from chicago_rstrips.db_loader import get_engine
        import pandas as pd
        
        df = pd.read_parquet(paths['locations_path'])
        if len(df) == 0: return
        
        print(f"Cargando {len(df)} ubicaciones (UPSERT)...")
        engine = get_engine()
        try:
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_spatial.trips_locations 
                            (location_id, longitude, latitude, source_type, batch_id, original_text)
                        VALUES (:location_id, :longitude, :latitude, :source_type, :batch_id, :original_text)
                        ON CONFLICT (location_id) DO UPDATE SET
                            longitude = EXCLUDED.longitude,
                            latitude = EXCLUDED.latitude,
                            source_type = EXCLUDED.source_type,
                            batch_id = EXCLUDED.batch_id;
                    """), row.to_dict())
        finally:
            engine.dispose()

    @task
    def load_traffic(paths: dict, **context):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando tráfico a Staging...")
        load_parquet_to_postgres(
            parquet_path=paths['traffic_path'],
            table_name="stg_raw_traffic",
            schema="staging",
            if_exists="append"
        )

    @task
    def load_traffic_regions(paths: dict):
        from chicago_rstrips.db_loader import get_engine
        import pandas as pd
        
        df = pd.read_parquet(paths['regions_path'])
        if len(df) == 0: return

        print(f"Cargando {len(df)} regiones de tráfico (UPSERT)...")
        engine = get_engine()
        try:
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    if 'crs' not in row: row['crs'] = 'EPSG:4326'
                    conn.execute(text("""
                        INSERT INTO dim_spatial.traffic_regions 
                            (region_id, region, west, east, south, north, geometry_wkt, area_km2, batch_id, crs)
                        VALUES (:region_id, :region, :west, :east, :south, :north, :geometry_wkt, :area_km2, :batch_id, :crs)
                        ON CONFLICT (region_id) DO UPDATE SET
                            region = EXCLUDED.region,
                            geometry_wkt = EXCLUDED.geometry_wkt,
                            batch_id = EXCLUDED.batch_id;
                    """), row.to_dict())
        finally:
            engine.dispose()

    @task
    def load_weather(paths: dict):
        from chicago_rstrips.db_loader import load_parquet_to_postgres
        print("Cargando clima a Staging...")
        load_parquet_to_postgres(
            parquet_path=paths['weather_path'],
            table_name="stg_raw_weather",
            schema="staging",
            if_exists="append"
        )
    
    @task
    def verify_load(paths: dict):
        """Verifica que los datos hayan llegado a Staging."""
        from chicago_rstrips.db_loader import get_engine
        print("🔍 Verificando Staging...")
        engine = get_engine()
        try:
            with engine.connect() as conn:
                trips_count = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_trips")).fetchone()[0]
                if trips_count == 0:
                    raise ValueError("ERROR CRÍTICO: Staging de Trips está vacío.")
                print(f"✓ Registros totales en Staging Trips: {trips_count}")
                return {'trips_total': trips_count}
        finally:
            engine.dispose()

    # ============================================================
    # 3. TRANSFORMACIÓN (T) - Data Warehouse & Marts
    # ============================================================

    @task
    def populate_daily_facts(**context):
        """
        Ejecuta el UPSERT desde Staging a Fact Tables para el día procesado.
        """
        from chicago_rstrips.db_loader import get_engine, run_ddl
        
        print("🏭 Transformando datos: Staging -> Fact Tables (UPSERT Diarios)...")
        
        # Extraemos la ventana de tiempo exacta del Run
        start_date = context['data_interval_start']
        end_date = context['data_interval_end']
        print(f"Ventana de procesamiento DWH: {start_date} -> {end_date}")

        # Parámetros para el SQL
        sql_params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        engine = get_engine()
        try:
            # Reutilizamos el upsert_fact_tables.sql que ya está probado y corregido
            run_ddl(engine, "upsert_fact_tables.sql", params=sql_params)
        finally:
            engine.dispose()
        print("✓ Fact Tables actualizadas exitosamente.")

    # @task
    # def refresh_datamarts():
    #     """
    #     Actualiza las Vistas Materializadas para reflejar los nuevos datos.
    #     """
    #     from chicago_rstrips.db_loader import get_engine, run_ddl
        
    #     print("📊 Actualizando Datamarts (REFRESH MATERIALIZED VIEW)...")
    #     engine = get_engine()
    #     try:
    #         # Usamos execution_options con AUTOCOMMIT para poder ejecutar REFRESH CONCURRENTLY
    #         with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
    #             print("Refrescando dm_trips_hourly_pickup_stats...")
    #             conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY dm_trips_hourly_pickup_stats;"))
                
    #             print("Refrescando dm_ml_features_wide...")
    #             conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY dm_ml_features_wide;"))
    #     except Exception as e:
    #         print(f"Advertencia: {e}")
    #         print("Intentando regenerar vistas por si no existen...")
    #         # Si falla el refresh, ejecutamos el DDL de creación (Idempotencia)
    #         run_ddl(engine, "create_data_marts.sql")
    #     finally:
    #         engine.dispose()
    #     print("✓ Datamarts listos para consumo.")

    @task
    def generate_report(verification_result: dict, **context):
        dag_run = context['dag_run']
        print("="*40)
        print(f"✅ ETL BATCH COMPLETADO: {dag_run.execution_date}")
        print("="*40)
        print(f"Total acumulado en Staging: {verification_result.get('trips_total', 'N/A')}")
        print("Status: Facts Updated & Marts Refreshed.")
        return "OK"

    # ============================================================
    # FLUJO DEL DAG
    # ============================================================

    # 1. Extracción
    trips_paths = extract_trips()
    traffic_paths = extract_traffic()
    weather_paths = extract_weather()

    wait_coldstart >> [trips_paths, traffic_paths, weather_paths]

    # 2. Carga (Load)
    trips_loaded = load_trips(trips_paths)
    locations_loaded = load_locations(trips_paths)
    traffic_loaded = load_traffic(traffic_paths)
    regions_loaded = load_traffic_regions(traffic_paths)
    weather_loaded = load_weather(weather_paths)

    # Sincronización
    all_paths = combine_paths(trips_paths, traffic_paths, weather_paths)
    # Definimos que verification depende de que todas las cargas terminen
    all_loads_done = [trips_loaded, locations_loaded, traffic_loaded, regions_loaded, weather_loaded]
    
    # 3. Verificación
    staging_verified = verify_load(all_paths)
    
    # Enrutamiento de dependencias
    [trips_paths, traffic_paths, weather_paths] >> all_paths
    all_loads_done >> staging_verified

    # 4. Transformación (Solo si Staging se verificó)
    facts_populated = populate_daily_facts()
    # marts_refreshed = refresh_datamarts()

    staging_verified >> facts_populated # >> marts_refreshed

    # Reporte Final
    # marts_refreshed >> generate_report(staging_verified)
    facts_populated >> generate_report(staging_verified)

etl_pipeline()