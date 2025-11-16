from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.db import provide_session
from airflow.utils.state import DagRunState

from datetime import datetime, timedelta
import pendulum
import pandas as pd

from chicago_rstrips.config import END_DATE, COLD_START_END_DATE, START_DATE
from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir

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
    max_active_runs=3,
    tags=["etl", "trips", "traffic", "incremental"],
    description="Pipeline ETL diario para viajes, tráfico y clima de Chicago",
    default_args=default_args
) 
def etl_pipeline():
    """
    Pipeline ETL diario para datos de trips, tráfico y clima:
    1. Espera que coldstart_etl_pipeline haya terminado exitosamente.
    2. Extrae datos de Trips, Traffic y Weather para el día de ejecución.
    3. Carga los datos a PostgreSQL usando append.
    4. Verifica la carga y genera un reporte.
    """
    
    def get_coldstart_execution_date(dt):
        """
        Busca el run más reciente del coldstart_etl_pipeline que haya completado exitosamente.
        
        IMPORTANTE: Como coldstart usa schedule=None (manual trigger), su execution_date
        será la fecha del trigger manual, NO la start_date del DAG.
        
        dt: execution_date del batch_etl_pipeline actual (no se usa aquí)
        
        Retorna:
        - execution_date del último run exitoso de coldstart_etl_pipeline
        - None si no encuentra ninguno (el sensor fallará después del timeout)
        """
        
        @provide_session
        def get_latest_success_run(session=None):
            """Query a la base de datos de Airflow para encontrar el último run exitoso."""
            latest_run = session.query(DagRun).filter(
                DagRun.dag_id == "coldstart_etl_pipeline",
                DagRun.state == DagRunState.SUCCESS
            ).order_by(DagRun.execution_date.desc()).first()
            
            if latest_run:
                print(f"✓ Encontrado run exitoso de coldstart:")
                print(f"  - Run ID: {latest_run.run_id}")
                print(f"  - Execution Date: {latest_run.execution_date}")
                print(f"  - State: {latest_run.state}")
                return latest_run.execution_date
            else:
                print(f"No se encontró ningún run exitoso de coldstart_etl_pipeline. Ejecutá primero coldstart_etl_pipeline.")
                # Retornar None hará que el sensor falle explícitamente después del timeout
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

    @task
    def extract_trips(**context) -> dict:
        """Extrae datos de trips para el día actual."""
        from chicago_rstrips.extract_trips_data import extract_trips_data
        from chicago_rstrips.utils import get_raw_data_dir
        
        # Construir batch_id con run_id y try_number del contexto
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
        print("Extrayendo datos de trips desde Socrata API...")
        
        # Los plazos de la extracción se pasan como argumento **context
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        # hay que convertirlos al formato que acepta la api
        start_timestamp = data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.000")
        end_timestamp = data_interval_end.subtract(seconds=1).strftime("%Y-%m-%dT%H:%M:%S.000")
        
        print(f"Intervalo de extracción: {start_timestamp} → {end_timestamp}")
        print(f"batch_id: {batch_id}")
        
        # los output filenames incluyen el batch_id como sufijo
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
        """Extrae datos de tráfico para el día actual."""
        from chicago_rstrips.extract_traffic_data import extract_traffic_data
        from chicago_rstrips.utils import get_raw_data_dir
        
        # Construir batch_id con run_id y try_number del contexto
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
        print("Extrayendo datos de tráfico desde Socrata API...")
        
        # Los plazos de la extracción se pasan como argumento **context
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        # hay que convertirlos al formato que acepta la api        
        start_timestamp = data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.000")
        end_timestamp = data_interval_end.subtract(seconds=1).strftime("%Y-%m-%dT%H:%M:%S.000")
        
        print(f"Intervalo de extracción: {start_timestamp} → {end_timestamp}")
        print(f"batch_id: {batch_id}")
        
        # los output filenames incluyen el batch_id como sufijo
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
        """Extrae datos de clima para el día actual."""
        from chicago_rstrips.extract_weather_data import extract_weather_data
        
        # Construir batch_id con run_id y try_number del contexto
        run_id = context['run_id']
        try_number = context['ti'].try_number
        batch_id = f"{run_id}__try{try_number}"
        
        print("Extrayendo datos de clima desde Visual Crossing Weather API...")
        
        # Los plazos de la extracción se pasan como argumento **context
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        # hay que convertirlos al formato que acepta la api                
        start_timestamp = data_interval_start.to_date_string()
        end_timestamp = data_interval_end.subtract(days=1).to_date_string()
        print(f"Intervalo de extracción: {start_timestamp} → {end_timestamp}")
        print(f"batch_id: {batch_id}")
        
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
 
    @task
    def combine_paths(trips_paths: dict, traffic_paths: dict, weather_paths: dict) -> dict:
        """Combina los diccionarios de paths de trips, traffic y weather."""
        return {**trips_paths, **traffic_paths, **weather_paths}

    @task
    def load_trips(paths: dict, **context):
        from chicago_rstrips.db_loader import load_parquet_to_postgres, get_engine
        from sqlalchemy import text
        import pandas as pd
        
        print("Cargando trips a PostgreSQL con deduplicación...")
        
        # Leer el parquet
        df = pd.read_parquet(paths['trips_path'])
        print(f"Registros en parquet: {len(df)}")
        
        # Obtener trip_ids que ya existen en la DB para este rango de fechas
        engine = get_engine()
        try:
            data_interval_start = context['data_interval_start']
            data_interval_end = context['data_interval_end']
            
            with engine.connect() as conn:
                existing_trips = conn.execute(text("""
                    SELECT trip_id 
                    FROM staging.stg_raw_trips 
                    WHERE trip_start_timestamp >= :start 
                      AND trip_start_timestamp < :end
                """), {
                    'start': data_interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                    'end': data_interval_end.strftime("%Y-%m-%d %H:%M:%S")
                }).fetchall()
                
                existing_trip_ids = set([row[0] for row in existing_trips])
                print(f"Trips ya existentes en DB: {len(existing_trip_ids)}")
            
            # Filtrar solo trips nuevos
            df_new = df[~df['trip_id'].isin(existing_trip_ids)]
            print(f"Trips nuevos a cargar: {len(df_new)}")
            
            if len(df_new) > 0:
                load_parquet_to_postgres(
                    parquet_path=paths['trips_path'],
                    table_name="stg_raw_trips",
                    schema="staging",
                    if_exists="append"
                )
            else:
                print("No hay trips nuevos para cargar (todos ya existen)")
        finally:
            engine.dispose()

    @task
    def load_locations(paths: dict):
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        import pandas as pd
        
        print("Cargando dimensión de ubicaciones con UPSERT...")
        
        # Leer parquet
        df = pd.read_parquet(paths['locations_path'])
        print(f"Ubicaciones en parquet: {len(df)}")
        
        if len(df) == 0:
            print("No hay ubicaciones para cargar")
            return
        
        engine = get_engine()
        try:
            # Usar UPSERT (INSERT ... ON CONFLICT DO UPDATE)
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_spatial.trips_locations 
                            (location_id, longitude, latitude, source_type, batch_id, original_text)
                        VALUES 
                            (:location_id, :longitude, :latitude, :source_type, :batch_id, :original_text)
                        ON CONFLICT (location_id) 
                        DO UPDATE SET
                            longitude = EXCLUDED.longitude,
                            latitude = EXCLUDED.latitude,
                            source_type = EXCLUDED.source_type,
                            batch_id = EXCLUDED.batch_id,
                            original_text = EXCLUDED.original_text
                    """), {
                        'location_id': row['location_id'],
                        'longitude': row['longitude'],
                        'latitude': row['latitude'],
                        'source_type': row['source_type'],
                        'batch_id': row['batch_id'],
                        'original_text': str(row.get('original_text', ''))
                    })
            print(f"✓ {len(df)} ubicaciones cargadas/actualizadas (UPSERT)")
        finally:
            engine.dispose()

    @task
    def load_traffic(paths: dict, **context):
        from chicago_rstrips.db_loader import load_parquet_to_postgres, get_engine
        from sqlalchemy import text
        import pandas as pd
        
        print("Cargando traffic a PostgreSQL con deduplicación...")
        
        # Leer el parquet
        df = pd.read_parquet(paths['traffic_path'])
        print(f"Registros en parquet: {len(df)}")
        
        # Obtener registros que ya existen para evitar duplicados
        engine = get_engine()
        try:
            data_interval_start = context['data_interval_start']
            data_interval_end = context['data_interval_end']
            
            with engine.connect() as conn:
                # Verificar si ya hay datos para este rango de fechas
                existing_count = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM staging.stg_raw_traffic 
                    WHERE time >= :start 
                      AND time < :end
                """), {
                    'start': data_interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                    'end': data_interval_end.strftime("%Y-%m-%d %H:%M:%S")
                }).scalar()
                
                print(f"Registros de traffic ya existentes: {existing_count}")
            
            if existing_count == 0:
                load_parquet_to_postgres(
                    parquet_path=paths['traffic_path'],
                    table_name="stg_raw_traffic",
                    schema="staging",
                    if_exists="append"
                )
            else:
                print(f"ADVERTENCIA: Ya existen {existing_count} registros para este período. Saltando carga para evitar duplicados.")
        finally:
            engine.dispose()

    @task
    def load_traffic_regions(paths: dict):
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        import pandas as pd
        
        print("Cargando dimensión de regiones de tráfico con UPSERT...")
        
        # Leer parquet
        df = pd.read_parquet(paths['regions_path'])
        print(f"Regiones en parquet: {len(df)}")
        
        if len(df) == 0:
            print("No hay regiones para cargar")
            return
        
        engine = get_engine()
        try:
            # Usar UPSERT
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO dim_spatial.traffic_regions 
                            (region_id, region, west, east, south, north, geometry_wkt, area_km2, batch_id, crs)
                        VALUES 
                            (:region_id, :region, :west, :east, :south, :north, :geometry_wkt, :area_km2, :batch_id, :crs)
                        ON CONFLICT (region_id) 
                        DO UPDATE SET
                            region = EXCLUDED.region,
                            west = EXCLUDED.west,
                            east = EXCLUDED.east,
                            south = EXCLUDED.south,
                            north = EXCLUDED.north,
                            geometry_wkt = EXCLUDED.geometry_wkt,
                            area_km2 = EXCLUDED.area_km2,
                            batch_id = EXCLUDED.batch_id,
                            crs = EXCLUDED.crs
                    """), {
                        'region_id': row['region_id'],
                        'region': row['region'],
                        'west': row['west'],
                        'east': row['east'],
                        'south': row['south'],
                        'north': row['north'],
                        'geometry_wkt': row['geometry_wkt'],
                        'area_km2': row['area_km2'],
                        'batch_id': row['batch_id'],
                        'crs': row.get('crs', 'EPSG:4326')
                    })
            print(f"✓ {len(df)} regiones cargadas/actualizadas (UPSERT)")
        finally:
            engine.dispose()

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
    
    @task
    def verify_load(paths: dict):
        """Verifica integridad de la carga."""
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        
        print("🔍 Verificando integridad de la carga...")
        engine = get_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_trips"))
                trips_count = result.fetchone()[0]
                print(f"✓ Trips en staging: {trips_count}")
                
                result = conn.execute(text("SELECT COUNT(*) FROM dim_spatial.trips_locations"))
                locations_count = result.fetchone()[0]
                print(f"✓ Ubicaciones en dimensión: {locations_count}")
                
                if trips_count == 0:
                    raise ValueError("No se cargaron trips!")
                
                return {'trips_count': trips_count, 'locations_count': locations_count}
                
        finally:
            engine.dispose()

    @task
    def generate_report(verification_result: dict):
        """Genera reporte JSON de la ejecución."""
        print("📋 Reporte generado:")
        print(f"  - Trips cargados: {verification_result['trips_count']}")
        print(f"  - Ubicaciones en dim: {verification_result['locations_count']}")
        return "report.json"

    # ====================================================================
    # FLUJO DEL DAG
    # ====================================================================

    # Extracciones (batch_id se construye dentro de cada tarea usando el contexto)
    trips_paths = extract_trips()
    traffic_paths = extract_traffic()
    weather_paths = extract_weather()

    # TODAS las extracciones esperan al coldstart
    wait_coldstart >> [trips_paths, traffic_paths, weather_paths]

    # Cargas
    trips_loaded = load_trips(trips_paths)
    locations_loaded = load_locations(trips_paths)
    traffic_loaded = load_traffic(traffic_paths)
    regions_loaded = load_traffic_regions(traffic_paths)
    weather_loaded = load_weather(weather_paths)

    # Combinar paths y verificar
    all_paths = combine_paths(trips_paths, traffic_paths, weather_paths)
    all_loads_done = [trips_loaded, locations_loaded, traffic_loaded, regions_loaded, weather_loaded]
    
    verification = verify_load(all_paths)
    verification.set_upstream(all_loads_done)

    # Reporte final
    report = generate_report(verification)

etl_pipeline()