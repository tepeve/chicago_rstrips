from airflow.decorators import dag, task
from pendulum import datetime

from chicago_rstrips.extract_raw_trips_data import extract_trips_data
from chicago_rstrips.load_raw_trips_data import load_trip_data_to_postgres
from chicago_rstrips.load_centroid_locations import load_centroid_locations_to_postgres
from chicago_rstrips.utils import get_outputs_dir, get_raw_data_dir


@dag(
    dag_id="chicago_trips_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "trips", "incremental"],
    description="Pipeline ETL para viajes de Chicago con dimensión de ubicaciones",
    default_args={
        "owner": "tepeve",
        "retries": 1,
    }
)
def etl_pipeline():
    """
    Pipeline ETL completo para datos de trips:
    1. Extrae datos de la API Socrata
    2. Construye/actualiza dimensión de ubicaciones
    3. Carga trips a staging
    4. Carga dimensión de ubicaciones
    5. Verifica integridad
    6. Genera reporte
    """

    @task
    def extract():
        """
        Extrae datos de trips y construye dimensión de ubicaciones.
        
        Returns:
            dict: Rutas de archivos generados
        """
        print("📥 Extrayendo datos de trips desde Socrata API...")
        
        # CAMBIO IMPORTANTE: Ahora genera AMBOS archivos
        trips_path = extract_trips_data(
            output_filename="raw_trips_data.parquet",
            build_locations=True,  # ← NUEVO: genera dim_centroid_location
            locations_strategy="incremental",  # ← NUEVO: actualiza incrementalmente
            locations_filename="centroid_locations.parquet"
        )
        
        if not trips_path:
            raise ValueError("No se pudo extraer datos de trips")
        
        # Construir path de locations
        raw_dir = get_raw_data_dir()
        locations_path = str(raw_dir / "centroid_locations.parquet")
        
        print(f"✓ Trips extraídos: {trips_path}")
        print(f"✓ Locations generadas: {locations_path}")
        
        return {
            'trips_path': trips_path,
            'locations_path': locations_path
        }

    @task
    def load_trips(paths: dict):
        """
        Carga datos de trips a staging.stg_raw_trips.
        
        Args:
            paths: Diccionario con rutas de archivos
        """
        print("💾 Cargando trips a PostgreSQL...")
        
        success = load_trip_data_to_postgres(
            parquet_path=paths['trips_path'],
            table_name="stg_raw_trips",
            ddl_path="create_stg_raw_trips.sql"
        )
        
        if not success:
            raise ValueError("Error al cargar trips a PostgreSQL")
        
        print("✓ Trips cargados exitosamente")
        return paths

    @task
    def load_locations(paths: dict):
        """
        Carga dimensión de ubicaciones a dim.dim_centroid_location.
        
        Args:
            paths: Diccionario con rutas de archivos
        """
        print("💾 Cargando dimensión de ubicaciones a PostgreSQL...")
        
        success = load_centroid_locations_to_postgres(
            parquet_path=paths['locations_path'],
            table_name="dim_centroid_location",
            ddl_path="create_dim_location_schema.sql"
        )
        
        if not success:
            raise ValueError("Error al cargar dimensión de ubicaciones")
        
        print("✓ Dimensión de ubicaciones cargada exitosamente")
        return paths

    @task
    def verify_load(paths: dict):
        """
        Verifica integridad de la carga.
        """
        from chicago_rstrips.db_loader import get_engine
        from sqlalchemy import text
        
        print("🔍 Verificando integridad de la carga...")
        
        engine = get_engine()
        try:
            with engine.connect() as conn:
                # 1. Contar trips
                result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_raw_trips"))
                trips_count = result.fetchone()[0]
                print(f"✓ Trips en staging: {trips_count}")
                
                # 2. Contar ubicaciones
                result = conn.execute(text("SELECT COUNT(*) FROM dimlocation.dim_centroid_location"))
                locations_count = result.fetchone()[0]
                print(f"✓ Ubicaciones en dimensión: {locations_count}")
                
                # 3. Verificar integridad referencial (ubicaciones huérfanas)
                orphan_query = text("""
                    SELECT COUNT(*) FROM staging.stg_raw_trips t
                    WHERE 
                        (t.pickup_location_id IS NOT NULL 
                         AND NOT EXISTS (
                            SELECT 1 FROM dimlocation.dim_centroid_location d 
                            WHERE d.location_id = t.pickup_location_id
                         ))
                        OR 
                        (t.dropoff_location_id IS NOT NULL 
                         AND NOT EXISTS (
                            SELECT 1 FROM dimlocation.dim_centroid_location d 
                            WHERE d.location_id = t.dropoff_location_id
                         ))
                """)
                result = conn.execute(orphan_query)
                orphans = result.fetchone()[0]
                
                if orphans > 0:
                    print(f"⚠️  Advertencia: {orphans} trips con location_id huérfanos")
                else:
                    print("✓ Integridad referencial OK")
                
                # 4. Verificar que hay coordenadas parseadas
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM dimlocation.dim_centroid_location 
                    WHERE longitude IS NOT NULL AND latitude IS NOT NULL
                """))
                parsed = result.fetchone()[0]
                print(f"✓ Ubicaciones con coordenadas parseadas: {parsed}/{locations_count}")
                
                if trips_count == 0:
                    raise ValueError("No se cargaron trips!")
                
                return {
                    'trips_count': trips_count,
                    'locations_count': locations_count,
                    'orphans': orphans,
                    'parsed_locations': parsed
                }
                
        finally:
            engine.dispose()

    @task
    def generate_report(verification_result: dict):
        """
        Genera reporte JSON de la ejecución.
        """
        import json
        from datetime import datetime as dt
        
        print("📋 Generando reporte de ejecución...")
        
        report = {
            'timestamp': dt.now().isoformat(),
            'dag_id': 'chicago_trips_pipeline',
            'status': 'success',
            'metrics': verification_result,
            'data_quality': {
                'orphaned_locations': verification_result['orphans'],
                'parse_rate': f"{verification_result['parsed_locations']}/{verification_result['locations_count']}"
            }
        }
        
        outputs_dir = get_outputs_dir()
        report_path = outputs_dir / f"trips_etl_report_{dt.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"✓ Reporte guardado en: {report_path}")
        return str(report_path)

    # ====================================================================
    # FLUJO DEL DAG
    # ====================================================================
    # 1. Extraer datos
    paths = extract()
    
    # 2. Cargar en paralelo (trips y locations son independientes)
    trips_loaded = load_trips(paths)
    locations_loaded = load_locations(paths)
    
    # 3. Verificar después de que ambas cargas terminen
    verification = verify_load(paths)
    verification.set_upstream([trips_loaded, locations_loaded])
    
    # 4. Generar reporte
    generate_report(verification)


etl_pipeline()