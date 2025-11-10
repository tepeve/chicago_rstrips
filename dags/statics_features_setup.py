from airflow.decorators import dag, task
from pendulum import datetime

from chicago_rstrips.load_geospatial_features import load_all_geospatial_features, get_engine
from chicago_rstrips.utils import get_outputs_dir


# ====================================================================
# DAG: Carga de features estáticas
# ====================================================================


@dag(
    dag_id="static_features_setup",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "features", "static", "one-time"],
    description="Pipeline de configuración inicial: carga features estáticas",
    default_args={
        "owner": "tepeve",
        "retries": 1,
    }
)
def static_features_setup():
    """
    DAG que orquesta la carga de features estáticas.
    
    Ahora simplificado: load_all_geospatial_features() se encarga de:
    - Generar features (si no se pasan como parámetros)
    - Ejecutar DDL completo desde SQL
    - Crear schema, tablas, PKs, FKs, constraints
    - Cargar los datos
    - Crear vistas
    - Guardar backups en parquet
    """

    @task
    def load_to_postgres():
        """
        Carga features a PostgreSQL.
        
        Esta función ahora encapsula TODO el proceso:
        - Genera features automáticamente
        - Ejecuta DDL
        - Carga datos
        - Crea backups
        """
        print("💾 Cargando features geoespaciales a PostgreSQL...")
        
        # Esta función ahora se encarga de TODO
        load_all_geospatial_features()
        
        print("✅ Features cargadas exitosamente")

    @task
    def verify_load():
        """
        Verifica que las features se cargaron correctamente.
        """
        from sqlalchemy import text
        
        print("🔍 Verificando carga en PostgreSQL...")
        
        engine = get_engine()
        try:
            with engine.connect() as conn:
                # Verificar conteos
                tables = {
                    'features.dim_weather_stations': 4,  # Esperamos 4 estaciones
                    'features.dim_voronoi_zones': 4,
                    'features.ref_city_boundary': 1
                }
                
                for table, expected_count in tables.items():
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    
                    if count != expected_count:
                        raise ValueError(f"{table}: esperaba {expected_count}, encontró {count}")
                    
                    print(f"✓ {table}: {count} registros")
                
                # Verificar vista
                result = conn.execute(text("SELECT COUNT(*) FROM features.vw_stations_with_zones"))
                view_count = result.fetchone()[0]
                print(f"✓ Vista combinada: {view_count} registros")
                
        finally:
            engine.dispose()

    @task
    def generate_report():
        """
        Genera reporte JSON de la carga.
        """
        from sqlalchemy import text
        import json
        from datetime import datetime as dt
        
        print("📋 Generando reporte...")
        
        engine = get_engine()
        report = {
            'timestamp': dt.now().isoformat(),
            'dag_id': 'static_features_setup',
            'status': 'success',
            'features': {}
        }
        
        try:
            with engine.connect() as conn:
                tables = [
                    'features.dim_weather_stations',
                    'features.dim_voronoi_zones',
                    'features.ref_city_boundary'
                ]
                
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    report['features'][table] = count
            
            output_dir = get_outputs_dir()
            report_path = output_dir / f"setup_report_{dt.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            print(f"✓ Reporte guardado en: {report_path}")
            return str(report_path)
            
        finally:
            engine.dispose()

    # ====================================================================
    # FLUJO DEL DAG (SIMPLIFICADO)
    # ====================================================================
    load_task = load_to_postgres()
    verify_task = verify_load()
    report_task = generate_report()
    
    # Flujo lineal: cargar → verificar → reportar
    load_task >> verify_task >> report_task


static_features_setup()