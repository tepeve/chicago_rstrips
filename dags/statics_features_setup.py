from airflow.decorators import dag, task
from pendulum import datetime

from chicago_rstrips.chicago_voronoi_zones import generate_all_geospatial_features
from chicago_rstrips.load_geospatial_features import load_all_geospatial_features, get_engine
from chicago_rstrips.utils import get_outputs_dir


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
    
    Por ahora incluye:
    - chicago_voronoi_zones.generate_all_geospatial_features() → Generar geometrías
    - load_geospatial_features.load_all_geospatial_features() → Cargar a PostgreSQL (con DDL robusto)
    """

    @task
    def generate_spatial_features():
        """
        Genera todas las features geoespaciales usando el script modular.
        
        Returns:
            dict: Diccionario serializable con los datos para XCom
        """
        print("🗺️  Generando features geoespaciales...")
        
        features = generate_all_geospatial_features()
        
        # Convertir GeoDataFrames a diccionarios para XCom
        # (Airflow no puede serializar GeoDataFrames directamente)
        return {
            'city_count': len(features['city']),
            'stations_count': len(features['stations']),
            'zones_count': len(features['zones']),
            'success': True
        }

    @task
    def load_to_postgres(generation_result: dict):
        """
        Carga features a PostgreSQL usando la función robusta existente.
        
        Esta función:
        - Ejecuta el DDL completo desde SQL
        - Crea schema, tablas, PKs, FKs, constraints
        - Carga los datos
        - Crea vistas
        
        Args:
            generation_result: Resultado del task anterior (para dependencia)
        """
        print("💾 Cargando features a PostgreSQL...")
        
        if not generation_result.get('success'):
            raise ValueError("Generación de features falló")
        
        # Re-generar features (no podemos pasar GeoDataFrames por XCom)
        features = generate_all_geospatial_features()
        
        # Usar la función ROBUSTA que ya tienes
        load_all_geospatial_features(
            points_gdf=features['stations'],
            voronoi_gdf=features['zones'],
            city_gdf=features['city']
        )
        
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
                    
                    assert count == expected_count, f"{table}: esperaba {expected_count}, encontró {count}"
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
    # FLUJO DEL DAG
    # ====================================================================
    generation_result = generate_spatial_features()
    load_task = load_to_postgres(generation_result)
    verify_task = verify_load()
    report_task = generate_report()
    
    load_task >> verify_task >> report_task


static_features_setup()