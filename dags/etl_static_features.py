from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd

from chicago_rstrips.db_loader import get_engine, run_ddl, load_dataframe_to_postgres

from chicago_rstrips.create_location_static_features import (
    generate_city_boundary,
    generate_weather_stations,
    generate_voronoi_zones,
    save_weather_stations_visualization
)

@dag(
    dag_id="etl_statics_features",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "features", "static", "one-time"],
    description="Carga features estáticas (Zonas, Estaciones, Límites) desde cero.",
    default_args={"owner": "tepeve", "retries": 1}
)
def static_features_setup():
    """
    DAG que orquesta la carga de features estáticas:
    1. Ejecuta DDL para crear schemas y tablas estáticas.
    2. Genera los DataFrames de features en memoria.
    3. Carga los DataFrames a PostgreSQL usando el loader en db_loader.py.
    4. Guarda una visualización de las zonas Voronoi generadas.
    """

    @task
    def setup_ddl():
        print("Ejecutando DDLs...")
        engine = get_engine()
        try:
            # Asegura que los schemas existan
            run_ddl(engine, "create_schemas.sql") 
            # Crea las tablas estáticas (con IF NOT EXISTS)
            run_ddl(engine, "create_dim_static_tables.sql")
        finally:
            engine.dispose()
        print("✓ DDLs ejecutados.")

    @task
    def generate_city():
        print("Generando mapa de ciudad...")
        return generate_city_boundary()

    @task
    def generate_stations():
        print("Generando estaciones de clima...")
        return generate_weather_stations()

    @task
    # Vamos a pasar los df de city_boundary y weather_stations como parámetros de entrada como XComArgs
    def generate_voronoi(city_df, stations_df):
        print("Generando zonas Voronoi...")
        return generate_voronoi_zones(city_df, stations_df)

    @task
    def load_city(df):
        print("Cargando límite de ciudad...")
        load_dataframe_to_postgres(
            df,
            table_name='chicago_city_boundary',
            schema='dim_spatial',
            if_exists='replace' 
        )

    @task
    def load_stations(df):
        print("Cargando estaciones...")
        load_dataframe_to_postgres(
            df,
            table_name='weather_stations_points',
            schema='dim_spatial',
            if_exists='replace'
        )

    @task
    def load_voronoi(df):
        print("Cargando zonas Voronoi...")
        load_dataframe_to_postgres(
            df,
            table_name='weather_voronoi_zones',
            schema='dim_spatial',
            if_exists='replace'
        )
    
    @task
    def visualize_task(city_df, stations_df, voronoi_df):
        print("Guardando visualización...")
        save_weather_stations_visualization(city_df, stations_df, voronoi_df)

    # --- FLUJO DEL DAG ---
    
    ddl_task = setup_ddl()
    
    city_df = generate_city()
    stations_df = generate_stations()
    
    # Voronoi depende de ciudad y estaciones
    voronoi_df = generate_voronoi(city_df, stations_df)

    # Las cargas dependen de que el DDL esté listo y los datos generados
    load_city_op = load_city(city_df)
    load_stations_op = load_stations(stations_df)
    load_voronoi_op = load_voronoi(voronoi_df)
    
    # La visualización depende de que todo esté generado
    visualize_op = visualize_task(city_df, stations_df, voronoi_df)

    # Definir dependencias
    ddl_task >> [city_df, stations_df]
    [city_df, stations_df] >> voronoi_df
    
    [ddl_task, city_df] >> load_city_op
    [ddl_task, stations_df] >> load_stations_op
    [ddl_task, voronoi_df] >> load_voronoi_op
    
    # Las tareas de verificación y reporte por ahora comentado
    # verify_task.set_upstream([load_city_op, load_stations_op, load_voronoi_op])

static_features_setup()