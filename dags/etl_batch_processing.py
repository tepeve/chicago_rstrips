# from airflow.decorators import dag, task
# from airflow.sensors.external_task import ExternalTaskSensor
# from pendulum import datetime
# import pandas as pd

# # Importar runners y utilidades
# from chicago_rstrips.db_loader import get_engine, run_ddl, load_parquet_to_postgres
# from chicago_rstrips.extract_trips_data import extract_trips_data
# from chicago_rstrips.extract_traffic_data import extract_traffic_data
# from chicago_rstrips.extract_weather_data import extract_weather_data
# from chicago_rstrips.utils import get_raw_data_dir

# @dag(
#     dag_id="etl_batch_processing_dwh",
#     start_date=datetime(2025, 9, 1),
#     schedule_interval="@daily",  # Ejecución diaria
#     catchup=True, # Permitir backfilling
#     max_active_runs=1,
#     tags=["dwh", "batch", "daily"],
#     description="DAG diario para procesar datos en lotes y cargarlos al DWH.",
#     default_args={"owner": "tepeve", "retries": 2}
# )
# def etl_batch_dwh():
#     """
#     ### DAG de Procesamiento por Lotes hacia el DWH
#     Este DAG orquesta la extracción diaria de datos, su carga en un área de
#     staging y finalmente su consolidación en las tablas de hechos del DWH.
#     Utiliza una estrategia de upsert para garantizar la idempotencia.
#     """

#     @task
#     def setup_dwh_tables():
#         """Crea las tablas del DWH si no existen."""
#         engine = get_engine()
#         try:
#             # Asume que las tablas de staging y dims ya existen.
#             # Solo crea las tablas del DWH.
#             run_ddl(engine, "create_dwh_tables.sql")
#         finally:
#             engine.dispose()

#     @task
#     def extract_and_stage_data(logical_date):
#         """
#         Extrae datos del día, los guarda en Parquet y los carga a staging.
#         Retorna los paths de los archivos generados.
#         """
#         # Lógica para sobreescribir START_DATE y END_DATE para la ejecución
#         # Aquí se debería implementar una lógica más robusta para pasar las fechas
#         # a las funciones de extracción. Por simplicidad, asumimos que las funciones
#         # pueden ser adaptadas para tomar un rango de fechas.
        
#         print(f"Procesando para el día: {logical_date}")
        
#         # NOTA: Las funciones de extracción actuales usan fechas del config.
#         # Se necesitaría refactorizarlas para que acepten `start_date` y `end_date` como parámetros.
#         # Por ahora, simulamos que lo hacen.
        
#         # 1. Extraer y cargar Trips
#         trips_path = extract_trips_data(output_filename=f"trips_{logical_date}.parquet", build_locations=True)
#         if trips_path:
#             load_parquet_to_postgres(trips_path, "stg_raw_trips", "staging", if_exists="replace")
#             loc_path = str(get_raw_data_dir() / "trips_locations.parquet")
#             load_parquet_to_postgres(loc_path, "trips_locations", "dim_spatial", if_exists="replace")

#         # 2. Extraer y cargar Traffic
#         traffic_path = extract_traffic_data(output_filename=f"traffic_{logical_date}.parquet", build_regions=True)
#         if traffic_path:
#             load_parquet_to_postgres(traffic_path, "stg_raw_traffic", "staging", if_exists="replace")
#             reg_path = str(get_raw_data_dir() / "traffic_regions.parquet")
#             load_parquet_to_postgres(reg_path, "traffic_regions", "dim_spatial", if_exists="replace")

#         # 3. Extraer y cargar Weather
#         weather_path = extract_weather_data(output_filename=f"weather_{logical_date}.parquet")
#         if weather_path:
#             load_parquet_to_postgres(weather_path, "stg_raw_weather", "staging", if_exists="replace")
            
#         return {"trips": trips_path, "traffic": traffic_path, "weather": weather_path}

#     @task
#     def transform_staging_to_dwh():
#         """Ejecuta el script SQL para mover datos de Staging a DWH."""
#         print("Moviendo datos de Staging a DWH con estrategia Upsert...")
#         engine = get_engine()
#         try:
#             run_ddl(engine, "populate_dwh_from_staging.sql")
#         finally:
#             engine.dispose()
#         print("✓ Datos consolidados en el DWH.")

#     # --- Flujo del DAG ---
#     setup_task = setup_dwh_tables()
#     extract_task = extract_and_stage_data("{{ ds }}") # Usamos la fecha lógica de Airflow
#     transform_task = transform_staging_to_dwh()

#     setup_task >> extract_task >> transform_task

# # Instanciar el DAG
# etl_batch_dwh()