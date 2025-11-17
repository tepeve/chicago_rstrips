from multiprocessing import context
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from sqlalchemy import text
from datetime import timedelta

from chicago_rstrips.config import START_DATE, END_DATE, COLD_START_END_DATE
from chicago_rstrips.db_loader import get_engine, run_ddl

# ...existing code...

# Inicio/fin del rango alineado con batch_etl_pipeline (arranca después del coldstart)
batch_start_date = pendulum.parse(COLD_START_END_DATE).add(days=1).start_of('day')
batch_end_date = pendulum.parse(END_DATE).end_of('day')

default_args = {
    "owner": "tepeve",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

@dag(
    dag_id="dwh_transformation_pipeline",
    schedule="@daily",
    start_date=batch_start_date,
    end_date=batch_end_date,
    catchup=True,
    max_active_runs=1,
    tags=["dwh", "transform", "analytics"],
    description="Ejecuta transformaciones para poblar el DWH y crear data marts.",
    default_args=default_args,
)
def dwh_transformation_pipeline():

    # Espera el run de batch_etl_pipeline con LA MISMA execution_date -> mapping 1:1
    wait_for_batch_etl = ExternalTaskSensor(
        task_id="wait_for_batch_etl",
        external_dag_id="batch_etl_pipeline",
        external_task_id="generate_report",  # debe existir exactamente con este task_id en batch_etl_pipeline
        execution_date_fn=lambda dt: dt,     # alinear execution_date 1:1
        mode="reschedule",
        poke_interval=60,                    # chequear cada 60s
        timeout=60 * 60 * 2,                 # esperar hasta 2 horas
        allowed_states=["success"],
    )

    @task
    def setup_ddl():
        """
        Asegura que existan schemas / tablas de DWH antes de ejecutar los UPSERTs.
        Ejecuta el DDL que crea las tablas de hechos/dimensiones (idempotente).
        """
        print("Ejecutando DDL de creación de tablas DWH (si no existen)...")
        engine = get_engine()
        try:
            # Archivo que debe contener CREATE SCHEMA / CREATE TABLE IF NOT EXISTS para dwh.fact_*
            run_ddl(engine, "create_fact_tables.sql")
        finally:
            engine.dispose()
        print("✓ DDL de tablas DWH aplicado.")

    @task
    def populate_fact_tables(**context):
        """
        Ejecuta el UPSERT incremental pasando las fechas de la ventana de datos.
        """
        print("Poblando Fact Tables desde Staging (Incremental)...")
        
        # Extraer fechas del intervalo de ejecución (Data Interval)
        # Esto asegura que procesemos exactamente el mismo día que procesó el batch_etl
        start_date = context['data_interval_start']
        end_date = context['data_interval_end']
        
        print(f"Ventana de procesamiento: {start_date} -> {end_date}")

        # Diccionario de parámetros para SQL
        # Convertimos a string ISO para asegurar compatibilidad con PostgreSQL
        sql_params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        engine = get_engine()
        try:
            # Pasamos el nombre del archivo y los parámetros
            run_ddl(engine, "upsert_fact_tables.sql", params=sql_params)
        finally:
            engine.dispose()
        print("✓ Fact Tables pobladas.")

    @task
    def create_analytics_views():
        print("Creando vistas materializadas de analytics...")
        engine = get_engine()
        try:
            run_ddl(engine, "create_data_marts.sql")
        finally:
            engine.dispose()
        print("✓ Vistas de analytics creadas.")

    @task
    def refresh_analytics_views():
        print("Refrescando vistas materializadas...")
        engine = get_engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY dm_trips_hourly_pickup_stats;"))
                conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY dm_ml_features_wide;"))
        finally:
            engine.dispose()
        print("✓ Vistas de analytics refrescadas.")

    # --- Flujo ---
    setup_ddl_op = setup_ddl()
    populate_op = populate_fact_tables()
    create_views_op = create_analytics_views()
    refresh_views_op = refresh_analytics_views()

    # Orden correcto:
    # 1) Esperar batch
    # 2) Asegurar DDL (crea dwh.fact_* si faltan)
    # 3) Poblar facts (UPSERT)
    # 4) Crear/Refrescar vistas
    wait_for_batch_etl >> setup_ddl_op >> populate_op >> create_views_op >> refresh_views_op

dwh_transformation_pipeline()
# ...existing code...