from airflow.decorators import dag, task
from pendulum import datetime

# Gracias al PYTHONPATH que definiste en Docker, esto funciona:
from chicago_rstrips.extract_raw_trips_data import extract_trips_data
from chicago_rstrips.load_raw_trips_data import load_data_to_postgres # (La función que crearás)

@dag(
    dag_id="chicago_trips_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def etl_pipeline():

    @task
    def extract():
        return extract_trips_data() # Tu función ya devuelve el path del archivo

    @task
    def load(parquet_path: str):
        if parquet_path:
            load_data_to_postgres(parquet_path) # Tu nueva función
        else:
            print("No se generó archivo, se omite la carga.")

    # Define la dependencia
    load( extract() )

# Llama al DAG para instanciarlo
etl_pipeline()