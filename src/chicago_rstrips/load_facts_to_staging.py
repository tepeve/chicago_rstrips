import pandas as pd
from sqlalchemy import create_engine, String, Integer, Float, Boolean, DateTime
from pathlib import Path
from chicago_rstrips.db_loader import get_engine,run_ddl,load_dataframe_to_postgres


def load_trip_data_to_postgres(parquet_path, table_name="stg_raw_trips", ddl_path="create_staging_tables.sql"):
    """
    Carga un archivo parquet a una tabla en PostgreSQL, ejecutando DDL si se provee.
    """
    try:
        if isinstance(parquet_path, str):
            parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            print(f"Error: El archivo {parquet_path} no existe.")
            return False

        print(f"Leyendo archivo parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"Se cargaron {len(df)} registros desde el archivo.")

        dtype_mapping = {
            'trip_id': String(50),
            'trip_start_timestamp': DateTime,
            'trip_end_timestamp': DateTime,
            'trip_seconds': Integer,
            'trip_miles': Float,
            'percent_time_chicago': Float,
            'percent_distance_chicago': Float,
            'pickup_community_area': Integer,
            'dropoff_community_area': Integer,
            'fare': Float,
            'tip': Float,
            'additional_charges': Float,
            'trip_total': Float,
            'shared_trip_authorized': Boolean,
            'trips_pooled': Integer,
            'pickup_location_id': String(20),
            'dropoff_location_id': String(20),
        }


        print(f"Cargando datos a la tabla '{table_name}'...")
        
        load_dataframe_to_postgres(
            df,
            table_name=table_name,
            schema="staging",
            dtype=dtype_mapping,
            ddl_path=ddl_path
        )

        print(f"✓ Datos cargados exitosamente a la tabla '{table_name}'")
        print(f"  - Registros insertados: {len(df)}")
        print(f"  - Columnas: {list(df.columns)}")

        return True

    except Exception as e:
        print(f"Error al cargar datos a PostgreSQL: {e}")
        return False



def load_traffic_data_to_postgres(parquet_path, table_name="stg_raw_traffic", ddl_path="create_staging_tables.sql"):
    """
    Carga un archivo parquet a una tabla en PostgreSQL, ejecutando DDL si se provee.
    """
    try:
        if isinstance(parquet_path, str):
            parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            print(f"Error: El archivo {parquet_path} no existe.")
            return False

        print(f"Leyendo archivo parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"Se cargaron {len(df)} registros desde el archivo.")

        dtype_mapping = {
            'time': DateTime,
            'region_id': Integer,
            'bus_count': Integer,
            'num_reads': Integer,
            'region': String,
            'description': String,
            'record_id': String,
            'speed': Float
        }


        print(f"Cargando datos a la tabla '{table_name}'...")
        
        load_dataframe_to_postgres(
            df,
            table_name=table_name,
            schema="staging",
            dtype=dtype_mapping,
            ddl_path=ddl_path
        )

        print(f"✓ Datos cargados exitosamente a la tabla '{table_name}'")
        print(f"  - Registros insertados: {len(df)}")
        print(f"  - Columnas: {list(df.columns)}")

        return True

    except Exception as e:
        print(f"Error al cargar datos a PostgreSQL: {e}")
        return False



# Para ejecutar como script independiente (útil para testing)
if __name__ == "__main__":
    from chicago_rstrips.utils import get_raw_data_dir

    raw_dir = get_raw_data_dir()
    parquet_files = list(raw_dir.glob("*.parquet"))

    if parquet_files:
        latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
        print(f"Cargando archivo: {latest_file}")
        # Ajusta la ruta al DDL según tu estructura
        ddl_path = "create_staging_schema.sql"
        load_trip_data_to_postgres(latest_file, ddl_path=ddl_path)
    else:
        print("No se encontraron archivos parquet en el directorio raw/")