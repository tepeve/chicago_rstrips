import pandas as pd
from sqlalchemy import String, Float, DateTime
from pathlib import Path
from chicago_rstrips.db_loader import load_dataframe_to_postgres
from chicago_rstrips.utils import get_raw_data_dir

def load_trips_locations_to_postgres(parquet_path=None, 
                                        table_name="trips_locations",
                                        ddl_path="create_dim_spatial_schema.sql"):
    """
    Carga la dimensión de ubicaciones desde parquet a PostgreSQL.
    Usa estrategia upsert simple: elimina y recrea.
    """
    try:
        if parquet_path is None:
            raw_dir = get_raw_data_dir()
            parquet_path = raw_dir / "trips_locations.parquet"
        
        if isinstance(parquet_path, str):
            parquet_path = Path(parquet_path)
        
        if not parquet_path.exists():
            print(f"Error: El archivo {parquet_path} no existe.")
            return False

        print(f"Leyendo dimensión de ubicaciones: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"Se cargaron {len(df)} ubicaciones únicas.")

        dtype_mapping = {
            'location_id': String(20),
            'original_text': String,
            'longitude': Float,
            'latitude': Float,
            'source_type': String(30),
        }

        load_dataframe_to_postgres(
            df,
            table_name=table_name,
            schema="dim_spatial",
            dtype=dtype_mapping,
            ddl_path=ddl_path
        )

        print(f"✓ Dimensión de ubicaciones cargada exitosamente a '{table_name}'")
        print(f"  - Ubicaciones únicas: {len(df)}")
        
        return True

    except Exception as e:
        print(f"Error al cargar dimensión de ubicaciones: {e}")
        return False
    

def load_traffic_regions_to_postgres(parquet_path=None, 
                                        table_name="traffic_regions",
                                        ddl_path="create_dim_spatial_schema.sql"):
    """
    Carga la dimensión de regiones de tráfico desde parquet a PostgreSQL.
    Usa estrategia upsert simple: elimina y recrea.
    """
    try:
        if parquet_path is None:
            raw_dir = get_raw_data_dir()
            parquet_path = raw_dir / "traffic_regions.parquet"
        
        if isinstance(parquet_path, str):
            parquet_path = Path(parquet_path)
        
        if not parquet_path.exists():
            print(f"Error: El archivo {parquet_path} no existe.")
            return False

        print(f"Leyendo dimensión de ubicaciones: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"Se cargaron {len(df)} ubicaciones únicas.")

        dtype_mapping = {
            'region_id': Integer,
            'region': String,
            'description': String,
            'west': Float,
            'east': Float,
            'south': Float,
            'north': Float,
            'geometry_wkt': String,
        }

        load_dataframe_to_postgres(
            df,
            table_name=table_name,
            schema="dim_spatial",
            dtype=dtype_mapping,
            ddl_path=ddl_path
        )

        print(f"✓ Dimensión de regiones cargada exitosamente a '{table_name}'")
        print(f"  - Regiones únicas: {len(df)}")
        
        return True

    except Exception as e:
        print(f"Error al cargar dimensión de regiones: {e}")
        return False

if __name__ == "__main__":
    load_trips_locations_to_postgres()
    load_traffic_regions_to_postgres()