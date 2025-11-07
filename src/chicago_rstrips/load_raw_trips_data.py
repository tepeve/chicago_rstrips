import pandas as pd
from sqlalchemy import create_engine, String, Integer, Float, Boolean, DateTime
from pathlib import Path
from chicago_rstrips.config import (
    POSTGRES_LOCAL_USER,
    POSTGRES_LOCAL_PASSWORD,
    POSTGRES_LOCAL_HOST,
    POSTGRES_LOCAL_PORT,
    POSTGRES_LOCAL_DB
)


def load_data_to_postgres(parquet_path, table_name="stg_raw_trips"):
    """
    Carga un archivo parquet a una tabla en PostgreSQL.
    
    Args:
        parquet_path (Path o str): Ruta al archivo parquet
        table_name (str): Nombre de la tabla destino en PostgreSQL
        
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario
    """
    try:
        # Convertir a Path si es string
        if isinstance(parquet_path, str):
            parquet_path = Path(parquet_path)
        
        # Verificar que el archivo existe
        if not parquet_path.exists():
            print(f"Error: El archivo {parquet_path} no existe.")
            return False
        
        # Leer el archivo parquet
        print(f"Leyendo archivo parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"Se cargaron {len(df)} registros desde el archivo.")
        
        # NUEVO: Definir esquema explícito para PostgreSQL
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
            'pickup_centroid_location': String(200),
            'dropoff_centroid_location': String(200),
        }

        # Construir la URL de conexión a PostgreSQL
        db_url = (
            f"postgresql+psycopg2://{POSTGRES_LOCAL_USER}:{POSTGRES_LOCAL_PASSWORD}"
            f"@{POSTGRES_LOCAL_HOST}:{POSTGRES_LOCAL_PORT}/{POSTGRES_LOCAL_DB}"
        )
        
        # Crear engine de SQLAlchemy
        print(f"Conectando a PostgreSQL en {POSTGRES_LOCAL_HOST}:{POSTGRES_LOCAL_PORT}/{POSTGRES_LOCAL_DB}")
        engine = create_engine(db_url)
        
        # Cargar datos a PostgreSQL
        print(f"Cargando datos a la tabla '{table_name}'...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
            dtype=dtype_mapping # esquema explícito
        )
        
        print(f"✓ Datos cargados exitosamente a la tabla '{table_name}'")
        print(f"  - Registros insertados: {len(df)}")
        print(f"  - Columnas: {list(df.columns)}")
        
        # Cerrar conexión
        engine.dispose()
        
        return True
        
    except Exception as e:
        print(f"Error al cargar datos a PostgreSQL: {e}")
        return False


# Para ejecutar como script independiente (útil para testing)
if __name__ == "__main__":
    from chicago_rstrips.utils import get_raw_data_dir
    
    # Buscar el archivo parquet más reciente en raw/
    raw_dir = get_raw_data_dir()
    parquet_files = list(raw_dir.glob("*.parquet"))
    
    if parquet_files:
        # Tomar el archivo más reciente
        latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
        print(f"Cargando archivo: {latest_file}")
        load_data_to_postgres(latest_file)
    else:
        print("No se encontraron archivos parquet en el directorio raw/")