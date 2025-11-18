import inspect
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from pathlib import Path

import sqlalchemy
from chicago_rstrips.config import (
    POSTGRES_LOCAL_USER,
    POSTGRES_LOCAL_PASSWORD,
    POSTGRES_LOCAL_HOST,
    POSTGRES_LOCAL_PORT,
    POSTGRES_LOCAL_DB
)

def get_engine():
    """Crear engine de SQLAlchemy con connection string de PostgreSQL."""
    db_url = (
        f"postgresql+psycopg2://{POSTGRES_LOCAL_USER}:{POSTGRES_LOCAL_PASSWORD}"
        f"@{POSTGRES_LOCAL_HOST}:{POSTGRES_LOCAL_PORT}/{POSTGRES_LOCAL_DB}"
    )
    return create_engine(db_url)


def run_ddl(engine, ddl_path, params=None):
    """
    Ejecuta un archivo DDL SQL en modo AUTOCOMMIT. Soporta parámetros opcionales.
    """
    ddl_full_path = Path(__file__).parent.parent.parent / "sql" / ddl_path
    if not ddl_full_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo DDL: {ddl_full_path}")
    
    with open(ddl_full_path, 'r', encoding='utf-8') as f:
        ddl = f.read()
    
    try:
        with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
            conn.execute(text(ddl), params if params is not None else {})
            
        print(f"✓ DDL ejecutado desde {ddl_path}")

    except Exception as e:
        print(f"ERROR al ejecutar DDL desde {ddl_path}: {e}")
        
        raise


def load_dataframe_to_postgres(
    df,
    table_name,
    schema=None,
    engine=None,
    if_exists="append",
    dtype=None
    ):
    """
    Carga un DataFrame a PostgreSQL de forma genérica.
    Maneja el ciclo de vida del engine (crea/cierra) si no se provee uno.
    """
    engine_created_locally = False
    if engine is None:
        engine = get_engine()
        engine_created_locally = True

    try:
        
        df_to_load = df 

        if if_exists == "replace":
            # Si es 'replace', borramos la tabla. pandas la creará de nuevo.
            print(f"Modo 'replace': Borrando tabla {schema}.{table_name}...")
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            with engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_table_name} CASCADE")
        
        elif if_exists == "append":
            # Si es 'append', necesitamos inspeccionar y filtrar columnas
            # para evitar errores (psycopg2.errors.UndefinedColumn).
            print(f"Modo 'append': Inspeccionando columnas de {schema}.{table_name}...")
            inspector = inspect(engine)
            try:
                db_columns_info = inspector.get_columns(table_name, schema=schema)
                db_columns = [col['name'] for col in db_columns_info]
            except sqlalchemy.exc.NoSuchTableError:
                print(f"ERROR: La tabla {schema}.{table_name} no existe para hacer 'append'.")
                raise 

            # Filtramos el df
            df_columns = list(df.columns)
            columns_to_load = [col for col in df_columns if col in db_columns]
            discarded_columns = [col for col in df_columns if col not in db_columns]            
            if discarded_columns:
                print(f"ADVERTENCIA: Se descartarán columnas (no en DB): {discarded_columns}")
            
            df_to_load = df[columns_to_load] 
                         
        # Cargar el DataFrame (df_to_load)
        df_to_load.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists=if_exists, # pandas manejará el 'replace' o 'append'
            index=False,
            method="multi",
            dtype=dtype
        )
        print(f"✓ {len(df_to_load)} registros cargados en {schema}.{table_name}")
        
    except Exception as e:
        print(f"Error al cargar datos a {schema}.{table_name}: {e}")
        raise
        
    finally:
        if engine_created_locally:
            engine.dispose()

def load_parquet_to_postgres(
    parquet_path,
    table_name,
    schema=None,
    engine=None,
    if_exists="append",
    dtype=None
):
    """
    wrapper de load_dataframe_to_postgres para cargar parquets a db
    Args:
        parquet_path: Path al archivo parquet
        el resto de los argumentos son los que requiere load_dataframe_to_postgres
    """
    if isinstance(parquet_path, str):
        parquet_path = Path(parquet_path)
    
    if not parquet_path.exists():
        raise FileNotFoundError(f"El archivo {parquet_path} no existe.")
    print(f"Leyendo archivo parquet: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"Se cargaron {len(df)} registros desde el archivo.")
    load_dataframe_to_postgres(
        df,
        table_name=table_name,
        schema=schema,
        engine=engine,
        if_exists=if_exists,
        dtype=dtype
    )
