import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
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

def run_ddl(engine, ddl_path):
    """
    Ejecuta un archivo DDL SQL.
    
    Args:
        engine: SQLAlchemy engine
        ddl_path: Nombre del archivo SQL (relativo a /sql/)
    """
    ddl_full_path = Path(__file__).parent.parent.parent / "sql" / ddl_path
    if not ddl_full_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo DDL: {ddl_full_path}")
    
    with open(ddl_full_path, 'r', encoding='utf-8') as f:
        ddl = f.read()
    
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
    
    print(f"✓ DDL ejecutado desde {ddl_path}")

def load_dataframe_to_postgres(
    df,
    table_name,
    schema=None,
    engine=None,
    if_exists="append",
    dtype=None,
    ddl_path=None
):
    """
    Carga un DataFrame a PostgreSQL de forma genérica.
    
    Args:
        df: DataFrame a cargar
        table_name: Nombre de la tabla
        schema: Schema de la tabla (opcional)
        engine: SQLAlchemy engine (si es None, se crea uno nuevo)
        if_exists: 'append', 'replace', 'fail'
        dtype: Diccionario de tipos de datos (opcional)
        ddl_path: Ruta al archivo DDL a ejecutar antes de cargar (opcional)
    """
    engine_created_locally = False
    if engine is None:
        engine = get_engine()
        engine_created_locally = True
    
    try:
        # Ejecutar DDL si se proporciona
        if ddl_path:
            run_ddl(engine, ddl_path)
        
        # Cargar DataFrame
        df.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            dtype=dtype
        )
        
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        print(f"✓ {len(df)} registros cargados en {full_table_name}")
        
    finally:
        # Solo dispose si el engine fue creado localmente
        if engine_created_locally:
            engine.dispose()