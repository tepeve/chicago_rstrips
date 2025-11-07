from chicago_rstrips.socrata_api_client import fetch_data_from_api
from chicago_rstrips.config import START_DATE, END_DATE
from chicago_rstrips.utils import get_raw_data_dir
import pandas as pd

# Definir la query SoQL
soql_query = f"""
SELECT
  trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, 
  trip_miles, percent_time_chicago, percent_distance_chicago, 
  shared_trip_authorized, trips_pooled,
  pickup_centroid_location, dropoff_centroid_location,
  pickup_community_area, dropoff_community_area, fare, tip, 
  additional_charges, trip_total
WHERE
  trip_start_timestamp BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND trip_id LIKE '%a0'
  AND percent_time_chicago = 1
  LIMIT 100
"""
def transform_data_types(df):
    """
    Convierte los tipos de datos del DataFrame a los tipos correctos.
    
    Args:
        df (pd.DataFrame): DataFrame con datos de la API
        
    Returns:
        pd.DataFrame: DataFrame con tipos corregidos
    """
    # Definir el esquema de tipos
    type_mapping = {
        # IDs y strings
        'trip_id': 'string',
        
        # Timestamps
        'trip_start_timestamp': 'datetime64[ns]',
        'trip_end_timestamp': 'datetime64[ns]',
        
        # Numéricos enteros
        'trip_seconds': 'Int64',  # Nullable integer
        'pickup_community_area': 'Int64',
        'dropoff_community_area': 'Int64',
        'trips_pooled': 'Int64',
        
        # Numéricos decimales
        'trip_miles': 'float64',
        'percent_time_chicago': 'float64',
        'percent_distance_chicago': 'float64',
        'fare': 'float64',
        'tip': 'float64',
        'additional_charges': 'float64',
        'trip_total': 'float64',
        
        # Booleanos
        'shared_trip_authorized': 'boolean',
        
        # Geolocation (mantener como string o parsear JSON)
        'pickup_centroid_location': 'string',
        'dropoff_centroid_location': 'string',
    }
    
    # Aplicar conversiones
    for col, dtype in type_mapping.items():
        if col in df.columns:
            try:
                if dtype.startswith('datetime'):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                elif dtype == 'boolean':
                    # 1. Mapear valores string
                    map_dict = {'true': True, 'false': False, True: True, False: False}
                    # 2. Convertir a tipo 'boolean' nullable para manejar nulos
                    df[col] = df[col].map(map_dict).astype('boolean')
                
                elif dtype == 'Int64':
                    # Nullable integer (maneja NaN/None)
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

                elif dtype == 'float64':
                    # *** ARREGLO IMPORTANTE ***
                    # Usar pd.to_numeric con errors='coerce' para floats
                    # Esto convierte '12.xx' en NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                elif dtype == 'string':
                    # Asignar explícitamente a string
                    df[col] = df[col].astype('string')
                
                # Quitado el 'else' genérico que causaba el error

            except Exception as e:
                print(f"Advertencia: No se pudo convertir columna '{col}' a {dtype}: {e}")
    
    return df


def extract_trips_data(output_filename="raw_trips_data.parquet"):
    """
    Extrae datos de trips y los guarda en formato parquet.
    
    Args:
        output_filename (str): Nombre del archivo de salida
        
    Returns:
        Path: Ruta del archivo guardado o None si no hay datos
    """
    # Llamar a la función para obtener los datos
    df = fetch_data_from_api(soql_query)
    
    # Convertir a parquet y almacenar en una base de datos si hay datos
    if df is not None:
        print(f"\nSe encontraron {len(df)} resultados.")
        
        # NUEVO: Transformar tipos de datos
        print("\n--- Transformando tipos de datos ---")
        df = transform_data_types(df)
        print("Tipos de datos después de transformación:")
        print(df.dtypes)
        
        print("\n--- Vista previa del DataFrame ---")
        print(df.head())
        
        # Construir path usando utils
        output_path = get_raw_data_dir() / output_filename
        df.to_parquet(output_path, index=False)
        print(f"Datos guardados en {output_path}")
        return str(output_path)
    else:
        print("No se encontraron datos para guardar.")
        return None

# Para ejecutar como script independiente
if __name__ == "__main__":
    extract_trips_data()