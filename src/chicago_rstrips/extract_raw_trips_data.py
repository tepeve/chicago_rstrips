from chicago_rstrips.socrata_api_client import fetch_data_from_api
from chicago_rstrips.config import START_DATE, END_DATE
from chicago_rstrips.utils import get_raw_data_dir

# Definir la query SoQL
soql_query = f"""
SELECT
  trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, 
  trip_miles, percent_time_chicago, percent_distance_chicago, 
  pickup_community_area, dropoff_community_area, fare, tip, 
  additional_charges, trip_total, shared_trip_authorized, trips_pooled
WHERE
  trip_start_timestamp BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND trip_id LIKE '%a0'
  LIMIT 100
"""

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
        print("\n--- Vista previa del DataFrame ---")
        print(df.head())
        
        # Construir path usando utils
        output_path = get_raw_data_dir() / output_filename
        df.to_parquet(output_path, index=False)
        print(f"Datos guardados en {output_path}")
        return output_path
    else:
        print("No se encontraron datos para guardar.")
        return None

# Para ejecutar como script independiente
if __name__ == "__main__":
    extract_trips_data()