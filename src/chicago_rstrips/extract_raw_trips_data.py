from chicago_rstrips.api_client import fetch_data_from_api
from chicago_rstrips.config import START_DATE, END_DATE

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

# Llamar a la función para obtener los datos
df = fetch_data_from_api(soql_query)

# Convertir a parquet y almacenar en una base de datos si hay datos
if df is not None:
    print(f"\nSe encontraron {len(df)} resultados.")
    print("\n--- Vista previa del DataFrame ---")
    print(df.head())
    df.to_parquet("../data/output.parquet", index=False)
    print("Datos guardados en ../data/output.parquet")
else:
    print("No se encontraron datos para guardar.")