import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv 

# 1. Cargar variables desde el archivo .env
load_dotenv()

# --- Configuración de la Solicitud ---

# Rango de fechas para un mes completo (Septiembre 2025)
start_date = '2025-09-01T00:00:00'
end_date = '2025-09-30T23:59:59'


# URL API ENDPOINT 
url = "https://data.cityofchicago.org/api/v3/views/6dvr-xwnh/query.json"

# CARGA APP TOKEN DESDE ENTONRNO 
app_token = os.getenv("SOCRATA_APP_TOKEN")

if not app_token:
    # Este error se mostrará si el .env no tiene la variable o si no se cargó correctamente.
    print("Error: La variable de entorno SOCRATA_APP_TOKEN no está configurada o no se cargó.")
    exit() 
    
    # 

# CONFIG HEADERS DE LA CONSULTA
headers = {
    "Accept": "application/json",
    "X-App-Token": app_token,
}

# METADATA DEL ENDPOINT
#https://dev.socrata.com/foundry/data.cityofchicago.org/6dvr-xwnh

# ESCRIMIMOS SoQL (Query Language de Socrata)
# soql_query = """
# SELECT
#   `trip_id`, `trip_start_timestamp`, `trip_end_timestamp`, `trip_seconds`, 
#   `trip_miles`, `percent_time_chicago`, `percent_distance_chicago`, 
#   `pickup_community_area`, `dropoff_community_area`, `fare`, `tip`, 
#   `additional_charges`, `trip_total`, `shared_trip_authorized`, `trips_pooled`
# LIMIT 5    
# """

soql_query = f"""
SELECT
  COUNT(*) AS total_registros_sampling
WHERE
  trip_start_timestamp BETWEEN '{start_date}' AND '{end_date}'
  AND trip_id LIKE '%a00'
"""

# EL PAYLOAD SÓLO DEBE CONTENER EL PARÁMETRO "query" para este endpoint
payload = {
    "query": soql_query,
} 


# EJECUTAMOS LA CONSULTA EN LA API CON EL MÉTODO POST

print(f"Realizando solicitud POST a: {url}")


try:
    # Realizar solicitud POST
    response = requests.post(url, headers=headers, json=payload)

    # VERIFICAMOS SI HUBO ERRORES HTTP 
    response.raise_for_status()

    # OBTENEMOS LOS RESULTADOS
    results = response.json()

    # 9. Imprimir los resultados de forma legible
    print("¡Solicitud exitosa!")
    
    # SOCRATA API v3 retorna una lista de diccionarios (objetos JSON).
    if isinstance(results, list) and len(results) > 0:
        # Crea el DataFrame de Pandas si hay datos
        df = pd.DataFrame(results)
        print(f"\nSe encontraron {len(df)} resultados.")
        print("\n--- Vista previa del DataFrame ---")
        print(df.head())
    else:
        # fallback si la consulta no devuelve filas
        # o si la API devuelve un error en formato JSON 
        print("\nLa API devolvió un resultado vacío o un formato inesperado.")
        print("Respuesta recibida:")
        print(json.dumps(results, indent=2))


except requests.exceptions.HTTPError as errh:
    print(f"Error HTTP: {errh}")
    # Imprime la respuesta del servidor (suele tener el error de Socrata)
    print(f"Respuesta del servidor: {response.text}")
except requests.exceptions.RequestException as err:
    print(f"Error Inesperado: {err}")