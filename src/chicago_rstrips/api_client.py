import pandas as pd
import requests
import json
from chicago_rstrips.config import START_DATE, END_DATE, CHIC_TNP_API_URL, SOCRATA_APP_TOKEN




###########  API REQUEST ##############

## METADATA DEL ENDPOINT
#https://dev.socrata.com/foundry/data.cityofchicago.org/6dvr-xwnh

# PARA LA EJECUTAR LA SOLICITUD A LA API USAMOS EL PAQUETE REQUESTS DE PYTHON
# VAMOS A ENVIAR LA CONSULTA MEDIANTE EL MÉTODO POST (RECOMENDADO PARA SOCRATA V3)
# CARGAMOS APP TOKEN, FECHAS Y URL DESDE ENTORNO A TRAVÉS DE CONFIG.PY

# DEFINIMOS HEADERS DE LA SOLICITUD
headers = {
    "Accept": "application/json",
    "X-App-Token": SOCRATA_APP_TOKEN,
}

# ESCRIBIMOS LOS PARÁMETROS DE LA CONSULTA 
# PARA ARMAR LA QUERY USAMOS SoQL (Query Language de Socrata)

# soql_query = """
# SELECT
#   `trip_id`, `trip_start_timestamp`, `trip_end_timestamp`, `trip_seconds`, 
#   `trip_miles`, `percent_time_chicago`, `percent_distance_chicago`, 
#   `pickup_community_area`, `dropoff_community_area`, `fare`, `tip`, 
#   `additional_charges`, `trip_total`, `shared_trip_authorized`, `trips_pooled`
# WHERE
# trip_start_timestamp BETWEEN '{start_date}' AND '{end_date}'
# AND trip_id LIKE '%a0'   
# """

soql_query = f"""
SELECT
  COUNT(*) AS total_registros_sampling
WHERE
  trip_start_timestamp BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND trip_id LIKE '%a0'
"""

# EL PAYLOAD SÓLO DEBE CONTENER EL PARÁMETRO "query" para este endpoint
payload = {
    "query": soql_query,
} 

# EJECUTAMOS LA CONSULTA EN LA API CON EL MÉTODO POST
print(f"Realizando solicitud POST a: {CHIC_TNP_API_URL}")
try:
    # Realizar solicitud POST
    response = requests.post(CHIC_TNP_API_URL, headers=headers, json=payload)

    # VERIFICAMOS SI HUBO ERRORES HTTP 
    response.raise_for_status()

    # OBTENEMOS LOS RESULTADOS
    results = response.json()
    print("Solicitud a la API exitosa")
    
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