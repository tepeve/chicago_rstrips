import pandas as pd
import requests
import json
from chicago_rstrips.config import CHIC_TNP_API_URL, SOCRATA_APP_TOKEN

def fetch_data_from_api(soql_query):
    """
    Fetch data from the Socrata API using a SoQL query.

    Args:
        soql_query (str): The SoQL query to execute.

    Returns:
        pd.DataFrame: A DataFrame containing the results, or None if no data is returned.
    """
    headers = {
        "Accept": "application/json",
        "X-App-Token": SOCRATA_APP_TOKEN,
    }

    payload = {"query": soql_query}

    print(f"Realizando solicitud POST a: {CHIC_TNP_API_URL}")
    try:
        response = requests.post(CHIC_TNP_API_URL, headers=headers, json=payload)
        response.raise_for_status()

        results = response.json()
        print("Solicitud a la API exitosa")

        if isinstance(results, list) and len(results) > 0:
            df = pd.DataFrame(results)
            return df
        else:
            print("\nLa API devolvió un resultado vacío o un formato inesperado.")
            print("Respuesta recibida:")
            print(json.dumps(results, indent=2))
            return None

    except requests.exceptions.HTTPError as errh:
        print(f"Error HTTP: {errh}")
        print(f"Respuesta del servidor: {response.text}")
    except requests.exceptions.RequestException as err:
        print(f"Error Inesperado: {err}")

    return None