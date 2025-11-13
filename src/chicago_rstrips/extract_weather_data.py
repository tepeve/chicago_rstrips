import pandas as pd
from sqlalchemy import text
from chicago_rstrips.config import START_DATE, WEATHER_API_KEY
from chicago_rstrips.db_loader import get_engine
from chicago_rstrips.utils import get_raw_data_dir, transform_dataframe_types
from datetime import datetime, timedelta
import urllib.request
import urllib.error
import json

# Mapeo de datatypes del DataFrame luego de la extracción en json desde la api
type_mapping = {
    "record_id": 'string',
    "datetime": 'datetime64[ns]',
    "station_id": 'string',
    "temp": 'float64',
    "feelslike": 'float64',
    "precipprob": 'float64',
    "windspeed": 'string',
    "winddir": 'float64',
    "conditions": 'string',
    }   

def get_weather_stations():
    engine = get_engine()  
    query = text("""
        SELECT station_id, longitude, latitude
        FROM dim_spatial.weather_stations_points;
    """)

    df = pd.read_sql(query, engine)
    return df


def fetch_weather_api(location, start_date, end_date):
    """
    Fetch weather data from the Visual Crossing Weather API for a given location and date range.
    documentation: https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/
    
    Parameters:
    - location (str): The location for which to fetch weather data, typically in "latitude,longitude" format.
    - start_date (datetime): The start date of the weather data range.
    - end_date (datetime): The end date of the weather data range.

    Returns:
    - dict: The weather data returned by the API, or None if the request fails.
    """
    api_key = WEATHER_API_KEY
    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
    # Convert dates to string in YYYY-MM-DD format
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    url = (f"{base_url}{location}/{start_str}/{end_str}?"
           f"unitGroup=metric&"
           f"contentType=json&"
           f"options=nonulls&"
           f"include=days&"
           f"elements=datetime,latitude,longitude,temp,feelslike,precipprob,windspeed,"
           f"winddir,conditions&"
           f"key={api_key}")

    print(f"Haciendo la API Request para: {location}")
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read().decode()
            weather_data = json.loads(data)
            return weather_data
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e.code} - {e.reason}")
        return None
    except urllib.error.URLError as e:
        print(f"URLError: {e.reason}")
        return None


def extract_weather_data(output_filename="stg_raw_weather.parquet"):
    start_date = datetime.strptime(START_DATE[:10], "%Y-%m-%d")
    end_date = start_date + timedelta(days=2)
    stations_df = get_weather_stations()
    locations = [
        (row.station_id, f"{row.latitude},{row.longitude}")
        for _, row in stations_df.iterrows()
        if pd.notnull(row.latitude) and pd.notnull(row.longitude)
    ]
    all_weather = []

    for station_id, location in locations:
        weather_data = fetch_weather_api(location, start_date, end_date)
        if weather_data:
            print("Solicitud a la API exitosa")
            # Si la API devuelve un dict con 'days', extrae esa lista
            if isinstance(weather_data, dict) and 'days' in weather_data:
                for day in weather_data['days']:
                    day['station_id'] = station_id
                    # record_id: station_id + datetime
                    day['record_id'] = f"{station_id}_{day['datetime']}"
                    all_weather.append(day)
            elif isinstance(weather_data, list):
                for day in weather_data:
                    day['station_id'] = station_id
                    day['record_id'] = f"{station_id}_{day['datetime']}"
                    all_weather.append(day)
            else:
                print("\nLa API devolvió un formato inesperado.")
                print("Respuesta recibida:")
                print(json.dumps(weather_data, indent=2))
        else:
            print(f"Fallo al obtener datos para la ubicación: {location}")
    if all_weather:
        df = pd.DataFrame(all_weather)
        # aplicar transformaciones de tipos y nos quedamos con las columnas necesarias
        df = transform_dataframe_types(df,type_mapping)
        api_columns_to_keep = list(type_mapping.keys())
        df = df[api_columns_to_keep]
        # guardamos el parquet
        raw_dir = get_raw_data_dir()
        weather_path = raw_dir / output_filename
        df.to_parquet(weather_path, index=False)
        print(f"staging weather parquet guardado en: {weather_path}")
        return str(weather_path)


    else:
        print("No se obtuvieron datos de ninguna estación.")
        return None


if __name__ == "__main__":
    extract_weather_data()