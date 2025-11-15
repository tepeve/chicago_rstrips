import pandas as pd
import hashlib
import json
import re
import ast

from chicago_rstrips.socrata_api_client import fetch_data_from_api
from chicago_rstrips.config import START_DATE, END_DATE, CHIC_TNP_API_URL
from chicago_rstrips.utils import get_raw_data_dir, transform_dataframe_types

from shapely import GEOSException, from_wkt

# ============================================================
## Definiciones previas
# ============================================================


# Defino el endpoint de la API
api_endpoint = CHIC_TNP_API_URL

# Mapeo de datatypes del DataFrame luego de la extracción en json desde la api
type_mapping = {
    # IDs y strings
    'trip_id': 'string',
    
    # Timestamps
    'trip_start_timestamp': 'datetime64[ns]',
    'trip_end_timestamp': 'datetime64[ns]',
    
    # Numéricos enteros
    'pickup_community_area': 'Int64',
    'dropoff_community_area': 'Int64',
    'trips_pooled': 'Int64',
    
    # Numéricos decimales
    'trip_seconds': 'float64',  # Nullable integer
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


# ============================================================
## Bloque para crear census tracts centroids id's
# ============================================================

def _parse_lon_lat(value):
    if value is None:
        return None, None
    try:
        point = from_wkt(str(value))
        return point.x, point.y
    except (GEOSException, TypeError, Exception):
        # Si el string NO es WKT (ej. es JSON), fallará
        # y devolverá None, None.
        return None, None

def _make_id(text, id_len):
    h = hashlib.sha1(str(text).encode("utf-8")).hexdigest()
    return h[:id_len]

def build_location_dimension(df: pd.DataFrame,
                             pickup_col: str = "pickup_centroid_location",
                             dropoff_col: str = "dropoff_centroid_location",
                             id_len: int = 16):
    vals = pd.concat([
        df.get(pickup_col, pd.Series(dtype="string")),
        df.get(dropoff_col, pd.Series(dtype="string")),
    ], ignore_index=True).dropna().astype("string").drop_duplicates()

    rows = []
    for txt in vals:
        lon, lat = _parse_lon_lat(txt)
        rows.append({
            "location_id": _make_id(txt, id_len=id_len),
            "original_text": str(txt),
            "longitude": lon,
            "latitude": lat,
            "source_type": "census_centroid"
        })
    dim_df = pd.DataFrame(rows).drop_duplicates(subset=["location_id"]).reset_index(drop=True)
    mapping = dict(zip(dim_df["original_text"], dim_df["location_id"]))
    return dim_df, mapping


def map_location_keys(df: pd.DataFrame,
                      mapping: dict,
                      pickup_col: str = "pickup_centroid_location",
                      dropoff_col: str = "dropoff_centroid_location"):
    trips_df = df.copy()
    trips_df["pickup_location_id"] = trips_df[pickup_col].map(mapping).astype("string")
    trips_df["dropoff_location_id"] = trips_df[dropoff_col].map(mapping).astype("string")
    return trips_df



def update_location_dimension(existing_dim: pd.DataFrame,
                              new_df: pd.DataFrame,
                              pickup_col: str = "pickup_centroid_location",
                              dropoff_col: str = "dropoff_centroid_location",
                              id_len: int = 16):
    # Extraer textos nuevos
    incoming = pd.concat([
        new_df.get(pickup_col, pd.Series(dtype="string")),
        new_df.get(dropoff_col, pd.Series(dtype="string")),
    ], ignore_index=True).dropna().astype("string").drop_duplicates()

    if existing_dim is None or existing_dim.empty:
        return build_location_dimension(new_df, pickup_col, dropoff_col, id_len)

    existing_texts = set(existing_dim["original_text"].astype(str))
    to_add = [t for t in incoming if t not in existing_texts]

    if not to_add:
        # No hay novedades
        mapping = dict(zip(existing_dim["original_text"], existing_dim["location_id"]))
        return existing_dim, mapping

    rows = []
    for txt in to_add:
        lon, lat = _parse_lon_lat(txt)
        rows.append({
            "location_id": _make_id(txt, id_len=id_len),
            "original_text": str(txt),
            "longitude": lon,
            "latitude": lat,
            "source_type": "census_centroid"
        })
    add_df = pd.DataFrame(rows)
    updated = pd.concat([existing_dim, add_df], ignore_index=True)
    updated = updated.drop_duplicates(subset=["location_id"]).reset_index(drop=True)
    mapping = dict(zip(updated["original_text"], updated["location_id"]))
    return updated, mapping



# ============================================================
## Bloque extract trips data
# ============================================================

def extract_trips_data(output_filename="raw_trips_data.parquet",
                       build_locations: bool = False,
                       locations_strategy: str = "incremental",  # 'incremental' | 'rebuild'
                       locations_filename: str = "centroid_locations.parquet",
                       start_timestamp: str = None,
                       end_timestamp: str = None):
    """
    Extrae datos de trips y los guarda en formato parquet.
    
    Args:
        output_filename (str): Nombre del archivo de salida
        
    Returns:
        Path: Ruta del archivo guardado o None si no hay datos
    """
    # Usar fechas de config si no se proveen
    start_timestamp = start_timestamp if start_timestamp else START_DATE
    end_timestamp = end_timestamp if end_timestamp else END_DATE

    # Defino la query SoQL
    soql_query = f"""
    SELECT
    trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, 
    trip_miles, percent_time_chicago, percent_distance_chicago, 
    shared_trip_authorized, trips_pooled,
    pickup_centroid_location, dropoff_centroid_location,
    pickup_community_area, dropoff_community_area, fare, tip, 
    additional_charges, trip_total
    WHERE
    trip_start_timestamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
    AND trip_id LIKE '%a0'
    AND percent_time_chicago = 1
    AND pickup_centroid_location IS NOT NULL
    AND dropoff_centroid_location IS NOT NULL
    """



    # Llamar a la función para obtener los datos
    df = fetch_data_from_api(soql_query,api_endpoint)
    
    # Convertir a parquet y almacenar en una base de datos si hay datos
    if df is not None:
        print(f"\nSe encontraron {len(df)} resultados.")
        
        # NUEVO: Transformar tipos de datos
        print("\n--- Transformando tipos de datos ---")
        df = transform_dataframe_types(df,type_mapping)
        print("Tipos de datos después de transformación:")
        print(df.dtypes)

        print("\n--- Aplicando filtro para dejar solamente las columnas que definimos en type_mapping ---")
        print(f"Columnas antes del filtro: {list(df.columns)}")
        api_columns_to_keep = list(type_mapping.keys())
        df = df[api_columns_to_keep]
        print(f"Columnas después del filtro: {list(df.columns)}")

        print("\n--- Vista previa del DataFrame ---")
        print(df.head())
        
        # Construir path usando utils
        raw_dir = get_raw_data_dir()
        trips_path = raw_dir / output_filename

        if build_locations:
            loc_path = raw_dir / locations_filename
            if locations_strategy == "rebuild" or not loc_path.exists():
                dim_df, mapping = build_location_dimension(df)
            else:
                existing_dim = pd.read_parquet(loc_path) if loc_path.exists() else None
                dim_df, mapping = update_location_dimension(existing_dim, df)
            trips_df = map_location_keys(df, mapping)


            trips_df.to_parquet(trips_path, index=False)
            dim_df.to_parquet(loc_path, index=False)
            print(f"Parquet viajes (sin geometrías): {trips_path}")
            print(f"Parquet dimensión ubicaciones: {loc_path}")
        else:
            # Guardar dataset crudo con geometrías intactas
            df.to_parquet(trips_path, index=False)
            print(f"Parquet viajes (crudo con geometrías): {trips_path}")

        return str(trips_path)
        
    else:
        print("No se encontraron datos para guardar.")
        return None

# Para ejecutar como script independiente
if __name__ == "__main__":
    extract_trips_data()