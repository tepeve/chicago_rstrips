
from chicago_rstrips.extract_trips_data import extract_trips_data
from chicago_rstrips.socrata_api_client import fetch_data_from_api
from chicago_rstrips.config import START_DATE, END_DATE, CHIC_TRAFFIC_API_URL
from chicago_rstrips.utils import get_raw_data_dir, transform_dataframe_types
import geopandas as gpd
from shapely.geometry import Polygon
import matplotlib.pyplot as plt
import pandas as pd

# Defino el endpoint de la API
api_endpoint = CHIC_TRAFFIC_API_URL

# Definir la query SoQL
soql_query = f"""
SELECT
  time, region_id, speed, region, bus_count, num_reads, hour, description, record_id, 
  west, east, north, south 
WHERE
  time BETWEEN '{START_DATE}' AND '{END_DATE}'
    AND region_id IS NOT NULL
ORDER BY time ASC
LIMIT 1000
"""

type_mapping = {

    # Timestamps
    'time': 'datetime64[ns]',
    
    # Numéricos enteros
    'region_id': 'Int64',  
    'bus_count': 'Int64',  
    'num_reads': 'Int64',  
    
    # Strings
    'region': 'string',
    'description': 'string',
    'record_id': 'string',
    
    # Numéricos decimales
    'speed': 'float64',
        
    # Geolocation (mantener como string o parsear JSON)
    'west': 'string',
    'east': 'string',
    'north': 'string',
    'south': 'string',
}



def build_location_dimension(df):
    """
    Construye la dimensión de regiones de tráfico como polígonos a partir de las columnas west, east, north, south.
    Devuelve un GeoDataFrame con columnas: region_id, region, description, geometry.
    """
    # Seleccionar columnas relevantes y eliminar duplicados
    regions = df[['region_id', 'region', 'description', 'west', 'east', 'south', 'north']].drop_duplicates()

    # Crear polígonos a partir de las coordenadas
    def make_poly(row):
        try:
            # Validar nulos y tipos
            for c in ['west', 'east', 'south', 'north']:
                if pd.isna(row[c]) or row[c] == '':
                    return None
            west = float(row['west'])
            east = float(row['east'])
            south = float(row['south'])
            north = float(row['north'])
            if not (west < east and south < north):
                print(f"Coordenadas inválidas para region_id={row['region_id']}")
                return None
            coords = [
                (west, south),
                (east, south),
                (east, north),
                (west, north),
                (west, south)
            ]
            return Polygon(coords)
        except Exception as e:
            print(f"Error creando polígono para region_id={row['region_id']}: {e}")
            return None

    regions['geometry'] = regions.apply(make_poly, axis=1)
    gdf = gpd.GeoDataFrame(regions, geometry='geometry', crs=4326)
    gdf = gdf.dropna(subset=['geometry'])

    # Crear mapping: region_id -> index (puedes cambiarlo según tu lógica)
    mapping = {row['region_id']: idx for idx, row in gdf.iterrows()}

    return gdf, mapping

def update_location_dimension(existing_dim, df):
    """
    Actualiza la dimensión de regiones de tráfico con nuevas regiones encontradas en df.
    """
    new_dim, _ = build_location_dimension(df)
    if existing_dim is not None and not existing_dim.empty:
        # Concatenar y eliminar duplicados por region_id
        combined = pd.concat([existing_dim, new_dim], ignore_index=True)
        combined = combined.drop_duplicates(subset=['region_id'])
        combined = gpd.GeoDataFrame(combined, geometry='geometry', crs=4326)
    else:
        combined = new_dim

    mapping = {row['region_id']: idx for idx, row in combined.iterrows()}
    return combined, mapping

def map_location_keys(df, mapping):
    """
    Asigna una clave de región a cada fila del DataFrame usando el mapping.
    """
    df = df.copy()
    df['region_key'] = df['region_id'].map(mapping)
    return df


def extract_traffic_data(output_filename="stg_raw_traffic.parquet",
                       build_regions: bool = False,
                       regions_strategy: str = "incremental",  # 'incremental' | 'rebuild'
                       traffic_regions_filename: str = "traffic_regions.parquet"):
    """
    Extrae datos de trafico y los guarda en formato parquet.
    
    Args:
        output_filename (str): Nombre del archivo de salida
        
    Returns:
        Path: Ruta del archivo guardado o None si no hay datos
    """
    # Llamar a la función para obtener los datos
    df = fetch_data_from_api(soql_query, api_endpoint)
    
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
        traffic_data_path = raw_dir / output_filename

        if build_regions:
            traffic_regions_path = raw_dir / traffic_regions_filename
            if regions_strategy == "rebuild" or not traffic_regions_path.exists():
                dim_df, mapping = build_location_dimension(df)
            else:
                existing_dim = pd.read_parquet(traffic_regions_path) if traffic_regions_path.exists() else None
                dim_df, mapping = update_location_dimension(existing_dim, df)
            trips_df = map_location_keys(df, mapping)
            # Convertir a WGS84
            if dim_df.crs.to_epsg() != 4326:
                dim_df = dim_df.to_crs(epsg=4326)
            # Calcular área en km² (proyectar temporalmente a UTM)
            dim_utm = dim_df.to_crs(epsg=26916)  # UTM Zone 16N
            areas_km2 = dim_utm.geometry.area / 1_000_000  # m² a km²
            # Preparar DataFrame
            dim_df = pd.DataFrame({
                'region_id': dim_df['region_id'],
                'region': dim_df['region'],
                'description': dim_df['description'],
                'west': dim_df['west'],
                'east': dim_df['east'],
                'south': dim_df['south'],
                'north': dim_df['north'],
                'geometry_wkt': dim_df.geometry.to_wkt(),
                'area_km2': areas_km2.round(2),
                'crs': 'EPSG:4326'
            })

            trips_df.to_parquet(traffic_data_path, index=False)
            dim_df.to_parquet(traffic_regions_path, index=False)
            print(f"Parquet traffic data (sin geometrías): {traffic_data_path}")
            print(f"Parquet traffic regions: {traffic_regions_path}")
        else:
            # Guardar dataset crudo con geometrías intactas
            df.to_parquet(traffic_data_path, index=False)
            print(f"Parquet traffic data (crudo con geometrías): {traffic_data_path}")
        return str(traffic_data_path)
        
    else:
        print("No se encontraron datos para guardar.")
        return None
    


# def save_traffic_regions_visualization(regions_gdf=dim_df, points_gdf=None, city_gdf=None, output_filename="traffic_regions_map.png"):
#     """
#     Guarda una visualización de las regiones de tráfico como polígonos.
    
#     Args:
#         regions_gdf: GeoDataFrame de regiones de tráfico (debe tener columna 'geometry')
#         points_gdf: (opcional) GeoDataFrame de puntos de referencia (por ejemplo, centroides)
#         city_gdf: (opcional) GeoDataFrame del límite de la ciudad
#         output_filename: nombre del archivo de salida (png)
#     """
#     try:
#         fig, ax = plt.subplots(figsize=(10, 10))

#         if city_gdf is not None:
#             city_gdf.plot(ax=ax, color='lightgrey', edgecolor='black', linewidth=1, alpha=0.7)
#         regions_gdf.plot(ax=ax, column='region_id', legend=True, alpha=0.5, cmap='tab20')
#         if points_gdf is not None:
#             points_gdf.plot(ax=ax, color='red', markersize=10, label='Puntos')

#         plt.title("Regiones de tráfico", fontsize=14)
#         plt.legend()

#         output_dir = get_raw_data_dir()
#         output_path = output_dir / output_filename
#         plt.savefig(output_path, dpi=300)
#         print(f"✓ Visualización de regiones guardada en: {output_path}")

#     except ImportError:
#         print("⚠️  matplotlib no instalado, omitiendo visualización")

# Para ejecutar como script independiente
if __name__ == "__main__":
    extract_traffic_data()