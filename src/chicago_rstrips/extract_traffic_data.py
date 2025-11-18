from chicago_rstrips.socrata_api_client import fetch_data_from_api
from chicago_rstrips.config import START_DATE, END_DATE, CHIC_TRAFFIC_API_URL
from chicago_rstrips.utils import get_raw_data_dir, transform_dataframe_types
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd

# Defino el endpoint de la API
api_endpoint = CHIC_TRAFFIC_API_URL

type_mapping = {
    # Timestamps
    'time': 'datetime64[ns]',
    # Numéricos enteros
    'region_id': 'Int64',  
    'bus_count': 'Int64',  
    'num_reads': 'Int64',  
    # Strings
    'region': 'string',
    'record_id': 'string',
    "batch_id": 'string',    
    # Numéricos decimales
    'speed': 'float64'
}

def build_location_dimension(df):
    """
    Construye la dimensión de regiones de tráfico como polígonos.
    Devuelve un GeoDataFrame con la geometría construida.
    """
    # Seleccionar columnas relevantes y eliminar duplicados
    regions = df[['region_id', 'region', 'west', 'east', 'south', 'north']].drop_duplicates()

    # Crear polígonos a partir de las coordenadas
    def make_poly(row):
        try:
            for c in ['west', 'east', 'south', 'north']:
                if pd.isna(row[c]) or row[c] == '':
                    return None
            west = float(row['west'])
            east = float(row['east'])
            south = float(row['south'])
            north = float(row['north'])
            
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

    # MODIFICACION: Ya no generamos un mapping basado en índices.
    # Devolvemos el gdf tal cual, confiando en el region_id original.
    return gdf

def update_location_dimension(existing_dim, df):
    """
    Actualiza la dimensión de regiones de tráfico con nuevas regiones encontradas en df.
    """
    new_dim = build_location_dimension(df)
    
    if existing_dim is not None and not existing_dim.empty:
        # Asegurar que existing_dim sea GeoDataFrame
        if not isinstance(existing_dim, gpd.GeoDataFrame):
             # Si leemos de parquet, necesitamos recrear la geometría si viene como WKT o bytes
             # Asumimos que aquí manejamos la lógica de concatenación simple
             pass

        combined = pd.concat([existing_dim, new_dim], ignore_index=True)
        combined = combined.drop_duplicates(subset=['region_id'])
        
        # Reconstruir GeoDataFrame si es necesario (depende de cómo se leyó existing_dim)
        if 'geometry' in combined.columns:
             combined = gpd.GeoDataFrame(combined, geometry='geometry', crs=4326)
    else:
        combined = new_dim

    return combined

def extract_traffic_data(output_filename="stg_raw_traffic.parquet",
                       build_regions: bool = False,
                       regions_strategy: str = "incremental",
                       start_timestamp: str = None,
                       end_timestamp: str = None,
                       batch_id: str = None,
                       traffic_regions_filename: str = "traffic_regions.parquet"):
    """
    Extrae datos de trafico y los guarda en formato parquet.
    """
    start_timestamp = start_timestamp if start_timestamp else START_DATE
    end_timestamp = end_timestamp if end_timestamp else END_DATE

    soql_query = f"""
    SELECT
    time, region_id, speed, region, bus_count, num_reads, hour, record_id, 
    west, east, north, south 
    WHERE
    time BETWEEN '{start_timestamp}' AND '{end_timestamp}'
        AND region_id IS NOT NULL
    ORDER BY time ASC
    LIMIT 1000
    """

    df = fetch_data_from_api(soql_query, api_endpoint)
    
    if df is not None:
        print(f"\nSe encontraron {len(df)} resultados.")
        
        if batch_id:
            df['batch_id'] = batch_id

        print("\n--- Transformando tipos de datos ---")
        df = transform_dataframe_types(df, type_mapping)
      
        raw_dir = get_raw_data_dir()
        traffic_data_path = raw_dir / output_filename
        
        # Inicializar traffic_df con el original
        traffic_df = df.copy()

        if build_regions:
            traffic_regions_path = raw_dir / traffic_regions_filename
            
            if regions_strategy == "rebuild" or not traffic_regions_path.exists():
                dim_df = build_location_dimension(df)
            else:
                # Lectura simplificada para el ejemplo
                existing_dim = None 
                if traffic_regions_path.exists():
                    try:
                         existing_dim = gpd.read_parquet(traffic_regions_path)
                    except:
                         existing_dim = pd.read_parquet(traffic_regions_path)
                
                dim_df = update_location_dimension(existing_dim, df)
            
            # MODIFICACION CRITICA:
            # Eliminamos la llamada a map_location_keys.
            # traffic_df mantiene sus IDs originales (1-29).

        print("\n--- Aplicando filtro de columnas ---")
        api_columns_to_keep = list(type_mapping.keys())
        traffic_df = traffic_df[api_columns_to_keep]
        print(traffic_df.head())

        if build_regions:
            # Convertir a WGS84 si es necesario
            if dim_df.crs and dim_df.crs.to_epsg() != 4326:
                dim_df = dim_df.to_crs(epsg=4326)
            
            # Calcular área
            # Proyección temporal para área (utm zona 16n aprox chicago)
            dim_utm = dim_df.to_crs(epsg=26916)
            areas_km2 = dim_utm.geometry.area / 1_000_000

            # Preparar DataFrame final de regiones
            dim_export = pd.DataFrame({
                'region_id': dim_df['region_id'], # ID ORIGINAL
                'region': dim_df['region'],
                'west': dim_df['west'],
                'east': dim_df['east'],
                'south': dim_df['south'],
                'north': dim_df['north'],
                'geometry_wkt': dim_df.geometry.to_wkt(),
                'area_km2': areas_km2.round(2),
                'batch_id': batch_id,
                'crs': 'EPSG:4326'
            })

            traffic_df.to_parquet(traffic_data_path, index=False)
            dim_export.to_parquet(traffic_regions_path, index=False)
            print(f"Parquet traffic data: {traffic_data_path}")
            print(f"Parquet traffic regions: {traffic_regions_path}")
        else:
            traffic_df.to_parquet(traffic_data_path, index=False)
            print(f"Parquet traffic data: {traffic_data_path}")
            
        return str(traffic_data_path)
        
    else:
        print("No se encontraron datos para guardar.")
        return None

if __name__ == "__main__":
    extract_traffic_data()