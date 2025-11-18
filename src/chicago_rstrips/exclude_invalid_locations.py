import pandas as pd
import geopandas as gpd
from shapely import unary_union, from_wkt
from sqlalchemy import text
from chicago_rstrips.db_loader import get_engine, load_dataframe_to_postgres


def fetch_city_boundary() -> gpd.GeoDataFrame:
    """
    Fetches the Chicago city boundary from the database.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing the city boundary geometry.
    """
    engine = get_engine()
    try:
        query = "SELECT geometry_wkt FROM dim_spatial.chicago_city_boundary LIMIT 1"
        city_boundary_df = pd.read_sql(query, engine)
        
        if city_boundary_df.empty:
            raise ValueError("City boundary not found in the database.")

        geometry = from_wkt(city_boundary_df['geometry_wkt'].iloc[0])
        city_boundary_gdf = gpd.GeoDataFrame(crs="EPSG:4326", geometry=[geometry])
        return city_boundary_gdf
    finally:
        engine.dispose()


def fetch_trips_locations() -> gpd.GeoDataFrame:
    """
    Fetches all trip locations from the database.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame with trip locations.
    """
    engine = get_engine()
    try:
        query = "SELECT location_id, longitude, latitude, batch_id FROM dim_spatial.trips_locations"
        trips_locations_df = pd.read_sql(query, engine)

        if trips_locations_df.empty:
            return gpd.GeoDataFrame()

        trips_locations_gdf = gpd.GeoDataFrame(
            trips_locations_df,
            geometry=gpd.points_from_xy(trips_locations_df['longitude'], trips_locations_df['latitude']),
            crs="EPSG:4326"
        )
        return trips_locations_gdf
    finally:
        engine.dispose()
    

def detect_invalid_locations(trips_locations_gdf: gpd.GeoDataFrame, city_boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Detects trip locations that are outside the city boundary.
    
    Args:
        trips_locations_gdf (gpd.GeoDataFrame): Trip locations.
        city_boundary_gdf (gpd.GeoDataFrame): City boundary.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame of invalid locations.
    """
    if trips_locations_gdf.empty or city_boundary_gdf.empty:
        return gpd.GeoDataFrame()

    if trips_locations_gdf.crs != city_boundary_gdf.crs:
        trips_locations_gdf = trips_locations_gdf.to_crs(city_boundary_gdf.crs)

    city_boundary_union = unary_union(city_boundary_gdf.geometry)

    # Detectar que las ubicaciones no estén dentro del límite de la ciudad
    is_invalid = ~trips_locations_gdf.geometry.within(city_boundary_union)
    invalid_locations_gdf = trips_locations_gdf[is_invalid].copy()

    return invalid_locations_gdf

def save_invalid_locations_to_db(invalid_locations_gdf: gpd.GeoDataFrame):
    """
    Saves the invalid locations DataFrame to the database.
    
    Args:
        invalid_locations_gdf (gpd.GeoDataFrame): The invalid locations to save.
    """
    if invalid_locations_gdf.empty:
        print("No invalid locations to save.")
        return

    # Prepare DataFrame for loading
    invalid_locations_df = pd.DataFrame({
        'location_id': invalid_locations_gdf['location_id'],
        'batch_id': invalid_locations_gdf.get('batch_id'), # Use .get() for safety
        'longitude': invalid_locations_gdf.geometry.x,
        'latitude': invalid_locations_gdf.geometry.y,
        'crs': invalid_locations_gdf.crs.to_string()
    })
    
    # Cargar a PostgreSQL
    load_dataframe_to_postgres(
        invalid_locations_df, 
        table_name='invalid_locations', 
        schema='dim_spatial', 
        if_exists='replace'
    )
    print(f"{len(invalid_locations_df)} invalid locations saved to 'dim_spatial.invalid_locations'.")


def find_and_save_invalid_locations():
    """
    Main function to run the full process: fetch, detect, and save.
    """
    print("Fetching city boundary...")
    city_boundary_gdf = fetch_city_boundary()
    
    print("Fetching trip locations...")
    trips_locations_gdf = fetch_trips_locations()
    
    print("Detecting invalid locations...")
    invalid_locations_gdf = detect_invalid_locations(trips_locations_gdf, city_boundary_gdf)
    
    print(f"Found {len(invalid_locations_gdf)} invalid locations.")
    if not invalid_locations_gdf.empty:
        print("Saving invalid locations to the database...")
        save_invalid_locations_to_db(invalid_locations_gdf)
    
    print("Process finished.")


# Ejecución standalone para pruebas
if __name__ == "__main__":
    find_and_save_invalid_locations()