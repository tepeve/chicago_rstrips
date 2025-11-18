import pandas as pd
import geopandas as gpd
from shapely import unary_union, from_wkt
from sqlalchemy import text
from chicago_rstrips.db_loader import get_engine, load_dataframe_to_postgres


def fetch_voronoi_zones() -> gpd.GeoDataFrame:
    """
    Fetches the voronois zones of each weather station from the database.

    Returns:
        gpd.GeoDataFrame:.
    """
    engine = get_engine()
    try:
        query = "SELECT station_id, geometry_wkt FROM dim_spatial.weather_voronoi_zones"
        voronoi_zones_df = pd.read_sql(query, engine)
        
        if voronoi_zones_df.empty:
            raise ValueError("Voronoi zones not found in the database.")

        geometry = from_wkt(voronoi_zones_df['geometry_wkt'])
        voronoi_zones_gdf = gpd.GeoDataFrame(voronoi_zones_df, crs="EPSG:4326", geometry=geometry)
        return voronoi_zones_gdf
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



def fetch_traffic_regions() -> gpd.GeoDataFrame:
    """
    Fetches all traffic regions from the database.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame with trip locations.
    """
    engine = get_engine()
    try:
        query = "SELECT region_id, geometry_wkt FROM dim_spatial.traffic_regions"
        traffic_regions_df = pd.read_sql(query, engine)

        if traffic_regions_df.empty:
            return gpd.GeoDataFrame()

        traffic_regions_gdf = gpd.GeoDataFrame(
            traffic_regions_df,
            geometry=traffic_regions_df['geometry_wkt'].apply(from_wkt),
            crs="EPSG:4326"
        )
        return traffic_regions_gdf
    finally:
        engine.dispose()


def map_trips_locations_to_traffic_and_weather(trips_locations_gdf: gpd.GeoDataFrame, traffic_regions_gdf: gpd.GeoDataFrame, weather_stations_voronoi_zones_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Maps every unique trip location to its corresponding traffic region and weather station voronoi zone by spatial joins.
        
    Returns:
        gpd.GeoDataFrame: A GeoDataFrame with location_id, region_id, and station_id.
    """
    # Unir espacialmente las ubicaciones de los viajes con las regiones de tráfico
    # Se usa un 'left join' para mantener todas las ubicaciones originales
    locations_with_traffic = gpd.sjoin(
        trips_locations_gdf, 
        traffic_regions_gdf, 
        how="left", 
        predicate="within"
    )

    # Unir espacialmente el resultado anterior con las zonas de Voronoi de las estaciones meteorológicas
    locations_mapped_gdf = gpd.sjoin(
        locations_with_traffic, 
        weather_stations_voronoi_zones_gdf, 
        how="left", 
        predicate="within",
        rsuffix="weather" # Sufijo para evitar conflictos de nombres de columnas
    )

    # Seleccionar y limpiar las columnas finales. 
    # Los sjoins agregan columnas como 'index_right' que no son necesarias.
    final_columns = ['location_id', 'region_id', 'station_id', 'batch_id']
    locations_mapped_gdf = locations_mapped_gdf[final_columns]

    return locations_mapped_gdf



def save_locations_mapped_to_db(locations_mapped_gdf: gpd.GeoDataFrame):
#     """
#     Saves the mapped locations DataFrame to the database.
    
#     Args:
#         locations_mapped_gdf (gpd.GeoDataFrame):  GeoDataFrame with location_id, region_id, and station_id mapped.

#     """
    if locations_mapped_gdf.empty:
        print("No mapped locations to save.")
        return

    # Prepare DataFrame for loading
    locations_mapped_df = pd.DataFrame({
        'location_id': locations_mapped_gdf['location_id'],
        'region_id': locations_mapped_gdf['region_id'],
        'station_id': locations_mapped_gdf['station_id'],
        'batch_id': locations_mapped_gdf.get('batch_id'), 
    })
    
    # Cargar a PostgreSQL
    load_dataframe_to_postgres(
        locations_mapped_df, 
        table_name='mapped_locations', 
        schema='dim_spatial', 
        if_exists='replace'
    )
    print(f"{len(locations_mapped_df)} mapped locations saved to 'dim_spatial.mapped_locations'.")


def join_spatial_dims():
    """
    Main function to run the full process: fetch, map, and save spatial dimensions.
    """
    print("Fetching trip locations...")
    trips_locations_gdf = fetch_trips_locations()
    
    print("Fetching traffic regions...")
    traffic_regions_gdf = fetch_traffic_regions()

    print("Fetching weather stations voronois...")
    weather_stations_voronoi_zones_gdf = fetch_voronoi_zones()


    print("Mapping trip locations to traffic regions and weather stations...")
    locations_mapped_gdf = map_trips_locations_to_traffic_and_weather(
        trips_locations_gdf, 
        traffic_regions_gdf, 
        weather_stations_voronoi_zones_gdf
    )

    print("Saving mapped locations to the database...")
    save_locations_mapped_to_db(locations_mapped_gdf)   
        
    print("Process finished.")
