import pandas as pd
import geopandas as gpd
from shapely import voronoi_polygons, unary_union, from_wkt, to_wkt
from chicago_rstrips.utils import get_outputs_dir
import matplotlib.pyplot as plt


def generate_city_boundary():
    """
    Descarga y valida el límite de Chicago.
    
    Returns:
        GeoDataFrame: Límite de Chicago en WGS84
    """
    url_de_api = "https://data.cityofchicago.org/resource/qqq8-j68g.geojson"
    city_poly_gdf = gpd.read_file(url_de_api)

    if city_poly_gdf.empty or city_poly_gdf.geometry.is_empty.all():
        raise ValueError("El polígono de la ciudad está vacío o no contiene geometrías válidas.")

    if city_poly_gdf.crs is None:
        city_poly_gdf = city_poly_gdf.set_crs(epsg=4326)

    # Calcular área
    city_utm = city_poly_gdf.to_crs(epsg=26916)
    area_km2 = city_utm.geometry.area.sum() / 1_000_000
    city_poly_gdf['area_km2'] = area_km2.round(2)

    
    # Preparar DataFrame
    city_boundary_df = pd.DataFrame({
        'city_name': ['Chicago'],
        'geometry_wkt': [city_poly_gdf.geometry.union_all().wkt],
        'area_km2': [city_poly_gdf['area_km2'].iloc[0]],
        'crs': ['EPSG:4326'],
        'source_url': ['https://data.cityofchicago.org/resource/qqq8-j68g.geojson']
    })

    return city_boundary_df


def generate_weather_stations():
    """
    Crea las estaciones meteorológicas hardcodeadas.
    
    Returns:
        GeoDataFrame: Puntos de estaciones en WGS84
    """
    points_df = {
        'station_id': ["Estacion_A", "Estacion_B", "Estacion_C", "Estacion_D"],
        'longitude': [-87.655, -87.755, -87.580, -87.670],
        'latitude': [41.970, 41.970, 41.790, 41.750]
    }
    # https://geopandas.org/en/stable/gallery/create_geopandas_from_pandas.html
    points_gdf = gpd.GeoDataFrame(
        points_df,
        geometry=gpd.points_from_xy(points_df['longitude'], points_df['latitude']),
        crs=4326
    )

    # Preparar DataFrame para PostgreSQL
    weather_stations_df = pd.DataFrame({
        'station_id': points_gdf['station_id'],
        'station_name': points_gdf['station_id'],
        'longitude': points_gdf.geometry.x,
        'latitude': points_gdf.geometry.y,
        'geometry_wkt': points_gdf.geometry.to_wkt(),
        'crs': 'EPSG:4326'
    })
    
    return weather_stations_df


def generate_voronoi_zones(city_boundary_df, weather_stations_df):
    """
    Genera zonas de Voronoi a partir de estaciones y límite de ciudad.
    
    Args:
        city_gdf: GeoDataFrame del límite de Chicago
        points_gdf: GeoDataFrame de estaciones
        
    Returns:
        tuple: (city_gdf, points_gdf, voronoi_gdf) todos en WGS84
    """

    points_gdf = gpd.GeoDataFrame(weather_stations_df, geometry=gpd.points_from_xy(weather_stations_df['longitude'], weather_stations_df['latitude']), crs=4326)
    city_gdf = gpd.GeoDataFrame(city_boundary_df, geometry=gpd.GeoSeries.from_wkt(city_boundary_df['geometry_wkt']), crs=4326)
    
    # Proyectar a metros
    crs_proyectado = 26916  # UTM Zone 16N
    city_proj = city_gdf.to_crs(epsg=crs_proyectado)
    points_proj = points_gdf.to_crs(epsg=crs_proyectado)

    # Generar polígonos de Voronoi
    points_union = unary_union(points_proj.geometry)
    voronoi_polys = voronoi_polygons(points_union, extend_to=city_proj.union_all())
    voronoi_gdf = gpd.GeoDataFrame(geometry=list(voronoi_polys.geoms), crs=crs_proyectado)

    # Asignar IDs de estaciones
    voronoi_con_id = gpd.sjoin(voronoi_gdf, points_proj, how="inner", predicate="contains")

    # Calcular área en km² (proyectar temporalmente a UTM)
    areas_km2 = voronoi_con_id.geometry.area / 1_000_000  # m² a km²
    voronoi_con_id['area_km2'] = areas_km2.round(2)

    # Reconvertir a WGS84
    # city_wgs84 = city_proj.to_crs(epsg=4326)
    # points_wgs84 = points_proj.to_crs(epsg=4326)
    voronoi_wgs84 = voronoi_con_id.to_crs(epsg=4326)

    # Preparar DataFrame
    voronoi_df = pd.DataFrame({
        'station_id': voronoi_wgs84['station_id'],
        'geometry_wkt': voronoi_wgs84.geometry.to_wkt(),
        'area_km2': voronoi_wgs84['area_km2'],
        'crs': 'EPSG:4326'
    })

    return voronoi_df


def save_weather_stations_visualization(city_boundary_df, weather_stations_df, voronoi_df):
    """
    Guarda visualización de las zonas de Voronoi.
    
    Args:
        city_gdf: Límite de la ciudad
        points_gdf: Estaciones
        voronoi_gdf: Zonas de Voronoi
    """
    points_gdf = gpd.GeoDataFrame(weather_stations_df, geometry=gpd.points_from_xy(weather_stations_df['longitude'], weather_stations_df['latitude']), crs=4326)
    city_gdf = gpd.GeoDataFrame(city_boundary_df, geometry=gpd.GeoSeries.from_wkt(city_boundary_df['geometry_wkt']), crs=4326)
    voronoi_gdf = gpd.GeoDataFrame(voronoi_df, geometry=gpd.GeoSeries.from_wkt(voronoi_df['geometry_wkt']), crs=4326)

    try:
        fig, ax = plt.subplots(figsize=(10, 10))

        city_gdf.plot(ax=ax, color='lightgrey', edgecolor='black', linewidth=1, alpha=0.7)
        voronoi_gdf.plot(column='station_id', ax=ax, legend=True, alpha=0.5, cmap='viridis')
        points_gdf.plot(ax=ax, color='red', markersize=10, label='Estaciones')

        # Ajustar límites
        minx_city, miny_city, maxx_city, maxy_city = city_gdf.total_bounds
        minx_voronoi, miny_voronoi, maxx_voronoi, maxy_voronoi = voronoi_gdf.total_bounds
        minx = min(minx_city, minx_voronoi)
        miny = min(miny_city, miny_voronoi)
        maxx = max(maxx_city, maxx_voronoi)
        maxy = max(maxy_city, maxy_voronoi)
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

        plt.title("Zonas de Voronoi para Estaciones de Chicago", fontsize=14)
        plt.legend()

        output_dir = get_outputs_dir()
        output_path = output_dir / "voronoi_zones_with_city.png"
        plt.savefig(output_path, dpi=300)
        print(f"✓ Visualización guardada en: {output_path}")

    except ImportError:
        print("⚠️  matplotlib no instalado, omitiendo visualización")


# Ejecución standalone (solo para generar y visualizar, NO persiste)
if __name__ == "__main__":
    # Generar features

    city_gdf = generate_city_boundary()
    print("✓ Límite de ciudad descargado")
    
    points_gdf = generate_weather_stations()
    print("✓ Estaciones meteorológicas creadas")
    
    voronoi_gdf = generate_voronoi_zones(city_gdf, points_gdf)
    print("✓ Zonas de Voronoi generadas")   

    features = {
        'city': city_gdf,
        'stations': points_gdf,
        'zones': voronoi_gdf
    }
    # Guardar visualización
    save_weather_stations_visualization(
        features['city'],
        features['stations'],
        features['zones']
    )
    print("✓ Visualización generada")
    # Retornar todo
