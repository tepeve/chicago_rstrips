import geopandas as gpd
from shapely import voronoi_polygons, unary_union
from chicago_rstrips.utils import get_outputs_dir
import matplotlib.pyplot as plt
import os


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
    
    return city_poly_gdf


def generate_weather_stations():
    """
    Crea las estaciones meteorológicas hardcodeadas.
    
    Returns:
        GeoDataFrame: Puntos de estaciones en WGS84
    """
    points_df = {
        'station_id': ["Estacion_A", "Estacion_B", "Estacion_C", "Estacion_D"],
        'lon': [-87.655, -87.755, -87.580, -87.670],
        'lat': [41.970, 41.970, 41.790, 41.750]
    }

    points_gdf = gpd.GeoDataFrame(
        points_df,
        geometry=gpd.points_from_xy(points_df['lon'], points_df['lat']),
        crs=4326
    )
    
    return points_gdf


def generate_voronoi_zones(city_gdf, points_gdf):
    """
    Genera zonas de Voronoi a partir de estaciones y límite de ciudad.
    
    Args:
        city_gdf: GeoDataFrame del límite de Chicago
        points_gdf: GeoDataFrame de estaciones
        
    Returns:
        tuple: (city_gdf, points_gdf, voronoi_gdf) todos en WGS84
    """
    crs_proyectado = 26916  # UTM Zone 16N
    
    # Proyectar a metros
    city_proj = city_gdf.to_crs(epsg=crs_proyectado)
    points_proj = points_gdf.to_crs(epsg=crs_proyectado)

    # Generar polígonos de Voronoi
    points_union = unary_union(points_proj.geometry)
    voronoi_polys = voronoi_polygons(points_union, extend_to=city_proj.union_all())
    voronoi_gdf = gpd.GeoDataFrame(geometry=list(voronoi_polys.geoms), crs=crs_proyectado)

    # Asignar IDs de estaciones
    voronoi_con_id = gpd.sjoin(voronoi_gdf, points_proj, how="inner", predicate="contains")

    # Convertir todo a WGS84
    city_wgs84 = city_proj.to_crs(epsg=4326)
    points_wgs84 = points_proj.to_crs(epsg=4326)
    voronoi_wgs84 = voronoi_con_id.to_crs(epsg=4326)

    return city_wgs84, points_wgs84, voronoi_wgs84


def generate_all_geospatial_features():
    """
    Función de alto nivel que genera todas las features geoespaciales.
    
    Returns:
        dict: Diccionario con 'city', 'stations', 'zones' como GeoDataFrames
    """
    print("🗺️  Generando features geoespaciales...")
    
    city_gdf = generate_city_boundary()
    print("✓ Límite de ciudad descargado")
    
    points_gdf = generate_weather_stations()
    print("✓ Estaciones meteorológicas creadas")
    
    city_gdf, points_gdf, voronoi_gdf = generate_voronoi_zones(city_gdf, points_gdf)
    print("✓ Zonas de Voronoi generadas")
    
    return {
        'city': city_gdf,
        'stations': points_gdf,
        'zones': voronoi_gdf
    }


def save_visualization(city_gdf, points_gdf, voronoi_gdf):
    """
    Guarda visualización de las zonas de Voronoi.
    
    Args:
        city_gdf: Límite de la ciudad
        points_gdf: Estaciones
        voronoi_gdf: Zonas de Voronoi
    """
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


# Ejecución standalone
if __name__ == "__main__":
    from chicago_rstrips.load_geospatial_features import load_all_geospatial_features
    
    # Generar features
    features = generate_all_geospatial_features()
    
    # Guardar visualización
    save_visualization(
        features['city'],
        features['stations'],
        features['zones']
    )
    
    # Persistir en PostgreSQL
    print("\n" + "="*60)
    print("PERSISTIENDO EN POSTGRESQL")
    print("="*60)
    
    try:
        load_all_geospatial_features(
            points_gdf=features['stations'],
            voronoi_gdf=features['zones'],
            city_gdf=features['city']
        )
    except Exception as e:
        print(f"⚠️  Error al persistir: {e}")