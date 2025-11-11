-- Archivo: sql/create_dim_location_schema.sql
-- Descripción: Schema y tablas para features geoespaciales

-- ============================================================
-- 1. CREAR SCHEMA
-- ============================================================
CREATE SCHEMA IF NOT EXISTS dim_spatial;


-- ============================================================
-- 2. ELIMINAR TABLAS EXISTENTES (en orden correcto por FKs)
-- ============================================================
DROP TABLE IF EXISTS dim_spatial.trips_locations CASCADE;
DROP TABLE IF EXISTS dim_spatial.weather_voronoi_zones CASCADE;
DROP TABLE IF EXISTS dim_spatial.weather_stations_points CASCADE;
DROP TABLE IF EXISTS dim_spatial.chicago_city_boundary CASCADE;



-- ============================================================
-- TABLA: Dimensión de Ubicaciones de Viajes
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_spatial.trips_locations (
    location_id VARCHAR(20) PRIMARY KEY,
    original_text TEXT NOT NULL,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    source_type VARCHAR(30) DEFAULT 'census_centroid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índice espacial si decides usar PostGIS más adelante
CREATE INDEX IF NOT EXISTS idx_dimlocation_location_coords ON dim_spatial.trips_locations(longitude, latitude);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dimlocation_location_text ON dim_spatial.trips_locations(original_text);

COMMENT ON TABLE dim_spatial.trips_locations IS 'Dimensión de ubicaciones basada en centroides de census tracts';
COMMENT ON COLUMN dim_spatial.trips_locations.location_id IS 'Hash SHA1 truncado del texto original del centroide';
COMMENT ON COLUMN dim_spatial.trips_locations.original_text IS 'Texto original del centroide desde la API (POINT, JSON, etc)';
COMMENT ON COLUMN dim_spatial.trips_locations.longitude IS 'Longitud parseada (NULL si no se pudo extraer)';
COMMENT ON COLUMN dim_spatial.trips_locations.latitude IS 'Latitud parseada (NULL si no se pudo extraer)';

-- ============================================================
-- TABLA: Dimensión de regiones de trafico
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_spatial.traffic_regions (
    region_id INTEGER PRIMARY KEY,
    region TEXT NOT NULL,
    description TEXT NOT NULL,
    west DOUBLE PRECISION,
    east DOUBLE PRECISION,
    south DOUBLE PRECISION,
    north DOUBLE PRECISION,
    geometry_wkt TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



COMMENT ON TABLE dim_spatial.traffic_regions IS 'Dimensión de regiones de tránsitoo basadas en áreas definidas por la ciudad';
COMMENT ON COLUMN dim_spatial.traffic_regions.region_id IS 'Número identificador de la región de tráfico - ya servido por el proveedor de datos';
COMMENT ON COLUMN dim_spatial.traffic_regions.geometry_wkt IS 'Geometría en formato Well-Known Text (WKT) representando el polígono de la región';

-- ============================================================
-- TABLA: Estaciones Meteorológicas
-- ============================================================
CREATE TABLE dim_spatial.weather_stations_points (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(100),
    longitude NUMERIC(10, 7) NOT NULL,  -- Precisión de ~1cm
    latitude NUMERIC(10, 7) NOT NULL,
    geometry_wkt TEXT,                   -- WKT format: "POINT(-87.655 41.970)"
    crs VARCHAR(20) DEFAULT 'EPSG:4326',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints adicionales
    CONSTRAINT chk_longitude CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT chk_latitude CHECK (latitude BETWEEN -90 AND 90)
);

-- Índice espacial para búsquedas por coordenadas
CREATE INDEX idx_stations_location 
ON dim_spatial.weather_stations_points (longitude, latitude);
-- Comentarios de documentación
COMMENT ON TABLE dim_spatial.weather_stations_points IS 
'Puntos de estaciones meteorológicas para consultas a APIs de clima';

COMMENT ON COLUMN dim_spatial.weather_stations_points.station_id IS 
'Identificador único de la estación (ej: Estacion_A)';

COMMENT ON COLUMN dim_spatial.weather_stations_points.geometry_wkt IS 
'Geometría en formato Well-Known Text (WKT)';

-- ============================================================
-- TABLA: Zonas de Voronoi
-- ============================================================
CREATE TABLE dim_spatial.weather_voronoi_zones (
    zone_id SERIAL PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL,
    geometry_wkt TEXT NOT NULL,
    area_km2 NUMERIC(10, 2),
    crs VARCHAR(20) DEFAULT 'EPSG:4326',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key a estaciones
    CONSTRAINT fk_voronoi_station 
        FOREIGN KEY (station_id) 
        REFERENCES dim_spatial.weather_stations_points(station_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    -- Una zona por estación
    CONSTRAINT uq_station_zone UNIQUE(station_id),
    
    -- Constraint de área positiva
    CONSTRAINT chk_area_positive CHECK (area_km2 > 0)
);

-- Índice en station_id para joins
CREATE INDEX idx_voronoi_station 
ON dim_spatial.weather_voronoi_zones (station_id);

-- Comentarios
COMMENT ON TABLE dim_spatial.weather_voronoi_zones IS 
'Polígonos de Voronoi para asociar clima a áreas de la ciudad';

COMMENT ON COLUMN dim_spatial.weather_voronoi_zones.area_km2 IS 
'Área del polígono en kilómetros cuadrados';

-- ============================================================
-- TABLA: Perímetro de Chicago
-- ============================================================
CREATE TABLE dim_spatial.chicago_city_boundary (
    boundary_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) DEFAULT 'Chicago',
    geometry_wkt TEXT NOT NULL,
    area_km2 NUMERIC(10, 2),
    crs VARCHAR(20) DEFAULT 'EPSG:4326',
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint de área positiva
    CONSTRAINT chk_boundary_area_positive CHECK (area_km2 > 0)
);

-- Comentarios
COMMENT ON TABLE dim_spatial.chicago_city_boundary IS 
'Límite geográfico de Chicago para visualizaciones y validaciones';

COMMENT ON COLUMN dim_spatial.chicago_city_boundary.source_url IS 
'URL de la API de datos abiertos de donde se obtuvo el límite';

-- ============================================================
-- VISTAS ÚTILES
-- ============================================================

-- Vista que combina estaciones con sus zonas
CREATE OR REPLACE VIEW dim_spatial.vw_stations_with_zones AS
SELECT 
    ws.station_id,
    ws.station_name,
    ws.longitude,
    ws.latitude,
    ws.geometry_wkt as station_geometry,
    vz.zone_id,
    vz.geometry_wkt as zone_geometry,
    vz.area_km2,
    ws.created_at
FROM dim_spatial.weather_stations_points ws
LEFT JOIN dim_spatial.weather_voronoi_zones vz ON ws.station_id = vz.station_id;

COMMENT ON VIEW dim_spatial.vw_stations_with_zones IS 
'Vista que combina estaciones meteorológicas con sus zonas de Voronoi';




