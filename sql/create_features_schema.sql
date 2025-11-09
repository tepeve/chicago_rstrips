-- Archivo: sql/create_features_schema.sql
-- Descripción: Schema y tablas para features geoespaciales
-- Fecha: 2025-11-09

-- ============================================================
-- 1. CREAR SCHEMA
-- ============================================================
CREATE SCHEMA IF NOT EXISTS features;

-- ============================================================
-- 2. ELIMINAR TABLAS EXISTENTES (en orden correcto por FKs)
-- ============================================================
DROP TABLE IF EXISTS features.dim_voronoi_zones CASCADE;
DROP TABLE IF EXISTS features.dim_weather_stations CASCADE;
DROP TABLE IF EXISTS features.ref_city_boundary CASCADE;

-- ============================================================
-- 3. TABLA: Estaciones Meteorológicas
-- ============================================================
CREATE TABLE features.dim_weather_stations (
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
ON features.dim_weather_stations (longitude, latitude);

-- Comentarios de documentación
COMMENT ON TABLE features.dim_weather_stations IS 
'Puntos de estaciones meteorológicas para consultas a APIs de clima';

COMMENT ON COLUMN features.dim_weather_stations.station_id IS 
'Identificador único de la estación (ej: Estacion_A)';

COMMENT ON COLUMN features.dim_weather_stations.geometry_wkt IS 
'Geometría en formato Well-Known Text (WKT)';

-- ============================================================
-- 4. TABLA: Zonas de Voronoi
-- ============================================================
CREATE TABLE features.dim_voronoi_zones (
    zone_id SERIAL PRIMARY KEY,
    station_id VARCHAR(50) NOT NULL,
    geometry_wkt TEXT NOT NULL,
    area_km2 NUMERIC(10, 2),
    crs VARCHAR(20) DEFAULT 'EPSG:4326',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key a estaciones
    CONSTRAINT fk_voronoi_station 
        FOREIGN KEY (station_id) 
        REFERENCES features.dim_weather_stations(station_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    -- Una zona por estación
    CONSTRAINT uq_station_zone UNIQUE(station_id),
    
    -- Constraint de área positiva
    CONSTRAINT chk_area_positive CHECK (area_km2 > 0)
);

-- Índice en station_id para joins
CREATE INDEX idx_voronoi_station 
ON features.dim_voronoi_zones (station_id);

-- Comentarios
COMMENT ON TABLE features.dim_voronoi_zones IS 
'Polígonos de Voronoi para asociar clima a áreas de la ciudad';

COMMENT ON COLUMN features.dim_voronoi_zones.area_km2 IS 
'Área del polígono en kilómetros cuadrados';

-- ============================================================
-- 5. TABLA: Perímetro de Chicago
-- ============================================================
CREATE TABLE features.ref_city_boundary (
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
COMMENT ON TABLE features.ref_city_boundary IS 
'Límite geográfico de Chicago para visualizaciones y validaciones';

COMMENT ON COLUMN features.ref_city_boundary.source_url IS 
'URL de la API de datos abiertos de donde se obtuvo el límite';

-- ============================================================
-- 6. VISTAS ÚTILES
-- ============================================================

-- Vista que combina estaciones con sus zonas
CREATE OR REPLACE VIEW features.vw_stations_with_zones AS
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
FROM features.dim_weather_stations ws
LEFT JOIN features.dim_voronoi_zones vz ON ws.station_id = vz.station_id;

COMMENT ON VIEW features.vw_stations_with_zones IS 
'Vista que combina estaciones meteorológicas con sus zonas de Voronoi';


-- ============================================================
-- 8. GRANTS (ajustar según tus usuarios)
-- ============================================================
-- Ejemplo: dar permisos de lectura a un usuario de solo lectura
-- GRANT USAGE ON SCHEMA features TO readonly_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA features TO readonly_user;