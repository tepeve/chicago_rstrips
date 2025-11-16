CREATE SCHEMA IF NOT EXISTS dim_spatial;

-- ============================================================
-- 1. ELIMINA TABLAS DINÁMICAS (en orden)
-- ============================================================
DROP TABLE IF EXISTS dim_spatial.trips_locations CASCADE;
DROP TABLE IF EXISTS dim_spatial.traffic_regions CASCADE;

-- ============================================================
-- 2. TABLA: Dimensión de Ubicaciones de Viajes
-- ============================================================
CREATE TABLE dim_spatial.trips_locations (
    location_id VARCHAR(20) PRIMARY KEY,
    original_text TEXT NOT NULL,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    source_type VARCHAR(30) DEFAULT 'census_centroid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_dimlocation_location_coords ON dim_spatial.trips_locations(longitude, latitude);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dimlocation_location_text ON dim_spatial.trips_locations(original_text);

COMMENT ON TABLE dim_spatial.trips_locations IS 'Dimensión de ubicaciones basada en centroides de census tracts';
COMMENT ON COLUMN dim_spatial.trips_locations.location_id IS 'Hash SHA1 truncado del texto original del centroide';
COMMENT ON COLUMN dim_spatial.trips_locations.original_text IS 'Texto original del centroide desde la API (POINT, JSON, etc)';
COMMENT ON COLUMN dim_spatial.trips_locations.longitude IS 'Longitud parseada (NULL si no se pudo extraer)';
COMMENT ON COLUMN dim_spatial.trips_locations.latitude IS 'Latitud parseada (NULL si no se pudo extraer)';

-- ============================================================
-- 3. TABLA: Dimensión de regiones de trafico
-- ============================================================
CREATE TABLE dim_spatial.traffic_regions (
    region_id INTEGER PRIMARY KEY,
    region TEXT NOT NULL,
    west DOUBLE PRECISION,
    east DOUBLE PRECISION,
    south DOUBLE PRECISION,
    north DOUBLE PRECISION,
    geometry_wkt TEXT,
    area_km2 DOUBLE PRECISION,
    crs VARCHAR(20) DEFAULT 'EPSG:4326',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(255)
);

COMMENT ON TABLE dim_spatial.traffic_regions IS 'Dimensión de regiones de tránsito basadas en áreas definidas por la ciudad';
COMMENT ON COLUMN dim_spatial.traffic_regions.region_id IS 'Número identificador de la región de tráfico - ya servido por el proveedor de datos';
COMMENT ON COLUMN dim_spatial.traffic_regions.geometry_wkt IS 'Geometría en formato Well-Known Text (WKT) representando el polígono de la región';