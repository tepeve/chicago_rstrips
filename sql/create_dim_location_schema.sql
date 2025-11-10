-- Crear esquema dim si no existe
CREATE SCHEMA IF NOT EXISTS dimlocation;

-- Tabla dimensión para centroides de ubicación
DROP TABLE IF EXISTS dimlocation.dim_centroid_location CASCADE;

CREATE TABLE IF NOT EXISTS dimlocation.dim_centroid_location (
    location_id VARCHAR(20) PRIMARY KEY,
    original_text TEXT NOT NULL,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    source_type VARCHAR(30) DEFAULT 'census_centroid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índice espacial si decides usar PostGIS más adelante
CREATE INDEX IF NOT EXISTS idx_dimlocation_location_coords ON dimlocation.dim_centroid_location(longitude, latitude);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dimlocation_location_text ON dimlocation.dim_centroid_location(original_text);

COMMENT ON TABLE dimlocation.dim_centroid_location IS 'Dimensión de ubicaciones basada en centroides de census tracts';
COMMENT ON COLUMN dimlocation.dim_centroid_location.location_id IS 'Hash SHA1 truncado del texto original del centroide';
COMMENT ON COLUMN dimlocation.dim_centroid_location.original_text IS 'Texto original del centroide desde la API (POINT, JSON, etc)';
COMMENT ON COLUMN dimlocation.dim_centroid_location.longitude IS 'Longitud parseada (NULL si no se pudo extraer)';
COMMENT ON COLUMN dimlocation.dim_centroid_location.latitude IS 'Latitud parseada (NULL si no se pudo extraer)';