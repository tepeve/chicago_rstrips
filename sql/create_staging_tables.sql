-- Archivo: sql/create_stg_raw_trips.sql
-- Descripción: Schema de la tabla de staging para viajes crudos

-- ============================================================
-- TABLA: Staging de Viajes
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.stg_raw_trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    trip_start_timestamp TIMESTAMP,
    trip_end_timestamp TIMESTAMP,
    trip_seconds DOUBLE PRECISION,
    trip_miles DOUBLE PRECISION,
    percent_time_chicago DOUBLE PRECISION,
    percent_distance_chicago DOUBLE PRECISION,
    pickup_community_area INTEGER,
    dropoff_community_area INTEGER,
    fare DOUBLE PRECISION,
    tip DOUBLE PRECISION,
    additional_charges DOUBLE PRECISION,
    trip_total DOUBLE PRECISION,
    shared_trip_authorized BOOLEAN,
    trips_pooled INTEGER,
    -- Claves a dimensión de ubicaciones (reemplazan geometrías)
    pickup_location_id VARCHAR(20),
    dropoff_location_id VARCHAR(20),
    -- Auditoría
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(255)
);

-- Índices para mejorar consultas
CREATE INDEX IF NOT EXISTS idx_stg_raw_trips_start_ts ON staging.stg_raw_trips(trip_start_timestamp);
CREATE INDEX IF NOT EXISTS idx_stg_raw_trips_pickup_loc ON staging.stg_raw_trips(pickup_location_id);
CREATE INDEX IF NOT EXISTS idx_stg_raw_trips_dropoff_loc ON staging.stg_raw_trips(dropoff_location_id);
CREATE INDEX IF NOT EXISTS idx_stg_raw_trips_community ON staging.stg_raw_trips(pickup_community_area, dropoff_community_area);

COMMENT ON TABLE staging.stg_raw_trips IS 'Staging de datos crudos de viajes sin geometrías. Las ubicaciones están normalizadas en dim.dim_centroid_location';
COMMENT ON COLUMN staging.stg_raw_trips.pickup_location_id IS 'FK a dim.dim_centroid_location (hash SHA1 truncado)';
COMMENT ON COLUMN staging.stg_raw_trips.dropoff_location_id IS 'FK a dim.dim_centroid_location (hash SHA1 truncado)';


-- ============================================================
-- TABLA: Staging de Tráficc
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.stg_raw_traffic (
    record_id VARCHAR(50) PRIMARY KEY,
    time TIMESTAMP,
    speed DOUBLE PRECISION,
    region_id INTEGER,
    region VARCHAR(100),
    bus_count INTEGER,
    num_reads INTEGER,
    -- Auditoría
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(255)
);

-- Índices para mejorar consultas
CREATE INDEX IF NOT EXISTS stg_raw_traffic_time ON staging.stg_raw_traffic(time);
CREATE INDEX IF NOT EXISTS stg_raw_traffic_region_id ON staging.stg_raw_traffic(region_id);

-- ============================================================
-- TABLA: Staging de weather
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_raw_weather (
    record_id VARCHAR(20) PRIMARY KEY,
    datetime TIMESTAMP,
    station_id TIMESTAMP,
    temp DOUBLE PRECISION,
    feelslike DOUBLE PRECISION,
    precipprob DOUBLE PRECISION,
    windspeed VARCHAR(10),
    winddir DOUBLE PRECISION,
    conditions TEXT,
    -- Auditoría
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(255)
);

