-- Crear el schema para el Data Warehouse si no existe
CREATE SCHEMA IF NOT EXISTS dwh;

-- ====================================================================
-- 1. Tabla de Hechos: Trips
-- ====================================================================
CREATE TABLE IF NOT EXISTS dwh.fact_trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    trip_start_timestamp TIMESTAMP,
    trip_end_timestamp TIMESTAMP,
    trip_seconds INTEGER,
    trip_miles FLOAT,
    percent_time_chicago FLOAT,
    percent_distance_chicago FLOAT,
    pickup_community_area INTEGER,
    dropoff_community_area INTEGER,
    fare FLOAT,
    tip FLOAT,
    additional_charges FLOAT,
    trip_total FLOAT,
    shared_trip_authorized BOOLEAN,
    trips_pooled INTEGER,
    pickup_location_id VARCHAR(20),
    dropoff_location_id VARCHAR(20),
    -- Foreign keys (opcional, pero recomendado)
    CONSTRAINT fk_pickup_location FOREIGN KEY (pickup_location_id) REFERENCES dim_spatial.trips_locations(location_id),
    CONSTRAINT fk_dropoff_location FOREIGN KEY (dropoff_location_id) REFERENCES dim_spatial.trips_locations(location_id)
);

-- ====================================================================
-- 2. Tabla de Hechos: Traffic
-- ====================================================================
CREATE TABLE IF NOT EXISTS dwh.fact_traffic (
    record_id VARCHAR PRIMARY KEY,
    time TIMESTAMP,
    region_id INTEGER,
    speed FLOAT,
    bus_count INTEGER,
    num_reads INTEGER,
    hour INTEGER,
    -- Foreign key
    CONSTRAINT fk_traffic_region FOREIGN KEY (region_id) REFERENCES dim_spatial.traffic_regions(region_id)
);

-- ====================================================================
-- 3. Tabla de Hechos: Weather
-- ====================================================================
CREATE TABLE IF NOT EXISTS dwh.fact_weather (
    record_id VARCHAR PRIMARY KEY,
    datetime TIMESTAMP,
    station_id VARCHAR,
    temp FLOAT,
    feelslike FLOAT,
    precipprob FLOAT,
    windspeed VARCHAR, -- Mantener como string si la data no es limpia
    winddir FLOAT,
    conditions VARCHAR,
    -- Foreign key
    CONSTRAINT fk_weather_station FOREIGN KEY (station_id) REFERENCES dim_spatial.weather_stations_points(station_id)
);