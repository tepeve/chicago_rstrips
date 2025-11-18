-- ====================================================================
-- VISTA Agregados por Hora y Área de Recogida
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS datamarts.dm_trips_hourly_pickup_stats AS
SELECT
    DATE_TRUNC('hour', trip_start_timestamp) AS trip_hour,
    pickup_community_area,
    COUNT(trip_id) AS total_trips,
    AVG(rate_per_mile) AS avg_rate_per_mile,
    AVG(duration_minutes) AS avg_duration_minutes
FROM
    fact_tables.fact_trips
WHERE
    pickup_community_area IS NOT NULL
GROUP BY
    1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_trips_hourly_pickup ON datamarts.dm_trips_hourly_pickup_stats(trip_hour, pickup_community_area);

-- ====================================================================
-- Vista de Hechos de Viajes con Datos de Tráfico y Clima
-- ====================================================================

DROP MATERIALIZED VIEW IF EXISTS datamarts.fact_trips_with_traffic_weather;
CREATE MATERIALIZED VIEW datamarts.fact_trips_with_traffic_weather AS
WITH 
fact_trips AS (
    SELECT 
        trip_id,
        trip_start_timestamp,
        pickup_location_id
    FROM fact_tables.fact_trips
),
trips_locations_mapped AS (
    SELECT 
        location_id,
        region_id,
        station_id
    FROM dim_spatial.mapped_locations
)
SELECT
    ft.trip_id,
    ft.trip_start_timestamp,
    ft.pickup_location_id,
    
    traffic.region_id AS pickup_traffic_region_id,
    traffic.speed AS traffic_speed,
    traffic.num_reads AS traffic_num_reads,
    
    weather.station_id AS pickup_weather_station_id,
    weather.temp AS weather_temp,
    weather.feelslike AS weather_feelslike,
    weather.windspeed AS weather_windspeed,
    weather.conditions AS weather_conditions

FROM fact_trips ft
LEFT JOIN trips_locations_mapped tlm ON ft.pickup_location_id = tlm.location_id

-- Subconsulta para obtener el registro de tráfico más cercano en el tiempo
LEFT JOIN LATERAL (
    SELECT 
        ftr.region_id,
        ftr.speed,
        ftr.num_reads
    FROM fact_tables.fact_traffic ftr
    WHERE ftr.region_id = tlm.region_id
    ORDER BY ABS(EXTRACT(EPOCH FROM (ft.trip_start_timestamp - ftr."time")))
    LIMIT 1
) AS traffic ON TRUE

-- Subconsulta para obtener el registro de clima más cercano en el tiempo
LEFT JOIN LATERAL (
    SELECT 
        fw.station_id,
        fw.temp,
        fw.feelslike,
        fw.windspeed,
        fw.conditions
    FROM fact_tables.fact_weather fw
    WHERE fw.station_id = tlm.station_id
    ORDER BY ABS(EXTRACT(EPOCH FROM (ft.trip_start_timestamp - fw.datetime)))
    LIMIT 1
) AS weather ON TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_trips_with_traffic_weather_trip_id ON datamarts.fact_trips_with_traffic_weather(trip_id);

-- ====================================================================
-- Tabla Base para Machine Learning
-- ====================================================================

DROP MATERIALIZED VIEW IF EXISTS datamarts.ml_base_table;

CREATE MATERIALIZED VIEW datamarts.ml_base_table AS
WITH 
trips_with_window_features AS (
    SELECT
        -- Passthrough de columnas de fact_trips
        *,
        -- Feature de Ventana 1: Viajes activos totales en el sistema
        (COUNT(*) OVER (
            ORDER BY trip_start_timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) -
        COUNT(*) OVER (
            ORDER BY trip_end_timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND '1 second' PRECEDING
        )) AS active_trips_total,

        -- Feature de Ventana 2: Demanda reciente en la misma ubicación (últimos 30 min)
        COUNT(*) OVER (
            PARTITION BY pickup_location_id
            ORDER BY trip_start_timestamp
            RANGE BETWEEN '30 minutes' PRECEDING AND '1 second' PRECEDING
        ) AS trips_from_pickup_loc_last_30m

    FROM fact_tables.fact_trips
)
SELECT
    -- 1. Identificadores y Target (desde el CTE)
    twf.trip_id, 
    twf.trip_start_timestamp, 
    twf.trip_end_timestamp, 
    twf.rate_per_mile, 
    twf.fare,
    twf.duration_minutes, 
    twf.trip_miles,
    twf.trip_total,
    
    -- 2. Features de Tiempo
    EXTRACT(HOUR FROM twf.trip_start_timestamp) AS start_hour,
    EXTRACT(DOW FROM twf.trip_start_timestamp) AS day_of_week,
    EXTRACT(MONTH FROM twf.trip_start_timestamp) AS month_of_trip,
  
    -- 3. Features de Ubicación (desde el CTE)
    twf.pickup_location_id,
    twf.dropoff_location_id,
    twf.pickup_community_area, 
    twf.dropoff_community_area,
    
    -- 4. Features de Tráfico y Clima (desde la vista pre-calculada)
    fwtw.traffic_speed AS pickup_traffic_speed,
    fwtw.traffic_num_reads AS pickup_traffic_num_reads,
    fwtw.weather_temp AS pickup_weather_temp,
    fwtw.weather_feelslike AS pickup_weather_feelslike,
    fwtw.weather_windspeed AS pickup_weather_windspeed,
    fwtw.weather_conditions AS pickup_weather_conditions,

    -- 5. Features Calculadas (Ventanas, desde el CTE)
    twf.active_trips_total,
    twf.trips_from_pickup_loc_last_30m

FROM trips_with_window_features twf
-- Join para features de tráfico y clima
LEFT JOIN datamarts.fact_trips_with_traffic_weather fwtw ON twf.trip_id = fwtw.trip_id;

-- Índice único para refresco concurrente
CREATE UNIQUE INDEX IF NOT EXISTS idx_ml_base_table_trip_id ON datamarts.ml_base_table(trip_id);