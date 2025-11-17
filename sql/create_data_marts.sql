-- ====================================================================
-- VISTA 1: Agregados por Hora y Área de Recogida
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dm_trips_hourly_pickup_stats AS
SELECT
    DATE_TRUNC('hour', trip_start_timestamp) AS trip_hour,
    pickup_community_area,
    COUNT(trip_id) AS total_trips,
    AVG(rate_per_mile) AS avg_rate_per_mile,
    AVG(duration_minutes) AS avg_duration_minutes,
    SUM(fare) AS total_fare
FROM
    fact_tables.fact_trips
WHERE
    pickup_community_area IS NOT NULL
GROUP BY
    1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_trips_hourly_pickup ON dm_trips_hourly_pickup_stats(trip_hour, pickup_community_area);

-- ====================================================================
-- VISTA 2: Tabla Ancha para Machine Learning
-- Esta vista une trips, traffic y weather.
-- ====================================================================

DROP MATERIALIZED VIEW IF EXISTS dm_ml_features_wide;
CREATE MATERIALIZED VIEW dm_ml_features_wide AS
WITH trips_with_window_features AS (
    SELECT
        trip_id,
        trip_start_timestamp,
        pickup_location_id,
        -- Viajes activos totales al iniciar este viaje
        COUNT(*) OVER (
            ORDER BY trip_start_timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) -
        COUNT(*) OVER (
            ORDER BY trip_end_timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND '1 second' PRECEDING
        ) AS active_trips_total,

        -- Viajes que terminaron en la misma ubicación de recogida en los últimos 15 min
        COUNT(*) OVER (
            PARTITION BY pickup_location_id
            ORDER BY trip_start_timestamp
            RANGE BETWEEN '15 minutes' PRECEDING AND '1 second' PRECEDING
        ) AS trips_ended_at_pickup_last_15m

    FROM fact_tables.fact_trips
)

, weather_per_trip AS (
    -- Asocia a cada viaje la lectura de clima más cercana en tiempo y espacio
    SELECT DISTINCT ON (t.trip_id)
        t.trip_id,
        w.temp,
        w.feelslike,
        w.precipprob,
        w.windspeed,
        w.conditions
    FROM fact_tables.fact_trips t
    JOIN dim_spatial.trips_locations loc ON t.pickup_location_id = loc.location_id
    -- La zona de Voronoi nos dice cuál es la estación más cercana a la ubicación de recogida
    JOIN dim_spatial.weather_voronoi_zones vz ON ST_Contains(vz.geometry, ST_Point(loc.longitude, loc.latitude))
    JOIN fact_tables.fact_weather w ON w.station_id = vz.station_id
    -- Buscamos el clima más cercano en el tiempo al inicio del viaje
    ORDER BY t.trip_id, ABS(EXTRACT(EPOCH FROM (t.trip_start_timestamp - w.datetime)))
)

SELECT
    -- Features del viaje
    ft.trip_id, ft.trip_start_timestamp, ft.rate_per_mile, ft.duration_minutes, ft.trip_miles,
    ft.shared_trip_authorized, ft.trips_pooled,
    
    -- Features de tiempo
    EXTRACT(HOUR FROM ft.trip_start_timestamp) AS start_hour,
    EXTRACT(DOW FROM ft.trip_start_timestamp) AS start_day_of_week,
    
    -- Features de ubicación
    ft.pickup_community_area, ft.dropoff_community_area,
    
    -- Features de clima
    wt.temp, wt.feelslike, wt.precipprob, wt.windspeed, wt.conditions,
    
    -- Features de tráfico
    (SELECT ftr.speed FROM fact_tables.fact_traffic ftr WHERE ftr.region_id = ft.pickup_community_area ORDER BY ABS(EXTRACT(EPOCH FROM (ft.trip_start_timestamp - ftr.time))) LIMIT 1) AS pickup_traffic_speed,

    -- Features de ventana
    twf.active_trips_total,
    twf.trips_ended_at_pickup_last_15m

FROM fact_tables.fact_trips ft
LEFT JOIN weather_per_trip wt ON ft.trip_id = wt.trip_id
LEFT JOIN trips_with_window_features twf ON ft.trip_id = twf.trip_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_ml_features_wide_trip_id ON dm_ml_features_wide(trip_id);