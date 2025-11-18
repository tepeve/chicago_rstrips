-- ====================================================================
-- 1. Upsert para Trips
-- Mueve datos de staging.stg_raw_trips a fact_tables.fact_trips
-- Solo procesa datos dentro de la ventana de ejecución (incremental)
-- ====================================================================
INSERT INTO fact_tables.fact_trips (
    trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
    percent_time_chicago, percent_distance_chicago, pickup_community_area,
    dropoff_community_area, fare, tip, additional_charges, trip_total,
    shared_trip_authorized, trips_pooled, pickup_location_id, dropoff_location_id,
    rate_per_mile, duration_minutes, batch_id, created_at
)
SELECT
    trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
    percent_time_chicago, percent_distance_chicago, pickup_community_area,
    dropoff_community_area, fare, tip, additional_charges, trip_total,
    shared_trip_authorized, trips_pooled, pickup_location_id, dropoff_location_id,
    CASE WHEN trip_miles > 0 THEN fare / trip_miles 
        ELSE 0 
    END AS rate_per_mile,
    trip_seconds / 60.0 AS duration_minutes,
    batch_id,
    CURRENT_TIMESTAMP
FROM
    staging.stg_raw_trips
WHERE
    -- FILTRO INCREMENTAL: Solo registros de esta ejecución
    trip_start_timestamp >= :start_date 
    AND trip_start_timestamp < :end_date
    -- FILTRO DE CALIDAD: Evita datos inválidos
    AND fare IS NOT NULL
    AND fare > 0
    AND trip_seconds > 60  -- Al menos 1 minuto
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    -- FILTRO DE GEOLOCALIZACIÓN: Excluye ubicaciones inválidas
    AND NOT EXISTS (
        SELECT il.location_id
        FROM dim_spatial.invalid_locations il
        WHERE il.location_id = staging.stg_raw_trips.pickup_location_id
    )
    AND NOT EXISTS (
        SELECT il.location_id
        FROM dim_spatial.invalid_locations il
        WHERE il.location_id = staging.stg_raw_trips.dropoff_location_id
    )
ON CONFLICT (trip_id) DO UPDATE SET
    trip_start_timestamp = EXCLUDED.trip_start_timestamp,
    trip_end_timestamp = EXCLUDED.trip_end_timestamp,
    trip_seconds = EXCLUDED.trip_seconds,
    trip_miles = EXCLUDED.trip_miles,
    fare = EXCLUDED.fare,
    tip = EXCLUDED.tip,
    trip_total = EXCLUDED.trip_total,
    shared_trip_authorized = EXCLUDED.shared_trip_authorized,
    trips_pooled = EXCLUDED.trips_pooled,
    rate_per_mile = EXCLUDED.rate_per_mile,
    duration_minutes = EXCLUDED.duration_minutes,
    batch_id = EXCLUDED.batch_id,
    created_at = EXCLUDED.created_at;

-- ====================================================================
-- 2. Upsert para Traffic
-- ====================================================================
INSERT INTO fact_tables.fact_traffic (
    record_id, time, region_id, speed, bus_count, num_reads, created_at, batch_id
)
SELECT
    record_id, time, region_id, speed, bus_count, num_reads, created_at, batch_id
FROM
    staging.stg_raw_traffic
WHERE
    time >= :start_date 
    AND time < :end_date
ON CONFLICT (record_id) DO UPDATE SET
    speed = EXCLUDED.speed,
    bus_count = EXCLUDED.bus_count,
    num_reads = EXCLUDED.num_reads,
    batch_id = EXCLUDED.batch_id,
    created_at = EXCLUDED.created_at;

-- ====================================================================
-- 3. Upsert para Weather
-- ====================================================================
INSERT INTO fact_tables.fact_weather (
    record_id, datetime, station_id, temp, feelslike, precipprob, windspeed, winddir, conditions, created_at, batch_id
)
SELECT
    record_id, datetime, station_id, temp, feelslike, precipprob, windspeed, winddir, conditions, created_at, batch_id
FROM
    staging.stg_raw_weather
WHERE
    datetime >= :start_date 
    AND datetime < :end_date
ON CONFLICT (record_id) DO UPDATE SET
    temp = EXCLUDED.temp,
    feelslike = EXCLUDED.feelslike,
    precipprob = EXCLUDED.precipprob,
    windspeed = EXCLUDED.windspeed,
    winddir = EXCLUDED.winddir,
    conditions = EXCLUDED.conditions,
    batch_id = EXCLUDED.batch_id,
    created_at = EXCLUDED.created_at;