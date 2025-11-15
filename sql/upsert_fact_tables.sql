-- ====================================================================
-- 1. Upsert para Trips
-- Mueve datos de staging.stg_raw_trips a dwh.fact_trips
-- ====================================================================
INSERT INTO dwh.fact_trips (
    trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
    percent_time_chicago, percent_distance_chicago, pickup_community_area,
    dropoff_community_area, fare, tip, additional_charges, trip_total,
    shared_trip_authorized, trips_pooled, pickup_location_id, dropoff_location_id
)
SELECT
    trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
    percent_time_chicago, percent_distance_chicago, pickup_community_area,
    dropoff_community_area, fare, tip, additional_charges, trip_total,
    shared_trip_authorized, trips_pooled, pickup_location_id, dropoff_location_id
FROM
    staging.stg_raw_trips
ON CONFLICT (trip_id) DO UPDATE SET
    trip_start_timestamp = EXCLUDED.trip_start_timestamp,
    trip_end_timestamp = EXCLUDED.trip_end_timestamp,
    trip_seconds = EXCLUDED.trip_seconds,
    trip_miles = EXCLUDED.trip_miles,
    fare = EXCLUDED.fare,
    tip = EXCLUDED.tip,
    trip_total = EXCLUDED.trip_total,
    shared_trip_authorized = EXCLUDED.shared_trip_authorized,
    trips_pooled = EXCLUDED.trips_pooled;

-- ====================================================================
-- 2. Upsert para Traffic
-- Mueve datos de staging.stg_raw_traffic a dwh.fact_traffic
-- ====================================================================
INSERT INTO dwh.fact_traffic (
    record_id, time, region_id, speed, bus_count, num_reads, hour
)
SELECT
    record_id, time, region_id, speed, bus_count, num_reads, hour
FROM
    staging.stg_raw_traffic
ON CONFLICT (record_id) DO UPDATE SET
    speed = EXCLUDED.speed,
    bus_count = EXCLUDED.bus_count,
    num_reads = EXCLUDED.num_reads;

-- ====================================================================
-- 3. Upsert para Weather
-- Mueve datos de staging.stg_raw_weather a dwh.fact_weather
-- ====================================================================
INSERT INTO dwh.fact_weather (
    record_id, datetime, station_id, temp, feelslike, precipprob, windspeed, winddir, conditions
)
SELECT
    record_id, datetime, station_id, temp, feelslike, precipprob, windspeed, winddir, conditions
FROM
    staging.stg_raw_weather
ON CONFLICT (record_id) DO UPDATE SET
    temp = EXCLUDED.temp,
    feelslike = EXCLUDED.feelslike,
    precipprob = EXCLUDED.precipprob,
    windspeed = EXCLUDED.windspeed,
    winddir = EXCLUDED.winddir,
    conditions = EXCLUDED.conditions;