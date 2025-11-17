-- ====================================================================
-- VISTA 1: Agregados por Hora y Área de Recogida
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS datamarts.dm_trips_hourly_pickup_stats AS
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
-- VISTA 2: Tabla Base para Machine Learning (Sin Join Espacial)
-- Esta vista prepara los datos de Trips y Tráfico. 
-- El cruce con Clima (Weather) se delega a Python/GeoPandas.
-- ====================================================================

DROP MATERIALIZED VIEW IF EXISTS datamarts.dm_ml_features_wide;

CREATE MATERIALIZED VIEW datamarts.dm_ml_features_wide AS
WITH trips_with_window_features AS (
    SELECT
        trip_id,
        trip_start_timestamp,
        pickup_location_id,
        -- Feature de Ventana 1: Viajes activos totales en el sistema
        COUNT(*) OVER (
            ORDER BY trip_start_timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) -
        COUNT(*) OVER (
            ORDER BY trip_end_timestamp
            RANGE BETWEEN UNBOUNDED PRECEDING AND '1 second' PRECEDING
        ) AS active_trips_total,

        -- Feature de Ventana 2: Demanda reciente en la misma ubicación (últimos 15 min)
        COUNT(*) OVER (
            PARTITION BY pickup_location_id
            ORDER BY trip_start_timestamp
            RANGE BETWEEN '15 minutes' PRECEDING AND '1 second' PRECEDING
        ) AS trips_ended_at_pickup_last_15m

    FROM fact_tables.fact_trips
)

SELECT
    -- 1. Identificadores y Target
    ft.trip_id, 
    ft.trip_start_timestamp, 
    ft.rate_per_mile, 
    ft.duration_minutes, 
    ft.trip_miles,
    ft.fare,
    ft.trip_total,
    
    -- 2. Features de Tiempo
    EXTRACT(HOUR FROM ft.trip_start_timestamp) AS start_hour,
    EXTRACT(DOW FROM ft.trip_start_timestamp) AS start_day_of_week,
    
    -- 3. Features de Ubicación (IDs y Coordenadas para Python)
    ft.pickup_community_area, 
    ft.dropoff_community_area,
    ft.pickup_location_id,
    loc.latitude AS pickup_latitude,   -- Agregado para facilitar GeoPandas
    loc.longitude AS pickup_longitude, -- Agregado para facilitar GeoPandas
    
    -- 4. Features de Tráfico (Join simple por ID, eficiente en SQL)
    -- Busca la velocidad registrada más cercana en tiempo para esa región
    (
        SELECT ftr.speed 
        FROM fact_tables.fact_traffic ftr 
        WHERE ftr.region_id = ft.pickup_community_area 
        ORDER BY ABS(EXTRACT(EPOCH FROM (ft.trip_start_timestamp - ftr.time))) 
        LIMIT 1
    ) AS pickup_traffic_speed,

    -- 5. Features Calculadas (Ventanas)
    twf.active_trips_total,
    twf.trips_ended_at_pickup_last_15m

FROM fact_tables.fact_trips ft
-- Join estándar para obtener coordenadas (No es espacial, es por ID)
LEFT JOIN dim_spatial.trips_locations loc ON ft.pickup_location_id = loc.location_id
LEFT JOIN trips_with_window_features twf ON ft.trip_id = twf.trip_id;

-- Índice único para refresco concurrente
CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_ml_features_wide_trip_id ON dm_ml_features_wide(trip_id);