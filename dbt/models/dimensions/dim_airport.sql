-- models/dimensions/dim_airport.sql
-- Airport dimension with weather context

{{ config(materialized='table', schema='gold') }}

WITH airports AS (
    SELECT DISTINCT airport_code
    FROM {{ ref('stg_airports') }}
),
weather_summary AS (
    SELECT
        airport_code,
        AVG(temperature_c)      AS avg_temperature_c,
        AVG(wind_speed_kmh)     AS avg_wind_speed_kmh,
        AVG(precipitation_mm)   AS avg_precipitation_mm,
        AVG(visibility_km)      AS avg_visibility_km
    FROM {{ ref('stg_weather') }}
    GROUP BY airport_code
)
SELECT
    a.airport_code,
    a.airport_code          AS airport_name,
    w.avg_temperature_c,
    w.avg_wind_speed_kmh,
    w.avg_precipitation_mm,
    w.avg_visibility_km
FROM airports a
LEFT JOIN weather_summary w ON a.airport_code = w.airport_code
