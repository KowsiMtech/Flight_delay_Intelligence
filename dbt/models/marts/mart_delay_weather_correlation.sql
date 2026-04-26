-- models/marts/mart_delay_weather_correlation.sql
-- Weather vs delay correlation mart

{{ config(materialized='table', schema='gold') }}

WITH flight_weather AS (
    SELECT
        f.flight_id,
        f.flight_date,
        f.origin,
        f.carrier,
        f.dep_delay,
        f.arr_delay,
        f.weather_delay,
        f.is_delayed,
        f.flight_year,
        f.flight_month,
        w.temperature_c,
        w.wind_speed_kmh,
        w.precipitation_mm,
        w.visibility_km,
        COALESCE(w.weather_severity, 'Unknown')    AS weather_severity
    FROM {{ ref('fact_flight_delays') }} f
    LEFT JOIN {{ ref('stg_weather') }} w
        ON f.origin = w.airport_code
        AND CAST(f.flight_date AS DATE) = w.weather_date
)
SELECT
    origin,
    flight_year,
    flight_month,
    COALESCE(weather_severity, 'Unknown')      AS weather_severity,
    COUNT(*)                                                AS total_flights,
    SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END)     AS weather_related_delays,
    ROUND(AVG(dep_delay), 2)                                AS avg_dep_delay_min,
    ROUND(AVG(weather_delay), 2)                            AS avg_weather_delay_min,
    ROUND(AVG(temperature_c), 2)                            AS avg_temperature_c,
    ROUND(AVG(wind_speed_kmh), 2)                           AS avg_wind_speed_kmh,
    ROUND(AVG(precipitation_mm), 2)                         AS avg_precipitation_mm,
    ROUND(AVG(visibility_km), 2)                            AS avg_visibility_km
FROM flight_weather
GROUP BY origin, flight_year, flight_month, weather_severity
