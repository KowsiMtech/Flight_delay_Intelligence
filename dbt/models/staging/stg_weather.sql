-- models/staging/stg_weather.sql
-- Staging model for silver weather table

{{ config(materialized='view', schema='gold') }}

SELECT
    airport_code,
    weather_date,
    weather_hour,
    event_timestamp,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    visibility_km,
    weather_code,
    weather_severity,
    _ingested_at
FROM flight_catalog.silver.weather
