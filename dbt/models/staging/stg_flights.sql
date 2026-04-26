-- models/staging/stg_flights.sql
-- Staging model for silver flights table

{{ config(materialized='view', schema='gold') }}

SELECT
    flight_id,
    flight_date,
    carrier,
    tail_number,
    origin,
    dest,
    scheduled_dep_time,
    arr_delay,
    dep_delay,
    carrier_delay,
    weather_delay,
    nas_delay,
    late_aircraft_delay,
    is_cancelled,
    cancellation_code,
    is_delayed,
    primary_delay_cause,
    flight_year,
    flight_month,
    flight_quarter,
    season,
    event_timestamp,
    _ingested_at
FROM flight_catalog.silver.flights
