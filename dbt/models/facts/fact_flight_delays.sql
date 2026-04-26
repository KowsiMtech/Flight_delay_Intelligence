-- models/facts/fact_flight_delays.sql
-- Core fact table for flight delay analysis

{{ config(materialized='table', schema='gold') }}

SELECT
    f.flight_id,
    f.flight_date,
    f.carrier,
    f.tail_number,
    f.origin,
    f.dest,
    f.scheduled_dep_time,
    f.dep_delay,
    f.arr_delay,
    f.carrier_delay,
    f.weather_delay,
    f.nas_delay,
    f.late_aircraft_delay,
    f.is_cancelled,
    f.cancellation_code,
    f.is_delayed,
    f.primary_delay_cause,
    f.flight_year,
    f.flight_month,
    f.flight_quarter,
    f.season,
    -- cascade delay info
    dc.is_cascade_delay,
    dc.cascade_delay_min,
    dc.prev_arr_delay,
    dc.prev_dest,
    dc.flight_sequence
FROM {{ ref('stg_flights') }} f
LEFT JOIN flight_catalog.silver.delay_cascade dc
    ON f.flight_id = dc.flight_id
