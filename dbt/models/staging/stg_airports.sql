-- models/staging/stg_airports.sql
-- Airport codes derived from flights (no dedicated airports source table)

{{ config(materialized='view', schema='gold') }}

SELECT DISTINCT
    origin AS airport_code,
    origin AS airport_name
FROM flight_catalog.silver.flights
WHERE origin IS NOT NULL

UNION

SELECT DISTINCT
    dest AS airport_code,
    dest AS airport_name
FROM flight_catalog.silver.flights
WHERE dest IS NOT NULL
