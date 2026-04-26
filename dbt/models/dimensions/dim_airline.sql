-- models/dimensions/dim_airline.sql
-- Airline dimension - unique carriers from flights

{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    carrier                                         AS airline_code,
    carrier                                         AS airline_name,
    COUNT(*)        OVER (PARTITION BY carrier)     AS total_flights,
    AVG(dep_delay)  OVER (PARTITION BY carrier)     AS avg_dep_delay,
    AVG(arr_delay)  OVER (PARTITION BY carrier)     AS avg_arr_delay,
    SUM(CASE WHEN is_cancelled = 1 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY carrier)     AS total_cancellations
FROM {{ ref('stg_flights') }}
WHERE carrier IS NOT NULL
