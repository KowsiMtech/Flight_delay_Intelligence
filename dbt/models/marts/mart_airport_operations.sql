-- models/marts/mart_airport_operations.sql
-- Airport operations mart - origin airport performance

{{ config(materialized='table', schema='gold') }}

SELECT
    origin                                                  AS airport_code,
    flight_year,
    flight_month,
    COUNT(*)                                                AS total_departures,
    SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END)     AS total_delayed,
    SUM(CASE WHEN is_cancelled = 1 THEN 1 ELSE 0 END)      AS total_cancelled,
    SUM(CASE WHEN is_cascade_delay = TRUE THEN 1 ELSE 0 END) AS total_cascade_delays,
    ROUND(AVG(dep_delay), 2)                                AS avg_dep_delay_min,
    ROUND(AVG(arr_delay), 2)                                AS avg_arr_delay_min,
    ROUND(
        SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                       AS delay_rate_pct,
    COUNT(DISTINCT carrier)                                 AS num_carriers_served
FROM {{ ref('fact_flight_delays') }}
GROUP BY origin, flight_year, flight_month
