-- models/marts/mart_carrier_performance.sql
-- Carrier performance mart for Power BI dashboard

{{ config(materialized='table', schema='gold') }}

SELECT
    carrier,
    flight_year,
    flight_month,
    flight_quarter,
    season,
    COUNT(*)                                                AS total_flights,
    SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END)     AS total_delayed,
    SUM(CASE WHEN is_cancelled = 1 THEN 1 ELSE 0 END)      AS total_cancelled,
    ROUND(AVG(dep_delay), 2)                                AS avg_dep_delay_min,
    ROUND(AVG(arr_delay), 2)                                AS avg_arr_delay_min,
    ROUND(AVG(carrier_delay), 2)                            AS avg_carrier_delay_min,
    ROUND(AVG(weather_delay), 2)                            AS avg_weather_delay_min,
    ROUND(AVG(nas_delay), 2)                                AS avg_nas_delay_min,
    ROUND(AVG(late_aircraft_delay), 2)                      AS avg_late_aircraft_delay_min,
    ROUND(
        SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                       AS delay_rate_pct,
    ROUND(
        SUM(CASE WHEN is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                       AS cancellation_rate_pct
FROM {{ ref('fact_flight_delays') }}
GROUP BY carrier, flight_year, flight_month, flight_quarter, season
