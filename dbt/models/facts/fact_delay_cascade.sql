-- models/facts/fact_delay_cascade.sql
-- Cascade delay detection — tracks delay chains by tail number
-- Answers Q4: which aircraft cause most cascade delays?
-- Key insight: same tail number, same day, sequential flights

{{
  config(
    materialized = 'incremental',
    schema = 'gold',
    unique_key = 'cascade_id',
    incremental_strategy = 'merge'
  )
}}

WITH ordered_flights AS (
    -- Order all flights by tail number and departure time within same day
    SELECT
        flight_id,
        flight_date,
        carrier,
        tail_number,
        origin,
        dest,
        scheduled_dep_time,
        dep_delay,
        arr_delay,
        late_aircraft_delay,
        is_delayed,
        season,
        flight_year,
        flight_month,

        -- Row number per tail number per day — identifies flight sequence
        ROW_NUMBER() OVER (
            PARTITION BY tail_number, flight_date
            ORDER BY scheduled_dep_time
        )                                                  AS flight_sequence,

        -- Previous flight arrival delay for same aircraft
        LAG(arr_delay) OVER (
            PARTITION BY tail_number, flight_date
            ORDER BY scheduled_dep_time
        )                                                  AS prev_flight_arr_delay,

        -- Previous flight destination = this flight's origin (sanity check)
        LAG(dest) OVER (
            PARTITION BY tail_number, flight_date
            ORDER BY scheduled_dep_time
        )                                                  AS prev_flight_dest

    FROM {{ ref('stg_flights') }}
    WHERE tail_number IS NOT NULL
),

cascade_flagged AS (
    SELECT
        *,
        -- A cascade delay is when:
        -- 1. Previous flight was delayed (arrived late)
        -- 2. This flight also has late_aircraft_delay > 0
        -- 3. Previous destination matches this origin
        CASE
            WHEN prev_flight_arr_delay > 15
             AND late_aircraft_delay> 0
             AND prev_flight_dest = origin
            THEN TRUE
            ELSE FALSE
        END                                                AS is_cascade_delay,

        -- How much of this delay is attributed to cascade
        CASE
            WHEN prev_flight_arr_delay > 15
             AND late_aircraft_delay> 0
            THEN late_aircraft_delay
            ELSE 0
        END                                                AS cascade_delay

    FROM ordered_flights
)

SELECT
    -- Surrogate key
    MD5(CONCAT(
        COALESCE(flight_id, ''),
        COALESCE(tail_number, ''),
        COALESCE(CAST(flight_date AS STRING), '')
    ))                                                     AS cascade_id,

    flight_id,
    flight_date,
    carrier,
    tail_number,
    origin,
    dest,
    flight_sequence,
    scheduled_dep_time,
    dep_delay,
    arr_delay,
    late_aircraft_delay,
    prev_flight_arr_delay,
    prev_flight_dest,
    is_delayed,
    is_cascade_delay,
    cascade_delay,
    season,
    flight_year,
    flight_month,

    -- Cascade severity
    CASE
        WHEN cascade_delay >= 60 THEN 'Severe (>60 min)'
        WHEN cascade_delay>= 30 THEN 'Moderate (30-60 min)'
        WHEN cascade_delay >= 15 THEN 'Minor (15-30 min)'
        ELSE 'None'
    END                                                    AS cascade_severity,

    CURRENT_TIMESTAMP()                                    AS dbt_loaded_at

FROM cascade_flagged

{% if is_incremental() %}
WHERE flight_date > (
    SELECT MAX(flight_date) FROM {{ this }}
)
{% endif %}
