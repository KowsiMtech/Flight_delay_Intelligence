# silver_dlt_pipeline.py
# DO NOT run cell by cell — run via DLT Pipeline only

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = 'flight_catalog'

# ── STEP 1: Read from Bronze ──────────────────────────────────

@dlt.table(name='bronze_flights_ref',
           comment='Bronze flights reference for DLT lineage')
def bronze_flights_ref():
    return spark.readStream.table(f'{CATALOG}.bronze.flights')

@dlt.table(name='bronze_weather_ref',
           comment='Bronze weather reference for DLT lineage')
def bronze_weather_ref():
    return spark.readStream.table(f'{CATALOG}.bronze.weather')

# ── STEP 2: Clean and validate → Silver flights ───────────────

@dlt.table(
    name    = 'flights',
    comment = 'Cleaned Silver flights'
)
@dlt.expect_or_drop('valid_flight_date', 'flight_date IS NOT NULL')
@dlt.expect_or_drop('valid_tail_number', 'tail_number IS NOT NULL')
@dlt.expect_or_drop('valid_origin',      'origin IS NOT NULL')
@dlt.expect_or_drop('valid_dest',        'dest IS NOT NULL')
@dlt.expect_or_drop('valid_arr_delay',   'arr_delay BETWEEN -100 AND 1000')
def silver_flights():
    return (
        dlt.read('bronze_flights_ref')
        .select(
            'flight_date',
            'carrier',
            'tail_number',
            'origin',
            'dest',
            F.coalesce('arr_delay',          F.lit(0)).alias('arr_delay'),
            F.coalesce('dep_delay',          F.lit(0)).alias('dep_delay'),
            F.coalesce('carrier_delay',      F.lit(0)).alias('carrier_delay'),
            F.coalesce('weather_delay',      F.lit(0)).alias('weather_delay'),
            F.coalesce('nas_delay',          F.lit(0)).alias('nas_delay'),
            F.coalesce('late_aircraft_delay',F.lit(0)).alias('late_aircraft_delay'),
            F.coalesce(F.col('cancelled').cast('int'), F.lit(0)).alias('is_cancelled'),
            'cancellation_code',
            # is_delayed: BTS standard = 15+ min late
            F.when(F.col('arr_delay') >= 15, True)
             .otherwise(False).alias('is_delayed'),
            # Primary delay cause
            F.when(
                (F.col('late_aircraft_delay') > 0) &
                (F.col('late_aircraft_delay') >= F.col('carrier_delay')) &
                (F.col('late_aircraft_delay') >= F.col('weather_delay')),
                'Late Aircraft'
            ).when(
                (F.col('carrier_delay') > 0) &
                (F.col('carrier_delay') >= F.col('weather_delay')),
                'Carrier'
            ).when(F.col('weather_delay') > 0, 'Weather')
             .when(F.col('nas_delay') > 0, 'NAS')
             .otherwise('On Time').alias('primary_delay_cause'),
            # Date parts for Power BI slicers
            F.year('flight_date').alias('flight_year'),
            F.month('flight_date').alias('flight_month'),
            F.quarter('flight_date').alias('flight_quarter'),
            F.when(F.month('flight_date').isin(12,1,2),'Winter')
             .when(F.month('flight_date').isin(3,4,5), 'Spring')
             .when(F.month('flight_date').isin(6,7,8), 'Summer')
             .otherwise('Fall').alias('season'),
            # Surrogate key
            F.md5(F.concat_ws('|',
                F.col('flight_date').cast('string'),
                F.col('carrier'),
                F.col('tail_number'),
                F.col('origin'),
                F.col('dest')
            )).alias('flight_id'),
            'event_timestamp',
            '_ingested_at'
        )
        .filter(
            F.col('origin').isin('YYC','LAX','ORD','JFK','YYZ') |
            F.col('dest').isin('YYC','LAX','ORD','JFK','YYZ')
        )
    )

# ── STEP 3: Clean weather → Silver weather ────────────────────

@dlt.table(name='weather', comment='Cleaned Silver weather')
@dlt.expect_or_drop('valid_airport', 'airport_code IS NOT NULL')
@dlt.expect_or_drop('valid_time',    'event_timestamp IS NOT NULL')
def silver_weather():
    return (
        dlt.read('bronze_weather_ref')
        .select(
            'airport_code',
            F.to_date('event_timestamp').alias('weather_date'),
            F.hour('event_timestamp').alias('weather_hour'),
            'event_timestamp',
            F.round('temperature_c',   1).alias('temperature_c'),
            F.round('precipitation_mm',2).alias('precipitation_mm'),
            F.round('wind_speed_kmh',  1).alias('wind_speed_kmh'),
            F.round(F.col('visibility_km') / 1000, 1).alias('visibility_km'),
            'weather_code',
            F.when(
                (F.col('precipitation_mm') >= 10) |
                (F.col('wind_speed_kmh') >= 60),    'Severe'
            ).when(
                (F.col('precipitation_mm') >= 5) |
                (F.col('wind_speed_kmh') >= 40),    'Moderate'
            ).when(
                F.col('precipitation_mm') > 0,      'Light'
            ).otherwise('Clear').alias('weather_severity'),
            '_ingested_at'
        )
        .filter(F.col('airport_code').isin('YYC','LAX','ORD','JFK','YYZ'))
    )

# ── STEP 4: Cascade detection ─────────────────────────────────

@dlt.table(name='delay_cascade', comment='Cascade delays by tail number')
def silver_delay_cascade():
    window = (
        Window
        .partitionBy('tail_number', 'flight_date')
        .orderBy('scheduled_dep_time')
    )
    return (
        dlt.read('flights')
        .withColumn('flight_sequence', F.row_number().over(window))
        .withColumn('prev_arr_delay',  F.lag('arr_delay').over(window))
        .withColumn('prev_dest',       F.lag('dest').over(window))
        .withColumn('is_cascade_delay',
            (F.col('prev_arr_delay') > 15) &
            (F.col('late_aircraft_delay') > 0) &
            (F.col('prev_dest') == F.col('origin'))
        )
        .withColumn('cascade_delay_min',
            F.when(F.col('is_cascade_delay'), F.col('late_aircraft_delay'))
             .otherwise(0)
        )
    )