# Databricks notebook source
# MAGIC %run "/Flight_Analytics/00_setup/includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold Layer - PySpark Aggregations (dbt Alternative)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

GOLD_CATALOG  = 'flight_catalog'
GOLD_SCHEMA   = 'gold_gold'
GOLD_PATH     = 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/gold'

def write_gold_table(df, table_name):
    """Write DataFrame to Unity Catalog Gold table and ADLS Parquet."""
    full_table  = f'{GOLD_CATALOG}.{GOLD_SCHEMA}.{table_name}'
    output_path = f'{GOLD_PATH}/{table_name}/'
    row_count   = df.count()

    # Write to Unity Catalog Delta table
    df.write \
      .format('delta') \
      .mode('overwrite') \
      .option('overwriteSchema', 'true') \
      .saveAsTable(full_table)

    # Write to ADLS as Parquet
    df.write \
      .format('parquet') \
      .mode('overwrite') \
      .option('compression', 'snappy') \
      .save(output_path)

    print(f' {table_name:<45} {row_count:>6} rows → Unity Catalog + ADLS')
    return row_count

print(f'Starting Gold layer build at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

# COMMAND ----------

## Read Silver Tables

silver_flights  = spark.table('flight_catalog.silver.flights')
silver_weather  = spark.table('flight_catalog.silver.weather')
silver_cascade  = spark.table('flight_catalog.silver.delay_cascade')

print(f'silver.flights        : {silver_flights.count():>6} rows')
print(f'silver.weather        : {silver_weather.count():>6} rows')
print(f'silver.delay_cascade  : {silver_cascade.count():>6} rows')

# COMMAND ----------

## dim_airline

dim_airline = silver_flights \
    .groupBy('carrier') \
    .agg(
        F.count('*')                                        .alias('total_flights'),
        F.round(F.avg('dep_delay'), 2)                      .alias('avg_dep_delay'),
        F.round(F.avg('arr_delay'), 2)                      .alias('avg_arr_delay'),
        F.sum(F.when(F.col('is_cancelled') == 1, 1)
               .otherwise(0))                               .alias('total_cancellations'),
        F.round(
            F.sum(F.when(F.col('is_delayed') == True, 1)
                   .otherwise(0)) * 100.0 / F.count('*'), 2
        )                                                   .alias('delay_rate_pct')
    ) \
    .withColumnRenamed('carrier', 'airline_code') \
    .withColumn('airline_name', F.col('airline_code'))

write_gold_table(dim_airline, 'dim_airline')

# COMMAND ----------

## dim_airport

airports = silver_flights \
    .select(F.col('origin').alias('airport_code')) \
    .union(silver_flights.select(F.col('dest').alias('airport_code'))) \
    .distinct() \
    .filter(F.col('airport_code').isNotNull())

weather_summary = silver_weather \
    .groupBy('airport_code') \
    .agg(
        F.round(F.avg('temperature_c'),   2).alias('avg_temperature_c'),
        F.round(F.avg('wind_speed_kmh'),  2).alias('avg_wind_speed_kmh'),
        F.round(F.avg('precipitation_mm'),2).alias('avg_precipitation_mm'),
        F.round(F.avg('visibility_km'),   2).alias('avg_visibility_km')
    )

dim_airport = airports \
    .join(weather_summary, on='airport_code', how='left') \
    .withColumn('airport_name', F.col('airport_code'))

write_gold_table(dim_airport, 'dim_airport')

# COMMAND ----------

## fact_flight_delays

fact_flight_delays = silver_flights \
    .join(
        silver_cascade.select(
            'flight_id', 'is_cascade_delay',
            'cascade_delay_min', 'prev_arr_delay',
            'prev_dest', 'flight_sequence'
        ),
        on='flight_id',
        how='left'
    ) \
    .select(
        'flight_id',
        'flight_date',
        'carrier',
        'tail_number',
        'origin',
        'dest',
        'scheduled_dep_time',
        'dep_delay',
        'arr_delay',
        'carrier_delay',
        'weather_delay',
        'nas_delay',
        'late_aircraft_delay',
        'is_cancelled',
        'cancellation_code',
        'is_delayed',
        'primary_delay_cause',
        'flight_year',
        'flight_month',
        'flight_quarter',
        'season',
        F.coalesce('is_cascade_delay',  F.lit(False)) .alias('is_cascade_delay'),
        F.coalesce('cascade_delay_min', F.lit(0.0))   .alias('cascade_delay_min'),
        'prev_arr_delay',
        'prev_dest',
        'flight_sequence',
    )

write_gold_table(fact_flight_delays, 'fact_flight_delays')

# COMMAND ----------

## fact_delay_cascade

window = Window \
    .partitionBy('tail_number', 'flight_date') \
    .orderBy('scheduled_dep_time')

fact_delay_cascade = silver_flights \
    .withColumn('flight_sequence',    F.row_number().over(window)) \
    .withColumn('prev_flight_arr_delay', F.lag('arr_delay').over(window)) \
    .withColumn('prev_flight_dest',   F.lag('dest').over(window)) \
    .withColumn('is_cascade_delay',
        (F.col('prev_flight_arr_delay') > 15) &
        (F.col('late_aircraft_delay') > 0) &
        (F.col('prev_flight_dest') == F.col('origin'))
    ) \
    .withColumn('cascade_delay_min',
        F.when(F.col('is_cascade_delay'), F.col('late_aircraft_delay'))
         .otherwise(F.lit(0.0))
    ) \
    .withColumn('cascade_severity',
        F.when(F.col('cascade_delay_min') >= 60, 'Severe (>60 min)')
         .when(F.col('cascade_delay_min') >= 30, 'Moderate (30-60 min)')
         .when(F.col('cascade_delay_min') >= 15, 'Minor (15-30 min)')
         .otherwise('None')
    ) \
    .withColumn('cascade_id',
        F.md5(F.concat(
            F.coalesce(F.col('flight_id'),   F.lit('')),
            F.coalesce(F.col('tail_number'), F.lit('')),
            F.coalesce(F.col('flight_date').cast('string'), F.lit(''))
        ))
    ) \
    .withColumn('dbt_loaded_at', F.current_timestamp()) \
    .select(
        'cascade_id',
        'flight_id',
        'flight_date',
        'carrier',
        'tail_number',
        'origin',
        'dest',
        'flight_sequence',
        'scheduled_dep_time',
        'dep_delay',
        'arr_delay',
        'late_aircraft_delay',
        'prev_flight_arr_delay',
        'prev_flight_dest',
        'is_delayed',
        'is_cascade_delay',
        'cascade_delay_min',
        'cascade_severity',
        'season',
        'flight_year',
        'flight_month',
        'dbt_loaded_at'
    )

write_gold_table(fact_delay_cascade, 'fact_delay_cascade')

# COMMAND ----------

## mart_carrier_performance

mart_carrier_performance = silver_flights \
    .groupBy('carrier', 'flight_year', 'flight_month', 'flight_quarter', 'season') \
    .agg(
        F.count('*')                                                        .alias('total_flights'),
        F.sum(F.when(F.col('is_delayed') == True,    1).otherwise(0))      .alias('total_delayed'),
        F.sum(F.when(F.col('is_cancelled') == 1,     1).otherwise(0))      .alias('total_cancelled'),
        F.round(F.avg('dep_delay'),          2)                             .alias('avg_dep_delay_min'),
        F.round(F.avg('arr_delay'),          2)                             .alias('avg_arr_delay_min'),
        F.round(F.avg('carrier_delay'),      2)                             .alias('avg_carrier_delay_min'),
        F.round(F.avg('weather_delay'),      2)                             .alias('avg_weather_delay_min'),
        F.round(F.avg('nas_delay'),          2)                             .alias('avg_nas_delay_min'),
        F.round(F.avg('late_aircraft_delay'),2)                             .alias('avg_late_aircraft_delay_min'),
        F.round(
            F.sum(F.when(F.col('is_delayed') == True, 1).otherwise(0))
            * 100.0 / F.count('*'), 2
        )                                                                   .alias('delay_rate_pct'),
        F.round(
            F.sum(F.when(F.col('is_cancelled') == 1, 1).otherwise(0))
            * 100.0 / F.count('*'), 2
        )                                                                   .alias('cancellation_rate_pct')
    )

write_gold_table(mart_carrier_performance, 'mart_carrier_performance')

# COMMAND ----------

## mart_airport_operations

mart_airport_operations = silver_flights \
    .groupBy('origin', 'flight_year', 'flight_month') \
    .agg(
        F.count('*')                                                        .alias('total_departures'),
        F.sum(F.when(F.col('is_delayed') == True,   1).otherwise(0))       .alias('total_delayed'),
        F.sum(F.when(F.col('is_cancelled') == 1,    1).otherwise(0))       .alias('total_cancelled'),
        F.round(F.avg('dep_delay'), 2)                                      .alias('avg_dep_delay_min'),
        F.round(F.avg('arr_delay'), 2)                                      .alias('avg_arr_delay_min'),
        F.round(
            F.sum(F.when(F.col('is_delayed') == True, 1).otherwise(0))
            * 100.0 / F.count('*'), 2
        )                                                                   .alias('delay_rate_pct'),
        F.countDistinct('carrier')                                          .alias('num_carriers_served')
    ) \
    .withColumnRenamed('origin', 'airport_code')

write_gold_table(mart_airport_operations, 'mart_airport_operations')

# COMMAND ----------

## mart_delay_weather_correlation

flight_weather = silver_flights \
    .join(
        silver_weather,
        on=[
            silver_flights.origin == silver_weather.airport_code,
            F.to_date(silver_flights.flight_date) == silver_weather.weather_date
        ],
        how='left'
    ) \
    .select(
        silver_flights.flight_id,
        silver_flights.flight_date,
        silver_flights.origin,
        silver_flights.carrier,
        silver_flights.dep_delay,
        silver_flights.arr_delay,
        silver_flights.weather_delay,
        silver_flights.is_delayed,
        silver_flights.flight_year,
        silver_flights.flight_month,
        silver_weather.temperature_c,
        silver_weather.wind_speed_kmh,
        silver_weather.precipitation_mm,
        silver_weather.visibility_km,
        F.coalesce(silver_weather.weather_severity, F.lit('Unknown')).alias('weather_severity')
    )

mart_delay_weather_correlation = flight_weather \
    .groupBy('origin', 'flight_year', 'flight_month', 'weather_severity') \
    .agg(
        F.count('*')                                                        .alias('total_flights'),
        F.sum(F.when(F.col('is_delayed') == True, 1).otherwise(0))         .alias('weather_related_delays'),
        F.round(F.avg('dep_delay'),        2)                               .alias('avg_dep_delay_min'),
        F.round(F.avg('weather_delay'),    2)                               .alias('avg_weather_delay_min'),
        F.round(F.avg('temperature_c'),    2)                               .alias('avg_temperature_c'),
        F.round(F.avg('wind_speed_kmh'),   2)                               .alias('avg_wind_speed_kmh'),
        F.round(F.avg('precipitation_mm'), 2)                               .alias('avg_precipitation_mm'),
        F.round(F.avg('visibility_km'),    2)                               .alias('avg_visibility_km')
    )

write_gold_table(mart_delay_weather_correlation, 'mart_delay_weather_correlation')