# Databricks notebook source
# MAGIC %sql
# MAGIC select distinct carrier in flight_catalog.silver.flights

# COMMAND ----------

# Row Counts
print("GOLD TABLE VERIFICATION")
print("=" * 60)

gold_tables = [
    'dim_airline',
    'dim_airport',
    'fact_flight_delays',
    'fact_delay_cascade',
    'mart_carrier_performance',
    'mart_airport_operations',
    'mart_delay_weather_correlation'
]

for table in gold_tables:
    try:
        count = spark.table(f'flight_catalog.gold_gold.{table}').count()
        status = '' if count > 0 else ' Warning: EMPTY'
        print(f'{status} {table:<45} {count:>6} rows')
    except Exception as e:
        print(f' {table:<45} ERROR: {e}')

# COMMAND ----------

# Check each table has actual data
display(spark.table('flight_catalog.gold_gold.dim_airline'))

# COMMAND ----------

# Key Business Logic Check 
# Check delay rates make sense
spark.sql("""
    SELECT 
        carrier,
        total_flights,
        total_delayed,
        delay_rate_pct,
        cancellation_rate_pct
    FROM flight_catalog.gold_gold.mart_carrier_performance
    ORDER BY delay_rate_pct DESC
""").show()

# COMMAND ----------

# Check ADLS Gold Parquet Files 
GOLD_PATH = 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/gold'

print("ADLS GOLD PARQUET FILES")
print("=" * 60)
for table in gold_tables:
    try:
        files = dbutils.fs.ls(f'{GOLD_PATH}/{table}/')
        parquet = [f for f in files if 'part-' in f.name]
        print(f' {table:<45} {len(parquet)} parquet files')
    except:
        print(f' {table:<45} NOT FOUND in ADLS')

# COMMAND ----------

# Data Quality Checks 
print("DATA QUALITY CHECKS")
print("=" * 60)
from pyspark.sql import functions as F
# Check no nulls in key columns
checks = [
    ('fact_flight_delays', 'flight_id'),
    ('fact_flight_delays', 'carrier'),
    ('fact_flight_delays', 'origin'),
    ('fact_delay_cascade', 'cascade_id'),
    ('dim_airline',        'airline_code'),
    ('dim_airport',        'airport_code'),
]

for table, col in checks:
    null_count = spark.table(f'flight_catalog.gold_gold.{table}') \
                      .filter(F.col(col).isNull()) \
                      .count()
    status = 'tables ok' if null_count == 0 else f'X {null_count} NULLs found!'
    print(f'{status} {table}.{col}')