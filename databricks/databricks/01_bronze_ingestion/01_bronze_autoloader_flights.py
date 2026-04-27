# Databricks notebook source
# MAGIC %run "/Flight_Analytics/00_setup/includes/config"

# COMMAND ----------

# Verifying landing zone has files
# flight-events, weather-events folders

display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

# Auto Loader: flights → bronze.flights
# Writes Delta files directly to ADLS bronze/ folder

from pyspark.sql import functions as F

FLIGHT_SOURCE     = f'{LANDING_PATH}/flight-events/'
FLIGHT_CHECKPOINT = f'{CHECKPOINT_BASE}/bronze_flights/'
FLIGHT_TABLE      = f'{CATALOG}.bronze.flights'

# ── NEW: ADLS path where Bronze Delta files will be stored ──────
FLIGHT_BRONZE_PATH = f'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/bronze/flights/'

df_flights = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option('cloudFiles.schemaLocation',
            f'{CHECKPOINT_BASE}/schema_flights/')
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
    .option('cloudFiles.inferColumnTypes', 'true')
    .load(FLIGHT_SOURCE)
    .withColumn('_ingested_at', F.current_timestamp())
    .withColumn('_source_file', F.col('_metadata.file_path'))
)

query = (
    df_flights.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', FLIGHT_CHECKPOINT)
    .option('mergeSchema', 'true')
    .trigger(availableNow=True)
    # ── NEW: write Delta files to ADLS path ─────────────────────
    .option('path', FLIGHT_BRONZE_PATH)
    # ── registers as external table in Unity Catalog ─────────────
    .toTable(FLIGHT_TABLE)
)

query.awaitTermination()
print(f'Done → {FLIGHT_TABLE}')
print(f'Data stored at → {FLIGHT_BRONZE_PATH}')
