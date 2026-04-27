# Databricks notebook source
# MAGIC %run "/Flight_Analytics/00_setup/includes/config"

# COMMAND ----------

# Verifying landing zone has files
# flight-events, weather-events folders

display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

# Auto Loader: weather → bronze.weather
# Writes Delta files directly to ADLS bronze/weather/ folder

from pyspark.sql import functions as F

WEATHER_SOURCE      = f'{LANDING_PATH}/weather-events/'
WEATHER_CHECKPOINT  = f'{CHECKPOINT_BASE}/bronze_weather/'
WEATHER_TABLE       = f'{CATALOG}.bronze.weather'

# ── NEW: ADLS path where Bronze Delta files will be stored ──────
WEATHER_BRONZE_PATH = 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/bronze/weather/'

df_weather = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option('cloudFiles.schemaLocation',
            f'{CHECKPOINT_BASE}/schema_weather/')
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
    .option('cloudFiles.inferColumnTypes', 'true')
    .load(WEATHER_SOURCE)
    .withColumn('_ingested_at', F.current_timestamp())
    .withColumn('_source_file', F.col('_metadata.file_path'))
)

query2 = (
    df_weather.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', WEATHER_CHECKPOINT)
    .option('mergeSchema', 'true')
    .trigger(availableNow=True)
    # ── NEW: write Delta files to ADLS path ─────────────────────
    .option('path', WEATHER_BRONZE_PATH)
    .toTable(WEATHER_TABLE)
)

query2.awaitTermination()
print(f'Done → {WEATHER_TABLE}')
print(f'Data stored at → {WEATHER_BRONZE_PATH}')
