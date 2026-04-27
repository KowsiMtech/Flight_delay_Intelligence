# Databricks notebook source
# Azure Storage Configuration 
storage_account = 'flightanalyticsadls'
container       = 'flight-data'
catalog_name    = 'flight_catalog'

# COMMAND ----------

# config notebook — no changes needed to paths
STORAGE_ACCOUNT = 'flightanalyticsadls'
CONTAINER       = 'flight-data'
CATALOG         = 'flight_catalog'

LANDING_PATH    = f'abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing'
BRONZE_PATH     = f'abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze'
SILVER_PATH     = f'abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver'
GOLD_PATH       = f'abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold'
CHECKPOINT_BASE = f'abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints'

# COMMAND ----------

# Landing path (Kafka → ADLS)
LANDING_FLIGHTS = f"abfss://{container}@{storage_account}.dfs.core.windows.net/landing/flight-events"

# Bronze output path
BRONZE_FLIGHTS = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/flights/"

# Checkpoint path
CHECKPOINT_FLIGHTS = f"abfss://{container}@{storage_account}.dfs.core.windows.net/checkpoints/flights/"

# COMMAND ----------

#  Unity Catalog Schema References 
bronze_schema = f'{catalog_name}.bronze'
silver_schema = f'{catalog_name}.silver'
gold_schema   = f'{catalog_name}.gold'


# COMMAND ----------

print(f'Landing: {LANDING_PATH}')
print(f'Bronze:  {BRONZE_PATH}')