# Databricks notebook source
# This notebook writes back the silver tables to ADLS

ADLS_SILVER = 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/silver'

# Export flights
spark.table('flight_catalog.silver.flights') \
    .write.format('delta') \
    .mode('overwrite') \
    .save(f'{ADLS_SILVER}/flights')

# Export weather
spark.table('flight_catalog.silver.weather') \
    .write.format('delta') \
    .mode('overwrite') \
    .save(f'{ADLS_SILVER}/weather')