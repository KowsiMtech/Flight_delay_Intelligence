-- Databricks notebook source
CREATE EXTERNAL LOCATION adls_flight_location
URL 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL adls_flight_credential);

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION adls_flight_location;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_flight_bronze
URL 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/bronze'
WITH (STORAGE CREDENTIAL adls_flight_credential);

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_flight_silver
URL 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/silver'
WITH (STORAGE CREDENTIAL adls_flight_credential);

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_flight_gold
URL 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/gold'
WITH (STORAGE CREDENTIAL adls_flight_credential);

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_flight_landing
URL 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/landing'
WITH (STORAGE CREDENTIAL adls_flight_credential);

-- COMMAND ----------

-- Create schemas
USE CATALOG flight_catalog;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;

SHOW SCHEMAS IN flight_catalog;

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;