-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS flight_catalog
MANAGED LOCATION 'abfss://flight-data@flightanalyticsadls.dfs.core.windows.net/unity-catalog/';

-- COMMAND ----------

-- Verify
SHOW CATALOGS;

-- COMMAND ----------

USE CATALOG fifa_catalog

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS flight_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS flight_catalog.silver;
CREATE SCHEMA IF NOT EXISTS flight_catalog.gold;
CREATE SCHEMA IF NOT EXISTS flight_catalog.quarantine;

-- COMMAND ----------

