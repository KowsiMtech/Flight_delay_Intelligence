# Databricks notebook source
import sys

sys.path.append("/Workspace/Users/dhinu.kowsi@gmail.com/New Pipeline 2026-04-24 09:31")

# COMMAND ----------

# !!! Before performing any data analysis, make sure to run the pipeline to materialize the sample datasets. The tables referenced in this notebook depend on that step.

display(spark.sql("SELECT * FROM databricks_learning_ws.default.sample_aggregation_apr_24_931"))