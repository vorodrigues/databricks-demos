# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Default notebook
# MAGIC
# MAGIC This default notebook is executed using Databricks Workflows as defined in resources/databricks_data_ops_job.yml.

# COMMAND ----------

from databricks_data_ops import main

main.get_taxis().show(10)
