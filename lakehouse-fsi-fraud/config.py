# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2Fconfig&demo_name=lakehouse-fsi-fraud&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-fsi-fraud%2Fconfig&version=1">

# COMMAND ----------

#Note: we do not recommend to change the catalog here as it won't impact all the demo resources such as DLT pipeline and Dashboards.
#Instead, please re-install the demo with a specific catalog and schema using dbdemos.install("...", catalog="..", schema="...")

catalog = "vr_demo"
schema = dbName = db = "fraud"

volume_name = "fraud_raw_data"
