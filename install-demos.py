# Databricks notebook source
# MAGIC %pip install dbdemos 

# COMMAND ----------

import dbdemos

dbdemos.install(
  demo_name='lakehouse-fsi-fraud',
  path='./', 
  catalog='vr_demo', 
  schema='fraud',
  overwrite=True
)
