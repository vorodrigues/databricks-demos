# Databricks notebook source
# MAGIC %pip install dbdemos 

# COMMAND ----------

import dbdemos

dbdemos.install(
  demo_name='feature-store',
  path='./', 
  catalog='vr_demo', 
  schema='feature_store',
  overwrite=True
)
