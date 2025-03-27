# Databricks notebook source
# MAGIC %pip install dbdemos 

# COMMAND ----------

import dbdemos

dbdemos.install(
  demo_name='lakehouse-iot-platform',
  path='./', 
  catalog='vr_demo', 
  schema='iot',
  overwrite=True
)
