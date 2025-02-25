# Databricks notebook source
# MAGIC %pip install dbdemos 

# COMMAND ----------

import dbdemos

dbdemos.install(
  demo_name='llm-fine-tuning',
  path='./', 
  catalog='vr_demo', 
  schema='fine_tuning',
  overwrite=True
)
