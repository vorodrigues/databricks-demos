# Databricks notebook source
# MAGIC %pip install dbdemos 

# COMMAND ----------

import dbdemos

dbdemos.install(
  demo_name='llm-rag-chatbot',
  path='./', 
  catalog='vr_demo', 
  schema='chatbot4',
  overwrite=True
)
