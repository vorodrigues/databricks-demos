# Databricks notebook source
# MAGIC %md
# MAGIC ## Para executar este notebook, você precisará ter as seguintes permissões: 
# MAGIC - ### CREATE CATALOG
# MAGIC - ### CREATE SCHEMA 
# MAGIC - ### CREATE TABLE

# COMMAND ----------

# DBTITLE 1,Databricks SDK Installation
# MAGIC %pip install "databricks-sdk>=0.28.0"
# MAGIC

# COMMAND ----------

# DBTITLE 1,Restart Python Library
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Databricks SDK Workspace Monitoring
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot,MonitorCronSchedule
import time

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# COMMAND ----------

w = WorkspaceClient()

df = spark.read.table(f"{catalog}.information_schema.tables").filter("table_type = 'MANAGED' OR table_type = 'EXTERNAL'").select('table_schema','table_name')

for row in df.filter(df.table_schema == schema).collect():
  table_name = row.table_name
  try:
    w.quality_monitors.run_refresh(f"{catalog}.{schema}.{table_name}")
    print(f"Monitoramento iniciado com sucesso! Tabela : {table_name}")
  except Exception as e:
    print(f"Monitoramento não iniciado! Tabela : {table_name} -> Motivo:{str(e)}")
  time.sleep(15)
