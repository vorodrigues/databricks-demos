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
# cron = f"{minute} {hour} {day_of_month} {month} {day_of_week} ?"
cron = '0 0 23 ? * Sun'

# COMMAND ----------

w = WorkspaceClient()

df = spark.read.table(f"{catalog}.information_schema.tables").filter("table_type = 'MANAGED' OR table_type = 'EXTERNAL'").select('table_schema','table_name')

for row in df.filter(df.table_schema == schema).collect():
  table_name = row.table_name
  try:
    w.quality_monitors.update(
      table_name=f"{catalog}.{schema}.{table_name}",
      schedule=MonitorCronSchedule(
          quartz_cron_expression=cron, # todos os dias 9:30, para alterar a frequencia altera a cron_expression
          timezone_id="America/Sao_Paulo")
    )
    print(f"Monitoramento atualizado com sucesso! Tabela : {table_name}")
  except Exception as e:
    print(f"Monitoramento não atualizado! Tabela : {table_name} -> Motivo:{str(e)}")
  time.sleep(15)
