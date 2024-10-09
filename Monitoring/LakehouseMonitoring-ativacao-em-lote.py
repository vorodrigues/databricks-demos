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

# DBTITLE 1,Create Lakehouse Monitoring
def set_monitoring(catalog,schema,user_email,output_catalog,cron,flg):
  df = spark.read.table(f"{catalog}.information_schema.tables").filter("table_type = 'MANAGED' OR table_type = 'EXTERNAL'").select('table_schema','table_name')
  for row in df.filter(df.table_schema == schema).collect():
    table_name = row.table_name
    w = WorkspaceClient()
    if flg == 'S':
      try:
        w.quality_monitors.delete(table_name=f"{catalog}.{schema}.{table_name}")
        print(f"Monitoramento excluido com sucesso! Tabela : {table_name}")
      except Exception as e:
        print(f"Monitoramento não excluido! Tabela : {table_name} -> Motivo:{str(e)}")
    try:
      w.quality_monitors.create(
        table_name=f"{catalog}.{schema}.{table_name}",
        assets_dir=f"/Workspace/Users/{user_email}/databricks_lakehouse_monitoring/{catalog}.{schema}.{table_name}",
        output_schema_name=f"{output_catalog}.{schema}",
        snapshot=MonitorSnapshot(),
        schedule=MonitorCronSchedule(
          quartz_cron_expression=cron, # todos os dias 9:30, para alterar a frequencia altera a cron_expression
          timezone_id="America/Sao_Paulo")
      )
      print(f"Monitoramento criado com sucesso! Tabela : {row.table_name}")
      time.sleep(15)
    except Exception as e:
      print(f"Motironado não criado! Tabela : {table_name} -> Motivo: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Creating Monitoring System
def create_monitoring():
    print("Vamos definir os parâmetros para criação do monitoramento")

    catalog = input("Digite o nome do catálogo onde estão suas tabelas que serão monitoradas")
    schema = input("Digite o nome do schema onde estão suas tabelas")
    user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    flg = input("Você deseja deletar os monitoramentos existentes antes da criação dos novos? (S/N)")
    print("Como uma boa prática, vamos criar um catálogo exclusivo para salvar os dados do monitoramento")
    time.sleep(3)
    print("Dentro deste catálogo, teremos os schemas com o mesmo nome do schema da tabela a ser monitorada")
    time.sleep(3)
    output_catalog=input("Digite o nome do catálogo onde deseja salvar os dados do monitoramento (se não existir, ele será criado)")
   
    print("Agora, vamos criar o agendamento do monitoramento")
    day_of_month = input("Caso a execução seja mensal, digite o dia do mês(1-31) caso contrário digite *): ")
    day_of_week = input("Para semanal, digite o dia da semana (0-6, 0=Domingo, * para diário): ")
    hour = input("Hora da execução (0-23)): ")
    minute = input("Minuto de início da execução (0-59): ")
    month = '*'
    cron_expression = f"{minute} {hour} {day_of_month} {month} {day_of_week} ?"
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {output_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_catalog}.{schema}")
    set_monitoring(catalog,schema,user_email,output_catalog,cron_expression,flg)


# COMMAND ----------

# DBTITLE 1,Create Monitor in Monitoring PRD
create_monitoring()
