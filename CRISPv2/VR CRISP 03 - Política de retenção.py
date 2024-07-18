# Databricks notebook source
# MAGIC %md # Preparando tabela de Políticas de Retenção

# COMMAND ----------

# MAGIC %sql
# MAGIC create table vr_demo.crisp.retention_policies (catalog string, schema string, table string, ts string, policy string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into vr_demo.crisp.retention_policies values
# MAGIC   ('vr_demo', 'crisp', 'sales_bronze', 'date_key', '10 years'),
# MAGIC   ('vr_demo', 'crisp', 'sales_silver', 'date_key', '10 years'),
# MAGIC   ('vr_demo', 'crisp', 'sales_gold', 'date_key', '10 years')

# COMMAND ----------

# MAGIC %md # Aplica Políticas de Retenção

# COMMAND ----------

tables = spark.sql('select * from vr_demo.crisp.retention_policies').collect()
display(tables)

# COMMAND ----------

for table in tables:
  print(f'Aplicando política de retenção a {table.catalog}.{table.schema}.{table.table}')
  r = spark.sql(f'delete from {table.catalog}.{table.schema}.{table.table} where {table.ts} < current_date() - interval {table.policy}').collect()
  print(f'Linhas excluídas: {r[0].num_affected_rows}')
