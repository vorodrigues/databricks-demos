# Databricks notebook source
# MAGIC %md # Torre de Controle Comercial

# COMMAND ----------

if dbutils.widgets.get('reset_data') == 'true':
  spark.sql('DROP TABLE IF EXISTS vr_demo.crisp.sales_bronze')
  spark.sql('DROP TABLE IF EXISTS vr_demo.crisp.sales_silver')
  spark.sql('DROP TABLE IF EXISTS vr_demo.crisp.sales_gold')
  dbutils.fs.rm("s3://one-env/vr/crisp/checkpoint", True)

# COMMAND ----------

# MAGIC %md ## Ingestão do dado

# COMMAND ----------

display(dbutils.fs.ls('s3://one-env/vr/crisp/sales'))

# COMMAND ----------

# Ingere incrementalmente arquivos JSON usando o Databricks Auto Loader e insere em uma tabela Delta
(spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", "s3://one-env/vr/crisp/schema")
     .load("s3://one-env/vr/crisp/sales")
     .writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint/batch")
     .trigger(availableNow=True)
     .toTable("vr_demo.crisp.sales_bronze")
     .awaitTermination())

# COMMAND ----------

# MAGIC %md ## Limpeza do dado

# COMMAND ----------

# Remove registros com problemas de qualidade de dados
df_clean = (spark.table("vr_demo.crisp.sales_bronze")
  .where("sales_id IS NOT NULL AND _rescued_data IS NULL")
  .drop("_rescued_data"))

# COMMAND ----------

# Deduplica os registros e mantém o mais recente
df_dedup = df_clean.orderBy("date_key", ascending=False).dropDuplicates(["sales_id"])

# COMMAND ----------

# Insere os dados em uma tabela Delta
df_dedup.write.saveAsTable("vr_demo.crisp.sales_silver")

# COMMAND ----------

# MAGIC %md ## Enriquecimento

# COMMAND ----------

# Lê os dados estruturados de Produto e Loja
transactions = spark.table("vr_demo.crisp.sales_silver")
product = spark.table("vr_demo.crisp.dim_product")
store = spark.table("vr_demo.crisp.dim_store")

# Enriquece o feed de transações
df_enriched = (transactions.join(product, on='product_id', how='left')
                           .join(store, on='store_id', how='left'))

# Insere os dados em uma tabela Delta
df_enriched.write.saveAsTable("vr_demo.crisp.sales_gold")

# COMMAND ----------

# MAGIC %md ## Versionamento de dados

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY vr_demo.crisp.sales_bronze

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM vr_demo.crisp.sales_bronze

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM vr_demo.crisp.sales_bronze VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM vr_demo.crisp.sales_bronze TIMESTAMP AS OF '2024-07-13T03:15:22.000+00:00'

# COMMAND ----------

# MAGIC %sql RESTORE TABLE vr_demo.crisp.sales_bronze VERSION AS OF 1

# COMMAND ----------

# MAGIC %md ## Auditoria

# COMMAND ----------

# MAGIC %sql select * from system.access.audit where user_identity.email = 'victor.rodrigues@databricks.com' and event_date = current_date() and service_name IN ('notebook', 'databrickssql')

# COMMAND ----------

# MAGIC %md ## Otimização

# COMMAND ----------

# MAGIC %sql -- Habilita o Databricks Predictive Optimization no schema vr_demo.crisp
# MAGIC ALTER SCHEMA vr_demo.crisp ENABLE PREDICTIVE OPTIMIZATION

# COMMAND ----------

# MAGIC %md ## Permissionamento
# MAGIC Escreva uma query para conceder permissões de leitura ao grupo analysts na tabela vr_demo.crisp.sales_gold

# COMMAND ----------

# MAGIC %sql -- Concede permissão de leitura ao grupo "analysts" à tabela "vr_demo.crisp.sales_gold"
# MAGIC GRANT SELECT ON TABLE vr_demo.crisp.sales_gold TO `analysts`;
