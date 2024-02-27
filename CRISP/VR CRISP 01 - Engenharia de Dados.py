# Databricks notebook source
# MAGIC %sql drop table if exists vr_demo.crisp.sales_bronze;
# MAGIC drop table if exists vr_demo.crisp.sales_silver;
# MAGIC drop table if exists vr_demo.crisp.sales_gold;

# COMMAND ----------

dbutils.fs.rm("s3://one-env/vr/crisp/checkpoint", True)

# COMMAND ----------

dbutils.fs.rm("s3://one-env/vr/crisp/schema", True)

# COMMAND ----------

# MAGIC %md # Torre de Controle Comercial

# COMMAND ----------

# MAGIC %md ## Ingestão do dado

# COMMAND ----------

# Ingere incrementalmente arquivos JSON usando o Databricks Auto Loader e insere em uma tabela Delta
(spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", "s3://one-env/vr/crisp/schema")
     .load("s3://one-env/vr/crisp/sales")
     .writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint")
     .trigger(availableNow=True)
     .toTable("vr_demo.crisp.sales_bronze"))

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

# MAGIC %md ## Visão de negócio

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
