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

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# COMMAND ----------

# Ingere incrementalmente arquivos JSON usando o Databricks Auto Loader e insere em uma tabela Delta
(spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", "s3://one-env/vr/crisp/schema")
     .option("cloudFiles.maxBytesPerTrigger", 1048576) # Apenas para demo / cadencia a execução
     .load("s3://one-env/vr/crisp/sales")
     .writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint/sales_bronze")
     .toTable("vr_demo.crisp.sales_bronze"))

# COMMAND ----------

# MAGIC %md ## Limpeza do dado

# COMMAND ----------

# Remove registros com problemas de qualidade de dados
df_clean = (spark.readStream.table("vr_demo.crisp.sales_bronze")
  .where("sales_id IS NOT NULL AND _rescued_data IS NULL")
  .drop("_rescued_data"))

# COMMAND ----------

# Insere os dados em uma tabela Delta
(df_clean.writeStream
  .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint/sales_silver") \
  .toTable("vr_demo.crisp.sales_silver"))

# COMMAND ----------

# MAGIC %md ## Enriquecimento

# COMMAND ----------

# Lê os dados estruturados de Produto e Loja
transactions = spark.readStream.table("vr_demo.crisp.sales_silver")
product = spark.table("vr_demo.crisp.dim_product")
store = spark.table("vr_demo.crisp.dim_store")

# Enriquece o feed de transações
df_enriched = (transactions.join(product, on='product_id', how='left')
                           .join(store, on='store_id', how='left'))

# Insere os dados em uma tabela Delta
(df_enriched.writeStream
  .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint/sales_gold")
  .toTable("vr_demo.crisp.sales_gold"))

# COMMAND ----------

# MAGIC %sql select count(*) from vr_demo.crisp.sales_gold
