# Databricks notebook source
# MAGIC %md ##### Crie um exemplo de ingestão com Databricks Autoloader

# COMMAND ----------

# MAGIC %md ##### Use o Databricks Auto Loader para ler um JSON a partir do S3 e inserir os dados na tabela vr_demo.crisp.sales_bronze

# COMMAND ----------

# MAGIC %sql drop table vr_demo.crisp.sales_bronze

# COMMAND ----------

dbutils.fs.rm("s3://one-env/vr/crisp/checkpoint", True)

# COMMAND ----------

dbutils.fs.rm("s3://one-env/vr/crisp/schema", True)

# COMMAND ----------

# Read JSON using Auto Loader and load into Delta table
(spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", "s3://one-env/vr/crisp/schema")
     .load("s3://one-env/vr/crisp/sales")
     .writeStream
     .format("delta")
     .outputMode("append")
     .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint")
     .toTable("vr_demo.crisp.sales_bronze"))

# COMMAND ----------

# MAGIC %sql select count(*) from vr_demo.crisp.sales_bronze
