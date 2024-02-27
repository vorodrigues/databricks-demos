# Databricks notebook source
# MAGIC %md # Torre de Controle Comercial

# COMMAND ----------

import dlt

# COMMAND ----------

# MAGIC %md ## Ingestão do dado

# COMMAND ----------

# Ingere incrementalmente arquivos JSON usando o Databricks Auto Loader
@dlt.table(comment="Dados de transações de vendas crus ingeridos incrementalmente a partir do storage da landing zone")
def sales_bronze():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://one-env/vr/crisp/sales"))

# COMMAND ----------

# MAGIC %md ## Limpeza do dado

# COMMAND ----------

# Remove registros com problemas de qualidade de dados e deduplica os registros e mantém o mais recente
@dlt.table(comment="Dados de transações de vendas limpos")
@dlt.expect_or_drop("Chave primária válida", "sales_id IS NOT NULL")
@dlt.expect_or_drop("Schema válido", "_rescued_data IS NULL")
def sales_silver():
  return dlt.read("sales_bronze").orderBy("date_key", ascending=False).dropDuplicates(["sales_id"])

# COMMAND ----------

# MAGIC %md ## Visão de negócio

# COMMAND ----------

# Enriquece as transações com os dados estruturados de produto e loja
@dlt.table(comment="Dados de transações de vendas enriquecido")
def sales_gold():
  transactions = dlt.read("sales_silver")
  product = spark.table("vr_demo.crisp.dim_product")
  store = spark.table("vr_demo.crisp.dim_store")
  return transactions.join(product, on='product_id', how='left').join(store, on='store_id', how='left')
