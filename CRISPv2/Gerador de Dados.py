# Databricks notebook source
(spark.table('vr_demo.crisp.sales_bronze').limit(20000000)   # 20M
  .write.option("maxRecordsPerFile", 1000000)                # 1M
  .json('s3://one-env/vr/crisp/sales/new', mode='append'))

# COMMAND ----------

display(dbutils.fs.ls('s3://one-env/vr/crisp/sales/new'))

# COMMAND ----------

dbutils.fs.rm('s3://one-env/vr/crisp/sales/new', True)
