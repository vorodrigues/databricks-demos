# Databricks notebook source
# dbutils.fs.mkdirs("/Volumes/vr_demo/fraud/fraud_raw_data/transactions_bkp")

# COMMAND ----------

# dbutils.fs.cp("dbfs:/Volumes/vr_demo/fraud/fraud_raw_data/transactions", "/Volumes/vr_demo/fraud/fraud_raw_data/transactions_bkp", True)

# COMMAND ----------

dbutils.fs.rm("/Volumes/vr_demo/fraud/fraud_raw_data/transactions/new", True)

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/vr_demo/fraud/fraud_raw_data/transactions/new")

# COMMAND ----------

from time import sleep

for f in dbutils.fs.ls("/Volumes/vr_demo/fraud/fraud_raw_data/transactions_bkp"):
  if f.path.endswith(".json"):
    dbutils.fs.cp(f.path, f.path.replace("transactions_bkp", "transactions/new"))
    sleep(10)
