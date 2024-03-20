# Databricks notebook source
import dlt

# COMMAND ----------

tbls = [
  {
    'name' : 'raw_txs',
    'comment' : 'New raw loan data incrementally ingested from cloud object storage landing zone',
    'path' : '/demos/dlt/loans/raw_transactions',
    'format' : 'json'
  },
  {
    'name' : 'raw_historical_loans',
    'comment' : 'Raw historical transactions',
    'path' : '/demos/dlt/loans/historical_loans',
    'format' : 'csv'
  }
]

# COMMAND ----------

for tbl in tbls:

  @dlt.table(
    name=tbl['name'],
    comment=tbl['comment']
  )
  def create_table():
    return (
      spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", tbl['format'])
        .option("cloudFiles.inferColumnTypes", "true")
        .load(tbl['path'])
    )
