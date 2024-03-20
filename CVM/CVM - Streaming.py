# Databricks notebook source
# MAGIC %md # Setup

# COMMAND ----------

# dbutils.widgets.text('cnpj', '14.196.832/0001-30')
# dbutils.widgets.text('min_vl', '10')
# dbutils.widgets.text('max_vl', '20')

# COMMAND ----------

cnpj = dbutils.widgets.get('cnpj')
min_vl = float(dbutils.widgets.get('min_vl'))
max_vl = float(dbutils.widgets.get('max_vl'))
dif = max_vl - min_vl

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS vr_cvm.ticker_stream')
dbutils.fs.rm('/tmp/vr_cvm.ticker_stream', recurse=True)

# COMMAND ----------

# MAGIC %md # Run stream

# COMMAND ----------

from pyspark.sql.functions import lit, round, rand, current_timestamp

ticker_stream = (
  spark.readStream.format('rate')
    .option('rowsPerSecond', 1)
    .load()
    .drop('value')
    .withColumn('CNPJ_FUNDO', lit(cnpj))
    .withColumn('DATAHORA', current_timestamp())
    .withColumn('VL_QUOTA', round(lit(min_vl)+rand()*lit(dif),2))   # 10-20
)

# COMMAND ----------

ticker_stream.writeStream.format('delta') \
  .option('checkpointLocation', '/tmp/vr_cvm.ticker_stream') \
  .toTable('vr_cvm.ticker_stream')
