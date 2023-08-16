# Databricks notebook source
# MAGIC %md # Multiple Streaming Joins

# COMMAND ----------

# MAGIC %md ## Setup environment

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %sql USE vr_fraud_dev

# COMMAND ----------

# MAGIC %fs rm /tmp/vr_test_streaming --true

# COMMAND ----------

# MAGIC %md ## Setup source streams

# COMMAND ----------

visits_df = spark.readStream.table('visits_silver') \
  .withColumn('ts', current_timestamp()) \
  .withWatermark('ts', '30 seconds')

customers_df = spark.readStream.table('customers_silver') \
  .withColumn('ts', current_timestamp()) \
  .withWatermark('ts', '30 seconds')
  
locations_df = spark.readStream.table('locations_silver') \
  .withColumn('ts', current_timestamp()) \
  .withWatermark('ts', '30 seconds')

# COMMAND ----------

# MAGIC %md ## Start streaming join

# COMMAND ----------

visits_df \
  .join(locations_df, on='atm_id', how='inner') \
  .join(customers_df, on='customer_id', how='inner') \
  .withColumn('state', col('city_state_zip.state')) \
  .withColumn('city', col('city_state_zip.city')) \
  .withColumn('zip', col('city_state_zip.zip')) \
  .drop('city_state_zip') \
  .writeStream.format('noop') \
  .option('checkpointLocation', '/tmp/vr_test_streaming') \
  .start()
