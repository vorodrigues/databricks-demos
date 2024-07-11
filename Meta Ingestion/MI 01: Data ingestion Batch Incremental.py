# Databricks notebook source
# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/victor.rodrigues@databricks.com/metaingestion/batch"
schema_location = f"{project_dir}/schemas"
checkpoint_location = f"{project_dir}/checkpoints"

# COMMAND ----------

# MAGIC %sql create database if not exists vr_demo.mi_batch_bronze;
# MAGIC create database if not exists vr_demo.mi_batch_silver;
# MAGIC create database if not exists vr_demo.mi_batch;

# COMMAND ----------

dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

# MAGIC %md # Meta Ingestion 01: Data Ingestion
# MAGIC
# MAGIC ![Delta Lake base architecture](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Overview.png)

# COMMAND ----------

# MAGIC %md ## 1/ Define ingestion functions

# COMMAND ----------

# MAGIC %md ### Incremental

# COMMAND ----------

def incremental_batch(target):

  ts_key = target.ts_key
  merge_keys = target.merge_keys
  on_clause = target.on_clause
  transformations = target.transformations

  silverDF = (spark.read.table(f"{catalog}.{database}_bronze.{table}")
    .filter("current_date()-2 < ts_key and ts_key <= current_date()-1")
    .orderBy(ts_key, ascending=False)
    .dropDuplicates(merge_keys)
  )

  if transformations:
    silverDF = silverDF.selectExpr(transformations)

  silverDF.createOrReplaceTempView("batch")

  spark.sql(f"""
    MERGE INTO {catalog}.{database}_silver.{table} t
    USING batch s
    ON {on_clause}
    -- WHEN MATCHED AND i.op_code = 'd' THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

# MAGIC %md ### Overwrite

# COMMAND ----------

def overwrite_batch(target):

  transformations = target.transformations

  silverDF = spark.read.table(f"{catalog}.{database}_bronze.{table}")

  if transformations:
    silverDF = silverDF.selectExpr(transformations)

  silverDF.write.saveAsTable(f"{catalog}.{database}_silver.{table}")

# COMMAND ----------

# MAGIC %md ### Ingestion

# COMMAND ----------

from pyspark.sql.functions import *

def BatchIngestion(target):
  
  catalog = target.catalog
  database = target.database
  table = target.table
  path = target.path
  schema = target.schema
  strategy = target.strategy # 'overwrite' ou 'incremental'

  print(f'Ingesting table {catalog}.{database}.{table}')

  # Bronze Layer

  # TBD
  # - read from JDBC (pending: connectivity to SQL Server)
  # - add column `query` to control table

  bronzeDF = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("cloudFiles.schemaLocation", f"{schema_location}/{catalog}/{database}/{table}")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", True)
    .load(path)
  )

  # TBD
  # - add columns `op` = 'i' and `ts_ms` = current_timestamp()

  (bronzeDF.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_location}/{catalog}/{database}_bronze/{table}")
    .trigger(availableNow=True)
    .table(f"{catalog}.{database}_bronze.{table}")
    .awaitTermination()
  )

  # Silver Layer

  if strategy == 'overwrite':
    overwrite_batch(target)
  elif strategy == 'incremental':
    incremental_batch(target)
  else:
    print('Unknown strategy')

# COMMAND ----------

# MAGIC %md ## 2/ Run ingestion

# COMMAND ----------

tables = spark.sql('select * from vr_demo.kafka.control').collect()
display(tables)

# COMMAND ----------

for table in tables:
  BatchIngestion(table)

# COMMAND ----------

# MAGIC %md ## 3/ Visualize data

# COMMAND ----------

display(spark.table('vr_demo.kafka_bronze.table1'))

# COMMAND ----------

display(spark.table('vr_demo.kafka_silver.table1'))

# COMMAND ----------

# MAGIC %sql select 'bronze' as layer, count(*) as cnt from vr_demo.kafka_bronze.table1
# MAGIC union select 'silver' as layer, count(*) as cnt from vr_demo.kafka_silver.table1
