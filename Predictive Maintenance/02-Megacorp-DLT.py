# Databricks notebook source
# MAGIC %md 
# MAGIC # Introducing Delta Live Table
# MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC 
# MAGIC <div><img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-pipeline.png"/></div>
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_ingestion_sql&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Delta Live Table example in SQL. BRONZE/SILVER/GOLD. Expectations to track data quality. Load model from MLFLow registry and call it to apply customer segmentation as last step.<br/><i>Usage: basic DLT demo / Lakehouse presentation.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"DLT": ["DLT customer SQL"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: ingest data stream

# COMMAND ----------

# DBTITLE 1,Ingest raw Turbine stream data in incremental mode
import dlt
@dlt.table(comment="Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution")
@dlt.expect("correct_schema","_rescued_data IS NULL")
def turbine_bronze_dlt():
  return (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("delta.autoOptimize.optimizeWrite", "true")
      .option("delta.autoOptimize.autoCompact", "true")
      .load("/mnt/quentin-demo-resources/turbine/incoming-data-json")
  )

# COMMAND ----------

# MAGIC %md ## 2/ Silver layer: cleanup data

# COMMAND ----------

# DBTITLE 1,Clean and drop unnecessary columns 
from pyspark.sql.functions import *

@dlt.table(comment="User data cleaned for analysis")
@dlt.expect("valid_id","id IS NOT NULL AND id > 0")
def turbine_silver_dlt():
  return (
    dlt.read_stream('turbine_bronze_dlt') \
                .drop('TORQUE') \
                .where('"ID" IS NOT NULL AND _rescued_data IS NULL')           
  )

# COMMAND ----------

# MAGIC %md ## 3/ Gold layer: join batch data

# COMMAND ----------

# DBTITLE 1,Ingest user spending score
@dlt.table(comment="Batch user data coming from parquet files")
def turbine_status_dlt():
  return (
    spark.read.format("parquet")
      .load("/mnt/quentin-demo-resources/turbine/status")
  )

# COMMAND ----------

# DBTITLE 1,Join both data to create our final table
@dlt.table(comment="Final user table with all information for Analysis / ML")
dlt.expect("valid_id","id IS NOT NULL AND id > 0")
def turbine_gold_dlt_v3():
  return dlt.read_stream("turbine_status_dlt").join(dlt.read("turbine_silver_dlt"), ["id"], "left")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Pipeline
# MAGIC 
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485&owned-by-me=false&name-order=ascend&name=megacorp#joblist/pipelines/e45e7d67-4f8e-4ba2-9f64-ebf387d58bd6
