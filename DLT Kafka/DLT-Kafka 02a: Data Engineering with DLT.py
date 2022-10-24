# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Simplify ETL with Delta Live Table
# MAGIC 
# MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Pythin, and DLT will handle the Data Engineering complexity for you.
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-1.png" width="700"/>
# MAGIC 
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC 
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC 
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC 
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC 
# MAGIC ## Our Delta Live Table pipeline
# MAGIC 
# MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
# MAGIC 
# MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_sql&dt=DLT">
# MAGIC <!-- [metadata={"description":"Full DLT demo, going into details. Use loan dataset",
# MAGIC  "authors":["dillon.bostwick@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "dlt"]}}] -->

# COMMAND ----------

# DBTITLE 1,Define parameters
kafka_bootstrap_servers_tls = dbutils.secrets.get("oetrta", "oetrta-kafka-servers-tls")
topic = "victor_rodrigues_kafka_demo"

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Bronze layer: ingestion incremental data leveraging Databricks Autoloader
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-2.png" width="500"/>
# MAGIC 
# MAGIC Our raw data is being sent to a blob storage. 
# MAGIC 
# MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
# MAGIC 
# MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
# MAGIC 
# MAGIC 
# MAGIC #### INCREMENTAL LIVE TABLE 
# MAGIC Defining tables as `INCREMENTAL` will garantee that you only consume new incoming data. Without incremental, you will scan and ingest all the data available at once. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) for more details

# COMMAND ----------

# DBTITLE 1,Stream events from Kafka to bronze table
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
  comment="Raw table with all OPEN and CLOSE events",
  table_properties={
    "quality": "bronze"
  }
)
def events_bronze():
  
  input_schema = spark.read.json("/databricks-datasets/structured-streaming/events").schema

  rawDF = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls) 
    .option("kafka.security.protocol", "SSL") 
    .option("subscribe", topic)
    .load()
  )

  bronzeDF = rawDF.select(
    col("key").cast("string").alias("eventId"), 
    from_json(col("value").cast("string"), input_schema).alias("json")
  )
  
  return bronzeDF

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Silver layer: exploding data while ensuring data quality
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-3.png" width="500"/>
# MAGIC 
# MAGIC Once the bronze layer is defined, we'll create the sliver layers by exploding data. Note that bronze tables are referenced using the `dlt` module. 
# MAGIC 
# MAGIC To consume only increment from the Bronze layer, we'll be using the `read_stream` method: `dlt.read_stream("events_bronze")`
# MAGIC 
# MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
# MAGIC 
# MAGIC #### Expectations
# MAGIC By defining expectations with `@dlt.expect`, you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

# COMMAND ----------

# DBTITLE 1,Stream events from bronze to silver table
@dlt.table(
  comment="Standardized table with exploded events",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect("Valid timestamp", "time > 1469510000")
@dlt.expect("Valid action", "action IN ('Open', 'Close')")
def events_silver():

  silverDF = dlt.read_stream("events_bronze") \
    .select("eventId", "json.*")

  return silverDF

# COMMAND ----------

# MAGIC %md ## Next steps
# MAGIC 
# MAGIC Your DLT pipeline is ready to be started.
# MAGIC 
# MAGIC To generate sample data, please run the [companion notebook]($./DLT-Kafka 00: Generate Data).
# MAGIC 
# MAGIC Open the [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/a949e8a4-a900-467e-92b2-ce4b529ff345/updates/16542208-9be7-40f5-a156-f00b3563aab8) and run it.

# COMMAND ----------

# MAGIC %md ## Tracking data quality
# MAGIC 
# MAGIC Expectations stats are automatically available as system table.
# MAGIC 
# MAGIC This information let you monitor your data ingestion quality. 
# MAGIC 
# MAGIC You can leverage DBSQL to request these table and build custom alerts based on the metrics your business is tracking.
# MAGIC 
# MAGIC 
# MAGIC See [how to access your DLT metrics]($./DLT-Kafka 02b: Analyze)
# MAGIC <br><br>
# MAGIC 
# MAGIC <img width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC <br><br>
# MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard example</a>
