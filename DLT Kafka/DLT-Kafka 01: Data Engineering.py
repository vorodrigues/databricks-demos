# Databricks notebook source
# You can connect to Kafka over either SSL/TLS encrypted connection, or with an unencrypted plaintext connection.
# Just choose the set of corresponding endpoints to use.
# If you chose the tls servers, you must enable SSL in the Kafka connection, see later for an example.
kafka_bootstrap_servers_tls = dbutils.secrets.get("oetrta", "oetrta-kafka-servers-tls")
#kafka_bootstrap_servers_plaintext = dbutils.secrets.get("oetrta", "oetrta-kafka-servers-plain")
# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")
# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/kafka_demo"
checkpoint_location = f"{project_dir}/checkpoints/kafka"
topic = f"{user}_kafka_demo"

# COMMAND ----------

# MAGIC %sql USE DATABASE vr_kafka_dev

# COMMAND ----------

# MAGIC %md # DLT-Kafka 01: Data Engineering
# MAGIC 
# MAGIC ![Delta Lake base architecture](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Overview.png)

# COMMAND ----------

# MAGIC %md ## 1/ Bronze layer: ingest data

# COMMAND ----------

# DBTITLE 1,Stream events from Kafka
from pyspark.sql.functions import col,from_json

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

display(bronzeDF)

# COMMAND ----------

# DBTITLE 1,Write events to Delta table
bronzeDF.writeStream.format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", f"{project_dir}/checkpoints/bronze") \
  .table("events_bronze")

# COMMAND ----------

# DBTITLE 1,Query bronze table
# MAGIC %sql SELECT json.action, count(1) FROM events_bronze GROUP BY json.action

# COMMAND ----------

# MAGIC %md ## 2/ Silver layer: standardize data

# COMMAND ----------

# DBTITLE 1,Stream events from bronze table
silverDF = spark.readStream.format("delta") \
  .table("events_bronze") \
  .select("eventId", "json.*")

display(silverDF)

# COMMAND ----------

# DBTITLE 1,Write events to silver table
silverDF.writeStream.format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", f"{project_dir}/checkpoints/silver") \
  .table("events_silver")

# COMMAND ----------

# DBTITLE 1,Query silver table
# MAGIC %sql
# MAGIC SELECT action, count(1) FROM events_silver GROUP BY action
