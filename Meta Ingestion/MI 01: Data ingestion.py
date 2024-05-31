# Databricks notebook source
# You can connect to Kafka over either SSL/TLS encrypted connection, or with an unencrypted plaintext connection.
# Just choose the set of corresponding endpoints to use.
# If you chose the tls servers, you must enable SSL in the Kafka connection, see later for an example.
kafka_bootstrap_servers_tls = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-tls")
#kafka_bootstrap_servers_plaintext = dbutils.secrets.get("oetrta", "oetrta-kafka-servers-plain")
# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")
# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/kafka_demo"
checkpoint_location = f"{project_dir}/checkpoints/kafka"

# COMMAND ----------

# MAGIC %sql create database if not exists vr_demo.kafka_bronze;
# MAGIC create database if not exists vr_demo.kafka_silver;
# MAGIC create database if not exists vr_demo.kafka;

# COMMAND ----------

dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

# MAGIC %md # Meta Ingestion 01: Data Ingestion
# MAGIC
# MAGIC ![Delta Lake base architecture](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Overview.png)

# COMMAND ----------

# MAGIC %md ## 1/ Define ingestion functions

# COMMAND ----------

def merge_delta(microbatch, target):

  t = f'{target.catalog}.{target.database}_silver.{target.table}'
  merge_keys = target.merge_keys.split(',')
  on_clause = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])
  ts_key = target.ts_key

  # Deduplica registros dentro do microbatch e mantém somente o mais recente
  microbatch.orderBy(ts_key, ascending=False).dropDuplicates(merge_keys).createOrReplaceTempView("microbatch")
  
  try:
    # Caso a tabela já exista, os dados serão atualizados com MERGE
    microbatch._jdf.sparkSession().sql(f"""
      MERGE INTO {t} t
      USING microbatch s
      ON {on_clause}
      -- WHEN MATCHED AND i.op_code = 'd' THEN DELETE
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  except:
    # Caso a tabela ainda não exista, esta será criada
    microbatch.writeTo(t).createOrReplace()

# COMMAND ----------

from pyspark.sql.functions import *

def KafkaIngestion(target):
  
  catalog = target.catalog
  database = target.database
  table = target.table
  topic = target.topic
  schema = target.schema

  print(f'Ingesting table {catalog}.{database}.{table}')

  # Bronze Layer

  rawDF = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
    .option("kafka.security.protocol", "SSL")
    .option("startingOffsets", "earliest")
    .option("subscribe", topic)
    .load()
  )

  bronzeDF = rawDF.select(
      col("key").cast("string").alias("key"),
      col("value").cast("string").alias("value")
  )

  (bronzeDF.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_location}/{catalog}/{database}_bronze/{table}")
    .trigger(availableNow=True)
    .table(f"{catalog}.{database}_bronze.{table}")
    .awaitTermination()
  )

  # Silver Layer

  silverDF = (spark.readStream.table(f"{catalog}.{database}_bronze.{table}")
    .select(col("key").alias("eventId"), from_json(col("value"), schema).alias("json"))
    .select("eventId", "json.*")
  )

  # TBD
  # - deduplicate records
  # - other data quality treatments
  # - merge into target table

  (silverDF.writeStream
    .outputMode("update")
    .option("checkpointLocation", f"{checkpoint_location}/{catalog}/{database}_silver/{table}")
    .trigger(availableNow=True)
    .foreachBatch(lambda microbatch, x: merge_delta(microbatch, target))
    .start()
    .awaitTermination()
  )

# COMMAND ----------

# MAGIC %md ## 2/ Run ingestion

# COMMAND ----------

tables = spark.sql('select * from vr_demo.kafka.control').collect()
display(tables)

# COMMAND ----------

for table in tables:
  KafkaIngestion(table)

# COMMAND ----------

# MAGIC %md ## 3/ Visualize data

# COMMAND ----------

display(spark.table('vr_demo.kafka_bronze.table1'))

# COMMAND ----------

display(spark.table('vr_demo.kafka_silver.table1'))

# COMMAND ----------

# MAGIC %sql select 'bronze' as layer, count(*) as cnt from vr_demo.kafka_bronze.table1
# MAGIC union select 'silver' as layer, count(*) as cnt from vr_demo.kafka_silver.table1
