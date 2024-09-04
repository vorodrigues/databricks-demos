# Databricks notebook source
# You can connect to Kafka over either SSL/TLS encrypted connection, or with an unencrypted plaintext connection.
# Just choose the set of corresponding endpoints to use.
# If you chose the tls servers, you must enable SSL in the Kafka connection, see later for an example.
kafka_bootstrap_servers_tls = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-tls")
#kafka_bootstrap_servers_plaintext = dbutils.secrets.get("oetrta", "oetrta-kafka-servers-plain")
# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")
# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/kafka_demo"
checkpoint_location = f"{project_dir}/checkpoints/kafka"
topic = f"{user}_kafka_demo"

# COMMAND ----------

# MAGIC %md ## 0/ Setup config table

# COMMAND ----------

# MAGIC %sql create database if not exists vr_demo.kafka;

# COMMAND ----------

# MAGIC %sql create table if not exists vr_demo.kafka.control (
# MAGIC   catalog string,
# MAGIC   database string,
# MAGIC   table string,
# MAGIC   topic string,
# MAGIC   schema string,
# MAGIC   merge_keys string,
# MAGIC   ts_key string,
# MAGIC   clustering_keys string
# MAGIC )

# COMMAND ----------

# MAGIC %md ## 1/ Infer schemas

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row
from time import sleep

# Define variáveis
temp_location = '/tmp/kafka/sample'
checkpoint_location = "/tmp/kafka/checkpoint"
schemas = []

# Define tabelas para a inferência de schema
tables = [
    {'catalog':'vr_demo', 'database':'kafka', 'table':'table1', 'topic':'victor_rodrigues_oetrta_kafka_topic', 'merge_keys':'eventid', 'ts_key':'processingTime', 'clustering_keys':'time,action'},
    {'catalog':'vr_demo', 'database':'kafka', 'table':'table2', 'topic':'victor_rodrigues_oetrta_kafka_topic2', 'merge_keys':'eventid', 'ts_key':'processingTime', 'clustering_keys':'time,action'}
]

for table in tables:

    topic = table['topic']

    # Ingere amostra de dados do Kafka
    df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls) 
      .option("kafka.security.protocol", "SSL") 
      .option("startingOffsets", "earliest")
      .option("subscribe", topic)
      .load()
      .limit(1000)
      .select(
        col("value").cast("string").alias("value")
      )
    )
    
    # Escreve amostra no tmp
    (df.writeStream.format('text')
      .option('outputMode', 'overwrite')
      .option('path', temp_location)
      .option('checkpointLocation', checkpoint_location)
      .trigger(availableNow=True)
      .start()
      .awaitTermination()
    )

    # Infere o schema a partir da amostra
    table['schema'] = spark.sql(f'select * from json.`{temp_location}`').schema.simpleString()
    schemas.append(table)

    # Limpa os arquivos temporários
    dbutils.fs.rm(temp_location, True)
    dbutils.fs.rm(checkpoint_location, True)

# Cria um Spark Dataframe com os schemas
schemasDF = spark.createDataFrame(Row(**x) for x in tables)
schemasDF.createOrReplaceTempView('schemas')

# Escreve os schemas na tabela de controle
spark.sql(f"""
  MERGE INTO vr_demo.kafka.control t
  USING schemas s
  ON s.catalog = t.catalog AND s.database = t.database AND s.table = t.table
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md ## 2/ Visualize config table

# COMMAND ----------

display(spark.table('vr_demo.kafka.control'))
