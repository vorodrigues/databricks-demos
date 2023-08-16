# Databricks notebook source
# MAGIC %md # StarCoder + SQLChain

# COMMAND ----------

# MAGIC %md ## Prepare environment

# COMMAND ----------

# MAGIC %pip install -U langchain sqlalchemy databricks-sql-connector

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
os.environ['TRANSFORMERS_CACHE'] = "/dbfs/tmp/cache/hf"

# COMMAND ----------

from langchain import SQLDatabase

db = SQLDatabase.from_databricks("samples", "nyctaxi", host="...", api_token="dapi...", warehouse_id="...")

# COMMAND ----------

import huggingface_hub
huggingface_hub.login(token="...")

# COMMAND ----------

# MAGIC %md ## Load model

# COMMAND ----------

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

model_ckpt = "bigcode/starcoderbase"
model = AutoModelForCausalLM.from_pretrained(model_ckpt, use_auth_token=True, torch_dtype=torch.bfloat16, device_map="balanced")
tokenizer = AutoTokenizer.from_pretrained(model_ckpt, use_auth_token=True)

# COMMAND ----------

pipe = pipeline("text-generation", model=model, tokenizer=tokenizer, max_new_tokens=256)

# COMMAND ----------

from langchain.llms import HuggingFacePipeline
from langchain import SQLDatabaseChain 

hf_pipeline = HuggingFacePipeline(pipeline=pipe)
db_chain = SQLDatabaseChain.from_llm(hf_pipeline, db, verbose=True)

# COMMAND ----------

# MAGIC %md ## Inference

# COMMAND ----------

# MAGIC %md ### Query generation

# COMMAND ----------

chain_result = db_chain.run("Find the maximum fare by pickup ZIP")

# COMMAND ----------

print(chain_result)

# COMMAND ----------

# MAGIC %md ### Query generation with DDL

# COMMAND ----------

table = "samples.nyctaxi.trips"
command = "Find the maximum fare by pickup ZIP"

create_stmt = spark.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]
create_stmt = create_stmt[:create_stmt.find("USING")]
sample_query = f"SELECT * FROM {table} LIMIT 3"
sample_data_md = spark.sql(sample_query).toPandas().to_markdown()
#prompt = f"<fim_prefix>{create_stmt}\n{sample_query}\n\n{sample_data_md}\n\n-- The following ANSI SQL query does the following: {command}\n<fim_suffix><fim_middle>"
prompt = f"<fim_prefix>{create_stmt}\n-- The following ANSI SQL query does the following: {command}\n<fim_suffix><fim_middle>"
print(prompt)

# COMMAND ----------

sql_query = pipe(prompt)[0]['generated_text'][len(prompt):]
print(sql_query)

# COMMAND ----------

display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md ### Prompt with DDL and Sample

# COMMAND ----------

# MAGIC %md #### Table comment generation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE samples.nyctaxi.trips (
# MAGIC   tpep_pickup_datetime TIMESTAMP,
# MAGIC   tpep_dropoff_datetime TIMESTAMP,
# MAGIC   trip_distance DOUBLE,
# MAGIC   fare_amount DOUBLE,
# MAGIC   pickup_zip INT,
# MAGIC   dropoff_zip INT)

# COMMAND ----------

print(db.get_table_info())

# COMMAND ----------

prompt = """<fim_prefix>CREATE TABLE trips (
	tpep_pickup_datetime TIMESTAMP, 
	tpep_dropoff_datetime TIMESTAMP, 
	trip_distance FLOAT, 
	fare_amount FLOAT, 
	pickup_zip INT, 
	dropoff_zip INT
) USING DELTA
COMMENT="<fim_suffix>"

/*
3 rows from trips table:
tpep_pickup_datetime	tpep_dropoff_datetime	trip_distance	fare_amount	pickup_zip	dropoff_zip
2016-02-14 16:52:13	2016-02-14 17:16:04	4.94	19.0	10282	10171
2016-02-04 18:44:19	2016-02-04 18:46:00	0.28	3.5	10110	10110
2016-02-17 17:13:57	2016-02-17 17:17:55	0.7	5.0	10103	10023
*/
<fim_middle>"""

# COMMAND ----------

print(pipe(prompt, min_new_tokens=32, max_new_tokens=128)[0]['generated_text'][len(prompt):])

# COMMAND ----------

# MAGIC %md #### Table documentation generation

# COMMAND ----------

prompt = """<fim_prefix>
/*
Table and column documentation:
<fim_suffix>
*/
CREATE TABLE trips (
	tpep_pickup_datetime TIMESTAMP, 
	tpep_dropoff_datetime TIMESTAMP, 
	trip_distance FLOAT, 
	fare_amount FLOAT, 
	pickup_zip INT, 
	dropoff_zip INT
) USING DELTA

/*
3 rows from trips table:
tpep_pickup_datetime	tpep_dropoff_datetime	trip_distance	fare_amount	pickup_zip	dropoff_zip
2016-02-14 16:52:13	2016-02-14 17:16:04	4.94	19.0	10282	10171
2016-02-04 18:44:19	2016-02-04 18:46:00	0.28	3.5	10110	10110
2016-02-17 17:13:57	2016-02-17 17:17:55	0.7	5.0	10103	10023
*/
<fim_middle>"""

# COMMAND ----------

print(pipe(prompt, min_new_tokens=32, max_new_tokens=128)[0]['generated_text'][len(prompt):])

# COMMAND ----------

# MAGIC %md #### Table documentation generation with prompt

# COMMAND ----------

print(pipe("""
CREATE TABLE trips (
	tpep_pickup_datetime TIMESTAMP, 
	tpep_dropoff_datetime TIMESTAMP, 
	trip_distance FLOAT, 
	fare_amount FLOAT, 
	pickup_zip INT, 
	dropoff_zip INT
) USING DELTA

/*
3 rows from trips table:
tpep_pickup_datetime	tpep_dropoff_datetime	trip_distance	fare_amount	pickup_zip	dropoff_zip
2016-02-14 16:52:13	2016-02-14 17:16:04	4.94	19.0	10282	10171
2016-02-04 18:44:19	2016-02-04 18:46:00	0.28	3.5	10110	10110
2016-02-17 17:13:57	2016-02-17 17:17:55	0.7	5.0	10103	10023
*/

Write a comment describing this table and each of its columns.
""")[0]['generated_text'])

# COMMAND ----------

# MAGIC %md ## Prompt example
# MAGIC ```
# MAGIC CREATE TABLE viagens (
# MAGIC 	tpep_pickup_datetime TIMESTAMP,
# MAGIC 	tpep_dropoff_datetime TIMESTAMP,
# MAGIC 	distancia FLOAT,
# MAGIC 	tarifa FLOAT,
# MAGIC 	pickup_zip INT,
# MAGIC 	dropoff_zip INT
# MAGIC ) USING DELTA
# MAGIC
# MAGIC /* 
# MAGIC 3 rows from trips table:
# MAGIC tpep_pickup_datetime	tpep_dropoff_datetime	distancia	tarifa	pickup_zip	dropoff_zip
# MAGIC 2016-02-14 16:52:13	2016-02-14 17:16:04	4.94	19.0	10282	10171
# MAGIC 2016-02-04 18:44:19	2016-02-04 18:46:00	0.28	3.5	10110	10110
# MAGIC 2016-02-17 17:13:57	2016-02-17 17:17:55	0.7	5.0	10103	10023
# MAGIC */
# MAGIC
# MAGIC -- calcule a tarifa máxima e a distância média das viagens
# MAGIC ```
