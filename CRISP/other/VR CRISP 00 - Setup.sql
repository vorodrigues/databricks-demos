-- Databricks notebook source
create or replace table vr_demo.crisp.sales as select * from crisp_retail_and_distributor.examples.harmonized_retailer_sales

-- COMMAND ----------

ALTER TABLE vr_demo.crisp.sales SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

-- COMMAND ----------

INSERT INTO vr_demo.crisp.sales 
SELECT * 
FROM vr_demo.crisp.sales 
LIMIT 100

-- COMMAND ----------

create table vr_demo.crisp.dim_store as
select distinct store_id, store, store_type, store_zip, store_lat_long from vr_demo.crisp.sales

-- COMMAND ----------

create or replace table vr_demo.crisp.inventory_store as select * from crisp_retail_and_distributor.examples.harmonized_retailer_inventory_store

-- COMMAND ----------

create or replace table vr_demo.crisp.inventory_dc as select * from crisp_retail_and_distributor.examples.harmonized_retailer_inventory_dc

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

# Create a DataFrame using the Auto Loader to read the JSON data from S3
df = spark.readStream.format("cloudFiles") \
             .option("cloudFiles.format", "json") \
             .option("cloudFiles.schemaLocation", "s3://one-env/vr/crisp/schema") \
             .load("s3://one-env/vr/crisp/sales")

# Insert the data from the DataFrame into the vr_demo.crisp.sales_bronze table
df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3://one-env/vr/crisp/checkpoint") \
        .toTable("vr_demo.crisp.sales_bronze")

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md # New

-- COMMAND ----------

-- MAGIC %md ## Criar Database

-- COMMAND ----------

create database if not exists vr_demo.crisp

-- COMMAND ----------

-- MAGIC %md ## Criar JSON de vendas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a dataframe with the desired columns
-- MAGIC df = spark.sql("""
-- MAGIC   SELECT sales_id, date_key, store_id, product_id, sales_quantity, sales_amount FROM vr_demo.crisp.sales
-- MAGIC   --WHERE
-- MAGIC   --  (product_id=506697609867662742 AND store_id=8658698973831929810) OR
-- MAGIC   --  (product_id=506697609867662742 AND store_id=8092803279587142042)OR
-- MAGIC   --  (product_id=4120371332641752996 AND store_id=8658698973831929810)OR
-- MAGIC   --  (product_id=4120371332641752996 AND store_id=7453018759137932870)
-- MAGIC """)
-- MAGIC
-- MAGIC # Save the dataframe in JSON format
-- MAGIC df.write.mode("overwrite").json("s3://one-env/vr/crisp/sales")

-- COMMAND ----------

-- MAGIC %md ## Criar tabela de produtos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df = spark.sql('''
-- MAGIC   with r as (
-- MAGIC     select distinct product_id, supplier, product, upc, rank() over (partition by product_id order by date_key desc) as rnk from vr_demo.crisp.sales limit 1000
-- MAGIC   )
-- MAGIC   select * except (rnk) from r where rnk = 1
-- MAGIC ''')
-- MAGIC
-- MAGIC server_name = "jdbc:sqlserver://jsfsql.database.windows.net:1433"
-- MAGIC database_name = "dbo"
-- MAGIC url = server_name + ";" + "databaseName=" + database_name + ";"
-- MAGIC
-- MAGIC table_name = "dim_product"
-- MAGIC username = "username" # Please specify user here
-- MAGIC password = "password123!#" # Please specify password here
-- MAGIC
-- MAGIC try:
-- MAGIC   df.write \
-- MAGIC     .format("com.microsoft.sqlserver.jdbc.spark") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .option("url", url) \
-- MAGIC     .option("dbtable", table_name) \
-- MAGIC     .option("user", username) \
-- MAGIC     .option("password", password) \
-- MAGIC     .save()
-- MAGIC except ValueError as error :
-- MAGIC     print("Connector write failed", error)

-- COMMAND ----------

-- MAGIC %md ## Criar tabela de lojas

-- COMMAND ----------

create or replace table vr_demo.crisp.dim_store as
select distinct store_id, retailer, store, store_type, store_zip, store_lat_long from vr_demo.crisp.sales
