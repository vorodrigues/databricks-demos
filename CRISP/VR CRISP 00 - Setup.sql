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
-- MAGIC   WHERE date_key between '2022-12-01'and '2022-12-31'
-- MAGIC """)
-- MAGIC
-- MAGIC # Save the dataframe in JSON format
-- MAGIC df.write.json("s3://one-env/vr/crisp/sales")

-- COMMAND ----------

-- MAGIC %md ## Criar tabela de lojas

-- COMMAND ----------

create table vr_demo.crisp.dim_store as
select distinct store_id, store, store_type, store_zip, store_lat_long from vr_demo.crisp.sales

-- COMMAND ----------

-- MAGIC %md ## Criar tabela de produtos

-- COMMAND ----------

create table vr_demo.crisp.dim_product as
select distinct product_id, supplier, product, upc from vr_demo.crisp.sales
