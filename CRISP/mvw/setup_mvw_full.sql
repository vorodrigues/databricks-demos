-- Databricks notebook source
-- MAGIC %md # database

-- COMMAND ----------

create database vr_demo.crisp_mvw

-- COMMAND ----------

-- MAGIC %md # raw

-- COMMAND ----------

-- select max(date_key) from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales

-- COMMAND ----------

-- select current_date() - max(date_key) from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales

-- COMMAND ----------

-- create or replace table vr_demo.crisp.sales_raw as 
-- select 
--   date_key + 920 as date_key, -- 79488000/60/60/24
--   * except (date_key)
-- from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales

-- COMMAND ----------

-- select max(date_key) from vr_demo.crisp.sales_raw

-- COMMAND ----------

-- %py 

-- (spark.table('vr_demo.crisp.sales_raw')
--   .select('sales_id', 'store_id', 'product_id', 'date_key', 'sales_quantity', 'sales_amount')
--   .write
--   .format('csv')
--   .mode('overwrite')
--   .option('header', 'true')
--   .save('s3://one-env/vr/crisp/sales')
-- )

-- COMMAND ----------

-- create or replace table vr_demo.crisp.inventory_store as 
-- select
--   date_key + 920 as date_key, -- 79488000/60/60/24
--   * except (date_key)
-- from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_inventory_store

-- COMMAND ----------

-- MAGIC %md # sample

-- COMMAND ----------

-- create or replace table vr_demo.crisp.sample as
-- select 
--   sales_id, store_id, product_id, date_key, sales_quantity, sales_amount, -- ft_sales
--   retailer, store, store_type, store_zip, store_lat_long, -- dim_store
--   supplier, product, upc -- dim_product
-- from vr_demo.crisp.sales_raw
-- limit 1000000

-- COMMAND ----------

-- MAGIC %md # ft_sales

-- COMMAND ----------

-- create or replace table vr_demo.crisp_mvw.ft_sales as
-- select sales_id, store_id, product_id, date_key, sales_quantity, sales_amount
-- from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC (spark.table('crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales')
-- MAGIC   .select('sales_id', 'store_id', 'product_id', 'date_key', 'sales_quantity', 'sales_amount')
-- MAGIC   .distinct()
-- MAGIC   .orderBy('sales_id')
-- MAGIC   .dropDuplicates(['sales_id'])
-- MAGIC   .write
-- MAGIC   .mode('overwrite')
-- MAGIC   .saveAsTable('vr_demo.crisp_mvw.ft_sales')
-- MAGIC )

-- COMMAND ----------

select sales_id, count(*) as cnt from vr_demo.crisp_mvw.ft_sales group by sales_id order by cnt desc limit 10

-- COMMAND ----------

-- MAGIC %md # dim_store

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC (spark.table('crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales')
-- MAGIC   .select('store_id', 'store', 'store_type', 'store_zip', 'store_lat_long', 'retailer')
-- MAGIC   .distinct()
-- MAGIC   .orderBy('store_id')
-- MAGIC   .dropDuplicates(['store_id'])
-- MAGIC   .write
-- MAGIC   .mode('overwrite')
-- MAGIC   .saveAsTable('vr_demo.crisp_mvw.dim_store')
-- MAGIC )

-- COMMAND ----------

select store_id, count(*) as cnt from vr_demo.crisp_mvw.dim_store group by store_id order by cnt desc limit 10

-- COMMAND ----------

-- MAGIC %md # dim_product

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC (spark.table('crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales')
-- MAGIC   .select('product_id', 'supplier', 'product', 'upc')
-- MAGIC   .distinct()
-- MAGIC   .orderBy('product_id')
-- MAGIC   .dropDuplicates(['product_id'])
-- MAGIC   .write
-- MAGIC   .mode('overwrite')
-- MAGIC   .saveAsTable('vr_demo.crisp_mvw.dim_product')
-- MAGIC )

-- COMMAND ----------

select product_id, count(*) as cnt from vr_demo.crisp_mvw.dim_product group by product_id order by cnt desc limit 10

-- COMMAND ----------

-- MAGIC %md # ft_inventory

-- COMMAND ----------

-- create or replace table vr_demo.crisp.ft_inventory as 
-- select i.inventory_id, i.store_id, i.product_id, i.date_key, s.sales_quantity * (1 + 0.5 * rand()) as on_hand_quantity -- i.on_hand_quantity
-- from vr_demo.crisp.inventory_store i
-- inner join (select distinct store_id, product_id, date_key, sales_quantity from vr_demo.crisp.sample) s
-- on i.store_id = s.store_id and i.product_id = s.product_id and i.date_key = s.date_key

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC df = (spark.table('crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_inventory_store')
-- MAGIC   .select('inventory_id', 'store_id', 'product_id', 'date_key', 'on_hand_quantity')
-- MAGIC   .distinct()
-- MAGIC   .orderBy('inventory_id')
-- MAGIC   .dropDuplicates(['inventory_id'])
-- MAGIC )
-- MAGIC
-- MAGIC spark.sql('''
-- MAGIC   create or replace table vr_demo.crisp_mvw.ft_inventory as
-- MAGIC   select i.inventory_id, i.store_id, i.product_id, i.date_key, s.sales_quantity * (1 + 0.5 * rand()) as on_hand_quantity
-- MAGIC   from {df} i
-- MAGIC   inner join (select store_id, product_id, date_key, sum(sales_quantity)/count(distinct sales_id) as sales_quantity from vr_demo.crisp_mvw.ft_sales group by all) s
-- MAGIC   on i.store_id = s.store_id and i.product_id = s.product_id and i.date_key = s.date_key
-- MAGIC ''', df=df)

-- COMMAND ----------

select inventory_id, count(*) as cnt from vr_demo.crisp_mvw.ft_inventory group by inventory_id order by cnt desc limit 10

-- COMMAND ----------

-- MAGIC %md # mvw_sales

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp_mvw.mvw_sales (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
  `Retailer`,
  `Product` COMMENT 'Product name',
  `Supplier`,
  `Date` COMMENT 'Date of the sale',
  `Sales Quantity`,
  `Sales Amount`,
  `Average Sales Ticket`
)
WITH METRICS
LANGUAGE YAML
COMMENT 'A Metric View for sales metrics.'
AS $$

  version: 0.1

  source: vr_demo.crisp_mvw.ft_sales

  joins:
  - name: dim_store
    source: vr_demo.crisp_mvw.dim_store
    using:
    - store_id
  - name: dim_product
    source: vr_demo.crisp_mvw.dim_product
    using:
    - product_id
  
  dimensions:
  - name: Store
    expr: dim_store.store
  - name: Store Type
    expr: dim_store.store_type
  - name: Store Zip
    expr: dim_store.store_zip
  - name: Retailer
    expr: dim_store.retailer
  - name: Product
    expr: dim_product.product
  - name: Supplier
    expr: dim_product.supplier
  - name: Date
    expr: source.date_key
  
  measures:
  - name: Sales Quantity
    expr: SUM(sales_quantity)
  - name: Sales Amount
    expr: SUM(sales_amount)
  - name: Average Sales Ticket
    expr: SUM(sales_amount) / SUM(sales_quantity)
  
$$

-- COMMAND ----------

-- MAGIC %md # mvw_inventory

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp_mvw.mvw_inventory (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
  `Retailer`,
  `Product` COMMENT 'Product name',
  `Supplier`,
  `Date` COMMENT 'Date of the sale',
  `Inventory Quantity`
)
WITH METRICS
LANGUAGE YAML
COMMENT 'A Metric View for inventory metrics.'
AS $$

  version: 0.1

  source: vr_demo.crisp_mvw.ft_inventory

  joins:
  - name: dim_store
    source: vr_demo.crisp_mvw.dim_store
    using:
    - store_id
  - name: dim_product
    source: vr_demo.crisp_mvw.dim_product
    using:
    - product_id
  
  dimensions:
  - name: Store
    expr: dim_store.store
  - name: Store Type
    expr: dim_store.store_type
  - name: Store Zip
    expr: dim_store.store_zip
  - name: Retailer
    expr: dim_store.retailer
  - name: Product
    expr: dim_product.product
  - name: Supplier
    expr: dim_product.supplier
  - name: Date
    expr: source.date_key
  
  measures:
  - name: Inventory Quantity
    expr: SUM(on_hand_quantity)
  
$$

-- COMMAND ----------

-- MAGIC %md # mvw_osa

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp_mvw.mvw_osa (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
  `Retailer`,
  `Product` COMMENT 'Product name',
  `Supplier`,
  `Date` COMMENT 'Date of the sale',
  `OSA` COMMENT 'On-Shelf Availability (defined by sales quantity / inventory quantity)'
)
WITH METRICS
LANGUAGE YAML
COMMENT 'A Metric View for On-Shelf Availability metrics.'
AS $$

  version: 0.1

  source: select i.store_id, i.product_id, i.date_key, i.on_hand_quantity, s.sales_quantity, s.sales_amount from vr_demo.crisp_mvw.ft_inventory i inner join vr_demo.crisp_mvw.ft_sales s on i.store_id = s.store_id and i.product_id = s.product_id and i.date_key = s.date_key

  joins:
  - name: dim_store
    source: vr_demo.crisp_mvw.dim_store
    using:
    - store_id
  - name: dim_product
    source: vr_demo.crisp_mvw.dim_product
    using:
    - product_id
  
  dimensions:
  - name: Store
    expr: dim_store.store
  - name: Store Type
    expr: dim_store.store_type
  - name: Store Zip
    expr: dim_store.store_zip
  - name: Retailer
    expr: dim_store.retailer
  - name: Product
    expr: dim_product.product
  - name: Supplier
    expr: dim_product.supplier
  - name: Date
    expr: source.date_key
  
  measures:
  - name: OSA
    expr: SUM(sales_quantity) / SUM(on_hand_quantity)
  
$$

-- COMMAND ----------

-- MAGIC %md # mvw_osa_v2

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_osa_v2 (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
  `Retailer`,
  `Product` COMMENT 'Product name',
  `Supplier`,
  `Date` COMMENT 'Date of the sale',
  `OSA` COMMENT 'On-Shelf Availability (defined by sales quantity / inventory quantity)'
)
WITH METRICS
LANGUAGE YAML
COMMENT 'A Metric View for On-Shelf Availability metrics.'
AS $$

  version: 0.1

  source: select i.store_id, i.product_id, i.date_key, i.on_hand_quantity, s.sales_quantity, s.sales_amount from vr_demo.crisp.ft_inventory i inner join vr_demo.crisp.ft_sales s on i.store_id = s.store_id and i.product_id = s.product_id and i.date_key = s.date_key

  joins:
  - name: dim_store
    source: vr_demo.crisp.dim_store
    using:
    - store_id
  - name: dim_product
    source: vr_demo.crisp.dim_product
    using:
    - product_id
  
  dimensions:
  - name: Store
    expr: dim_store.store
  - name: Store Type
    expr: dim_store.store_type
  - name: Store Zip
    expr: dim_store.store_zip
  - name: Retailer
    expr: dim_store.retailer
  - name: Product
    expr: dim_product.product
  - name: Supplier
    expr: dim_product.supplier
  - name: Date
    expr: source.date_key
  
  measures:
  - name: OSA
    expr: SUM(sales_quantity) / SUM(on_hand_quantity)
  
$$
