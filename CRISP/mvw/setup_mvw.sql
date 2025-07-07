-- Databricks notebook source
-- MAGIC %md # database

-- COMMAND ----------

create database vr_demo.crisp

-- COMMAND ----------

-- MAGIC %md # sample

-- COMMAND ----------

create or replace table vr_demo.crisp.sample as
select 
  sales_id, store_id, product_id, date_key, sales_quantity, sales_amount, -- ft_sales
  store, store_type, store_zip, store_lat_long, -- dim_store
  supplier, product, upc -- dim_product
from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_sales
limit 1000000

-- COMMAND ----------

-- MAGIC %md # ft_sales

-- COMMAND ----------

create or replace table vr_demo.crisp.ft_sales as
select sales_id, store_id, product_id, date_key, sales_quantity, sales_amount
from vr_demo.crisp.sample

-- COMMAND ----------

-- MAGIC %md # dim_store

-- COMMAND ----------

create or replace table vr_demo.crisp.dim_store as
select distinct store_id, store, store_type, store_zip, store_lat_long
from vr_demo.crisp.sample

-- COMMAND ----------

-- MAGIC %md # dim_product

-- COMMAND ----------

create or replace table vr_demo.crisp.dim_product as
select distinct product_id, supplier, product, upc
from vr_demo.crisp.sample

-- COMMAND ----------

-- MAGIC %md # ft_inventory

-- COMMAND ----------

create or replace table vr_demo.crisp.ft_inventory as 
select i.inventory_id, i.store_id, i.product_id, i.date_key, s.sales_quantity * (1 + 0.5 * rand()) as on_hand_quantity -- i.on_hand_quantity
from crisp_inc_cpg_retail_and_distributor_data_samples.examples.harmonized_retailer_inventory_store i
inner join (select distinct store_id, product_id, date_key, sales_quantity from vr_demo.crisp.sample) s
on i.store_id = s.store_id and i.product_id = s.product_id and i.date_key = s.date_key

-- COMMAND ----------

-- MAGIC %md # mvw_sales

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_sales (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
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

  source: vr_demo.crisp.ft_sales

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

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_inventory (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
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

  source: vr_demo.crisp.ft_inventory

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

select product_id, sum(on_hand_quantity) as inventory from vr_demo.crisp.ft_inventory group by product_id order by inventory desc limit 10

-- COMMAND ----------

-- MAGIC %md # mvw_osa

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_osa (
  `Store` COMMENT 'Store name',
  `Store Type`,
  `Store Zip`,
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
