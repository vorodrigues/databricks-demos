-- Databricks notebook source
-- MAGIC %md # mvw_sales

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_sales (
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

select
  `Store Type`,
  measure(`Sales Amount`) as `Sales Amount`,
  measure(`Average Sales Ticket`) as `Average Sales Ticket`
from vr_demo.crisp.mvw_sales
group by `Store Type`

-- COMMAND ----------

-- MAGIC %md # mvw_inventory

-- COMMAND ----------

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_inventory (
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

CREATE OR REPLACE VIEW vr_demo.crisp.mvw_osa (
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
