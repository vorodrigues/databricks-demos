-- Databricks notebook source
-- MAGIC %md # database

-- COMMAND ----------

create database if not exists vr_demo.crisp_handson

-- COMMAND ----------

use vr_demo.crisp_handson

-- COMMAND ----------

-- MAGIC %md # vendas

-- COMMAND ----------

create or replace table vendas as 

with product_ids as (
  select 
    product_id, 
    rank() over (order by product_id) as product_id_new
  from (select distinct product_id from vr_demo.crisp.sales_silver)
)

select
  cast(sales_id as long) as id_venda,
  cast(store_id as long) as id_loja,
  p.product_id_new as id_produto,
  cast(date_key as date) as dt_venda,
  cast(sales_quantity as int) as qt_venda,
  cast(sales_amount as double) as vl_venda
from (select * from vr_demo.crisp.sales_silver where sales_quantity > 0) s
left join product_ids p
on s.product_id = p.product_id
limit 500000

-- COMMAND ----------

select min(dt_venda) as min_dt, max(dt_venda) as max_dt from vendas

-- COMMAND ----------

select count(*) from vendas

-- COMMAND ----------

-- MAGIC %md # dim_loja

-- COMMAND ----------

create or replace table dim_loja (
  id_loja long,
  cod long not null,
  varejista string,
  nlj string,
  tipo string,
  cep string,
  lat_long string
);

insert into dim_loja 
select distinct
  row_number() over (order by s.store_id) as id_loja,
  s.store_id as cod,
  s.retailer as varejista,
  s.store as nlj,
  s.store_type as tipo,
  s.store_zip as cep,
  s.store_lat_long as lat_long
from vr_demo.crisp.dim_store s
inner join (select distinct id_loja from vendas) t 
on t.id_loja = s.store_id

-- COMMAND ----------

select count(distinct id_loja) from vendas

-- COMMAND ----------

-- MAGIC %md # dim_medicamento

-- COMMAND ----------

create or replace table dim_medicamento as
select m.* from luis_assuncao.genie_aibi.dim_medicamento m
inner join (select distinct id_produto from vendas) t
on m.id_produto = t.id_produto

-- COMMAND ----------

select count(*) from dim_medicamento

-- COMMAND ----------

select count(distinct id_produto) from vendas

-- COMMAND ----------

select count(*) from from luis_assuncao.genie_aibi.dim_medicamento

-- COMMAND ----------

-- MAGIC %md # estoque

-- COMMAND ----------

create or replace table estoque (
  id_estoque bigint,
  data_estoque date,
  id_produto bigint,
  estoque double
);


insert into estoque

with product_ids as (
  select s.* from (
    select 
      product_id, 
      rank() over (order by product_id) as id_produto
    from (select distinct product_id from vr_demo.crisp.sales_silver)) s
  inner join (select distinct id_produto from vendas) t
  on s.id_produto = t.id_produto
),

ids as (
  select distinct 
    pi.id_produto,
    pi.product_id, 
    v.dt_venda
  from product_ids pi
  inner join vendas v 
  on pi.id_produto = v.id_produto
)

select
  i.inventory_id as id_estoque,
  i.date_key as data_estoque,
  p.id_produto as id_produto,
  i.on_hand_quantity as estoque
from vr_demo.crisp.inventory_dc i
inner join ids p on i.product_id = p.product_id and i.date_key = p.dt_venda
group by all

-- COMMAND ----------

select count(*) from estoque
