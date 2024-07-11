-- Databricks notebook source
-- MAGIC %md # Ciência de Dados

-- COMMAND ----------

-- MAGIC %md ## Preparação dos Dados
-- MAGIC Calcula o sales_amount por mês com base no date_key para cada produto e loja usando os dados da tabela vr_demo.crisp.sales

-- COMMAND ----------

CREATE OR REPLACE TABLE vr_demo.crisp.sales_monthly AS
SELECT
  product_id,
  store_id,
  trunc(date_key, "month") as month,
  sum(sales_amount) as sales_amount
FROM vr_demo.crisp_dlt.sales_gold
GROUP BY ALL
