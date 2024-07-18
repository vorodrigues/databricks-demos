# Databricks notebook source
# MAGIC %md # Ciência de Dados

# COMMAND ----------

# MAGIC %md ## Resultado

# COMMAND ----------

model_output_table_name = dbutils.jobs.taskValues.get(taskKey = "Previsao", key = "model_output_table_name")
print(model_output_table_name)

# COMMAND ----------

spark.sql(f'''
CREATE OR REPLACE TABLE vr_demo.crisp.sales_forecast AS

WITH sales AS (
    SELECT
        coalesce(h.product_id, f.product_id) as product_id,
        coalesce(h.store_id, f.store_id) as store_id,
        coalesce(h.month, f.month) as month,
        h.sales_amount,
        f.sales_amount as forecast,
        f.sales_amount_lower as forecast_lower,
        f.sales_amount_upper as forecast_upper
    FROM vr_demo.crisp.sales_monthly h
    FULL JOIN {model_output_table_name} f
    ON
        h.product_id = f.product_id 
        AND h.store_id = f.store_id
        AND h.month = f.month
)

SELECT
    a.product_id,
    a.store_id,
    a.month,
    p.supplier,
    p.product,
    p.upc,
    s.retailer,
    s.store,
    s.store_type,
    s.store_zip,
    s.store_lat_long,
    a.sales_amount,
    a.forecast,
    a.forecast_lower,
    a.forecast_upper
FROM sales a
LEFT JOIN vr_demo.crisp.dim_product p
ON a.product_id = p.product_id
LEFT JOIN vr_demo.crisp.dim_store s
ON a.store_id = s.store_id
''')
