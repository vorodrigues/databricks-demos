# Databricks notebook source
# MAGIC %md # Ciência de Dados

# COMMAND ----------

# MAGIC %md ## AutoML

# COMMAND ----------

import databricks.automl
from datetime import datetime

# Formata a data e hora atual
now = datetime.now()
formatted_date = now.strftime('%Y-%m-%d_%H-%M')

# Crie um modelo com Databricks AutoML
model = databricks.automl.forecast(
  experiment_name=f"VR CRISP Sales Forecast {formatted_date}",
  dataset=spark.table("vr_demo.crisp.sales_monthly").where("""
    (product_id=506697609867662742 AND store_id=8658698973831929810) OR
    (product_id=506697609867662742 AND store_id=8092803279587142042)OR
    (product_id=4120371332641752996 AND store_id=8658698973831929810)OR
    (product_id=4120371332641752996 AND store_id=7453018759137932870)
  """),
  target_col="sales_amount", 
  identity_col=["product_id", "store_id"], 
  time_col="month",
  frequency="month",
  horizon=6, 
  country_code="BR", 
  primary_metric="smape",
  output_database="vr_demo.crisp",
  timeout_minutes=120
)

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE VIEW vr_demo.crisp.forecast AS SELECT * FROM {model.output_table_name}")
print(model.output_table_name)
