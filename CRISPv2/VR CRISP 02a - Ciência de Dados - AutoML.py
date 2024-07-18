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
    (product_id=7529767289596171178 AND store_id=4028576256296879621) OR
    (product_id=7529767289596171178 AND store_id=8255463680201369049) OR
    (product_id=7262525695453454463 AND store_id=7909929122585239790) OR
    (product_id=7529767289596171178 AND store_id=1162676141249511919) OR
    (product_id=5131941246915832690 AND store_id=4028576256296879621) OR
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

dbutils.jobs.taskValues.set(key = "model_output_table_name", value = model.output_table_name)
print(model.output_table_name)

# COMMAND ----------

perf = model.best_trial.evaluation_metric_score
dbutils.jobs.taskValues.set(key = "perf", value = perf)
print(perf)

# COMMAND ----------

approved = (perf < 1.5)
dbutils.jobs.taskValues.set(key = "approved", value = approved)
print(approved)
