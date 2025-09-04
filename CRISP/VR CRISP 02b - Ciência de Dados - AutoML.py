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
    (product_id=820558267321802768 AND store_id=1186089951684251282) OR
    (product_id=181643065667445703 AND store_id=2282245423997110910) OR
    (product_id=5655945986815814052 AND store_id=4328351085320015674) OR
    (product_id=181643065667445703 AND store_id=5029475122466982522) OR
    (product_id=181643065667445703 AND store_id=749605886145008442) OR
    (product_id=820558267321802768 AND store_id=1726271638195919732) OR
    (product_id=820558267321802768 AND store_id=1922502944801829527) OR
    (product_id=181643065667445703 AND store_id=6732994364523568257) OR
    (product_id=820558267321802768 AND store_id=806622226360471425) OR
    (product_id=181643065667445703 AND store_id=2391965958404173729)
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
