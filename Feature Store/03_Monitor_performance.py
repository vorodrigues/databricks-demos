# Databricks notebook source
# MAGIC %md # Avalia a performance do modelo

# COMMAND ----------

alert = spark.sql('''
select
    window.start as window,
    f1_score.weighted,
    case when f1_score.weighted < 0.85 then True else False end as alert_perf
from vr_demo.feature_store_monitor.expert_predictions_labels_profile_metrics
where
    model = 'vr_demo.feature_store.vr_travel_expert' and
    granularity = '1 day' and
    slice_key is null and
    column_name = ":table"
order by window.start desc
limit 1
''').collect()[0].alert_perf
display(alert)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "alert", value = alert)
