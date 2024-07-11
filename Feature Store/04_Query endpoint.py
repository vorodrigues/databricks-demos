# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering==0.2.0 databricks-sdk==0.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

df = (spark.table('vr_demo.feature_store.travel_purchase')
    .select('ts', 'destination_id', 'user_id', 'user_latitude', 'user_longitude', 'booking_date')
    .limit(10))
lookup_keys = df.toPandas().astype({'ts': 'str', 'booking_date': 'str'}).to_dict(orient="records")

# COMMAND ----------

import timeit
from databricks.sdk import WorkspaceClient

wc = WorkspaceClient()
endpoint_name = "vr_travel_expert_endpoint"

#Query the endpoint
for lookup_key in lookup_keys:
    print(f'Compute the propensity score for this customer: {lookup_key}')
    starting_time = timeit.default_timer()
    inferences = wc.serving_endpoints.query(endpoint_name, inputs=lookup_key)
    end_time = timeit.default_timer()
    print(inferences)
    print(f"Inference time, end 2 end :{round((end_time - starting_time)*1000)}ms")
