# Databricks notebook source
# MAGIC %md
# MAGIC This is an example notebook that shows how to use [TimesFM](https://github.com/google-research/timesfm) models on Databricks. 
# MAGIC
# MAGIC The notebook loads the model, distributes the inference, registers the model, deploys the model and makes online forecasts.
# MAGIC
# MAGIC As of June 5, 2024, TimesFM supports python version below [3.10](https://github.com/google-research/timesfm/issues/60). So make sure your cluster is below DBR ML 14.3.

# COMMAND ----------

# MAGIC %pip install jax[cuda12]==0.4.26 --quiet
# MAGIC %pip install protobuf==3.20.* --quiet
# MAGIC %pip install utilsforecast --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
import subprocess
package = "git+https://github.com/google-research/timesfm.git"
subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data
# MAGIC Make sure that the catalog and the schema already exist.

# COMMAND ----------

catalog = "mmf"  # Name of the catalog we use to manage our assets
db = "m4"  # Name of the schema we use to manage our assets (e.g. datasets)
n = 100  # Number of time series to sample

# COMMAND ----------

# This cell will create tables: 
# 1. {catalog}.{db}.m4_daily_train
# 2. {catalog}.{db}.m4_monthly_train
dbutils.notebook.run("../data_preparation", timeout_seconds=0, arguments={"catalog": catalog, "db": db, "n": n})

# COMMAND ----------

# Make sure that the data exists
df = spark.table(f'{catalog}.{db}.m4_daily_train').toPandas()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribute Inference

# COMMAND ----------

# MAGIC %md
# MAGIC Distribution of the inference is managed by TimesFM so we don't need to use Pandas UDF. See the [github repository](https://github.com/google-research/timesfm/tree/master?tab=readme-ov-file#initialize-the-model-and-load-a-checkpoint) of TimesFM for detailed description of the input parameters. 

# COMMAND ----------

import timesfm

tfm = timesfm.TimesFm(
    context_len=512,  # Max context length of the model. It needs to be a multiplier of input_patch_len, i.e. a multiplier of 32.
    horizon_len=10,  # Horizon length can be set to anything. We recommend setting it to the largest horizon length.
    input_patch_len=32,
    output_patch_len=128,
    num_layers=20,
    model_dims=1280,
    backend="gpu",
)

tfm.load_from_checkpoint(repo_id="google/timesfm-1.0-200m")

forecast_df = tfm.forecast_on_df(
    inputs=df,
    freq="D",
    value_name="y",
    num_jobs=-1,
)

display(forecast_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Register Model

# COMMAND ----------

# MAGIC %md
# MAGIC We should ensure that any non-serializable attributes (like the timesfm model in TimesFMModel class) are not included in the serialization process. One common approach is to override the __getstate__ and __setstate__ methods in the class to manage what gets pickled. This modification ensures that the timesfm model is not included in the serialization process, thus avoiding the error. The load_model method is called to load the model when needed, such as during prediction or after deserialization.

# COMMAND ----------

import mlflow
import torch
import numpy as np
from mlflow.models import infer_signature
from mlflow.models.signature import ModelSignature
from mlflow.types import DataType, Schema, TensorSpec
mlflow.set_registry_uri("databricks-uc")


class TimesFMModel(mlflow.pyfunc.PythonModel):
  def __init__(self, repository):
    self.repository = repository
    self.tfm = None

  def load_model(self):
    import timesfm
    self.tfm = timesfm.TimesFm(
        context_len=512,
        horizon_len=10,
        input_patch_len=32,
        output_patch_len=128,
        num_layers=20,
        model_dims=1280,
        backend="gpu",
    )
    self.tfm.load_from_checkpoint(repo_id=self.repository)

  def predict(self, context, input_df, params=None):
    if self.tfm is None:
      self.load_model()
      
    forecast_df = self.tfm.forecast_on_df(
      inputs=input_df,
      freq="D",
      value_name="y",
      num_jobs=-1,
    )
    return forecast_df
  
  def __getstate__(self):
    state = self.__dict__.copy()
    # Remove the tfm attribute from the state, as it's not serializable
    del state['tfm']
    return state

  def __setstate__(self, state):
    # Restore instance attributes
    self.__dict__.update(state)
    # Reload the model since it was not stored in the state
    self.load_model()


pipeline = TimesFMModel("google/timesfm-1.0-200m")
signature = infer_signature(
  model_input=df,
  model_output=pipeline.predict(None, df),
)

registered_model_name=f"{catalog}.{db}.timesfm-1-200m"

with mlflow.start_run() as run:
  mlflow.pyfunc.log_model(
    "model",
    python_model=pipeline,
    registered_model_name=registered_model_name,
    signature=signature,
    input_example=df,
    pip_requirements=[
      "jax[cuda12]==0.4.26",
      "utilsforecast==0.1.10",
      "git+https://github.com/google-research/timesfm.git",
    ],
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reload Model

# COMMAND ----------

from mlflow import MlflowClient
client = MlflowClient()

def get_latest_model_version(client, registered_model_name):
    latest_version = 1
    for mv in client.search_model_versions(f"name='{registered_model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

model_version = get_latest_model_version(client, registered_model_name)
logged_model = f"models:/{registered_model_name}/{model_version}"

# Load model as a PyFuncModel
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Generate forecasts
loaded_model.predict(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Model on Databricks Model Serving

# COMMAND ----------

# With the token, you can create our authorization header for our subsequent REST calls
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Next you need an endpoint at which to execute your request which you can get from the notebook's tags collection
java_tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()

# This object comes from the Java CM - Convert the Java Map opject to a Python dictionary
tags = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(java_tags)

# Lastly, extract the Databricks instance (domain name) from the dictionary
instance = tags["browserHostName"]

# COMMAND ----------

import requests

model_serving_endpoint_name = "timesfm-1-200m"

my_json = {
    "name": model_serving_endpoint_name,
    "config": {
        "served_models": [
            {
                "model_name": registered_model_name,
                "model_version": model_version,
                "workload_type": "GPU_SMALL",
                "workload_size": "Small",
                "scale_to_zero_enabled": "true",
            }
        ],
        "auto_capture_config": {
            "catalog_name": catalog,
            "schema_name": db,
            "table_name_prefix": model_serving_endpoint_name,
        },
    },
}

# Make sure to drop the inference table of it exists
_ = spark.sql(
    f"DROP TABLE IF EXISTS {catalog}.{db}.`{model_serving_endpoint_name}_payload`"
)

# COMMAND ----------

def func_create_endpoint(model_serving_endpoint_name):
    # get endpoint status
    endpoint_url = f"https://{instance}/api/2.0/serving-endpoints"
    url = f"{endpoint_url}/{model_serving_endpoint_name}"
    r = requests.get(url, headers=headers)
    if "RESOURCE_DOES_NOT_EXIST" in r.text:
        print(
            "Creating this new endpoint: ",
            f"https://{instance}/serving-endpoints/{model_serving_endpoint_name}/invocations",
        )
        re = requests.post(endpoint_url, headers=headers, json=my_json)
    else:
        new_model_version = (my_json["config"])["served_models"][0]["model_version"]
        print(
            "This endpoint existed previously! We are updating it to a new config with new model version: ",
            new_model_version,
        )
        # update config
        url = f"{endpoint_url}/{model_serving_endpoint_name}/config"
        re = requests.put(url, headers=headers, json=my_json["config"])
        # wait till new config file in place
        import time, json

        # get endpoint status
        url = f"https://{instance}/api/2.0/serving-endpoints/{model_serving_endpoint_name}"
        retry = True
        total_wait = 0
        while retry:
            r = requests.get(url, headers=headers)
            assert (
                r.status_code == 200
            ), f"Expected an HTTP 200 response when accessing endpoint info, received {r.status_code}"
            endpoint = json.loads(r.text)
            if "pending_config" in endpoint.keys():
                seconds = 10
                print("New config still pending")
                if total_wait < 6000:
                    # if less the 10 mins waiting, keep waiting
                    print(f"Wait for {seconds} seconds")
                    print(f"Total waiting time so far: {total_wait} seconds")
                    time.sleep(10)
                    total_wait += seconds
                else:
                    print(f"Stopping,  waited for {total_wait} seconds")
                    retry = False
            else:
                print("New config in place now!")
                retry = False

    assert (
        re.status_code == 200
    ), f"Expected an HTTP 200 response, received {re.status_code}"


def func_delete_model_serving_endpoint(model_serving_endpoint_name):
    endpoint_url = f"https://{instance}/api/2.0/serving-endpoints"
    url = f"{endpoint_url}/{model_serving_endpoint_name}"
    response = requests.delete(url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status {response.status_code}, {response.text}"
        )
    else:
        print(model_serving_endpoint_name, "endpoint is deleted!")
    return response.json()

# COMMAND ----------

func_create_endpoint(model_serving_endpoint_name)

# COMMAND ----------

import time, mlflow


def wait_for_endpoint():
    endpoint_url = f"https://{instance}/api/2.0/serving-endpoints"
    while True:
        url = f"{endpoint_url}/{model_serving_endpoint_name}"
        response = requests.get(url, headers=headers)
        assert (
            response.status_code == 200
        ), f"Expected an HTTP 200 response, received {response.status_code}\n{response.text}"

        status = response.json().get("state", {}).get("ready", {})
        # print("status",status)
        if status == "READY":
            print(status)
            print("-" * 80)
            return
        else:
            print(f"Endpoint not ready ({status}), waiting 5 miutes")
            time.sleep(300)  # Wait 300 seconds


api_url = mlflow.utils.databricks_utils.get_webapp_url()

wait_for_endpoint()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online Forecast

# COMMAND ----------

import os
import requests
import pandas as pd
import json
import matplotlib.pyplot as plt

# Replace URL with the end point invocation url you get from Model Seriving page.
endpoint_url = f"https://{instance}/serving-endpoints/{model_serving_endpoint_name}/invocations"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
def forecast(input_data, url=endpoint_url, databricks_token=token):
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }
    body = {"inputs": input_data.tolist()}
    data = json.dumps(body)
    response = requests.request(method="POST", headers=headers, url=url, data=data)
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status {response.status_code}, {response.text}"
        )
    return response.json()

# COMMAND ----------

forecast(df)

# COMMAND ----------

#func_delete_model_serving_endpoint(model_serving_endpoint_name)
