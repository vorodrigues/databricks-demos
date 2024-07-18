# Databricks notebook source
import os
import requests
import numpy as np
import pandas as pd
import json

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def create_tf_serving_json(data):
    return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
    url = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/dbdemos_endpoint_vr_demo_chatbot3/invocations'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
    data_json = json.dumps(ds_dict, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

# COMMAND ----------

question = "Onde posso acompanhar meu consumo no Databricks?"

data = {'query': ['Responda em Português: O que é Databricks?']}

score_model(pd.DataFrame(data))
