# Databricks notebook source
# MAGIC %md # Prepara ambiente

# COMMAND ----------

import requests
import json

# COMMAND ----------

host = dbutils.library.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.library.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {'Authorization': f'Bearer {token}'}
catalog = 'vr_demo'
schema = 'crisp'

# COMMAND ----------

# MAGIC %md # Constulta o Unity Catalog via API REST

# COMMAND ----------

r = requests.request('GET', f'{host}/api/2.1/unity-catalog/tables?catalog_name={catalog}&schema_name={schema}', headers=headers)
j = r.json()
print(json.dumps(j, indent=4))

# COMMAND ----------

# MAGIC %md # Executar queries via API

# COMMAND ----------

data = {
  "warehouse_id": "475b94ddc7cd5211",
  "statement": "select * from vr_demo.crisp.sales_gold limit 10",
  "wait_timeout": "20s"
}

r = requests.request('POST', f'{host}/api/2.0/sql/statements', headers=headers, data=json.dumps(data))
j = r.json()
print(json.dumps(j, indent=4))
