# Databricks notebook source
# MAGIC %md # New

# COMMAND ----------

# MAGIC %md ## Pyfunc for Prompt Engineering

# COMMAND ----------

token = dbutils.secrets.get('dbdemos', 'rag_vr_token')

# COMMAND ----------

class ChatbotRAG(mlflow.pyfunc.PythonModel):

  def __init__(self, model):
    self.chain = model

  def predict(self, context, model_input):
    question = model_input['prompt'][0]
    answer = self.chain.run(question)
    return {'candidates' : [answer]}

# COMMAND ----------

chatbot = ChatbotRAG(chain)

# COMMAND ----------

question = {"prompt": "How can I track billing usage on my workspaces?", "temperature":0.1, "max_tokens":100}

# COMMAND ----------

chatbot.predict(model_input=question, context=None)

# COMMAND ----------

model_name = 'vr_dbdocs_chatbot'

# COMMAND ----------

from mlflow.models.signature import ModelSignature
from mlflow.types import DataType, Schema, ColSpec

with mlflow.start_run(run_name=model_name) as run:
  
  input_schema = Schema([
    ColSpec(DataType.string, "prompt"), 
    ColSpec(DataType.double, "temperature"), 
    ColSpec(DataType.long, "max_tokens")])
  output_schema = Schema([ColSpec(DataType.string)])
  signature = ModelSignature(inputs=input_schema, outputs=output_schema)
  
  mlflow.pyfunc.log_model(
    python_model=chatbot, 
    artifact_path='model',
    registered_model_name=model_name, 
    signature=signature, 
    pip_requirements=[
            "git+https://github.com/mlflow/mlflow.git@gateway-migration",
            "git+https://github.com/langchain-ai/langchain.git@master#subdirectory=libs/langchain",
            "databricks-vectorsearch"
    ], 
    input_example=question)
  
  import mlflow.models.utils
  v = get_latest_model_version(model_name)
  mlflow.models.utils.add_libraries_to_model(f"models:/{model_name}/{v}")


# COMMAND ----------

# Create or update serving endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput

serving_endpoint_name = model_name
latest_model_version = get_latest_model_version(model_name)

w = WorkspaceClient()
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_models=[
        ServedModelInput(
            model_name=model_name,
            model_version=latest_model_version,
            workload_size="Small",
            scale_to_zero_enabled=True,
            environment_vars={
                "DATABRICKS_HOST": "{{secrets/dbdemos/rag_vr_host}}",
                "DATABRICKS_TOKEN": "{{secrets/dbdemos/rag_vr_token}}"  # <scope>/<secret> that contains an access token
            }
        )
    ]
)

existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), None
)
if existing_endpoint == None:
    print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config)
else:
    print(f"Updating the endpoint {serving_endpoint_name} to version {latest_model_version}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.update_config_and_wait(served_models=endpoint_config.served_models, name=serving_endpoint_name)

# COMMAND ----------

question = "How can I track billing usage on my workspaces?"

w = WorkspaceClient()
answer = w.serving_endpoints.query(serving_endpoint_name, inputs={"prompt": [question], "temperature": [0.1], "max_tokens": [1000]})
print(answer.predictions['candidates'][0])

# COMMAND ----------

# MAGIC %md ## AI Gateway

# COMMAND ----------

ai_gateway_route_name = model_name

databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

mlflow.gateway.create_route(
    name=ai_gateway_route_name,
    route_type="llm/v1/completions",
    model= {
        "name": serving_endpoint_name,
        "provider": "databricks-model-serving",
        "databricks_model_serving_config": {
          "databricks_api_token": token,
          "databricks_workspace_url": databricks_url
        }
    }
)

# COMMAND ----------

import mlflow

try:
    response = mlflow.gateway.query(
        route=ai_gateway_route_name,
        data={"prompt": """Responda a pergunta seguinte em Português do Brasil: Como posso rastrear os custos dos meus workspaces? """, "temperature": 0.3, "max_tokens": 512}
)
    print(response['candidates'][0]['text'])
except Exception as e:
    print(e)

# COMMAND ----------

mlflow.gateway.delete_route(ai_gateway_route_name)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Pyfunc for AI_QUERY

# COMMAND ----------

token = dbutils.secrets.get('dbdemos', 'rag_vr_token')

# COMMAND ----------

class Chatbot(mlflow.pyfunc.PythonModel):

  def __init__(self, model):
    self.chain = model

  def predict(self, context, model_input):
    question = model_input['prompt'][0]
    answer = self.chain.run(question)
    return [answer]

# COMMAND ----------

chatbot = Chatbot(chain)

# COMMAND ----------

question = {"prompt": "How can I track billing usage on my workspaces?", "temperature":0.1, "max_tokens":100}

# COMMAND ----------

answer = chatbot.predict(model_input=question, context=None)
print(answer)

# COMMAND ----------

from mlflow.models.signature import ModelSignature
from mlflow.types import DataType, Schema, ColSpec
# from mlflow.models import infer_signature
# import numpy as np

with mlflow.start_run(run_name='dbdemos_chatbot_rag3') as run:
  
  input_schema = Schema([
    ColSpec(DataType.string, "prompt"), 
    ColSpec(DataType.double, "temperature"), 
    ColSpec(DataType.long, "max_tokens")])
  output_schema = Schema([ColSpec(DataType.string)])
  signature = ModelSignature(inputs=input_schema, outputs=output_schema)

  # signature = infer_signature(np.array(list(question.items())), answer)
  
  mlflow.pyfunc.log_model(
    python_model=chatbot, 
    artifact_path='model',
    registered_model_name=model_name+'3', 
    signature=signature, 
    pip_requirements=[
            "git+https://github.com/mlflow/mlflow.git@gateway-migration",
            "git+https://github.com/langchain-ai/langchain.git@master#subdirectory=libs/langchain",
            "databricks-vectorsearch"
    ], 
    input_example=question)
  
  import mlflow.models.utils
  v = get_latest_model_version(model_name+'3')
  mlflow.models.utils.add_libraries_to_model(f"models:/{model_name}3/{v}")


# COMMAND ----------

# Create or update serving endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput

serving_endpoint_name = f"dbdemos_endpoint_{catalog}_{db}_query"[:63]
latest_model_version = get_latest_model_version(model_name+'3')

w = WorkspaceClient()
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_models=[
        ServedModelInput(
            model_name=model_name+'3',
            model_version=latest_model_version,
            workload_size="Small",
            scale_to_zero_enabled=True,
            environment_vars={
                "DATABRICKS_HOST": "{{secrets/dbdemos/rag_vr_host}}",
                "DATABRICKS_TOKEN": "{{secrets/dbdemos/rag_vr_token}}"  # <scope>/<secret> that contains an access token
            }
        )
    ]
)

existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), None
)
if existing_endpoint == None:
    print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config)
else:
    print(f"Updating the endpoint {serving_endpoint_name} to version {latest_model_version}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.update_config_and_wait(served_models=endpoint_config.served_models, name=serving_endpoint_name)

# COMMAND ----------

w = WorkspaceClient()
answer = w.serving_endpoints.query(serving_endpoint_name, inputs={"prompt": [question['prompt']], "temperature": [0.1], "max_tokens": [1000]})
print(answer.predictions)
