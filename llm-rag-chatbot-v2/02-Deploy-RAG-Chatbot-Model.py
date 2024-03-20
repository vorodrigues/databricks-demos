# Databricks notebook source
# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-llm-rag-chatbot-victor_rodrigues` from the dropdown menu ([open cluster configuration](https://e2-demo-field-eng.cloud.databricks.com/#setting/clusters/1201-143005-dxhedgri/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('llm-rag-chatbot')` or re-install the demo: `dbdemos.install('llm-rag-chatbot')`*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # 2/ Creating the chatbot with Retrieval Augmented Generation (RAG)
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-flow-2.png?raw=true" style="float: right; margin-left: 10px"  width="900px;">
# MAGIC
# MAGIC Our Vector Search Index is now ready!
# MAGIC
# MAGIC Let's now create and deploy a new Model Serving Endpoint to perform RAG.
# MAGIC
# MAGIC The flow will be the following:
# MAGIC
# MAGIC - A user asks a question
# MAGIC - The question is sent to our serverless Chatbot RAG endpoint
# MAGIC - The endpoint compute the embeddings and searches for docs similar to the question, leveraging the Vector Search Index
# MAGIC - The endpoint creates a prompt enriched with the doc
# MAGIC - The prompt is sent to the Foundation Model Serving Endpoint
# MAGIC - We display the output to our users!
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=local&notebook=%2F02-Deploy-RAG-Chatbot-Model&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2F02-Deploy-RAG-Chatbot-Model&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC *Note: RAG performs document searches using Databricks Vector Search. In this notebook, we assume that the search index is ready for use. Make sure you run the previous [01-Data-Preparation-and-Index]($./01-Data-Preparation-and-Index [DO NOT EDIT]) notebook.*
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install the required libraries
# MAGIC %pip install "git+https://github.com/langchain-ai/langchain.git@master#subdirectory=libs/langchain" "git+https://github.com/mlflow/mlflow.git@gateway-migration" databricks-vectorsearch==0.21 databricks-sdk==0.12.0 mlflow[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $reset_all_data=false

# COMMAND ----------



# COMMAND ----------

VECTOR_SEARCH_ENDPOINT_NAME="dbdemos_vs_endpoint"

DATABRICKS_SITEMAP_URL = "https://docs.databricks.com/en/doc-sitemap.xml"

catalog = "vr_demo"

#email = spark.sql('select current_user() as user').collect()[0]['user']
#username = email.split('@')[0].replace('.', '_')
#dbName = db = f"rag_chatbot_{username}"
dbName = db = "chatbot2"

# COMMAND ----------

reset_all_data = False

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, length, pandas_udf
import os
import mlflow
from mlflow import MlflowClient

# COMMAND ----------

import re
min_required_version = "11.3"
version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
version_search = re.search('^([0-9]*\.[0-9]*)', version_tag)
assert version_search, f"The Databricks version can't be extracted from {version_tag}, shouldn't happen, please correct the regex"
current_version = float(version_search.group(1))
assert float(current_version) >= float(min_required_version), f'The Databricks version of the cluster must be >= {min_required_version}. Current version detected: {current_version}'

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  import requests
  xp_root_path = f"/dbdemos/experiments/{demo_name}"
  r = requests.post(f"{url}/api/2.0/workspace/mkdirs", headers = {"Accept": "application/json", "Authorization": f"Bearer {pat_token}"}, json={ "path": xp_root_path})
  mlflow.set_experiment(f"{xp_root_path}/{experiment_name}")

# COMMAND ----------

if reset_all_data:
  print(f'clearing up db {dbName}')
  spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")

# COMMAND ----------

def use_and_create_db(catalog, dbName, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  spark.sql(f"USE CATALOG `{catalog}`")
  spark.sql(f"""create database if not exists `{dbName}` """)

assert catalog not in ['hive_metastore', 'spark_catalog']
#If the catalog is defined, we force it to the given value and throw exception if not.
if len(catalog) > 0:
  current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
  if current_catalog != catalog:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs:
      spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
      if catalog == 'dbdemos':
        spark.sql(f"ALTER CATALOG {catalog} OWNER TO `account users`")
  use_and_create_db(catalog, dbName)

if catalog == 'dbdemos':
  try:
    spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{dbName} TO `account users`")
    spark.sql(f"ALTER SCHEMA {catalog}.{dbName} OWNER TO `account users`")
  except Exception as e:
    print("Couldn't grant access to the schema to all users:"+str(e))    

print(f"using catalog.database `{catalog}`.`{dbName}`")
spark.sql(f"""USE `{catalog}`.`{dbName}`""")    

# COMMAND ----------

# DBTITLE 1,Optional: Allowing Model Serving IPs
#If your workspace has ip access list, you need to allow your model serving endpoint to hit your AI gateway. Based on your region, IPs might change. Please reach out your Databrics Account team for more details.

# def allow_serverless_ip():
#   base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
#   headers = {"Authorization": f"Bearer {<Your PAT Token>}", "Content-Type": "application/json"}
#   return requests.post(f"{base_url}/api/2.0/ip-access-lists", json={"label": "serverless-model-serving", "list_type": "ALLOW", "ip_addresses": ["<IP RANGE>"], "enabled": "true"}, headers = headers).json()

# COMMAND ----------

# Helper function
def get_latest_model_version(model_name):
    mlflow_client = MlflowClient()
    latest_version = 1
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

# COMMAND ----------

# DBTITLE 1,endpoint
import time
def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    endpoint = vsc.get_endpoint(vs_endpoint_name)
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

# DBTITLE 1,index
def index_exists(vsc, endpoint_name, index_full_name):
    indexes = vsc.list_indexes(endpoint_name).get("vector_indexes", list())
    return any(index_full_name == index.get("name") for index in indexes)
    
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 20 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import StringType


def download_databricks_documentation_articles(max_documents=None):
    # Fetch the XML content from sitemap
    response = requests.get(DATABRICKS_SITEMAP_URL)
    root = ET.fromstring(response.content)

    # Find all 'loc' elements (URLs) in the XML
    urls = [loc.text for loc in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
    if max_documents:
        urls = urls[:max_documents]

    # Create DataFrame from URLs
    df_urls = spark.createDataFrame(urls, StringType()).toDF("url").repartition(10)

    # Pandas UDF to fetch HTML content for a batch of URLs
    @pandas_udf("string")
    def fetch_html_udf(urls: pd.Series) -> pd.Series:
        def fetch_html(url):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    return response.content
            except requests.RequestException:
                return None
            return None

        with ThreadPoolExecutor(max_workers=200) as executor:
            results = list(executor.map(fetch_html, urls))
        return pd.Series(results)

    # Pandas UDF to process HTML content and extract text
    @pandas_udf("string")
    def download_web_page_udf(html_contents: pd.Series) -> pd.Series:
        def extract_text(html_content):
            if html_content:
                soup = BeautifulSoup(html_content, "html.parser")
                article_div = soup.find("div", itemprop="articleBody")
                if article_div:
                    return str(article_div).strip()
            return None

        return html_contents.apply(extract_text)

    # Apply UDFs to DataFrame
    df_with_html = df_urls.withColumn("html_content", fetch_html_udf("url"))
    final_df = df_with_html.withColumn("text", download_web_page_udf("html_content"))

    # Select and filter non-null results
    final_df = final_df.select("url", "text").filter("text IS NOT NULL")
    if final_df.isEmpty():
      raise Exception("Dataframe is empty, couldn't download Databricks documentation, please check sitemap status.")

    return final_df

# COMMAND ----------

def display_gradio_app(space_name = "databricks-demos-chatbot"):
    displayHTML(f'''<div style="margin: auto; width: 1000px"><iframe src="https://{space_name}.hf.space" frameborder="0" width="1000" height="950" style="margin: auto"></iframe></div>''')

# COMMAND ----------

# DBTITLE 1,Temporary langchain wrapper - will be part of langchain soon
from typing import Any, Iterator, List, Optional

try:
    from langchain.pydantic_v1 import BaseModel
    from langchain.schema.embeddings import Embeddings
    from langchain.callbacks.manager import CallbackManagerForLLMRun
    from langchain.chat_models.base import SimpleChatModel
    from langchain.schema.messages import AIMessage, BaseMessage
    from langchain.adapters.openai import convert_message_to_dict
    import langchain


    class DatabricksEmbeddings(Embeddings):
        def __init__(self, model: str, host: str, **kwargs):
            super().__init__(**kwargs)
            self.model = model
            self.host = host

        def _query(self, texts: List[str]) -> List[List[float]]:
            os.environ['DATABRICKS_HOST'] = self.host
            def _chunk(texts: List[str], size: int) -> Iterator[List[str]]:
                for i in range(0, len(texts), size):
                    yield texts[i : i + size]

            from databricks_genai_inference import Embedding

            embeddings = []
            for txt in _chunk(texts, 20):
                response = Embedding.create(model=self.model, input=txt)
                embeddings.extend(response.embeddings)
            return embeddings

        def embed_documents(self, texts: List[str]) -> List[List[float]]:
            return self._query(texts)

        def embed_query(self, text: str) -> List[float]:
            return self._query([text])[0]


    class DatabricksChatModel(SimpleChatModel):
        model: str

        @property
        def _llm_type(self) -> str:
            return "databricks-chat-model"

        def _call(
            self,
            messages: List[BaseMessage],
            stop: Optional[List[str]] = None,
            run_manager: Optional[CallbackManagerForLLMRun] = None,
            **kwargs: Any,
        ) -> str:
            try:
                from databricks_genai_inference import ChatCompletion
            except ImportError as e:
                raise ImportError("message") from e

            messages_dicts = [convert_message_to_dict(m) for m in messages]

            response = ChatCompletion.create(
                model=self.model,
                messages=[
                    m
                    for m in messages_dicts
                    if m["role"] in {"system", "user", "assistant"} and m["content"]
                ],
                **kwargs,
            )
            return response.message
except:
    print('langchain not installed')

# COMMAND ----------

# DBTITLE 1,Cleanup utility to remove demo assets
def cleanup_demo(catalog, db, serving_endpoint_name, vs_index_fullname):
  vsc = VectorSearchClient()
  try:
    vsc.delete_index(endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME, index_name=vs_index_fullname)
  except Exception as e:
    print(f"can't delete index {VECTOR_SEARCH_ENDPOINT_NAME} {vs_index_fullname} - might not be existing: {e}")
  try:
    WorkspaceClient().serving_endpoints.delete(serving_endpoint_name)
  except Exception as e:
    print(f"can't delete serving endpoint {serving_endpoint_name} - might not be existing: {e}")
  spark.sql(f'DROP SCHEMA `{catalog}`.`{db}` CASCADE')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC ###  This demo requires a secret to work:
# MAGIC Your Model Serving Endpoint needs a secret to authenticate against your Vector Search Index (see [Documentation](https://docs.databricks.com/en/security/secrets/secrets.html)).  <br/>
# MAGIC - You'll need to setup the Databricks CLI on your laptop or using this cluster terminal: <br/>
# MAGIC `pip install databricks-cli` <br/>
# MAGIC - Configure the CLI. You'll need your workspace URL and a PAT token from your profile page<br>
# MAGIC `databricks configure`
# MAGIC - Create the dbdemos scope:<br/>
# MAGIC `databricks secrets create-scope dbdemos`
# MAGIC - Save your service principal secret. It will be used by the Model Endpoint to autenticate. If this is a demo/test, you can use one of your PAT token.<br>
# MAGIC `databricks secrets put-secret dbdemos rag_sp_token`
# MAGIC
# MAGIC *Note: Make sure your service principal has access to the Vector Search index:*
# MAGIC
# MAGIC ```
# MAGIC spark.sql('GRANT USAGE ON CATALOG <catalog> TO `<YOUR_SP>`');
# MAGIC spark.sql('GRANT USAGE ON DATABASE <catalog>.<db> TO `<YOUR_SP>`');
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC import databricks.sdk.service.catalog as c
# MAGIC WorkspaceClient().grants.update(c.SecurableType.TABLE, <index_name>, 
# MAGIC                                 changes=[c.PermissionsChange(add=[c.Privilege["SELECT"]], principal="<YOUR_SP>")])
# MAGIC   ```

# COMMAND ----------

# DBTITLE 1,Give your SP read access to your Vector Search Index
index_name=f"{catalog}.{db}.databricks_documentation_vs_index"

# Make sure you replace sp_name with the SP owning the token in the secret. It has be the principal in the PAT token used in the model
sp_name = spark.sql('select current_user() as user').collect()[0]['user'] #Set to current user if you are using your own personal PAT token for the demo
spark.sql(f'GRANT USAGE ON CATALOG {catalog} TO `{sp_name}`');
spark.sql(f'GRANT USAGE ON DATABASE {catalog}.{db} TO `{sp_name}`');
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c
WorkspaceClient().grants.update(c.SecurableType.TABLE, index_name, 
                                changes=[c.PermissionsChange(add=[c.Privilege["SELECT"]], principal=sp_name)])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Langchain retriever
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-model-1.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Let's start by building our Langchain retriever. 
# MAGIC
# MAGIC It will be in charge of:
# MAGIC
# MAGIC * Creating the input question embeddings (with Databricks `bge-large-en`)
# MAGIC * Calling the vector search index to find similar documents to augment the prompt with
# MAGIC
# MAGIC Databricks Langchain wrapper makes it easy to do in one step, handling all the underlying logic and API call for you.

# COMMAND ----------

# DBTITLE 1,Setup authentication for our model
# url used to send the request to your model from the serverless endpoint
host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get("dbdemos", "rag_vr_token")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from langchain.vectorstores import DatabricksVectorSearch
from langchain.embeddings import DatabricksEmbeddings

# Test embedding Langchain model
#NOTE: your question embedding model must match the one used in the chunk in the previous model 
embedding_model = DatabricksEmbeddings(endpoint="databricks-bge-large-en")
print(f"Test embeddings: {embedding_model.embed_query('What is Apache Spark?')[:20]}...")

def get_retriever(persist_dir: str = None):
    os.environ["DATABRICKS_HOST"] = host
    #Get the vector search index
    vsc = VectorSearchClient(workspace_url=host, personal_access_token=os.environ["DATABRICKS_TOKEN"])
    vs_index = vsc.get_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
        index_name=index_name
    )

    # Create the retriever
    vectorstore = DatabricksVectorSearch(
        vs_index, text_column="content", embedding=embedding_model
    )
    return vectorstore.as_retriever()


# test our retriever
vectorstore = get_retriever()
similar_documents = vectorstore.get_relevant_documents("How do I track my Databricks Billing?")
print(f"Relevant documents: {similar_documents[0]}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Building Databricks Chat Model to query llama-2-70b-chat foundation model
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-model-3.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Our chatbot will be using llama2 foundation model to provide answer. 
# MAGIC
# MAGIC While the model is available using the built-in [Foundation endpoint](/ml/endpoints) (using the `/serving-endpoints/databricks-llama-2-70b-chat/invocations` API), we can use Databricks Langchain Chat Model wrapper to easily build our chain.  
# MAGIC
# MAGIC Note: multipe type of endpoint or langchain models can be used:
# MAGIC
# MAGIC - Databricks Foundation models (what we'll use)
# MAGIC - Your fined-tune model
# MAGIC - An external model provider (such as Azure OpenAI)

# COMMAND ----------

# Test Databricks Foundation LLM model
from langchain.chat_models import ChatDatabricks
chat_model = ChatDatabricks(endpoint="databricks-llama-2-70b-chat", max_tokens = 200)
print(f"Test chat model: {chat_model.predict('What is Apache Spark')}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Assembling the complete RAG Chain
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-model-2.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC
# MAGIC Let's now merge the retriever and the model in a single Langchain chain.
# MAGIC
# MAGIC We will use a custom langchain template for our assistant to give proper answer.
# MAGIC
# MAGIC Make sure you take some time to try different templates and adjust your assistant tone and personality for your requirement.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain.chat_models import ChatDatabricks

TEMPLATE = """You are an assistant for Databricks users. You are answering python, coding, SQL, data engineering, spark, data science, DW and platform, API or infrastructure administration question related to Databricks. If the question is not related to one of these topics, kindly decline to answer. If you don't know the answer, just say that you don't know, don't try to make up an answer. Keep the answer as concise as possible.
Use the following pieces of context to answer the question at the end:
{context}
Question: {question}
Answer:
"""
prompt = PromptTemplate(template=TEMPLATE, input_variables=["context", "question"])

chain = RetrievalQA.from_chain_type(
    llm=chat_model,
    chain_type="stuff",
    retriever=get_retriever(),
    chain_type_kwargs={"prompt": prompt}
)

# COMMAND ----------

# DBTITLE 1,Let's try our chatbot in the notebook directly:
# langchain.debug = True #uncomment to see the chain details and the full prompt being sent
question = {"query": "How can I track billing usage on my workspaces?"}
answer = chain.run(question)
print(answer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving our model to Unity Catalog registry
# MAGIC
# MAGIC Now that our model is ready, we can register it within our Unity Catalog schema:

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
init_experiment_for_batch("chatbot-rag-llm", "simple")

# COMMAND ----------

# DBTITLE 1,Register our chain to MLFlow
from mlflow.models import infer_signature
import mlflow

mlflow.set_registry_uri("databricks-uc")
model_name = f"{catalog}.{db}.dbdemos_chatbot_model"

with mlflow.start_run(run_name="dbdemos_chatbot_rag") as run:
    signature = infer_signature(question, answer)
    model_info = mlflow.langchain.log_model(
        chain,
        loader_fn=get_retriever,  # Load the retriever with DATABRICKS_TOKEN env as secret (for authentication).
        artifact_path="chain",
        registered_model_name=model_name,
        pip_requirements=[
            "git+https://github.com/mlflow/mlflow.git@gateway-migration",
            "git+https://github.com/langchain-ai/langchain.git@master#subdirectory=libs/langchain",
            "databricks-vectorsearch"
        ],
        input_example=question,
        signature=signature
    )
    #------------------------
    # TODO: temporary fix to add the wheel, we won't need this after we switch to using PyPI
    import mlflow.models.utils
    mlflow.models.utils.add_libraries_to_model(
        f"models:/{model_name}/{get_latest_model_version(model_name)}"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Deploying our Chat Model as a Serverless Model Endpoint 
# MAGIC
# MAGIC Our model is saved in Unity Catalog. The last step is to deploy it as a Model Serving.
# MAGIC
# MAGIC We'll then be able to sending requests from our assistant frontend.

# COMMAND ----------

# Create or update serving endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput

serving_endpoint_name = f"dbdemos_endpoint_{catalog}_{db}"[:63]
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
                "DATABRICKS_TOKEN": "{{secrets/dbdemos/rag_vr_token}}",  # <scope>/<secret> that contains an access token
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

# MAGIC %md
# MAGIC Our endpoint is now deployed! You can search endpoint name on the [Serving Endpoint UI](#/mlflow/endpoints) and visualize its performance!
# MAGIC
# MAGIC Let's run a REST query to try it in Python. As you can see, we send the `test sentence` doc and it returns an embedding representing our document.

# COMMAND ----------

# DBTITLE 1,Let's try to send a query to our chatbot
question = "How can I track billing usage on my workspaces?"

answer = w.serving_endpoints.query(serving_endpoint_name, inputs=[{"query": question}])
print(answer.predictions[0])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Let's give it a try, using Gradio as UI!
# MAGIC
# MAGIC All you now have to do is deploy your chatbot UI. Here is a simple example using Gradio ([License](https://github.com/gradio-app/gradio/blob/main/LICENSE)). Explore the chatbot gradio [implementation](https://huggingface.co/spaces/databricks-demos/chatbot/blob/main/app.py).
# MAGIC
# MAGIC *Note: this UI is hosted and maintained by Databricks for demo purpose and don't use the model you just created. We'll soon show you how to do that with Lakehouse Apps!*

# COMMAND ----------

display_gradio_app("databricks-demos-chatbot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! You have deployed your first GenAI RAG model!
# MAGIC
# MAGIC You're now ready to deploy the same logic for your internal knowledge base leveraging Lakehouse AI.
# MAGIC
# MAGIC We've seen how the Lakehouse AI is uniquely positioned to help you solve your GenAI challenge:
# MAGIC
# MAGIC - Simplify Data Ingestion and preparation with Databricks Engineering Capabilities
# MAGIC - Accelerate Vector Search  deployment with fully managed indexes
# MAGIC - Leverage Databricks LLama 2 foundation model endpoint
# MAGIC - Deploy realtime model endpoint to perform RAG and provide Q&A capabilities
# MAGIC
# MAGIC Lakehouse AI is uniquely positioned to accelerate your GenAI deployment.

# COMMAND ----------

# MAGIC %md # Cleanup
# MAGIC
# MAGIC To free up resources, please delete uncomment and run the below cell.

# COMMAND ----------

# /!\ THIS WILL DROP YOUR DEMO SCHEMA ENTIRELY /!\ 
# cleanup_demo(catalog, db, serving_endpoint_name, f"{catalog}.{db}.databricks_documentation_vs_index")
