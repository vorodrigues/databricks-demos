# Databricks notebook source
# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-llm-rag-chatbot-victor_rodrigues` from the dropdown menu ([open cluster configuration](https://e2-demo-field-eng.cloud.databricks.com/#setting/clusters/1201-143005-dxhedgri/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('llm-rag-chatbot')` or re-install the demo: `dbdemos.install('llm-rag-chatbot')`*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # 1/ Data preparation for LLM Chatbot RAG
# MAGIC
# MAGIC ## Building and indexing our knowledge base into Databricks Vector Search
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-flow-1.png?raw=true" style="float: right; width: 800px; margin-left: 10px">
# MAGIC
# MAGIC In this notebook, we'll ingest our documentation pages and index them with a Vector Search index to help our chatbot provide better answers.
# MAGIC
# MAGIC Preparing high quality data is key for your chatbot performance. We recommend taking time to implement these next steps with your own dataset.
# MAGIC
# MAGIC Thankfully, Lakehouse AI provides state of the art solutions to accelerate your AI and LLM projects, and also simplifies data ingestion and preparation at scale.
# MAGIC
# MAGIC For this example, we will use Databricks documentation from [docs.databricks.com](docs.databricks.com):
# MAGIC - Download the web pages
# MAGIC - Split the pages in small chunks of text
# MAGIC - Compute the embeddings using a Databricks Foundation model as part of our Delta Table
# MAGIC - Create a Vector Search index based on our Delta Table  
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=local&notebook=%2F01-Data-Preparation-and-Index&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2F01-Data-Preparation-and-Index&version=1">

# COMMAND ----------

# DBTITLE 1,Install required external libraries 
# MAGIC %pip install "git+https://github.com/mlflow/mlflow.git@gateway-migration" lxml==4.9.3 transformers==4.30.2 langchain==0.0.342 databricks-vectorsearch==0.21
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Init our resources and catalog
# MAGIC %run ./_resources/00-init $reset_all_data=false

# COMMAND ----------



# COMMAND ----------



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



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Extracting Databricks documentation sitemap and pages
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-prep-1.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC First, let's create our raw dataset as a Delta Lake table.
# MAGIC
# MAGIC For this demo, we will directly download a few documentation pages from `docs.databricks.com` and save the HTML content.
# MAGIC
# MAGIC Here are the main steps:
# MAGIC
# MAGIC - Run a quick script to extract the page URLs from the `sitemap.xml` file
# MAGIC - Download the web pages
# MAGIC - Use BeautifulSoup to extract the ArticleBody
# MAGIC - Save the HTML results in a Delta Lake table

# COMMAND ----------

if not spark.catalog.tableExists("raw_documentation") or spark.table("raw_documentation").isEmpty():
    # Download Databricks documentation to a DataFrame (see _resources/00-init for more details)
    doc_articles = download_databricks_documentation_articles()
    #Save them as a raw_documentation table
    doc_articles.write.mode('overwrite').saveAsTable("raw_documentation")

display(spark.table("raw_documentation").limit(2))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Splitting documentation pages into small chunks
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-prep-2.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC LLM models typically have a maximum input context length, and you won't be able to compute embeddings for very long texts.
# MAGIC In addition, the longer your context length is, the longer it will take for the model to provide a response.
# MAGIC
# MAGIC Document preparation is key for your model to perform well, and multiple strategies exist depending on your dataset:
# MAGIC
# MAGIC - Split document into small chunks (paragraph, h2...)
# MAGIC - Truncate documents to a fixed length
# MAGIC - The chunk size depends on your content and how you'll be using it to craft your prompt. Adding multiple small doc chunks in your prompt might give different results than sending only a big one
# MAGIC - Split into big chunks and ask a model to summarize each chunk as a one-off job, for faster live inference
# MAGIC - Create multiple agents to evaluate each bigger document in parallel, and ask a final agent to craft your answer...
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Splitting our big documentation pages in smaller chunks (h2 sections)
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/chunk-window-size.png?raw=true" style="float: right" width="700px">
# MAGIC <br/>
# MAGIC In this demo, we have big documentation articles, which are too long for the prompt to our model. 
# MAGIC
# MAGIC We won't be able to use multiple documents as RAG context as they would exceed our max input size. Some recent studies also suggest that bigger window size isn't always better, as the LLMs seem to focus on the beginning and end of your prompt.
# MAGIC
# MAGIC In our case, we'll split these articles between HTML `h2` tags, remove HTML and ensure that each chunk is less than 500 tokens using LangChain. 
# MAGIC
# MAGIC #### LLM Window size and Tokenizer
# MAGIC
# MAGIC The same sentence might return different tokens for different models. LLMs are shipped with a `Tokenizer` that you can use to count tokens for a given sentence (usually more than the number of words) (see [Hugging Face documentation](https://huggingface.co/docs/transformers/main/tokenizer_summary) or [OpenAI](https://github.com/openai/tiktoken))
# MAGIC
# MAGIC Make sure the tokenizer you'll be using here matches your model. We'll be using the `transformers` library to count llama2 tokens with its tokenizer. This will also keep our document token size below our embedding max size (1024).
# MAGIC
# MAGIC <br/>
# MAGIC <br style="clear: both">
# MAGIC <div style="background-color: #def2ff; padding: 15px;  border-radius: 30px; ">
# MAGIC   <strong>Information</strong><br/>
# MAGIC   Remember that the following steps are specific to your dataset. This is a critical part to building a successful RAG assistant.
# MAGIC   <br/> Always take time to manually review the chunks created and ensure that they make sense and contain relevant information.
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Splitting our html pages in smaller chunks
from langchain.text_splitter import HTMLHeaderTextSplitter, RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

max_chunk_size = 500

tokenizer = AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")
text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(tokenizer, chunk_size=max_chunk_size, chunk_overlap=50)
html_splitter = HTMLHeaderTextSplitter(headers_to_split_on=[("h2", "header2")])

# Split on H2, but merge small h2 chunks together to avoid too small. 
def split_html_on_h2(html, min_chunk_size = 20, max_chunk_size=500):
  h2_chunks = html_splitter.split_text(html)
  chunks = []
  previous_chunk = ""
  # Merge chunks together to add text before h2 and avoid too small docs.
  for c in h2_chunks:
    # Concat the h2 (note: we could remove the previous chunk to avoid duplicate h2)
    content = c.metadata.get('header2', "") + "\n" + c.page_content
    if len(tokenizer.encode(previous_chunk + content)) <= max_chunk_size/2:
        previous_chunk += content + "\n"
    else:
        chunks.extend(text_splitter.split_text(previous_chunk.strip()))
        previous_chunk = content + "\n"
  if previous_chunk:
      chunks.extend(text_splitter.split_text(previous_chunk.strip()))
  # Discard too small chunks
  return [c for c in chunks if len(tokenizer.encode(c)) > min_chunk_size]

# Let's create a user-defined function (UDF) to chunk all our documents with spark
@pandas_udf("array<string>")
def parse_and_split(docs: pd.Series) -> pd.Series:
    return docs.apply(split_html_on_h2)
  
# Let's try our chunking function
html = spark.table("raw_documentation").limit(1).collect()[0]['text']
split_html_on_h2(html)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What's required for our Vector Search Index
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/databricks-vector-search-type.png?raw=true" style="float: right" width="800px">
# MAGIC
# MAGIC Databricks provides multiple types of vector search indexes:
# MAGIC
# MAGIC - **Managed embeddings**: you provide a text column and endpoint name and Databricks synchronizes the index with your Delta table 
# MAGIC - **Self Managed embeddings**: you compute the embeddings and save them as a field of your Delta Table, Databricks will then synchronize the index
# MAGIC - **Direct index**: when you want to use and update the index without having a Delta Table
# MAGIC
# MAGIC In this demo, we will show you how to setup a **Self-managed Embeddings** index. 
# MAGIC
# MAGIC To do so, we will have to first compute the embeddings of our chunks and save them as a Delta Lake table field as `array&ltfloat&gt`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Introducing Databricks BGE Embeddings Foundation Model endpoints
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-prep-4.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC Foundation Models are provided by Databricks, and can be used out-of-the-box.
# MAGIC
# MAGIC Databricks supports several endpoint types to compute embeddings or evaluate a model:
# MAGIC - A **foundation model endpoint**, provided by Databricks (ex: llama2-70B, MPT...)
# MAGIC - An **external endpoint**, acting as a gateway to an external model (ex: Azure OpenAI)
# MAGIC - A **custom**, fined-tuned model hosted on Databricks model service
# MAGIC
# MAGIC Open the [Model Serving Endpoint page](/ml/endpoints) to explore and try the foundation models.
# MAGIC
# MAGIC For this demo, we will use the foundation model `BGE` (embeddings) and `llama2-70B` (chat). <br/><br/>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/databricks-foundation-models.png?raw=true" width="600px" >

# COMMAND ----------

# DBTITLE 1,Using Databricks Foundation model BGE as an embedding endpoint
import mlflow.deployments
deploy_client = mlflow.deployments.get_deploy_client("databricks")

response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": ["What is Apache Spark?"]})
embeddings = [e['embedding'] for e in response.data]
print(embeddings)
# NOTE: if you change your embedding model here, make sure you change it in the query step too

# COMMAND ----------

# DBTITLE 1,Create the final databricks_documentation table containing chunks
# MAGIC %sql
# MAGIC --Note that we need to enable Change Data Feed on the table to create the index
# MAGIC CREATE TABLE IF NOT EXISTS databricks_documentation (
# MAGIC   id BIGINT GENERATED BY DEFAULT AS IDENTITY,
# MAGIC   url STRING,
# MAGIC   content STRING,
# MAGIC   embedding ARRAY <FLOAT>
# MAGIC ) TBLPROPERTIES (delta.enableChangeDataFeed = true); 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Computing the chunk embeddings and saving them to our Delta Table
# MAGIC
# MAGIC The last step is to compute an embedding for all our documentation chunks. Let's create a udf to compute the embeddings using the foundation model endpoint.
# MAGIC
# MAGIC *Note that this part would typically be setup as a production-grade job, running as soon as a new documentation page is updated. <br/> This could be setup as a Delta Live Table pipeline to incrementally consume updates.*

# COMMAND ----------

import mlflow.deployments
deploy_client = mlflow.deployments.get_deploy_client("databricks")

@pandas_udf("array<float>")
def get_embedding(contents: pd.Series) -> pd.Series:
    def get_embeddings(batch):
        #Note: this will gracefully fail if an exception is thrown during embedding creation (add try/except if needed) 
        response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": batch})
        return [e['embedding'] for e in response.data]

    # Splitting the contents into batches of 150 items each, since the embedding model takes at most 150 inputs per request.
    max_batch_size = 150
    batches = [contents.iloc[i:i + max_batch_size] for i in range(0, len(contents), max_batch_size)]

    # Process each batch and collect the results
    all_embeddings = []
    for batch in batches:
        all_embeddings += get_embeddings(batch.tolist())

    return pd.Series(all_embeddings)

# COMMAND ----------

(spark.table("raw_documentation")
      .withColumn('content', F.explode(parse_and_split('text')))
      .withColumn('embedding', get_embedding('content'))
      .drop("text")
      .write.mode('overwrite').saveAsTable("databricks_documentation"))

display(spark.table("databricks_documentation"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Create Vector Search Index
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-prep-3.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC Our dataset is now ready. We chunked the documentation page into small sections, computed the embeddings and saved it as a Delta Lake table.
# MAGIC
# MAGIC Next, we'll configure Databricks Vector Search to ingest data from this table.
# MAGIC
# MAGIC Vector search index uses a Vector search endpoint to serve the embeddings (you can think about it as your Vector Search API endpoint). <br/>
# MAGIC Multiple Indexes can use the same endpoint. Let's start by creating one.

# COMMAND ----------

# DBTITLE 1,Creating the Vector Search endpoint
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if VECTOR_SEARCH_ENDPOINT_NAME not in [e['name'] for e in vsc.list_endpoints()['endpoints']]:
    print('Creating Vector Search endpoint...')
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")
else:
    print('Vector Search endpoint already exists!')

wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC You can view your endpoint on the [Vector Search Endpoints UI](#/setting/clusters/vector-search). Click on the endpoint name to see all indexes that are served by the endpoint.

# COMMAND ----------

# DBTITLE 1,Create the Self-managed vector search using our endpoint
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = f"{catalog}.{db}.databricks_documentation"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.databricks_documentation_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_dimension=1024, #Match your model embedding size (bge)
    embedding_vector_column="embedding"
  )


#Let's wait for the index to be ready and all our embeddings to be created and indexed
wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Searching for similar content
# MAGIC
# MAGIC That's all we have to do. Databricks will automatically capture and synchronize new entries in your Delta Live Table.
# MAGIC
# MAGIC Note that depending on your dataset size and model size, index creation can take a few seconds to start and index your embeddings.
# MAGIC
# MAGIC Let's give it a try and search for similar content.
# MAGIC
# MAGIC *Note: `similarity_search` also support a filters parameter. This is useful to add a security layer to your RAG system: you can filter out some sensitive content based on who is doing the call (for example filter on a specific department based on the user preference).*

# COMMAND ----------

import mlflow.deployments
deploy_client = mlflow.deployments.get_deploy_client("databricks")

question = "How can I track billing usage on my workspaces?"
response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": [question]})
embeddings = [e['embedding'] for e in response.data]

results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
  query_vector=embeddings[0],
  columns=["url", "content"],
  num_results=1)
docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next step: Deploy our chatbot model with RAG
# MAGIC
# MAGIC We've seen how Databricks Lakehouse AI makes it easy to ingest and prepare your documents, and deploy a Vector Search index on top of it with just a few lines of code and configuration.
# MAGIC
# MAGIC This simplifies and accelerates your data projects so that you can focus on the next step: creating your real-time chatbot endpoint with well-crafted prompt augmentation.
# MAGIC
# MAGIC Open the [02-Deploy-RAG-Chatbot-Model]($./02-Deploy-RAG-Chatbot-Model) notebook to create and deploy a chatbot endpoint.
