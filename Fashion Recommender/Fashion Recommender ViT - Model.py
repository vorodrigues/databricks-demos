# Databricks notebook source
# MAGIC %md # Prepara ambiente

# COMMAND ----------

# MAGIC %pip install transformers torch

# COMMAND ----------

import mlflow
from transformers import ViTImageProcessor, ViTModel
from PIL import Image
import io
import codecs

import requests

# COMMAND ----------

# MAGIC %md # Prepara Wrapper

# COMMAND ----------

url = 'http://images.cocodataset.org/val2017/000000039769.jpg'
image = Image.open(requests.get(url, stream=True).raw)

processor = ViTImageProcessor.from_pretrained('google/vit-base-patch16-224-in21k')
model = ViTModel.from_pretrained('google/vit-base-patch16-224-in21k')

# COMMAND ----------

class VitWrapper(mlflow.pyfunc.PythonModel):
  
  def __init__(self, processor, model):
    self.processor = processor
    self.model = model
    
  def predict(self, context, model_input):

    print(model_input)

    # Extrai o conteúdo da imagem
    img_str = model_input[0][0]

    # Prepara a imagem
    try:
      image = Image.open(io.BytesIO(codecs.decode(codecs.encode(img_str, "utf-8"), 'base64')))
    except Exception:
      raise Exception(str(model_input))

    # Aplica o modelo
    inputs = processor(images=image, return_tensors="pt")
    outputs = model(**inputs)
    embeddings = outputs.pooler_output[0].detach().numpy().tolist() # alternativa: last_hidden_state

    # Retorna os embeddings
    return embeddings

# COMMAND ----------

# MAGIC %md # Testa Wrapper

# COMMAND ----------

df = spark.read.format("binaryFile").load("/Volumes/vr_demo/fashion_recommender/img")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, lit
from helpers.resize import resize_image_udf

# Redimensiona todas as imagens
df_resized = (df
    .withColumn("content_large", resize_image_udf(col("content"), lit(700), lit(700)))
    .withColumn("content", resize_image_udf(col("content"), lit(128), lit(128))))

# COMMAND ----------

img_str = df_resized.collect()[5]['content']
print(img_str)
print(type(img_str))

# COMMAND ----------

vit = VitWrapper(processor, model)

# COMMAND ----------

vit.predict(context=None, model_input=[[img_str]])

# COMMAND ----------

# MAGIC %md # Registra Modelo

# COMMAND ----------

import mlflow

with mlflow.start_run(run_name='ViT Embeddings Model') as run:
  mlflow.pyfunc.log_model(
    python_model=vit, 
    artifact_path='model',
    input_example= [[img_str]],
    registered_model_name='vr_demo.fashion_recommender.vit_embeddings'
    # signature
  )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md # Cria um daframe de imagens

# COMMAND ----------

df = spark.read.format("binaryFile").load("/Volumes/vr_demo/fashion_recommender/img")
display(df)

# COMMAND ----------

# MAGIC %md # Redimensiona as imagens

# COMMAND ----------

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import BinaryType
from PIL import Image
import io

# Define a UDF to resize images
def resize_image(data, width, height):
    image = Image.open(io.BytesIO(data))
    resized_image = image.resize((width, height))
    byte_array = io.BytesIO()
    resized_image.save(byte_array, format='PNG')
    return byte_array.getvalue()

# Register the UDF
resize_image_udf = udf(resize_image, BinaryType())

# Apply the UDF to the DataFrame
df_resized = df.withColumn("content", resize_image_udf(df["content"], lit(128), lit(128)))

display(df_resized)

# COMMAND ----------

# MAGIC %md # Salva imagens em uma tabela Delta

# COMMAND ----------

df_resized.write.saveAsTable('vr_demo.fashion_recommender.images', mode="overwrite")

# COMMAND ----------

# MAGIC %sql create or replace table vr_demo.fashion_recommender.images_str as select
# MAGIC   * except (content),
# MAGIC   cast(content as string) as content 
# MAGIC from vr_demo.fashion_recommender.images

# COMMAND ----------

# MAGIC %sql ALTER TABLE vr_demo.fashion_recommender.images_str SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md # Extrai os embeddings de imagem

# COMMAND ----------

# MAGIC %md ## Teste

# COMMAND ----------

import mlflow
from transformers import ViTImageProcessor, ViTModel
from PIL import Image
import requests

# COMMAND ----------

inputs = processor(images=image, return_tensors="pt")
outputs = model(**inputs)
last_hidden_states = outputs.last_hidden_state
display(last_hidden_states)

# COMMAND ----------

# MAGIC %md ## Cria UDF para extração de embeddings

# COMMAND ----------

b = df.limit(1).collect()[0]['content']

# COMMAND ----------

image = Image.open(io.BytesIO(b))

# COMMAND ----------

processor = ViTImageProcessor.from_pretrained('google/vit-base-patch16-224-in21k')
model = ViTModel.from_pretrained('google/vit-base-patch16-224-in21k')

# COMMAND ----------

inputs = processor(images=image, return_tensors="pt")
outputs = model(**inputs)
last_hidden_states = outputs.last_hidden_state
display(last_hidden_states)

# COMMAND ----------

model_input = spark.table('vr_demo.fashion_recommender.images').limit(1).toPandas()

# COMMAND ----------

print(type(model_input['content'][0]))

# COMMAND ----------

embeddings = last_hidden_states.detach().numpy()
display(embeddings.shape)

# COMMAND ----------

last_hidden_states.shape

# COMMAND ----------

outputs.last_hidden_state.shape

# COMMAND ----------

outputs.pooler_output.shape

# COMMAND ----------

# MAGIC %md # Consulta Vector Search

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
index = vsc.get_index('one-env-shared-endpoint-9', 'vr_demo.fashion_recommender.images_vs_index')

# COMMAND ----------

row = spark.table('vr_demo.fashion_recommender.images_str').where('path like "%pants02.png"').collect()
img_str = row[0]['content']
display(row)

# COMMAND ----------

results = index.similarity_search(
    query_text=img_str,
    columns=["path"],
    num_results=3
)
display(results)
