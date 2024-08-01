# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md-sandbox # Recomendação de Ofertas Baseada em Imagens
# MAGIC
# MAGIC <img style="float:right" src="https://github.com/vorodrigues/databricks-demos/blob/dev/Fashion%20Recommender/img/vs.png?raw=true" width="700"/>
# MAGIC
# MAGIC Encontrar uma peça de vestuário em um site de *ecommerce* pode ser bastante trabalhoso e frustante para os clientes. Normalmente, temos algumas opções para filtrar os produtos existentes e/ou ferramentas de busca textual simples, mas ainda assim não é o suficiente para trazer resultados relevantes e customizados para o estilo e necessidade momentânea de cada um.
# MAGIC
# MAGIC Por isso, uma busca baseada em imagens pode ajudar a descoberta de produtos pelos clientes de forma muito mais rápida e eficiente e, consequentemente, ajudar na conversão de vendas e fidelização.
# MAGIC
# MAGIC Vamos demonstrar como o Databricks Vector Search pode ser utilizado para indexar imagens de produtos e ser utilizado por aplicações transacionais para consultar produtos similares em tempo real.

# COMMAND ----------

# MAGIC %md ## Acessa as imagens armazendas

# COMMAND ----------

df = spark.read.format("binaryFile").load("/Volumes/vr_demo/fashion_recommender/img")
display(df)

# COMMAND ----------

# MAGIC %md ## Redimensiona as imagens

# COMMAND ----------

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType
from PIL import Image
import io
import codecs

# Define a UDF para redimensionar as imagens
def resize_image(data, width, height):
    image = Image.open(io.BytesIO(data))
    resized_image = image.resize((width, height))
    byte_array = io.BytesIO()
    resized_image.save(byte_array, format='PNG')
    return codecs.decode(codecs.encode(byte_array.getvalue(), "base64"), 'utf-8')

# Registra a UDF
resize_image_udf = udf(resize_image, StringType())

# Aplica a UDF ao DataFrame
df_resized = df \
    .withColumn("content", resize_image_udf(df["content"], lit(128), lit(128)))

display(df_resized)

# COMMAND ----------

# MAGIC %md ## Salva imagens em uma tabela Delta

# COMMAND ----------

df_resized.write.saveAsTable('vr_demo.fashion_recommender.images', mode="overwrite")

# COMMAND ----------

# MAGIC %sql ALTER TABLE vr_demo.fashion_recommender.images SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md ## Cria um índice do Vector Search
# MAGIC
# MAGIC Criar um índice do Vector Search é muito simples! Basta seguir os passos abaixo:
# MAGIC
# MAGIC 1. Busque a tabela desejada no Catálogo
# MAGIC 1. Clique no botão **`Criar`**
# MAGIC 1. Selecione **`Vector Search Index`**
# MAGIC 1. Preencha as informações no formulário
# MAGIC 1. Clique no botão **`Criar`** e pronto!

# COMMAND ----------

# MAGIC %md ## Busca imagens similares no Vector Search

# COMMAND ----------

# Cria um cliente do Vector Search
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
index = vsc.get_index('one-env-shared-endpoint-9', 'vr_demo.fashion_recommender.images_vs_index')

# COMMAND ----------

# Carrega o conteúdo da imagem desejada
img_str = (spark.table('vr_demo.fashion_recommender.images')
    .where('path like "%pants01.png"')
    .collect()[0]['content'])

# Busca as 3 imagens com maior similaridade no índice do Vector Search
results = index.similarity_search(
    query_text=img_str,
    columns=["path"],
    num_results=3
)

# Prepara os resultados para utilização na consulta
paths = ', '.join([f"'{result[0]}'" for result in results['result']['data_array']])

display(results)

# COMMAND ----------

# Exibe as imagens encontradas
display(df.where(f"path in ({paths})"))

# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import BinaryType
from PIL import Image
import io
import codecs

# Define a UDF para redimensionar as imagens
def string_to_bytes(data):
    return codecs.decode(codecs.encode(data, "utf-8"), 'base64')

# Registra a UDF
string_to_bytes_udf = udf(string_to_bytes, BinaryType())

# Aplica a UDF ao DataFrame
df_bin = spark.table('vr_demo.fashion_recommender.images') \
    .withColumn("content_bin", string_to_bytes_udf(col("content")))

display(df_bin)
