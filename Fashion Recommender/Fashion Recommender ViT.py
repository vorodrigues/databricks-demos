# Databricks notebook source
# MAGIC %md-sandbox # Recomendação de Ofertas Baseada em Imagens
# MAGIC
# MAGIC <img style="float:right" src="https://github.com/vorodrigues/databricks-demos/blob/dev/Fashion%20Recommender/img/vs.png?raw=true" width="800"/>
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

from pyspark.sql.functions import col, lit
from helpers.resize import resize_image_udf

# Redimensiona todas as imagens
df_resized = (df
    .withColumn("content_large", resize_image_udf(col("content"), lit(700), lit(700)))
    .withColumn("content", resize_image_udf(col("content"), lit(128), lit(128))))

# COMMAND ----------

# MAGIC %md ## Salva imagens em uma tabela Delta

# COMMAND ----------

df_resized.write.saveAsTable('vr_demo.fashion_recommender.images', mode="overwrite")

# COMMAND ----------

# MAGIC %sql ALTER TABLE vr_demo.fashion_recommender.images SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md-sandbox ## Cria um índice do Vector Search
# MAGIC
# MAGIC <img style="float:right" src="https://github.com/vorodrigues/databricks-demos/blob/dev/Fashion%20Recommender/img/vs-create.png?raw=true" width="500"/>
# MAGIC
# MAGIC Criar um índice do Vector Search é muito simples! Basta seguir os passos abaixo:
# MAGIC
# MAGIC 1. Busque a tabela desejada no Catálogo
# MAGIC 1. Clique no botão **`Criar`**
# MAGIC 1. Selecione **`Vector Search Index`**
# MAGIC 1. Preencha as informações no formulário
# MAGIC 1. Clique no botão **`Criar`** e pronto!
# MAGIC
# MAGIC Agora, temos um índice para realizar buscas por similaridade em tempo real com baixa latência!
# MAGIC
# MAGIC Além disso, o **Databricks Vector Search** também gerencia a extração de embeddings com **Databricks Foundation Models** *(bge-large-en)* tanto durante a indexação do nosso banco de imagens, quanto também durante a execução das buscas em tempo real para que não precisemos nos preocupar com isso.

# COMMAND ----------

# MAGIC %md-sandbox ## Busca imagens similares no Vector Search
# MAGIC
# MAGIC <img style="float:right; padding: 15px 0px 15px 15px" src="https://github.com/vorodrigues/databricks-demos/blob/dev/Fashion%20Recommender/img/vs-gradio?raw=true" width="700"/>
# MAGIC
# MAGIC Para buscar imagens similares no Vector Search, vamos utilizar um aplicativo desenvolvido com **Databricks Lakehouse Apps**. Dessa forma, podemos criar e implantar produtos de dados rapidamente usando os principais frameworks de mercado, como o `Gradio`, `FastAPI` e outros, diretamente na sua conta Databricks.
# MAGIC
# MAGIC Com isso, contamos com a mesma infraestrutura e governança já existente na sua plataforma de Data Intelligence e pode acelerar a implantação dos seus produtos de dados.
