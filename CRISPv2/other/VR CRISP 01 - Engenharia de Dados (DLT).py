# Databricks notebook source
# MAGIC %md
# MAGIC #Torre de controle comercial

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img style="float:center" src="https://github.com/juliandrof/pics/blob/main/dlt.jpeg?raw=true" width="70%"/>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img style="float:center" src="https://github.com/juliandrof/pics/blob/main/dlt2.jpeg?raw=true" width="70%"/>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Simplifique o ETL com o Delta Live Table
# MAGIC
# MAGIC O DLT torna a Engenharia de Dados acessível para todos. Basta declarar suas transformações em SQL ou Python, e o DLT lidará com a complexidade da Engenharia de Dados para você.
# MAGIC
# MAGIC <img style="float:right" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_1.png" width="700"/>
# MAGIC
# MAGIC **Acelere o desenvolvimento do ETL** <br/>
# MAGIC Permita que analistas e engenheiros de dados inovem rapidamente com desenvolvimento e manutenção simples de pipelines
# MAGIC
# MAGIC **Remova a complexidade operacional** <br/>
# MAGIC Automatizando tarefas administrativas complexas e obtendo visibilidade mais ampla nas operações de pipelines
# MAGIC
# MAGIC **Confie em seus dados** <br/>
# MAGIC Com controles de qualidade incorporados e monitoramento de qualidade para garantir BI, Ciência de Dados e ML precisos e úteis
# MAGIC
# MAGIC **Simplifique o processamento em lote e em tempo real** <br/>
# MAGIC Com pipelines de dados de auto-otimização e dimensionamento automático para processamento em lote ou em tempo real
# MAGIC
# MAGIC ## Nosso pipeline do Delta Live Table
# MAGIC
# MAGIC Vamos usar como input um conjunto de dados brutos contendo informações sobre empréstimos de nossos clientes e transações históricas.
# MAGIC
# MAGIC Nosso objetivo é ingerir esses dados quase em tempo real e construir uma tabela para nossa equipe de analistas, garantindo a qualidade dos dados.
# MAGIC
# MAGIC **Seu pipeline DLT está pronto!** Seu pipeline foi iniciado usando este notebook e está <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/460f840c-9ecc-4d19-a661-f60fd3a88297">disponível aqui</a>.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_sql&dt=DLT">

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Camada Bronze: Ingestão incremental de dados com Databricks Autoloader
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_2.png" width="600"/>
# MAGIC
# MAGIC Nossos dados brutos estão sendo enviados para um blob storage.
# MAGIC
# MAGIC O Autoloader simplifica essa ingestão, incluindo inferência de esquema, evolução de esquema, sendo capaz de escalar para milhões de arquivos de entrada.
# MAGIC
# MAGIC O Autoloader está disponível no SQL usando a função `cloud_files` e pode ser usado com vários formatos (json, csv, avro...):
# MAGIC
# MAGIC Para mais detalhes sobre o Autoloader, você pode ver `dbdemos.install('auto-loader')`
# MAGIC
# MAGIC #### STREAMING LIVE TABLE
# MAGIC Definir tabelas como `STREAMING` garante que você consuma apenas os novos dados recebidos. Sem o `STREAMING`, você irá escanear e ingerir todos os dados disponíveis de uma vez. Consulte a [documentação](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) para mais detalhes.

# COMMAND ----------

# Ingere incrementalmente arquivos JSON usando o Databricks Auto Loader
@dlt.table(comment="Dados de transações de vendas crus ingeridos incrementalmente a partir do storage da landing zone")
def sales_bronze():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://one-env/vr/crisp/sales"))

# COMMAND ----------

@dlt.view()
def product():
  return spark.table("vr_demo.crisp.dim_product")

# COMMAND ----------

@dlt.view()
def store():
  return spark.table("vr_demo.crisp.dim_store")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Camada Prata: Qualidade dos Dados
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_3.png" width="600"/>
# MAGIC
# MAGIC Uma vez que a camada bronze for definida, iremos criar as camadas prata através do Join dos dados. Note que as tabelas bronze são referenciadas usando dlt.read
# MAGIC
# MAGIC Para consumir apenas os incrementos da camada bronze.
# MAGIC
# MAGIC Note que não precisamos nos preocupar com compactações, o DLT lida com isso para nós.
# MAGIC
# MAGIC #### Expectations
# MAGIC Definindo expectativas (`CONSTRAINT <nome> EXPECT <condição>`), você pode garantir e acompanhar a qualidade dos seus dados. Veja a [documentação](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) para mais detalhes.

# COMMAND ----------

# Remove registros com problemas de qualidade de dados e deduplica os registros e mantém o mais recente
@dlt.table(comment="Dados de transações de vendas limpos")
@dlt.expect_or_drop("Chave primária válida", "sales_id IS NOT NULL")
@dlt.expect_or_drop("Schema válido", "_rescued_data IS NULL")
def sales_silver():
  return dlt.read("sales_bronze").orderBy("date_key", ascending=False).dropDuplicates(["sales_id"]).withColumn('date_key', col('date_key').cast('date'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Camada Gold: Data Mart / Warehouse
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_4.png" width="600"/>
# MAGIC
# MAGIC Nosso último passo é materializar a Camada Gold.
# MAGIC
# MAGIC Como essas tabelas serão solicitadas em escala usando um endpoint SQL, adicionaremos o Zorder no nível da tabela para garantir consultas mais rápidas usando `pipelines.autoOptimize.zOrderCols`, e o DLT cuidará do resto.

# COMMAND ----------

# Enriquece as transações com os dados estruturados de produto e loja
@dlt.table(
  comment="A **CRISP** se conecta aos dados de [mais de 40 varejistas e distribuidores dos EUA](https://www.gocrisp.com/catalog/tag/inbound-connectors) e provê informações de *supply chain*, vendas, dentre outras. Nesta tabela, temos os dados harmonizados de vendas no varejo por produto, vendedor e data.",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "pipelines.autoOptimize.zOrderCols": "product_id,store_id,date_key"
  },
  schema="""
    sales_id LONG COMMENT 'Chave primária. Identificador da venda gerado pela CRISP',
    product_id LONG COMMENT 'Identificador do produto gerado pela CRISP',
    store_id LONG COMMENT 'Identificador da loja gerado pela CRISP',
    date_key DATE COMMENT 'Data da venda',
    supplier STRING COMMENT 'Fornecedor do produto',
    product STRING COMMENT 'Nome do produto',
    upc STRING COMMENT 'Código UPC/GTIN-13. 13 dígitos. Sem dígito verificador',
    retailer STRING COMMENT 'Nome do varejista',
    store STRING COMMENT 'Nome da loja',
    store_type STRING COMMENT 'Tipo de loja definido pelo varejista',
    store_zip STRING COMMENT 'Código postal da loja',
    store_lat_long STRING COMMENT 'Latitude e longitude da loja (separados por vírgula)',
    sales_quantity LONG COMMENT 'Quantidade de unidades vendidas',
    sales_amount DOUBLE COMMENT 'Valor total da venda'
  """
)
def sales_gold():
  transactions = dlt.read("sales_silver").drop("_rescued_data")
  product = dlt.read("product")
  store = dlt.read("store")
  return transactions.join(product, on='product_id', how='left').join(store, on='store_id', how='left')

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
