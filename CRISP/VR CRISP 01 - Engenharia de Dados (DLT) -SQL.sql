-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Torre de controle comercial

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <img style="float:center" src="https://github.com/juliandrof/pics/blob/main/dlt.jpeg?raw=true" width="70%"/>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <img style="float:center" src="https://github.com/juliandrof/pics/blob/main/dlt2.jpeg?raw=true" width="70%"/>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Simplifique o ETL com o Delta Live Table
-- MAGIC
-- MAGIC O DLT torna a Engenharia de Dados acessível para todos. Basta declarar suas transformações em SQL ou Python, e o DLT lidará com a complexidade da Engenharia de Dados para você.
-- MAGIC
-- MAGIC <img style="float:right" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_1.png" width="700"/>
-- MAGIC
-- MAGIC **Acelere o desenvolvimento do ETL** <br/>
-- MAGIC Permita que analistas e engenheiros de dados inovem rapidamente com desenvolvimento e manutenção simples de pipelines
-- MAGIC
-- MAGIC **Remova a complexidade operacional** <br/>
-- MAGIC Automatizando tarefas administrativas complexas e obtendo visibilidade mais ampla nas operações de pipelines
-- MAGIC
-- MAGIC **Confie em seus dados** <br/>
-- MAGIC Com controles de qualidade incorporados e monitoramento de qualidade para garantir BI, Ciência de Dados e ML precisos e úteis
-- MAGIC
-- MAGIC **Simplifique o processamento em lote e em tempo real** <br/>
-- MAGIC Com pipelines de dados de auto-otimização e dimensionamento automático para processamento em lote ou em tempo real
-- MAGIC
-- MAGIC ## Nosso pipeline do Delta Live Table
-- MAGIC
-- MAGIC Vamos usar como input um conjunto de dados brutos contendo informações sobre empréstimos de nossos clientes e transações históricas.
-- MAGIC
-- MAGIC Nosso objetivo é ingerir esses dados quase em tempo real e construir uma tabela para nossa equipe de analistas, garantindo a qualidade dos dados.
-- MAGIC
-- MAGIC **Seu pipeline DLT está pronto!** Seu pipeline foi iniciado usando este notebook e está <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/460f840c-9ecc-4d19-a661-f60fd3a88297">disponível aqui</a>.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_sql&dt=DLT">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Camada Bronze: Ingestão incremental de dados com Databricks Autoloader
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_2.png" width="600"/>
-- MAGIC
-- MAGIC Nossos dados brutos estão sendo enviados para um blob storage.
-- MAGIC
-- MAGIC O Autoloader simplifica essa ingestão, incluindo inferência de esquema, evolução de esquema, sendo capaz de escalar para milhões de arquivos de entrada.
-- MAGIC
-- MAGIC O Autoloader está disponível no SQL usando a função `cloud_files` e pode ser usado com vários formatos (json, csv, avro...):
-- MAGIC
-- MAGIC Para mais detalhes sobre o Autoloader, você pode ver `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC #### STREAMING LIVE TABLE
-- MAGIC Definir tabelas como `STREAMING` garante que você consuma apenas os novos dados recebidos. Sem o `STREAMING`, você irá escanear e ingerir todos os dados disponíveis de uma vez. Consulte a [documentação](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) para mais detalhes.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_bronze_sql  AS
SELECT
  *
FROM
  cloud_files(
    's3://one-env/vr/crisp/sales',
    'json',
    map("cloudFiles.inferColumnTypes", "true")
  
  )

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW product_sql
AS
 SELECT * FROM vr_demo.crisp.dim_product
 --jsfdb.dbo.dim_product
 

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW store_sql
AS
SELECT * FROM vr_demo.crisp.dim_store

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Camada Prata: Qualidade dos Dados
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_3.png" width="600"/>
-- MAGIC
-- MAGIC Uma vez que a camada bronze for definida, iremos criar as camadas prata através do Join dos dados. Note que as tabelas bronze são referenciadas usando dlt.read
-- MAGIC
-- MAGIC Para consumir apenas os incrementos da camada bronze.
-- MAGIC
-- MAGIC Note que não precisamos nos preocupar com compactações, o DLT lida com isso para nós.
-- MAGIC
-- MAGIC #### Expectations
-- MAGIC Definindo expectativas (`CONSTRAINT <nome> EXPECT <condição>`), você pode garantir e acompanhar a qualidade dos seus dados. Veja a [documentação](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) para mais detalhes.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_silver_sql (
  CONSTRAINT `Chave primaria valida` EXPECT (sales_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT `Schema valido` EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
AS SELECT * from STREAM(live.sales_bronze_sql)

-- COMMAND ----------

-- DBTITLE 1,A 
-- MAGIC %md-sandbox
-- MAGIC ## Camada Gold: Data Mart / Warehouse
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://e2-demo-field-eng.cloud.databricks.com/files/tables/vr/crisp/crisp_de_4.png" width="600"/>
-- MAGIC
-- MAGIC Nosso último passo é materializar a Camada Gold.
-- MAGIC
-- MAGIC Como essas tabelas serão solicitadas em escala usando um endpoint SQL, adicionaremos o Zorder no nível da tabela para garantir consultas mais rápidas usando `pipelines.autoOptimize.zOrderCols`, e o DLT cuidará do resto.

-- COMMAND ----------

CREATE LIVE TABLE sales_gold_sql
AS
SELECT
    s.sales_id ,
    s.product_id,
    s.store_id ,
    s.date_key ,
    p.supplier,
    p.product,
    p.upc,
    t.retailer,
    t.store,
    t.store_type,
    t.store_zip,
    t.store_lat_long,
    s.sales_quantity,
    s.sales_amount
FROM  live.sales_silver_sql s
LEFT JOIN live.store_sql t
  on s.store_id =t.store_id
LEFT JOIN live.product_sql p
  on s.product_id=p.product_id

