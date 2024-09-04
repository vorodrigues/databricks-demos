# Databricks notebook source
# MAGIC %md # Comparação de Bulas

# COMMAND ----------

# MAGIC %md ## Preparação do ambiente
# MAGIC
# MAGIC Aqui, vamos definir o banco de dados que será utilizado.

# COMMAND ----------

# MAGIC %sql USE vr_demo.bulas

# COMMAND ----------

# MAGIC %md ## Conjunto de Dados
# MAGIC
# MAGIC Para essa demonstração usamos dados disponíveis no [Bulário da ANVISA](https://consultas.anvisa.gov.br/#/bulario/).
# MAGIC
# MAGIC Por simplicidade, extraímos algumas bulas do paciente do [Aciclovir](https://consultas.anvisa.gov.br/#/bulario/detalhe/1095626?numeroRegistro=143810181) em PDF e carregamos no nosso ambiente.
# MAGIC
# MAGIC Abaixo, podemos listar os arquivos disponibilizados.

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/vr_demo/bulas/pdfs"))

# COMMAND ----------

# MAGIC %md ## Lê conteúdo dos PDFs
# MAGIC
# MAGIC Para podermos aplicar os modelos de IA Generativa, vamos extrair o conteúdo desses PDFs.

# COMMAND ----------

import io
import warnings
from pypdf import PdfReader
from pyspark.sql.functions import pandas_udf
from typing import Iterator
import pandas as pd

def parse_bytes_pypdf(raw_doc_contents_bytes: bytes):
    try:
        pdf = io.BytesIO(raw_doc_contents_bytes)
        reader = PdfReader(pdf)
        parsed_content = [page_content.extract_text() for page_content in reader.pages]
        return "\n".join(parsed_content)
    except Exception as e:
        warnings.warn(f"Exception {e} has been thrown during parsing")
        return None
    
@pandas_udf("string")
def parse_pdf(content: pd.Series) -> pd.Series:
    return content.apply(parse_bytes_pypdf)

df = (spark.read.format('binaryFile')
    .load('/Volumes/vr_demo/bulas/pdfs')
    .withColumn("parsed_content", parse_pdf("content"))
)

df.createOrReplaceTempView("pdfs")

display(df)

# COMMAND ----------

# MAGIC %md ## Prepara função para comparação
# MAGIC
# MAGIC Para facilitar a aplicação dos modelos de IA Generativa, podemos utilizar os **Databricks Foundation Models** e **AI Functions**.
# MAGIC
# MAGIC Os **Databricks Foundation Models** são grandes modelos de linguagem (LLMs) servidos de forma serverless para que vocês não precisem se preocupar em criar e gerenciar a infraestrutura destes mesmos.
# MAGIC
# MAGIC E as **AI Functions** permitem aplicar esses modelos massivamente sobre os dados existentes diretamente nas suas consultas.
# MAGIC
# MAGIC Abaixo, vamos criar uma função utilizando esses recursos para fazer a comparação entre duas bulas.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION comparar_bulas(bula1 STRING, bula2 STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN 
# MAGIC AI_QUERY('databricks-meta-llama-3-1-405b-instruct',
# MAGIC CONCAT(
# MAGIC 'Identifique diferenças entre as bulas abaixo:
# MAGIC
# MAGIC ### BULA 1 ###:
# MAGIC
# MAGIC ', bula1,'
# MAGIC
# MAGIC ### BULA 2 ###:
# MAGIC
# MAGIC ', bula2
# MAGIC ));

# COMMAND ----------

# MAGIC %md ## Compara todas as bulas
# MAGIC
# MAGIC Por fim, podemos aplicar essa função sobre todas as bulas que carregamos e visualizar as diferenças encontradas pela IA.

# COMMAND ----------

# MAGIC %sql
# MAGIC select parsed_content, comparar_bulas(parsed_content, lag(parsed_content) over (order by path)) as comparacao from pdfs
