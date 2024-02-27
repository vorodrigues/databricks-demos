# Databricks notebook source
# MAGIC %md # Análise de Fundos de Investimento
# MAGIC ## Captura e Organização dos Dados<br><br>
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/cvm/Screenshot_2023_04_24_at_23_11_51.png)
# MAGIC <br><br>
# MAGIC
# MAGIC
# MAGIC Neste notebook, iremos explorar como criar um **Lakehouse de Fundos de Investimento** a partir de dados abertos da CVM (Comissão de Valores Mobiliários). Com isso, poderemos analisar a performance passada dos fundos de investimento e descobrir ativos com características de retorno, risco e classe desejadas. 
# MAGIC
# MAGIC
# MAGIC Vamos utilizar a plataforma da **Databricks** como base para criar um sistema em nuvem colaborativo, escalável e com ótimo custo-benefício. Dessa forma, conseguimos **democratizar** o acesso a essas informações e permitir com que o negócio crie suas análises na velocidade necessária.
# MAGIC
# MAGIC
# MAGIC Separaremos o processo em duas partes:<br><br>
# MAGIC
# MAGIC
# MAGIC - **Download dos dados**
# MAGIC - **Preparação dos dados**
# MAGIC
# MAGIC
# MAGIC Vamos começar preparando nosso ambiente!

# COMMAND ----------

# MAGIC %pip install scrapy

# COMMAND ----------

import os
from pyspark.sql.functions import col
from pyspark.sql.types import *
from subprocess import Popen, PIPE, STDOUT

# COMMAND ----------

def downloadInfs(path_zip, path_csv, limit=0):
    command = f'scrapy runspider /databricks/driver/cvm-spyder.py -a limit={limit} -a path_zip={path_zip} -a path_csv={path_csv}'
    process = Popen(command, stdout=PIPE, shell=True, stderr=STDOUT, bufsize=1, close_fds=True)
    for line in iter(process.stdout.readline, b''):
        print(line.rstrip().decode('utf-8'))
    process.stdout.close()
    process.wait()
    print('Done!')

# COMMAND ----------

# MAGIC %sh scrapy runspider /databricks/driver/cvm-spyder.py -a limit={limit} -a path_zip={path_zip} -a path_csv={path_csv}

# COMMAND ----------

# MAGIC %md ## Download dos Dados
# MAGIC
# MAGIC Uma das vantagens do Lakehouse é ser uma plataforma aberta. Com isso, podemos utilizar diversas ferramentas para estender suas capacidades.
# MAGIC
# MAGIC Iremos usar o **Scrapy** para criar um web crawler para percorrer as listas de arquivos disponíveis no portal da CVM e fazer o download para o nosso Lakehouse.
# MAGIC
# MAGIC Já deixamos uma função pronta para isso e agora basta apontar quantos informes desejamos baixar (mais recentes)!

# COMMAND ----------

downloadInfs(path_zip=path_zip,
             path_csv=path_csv,
             limit=3)

# COMMAND ----------

# MAGIC %md ## Preparação dos Dados
# MAGIC
# MAGIC Com os dados baixados, podemos começar a organizá-los no nosso Lakehouse.
# MAGIC
# MAGIC Vamos utilizar o **Apache Spark**, já integrado na plataforma, para fazer a limpeza dos dados e carregá-los em tabelas **Delta**. Dessa forma, entregamos maior confiança aos indicadores gerados e também uma maior agilidade para as análises que desenvolveremos mais adiante!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cadastro
# MAGIC
# MAGIC Dados cadastrais de fundos de investimento estruturados e não estruturados (ICVM 555), tais como: CNPJ, data de registro e situação do fundo.
# MAGIC
# MAGIC http://dados.cvm.gov.br/dataset/fi-cad

# COMMAND ----------

CAD_DIR = '/FileStore/CVM/cadastros'

# COMMAND ----------

cad = spark.read.csv(CAD_DIR, sep=';', encoding='latin1', header=True, inferSchema=True)

# COMMAND ----------

print((cad.count(), len(cad.columns)))

# COMMAND ----------

cad = cad[['CNPJ_FUNDO','DENOM_SOCIAL','SIT','CLASSE','TP_FUNDO','PUBLICO_ALVO','FUNDO_EXCLUSIVO']]

# COMMAND ----------

cad = cad[cad['SIT'] == 'EM FUNCIONAMENTO NORMAL']
cad = cad[cad['FUNDO_EXCLUSIVO'] != 'S']
cad.cache()

# COMMAND ----------

print((cad.count(), len(cad.columns)))

# COMMAND ----------

cad = cad.distinct()
cad.cache

# COMMAND ----------

print((cad.count(), len(cad.columns)))

# COMMAND ----------

cad.printSchema()

# COMMAND ----------

display(cad)

# COMMAND ----------

cad.writeTo('VR_CVM.CADASTRO').createOrReplace()

# COMMAND ----------

# MAGIC %sql select distinct classe from vr_cvm.cadastro

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lâminas 
# MAGIC
# MAGIC O conjunto de dados disponibiliza as Lâminas, referentes aos Fundos de Investimento da ICVM 555, nos últimos doze meses, a partir de janeiro/2019.
# MAGIC
# MAGIC http://dados.cvm.gov.br/dataset/fi-doc-lamina

# COMMAND ----------

LAM_DIR = '/FileStore/CVM/laminas'

# COMMAND ----------

lam = spark.read.csv(LAM_DIR, sep=';', encoding='latin1', header=True, inferSchema=True)

# COMMAND ----------

lam = lam.withColumn('LIQ', col('QT_DIA_CONVERSAO_COTA_RESGATE'))
lam.cache()

# COMMAND ----------

print((lam.count(), len(lam.columns)))

# COMMAND ----------

lam = lam.dropDuplicates(['CNPJ_FUNDO'])
lam.cache()
print((lam.count(), len(lam.columns)))

# COMMAND ----------

lam.printSchema()

# COMMAND ----------

display(lam)

# COMMAND ----------

lam.writeTo('VR_CVM.LAMINAS').createOrReplace()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Informes Diários 
# MAGIC
# MAGIC O informe diário é um demonstrativo que contém as seguintes informações do fundo, relativas à data de competência:<br><br>
# MAGIC
# MAGIC - Valor total da carteira do fundo
# MAGIC - Patrimônio líquido
# MAGIC - Valor da cota
# MAGIC - Captações realizadas no dia
# MAGIC - Resgates pagos no dia
# MAGIC - Número de cotistas
# MAGIC
# MAGIC http://dados.cvm.gov.br/dataset/fi-doc-inf_diario

# COMMAND ----------

INF_DIR1 = '/FileStore/CVM/informes/001'
INF_DIR2 = '/FileStore/CVM/informes/002'

# COMMAND ----------

infs_schema1 = StructType([
  StructField('CNPJ_FUNDO', StringType(), False),
  StructField('DT_COMPTC', DateType(), False),
  StructField('VL_TOTAL', FloatType()),
  StructField('VL_QUOTA', FloatType()),
  StructField('VL_PATRIM_LIQ', FloatType()),
  StructField('CAPTC_DIA', FloatType()),
  StructField('RESG_DIA', FloatType()),
  StructField('NR_COTST', FloatType())
])

infs_schema2 = StructType([
  StructField('TP_FUNDO', StringType(), True),
  StructField('CNPJ_FUNDO', StringType(), False),
  StructField('DT_COMPTC', DateType(), False),
  StructField('VL_TOTAL', FloatType()),
  StructField('VL_QUOTA', FloatType()),
  StructField('VL_PATRIM_LIQ', FloatType()),
  StructField('CAPTC_DIA', FloatType()),
  StructField('RESG_DIA', FloatType()),
  StructField('NR_COTST', FloatType())
])

# COMMAND ----------

infs1 = spark.read.schema(infs_schema1).option('dateFormat', 'yyyy-MM-dd').csv(INF_DIR1, sep=';', encoding='latin1', nullValue='NaN', header=True, enforceSchema=True)
infs2 = spark.read.schema(infs_schema2).option('dateFormat', 'yyyy-MM-dd').csv(INF_DIR2, sep=';', encoding='latin1', nullValue='NaN', header=True, enforceSchema=True)
infs = infs1.unionByName(infs2, allowMissingColumns=True)
infs.cache()

# COMMAND ----------

print((infs.count(), len(infs.columns)))

# COMMAND ----------

infs.printSchema()

# COMMAND ----------

display(infs)

# COMMAND ----------

infs.writeTo('VR_CVM.INFORMES').createOrReplace()

# COMMAND ----------

# MAGIC %sql OPTIMIZE vr_cvm.informes ZORDER BY dt_comptc

# COMMAND ----------

# MAGIC %md ## Validação
# MAGIC
# MAGIC Agora, vamos fazer algumas análises para começar a interagir com nosso Lakehouse.

# COMMAND ----------

# MAGIC %sql -- Qual o período disponível?
# MAGIC SELECT
# MAGIC   min(DT_COMPTC) as MIN_DT,
# MAGIC   max(DT_COMPTC) as MAX_DT
# MAGIC FROM
# MAGIC   vr_cvm.informes

# COMMAND ----------

# MAGIC %sql -- Quantos registros temos neste histórico?
# MAGIC SELECT count(*) FROM vr_cvm.informes

# COMMAND ----------

# MAGIC %md
# MAGIC # Análise dos Dados
# MAGIC <br>![](/files/shared_uploads/victor.rodrigues@databricks.com/cvm/Screenshot_2023_04_24_at_23_12_43.png)
# MAGIC <br><br>
# MAGIC
# MAGIC
# MAGIC Com nosso **Lakehouse de Fundos de Investimento** pronto, podemos começar a explorá-lo com o **Databricks Serverless SQL Warehouse** e **Power BI**!
# MAGIC
# MAGIC Primeiro, vamos começar vendo como fazer queries diretamente do Lakehouse.
