# Databricks notebook source
# MAGIC %md # Fundos de Investimento

# COMMAND ----------

# MAGIC %pip install scrapy

# COMMAND ----------

import os
from pyspark.sql.functions import col
from pyspark.sql.types import *

# COMMAND ----------

import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT


def downloadInfs(path_zip, path_csv, limit=0):
    command = f'scrapy runspider /databricks/driver/cvm-spyder.py -a limit={limit} -a path_zip={path_zip} -a path_csv={path_csv}'
    process = Popen(command, stdout=PIPE, shell=True, stderr=STDOUT, bufsize=1, close_fds=True)
    for line in iter(process.stdout.readline, b''):
        print(line.rstrip().decode('utf-8'))
    process.stdout.close()
    process.wait()
    print('Done!')


def comparacao(cad, infs, fundos):
    
    hist = cad.query('CNPJ_FUNDO in '+str(fundos))[['DENOM_SOCIAL']] \
              .join(infs[['DT_COMPTC','VL_QUOTA']])
    hist['DENOM_SOCIAL'] = hist['DENOM_SOCIAL'].str[0:40]
    
    # Retorno x Tempo
    ini = infs.groupby('CNPJ_FUNDO').head(1).rename(columns={'VL_QUOTA':'INI'})['INI']
    ret = hist.join(ini)
    ret['RET'] = (ret['VL_QUOTA'] / ret['INI'] - 1) * 100
    ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET') \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.show()
    
    # Clustermap de Correlação
    ret = hist
    ret['RET'] = hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(0) / hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(252)
    pvt = ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET')
    sns.clustermap(abs(pvt.corr(method='pearson')), 
                   annot=True, 
                   cmap=sns.diverging_palette(220, 20, as_cmap=True),
                   vmin=0,
                   vmax=1
                  )
    plt.show()


def comparacao2(cad, infs, fundos):
    
    hist = cad.query('CNPJ_FUNDO in '+str(fundos))[['DENOM_SOCIAL']] \
              .join(infs[['DT_COMPTC','VL_QUOTA']])
    hist['DENOM_SOCIAL'] = hist['DENOM_SOCIAL'].str[0:40]
    
    # Retorno x Tempo
    ini = infs.groupby('CNPJ_FUNDO').head(1).rename(columns={'VL_QUOTA':'INI'})['INI']
    ret = hist.join(ini)
    ret['RET'] = (ret['VL_QUOTA'] / ret['INI'] - 1) * 100
    ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET') \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.show()
    
    # Clustermap de Correlação
    ret = hist
    ret['RET'] = hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(0) / hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(126)
    pvt = ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET')
    sns.clustermap(abs(pvt.corr(method='pearson')), 
                   annot=True, 
                   cmap=sns.diverging_palette(220, 20, as_cmap=True),
                   vmin=0,
                   vmax=1
                  )
    plt.show()


def acompanhamento(cad, infs, fundos, dtini, dtfim):

    fundos = pd.DataFrame(fundos, columns = ['CNPJ_FUNDO','COTAS'])
    fundos = fundos.set_index('CNPJ_FUNDO')

    hist = fundos \
              .join(cad[['DENOM_SOCIAL']]) \
              .join(infs[['DT_COMPTC','VL_QUOTA']]\
                    .loc[(str(dtini)[:10]<=infs['DT_COMPTC']) & (infs['DT_COMPTC']<=str(dtfim)[:10])])

    # Total x Tempo
    tot = hist
    tot['POS'] = tot['COTAS'] * tot['VL_QUOTA']
    tot.groupby('DT_COMPTC')['POS'].sum() \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.show()

    # Retorno x Tempo
    ini = hist.sort_values('DT_COMPTC').groupby('CNPJ_FUNDO').head(1).rename(columns={'VL_QUOTA':'INI'})['INI']
    ret = hist.join(ini)
    ret['RET'] = (ret['VL_QUOTA'] / ret['INI'] - 1) * 100
    ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET') \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.legend(loc='upper left', bbox_to_anchor=(0, -0.1))
    plt.show()

    # Retorno por Fundo
    fin = pd.DataFrame(ret.groupby('CNPJ_FUNDO').tail(1))
    fin['POS'] = fin['COTAS'] * fin['VL_QUOTA']
    fin['VAR'] = fin['POS'] * (1 - 1 / (1 + fin['RET'] / 100))
    fin = fin.sort_values('RET', ascending=False)

    # Retorno Total
    res = fin[['POS','VAR']].sum()
    res['RET'] = res['VAR'] / (res['POS'] - res['VAR']) * 100
    res['DENOM_SOCIAL'] = 'Total'
    display(fin.append(pd.DataFrame([res]))[['DENOM_SOCIAL','POS','VAR','RET']])

# COMMAND ----------

# MAGIC %md # Download dos Dados 

# COMMAND ----------

# MAGIC %md ## Ano < 2021

# COMMAND ----------

# MAGIC %run ./cvm-hist-spyder

# COMMAND ----------

path_zip = "/databricks/driver/CVM"
if not os.path.exists(path_zip):
  os.makedirs(path_zip)
path_csv = "/dbfs/FileStore/CVM"
if not os.path.exists(path_csv):
  os.makedirs(path_csv)

# COMMAND ----------

limit = 0

command = f'scrapy runspider /databricks/driver/cvm-hist-spyder.py -a limit={limit} -a path_zip={path_zip} -a path_csv={path_csv}'
process = Popen(command, stdout=PIPE, shell=True, stderr=STDOUT, bufsize=1, close_fds=True)
for line in iter(process.stdout.readline, b''):
    print(line.rstrip().decode('utf-8'))
process.stdout.close()
process.wait()
print('Done!')

# COMMAND ----------

for f in dbutils.fs.ls('/FileStore/CVM'):
  if f.isFile():
    if f.path < 'dbfs:/FileStore/CVM/inf_diario_fi_202004.csv':
      print('001: %s' % f.name)
      dbutils.fs.mv(f.path, '/FileStore/CVM/informes/001/%s' % f.name)
    else:
      print('002: %s' % f.name)
      dbutils.fs.mv(f.path, '/FileStore/CVM/informes/002/%s' % f.name)

# COMMAND ----------

# MAGIC %md ## Ano >= 2021

# COMMAND ----------

# MAGIC %run ./cvm-spyder

# COMMAND ----------

path_zip = "/databricks/driver/CVM"
if not os.path.exists(path_zip):
  os.makedirs(path_zip)
path_csv = "/dbfs/FileStore/CVM"
if not os.path.exists(path_csv):
  os.makedirs(path_csv)

# COMMAND ----------

downloadInfs(path_zip=path_zip,
             path_csv=path_csv,
             limit=3)

# COMMAND ----------

for f in dbutils.fs.ls('/FileStore/CVM'):
  if f.isFile():
    if f.path < 'dbfs:/FileStore/CVM/inf_diario_fi_202004.csv':
      print('001: %s' % f.name)
      dbutils.fs.mv(f.path, '/FileStore/CVM/informes/001/%s' % f.name)
    else:
      print('002: %s' % f.name)
      dbutils.fs.mv(f.path, '/FileStore/CVM/informes/002/%s' % f.name)

# COMMAND ----------

# MAGIC %md # Preparação dos Dados

# COMMAND ----------

# MAGIC %md ## Ingestão

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS VR_CVM

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cadastro
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

# MAGIC %sql OPTIMIZE VR_CVM.INFORMES ZORDER BY DT_COMPTC

# COMMAND ----------

# MAGIC %md ### Carteiras

# COMMAND ----------

#%fs rm dbfs:/FileStore/shared_uploads/victor.rodrigues@databricks.com/CVM_Carteiras.csv

# COMMAND ----------

CAR_DIR = 'dbfs:/FileStore/shared_uploads/victor.rodrigues@databricks.com/CVM_Carteiras.csv'
car = spark.read.csv(CAR_DIR, encoding='latin1', header=True, inferSchema=True)

# COMMAND ----------

print((car.count(), len(car.columns)))

# COMMAND ----------

car.printSchema()

# COMMAND ----------

display(car)

# COMMAND ----------

car.writeTo('VR_CVM.CARTEIRAS').createOrReplace()

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT
# MAGIC   min(DT_COMPTC) as MIN_DT,
# MAGIC   max(DT_COMPTC) as MAX_DT
# MAGIC FROM
# MAGIC   VR_CVM.INFORMES
