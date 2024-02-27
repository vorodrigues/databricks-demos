# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Code

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Gere um código que converta o dataframe spark para um dataframe pandas e mostre o total de ids de avaliações por sentimento

# COMMAND ----------

import pandas as pd

# Ler o DataFrame do Spark
df = spark.read.table('vr_demo.aifunc.avaliacoes_revisadas')

# COMMAND ----------

import pandas as pd

# Ler o DataFrame do Spark
df = spark.read.table('vr_demo.aifunc.avaliacoes_revisadas')

# Converter o DataFrame do Spark para um DataFrame do Pandas
pandas_df = df.toPandas()

# Calcular o total de avaliações por sentimento
total_aval_por_sent = pandas_df.groupby('sentimento')['id'].nunique()

print(total_aval_por_sent)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform/Optimize Code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo 1

# COMMAND ----------

# MAGIC %md
# MAGIC 2) Converter esse código para PySpark

# COMMAND ----------

# Importar bibliotecas necessárias
import pandas as pd

# Ler o DataFrame do Spark
df = spark.read.table('vr_demo.aifunc.avaliacoes_revisadas')

# Converter o DataFrame do Spark para um DataFrame do Pandas
pandas_df = df.toPandas()

# Calcular o total de avaliações por sentimento
total_negativo = pandas_df[pandas_df['sentimento'] == 'NEGATIVO'].shape[0]

print(total_negativo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo 2

# COMMAND ----------

# MAGIC %md
# MAGIC 3) Mostre-me um exemplo de código python ineficiente, explique por que ele é ineficiente e, em seguida, mostre-me uma versão aprimorada desse código que é mais eficiente. Explique por que é mais eficiente e, em seguida, forneça uma lista de strings para testar e o código para comparar, tentando cada uma delas

# COMMAND ----------

# MAGIC %md
# MAGIC # Complete Code
# MAGIC - On macOS, press **shift + option + space** directly in a cell
# MAGIC - On Windows, press **ctrl + shift + space** directly in a cell
# MAGIC
# MAGIC
# MAGIC To accept the suggested code, press tab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo 1

# COMMAND ----------

# MAGIC %md
# MAGIC 4) Autocomplete o código abaixo

# COMMAND ----------

# Escreva um código para excluir uma coluna de um dataframe spark
dataframe = dataframe.drop('nome_da_coluna', axis=1)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exemplo 2

# COMMAND ----------

# MAGIC %md
# MAGIC 5) Autocomplete o código abaixo

# COMMAND ----------

# Carregue o dataset wine em um dataframe do sklearn, divida o dado em 3 grupos por flavonóides e então visualize em gráfico de barras
from sklearn.datasets import load_wine
import pandas as pd
import matplotlib.pyplot as plt

wine = load_wine()
df_wine = pd.DataFrame(wine.data, columns = wine.feature_names)
df_wine['flavonoids_categories'] = pd.cut(df_wine['flavanoids'], bins=3, labels=['low', 'medium','high'])
bar_df = df_wine.groupby(['flavonoids_categories']).size()
bar_df.plot(kind='bar')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explain Code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo 1

# COMMAND ----------

# MAGIC %md
# MAGIC 6) Poderia me explicar o que esse código faz?

# COMMAND ----------

# Importar bibliotecas necessárias
import pandas as pd

# Ler o DataFrame do Spark
df = spark.read.table('vr_demo.aifunc.avaliacoes_revisadas')

# Converter o DataFrame do Spark para um DataFrame do Pandas
pandas_df = df.toPandas()

# Calcular o total de avaliações por sentimento
count_by_sentimento = pandas_df.groupby('sentimento')['id_avaliacao'].count()

print(count_by_sentimento)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo 2

# COMMAND ----------

# MAGIC %md
# MAGIC 7) Quando eu devo usar repartition() e coalesce() em Spark?

# COMMAND ----------

# MAGIC %md
# MAGIC # Fix Code

# COMMAND ----------

# MAGIC %md
# MAGIC 8) Por que estou tendo este erro? Como eu corrijo isso? 

# COMMAND ----------

from pyspark.sql.functions import col

# cria um dataset com duas colunas: a and b
df = spark.range(5).select(col('id').alias('a'), col('id').alias('b'))

# tenta acessar uma coluna não-existente c
df.select(col('c')).show()

# COMMAND ----------

import pandas as pd

# carregue o DataFrame
df = spark.read.table('vr_demo.aifunc.avaliacoes_revisadas')

# converta o DataFrame do Spark para um DataFrame do Pandas
pandas_df = df.toPandas()

# calcule a média de idade por gênero dos passageiros usando pandas
count_by_sentimento = pandas_df.groupby("sentiment")["id_avaliacao"].count()

print(count_by_sentimento)

# COMMAND ----------

import pandas as pd

# carregue o DataFrame
df = spark.read.table('vr_demo.aifunc.avaliacoes_revisadas')

# converta o DataFrame do Spark para um DataFrame do Pandas
pandas_df = df.to_Pandas()

# calcule a média de idade por gênero dos passageiros usando pandas
count_by_sentimento = pandas_df.groupby("sentimento")["id_avaliacao"].count()

print(count_by_sentimento)
