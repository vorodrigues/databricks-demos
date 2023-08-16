# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, substring, cast
from pyspark.sql.types import IntegerType

# COMMAND ----------

# Read raw data
df = spark.table('vr_demo.dais.gol')

# COMMAND ----------

# Identify identifier and feature columns
cols = df.columns

infs = [
          {
            'raw' : col,
            'clean' : col.replace(':','')
          }
          for col in cols if col.endswith(':')
        ]
infsExpr = [f'`{col["raw"]}` as `{col["clean"]}`' for col in infs]

feats = [
          {
            'raw' : col,
            'clean' : col.replace('As funcionalidades apresentadas nesta sessão fazem sentido para o seu trabalho? [','')
                         .replace(']','')
          }
          for col in cols if col.endswith(']')
        ]
featsExpr = [f'`{col["raw"]}` as `{col["clean"]}`' for col in feats]

# COMMAND ----------

# Select required columns
df = df.selectExpr(*infsExpr, *featsExpr)

# COMMAND ----------

# Unpivot feature columns and generate OBT
df = ( df
  .unpivot(
    ids=[col['clean'] for col in infs],
    values=[col['clean'] for col in feats],
    variableColumnName='Feature',
    valueColumnName='Resposta'
  )
)

# COMMAND ----------

# Calculate feature's scores
df = df.withColumn('Resposta', cast(IntegerType, substring('Resposta', 0, 1)) - 1)

# COMMAND ----------

# Write gold table
df.write.mode('overwrite').saveAsTable('vr_demo.dais.gol_gold')
