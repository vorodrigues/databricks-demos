# Databricks notebook source
# MAGIC %md # Data Engineering

# COMMAND ----------

# MAGIC %md ##### Leia os dados da tabela vr_demo.crisp.sales e exiba uma amostra

# COMMAND ----------

# Leia os dados da tabela vr_demo.crisp.sales e exiba uma amostra
df = spark.sql("SELECT * FROM vr_demo.crisp.sales LIMIT 5")
display(df)

# COMMAND ----------

# MAGIC %md ##### Leia o Delta Change Data Feed da tabela vr_demo.crisp.sales a partir da versão 6

# COMMAND ----------

# Leia o Delta Change Data Feed da tabela vr_demo.crisp.sales a partir da versão 6
df = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", "6").table("vr_demo.crisp.sales")

# COMMAND ----------

# MAGIC %md ##### Deduplique os registros com base no store_id, mantendo somente os registros com o _commit_timestamp mais recente

# COMMAND ----------

# Deduplicate records based on store_id, keeping only the records with the most recent _commit_timestamp
df = df.orderBy("_commit_timestamp", ascending=False).dropDuplicates(["store_id"])

# COMMAND ----------

# MAGIC %md ##### Crie uma view temporária e atualize a tabela vr_demo.crisp.dim_store com um MERGE

# COMMAND ----------

# Crie uma view temporária a partir do dataframe df
df.createOrReplaceTempView("temp_view")

# Atualize a tabela dim_store usando o MERGE
spark.sql("""
    MERGE INTO vr_demo.crisp.dim_store AS target
    USING temp_view AS source
    ON target.store_id = source.store_id
    WHEN MATCHED THEN
        UPDATE SET
        target.store = source.store,
        target.store_type = source.store_type,
        target.store_zip = source.store_zip,
        target.store_lat_long = source.store_lat_long
    WHEN NOT MATCHED THEN
        INSERT (store_id, store, store_type, store_zip, store_lat_long)
        VALUES (source.store_id, source.store, source.store_type, source.store_zip, source.store_lat_long)
""")

# COMMAND ----------

# MAGIC %md ##### Ative o Databricks Predictive Optimization no schema vr_demo.crisp

# COMMAND ----------

# Enable Predictive Optimization in the vr_demo.crisp schema
spark.sql("ALTER SCHEMA vr_demo.crisp ENABLE PREDICTIVE OPTIMIZATION")

# COMMAND ----------

# MAGIC %md ##### Verifique se o Databricks Predictive Optimization foi executado no schema vr_demo.crisp. Use a tabela system.storage.predictive_optimization_operations_history

# COMMAND ----------

# Check if Databricks Predictive Optimization has been executed
spark.sql("""
    SELECT *
    FROM system.storage.predictive_optimization_operations_history
    WHERE catalog_name = 'vr_demo' and schema_name = 'crisp'
""").display()

# COMMAND ----------

# MAGIC %md # Data Science

# COMMAND ----------

# MAGIC %md ##### Calcule o sales_amount por semana com base no date_key para cada produto e loja usando os dados da tabela vr_demo.crisp.sales

# COMMAND ----------

from pyspark.sql.functions import trunc

# Read the data from the table
sales_data = spark.table("vr_demo.crisp.sales")

# Truncate the date_key column to the beginning of the week
sales_data = sales_data.withColumn("week", trunc("date_key", "week"))

# Calculate the sales_amount per week
weekly_sales = sales_data.groupby("product_id", "store_id", "week").sum("sales_amount")

# Show the result
display(weekly_sales)

# COMMAND ----------

# MAGIC %md ##### Crie um modelo SARIMAX para fazer a projeção das vendas do produto '5655945986815814052' e loja '8271322667487204067' para as próximas 4 semanas a partir do dataframe weekly_sales. Exiba o resultado em um gráfico de linha

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX

# Filter the data for the specific product and store
filtered_data = weekly_sales.filter((weekly_sales.product_id == '5655945986815814052') & (weekly_sales.store_id == '8271322667487204067'))

# Convert the dataframe to a pandas dataframe
sales_df = filtered_data.toPandas()

# Convert the week column to datetime format
sales_df['week'] = pd.to_datetime(sales_df['week'])

# Sort the dataframe by week
sales_df.sort_values('week', inplace=True)

# Create a SARIMAX model
model = SARIMAX(sales_df['sum(sales_amount)'], order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
fitted_model = model.fit()

# Forecast sales for the next 4 weeks
forecast = fitted_model.forecast(steps=4)

# Create a dataframe for the forecast
forecast_df = pd.DataFrame({'week': pd.date_range(start=sales_df['week'].iloc[-1], periods=4, freq='W'),
                            'sales_forecast': forecast})

# Plot the actual sales and forecasted sales
plt.plot(sales_df['week'], sales_df['sum(sales_amount)'], label='Actual Sales')
plt.plot(forecast_df['week'], forecast_df['sales_forecast'], label='Sales Forecast')

plt.xlabel('Week')
plt.ylabel('Sales Amount')
plt.title('Sales Forecast for Product 5655945986815814052 in Store 8271322667487204067')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md ##### Crie uma Spark Pandas UDF que usa um modelo SARIMAX para fazer a projeção das vendas para as próximas 4 semanas. Aplique esta UDF no dataframe weekly_sales para cada produto e loja

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

# Define a SARIMAX function to be applied on each partition of the dataframe
@pandas_udf("product_id string, store_id string, week string, sales_forecast double", PandasUDFType.GROUPED_MAP)
def sarimax_forecast(data):
    # Format sales data for SARIMAX model
    dates = pd.to_datetime(data['week'])
    sales = data['sum(sales_amount)']
    
    # Create SARIMAX model
    model = SARIMAX(sales, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
    fitted_model = model.fit()
    
    # Forecast sales for the next 4 weeks
    forecast = fitted_model.forecast(steps=4)
    
    # Create forecast dataframe
    forecast_df = pd.DataFrame({'product_id': data['product_id'].iloc[0], 
                                'store_id': data['store_id'].iloc[0], 
                                'week': pd.date_range(start=dates.max(), periods=4, freq='W'), 
                                'sales_forecast': forecast})
    
    return forecast_df

# Apply the SARIMAX forecast UDF on weekly_sales dataframe
forecast_data = weekly_sales.groupby("product_id", "store_id").apply(sarimax_forecast)

# COMMAND ----------

# MAGIC %md ##### Crie um modelo de previsão usando o Databricks AutoML a partir do dataframe weekly_sales

# COMMAND ----------

import databricks.automl

# Crie um modelo com Databricks AutoML
model = databricks.automl.forecast(
  dataset=weekly_sales.filter('''
                              (product_id=7716269855258040421 and store_id=9001443411346643077) or
                              (product_id=3534159492567507482 and store_id=3215494003739189528)
                              '''),
  target_col="sum(sales_amount)", 
  identity_col=["product_id", "store_id"], 
  time_col="week",
  frequency="W",
  horizon=4, 
  timeout_minutes=60, 
  primary_metric="smape"
)
