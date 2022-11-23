# Databricks notebook source
# Connection information for cloud storage
# dbutils.widgets.text("External Location", "")

# COMMAND ----------

# MAGIC %md # End to End Industrial IoT (IIoT) on Databricks 
# MAGIC ## Part 2 - Machine Learning
# MAGIC 
# MAGIC 
# MAGIC Now that our data is flowing reliably from our sensor devices into an enriched Delta table in cloud storage, we can start to build ML models to predict power output and remaining life of our assets using historical sensor, weather, power and maintenance data. 
# MAGIC 
# MAGIC We create two models ***for each Wind Turbine***:
# MAGIC 1. **Turbine Power Output** - using current readings for turbine operating parameters (angle, RPM) and weather (temperature, humidity, etc.), predict the expected power output 6 hours from now
# MAGIC 2. **Turbine Remaining Life** - predict the remaining life in days until the next maintenance event
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/turbine_models.png" width=800>
# MAGIC 
# MAGIC We will use the XGBoost framework to train regression models. Due to the size of the data and number of Wind Turbines, we will use Spark UDFs to distribute training across all the nodes in our cluster.
# MAGIC 
# MAGIC 
# MAGIC The notebook is broken into sections following these steps:<br>
# MAGIC 1. **Machine Learning** - train XGBoost regression models using distributed ML to predict power output and asset remaining life on historical sensor data<br>
# MAGIC 2. **Model Deployment** - deploy trained models for real-time serving in Azure ML services<br>
# MAGIC 3. **Model Inference** - score real data instantly against hosted models via REST API

# COMMAND ----------

# MAGIC %md ## 0. Environment Setup
# MAGIC 
# MAGIC The pre-requisites are listed below:
# MAGIC 
# MAGIC ### Services Required
# MAGIC * Cloud storage
# MAGIC 
# MAGIC ### Databricks Configuration Required
# MAGIC * 3-node (min) Databricks Cluster running **DBR 7.0ML+** and the following libraries:
# MAGIC   * **Azure Event Hubs Connector for Databricks** - Maven coordinates `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.16`
# MAGIC * **Part 1 Notebook Run to generate and process the data** (this can be found [here](https://databricks.com/notebooks/iiot/iiot-end-to-end-part-1.html)). Ensure the following tables have been created:
# MAGIC   * **turbine_maintenance** - Maintenance dates for each Wind Turbine
# MAGIC   * **turbine_power** - Hourly power output for each Wind Turbine
# MAGIC   * **turbine_enriched** - Hourly turbine sensor readinigs (RPM, Angle) enriched with weather readings (temperature, wind speed/direction, humidity)
# MAGIC   * **gold_readings** - Combined view containing all 3 tables

# COMMAND ----------

# Setup storage locations for all data
ROOT_PATH = dbutils.widgets.get("External Location")

# Pyspark and ML Imports
import os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType, col, lit
import numpy as np
import pandas as pd
import xgboost as xgb
import mlflow, mlflow.xgboost
import random, string

# COMMAND ----------

# MAGIC %sql USE vr_iiot.dev

# COMMAND ----------

# MAGIC %md ## 1. Model Training
# MAGIC 
# MAGIC The next step is to train lots of different models using different algorithms and parameters in search for the one that optimally solves our business problem.
# MAGIC 
# MAGIC That's where the **Spark** + **HyperOpt** + **MLflow** framework can be leveraged to easily distribute the training proccess across a cluster, efficiently optimize hyperparameters and track all experiments in order to quickly evaluate many models, choose the best one and guarantee its reproducibility.<br><br>
# MAGIC 
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/ml_2.jpg)

# COMMAND ----------

# MAGIC %md ### 1a. Feature Engineering
# MAGIC In order to predict power output 6 hours ahead, we need to first time-shift our data to create our label column. We can do this easily using Spark Window partitioning. 
# MAGIC 
# MAGIC In order to predict remaining life, we need to backtrace the remaining life from the maintenance events. We can do this easily using cross joins. The following diagram illustrates the ML Feature Engineering pipeline:
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/ml_pipeline.png" width=800>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate the age of each turbine and the remaining life in days
# MAGIC CREATE OR REPLACE VIEW turbine_age AS
# MAGIC WITH reading_dates AS (SELECT distinct date, deviceid FROM turbine_power),
# MAGIC   maintenance_dates AS (
# MAGIC     SELECT d.*, datediff(nm.date, d.date) as datediff_next, datediff(d.date, lm.date) as datediff_last 
# MAGIC     FROM reading_dates d LEFT JOIN turbine_maintenance nm ON (d.deviceid=nm.deviceid AND d.date<=nm.date)
# MAGIC     LEFT JOIN turbine_maintenance lm ON (d.deviceid=lm.deviceid AND d.date>=lm.date ))
# MAGIC SELECT date, deviceid, ifnull(min(datediff_last),0) AS age, ifnull(min(datediff_next),0) AS remaining_life
# MAGIC FROM maintenance_dates 
# MAGIC GROUP BY deviceid, date;
# MAGIC 
# MAGIC -- Calculate the power 6 hours ahead using Spark Windowing and build a feature_table to feed into our ML models
# MAGIC CREATE OR REPLACE VIEW feature_table AS
# MAGIC SELECT r.*, age, remaining_life,
# MAGIC   LEAD(power, 72, power) OVER (PARTITION BY r.deviceid ORDER BY window) as power_6_hours_ahead
# MAGIC FROM gold_readings r JOIN turbine_age a ON (r.date=a.date AND r.deviceid=a.deviceid)
# MAGIC WHERE r.date < CURRENT_DATE();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT window, power, power_6_hours_ahead FROM feature_table WHERE deviceid='WindTurbine-1'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, avg(age) as age, avg(remaining_life) as life FROM feature_table WHERE deviceid='WindTurbine-1' GROUP BY date ORDER BY date

# COMMAND ----------

# MAGIC %md ### 1b. Power Output
# MAGIC [Pandas UDFs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/udf-python-pandas?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json) allow us to vectorize Pandas code across multiple nodes in a cluster. Here we create a UDF to train an XGBoost Regressor model against all the historic data for a particular Wind Turbine. We use a Grouped Map UDF as we perform this model training on the Wind Turbine group level.

# COMMAND ----------

# Create a function to train a XGBoost Regressor on a turbine's data
def train_distributed_xgb(readings_pd, model_type, label_col, prediction_col):
  deviceid = readings_pd['deviceid'][0]
  mlflow.xgboost.autolog()
  with mlflow.start_run(run_name=f'{model_type}_{deviceid}'):
    # Log the model type and device ID
    mlflow.log_param('deviceid', deviceid)
    mlflow.log_param('model', model_type)

    # Train an XGBRegressor on the data for this Turbine
    alg = xgb.XGBRegressor() 
    train_dmatrix = xgb.DMatrix(data=readings_pd[feature_cols].astype('float'),label=readings_pd[label_col])
    params = {'learning_rate': 0.5, 'alpha':10, 'colsample_bytree': 0.5, 'max_depth': 5}
    model = xgb.train(params=params, dtrain=train_dmatrix, evals=[(train_dmatrix, 'train')])

    # Make predictions on the dataset and return the results
    readings_pd[prediction_col] = model.predict(train_dmatrix)
  return readings_pd

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','remaining_life']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'power_6_hours_ahead'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('feature_table').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}'])

# Register a Pandas UDF to distribute XGB model training using Spark
@pandas_udf(feature_df.schema, PandasUDFType.GROUPED_MAP)
def train_power_models(readings_pd):
  return train_distributed_xgb(readings_pd, 'power_prediction', label_col, prediction_col)

# Run the Pandas UDF against our feature dataset - this will train 1 model for each turbine
power_predictions = feature_df.groupBy('deviceid').apply(train_power_models)

# Save predictions to storage
power_predictions.write.format("delta").mode("overwrite").partitionBy("date").saveAsTable("turbine_power_predictions")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Plot actuals vs. predicted
# MAGIC SELECT date, deviceid, avg(power_6_hours_ahead) as actual, avg(power_6_hours_ahead_predicted) as predicted FROM turbine_power_predictions GROUP BY date, deviceid

# COMMAND ----------

# MAGIC %md **Automated Model Tracking in Databricks**
# MAGIC 
# MAGIC As you train the models, notice how Databricks-managed MLflow automatically tracks each run in the "Runs" tab of the notebook. You can open each run and view the parameters, metrics, models and model artifacts that are captured by MLflow Autologging. For XGBoost Regression models, MLflow tracks: 
# MAGIC 1. Any model parameters (alpha, colsample, learning rate, etc.) passed to the `params` variable
# MAGIC 2. Metrics specified in `evals` (RMSE by default)
# MAGIC 3. The trained XGBoost model file
# MAGIC 4. Feature importances
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/iiot_mlflow_tracking.gif" width=800>

# COMMAND ----------

# MAGIC %md ### 1c. Remaining Life
# MAGIC Our second model predicts the remaining useful life of each Wind Turbine based on the current operating conditions. We have historical maintenance data that indicates when a replacement activity occured - this will be used to calculate the remaining life as our training label. 
# MAGIC 
# MAGIC Once again, we train an XGBoost model for each Wind Turbine to predict the remaining life given a set of operating parameters and weather conditions

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','power_6_hours_ahead_predicted']
label_col = 'remaining_life'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('turbine_power_predictions').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}'])

# Register a Pandas UDF to distribute XGB model training using Spark
@pandas_udf(feature_df.schema, PandasUDFType.GROUPED_MAP)
def train_life_models(readings_pd):
  return train_distributed_xgb(readings_pd, 'life_prediction', label_col, prediction_col)

# Run the Pandas UDF against our feature dataset - this will train 1 model per turbine and write the predictions to a table
life_predictions = feature_df.groupBy('deviceid').apply(train_life_models)

# Save predictions to storage
life_predictions.write.format("delta").mode("overwrite").partitionBy("date").saveAsTable("turbine_life_predictions")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT date, avg(remaining_life) as Actual_Life, avg(remaining_life_predicted) as Predicted_Life 
# MAGIC FROM turbine_life_predictions 
# MAGIC WHERE deviceid='WindTurbine-1' 
# MAGIC GROUP BY date ORDER BY date

# COMMAND ----------

# MAGIC %md The models to predict remaining useful life have been trained and logged by MLflow. We can now move on to model deployment in AzureML.

# COMMAND ----------

# MAGIC %md ## 2. Model Deployment
# MAGIC 
# MAGIC After choosing a model that best fits our needs, we can then go ahead and kick off its operationalization proccess.
# MAGIC 
# MAGIC The first step is to register it to the **Model Registry**, where we can version, manage its life cycle with an workflow and track/audit all changes.<br><br>
# MAGIC 
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/ml_3.jpg)

# COMMAND ----------

# Retrieve the remaining_life and power_output experiments on WindTurbine-1 and get the best performing model (min RMSE)

turbine = "WindTurbine-1"
power_model = "power_prediction"
life_model = "life_prediction"

best_life_model = mlflow.search_runs(filter_string=f'params.deviceid="{turbine}" and params.model="{life_model}"')\
  .dropna().sort_values("metrics.train-rmse")['run_id'].iloc[0]
best_power_model = mlflow.search_runs(filter_string=f'params.deviceid="{turbine}" and params.model="{power_model}"')\
  .dropna().sort_values("metrics.train-rmse")['run_id'].iloc[0]

print(f'Best Life Model: {best_life_model}')
print(f'Best Power Model: {best_power_model}')

# COMMAND ----------

mlflow.register_model(
  model_uri=f'runs:/{best_power_model}/model',
  name='VR IIoT Power Prediction'
)

# COMMAND ----------

mlflow.register_model(
  model_uri=f'runs:/{best_life_model}/model',
  name='VR IIoT Remaining Life'
)

# COMMAND ----------

# MAGIC %md ## 3. Model Inference
# MAGIC 
# MAGIC We can now score this model in batch, streaming or via REST API calls (in case we choose to enable Serverless Model Serving) so we can consume it from any other applications or tools, like Power BI.
# MAGIC 
# MAGIC In this example, we'll batch score new data in order to make decisions about our production.<br><br>
# MAGIC 
# MAGIC ![](/files/shared_uploads/victor.rodrigues@databricks.com/ml_4.jpg)

# COMMAND ----------

# MAGIC %md ### 3a. Power Output

# COMMAND ----------

power_udf = mlflow.pyfunc.spark_udf(spark, 'models:/VR IIoT Power Prediction/1')

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','remaining_life']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'power_6_hours_ahead'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('feature_table').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}']).filter(f'deviceid="{turbine}"')

# Score model
scored_df = feature_df.withColumn(
  prediction_col,
  power_udf(*feature_cols)
)

display(scored_df)

# COMMAND ----------

# MAGIC %md ### 3b. Remaining Life

# COMMAND ----------

life_udf = mlflow.pyfunc.spark_udf(spark, 'models:/VR IIoT Remaining Life/1')

# COMMAND ----------

# Create a Spark Dataframe that contains the features and labels we need
non_feature_cols = ['date','window','deviceid','winddirection','power_6_hours_ahead_predicted']
feature_cols = ['angle','rpm','temperature','humidity','windspeed','power','age']
label_col = 'remaining_life'
prediction_col = label_col + '_predicted'

# Read in our feature table and select the columns of interest
feature_df = spark.table('turbine_power_predictions').selectExpr(non_feature_cols + feature_cols + [label_col] + [f'0 as {prediction_col}']).filter(f'deviceid="{turbine}"')

# Score model
scored_df = feature_df.withColumn(
  prediction_col,
  life_udf(*feature_cols)
)

display(scored_df)

# COMMAND ----------

# MAGIC %md ## 4. Asset Optimization
# MAGIC We can now identify the optimal operating conditions for maximizing power output while also maximizing asset useful life. 
# MAGIC 
# MAGIC \\(Revenue = Price\displaystyle\sum_{d=1}^{365} Power_d\\)
# MAGIC 
# MAGIC \\(Cost = {365 \over Life} Price \displaystyle\sum_{h=1}^{24} Power_h \\)
# MAGIC 
# MAGIC \\(Profit = Revenue - Cost\\)
# MAGIC 
# MAGIC \\(Power_t\\) and \\(Life\\) will be calculated by scoring many different setting values. The results can be visualized to identify the optimal setting that yields the highest profit.

# COMMAND ----------

# Create a baseline scenario — ideally, it would reflect current operating conditions
scenario = {
  'angle':None,
  'rpm':8.0,
  'temperature':25.0,
  'humidity':50.0,
  'windspeed':5.0,
  'power':150.0,
  'age':10.0
}

# Generate 15 different RPM configurations to be evaluated
scenarios = []
for setting in range(1,15):
  this_scenario = scenario.copy()
  this_scenario['angle'] = float(setting)
  scenarios.append(this_scenario)
scenarios_df = spark.createDataFrame(scenarios)

# Calculalte the Revenue, Cost and Profit generated for each RPM configuration
opt_df = (scenarios_df
  .withColumn('Expected Power', power_udf(*feature_cols))
  .withColumn('Expected Life',life_udf(*feature_cols))
  .withColumn('Revenue', col('Expected Power') * lit(24*365))
  .withColumn('Cost', col('Expected Power') * lit(24*365) / col('Expected Life'))
  .withColumn('Profit', col('Revenue') - col('Cost'))
)

display(opt_df)

# COMMAND ----------

# MAGIC %md The optimal operating parameters for **WindTurbine-1** given the specified weather conditions is **7 degrees** for generating a maximum profit of **$1.26M**! Your results may vary due to the random nature of the sensor readings. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Serving and Visualization
# MAGIC Now that our models are created and the data is scored, we can use Databricks SQL Warehouses with PowerBI to perform data warehousing and analyltic reporting to generate a report like the one below without needing to move and replicate your data.
# MAGIC 
# MAGIC <br><img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/PBI_report.gif" width=800>
