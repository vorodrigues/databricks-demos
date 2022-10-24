# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./resources/00-setup $reset_all=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #MegaCorp | Databricks: Data Science and Machine Learning
# MAGIC 
# MAGIC There are many architectures that can make data science and machine learning tasks challening and difficult:
# MAGIC 1. Machine learning experimentation can be cumbersome and slow
# MAGIC 2. Productionalizing and deploying models can require specialized knowledge
# MAGIC 3. Massive datasets make training models infinitely slow
# MAGIC 
# MAGIC With Databricks, however, MegaCorp can leverage the Lakehouse and MLflow to:
# MAGIC 1. Easily and quickly perform machine learning experiments
# MAGIC 2. Design, test, and deploy models all from within the same tool using Notebooks
# MAGIC 3. Use distributed ML to tackle huge datasets
# MAGIC 
# MAGIC Let's see what this might look like in action by building a classification model that predicts the status of MegaCorp's gas turbines.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Exploration
# MAGIC What do the distributions of sensor readings look like for our turbines?

# COMMAND ----------

# DBTITLE 1,Read sensor data
dataset = spark.read.table("turbine_gold_for_ml")
display(dataset)

# COMMAND ----------

# DBTITLE 1,Visualize Feature Distributions
import seaborn as sns

sns.pairplot(dataset.limit(1000).toPandas()[[var for var in dataset.columns if var not in ['ID','TORQUE']]], hue="status")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Train Model and Track Experiments

# COMMAND ----------

# DBTITLE 1,Train and Log Model
# Once the data is ready, we can train a model
import mlflow
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.mllib.evaluation import MulticlassMetrics

# Enable autolog
mlflow.autolog()

with mlflow.start_run():

  training, test = dataset.limit(1000).randomSplit([0.9, 0.1], seed = 5)
  
  gbt = GBTClassifier(labelCol="label", featuresCol="features").setMaxIter(5)
  grid = ParamGridBuilder().addGrid(gbt.maxDepth, [3,4,5,10,15,25,30]).build()

  metrics = MulticlassClassificationEvaluator(metricName="f1")
  cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=metrics, numFolds=2)

  featureCols = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]
  stages = [VectorAssembler(inputCols=featureCols, outputCol="va"), StandardScaler(inputCol="va", outputCol="features"), StringIndexer(inputCol="status", outputCol="label"), cv]
  pipeline = Pipeline(stages=stages)

  pipelineTrained = pipeline.fit(training)
  
  predictions = pipelineTrained.transform(test)
  metrics = MulticlassMetrics(predictions.select(['prediction', 'label']).rdd)
  
  # Log your metrics (precision, recall, f1 etc) 
  mlflow.log_metric("accuracy", metrics.accuracy)
  
  # Log your model under "turbine_gbt"
  mlflow.spark.log_model(pipelineTrained, "turbine_gbt")
  mlflow.set_tag("model", "turbine_gbt")

# COMMAND ----------

# MAGIC %md ## Save to the model registry
# MAGIC Get the model having the best metrics.accuracy from the registry

# COMMAND ----------

# DBTITLE 1,Register champion model to Model Registry
# Get the best model from the registry
best_model = mlflow.search_runs(filter_string='tags.model="turbine_gbt" and attributes.status = "FINISHED"').sort_values("metrics.accuracy").iloc[0]

# Register that model to MLFLow registry
model_registered = mlflow.register_model(f"runs:/{best_model.run_id}/turbine_gbt", "vr_turbine_model")

# COMMAND ----------

# DBTITLE 1,Move model to Production stage
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")

client.transition_model_version_stage(name="vr_turbine_model", version=model_registered.version, stage="Production")

# COMMAND ----------

# MAGIC %md #Deploying & using our model in production
# MAGIC 
# MAGIC Now that our model is in our MLFlow registry, we can start to use it in a production pipeline.

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

# DBTITLE 1,Retrieve production model from Model Registry
import mlflow.pyfunc

# Create UDF
model_uri = "models:/vr_turbine_model/production"
predict_status = mlflow.pyfunc.spark_udf(spark, model_uri)

# Register UDF so we can call it from SQL
spark.udf.register("predict_status", predict_status)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Call the model in SQL using the udf registered as function 
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   predict_status(struct(AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10)) AS status_forecast 
# MAGIC FROM turbine_gold_for_ml

# COMMAND ----------

# MAGIC %md 
# MAGIC #MegaCorp | Databricks: Data Science and Machine Learning
# MAGIC 
# MAGIC We looked at how we can:
# MAGIC - Perform data science and machine learning tasks using the same platform we use for everything else
# MAGIC - Use SparkML for distributed machine learning
# MAGIC - Use MLflow to simplify experimentation and track, evaluate, and monitor your models
# MAGIC - Easily deploy models using Notebooks
# MAGIC 
# MAGIC All of these together would significantly increase MegaCorp's ROI on machine learning and data science tasks. Specifically:
# MAGIC - No more slow and naive ML experimentation!
# MAGIC - Distributed and scalable ML means huge datasets are no longer a problem!
# MAGIC - Pure SQL teams can use SQL to access estimators!
# MAGIC 
# MAGIC ###### And By The Way
# MAGIC If this felt a little bit like too much code to write, especially considering we didn't even do any of the cool stuff like hyperparameter tuning, remember that there's UI options for just about everything we did and more, and that we have a number of [Solutions Accelerators](https://databricks.com/solutions/accelerators) that can kickstart your ML journey.
