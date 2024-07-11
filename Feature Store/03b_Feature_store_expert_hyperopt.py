# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Feature store - full example for Travel recommendation
# MAGIC
# MAGIC This notebook will illustrate the full capabilities of the feature store to provide recommendation for a Travel website and increase or conversion rate.
# MAGIC
# MAGIC If you don't know about feature store yet, we recommand you start with the first version to cover the basics.
# MAGIC
# MAGIC We'll go in details and introduce:
# MAGIC
# MAGIC * Streaming feature store tables, to refresh your data in near realtime
# MAGIC * Live feature computation, reusing the same code for training and inference with the Pandas On Spark APIs (current booking time & distance to location)
# MAGIC * Point in time lookup with multiple feature table
# MAGIC * Automl to bootstrap model creation
# MAGIC
# MAGIC In addition, we'll see how we can perform realtime inference:
# MAGIC
# MAGIC * Create online backed for the feature store table
# MAGIC * Create online functions to add additional, realtime feature (distance and date)
# MAGIC * Deploy the model using realtime serverless Model Serving fetching features in the online store
# MAGIC * Send realtime REST queries for live inference.
# MAGIC
# MAGIC *Note: For more detail on this notebook, you can read the [Databricks blog post](https://www.databricks.com/blog/2023/02/16/best-practices-realtime-feature-computation-databricks.html) .*

# COMMAND ----------

# MAGIC %md # 0: Setup environment

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering==0.2.0 databricks-sdk==0.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-expert

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1: Create the feature tables
# MAGIC
# MAGIC The first step is to create our feature store tables. We'add a new datasource that we'll consume in streaming, making sure our Feature Table is refreshed in near realtime.
# MAGIC
# MAGIC In addition, we'll compute the "on-demande" feature (distance between the user and a destination, booking time) using the pandas API during training, this will allow us to use the same code for realtime inferences.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-flow-training.png?raw=true" width="1200px"/>

# COMMAND ----------

# MAGIC %md ## Review dataset

# COMMAND ----------

# MAGIC %sql SELECT * FROM travel_purchase 

# COMMAND ----------

# MAGIC %sql SELECT * FROM destination_location

# COMMAND ----------

# MAGIC %md ## Compute batch features
# MAGIC
# MAGIC Calculate the aggregated features from the vacation purchase logs for destination and users. The destination features include popularity features such as impressions, clicks, and pricing features like price at the time of booking. The user features capture the user profile information such as past purchased price. Because the booking data does not change very often, it can be computed once per day in batch.

# COMMAND ----------

#Delete potential existing tables to reset all the demo
delete_fss(catalog, db, ["user_features", "destination_features", "destination_location_features", "availability_features"])

from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

# Reuse the same features as the previous example 02_Feature_store_advanced
# For more details these functions are available under ./_resources/00-init-expert
user_features_df = create_user_features(spark.table('travel_purchase'))
fe.create_table(name=f"{catalog}.{db}.user_features",
                primary_keys=["user_id", "ts"], 
                timestamp_keys="ts", 
                df=user_features_df, 
                description="User Features")

destination_features_df = destination_features_fn(spark.table('travel_purchase'))
fe.create_table(name=f"{catalog}.{db}.destination_features", 
                primary_keys=["destination_id", "ts"], 
                timestamp_keys="ts", 
                df=destination_features_df, 
                description="Destination Popularity Features")


#Add the destination location dataset
destination_location = spark.table("destination_location")
fe.create_table(name=f"{catalog}.{db}.destination_location_features", 
                primary_keys="destination_id", 
                df=destination_location, 
                description="Destination location features.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute streaming features
# MAGIC
# MAGIC Availability of the destination can hugely affect the prices. Availability can change frequently especially around the holidays or long weekends during busy season. This data has a freshness requirement of every few minutes, so we use Spark structured streaming to ensure data is fresh when doing model prediction. 

# COMMAND ----------

# MAGIC %md <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/realtime/streaming.png"/>

# COMMAND ----------

spark.sql('CREATE VOLUME IF NOT EXISTS feature_store_volume')
destination_availability_stream = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json") #Could be "kafka" to consume from a message queue
  .option("cloudFiles.inferSchema", "true")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .option("cloudFiles.schemaHints", "event_ts timestamp, booking_date date, destination_id int")
  .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{db}/feature_store_volume/stream/availability_schema")
  .option("cloudFiles.maxFilesPerTrigger", 100) #Simulate streaming
  .load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json")
  .drop("_rescued_data")
  .withColumnRenamed("event_ts", "ts")
)

DBDemos.stop_all_streams_asynch(sleep_time=30)
display(destination_availability_stream)

# COMMAND ----------

fe.create_table(
    name=f"{catalog}.{db}.availability_features", 
    primary_keys=["destination_id", "booking_date", "ts"],
    timestamp_keys=["ts"],
    schema=destination_availability_stream.schema,
    description="Destination Availability Features"
)

# Now write the data to the feature table in "merge" mode using a stream
fe.write_table(
    name=f"{catalog}.{db}.availability_features", 
    df=destination_availability_stream,
    mode="merge",
    checkpoint_location= f"/Volumes/{catalog}/{db}/feature_store_volume/stream/availability_checkpoint",
    trigger={'once': True} #Refresh the feature store table once, or {'processingTime': '1 minute'} for every minute-
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Compute on-demand live features
# MAGIC
# MAGIC User location is a context feature that is captured at the time of the query. This data is not known in advance. 
# MAGIC
# MAGIC Derivated features can be computed from this location. For example, user distance from destination can only be computed in realtime at the prediction time.
# MAGIC
# MAGIC This introduce a new challenge, we now have to link some function to transform the data and make sure the same is being used for training and inference (batch or realtime). 
# MAGIC
# MAGIC To solve this, Databricks introduced Feature Spec. With Feature Spec, you can create custom function (SQL/PYTHON) to transform your data into new features, and link them to your model and feature store.
# MAGIC
# MAGIC Because it's shipped as part of your FeatureLookup definition, the same code will be used at inference time, offering a garantee that we compute the feature the same way, and adding flexibility while increasing model version.
# MAGIC
# MAGIC Note that this function will be available as `catalog.schema.distance_udf` in the browser.

# COMMAND ----------

# DBTITLE 1,Define a function to compute distance 
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION distance_udf(lat1 DOUBLE, lon1 DOUBLE, lat2 DOUBLE, lon2 DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculate hearth distance from latitude and longitude'
# MAGIC AS $$
# MAGIC   import numpy as np
# MAGIC   dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
# MAGIC   a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
# MAGIC   return 2 * 6371 * np.arcsin(np.sqrt(a))
# MAGIC $$

# COMMAND ----------

# DBTITLE 1,Try the function to compute the distance between a user and a destination
# MAGIC %sql
# MAGIC SELECT distance_udf(user_latitude, user_longitude, latitude, longitude) AS hearth_distance, *
# MAGIC     FROM destination_location_features
# MAGIC         JOIN destination_features USING (destination_id)
# MAGIC         JOIN user_features USING (ts)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 2: Train a custom model with batch, on-demand and streaming features
# MAGIC
# MAGIC That's all we have to do. We're now ready to train our model with this new feature.
# MAGIC
# MAGIC *Note: In a typical deployment, you would add more functions such as timestamp features (cos/sin for the hour/day of the week) etc.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get ground-truth training labels and key + timestamp

# COMMAND ----------

# Split to define a training and inference set
training_keys = spark.table('travel_purchase').select('ts', 'purchased', 'destination_id', 'user_id', 'user_latitude', 'user_longitude', 'booking_date')
training_df = training_keys.where("ts < '2022-11-23'")
test_df = training_keys.where("ts >= '2022-11-23'").cache()

display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Create the training set
# MAGIC
# MAGIC Note the use of `FeatureFunction`, pointing to the new distance_udf function that we saved in Unity Catalog.

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_function import FeatureFunction
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup

fe = FeatureEngineeringClient()

feature_lookups = [ # Grab all useful features from different feature store tables
  FeatureLookup(
      table_name="user_features", 
      lookup_key="user_id",
      timestamp_lookup_key="ts",
      feature_names=["mean_price_7d"]
  ),
  FeatureLookup(
      table_name="destination_features", 
      lookup_key="destination_id",
      timestamp_lookup_key="ts"
  ),
  FeatureLookup(
      table_name="destination_location_features",  
      lookup_key="destination_id",
      feature_names=["latitude", "longitude"]
  ),
  FeatureLookup(
      table_name="availability_features", 
      lookup_key=["destination_id", "booking_date"],
      timestamp_lookup_key="ts",
      feature_names=["availability"]
  ),
  # Add our function to compute the distance between the user and the destination 
  FeatureFunction(
      udf_name="distance_udf",
      input_bindings={"lat1": "user_latitude", "lon1": "user_longitude", "lat2": "latitude", "lon2": "longitude"},
      output_name="distance"
  )]

#Create the training set
training_set = fe.create_training_set(
    df=training_df,
    feature_lookups=feature_lookups,
    exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'],
    label='purchased'
)

# COMMAND ----------


training_set_df = training_set.load_df()
#Let's cache the training dataset for automl (to avoid recomputing it everytime)
training_features_df = training_set_df.cache()

display(training_features_df)

# COMMAND ----------

# MAGIC %md ## Tune XGBoost with Hyperopt

# COMMAND ----------

# MAGIC %md ### Prepare data

# COMMAND ----------

import sklearn
from sklearn.model_selection import train_test_split

# split train and test datasets
train, test = train_test_split(training_set_df.toPandas(), train_size=0.7)

# separate features and labels
X_train_raw = train.drop('purchased', axis=1)
y_train = train['purchased']

# separate features and labels
X_test_raw = test.drop('purchased', axis=1)
y_test = test['purchased']

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn import set_config
from category_encoders.woe import WOEEncoder
set_config(display="diagram")

# Separate variables
numVars = ['user_latitude', 'user_longitude','mean_price_7d','sum_clicks_7d','sum_impressions_7d','latitude','longitude','availability','distance']
# catVars = ['brand', 'type']

# Handling numerical data
num = Pipeline(steps=[
    ('std', StandardScaler()),
    ('imp', SimpleImputer(strategy='mean'))
])

# Handling categorical data
# cat = Pipeline(steps=[
#     ('imp', SimpleImputer(strategy='most_frequent')),
#     ('enc', WOEEncoder())
# ])

# Preprocessor
pre = Pipeline(steps=[(
  'preprocessor', ColumnTransformer(transformers=[
    ('num', num, numVars),
    # ('cat', cat, catVars)
  ])
)])

# Transform data
X_train = pre.fit_transform(X_train_raw, y_train)
X_test = pre.transform(X_test_raw)

display(pre)

# COMMAND ----------

# MAGIC %md ### Define Experiment
# MAGIC
# MAGIC The XGBClassifier makes available a [wide variety of hyperparameters](https://xgboost.readthedocs.io/en/latest/python/python_api.html#xgboost.XGBClassifier) which can be used to tune model training.  Using some knowledge of our data and the algorithm, we might attempt to manually set some of the hyperparameters. But given the complexity of the interactions between them, it can be difficult to know exactly which combination of values will provide us the best model results.  It's in scenarios such as these that we might perform a series of model runs with different hyperparameter settings to observe how the model responds and arrive at an optimal combination of values.
# MAGIC
# MAGIC Using hyperopt, we can automate this task, providing the hyperopt framework with a range of potential values to explore.  Calling a function which trains the model and returns an evaluation metric, hyperopt can through the available search space to towards an optimum combination of values.
# MAGIC
# MAGIC For model evaluation, we will be using the Area Under the Curve (AUC) score which increases towards 1.0 as the model improves.  Because hyperopt recognizes improvements as our evaluation metric declines, we will use `-1 * AUC` as our loss metric within the framework. 
# MAGIC
# MAGIC Putting this all together, we might arrive at model training and evaluation function as follows:

# COMMAND ----------

sources = []
for lookup in feature_lookups:
  try:
    table_name = lookup.table_name
    table_version = spark.sql(f'describe history {table_name}').head(1)[0]['version']
    print(f'{table_name} : {table_version}')
    sources.append({'table_name': table_name, 'table_version': table_version})
  except:
    pass

# COMMAND ----------

from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score

def evaluate_model(hyperopt_params):
  
  # accesss replicated input data
  X_train_input = X_train
  y_train_input = y_train
  X_test_input = X_test
  y_test_input = y_test  
  
  # configure model parameters
  params = hyperopt_params
  
  if 'max_depth' in params: params['max_depth']=int(params['max_depth'])   # hyperopt supplies values as float but must be int
  if 'min_child_weight' in params: params['min_child_weight']=int(params['min_child_weight']) # hyperopt supplies values as float but must be int
  if 'max_delta_step' in params: params['max_delta_step']=int(params['max_delta_step']) # hyperopt supplies values as float but must be int
  # all other hyperparameters are taken as given by hyperopt
  
  # instantiate model with parameters
  model = XGBClassifier(**params)
  
  # train
  model.fit(X_train_input, y_train_input)
  
  # predict
  prob_train = model.predict_proba(X_train_input)
  prob_test = model.predict_proba(X_test_input)
  
  # evaluate
  auc_train = roc_auc_score(y_train_input, prob_train[:,1])
  auc_test = roc_auc_score(y_test_input, prob_test[:,1])
  
  # log model
  mlflow.log_metric('train_auc', auc_train)
  mlflow.log_metric('test_auc', auc_test)

  # set tags
  mlflow.set_tag('sources', sources)
  
  # invert metric for hyperopt
  loss = -1 * auc_test  
  
  # return results
  return {'loss': loss, 'status': STATUS_OK}

# COMMAND ----------

# MAGIC %md ### Define Search Space
# MAGIC The hyperparameter values delivered to the function by hyperopt are derived from a search space defined in the next cell.  Each hyperparameter in the search space is defined using an item in a dictionary, the name of which identifies the hyperparameter and the value of which defines a range of potential values for that parameter.  When defined using *hp.choice*, a parameter is selected from a predefined list of values.  When defined *hp.loguniform*, values are generated from a continuous range of values.  When defined using *hp.quniform*, values are generated from a continuous range but truncated to a level of precision identified by the third argument  in the range definition.  Hyperparameter search spaces in hyperopt may be defined in many other ways as indicated by the library's [online documentation](https://github.com/hyperopt/hyperopt/wiki/FMin#21-parameter-expressions):  

# COMMAND ----------

from sklearn.utils.class_weight import compute_class_weight
from hyperopt import hp, fmin, tpe, SparkTrials, STATUS_OK, space_eval
import numpy as np

# define minimum positive class scale factor (as shown in previous notebook)
weights = compute_class_weight(
  'balanced', 
  classes=np.unique(y_train), 
  y=y_train
)
scale = weights[1]/weights[0]

# define hyperopt search space
search_space = {
    'max_depth' : hp.quniform('max_depth', 1, 10, 1)                                  # depth of trees (preference is for shallow trees or even stumps (max_depth=1))
    ,'learning_rate' : hp.loguniform('learning_rate', np.log(0.01), np.log(0.40))     # learning rate for XGBoost
    ,'gamma': hp.quniform('gamma', 0.0, 1.0, 0.001)                                   # minimum loss reduction required to make a further partition on a leaf node
    ,'min_child_weight' : hp.quniform('min_child_weight', 1, 20, 1)                   # minimum number of instances per node
    ,'subsample' : hp.loguniform('subsample', np.log(0.1), np.log(1.0))               # random selection of rows for training,
    ,'colsample_bytree' : hp.loguniform('colsample_bytree', np.log(0.1), np.log(1.0)) # proportion of columns to use per tree
    ,'colsample_bylevel': hp.loguniform('colsample_bylevel', np.log(0.1), np.log(1.0))# proportion of columns to use per level
    ,'colsample_bynode' : hp.loguniform('colsample_bynode', np.log(0.1), np.log(1.0)) # proportion of columns to use per node
    ,'scale_pos_weight' : hp.loguniform('scale_pos_weight', np.log(scale), np.log(scale*1.1))   # weight to assign positive label to manage imbalance
}

# COMMAND ----------

# MAGIC %md ### Run Experiment
# MAGIC
# MAGIC The remainder of the model evaluation function is fairly straightforward.  We simply train and evaluate our model and return our loss value, *i.e.* `-1 * AUC`, as part of a dictionary interpretable by hyperopt.  Based on returned values, hyperopt will generate a new set of hyperparameter values from within the search space definition with which it will attempt to improve our metric. We will limit the number of hyperopt evaluations to 250 simply based on a few trail runs we performed (not shown).  The larger the potential search space and the degree to which the model (in combination with the training dataset) responds to different hyperparameter combinations determines how many iterations are required for hyperopt to arrive at locally optimal values.  You can examine the output of the hyperopt run to see how our loss metric slowly improves over the course of each of these evaluations:

# COMMAND ----------

import mlflow

# perform evaluation
with mlflow.start_run(run_name='XGBClassifer'):
  argmin = fmin(
    fn=evaluate_model,
    space=search_space,
    algo=tpe.suggest,
    max_evals=50,
    trials=SparkTrials(parallelism=8), # set to the number of available cores
    verbose=True
  )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Save best model to Unity Catalog
# MAGIC
# MAGIC Next, we'll get Automl best model and add it to our registry. Because we the feature store to keep track of our model & features, we'll log the best model as a new run using the `FeatureStoreClient.log_model()` function.
# MAGIC
# MAGIC Because our model need live features, we'll wrap our best model with `OnDemandComputationModelWrapper`
# MAGIC
# MAGIC Because we re-define the `.predict()` function, the wrapper will automatically add our live feature depending on the input.

# COMMAND ----------

model_name = "vr_travel_expert"
model_full_name = f"{catalog}.{db}.{model_name}"

# COMMAND ----------

from mlflow.models.signature import infer_signature

# train model with optimal settings 
with mlflow.start_run(run_name='XGB Final Model') as run:
  
  # capture run info for later use
  run_id = run.info.run_id
  run_name = run.data.tags['mlflow.runName']
   
  # configure params
  params = space_eval(search_space, argmin)
  if 'max_depth' in params: params['max_depth']=int(params['max_depth'])       
  if 'min_child_weight' in params: params['min_child_weight']=int(params['min_child_weight'])
  if 'max_delta_step' in params: params['max_delta_step']=int(params['max_delta_step'])
  if 'scale_pos_weight' in params: params['scale_pos_weight']=int(params['scale_pos_weight'])    
  params['tree_method']='hist'
  params['predictor']='cpu_predictor'
  mlflow.log_params(params)
  
  # train
  model = Pipeline(steps=[
    ('pre', pre),
    ('clf', XGBClassifier(**params))
  ])
  model.fit(X_train_raw, y_train)
  
  # predict
  prob_train = model.predict_proba(X_train_raw)
  prob_test = model.predict_proba(X_test_raw)
  
  # evaluate
  auc_train = roc_auc_score(y_train, prob_train[:,1])
  auc_test = roc_auc_score(y_test, prob_test[:,1])

  #Use the feature store client to log our best model
  fe.log_model(
    model=model, # object of your model
    artifact_path="model", #name of the Artifact under MlFlow
    flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a Sklearn Flavour)
    training_set=training_set, # training set you used to train your model with AutoML
    input_example=X_train_raw.head(10), # Dataset example (Pandas dataframe)
    registered_model_name=model_full_name, # register your best model
    # conda_env=env
  )
  mlflow.log_metric('train_auc', auc_train)
  mlflow.log_metric('test_auc', auc_test)
  mlflow.set_tag('sources', sources)

  print('Model logged under run_id "{0}" with AUC score of {1:.5f}'.format(run_id, auc_test))
  display(model)

# COMMAND ----------

# MAGIC %md ### Flag model as Production

# COMMAND ----------

latest_model = get_last_model_version(model_full_name)
#Move it in Production
production_alias = "production"
if len(latest_model.aliases) == 0 or latest_model.aliases[0] != production_alias:
  print(f"updating model {latest_model.version} to Production")
  mlflow_client = MlflowClient(registry_uri="databricks-uc")
  mlflow_client.set_registered_model_alias(model_full_name, production_alias, version=latest_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC Our model is ready! you can open the Unity Catalog Explorer to review it.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Batch score test set
# MAGIC
# MAGIC Let's make sure our model is working as expected and try to score our test dataset

# COMMAND ----------

scored_df = fe.score_batch(model_uri=f"models:/{model_full_name}@{production_alias}", df=test_df, result_type="string")
display(scored_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Accuracy

# COMMAND ----------

from sklearn.metrics import accuracy_score

# simply convert the original probability predictions to true or false
pd_scoring = scored_df.selectExpr("purchased", "case when prediction = 1 then true else false end as prediction").toPandas()
print("Accuracy: ", accuracy_score(pd_scoring["purchased"], pd_scoring["prediction"]))

# COMMAND ----------

# MAGIC %md 
# MAGIC # 3: Real time serving and inference
# MAGIC
# MAGIC We're now going to deploy our model supporting real time inference.
# MAGIC
# MAGIC To provide inference with ms response time, we need to be able to lookup the features for a single user or destination with low latencies.
# MAGIC
# MAGIC To do that, we'll deploy online stores. These are fully serverless and managed by Databricks. You can think of them as a  (K/V store, such as Mysql, dynamoDB, CosmoDB...).
# MAGIC
# MAGIC Databricks will automatically synchronize the Delta Live Table content with the online store (you can chose to trigger the update yourself or do it on a schedule).
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-flow.png?raw=true" width="1200px" />

# COMMAND ----------

# MAGIC %md ## Publish feature tables as Databricks-managed online tables
# MAGIC
# MAGIC By publishing our tables to a Databricks-managed online table, Databricks will automatically synchronize the data written to your feature store to the realtime backend.
# MAGIC
# MAGIC Apart from Databricks-managed online tables, Databricks also supports different third-party backends. You can find more information about integrating Databricks feature tables with third-party online stores in the links below.
# MAGIC
# MAGIC * AWS dynamoDB ([doc](https://docs.databricks.com/machine-learning/feature-store/online-feature-stores.html))
# MAGIC * Azure cosmosDB [doc](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-stores)
# MAGIC
# MAGIC
# MAGIC **Important note for Azure users:** please make sure you have installed [Azure Cosmos DB Apache Spark 3 OLTP Connector for API for NoSQL](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-java-spark-v3) (i.e. `com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.17.2`) to your cluster before running this demo.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def create_online_table(table_name, pks, timeseries_key=None):
    w = WorkspaceClient()
    online_table_name = table_name+'_online'
    if not online_table_exists(online_table_name):
        from databricks.sdk.service import catalog as c
        print(f"Creating online table for {online_table_name}...")
        spark.sql(f'ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)')
        spec = c.OnlineTableSpec(source_table_full_name=table_name, primary_key_columns=pks, run_triggered={'triggered': 'true'}, timeseries_key=timeseries_key)
        w.online_tables.create(name=online_table_name, spec=spec)

create_online_table(f"{catalog}.{db}.user_features",                 ["user_id"], "ts")
create_online_table(f"{catalog}.{db}.destination_features",          ["destination_id"], "ts")
create_online_table(f"{catalog}.{db}.destination_location_features", ["destination_id"])
create_online_table(f"{catalog}.{db}.availability_features",         ["destination_id", "booking_date"], "ts")

#wait for all the tables to be online
wait_for_online_tables(catalog, db, ["user_features_online", "destination_features_online", "destination_location_features_online", "availability_features_online"])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Deploy Serverless Model serving Endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-model-serving.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Once our Model, Function and Online feature store are in Unity Catalog, we can deploy the model as using Databricks Model Serving.
# MAGIC
# MAGIC This will provide a REST API to serve our model in realtime.
# MAGIC
# MAGIC ### Enable model inference via the UI
# MAGIC
# MAGIC After calling `log_model`, a new version of the model is saved. To provision a serving endpoint, follow the steps below.
# MAGIC
# MAGIC 1. Within the Machine Learning menu, click [Serving menu](ml/endpoints) in the left sidebar. 
# MAGIC 2. Create a new endpoint, select the most recent model version from Unity Catalog and start the serverless model serving
# MAGIC
# MAGIC You can use the UI, in this demo We will use the API to programatically start the endpoint:

# COMMAND ----------

# DBTITLE 1,Deploy model
from databricks.sdk import WorkspaceClient

endpoint_name = "vr_travel_expert_endpoint"
wc = WorkspaceClient()
served_models =[ServedModelInput(model_full_name, model_version=latest_model.version, workload_size=ServedModelInputWorkloadSize.SMALL, scale_to_zero_enabled=True)]
try:
    print(f'Creating endpoint {endpoint_name} with latest version...')
    wc.serving_endpoints.create_and_wait(endpoint_name, config=EndpointCoreConfigInput(served_models=served_models))
except Exception as e:
    if 'already exists' in str(e):
        print(f'Endpoint exists, updating with latest model version...')
        wc.serving_endpoints.update_config_and_wait(endpoint_name, served_models=served_models)
    else: 
        raise e

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-model-serving-inference.png?raw=true" style="float: right" width="700"/>
# MAGIC
# MAGIC Once our model deployed, you can easily test your model using the Model Serving endpoint UI.
# MAGIC
# MAGIC Let's call it using the REST API directly.
# MAGIC
# MAGIC The endpoint will answer in millisec, what will happen under the hood is the following:
# MAGIC
# MAGIC * The endpoint receive the REST api call
# MAGIC * It calls our 4 online table to get the features
# MAGIC * Call the `distance_udf` function to compute the distance
# MAGIC * Call the ML model
# MAGIC * Returns the final answer

# COMMAND ----------

# DBTITLE 1,Query endpoint
from databricks.sdk import WorkspaceClient

wc = WorkspaceClient()
endpoint_name = "vr_travel_expert_endpoint"

lookup_keys = test_df.drop('purchased').limit(3).toPandas().astype({'ts': 'str', 'booking_date': 'str'}).to_dict(orient="records")
print(f'Compute the propensity score for these customers: {lookup_keys}')
#Query the endpoint
for i in range(3):
    starting_time = timeit.default_timer()
    inferences = wc.serving_endpoints.query(endpoint_name, inputs=lookup_keys)
    print(f"Inference time, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms")
    print(inferences)

# COMMAND ----------

# MAGIC %md # Optional: Deploy our Function as Feature Spec to compute transformations in realtime
# MAGIC
# MAGIC Our function can be saved as a Feature Spec and deployed standalone Model Serving Endpoint to serve any transformation.
# MAGIC
# MAGIC Here is an example on how you can create a feature spec to compute the distance between 2 points.
# MAGIC
# MAGIC This feature spec will do the lookup for you and call the `distance_df` function (as define in the `feature_lookups`).
# MAGIC
# MAGIC Once the feature spec deployed, you can use the [Serving Endpoint menu](ml/endpoints) to create a new endpoint.

# COMMAND ----------

# DBTITLE 1,Save the feature spec within Unity Catalog
feature_spec_name = f"{catalog}.{db}.travel_feature_spec"
try:
    fe.create_feature_spec(name=feature_spec_name, features=feature_lookups, exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'])
except Exception as e:
    if "RESOURCE_ALREADY_EXISTS" not in str(e): raise e

# COMMAND ----------

# DBTITLE 1,Create the endpoint using the API
from databricks.feature_engineering.entities.feature_serving_endpoint import AutoCaptureConfig, EndpointCoreConfig, ServedEntity

# Create endpoint
feature_endpoint_name = "dbdemos-fse-travel-spec"
try: 
    status = fe.create_feature_serving_endpoint(name=feature_endpoint_name, 
                                                config=EndpointCoreConfig(served_entities=ServedEntity(scale_to_zero_enabled= True, feature_spec_name=feature_spec_name)))
except Exception as e:
    if "already exists" not in str(e): raise e

ep = wait_for_feature_endpoint_to_start(fe, feature_endpoint_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try our new feature spec endpoints. We can send send our queries using the UI or via REST API directly:

# COMMAND ----------

print(f'Compute the propensity score for these customers: {lookup_keys}')

def query_endpoint(url, lookup_keys):
    return requests.request(method='POST', headers=get_headers(), url=url, json={'dataframe_records': lookup_keys}).json()
query_endpoint(ep.url+"/invocations", lookup_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Conclusion
# MAGIC
# MAGIC In this series of demos you've learned how to use **Databricks Feature Store** in 3 different manner:
# MAGIC - `batch (offline Feature Store)`
# MAGIC - `streaming (offline Feature Store)`
# MAGIC - `real-time (online Feature Store)`
# MAGIC
# MAGIC The use of the each from the above would depend whether your organization requires scheduled batch jobs, near real-time streaming or real-time on the fly computations. 
# MAGIC
# MAGIC To summarize, if you required to have a real-time feature computations, then figure out what type of data you have, data freshness and latency requirements and make sure to:
# MAGIC
# MAGIC - Map your data to batch, streaming, and on-demand computational architecture based on data freshness requirements.
# MAGIC - Use spark structured streaming to stream the computation to offline store and online store
# MAGIC - Use on-demand computation with MLflow pyfunc
# MAGIC - Use Databricks Serverless realtime inference to perform low-latency predictions on your model
