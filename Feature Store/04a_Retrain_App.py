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

dbutils.widgets.text("exp_name", "")
dbutils.widgets.text("pct_train", "0.7")
dbutils.widgets.text("n_exp", "50")
dbutils.widgets.text("min_depth", "1")
dbutils.widgets.text("max_depth", "10")
dbutils.widgets.text("min_learning_rate", "0.01")
dbutils.widgets.text("max_learning_rate", "0.40")

# COMMAND ----------

exp_name = dbutils.widgets.get("exp_name")
pct_train = float(dbutils.widgets.get("pct_train"))
n_exp = int(dbutils.widgets.get("n_exp"))
min_depth = int(dbutils.widgets.get("min_depth"))
max_depth = int(dbutils.widgets.get("max_depth"))
min_learning_rate = float(dbutils.widgets.get("min_learning_rate"))
max_learning_rate = float(dbutils.widgets.get("max_learning_rate"))

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
train, test = train_test_split(training_set_df.toPandas(), train_size=pct_train)

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
  under = (auc_train < 0.8)
  over = (auc_test / auc_train > 1.05)
  
  # log model
  mlflow.log_metric('train_auc', auc_train)
  mlflow.log_metric('test_auc', auc_test)

  # set tags
  mlflow.set_tag('sources', sources)
  if under: mlflow.set_tag('FITTING', 'UNDER')
  if over: mlflow.set_tag('FITTING', 'OVER')
  
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
    'max_depth' : hp.quniform('max_depth', min_depth, max_depth, 1)                   # depth of trees (preference is for shallow trees or even stumps (max_depth=1))
    ,'learning_rate' : hp.loguniform('learning_rate', np.log(min_learning_rate), np.log(max_learning_rate)) # learning rate for XGBoost
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

mlflow.set_experiment(f"/Users/victor.rodrigues@databricks.com/AutoMLApp/{exp_name}")

# COMMAND ----------

from datetime import datetime

current_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
print(current_timestamp)

# COMMAND ----------

import mlflow

# perform evaluation
with mlflow.start_run(run_name=f'XGBClassifer_{current_timestamp}'):
  argmin = fmin(
    fn=evaluate_model,
    space=search_space,
    algo=tpe.suggest,
    max_evals=n_exp,
    trials=SparkTrials(parallelism=8), # set to the number of available cores
    verbose=True
  )
