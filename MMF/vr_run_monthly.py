# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("db", "")

catalog = dbutils.widgets.get("catalog")
db = dbutils.widgets.get("db")

# COMMAND ----------

# MAGIC %md
# MAGIC # Many Models Forecasting Demo
# MAGIC This notebook showcases how to run MMF with global models on multiple time series of monthly resolution. We will use [M4 competition](https://www.sciencedirect.com/science/article/pii/S0169207019301128#sec5) data. The descriptions here are mostly the same as the case with the [daily resolution](https://github.com/databricks-industry-solutions/many-model-forecasting/blob/main/examples/global_daily.py), so we will skip the redundant parts and focus only on the essentials.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster setup
# MAGIC
# MAGIC We recommend using a cluster with [Databricks Runtime 14.3 LTS for ML](https://docs.databricks.com/en/release-notes/runtime/14.3lts-ml.html) or above. The cluster should be single-node with one or more GPU instances: e.g. [g4dn.12xlarge [T4]](https://aws.amazon.com/ec2/instance-types/g4/) on AWS or [Standard_NC64as_T4_v3](https://learn.microsoft.com/en-us/azure/virtual-machines/nct4-v3-series) on Azure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install and import packages
# MAGIC Check out [requirements.txt](https://github.com/databricks-industry-solutions/many-model-forecasting/blob/main/requirements.txt) if you're interested in the libraries we use.

# COMMAND ----------

# MAGIC %pip install -r requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

# COMMAND ----------

import uuid
import pathlib
import pandas as pd
from datasetsforecast.m4 import M4
from mmf_sa import run_forecast

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data 
# MAGIC We are using [`datasetsforecast`](https://github.com/Nixtla/datasetsforecast/tree/main/) package to download M4 data.

# COMMAND ----------

# Number of time series
n = 100


def create_m4_monthly():
    y_df, _, _ = M4.load(directory=str(pathlib.Path.home()), group="Monthly")
    _ids = [f"M{i}" for i in range(1, n + 1)]
    y_df = (
        y_df.groupby("unique_id")
        .filter(lambda x: x.unique_id.iloc[0] in _ids)
        .groupby("unique_id")
        .apply(transform_group)
        .reset_index(drop=True)
    )
    return y_df


def transform_group(df):
    unique_id = df.unique_id.iloc[0]
    _cnt = 60  # df.count()[0]
    _start = pd.Timestamp("2018-01-01")
    _end = _start + pd.DateOffset(months=_cnt)
    date_idx = pd.date_range(start=_start, end=_end, freq="M", name="date")
    _df = (
        pd.DataFrame(data=[], index=date_idx)
        .reset_index()
        .rename(columns={"index": "date"})
    )
    _df["unique_id"] = unique_id
    _df["y"] = df[:60].y.values
    return _df


# COMMAND ----------

# MAGIC %md
# MAGIC We are going to save this data in a delta lake table. Provide catalog and database names where you want to store the data.__

# COMMAND ----------

# catalog = "mmf" # Name of the catalog we use to manage our assets
# db = "m4" # Name of the schema we use to manage our assets (e.g. datasets)
# user = spark.sql('select current_user() as user').collect()[0]['user'] # User email address

# COMMAND ----------

# Making sure that the catalog and the schema exist
_ = spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
_ = spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

(
    spark.createDataFrame(create_m4_monthly())
    .write.format("delta").mode("overwrite")
    .saveAsTable(f"{catalog}.{db}.m4_monthly_train")
)

# COMMAND ----------

# MAGIC %md Let's take a peak at the dataset:

# COMMAND ----------

display(spark.sql(f"select unique_id, count(date) as count from {catalog}.{db}.m4_monthly_train group by unique_id order by unique_id"))

# COMMAND ----------

display(
  spark.sql(f"select * from {catalog}.{db}.m4_monthly_train where unique_id in ('M1', 'M2', 'M3', 'M4', 'M5') order by unique_id, date")
  )

# COMMAND ----------

# MAGIC %md ### Models
# MAGIC Let's configure a list of models we are going to apply to our time series for evaluation and forecasting. A comprehensive list of all supported models is available in [mmf_sa/models/models_conf.yaml](https://github.com/databricks-industry-solutions/many-model-forecasting/blob/main/mmf_sa/models/models_conf.yaml). Look for the models where `model_type: global`; these are the global models we import from [neuralforecast](https://github.com/Nixtla/neuralforecast). Check their documentation for the detailed description of each model. 

# COMMAND ----------

active_models = [
    # "NeuralForecastRNN",
    # "NeuralForecastLSTM",
    # "NeuralForecastNBEATSx",
    # "NeuralForecastNHITS",
    # "NeuralForecastAutoRNN",
    # "NeuralForecastAutoLSTM",
    # "NeuralForecastAutoNBEATSx",
    "NeuralForecastAutoNHITS",
    # "NeuralForecastAutoTiDE",
    # "NeuralForecastAutoPatchTST",
]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Run MMF
# MAGIC
# MAGIC Now, we can run the evaluation and forecasting using `run_forecast` function defined in [mmf_sa/models/__init__.py](https://github.com/databricks-industry-solutions/many-model-forecasting/blob/main/mmf_sa/models/__init__.py). Refer to [README.md](https://github.com/databricks-industry-solutions/many-model-forecasting/blob/main/README.md#parameters-description) for a comprehensive description of each parameter. Make sure to set `freq="M"` in `run_forecast` function called in [examples/run_monthly.py](https://github.com/databricks-industry-solutions/many-model-forecasting/blob/main/examples/run_monthly.py).

# COMMAND ----------

from mmf_sa import run_forecast
import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)


run_forecast(
    spark=spark,
    train_data=f"{catalog}.{db}.m4_monthly_train",
    scoring_data=f"{catalog}.{db}.m4_monthly_train",
    scoring_output=f"{catalog}.{db}.monthly_scoring_output",
    evaluation_output=f"{catalog}.{db}.monthly_evaluation_output",
    model_output=f"{catalog}.{db}",
    group_id="unique_id",
    date_col="date",
    target="y",
    freq="M",
    prediction_length=3,
    backtest_months=12,
    stride=1,
    metric="smape",
    train_predict_ratio=1,
    data_quality_check=True,
    resample=False,
    active_models=active_models,
    experiment_path=f"/Users/victor.rodrigues@databricks.com/mmf/m4_monthly",
    use_case_name="vr_m4_monthly",
    run_id='test1',
    # accelerator="gpu",
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Evaluate
# MAGIC In `evaluation_output` table, the we store all evaluation results for all backtesting trials from all models.

# COMMAND ----------

display(spark.sql(f"select * from {catalog}.{db}.monthly_evaluation_output order by unique_id, model, backtest_window_start_date"))

# COMMAND ----------

# MAGIC %md ### Forecast
# MAGIC In `scoring_output` table, forecasts for each time series from each model are stored.

# COMMAND ----------

display(spark.sql(f"select * from {catalog}.{db}.monthly_scoring_output order by unique_id, model, date"))

# COMMAND ----------

display(spark.sql(f"""
SELECT unique_id, model, items.*
FROM (
    SELECT unique_id, model, EXPLODE(ARRAYS_ZIP(date, y)) as items
    FROM {catalog}.{db}.monthly_scoring_output
) ORDER BY unique_id, model, items.date
"""))
