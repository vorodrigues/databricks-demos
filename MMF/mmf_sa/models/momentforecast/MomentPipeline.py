from abc import ABC
import subprocess
import sys
import pandas as pd
import numpy as np
import torch
import mlflow
from mlflow.types import Schema, TensorSpec
from mlflow.models.signature import ModelSignature
from sktime.performance_metrics.forecasting import (
    MeanAbsoluteError,
    MeanSquaredError,
    MeanAbsolutePercentageError,
)
from typing import Iterator
from pyspark.sql.functions import collect_list, pandas_udf
from pyspark.sql import DataFrame
from mmf_sa.models.abstract_model import ForecastingRegressor


class MomentForecaster(ForecastingRegressor):
    def __init__(self, params):
        super().__init__(params)
        self.params = params
        self.device = None
        self.model = None
        self.install("git+https://github.com/moment-timeseries-foundation-model/moment.git")

    @staticmethod
    def install(package: str):
        subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])

    def register(self, registered_model_name: str):
        pipeline = MomentModel(
            self.repo,
            self.params["prediction_length"],
        )
        input_schema = Schema([TensorSpec(np.dtype(np.double), (-1,))])
        output_schema = Schema([TensorSpec(np.dtype(np.uint8), (-1,))])
        signature = ModelSignature(inputs=input_schema, outputs=output_schema)
        input_example = np.random.rand(52)
        mlflow.pyfunc.log_model(
            "model",
            python_model=pipeline,
            registered_model_name=registered_model_name,
            signature=signature,
            input_example=input_example,
            pip_requirements=[
                "git+https://github.com/moment-timeseries-foundation-model/moment.git",
                "git+https://github.com/databricks-industry-solutions/many-model-forecasting.git",
                "pyspark==3.5.0",
            ],
        )

    def create_horizon_timestamps_udf(self):
        @pandas_udf('array<timestamp>')
        def horizon_timestamps_udf(batch_iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            batch_horizon_timestamps = []
            for batch in batch_iterator:
                for series in batch:
                    last = series.max()
                    horizon_timestamps = []
                    for i in range(self.params["prediction_length"]):
                        last = last + self.one_ts_offset
                        horizon_timestamps.append(last.to_numpy())
                    batch_horizon_timestamps.append(np.array(horizon_timestamps))
            yield pd.Series(batch_horizon_timestamps)
        return horizon_timestamps_udf

    def prepare_data(self, df: pd.DataFrame, future: bool = False, spark=None) -> DataFrame:
        df = spark.createDataFrame(df)
        df = (
            df.groupBy(self.params.group_id)
            .agg(
                collect_list(self.params.date_col).alias('ds'),
                collect_list(self.params.target).alias('y'),
            ))
        return df

    def predict(self,
                hist_df: pd.DataFrame,
                val_df: pd.DataFrame = None,
                curr_date=None,
                spark=None):
        hist_df = self.prepare_data(hist_df, spark=spark)
        horizon_timestamps_udf = self.create_horizon_timestamps_udf()
        forecast_udf = self.create_predict_udf()
        device_count = torch.cuda.device_count()
        forecast_df = (
            hist_df.repartition(device_count, self.params.group_id)
            .select(
                hist_df.unique_id,
                horizon_timestamps_udf(hist_df.ds).alias("ds"),
                forecast_udf(hist_df.y).alias("y"))
        ).toPandas()

        forecast_df = forecast_df.reset_index(drop=False).rename(
            columns={
                "unique_id": self.params.group_id,
                "ds": self.params.date_col,
                "y": self.params.target,
            }
        )

        # Todo
        # forecast_df[self.params.target] = forecast_df[self.params.target].clip(0.01)
        return forecast_df, self.model

    def forecast(self, df: pd.DataFrame, spark=None):
        return self.predict(df, spark=spark)

    def calculate_metrics(
        self, hist_df: pd.DataFrame, val_df: pd.DataFrame, curr_date, spark=None
    ) -> list:
        pred_df, model_pretrained = self.predict(hist_df, val_df, curr_date, spark)
        keys = pred_df[self.params["group_id"]].unique()
        metrics = []
        if self.params["metric"] == "smape":
            metric_name = "smape"
        elif self.params["metric"] == "mape":
            metric_name = "mape"
        elif self.params["metric"] == "mae":
            metric_name = "mae"
        elif self.params["metric"] == "mse":
            metric_name = "mse"
        elif self.params["metric"] == "rmse":
            metric_name = "rmse"
        else:
            raise Exception(f"Metric {self.params['metric']} not supported!")
        for key in keys:
            actual = val_df[val_df[self.params["group_id"]] == key][self.params["target"]].to_numpy()
            forecast = pred_df[pred_df[self.params["group_id"]] == key][self.params["target"]].to_numpy()[0]
            try:
                if metric_name == "smape":
                    smape = MeanAbsolutePercentageError(symmetric=True)
                    metric_value = smape(actual, forecast)
                elif metric_name == "mape":
                    mape = MeanAbsolutePercentageError(symmetric=False)
                    metric_value = mape(actual, forecast)
                elif metric_name == "mae":
                    mae = MeanAbsoluteError()
                    metric_value = mae(actual, forecast)
                elif metric_name == "mse":
                    mse = MeanSquaredError(square_root=False)
                    metric_value = mse(actual, forecast)
                elif metric_name == "rmse":
                    rmse = MeanSquaredError(square_root=True)
                    metric_value = rmse(actual, forecast)
                metrics.extend(
                    [(
                        key,
                        curr_date,
                        metric_name,
                        metric_value,
                        forecast,
                        actual,
                        b'',
                    )])
            except:
                pass
        return metrics

    def create_predict_udf(self):
        @pandas_udf('array<double>')
        def predict_udf(batch_iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            # initialization step
            import torch
            import pandas as pd
            from momentfm import MOMENTPipeline
            model = MOMENTPipeline.from_pretrained(
                self.repo,
                model_kwargs={
                    'task_name': 'forecasting',
                    'forecast_horizon': self.params["prediction_length"],
                    'head_dropout': 0.1,
                    'weight_decay': 0,
                    'freeze_encoder': True,  # Freeze the patch embedding layer
                    'freeze_embedder': True,  # Freeze the transformer encoder
                    'freeze_head': False,  # The linear forecasting head must be trained
                },
            )
            model.init()
            # inference
            for batch in batch_iterator:
                batch_forecast = []
                for series in batch:
                    # takes in tensor of shape [batchsize, n_channels, context_length]
                    context = list(series)
                    if len(context) < 512:
                        input_mask = [1] * len(context) + [0] * (512 - len(context))
                        context = context + [0] * (512 - len(context))
                    else:
                        input_mask = [1] * 512
                        context = context[-512:]
                    input_mask = torch.reshape(torch.tensor(input_mask), (1, 512))
                    context = torch.reshape(torch.tensor(context), (1, 1, 512)).to(dtype=torch.float32)
                    output = model(context, input_mask=input_mask)
                    forecast = output.forecast.squeeze().tolist()
                    batch_forecast.append(forecast)
            yield pd.Series(batch_forecast)
        return predict_udf


class Moment1Large(MomentForecaster):
    def __init__(self, params):
        super().__init__(params)
        self.params = params
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.repo = "AutonLab/MOMENT-1-large"


class MomentModel(mlflow.pyfunc.PythonModel):
    def __init__(self, repository, prediction_length):
        from momentfm import MOMENTPipeline
        self.repository = repository
        self.prediction_length = prediction_length
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.pipeline = MOMENTPipeline.from_pretrained(
          self.repository,
          device_map=self.device,
          model_kwargs={
            "task_name": "forecasting",
            "forecast_horizon": self.prediction_length},
          )
        self.pipeline.init()
        self.pipeline = self.pipeline.to(self.device)

    def predict(self, context, input_data, params=None):
        series = list(input_data)
        if len(series) < 512:
            input_mask = [1] * len(series) + [0] * (512 - len(series))
            series = series + [0] * (512 - len(series))
        else:
            input_mask = [1] * 512
            series = series[-512:]
        input_mask = torch.reshape(torch.tensor(input_mask),(1, 512)).to(self.device)
        series = torch.reshape(torch.tensor(series),(1, 1, 512)).to(dtype=torch.float32).to(self.device)
        output = self.pipeline(series, input_mask=input_mask)
        forecast = output.forecast.squeeze().tolist()
        return forecast

