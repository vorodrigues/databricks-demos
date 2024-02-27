# Databricks notebook source
# MAGIC %md # Sklearn MLflow train and predict with ONNX
# MAGIC
# MAGIC ##### Overview
# MAGIC * Trains and saves model as Sklearn and ONNX flavors
# MAGIC * Predicts using ONNX native, Pyfunc and Spark UDF flavors

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %pip install onnx==1.15.0
# MAGIC %pip install onnxruntime==1.16.1
# MAGIC %pip install skl2onnx==1.15.0

# COMMAND ----------

# MAGIC %run ./Common

# COMMAND ----------

dbutils.widgets.text("1. Experiment name", "")
dbutils.widgets.text("2. Registered model", "")
dbutils.widgets.text("3. Max Depth", "1") 

experiment_name = dbutils.widgets.get("1. Experiment name")
model_name = dbutils.widgets.get("2. Registered model")
max_depth = int(dbutils.widgets.get("3. Max Depth"))

model_name = model_name or None
experiment_name = experiment_name or None

print("experiment_name:", experiment_name)
print("model_name:", model_name)
print("max_depth:", max_depth)

# COMMAND ----------

import mlflow
import onnx
import skl2onnx

# COMMAND ----------

# MAGIC %md ### Prepare data

# COMMAND ----------

data = WineQuality.load_pandas_data()
train_x, test_x, train_y, test_y = WineQuality.prep_training_data(data)
display(data)

# COMMAND ----------

# MAGIC %md ### Train & Register

# COMMAND ----------

import numpy as np
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from mlflow.models.signature import infer_signature

# COMMAND ----------

class OnnxWrapper(mlflow.pyfunc.PythonModel):
    
    def __init__(self, onnx_model):
        self.onnx_model = onnx_model
    
    def load_context(self, context):
        # Como não é possível fazer um pickle de uma InferenceSession,
        # utilzamos o load_context() para instanciá-la. Este método é executado
        # automaticamente durante a inicialização do modelo
        self.model = onnxruntime.InferenceSession(self.onnx_model.SerializeToString())
        self.inputs = self.model.get_inputs()[0].name

    def predict(self, context, data):            
        feed_dict = {self.inputs : data.to_numpy().astype(np.float32)}
        return self.model.run(None, feed_dict)[0]

# COMMAND ----------

with mlflow.start_run(run_name="sklearn_onnx") as run:
    
    # Log basic info
    run_id = run.info.run_uuid
    print("MLflow:")
    print("  run_id:",run_id)
    print("  experiment_id:",run.info.experiment_id)
    
    mlflow.set_tag("version.mlflow", mlflow.__version__)
    mlflow.set_tag("version.onnx", onnx.__version__)

    mlflow.log_param("max_depth", max_depth)

    # Train model
    sklearn_model = DecisionTreeRegressor(max_depth=max_depth)
    sklearn_model.fit(train_x, train_y)

    # Run predictions and log metrics
    predictions = sklearn_model.predict(test_x)       
    mlflow.log_metric("rmse", np.sqrt(mean_squared_error(test_y, predictions))) 

    # Infer signature
    signature = infer_signature(train_x, predictions)
    
    # Wrap ONNX model
    initial_type = [('float_input', skl2onnx.common.data_types.FloatTensorType([None, test_x.shape[1]]))]
    onnx_model = skl2onnx.convert_sklearn(sklearn_model, initial_types=initial_type)
    wrapped_model = OnnxWrapper(onnx_model)
        
    # Log PyFunc model
    mlflow.pyfunc.log_model("model", 
        python_model=wrapped_model,
        signature = signature,
        input_example = test_x,
        registered_model_name = model_name
    )
