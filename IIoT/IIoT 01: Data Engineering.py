# Databricks notebook source
# Connection information for Azure IoT Hub and cloud storage
# dbutils.widgets.text("Event Hub Name","")
# dbutils.widgets.text("IoT Hub Connection String", "")
# dbutils.widgets.text("External Location", "")

# COMMAND ----------

# MAGIC %md # End to End Industrial IoT (IIoT) on Databricks
# MAGIC ## Part 1 - Data Engineering
# MAGIC This notebook demonstrates the following architecture for IIoT Ingest, Processing and Analytics on Databricks. The following architecture is implemented for the demo. 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/end_to_end_architecture.png" width=800>
# MAGIC 
# MAGIC The notebook is broken into sections following these steps:
# MAGIC 1. **Data Ingest** - stream real-time raw sensor data from Azure IoT Hubs into the Delta format in cloud storage
# MAGIC 2. **Data Processing** - stream process sensor data from raw (Bronze) to silver (aggregated) to gold (enriched) Delta tables on cloud storage

# COMMAND ----------

# MAGIC %md ## 0. Environment Setup
# MAGIC 
# MAGIC The pre-requisites are listed below:
# MAGIC 
# MAGIC ### Services Required
# MAGIC * Azure IoT Hub 
# MAGIC * [Azure IoT Simulator](https://azure-samples.github.io/raspberry-pi-web-simulator/) running with the code provided in [this github repo](https://github.com/tomatoTomahto/azure_databricks_iot/blob/master/azure_iot_simulator.js) and configured for your IoT Hub
# MAGIC * Cloud storage
# MAGIC 
# MAGIC ### Databricks Configuration Required
# MAGIC * 3-node (min) Databricks Cluster running **DBR 7.0+** and the following libraries:
# MAGIC   * **Azure Event Hubs Connector for Databricks** - Maven coordinates `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17`

# COMMAND ----------

# Setup storage locations for all data
ROOT_PATH = dbutils.widgets.get("External Location")
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# Other initializations
# dbutils.secrets.get('iot','iothub-cs') # IoT Hub connection string (Event Hub Compatible)
ehConf = { 
  'ehName':dbutils.widgets.get("Event Hub Name"),
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(dbutils.widgets.get("IoT Hub Connection String")),
  'eventhubs.startingPosition':'{"offset":"-1", "seqNo":-1, "enqueuedTime":null, "isInclusive":true}'
}

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")

# Pyspark and ML Imports
import os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS vr_iiot;
# MAGIC CREATE DATABASE IF NOT EXISTS vr_iiot.dev;
# MAGIC USE vr_iiot.dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up tables & views
# MAGIC DROP TABLE IF EXISTS turbine_raw;
# MAGIC DROP TABLE IF EXISTS weather_raw;
# MAGIC DROP TABLE IF EXISTS turbine_agg;
# MAGIC DROP TABLE IF EXISTS weather_agg;
# MAGIC DROP TABLE IF EXISTS turbine_enriched;
# MAGIC DROP TABLE IF EXISTS turbine_power;
# MAGIC DROP TABLE IF EXISTS turbine_maintenance;
# MAGIC DROP VIEW IF EXISTS turbine_combined;
# MAGIC DROP VIEW IF EXISTS feature_view;
# MAGIC DROP TABLE IF EXISTS turbine_life_predictions;
# MAGIC DROP TABLE IF EXISTS turbine_power_predictions;

# COMMAND ----------

# Make sure root path is empty
dbutils.fs.rm(ROOT_PATH, True)

# COMMAND ----------

# MAGIC %md ## 1. Data Ingestion from IoT Hubs
# MAGIC Databricks provides a native connector to IoT and Event Hubs. Below, we will use PySpark Structured Streaming to read from an IoT Hub stream of data and write the data in it's raw format directly into Delta. 
# MAGIC 
# MAGIC Make sure that your IoT Simulator is sending payloads to IoT Hub as shown below.
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/iot_simulator.gif" width=800>
# MAGIC 
# MAGIC We have two separate types of data payloads in our IoT Hub:
# MAGIC 1. **Turbine Sensor readings** - this payload contains `date`,`timestamp`,`deviceid`,`rpm` and `angle` fields
# MAGIC 2. **Weather Sensor readings** - this payload contains `date`,`timestamp`,`temperature`,`humidity`,`windspeed`, and `winddirection` fields
# MAGIC 
# MAGIC We split out the two payloads into separate streams and write them both into Delta locations on cloud Storage. We are able to query these two Bronze tables *immediately* as the data streams in.

# COMMAND ----------

# MAGIC %md ### 1a. Bronze Layer

# COMMAND ----------

# Schema of incoming data from IoT hub
schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"

# Read directly from IoT Hub using the EventHubs library for Databricks
iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
)

# Split our IoT Hub stream into separate streams and write them both into their own Delta locations
# Write turbine events
(iot_stream.filter('temperature is null')                                          # Filter out turbine telemetry from other data streams
  .select('date','timestamp','deviceId','rpm','angle')                             # Extract the fields of interest
  .writeStream.format('delta')                                                     # Write our stream to the Delta format
  .partitionBy('date')                                                             # Partition our data by Date for performance
  .option("checkpointLocation", CHECKPOINT_PATH + "turbine_raw")                   # Checkpoint so we can restart streams gracefully
  .toTable('turbine_raw')                                                          # Stream the data into a Delta Table
)

# Write weather events
(iot_stream.filter(iot_stream.temperature.isNotNull())                             # Filter out weather telemetry only
  .select('date','deviceid','timestamp','temperature','humidity','windspeed','winddirection') 
  .writeStream.format('delta')                                                     # Write our stream to the Delta format
  .partitionBy('date')                                                             # Partition our data by Date for performance
  .option("checkpointLocation", CHECKPOINT_PATH + "weather_raw")                   # Checkpoint so we can restart streams gracefully
  .toTable('weather_raw')                                                          # Stream the data into a Delta Table
)

# COMMAND ----------

# MAGIC %md ## 2. Data Processing
# MAGIC While our raw sensor data is being streamed into Bronze Delta tables on cloud storage, we can create streaming pipelines on this data that flow it through Silver and Gold data sets.
# MAGIC 
# MAGIC We will use the following schema for Silver and Gold data sets:
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/iot_delta_bronze_to_gold.png" width=800>

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- We can query the data directly from storage immediately as soon as it starts streams into Delta 
# MAGIC SELECT * FROM turbine_raw --WHERE deviceid = 'WindTurbine-1'

# COMMAND ----------

# MAGIC %md ### 2a. Silver Layer
# MAGIC The first step of our processing pipeline will clean and aggregate the measurements to 1 hour intervals. 
# MAGIC 
# MAGIC Since we are aggregating time-series values and there is a likelihood of late-arriving data and data changes, we will use the [**MERGE**](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/merge-into?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json) functionality of Delta to upsert records into target tables. 
# MAGIC 
# MAGIC MERGE allows us to upsert source records into a target storage location. This is useful when dealing with time-series data as:
# MAGIC 1. Data often arrives late and requires aggregation states to be updated
# MAGIC 2. Historical data needs to be backfilled while streaming data is feeding into the table
# MAGIC 
# MAGIC When streaming source data, `foreachBatch()` can be used to perform a merges on micro-batches of data.

# COMMAND ----------

# Create functions to merge turbine and weather data into their target Delta tables
def merge_delta(incremental, target): 
  incremental.dropDuplicates(['date','window','deviceid']).createOrReplaceTempView("incremental")
  
  try:
    # MERGE records into the target table using the specified join key
    incremental._jdf.sparkSession().sql(f"""
      MERGE INTO delta.`{target}` t
      USING incremental i
      ON i.date=t.date AND i.window = t.window AND i.deviceId = t.deviceid
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  except:
    # If the †arget table does not exist, create one
    incremental.writeTo(target).partitionedBy('date').createOrReplace()
    
# Stream turbine events to silver layer
(spark.readStream.format('delta').table("turbine_raw")                       # Read data as a stream from our source Delta table
  .groupBy('deviceId','date',F.window('timestamp','5 minutes'))              # Aggregate readings to hourly intervals
  .agg(F.avg('rpm').alias('rpm'), F.avg("angle").alias("angle"))
  .writeStream                                                               # Write the resulting stream
  .foreachBatch(lambda i, b: merge_delta(i, "turbine_agg"))                  # Pass each micro-batch to a function
  .outputMode("update")                                                      # Merge works with update mode
  .option("checkpointLocation", CHECKPOINT_PATH + "turbine_agg")             # Checkpoint so we can restart streams gracefully
  .start()
)

# Stream wheather events to silver layer
(spark.readStream.format('delta').table("weather_raw")                       # Read data as a stream from our source Delta table
  .groupBy('deviceid','date',F.window('timestamp','5 minutes'))              # Aggregate readings to hourly intervals
  .agg({"temperature":"avg","humidity":"avg","windspeed":"avg","winddirection":"last"})
  .selectExpr('date','window','deviceid','`avg(temperature)` as temperature','`avg(humidity)` as humidity',
              '`avg(windspeed)` as windspeed','`last(winddirection)` as winddirection')
  .writeStream                                                               # Write the resulting stream
  .foreachBatch(lambda i, b: merge_delta(i, "weather_agg"))                  # Pass each micro-batch to a function
  .outputMode("update")                                                      # Merge works with update mode
  .option("checkpointLocation", CHECKPOINT_PATH + "weather_agg")             # Checkpoint so we can restart streams gracefully
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As data gets merged in real-time to our hourly table, we can query it immediately
# MAGIC SELECT * FROM turbine_agg t JOIN weather_agg w ON (t.date=w.date AND t.window=w.window) WHERE t.deviceid='WindTurbine-1' ORDER BY t.window DESC

# COMMAND ----------

# MAGIC %md ### 2b. Gold Layer
# MAGIC Next we perform a streaming join of weather and turbine readings to create one enriched dataset we can use for data science and model training.

# COMMAND ----------

# Read streams from Delta Silver tables and join them together on common columns (date & window)
turbine_agg = spark.readStream.format('delta').option("ignoreChanges", True).table('turbine_agg')
weather_agg = spark.readStream.format('delta').option("ignoreChanges", True).table('weather_agg').drop('deviceid')
turbine_enriched = turbine_agg.join(weather_agg, ['date','window'])

# Write the stream to a foreachBatch function which performs the MERGE as before
(turbine_enriched
  .selectExpr('date','deviceid','window.start as window','rpm','angle','temperature','humidity','windspeed','winddirection')
  .writeStream 
  .foreachBatch(lambda i, b: merge_delta(i, "turbine_enriched"))
  .option("checkpointLocation", CHECKPOINT_PATH + "turbine_enriched")         
  .start()
)

# COMMAND ----------

# MAGIC %sql SELECT * FROM turbine_enriched WHERE deviceid='WindTurbine-1'

# COMMAND ----------

# MAGIC %md ## 3. Simulate Historical Data
# MAGIC In order to train a model, we will need to backfill our streaming data with historical data. The cell below generates 1 year of historical hourly turbine and weather data and inserts it into our Gold Delta table.

# COMMAND ----------

import pandas as pd
import numpy as np

# Function to simulate generating time-series data given a baseline, slope, and some seasonality
def generate_series(time_index, baseline, slope=0.01, period=365*24*12):
  rnd = np.random.RandomState(time_index)
  season_time = (time_index % period) / period
  seasonal_pattern = np.where(season_time < 0.4, np.cos(season_time * 2 * np.pi), 1 / np.exp(3 * season_time))
  return baseline * (1 + 0.1 * seasonal_pattern + 0.1 * rnd.randn(len(time_index)))
  
# Get start and end dates for our historical data
dates = spark.sql('select max(date)-interval 365 days as start, max(date) as end from turbine_enriched').toPandas()
  
# Get the baseline readings for each sensor for backfilling data
turbine_enriched_pd = spark.table('turbine_enriched').toPandas()
baselines = turbine_enriched_pd.min()[3:8]
devices = turbine_enriched_pd['deviceid'].unique()

# Iterate through each device to generate historical data for that device
print("---Generating Historical Enriched Turbine Readings---")
for deviceid in devices:
  print(f'Backfilling device {deviceid}')
  windows = pd.date_range(start=dates['start'][0], end=dates['end'][0], freq='5T') # Generate a list of hourly timestamps from start to end date
  historical_values = pd.DataFrame({
    'date': windows.date,
    'window': windows, 
    'winddirection': np.random.choice(['N','NW','W','SW','S','SE','E','NE'], size=len(windows)),
    'deviceId': deviceid
  })
  time_index = historical_values.index.to_numpy()                                 # Generate a time index

  for sensor in baselines.keys():
    historical_values[sensor] = generate_series(time_index, baselines[sensor])    # Generate time-series data from this sensor

  # Write dataframe to enriched_readings Delta table
  spark.createDataFrame(historical_values).write.format("delta").mode("append").saveAsTable("turbine_enriched")
  
# Create power readings based on weather and operating conditions
print("---Generating Historical Turbine Power Readings---")
spark.sql(f'CREATE TABLE turbine_power USING DELTA PARTITIONED BY (date) LOCATION "{GOLD_PATH + "turbine_power"}" AS SELECT date, window, deviceId, 0.1 * (temperature/humidity) * (3.1416 * 25) * windspeed * rpm AS power FROM turbine_enriched')

# Create a maintenance records based on peak power usage
print("---Generating Historical Turbine Maintenance Records---")
spark.sql(f'CREATE TABLE turbine_maintenance USING DELTA LOCATION "{GOLD_PATH + "turbine_maintenance"}" AS SELECT DISTINCT deviceid, FIRST(date) OVER (PARTITION BY deviceid, year(date), month(date) ORDER BY power) AS date, True AS maintenance FROM turbine_power')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize all 3 tables for querying and model training performance
# MAGIC OPTIMIZE turbine_enriched WHERE date<current_date() ZORDER BY deviceid, window;
# MAGIC OPTIMIZE turbine_power ZORDER BY deviceid, window;
# MAGIC OPTIMIZE turbine_maintenance ZORDER BY deviceid;

# COMMAND ----------

# MAGIC %md Our Delta Gold tables are now ready for predictive analytics! We now have hourly weather, turbine operating and power measurements, and daily maintenance logs going back one year. We can see that there is significant correlation between most of the variables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query all 3 tables
# MAGIC CREATE OR REPLACE VIEW gold_readings AS
# MAGIC SELECT r.*, 
# MAGIC   p.power, 
# MAGIC   ifnull(m.maintenance,False) as maintenance
# MAGIC FROM turbine_enriched r 
# MAGIC   JOIN turbine_power p ON (r.date=p.date AND r.window=p.window AND r.deviceid=p.deviceid)
# MAGIC   LEFT JOIN turbine_maintenance m ON (r.date=m.date AND r.deviceid=m.deviceid);
# MAGIC   
# MAGIC SELECT * FROM gold_readings ORDER BY deviceid, window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Benefits of Delta Lake on Time-Series Data
# MAGIC A key component of this architecture is the **Delta** storage format, which provides a layer of resiliency and performance on all data sources in cloud storage. However, Data Lakes alone do not solve challenges that come with time-series streaming data. Specifically for time-series data, Delta provides the following advantages over other storage formats:
# MAGIC 
# MAGIC |**Required Capability**|**Other formats on ADLS**|**Delta Format on ADLS**|
# MAGIC |--------------------|-----------------------------|---------------------------|
# MAGIC |**Unified batch & streaming**|Data Lakes are often used in conjunction with a streaming store like CosmosDB, resulting in a complex architecture|ACID-compliant transactions enable data engineers to perform streaming ingest and historically batch loads into the same locations on cloud storage|
# MAGIC |**Schema enforcement and evolution**|Data Lakes do not enforce schema, requiring all data to be pushed into a relational database for reliability|Schema is enforced by default. As new IoT devices are added to the data stream, schemas can be evolved safely so downstream applications don’t fail|
# MAGIC |**Efficient Upserts**|Data Lakes do not support in-line updates and merges, requiring deletion and insertions of entire partitions to perform updates|MERGE commands are effective for situations handling delayed IoT readings, modified dimension tables used for real-time enrichment, or if data needs to be reprocessed|
# MAGIC |**File Compaction**|Streaming time-series data into Data Lakes generate hundreds or even thousands of tiny files|Auto-compaction in Delta optimizes the file sizes to increase throughput and parallelism|
# MAGIC |**Multi-dimensional clustering**|Data Lakes provide push-down filtering on partitions only|ZORDERing time-series on fields like timestamp or sensor ID allows Databricks to filter and join on those columns up to 100x faster than simple partitioning techniques|

# COMMAND ----------

# MAGIC %md ## 5. Analyze data and develop ML models with Databricks
# MAGIC Databricks SQL Warehouses provides on-demand SQL directly on Data Lake source formats, which can also directly serve data for **Data Warehousing** workloads like **BI dashboarding** and **reporting**. 
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/synapse_databricks_delta.png" width=800>
