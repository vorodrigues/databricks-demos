# Databricks notebook source
# MAGIC %md ## Tests

# COMMAND ----------

ehConf = { 
  'ehName':dbutils.widgets.get("Event Hub Name"),
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(dbutils.widgets.get("IoT Hub Connection String")),
#   'eventhubs.startingPosition':'{"offset":"-1", "seqNo":-1, "enqueuedTime":null, "isInclusive":true}'
}

dbutils.fs.rm(CHECKPOINT_PATH, True)

# schema = "deviceId string, timestamp timestamp, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"
# schema = "deviceId string, timestamp timestamp, rpm double, angle double"
schema = "deviceId string, timestamp timestamp, rpm double, angle double"

iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .filter('systemProperties.`iothub-connection-device-id` like "%WindTurbine%"')
#     .withColumn('reading_str', F.lit('{ "deviceId": "WindTurbine-000002", "timestamp": "2022-12-08T14:46:11.4567519+00:00", "rpm": 90, "angle": 1 }'))
#     .withColumn('reading', F.from_json(F.col('reading_str'), schema))        # Extract the "body" payload from the messages
    .withColumn('reading_str', F.col('body').cast('string'))                         # Extract the "body" payload from the messages
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
#     .select('reading_str', 'reading', 'reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
)

display(iot_stream)

# COMMAND ----------

ehConf = { 
  'ehName':dbutils.widgets.get("Event Hub Name"),
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(dbutils.widgets.get("IoT Hub Connection String")),
#   'eventhubs.startingPosition':'{"offset":"-1", "seqNo":-1, "enqueuedTime":null, "isInclusive":true}'
}

dbutils.fs.rm(CHECKPOINT_PATH, True)

# schema = "deviceId string, timestamp timestamp, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"
schema = "deviceId string, timestamp timestamp, temperature double, humidity double, windspeed double, winddirection string"

iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .filter('systemProperties.`iothub-connection-device-id` like "%Weather%"')
#     .withColumn('reading_str', F.lit('{ "deviceId": "WindTurbine-000002", "timestamp": "2022-12-08T14:46:11.4567519+00:00", "rpm": 90, "angle": 1 }'))
#     .withColumn('reading', F.from_json(F.col('reading_str'), schema))        # Extract the "body" payload from the messages
    .withColumn('reading_str', F.col('body').cast('string'))                         # Extract the "body" payload from the messages
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
#     .select('reading_str', 'reading', 'reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
)

display(iot_stream)
