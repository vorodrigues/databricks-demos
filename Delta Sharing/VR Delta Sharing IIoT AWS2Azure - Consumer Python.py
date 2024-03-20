# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing: Easily Share & Distribute Data as a Product   <img src="https://i.ibb.co/vdNHFZN/deltasharingimage.png" width = "100"></a>
# MAGIC
# MAGIC With Delta Sharing, organizations can better govern, package, and share their data to both <b>Internal</b> and <b>External Customers.</b>
# MAGIC
# MAGIC Delta Sharing is an Open protocol, meaning that any recipient can access the data regardless of their data platform, without any locking. 
# MAGIC
# MAGIC This allow you to extend your reach and share data with any organization.
# MAGIC
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_sharing%2Fnotebook_presentation%2Faccess&dt=FEATURE_DELTA_SHARING">

# COMMAND ----------

# MAGIC %md ## Provider Setup
# MAGIC
# MAGIC Delta Sharing let you share data with external recipient without creating copy of the data. Once they're authorized, recipients can access and download your data directly.
# MAGIC
# MAGIC <br>
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow.png" width="1000" />

# COMMAND ----------

# MAGIC %md ## Consumer
# MAGIC
# MAGIC In the previous notebook, we shared our data and granted read access to our RECIPIENT.
# MAGIC
# MAGIC Let's now see how external consumers can directly access the data.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/delta-sharing/resources/images/delta-sharing-flow.png" width="900px"/>
# MAGIC
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_sharing%2Fnotebook_consumer%2Faccess&dt=FEATURE_DELTA_SHARING">

# COMMAND ----------

# MAGIC %md ### Simple Select

# COMMAND ----------

display(spark.table('vr_iiot_aws.dev.turbine_raw'))

# COMMAND ----------

# MAGIC %md ### Timetravel

# COMMAND ----------

# MAGIC %sql select * from vr_iiot_aws.dev.turbine_raw version as of 678

# COMMAND ----------

# MAGIC %md ### Simple Filter

# COMMAND ----------

display(spark.table('vr_iiot_aws.dev.turbine_raw').filter("deviceId = 'WindTurbine-293'"))

# COMMAND ----------

# MAGIC %md ### Complex Filter

# COMMAND ----------

display(spark.table('vr_iiot_aws.dev.turbine_raw').filter("deviceId = 'WindTurbine-293' and date(timestamp) = '2022-04-13'"))

# COMMAND ----------

# MAGIC %md ### Aggregation

# COMMAND ----------

display(spark.table('vr_iiot_aws.dev.turbine_raw').groupBy('deviceId').avg('rpm'))

# COMMAND ----------

# MAGIC %md ### Join with another Share

# COMMAND ----------

turbine_df = spark.table('vr_iiot_aws.dev.turbine_raw').select('date', 'timestamp', 'deviceId', 'rpm')
weather_df = spark.table('vr_iiot_aws.dev.weather_raw').select('date', 'timestamp', 'temperature')
display(turbine_df.join(weather_df, on=['timestamp']))

# COMMAND ----------

# MAGIC %md ### Join with local Catalog

# COMMAND ----------

turbine_df = spark.table('vr_iiot_aws.dev.turbine_raw').select('date', 'timestamp', 'deviceId', 'rpm')
weather_df = spark.table('vr_iiot_azure.dev.weather_raw').select('date', 'timestamp', 'temperature')
display(turbine_df.join(weather_df, on=['timestamp']))

# COMMAND ----------

# MAGIC %md ### Change Data Feed

# COMMAND ----------

# MAGIC %sql -- Restore table to initial version
# MAGIC create or replace table vr_iiot_azure.dev.weather_raw_cdc as select * from vr_iiot_aws.dev.weather_raw_cdc version as of 0

# COMMAND ----------

display(
  spark.read.format('deltaSharing')
  .option('readChangeFeed', 'true')
  .option('startingTimestamp', '2023-06-23 12:27:00')
  .table('vr_iiot_aws.dev.weather_raw_cdc')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC with tbl_changes as (
# MAGIC   select * from (
# MAGIC     select *, rank() over (partition by deviceId, timestamp order by _commit_timestamp) as rank
# MAGIC     from table_changes('vr_iiot_aws.dev.weather_raw_cdc', '2023-06-23 12:27:00')
# MAGIC     where _change_type != 'update_preimage'
# MAGIC   )
# MAGIC   where rank = 1
# MAGIC )
# MAGIC
# MAGIC merge into vr_iiot_azure.dev.weather_raw_cdc t
# MAGIC using tbl_changes s
# MAGIC on t.deviceId = s.deviceId and t.timestamp = s.timestamp
# MAGIC   when matched and s._change_type = 'update_postimage' then update set *
# MAGIC   when matched and s._change_type = 'delete' then delete
# MAGIC   when not matched then insert *

# COMMAND ----------

# MAGIC %md ### Streaming

# COMMAND ----------

# MAGIC %sql -- Restore table to initial version
# MAGIC create or replace table vr_iiot_azure.dev.weather_raw_cdc as select * from vr_iiot_aws.dev.weather_raw_cdc version as of 0

# COMMAND ----------

def mergeChanges(batchDF, batchId):
  # Set the dataframe to view name
  batchDF.createOrReplaceTempView("batch_changes")

  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  batchDF.sparkSession.sql("""
    with tbl_changes as (
      select * from (
        select *, rank() over (partition by deviceId, timestamp order by _commit_timestamp) as rank
        from batch_changes
        where _change_type != 'update_preimage'
      )
      where rank = 1
    )

    merge into vr_iiot_azure.dev.weather_raw_cdc t
    using tbl_changes s
    on t.deviceId = s.deviceId and t.timestamp = s.timestamp
      when matched and s._change_type = 'update_postimage' then update set *
      when matched and s._change_type = 'delete' then delete
      when not matched then insert *
  """)

# COMMAND ----------

(
  spark.readStream.format('deltaSharing')
    .option('readChangeFeed', 'true')
    .option('startingTimestamp', '2023-06-23 12:27:00')
    .table('vr_iiot_aws.dev.weather_raw_cdc')
    .writeStream
    .foreachBatch(mergeChanges)
    .outputMode('update')
    .start()
)

# COMMAND ----------

# MAGIC %sql -- Validate inserted row
# MAGIC select * from vr_iiot_azure.dev.weather_raw_cdc where date > '2023-12-31'

# COMMAND ----------

# MAGIC %sql -- Validate updated row
# MAGIC select * from vr_iiot_azure.dev.weather_raw_cdc where timestamp = '2022-01-01 00:00:00.000'

# COMMAND ----------

# MAGIC %sql -- Validate deleted row
# MAGIC select * from vr_iiot_azure.dev.weather_raw_cdc where timestamp = '2022-01-01 00:00:15.000'
