# Databricks notebook source
# MAGIC %md ### Setup tables and checkpoint

# COMMAND ----------

# MAGIC %sql -- Restore table to initial version
# MAGIC drop table if exists vr_iiot.dev.weather_raw_cdc_src;
# MAGIC drop table if exists vr_iiot.dev.weather_raw_cdc_tgt;
# MAGIC create table vr_iiot.dev.weather_raw_cdc_src tblproperties (delta.enableChangeDataFeed = true) as select * from vr_iiot.dev.weather_raw_cdc version as of 0;
# MAGIC create table vr_iiot.dev.weather_raw_cdc_tgt as select * from vr_iiot.dev.weather_raw_cdc version as of 0;

# COMMAND ----------

# Remove checkpoint
dbutils.fs.rm('/FileStore/vr/tests/cdc/checkpoint', True)

# COMMAND ----------

# MAGIC %md ### Apply changes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- UPDATE
# MAGIC update vr_iiot.dev.weather_raw_cdc_src set temperature = 200 where timestamp = '2022-01-01 00:00:00.000';
# MAGIC
# MAGIC -- DELETE
# MAGIC delete from vr_iiot.dev.weather_raw_cdc_src where timestamp = '2022-01-01 00:00:15.000';
# MAGIC
# MAGIC -- INSERT
# MAGIC insert into vr_iiot.dev.weather_raw_cdc_src values (
# MAGIC   '2024-01-01',
# MAGIC   '2024-01-01 00:00:00.000',
# MAGIC   'WeatherCapture',
# MAGIC   18.56,
# MAGIC   38.22,
# MAGIC   7.81,
# MAGIC   'W'
# MAGIC );

# COMMAND ----------

# MAGIC %md ### Start CDC stream

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

    merge into vr_iiot.dev.weather_raw_cdc_tgt t
    using tbl_changes s
    on t.deviceId = s.deviceId and t.timestamp = s.timestamp
      when matched and s._change_type = 'update_postimage' then update set *
      when matched and s._change_type = 'delete' then delete
      when not matched then insert *
  """)

# COMMAND ----------

(
  spark.readStream.format('delta')
    .option('readChangeFeed', 'true')
    .option('startingVersion', 1)
    .table('vr_iiot.dev.weather_raw_cdc_src')
    .writeStream
    .foreachBatch(mergeChanges)
    .option('checkpointLocation', '/FileStore/vr/tests/cdc/checkpoint')
    .trigger(availableNow=True)
    .outputMode('update')
    .start()
)

# COMMAND ----------

# MAGIC %md ### Validate changes

# COMMAND ----------

# MAGIC %sql -- Validate inserted row
# MAGIC select * from vr_iiot.dev.weather_raw_cdc_tgt where date > '2023-12-31'

# COMMAND ----------

# MAGIC %sql -- Validate updated row
# MAGIC select * from vr_iiot.dev.weather_raw_cdc_tgt where timestamp = '2022-01-01 00:00:00.000'

# COMMAND ----------

# MAGIC %sql -- Validate deleted row
# MAGIC select * from vr_iiot.dev.weather_raw_cdc_tgt where timestamp = '2022-01-01 00:00:15.000'

# COMMAND ----------

# MAGIC %md ### Apply new changes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- UPDATE
# MAGIC update vr_iiot.dev.weather_raw_cdc_src set temperature = 200 where timestamp = '2022-01-01 00:00:30.000';
# MAGIC
# MAGIC -- DELETE
# MAGIC delete from vr_iiot.dev.weather_raw_cdc_src where timestamp = '2022-01-01 00:00:45.000';
# MAGIC
# MAGIC -- INSERT
# MAGIC insert into vr_iiot.dev.weather_raw_cdc_src values (
# MAGIC   '2024-01-01',
# MAGIC   '2024-01-01 00:00:15.000',
# MAGIC   'WeatherCapture',
# MAGIC   18.56,
# MAGIC   38.22,
# MAGIC   7.81,
# MAGIC   'W'
# MAGIC );

# COMMAND ----------

# MAGIC %md ### Resume CDC stream

# COMMAND ----------

# MAGIC %sql drop table if exists vr_iiot.dev.weather_raw_cdc_feed

# COMMAND ----------

(
  spark.readStream.format('delta')
    .option('readChangeFeed', 'true')
    .option('startingVersion', 0)
    .table('vr_iiot.dev.weather_raw_cdc_src')
    .writeStream
    .option('checkpointLocation', '/FileStore/vr/tests/cdc/checkpoint')
    .trigger(availableNow=True)
    .toTable('vr_iiot.dev.weather_raw_cdc_feed')
)

# COMMAND ----------

# MAGIC %sql select * from vr_iiot.dev.weather_raw_cdc_feed

# COMMAND ----------

# MAGIC %md ### Resume CDC stream with no changes

# COMMAND ----------

# MAGIC %sql drop table if exists vr_iiot.dev.weather_raw_cdc_feed

# COMMAND ----------

(
  spark.readStream.format('delta')
    .option('readChangeFeed', 'true')
    .option('startingVersion', 0)
    .table('vr_iiot.dev.weather_raw_cdc_src')
    .writeStream
    .option('checkpointLocation', '/FileStore/vr/tests/cdc/checkpoint')
    .trigger(availableNow=True)
    .toTable('vr_iiot.dev.weather_raw_cdc_feed')
)

# COMMAND ----------

# MAGIC %sql select * from vr_iiot.dev.weather_raw_cdc_feed
