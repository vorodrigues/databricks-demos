-- Databricks notebook source
-- CREATE TABLE vr_kafka_dev.system_event_log_raw using delta LOCATION 'dbfs:/pipelines/a949e8a4-a900-467e-92b2-ce4b529ff345/system/events'

-- COMMAND ----------

CREATE LIVE TABLE expectations 
  COMMENT "Expectation DLT logs"
  TBLPROPERTIES ("quality" = "silver")
as (
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics.num_output_rows as output_records,
    details:flow_progress.data_quality.dropped_records,
    details:flow_progress.status as status_update,
    explode(from_json(details:flow_progress.data_quality.expectations
             ,'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) expectations
  FROM vr_kafka_dev.system_event_log_raw  
  where details:flow_progress.status='COMPLETED' and details:flow_progress.data_quality.expectations is not null)
