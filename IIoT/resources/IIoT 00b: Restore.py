# Databricks notebook source
# Restore for IoT Hub

# Create a new IoT Hub
# Get new Event Hub-compatible endpoint and name at Built-in endpoints
# Update DE notebook
# Create devices:
#   - WindTurbine-000001
#   - WindTurbine-000002
#   - WeatherCapture
# Get new connection strings at Device details
# Update sendMessagesAsync.py

# Reset bronze checkpoints
# dbutils.fs.rm('s3://databricks-vr/iiot/checkpoints/turbine_raw', True)
# dbutils.fs.rm('s3://databricks-vr/iiot/checkpoints/weather_raw', True)

# COMMAND ----------

# Full Restore
# for src in ['weather_raw', 'weather_agg', 'turbine_raw_3', 'turbine_agg_3', 'turbine_enriched_3']:
#   dst = src.replace('_3','')
#   print(f'Resetting table {dst}...')
#   spark.sql(f'ALTER SHARE vr_iiot_share REMOVE TABLE dev.{dst}')
#   spark.sql(f'DROP TABLE vr_iiot.dev.{dst}')
#   spark.sql(f'CREATE TABLE vr_iiot.dev.{dst} PARTITIONED BY (date) AS SELECT * FROM vr_iiot.backup.{src}')
#   spark.sql(f'ALTER SHARE vr_iiot_share ADD TABLE vr_iiot.dev.{dst}')

# print('Resetting checkpoint...')
# dbutils.fs.rm('s3://databricks-vr/iiot/checkpoints/', True)
