# Databricks notebook source
# MAGIC %md # Setup Hive databases and tables

# COMMAND ----------

# MAGIC %sql create database if not exists vr_tests

# COMMAND ----------

# MAGIC %sql create table if not exists vr_tests.sync_parquet using parquet 
# MAGIC location 's3://databricks-vr/tests/uc_sync_parquet'
# MAGIC as select * from vr_fraud_dev.visits_gold limit 1000

# COMMAND ----------

# MAGIC %sql create table if not exists vr_tests.sync_csv using csv 
# MAGIC location 's3://databricks-vr/tests/uc_sync_csv'
# MAGIC options (FIELDDELIM=',', ESCAPEDELIM='"', LINEDELIM='\n')
# MAGIC as select first_name, last_name from vr_fraud_dev.visits_gold limit 1000

# COMMAND ----------

# MAGIC %md # Setup UC database

# COMMAND ----------

# MAGIC %sql create database if not exists vr_tests.uc

# COMMAND ----------

# MAGIC %md # SYNC tables

# COMMAND ----------

# MAGIC %sql drop table vr_tests.uc.sync_parquet

# COMMAND ----------

# MAGIC %sql SYNC TABLE vr_tests.uc.sync_parquet FROM vr_tests.sync_parquet

# COMMAND ----------

# MAGIC %sql drop table vr_tests.uc.sync_csv

# COMMAND ----------

# MAGIC %sql SYNC TABLE vr_tests.uc.sync_csv FROM vr_tests.sync_csv

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED hive_metastore.vr_tests.sync_parquet
