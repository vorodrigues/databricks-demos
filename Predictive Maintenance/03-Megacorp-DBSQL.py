# Databricks notebook source
# MAGIC %md #TODO: build your DBSQL dashboard
# MAGIC 
# MAGIC Your story should be end 2 end and include a SQL dashboard to present some KPIs

# COMMAND ----------

# MAGIC %md ### Keep it simple
# MAGIC The goal for you is to understand how DBSQL is working. 
# MAGIC 
# MAGIC To make it simple, we've created some table for you that you can reuse in DBSQL

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo_turbine;
# MAGIC CREATE TABLE if not exists `demo_turbine`.`turbine_bronze` USING delta LOCATION 'dbfs:/mnt/quentin-demo-resources/turbine/bronze/data';
# MAGIC CREATE TABLE if not exists `demo_turbine`.`turbine_silver` USING delta LOCATION 'dbfs:/mnt/quentin-demo-resources/turbine/silver/data';
# MAGIC CREATE TABLE if not exists `demo_turbine`.`turbine_gold`   USING delta LOCATION 'dbfs:/mnt/quentin-demo-resources/turbine/gold/data';
# MAGIC 
# MAGIC CREATE TABLE if not exists `demo_turbine`.`turbine_power_prediction` USING delta LOCATION 'dbfs:/mnt/quentin-demo-resources/turbine/power/prediction/data';
# MAGIC CREATE TABLE if not exists `demo_turbine`.`turbine_power_bronze`     USING delta LOCATION 'dbfs:/mnt/quentin-demo-resources/turbine/power/bronze/data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo_turbine.turbine_status AS SELECT
# MAGIC   ID,
# MAGIC   STATUS
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     ID,
# MAGIC     STATUS,
# MAGIC     row_number() over (partition by ID order by STATUS asc) as rownum
# MAGIC   FROM parquet.`/mnt/quentin-demo-resources/turbine/status`
# MAGIC )
# MAGIC WHERE rownum = 1

# COMMAND ----------

# MAGIC %md Make sure your dashboard include data coming from ML, otherwise you could do the same with any DataWarehouse and you aren't showing a clear distinction vs the competition
# MAGIC 
# MAGIC Be smart, fake your data in your SQL select statement for the counters to save some time.

# COMMAND ----------

# MAGIC %md 
# MAGIC Looking for inspiration ? Check this:
# MAGIC 
# MAGIC 
# MAGIC ![turbine-demo-dashboard](https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/turbine-demo-dashboard1.png)
# MAGIC 
# MAGIC [Open SQL Analytics dashboard example](https://e2-demo-west.cloud.databricks.com/sql/dashboards/a81f8008-17bf-4d68-8c79-172b71d80bf0-turbine-demo?o=2556758628403379)
