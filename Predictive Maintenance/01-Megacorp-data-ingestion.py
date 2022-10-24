# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Megacorp Demo
# MAGIC 
# MAGIC MegaCorp has been touting the benefits of creating an “Electricity Value Network,” in which digitization allows for visualization and predictive ML using sensor data from the plant. 
# MAGIC 
# MAGIC A great example is Bord Gáis Energy’s Whitegate power plant, a 445-MW gas combined-cycle plant. There are sensors across the plant that provide round-the-clock monitoring. ML can be used to improve plant efficiency.
# MAGIC 
# MAGIC Whitegate engineers desire a single, consolidated picture of Whitegate’s performance and how to run the plant most optimally.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/09/delta-lake-medallion-model-scaled.jpg" width=1012/>

# COMMAND ----------

# DBTITLE 1,Visualize incoming files
# MAGIC %fs ls /mnt/quentin-demo-resources/turbine/incoming-data-json

# COMMAND ----------

# DBTITLE 1,Visualize incoming sensor data
# MAGIC %sql
# MAGIC SELECT * from json.`/mnt/quentin-demo-resources/turbine/incoming-data-json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: ingest data stream

# COMMAND ----------

# MAGIC %md ### Auto Loader
# MAGIC 
# MAGIC Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage.
# MAGIC 
# MAGIC Auto Loader provides a Structured Streaming source called cloudFiles. Given an input directory path on the cloud file storage, the cloudFiles source automatically processes new files as they arrive, with the option of also processing existing files in that directory.
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2020/02/autoloader.png)

# COMMAND ----------

# DBTITLE 1,Stream landing files from cloud storage
#Ingest data using Auto Loader.
bronzeDF = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "json") \
                .option("cloudFiles.schemaLocation", path+"/bronze_schema") \
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
                .option("cloudFiles.inferColumnTypes", True) \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .load("/mnt/quentin-demo-resources/turbine/incoming-data-json")

# Write Stream as Delta Table
bronzeDF.writeStream.format("delta") \
        .option("checkpointLocation", path+"/bronze_checkpoint") \
        .trigger(processingTime="10 seconds") \
        .toTable("turbine_bronze")

# COMMAND ----------

# DBTITLE 1,Our raw data is now available in a Delta table
# MAGIC %sql
# MAGIC -- you should have a bronze table structured with the following columns: ID AN3 AN4 AN5 AN6 AN7 AN8 AN9 AN10 SPEED TORQUE _rescued
# MAGIC select * from turbine_bronze;

# COMMAND ----------

# MAGIC %md ### Auto Optimize
# MAGIC 
# MAGIC Auto Optimize is an optional set of features that automatically compact small files during individual writes to a Delta table. Paying a small cost during writes offers significant benefits for tables that are queried actively. It is particularly useful in the following scenarios:<br><br>
# MAGIC   - Streaming use cases where latency in the order of minutes is acceptable
# MAGIC   - MERGE INTO is the preferred method of writing into Delta Lake
# MAGIC   - CREATE TABLE AS SELECT or INSERT INTO are commonly used operations
# MAGIC ![](https://docs.databricks.com/_images/optimized-writes.png)

# COMMAND ----------

# DBTITLE 1,Enable Auto Optimize
# MAGIC %sql
# MAGIC ALTER TABLE turbine_bronze SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver layer: cleanup data and remove unecessary column

# COMMAND ----------

# MAGIC %md ### Cleanup data

# COMMAND ----------

# DBTITLE 1,Cleanup Silver table
#Cleanup the silver table
#Our bronze silver should have TORQUE with mostly NULL value and the _rescued column should be empty.
#Drop the TORQUE column, filter on _rescued to select only the rows without json error from the autoloader, filter on ID not null as you'll need it for your join later
silverDF = spark.readStream.table('turbine_bronze') \
                .drop('TORQUE') \
                .where('"ID" IS NOT NULL AND _rescued_data IS NULL')

#Write it back to your "turbine_silver" table
silverDF.writeStream.format('delta') \
        .option("checkpointLocation", path+"/silver_checkpoint") \
        .toTable("turbine_silver")

# COMMAND ----------

# DBTITLE 1,Visualize Silver table
# MAGIC %sql
# MAGIC select * from turbine_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold layer: join information on Turbine status to add a label to our dataset

# COMMAND ----------

# MAGIC %md ### Join batch data

# COMMAND ----------

# DBTITLE 1,Visualize turbine status files
display(dbutils.fs.ls('/mnt/quentin-demo-resources/turbine/status'))

# COMMAND ----------

# DBTITLE 1,Visualize turbine status data
spark.read.format("parquet").load("/mnt/quentin-demo-resources/turbine/status").display()

# COMMAND ----------

# DBTITLE 1,Convert turbine status to a Delta table
# MAGIC %sql
# MAGIC --Save the status data as our turbine_status table
# MAGIC CREATE OR REPLACE TABLE turbine_status AS SELECT * FROM parquet.`/mnt/quentin-demo-resources/turbine/status`;
# MAGIC 
# MAGIC SELECT * FROM turbine_status

# COMMAND ----------

# DBTITLE 1,Join data with turbine status (Damaged or Healthy)
turbine_stream = spark.readStream.table('turbine_silver')
turbine_status = spark.read.table("turbine_status")

#Left join between turbine_stream and turbine_status on the 'id' key and save back the result as the "turbine_gold" table
turbine_stream.join(turbine_status, on='ID', how='left') \
              .writeStream.format('delta') \
              .option("checkpointLocation", path+"/gold_checkpoint") \
              .table('turbine_gold')

# COMMAND ----------

# DBTITLE 1,Visualize our Gold data
# MAGIC %sql
# MAGIC --Our turbine gold table should be up and running!
# MAGIC select TIMESTAMP, AN3, SPEED, status from turbine_gold;

# COMMAND ----------

# MAGIC %md ### Constraints ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC Let's create some constraints to avoid bad data to flow through our pipeline and to help us identify potential issues with our data.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE turbine_gold CHANGE COLUMN ID SET NOT NULL;
# MAGIC ALTER TABLE turbine_gold ADD CONSTRAINT validateStatus CHECK (status IN ('healthy', 'damaged'));

# COMMAND ----------

# MAGIC %md Let's try to insert data with `null` id.

# COMMAND ----------

# MAGIC %sql INSERT INTO turbine_gold VALUES (null, 1, 1, 1, 1, 1, 1, 1, 1, 1, '2020-06-09T11:56:31.000Z', null, 'damaged')

# COMMAND ----------

# MAGIC %md Now, let's try to insert data with a date out of the defined range.

# COMMAND ----------

# MAGIC %sql INSERT INTO turbine_gold VALUES (1000, 1, 1, 1, 1, 1, 1, 1, 1, 1, '2020-06-09T11:56:31.000Z', null, 'running')

# COMMAND ----------

# MAGIC %md ### Schema Enforcement ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC To show you how schema enforcement works, let's try to insert a record with a new column -- `new_col` -- that doesn't match our existing Delta Lake table schema.

# COMMAND ----------

# MAGIC %sql INSERT INTO turbine_gold SELECT *, 0 as new_col FROM turbine_gold LIMIT 1

# COMMAND ----------

# MAGIC %md **Schema enforcement helps keep our tables clean and tidy so that we can trust the data we have stored in Delta Lake.** The writes above were blocked because the schema of the new data did not match the schema of table (see the exception details). See more information about how it works [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html).

# COMMAND ----------

# MAGIC %md ### Schema Evolution ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC If we ***want*** to update our Delta Lake table to match this data source's schema, we can do so using schema evolution. Simply enable the `autoMerge` option.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = True;
# MAGIC INSERT INTO turbine_gold SELECT *, 0 as new_col FROM turbine_gold LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full DML Support ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC 
# MAGIC Delta Lake brings ACID transactions and full DML support to data lakes: `DELETE`, `UPDATE`, `MERGE INTO`

# COMMAND ----------

# MAGIC %md
# MAGIC We just realized that something is wrong in the data before 2020! Let's DELETE all this data from our gold table as we don't want to have wrong value in our dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM turbine_gold where timestamp < '2020-00-01';

# COMMAND ----------

# MAGIC %md
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif" alt='Merge process' width=600/>
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO turbine_gold AS l
# MAGIC USING (SELECT * FROM turbine_gold LIMIT 10) AS m
# MAGIC ON l.ID = m.ID
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md ### Time Travel ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC 
# MAGIC Delta Lake’s time travel capabilities simplify building data pipelines for use cases including:
# MAGIC 
# MAGIC * Auditing Data Changes
# MAGIC * Reproducing experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC <img src="https://github.com/risan4841/img/blob/master/transactionallogs.png?raw=true" width=250/>
# MAGIC 
# MAGIC You can query snapshots of your tables by:
# MAGIC 1. **Version number**, or
# MAGIC 2. **Timestamp.**
# MAGIC 
# MAGIC using Python, Scala, and/or SQL syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to the [docs](https://docs.delta.io/latest/delta-utility.html#history), or [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md Review Delta Lake Table History for  Auditing & Governance
# MAGIC 
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY turbine_gold

# COMMAND ----------

# MAGIC %md Use time travel to count records both in the latest version of the data, as well as the initial version.
# MAGIC 
# MAGIC As you can see, 10 new records was added.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 'latest' as ver, count(*) as cnt FROM turbine_gold
# MAGIC UNION
# MAGIC SELECT 'initial' as ver, count(*) as cnt FROM turbine_gold VERSION AS OF 0

# COMMAND ----------

# MAGIC %md Rollback table to initial version using `RESTORE`

# COMMAND ----------

# MAGIC %sql RESTORE turbine_gold VERSION AS OF 433

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Access to Database
# MAGIC If on a Table-ACLs enabled High-Concurrency Cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: this won't work with standard cluster. 
# MAGIC -- DO NOT try to make it work during the demo.
# MAGIC -- Understand what's required as of now (which cluster type) and the implications
# MAGIC -- explore Databricks Unity Catalog initiative (go/uc) 
# MAGIC 
# MAGIC GRANT SELECT ON DATABASE turbine_gold TO `data.scientist@databricks.com`;
# MAGIC GRANT SELECT ON DATABASE turbine_gold TO `data.analyst@databricks.com`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Don't forget to Cancel all the streams once your demo is over

# COMMAND ----------

for s in spark.streams.active:
  s.stop()
