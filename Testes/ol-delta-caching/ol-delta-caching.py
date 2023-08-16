# Databricks notebook source
# MAGIC %md # Parquet
# MAGIC
# MAGIC In all scenarios, we get the following error:
# MAGIC ```
# MAGIC FileReadException: Error while reading file dbfs:/<file_path>.snappy.parquet. It is possible the underlying files have been updated.
# MAGIC You can explicitly invalidate the cache in Spark by running 'REFRESH TABLE tableName' command in SQL or by recreating the Dataset/DataFrame involved.
# MAGIC If Delta cache is stale or the underlying files have been removed, you can invalidate Delta cache manually by restarting the cluster.
# MAGIC Caused by: FileNotFoundException: dbfs:/<file_path>.snappy.parquet
# MAGIC ```
# MAGIC
# MAGIC It occurs after following the steps below:<br><br>
# MAGIC
# MAGIC 1. Read a table
# MAGIC 2. Update it
# MAGIC 3. Read again
# MAGIC
# MAGIC It can be solved by the following approaches (which can be expensive and not practical to implement):<br><br>
# MAGIC
# MAGIC - Explicitly invalidating the cache with `REFRESH TABLE tableName` (as suggested in the error message)
# MAGIC - Detaching & re-attaching the notebook to the cluster

# COMMAND ----------

# MAGIC %md ## Examine data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists vr_tests.visits_gold_parquet;
# MAGIC create table vr_tests.visits_gold_parquet using parquet as select * from vr_fraud_dev.visits_gold limit 1000

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

# MAGIC %sql select count(*) from vr_tests.visits_gold_parquet

# COMMAND ----------

# MAGIC %sql describe extended vr_tests.visits_gold_parquet

# COMMAND ----------

# MAGIC %md ## Delta Cache enabled

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', 'true')
spark.conf.set('spark.sql.files.ignoreMissingFiles', 'false')

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

from pyspark.sql.functions import *

(spark.table('vr_tests.visits_gold_parquet')
  .withColumn('bank', when(col('bank')=='bank_of_america', lit('bofa')).otherwise(col('bank')))
  .write
  .mode('overwrite')
  .parquet('dbfs:/user/hive/warehouse/vr_tests.db/visits_gold_parquet')
)

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

# MAGIC %md ## Delta Cache disabled

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', 'false')
spark.conf.set('spark.sql.files.ignoreMissingFiles', 'false')

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

from pyspark.sql.functions import *

(spark.table('vr_tests.visits_gold_parquet')
  .withColumn('bank', when(col('bank')=='bank_of_america', lit('bofa')).otherwise(col('bank')))
  .write
  .mode('overwrite')
  .parquet('dbfs:/user/hive/warehouse/vr_tests.db/visits_gold_parquet')
)

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

# MAGIC %md ## ignoreMissingFiles enabled
# MAGIC
# MAGIC In this scenario, there are no errors thrown, but the updated files aren't read

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', 'false')
spark.conf.set('spark.sql.files.ignoreMissingFiles', 'true')

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

from pyspark.sql.functions import *

(spark.table('vr_tests.visits_gold_parquet')
  .withColumn('bank', when(col('bank')=='bank_of_america', lit('bofa')).otherwise(col('bank')))
  .write
  .mode('overwrite')
  .parquet('dbfs:/user/hive/warehouse/vr_tests.db/visits_gold_parquet')
)

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_parquet

# COMMAND ----------

# MAGIC %md # Delta
# MAGIC
# MAGIC In this scenario, we get no errors and the process executes successfully

# COMMAND ----------

# MAGIC %md ## Examine data

# COMMAND ----------

# MAGIC %sql -- using drop/create to avoid data versioning
# MAGIC drop table if exists vr_tests.visits_gold_delta;
# MAGIC create table vr_tests.visits_gold_delta as select * from vr_fraud_dev.visits_gold limit 1000

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_delta

# COMMAND ----------

# MAGIC %sql select count(*) from vr_tests.visits_gold_delta

# COMMAND ----------

# MAGIC %sql describe extended vr_tests.visits_gold_delta

# COMMAND ----------

# MAGIC %md ## Delta Cache enabled

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', 'true')
spark.conf.set('spark.sql.files.ignoreMissingFiles', 'false')

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_delta

# COMMAND ----------

from pyspark.sql.functions import *

(spark.table('vr_tests.visits_gold_delta')
  .withColumn('bank', when(col('bank')=='bank_of_america', lit('bofa')).otherwise(col('bank')))
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/user/hive/warehouse/vr_tests.db/visits_gold_delta')
)

# COMMAND ----------

# MAGIC %sql select * from vr_tests.visits_gold_delta
