// Databricks notebook source
// MAGIC %md **Configurations**
// MAGIC 
// MAGIC **DBR:**
// MAGIC - 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12)
// MAGIC 
// MAGIC **Lib:** 
// MAGIC - org.apache.hudi:hudi-spark3-bundle_2.12:0.10.0
// MAGIC 
// MAGIC **Spark config:**
// MAGIC - spark.serializer org.apache.spark.serializer.KryoSerializer

// COMMAND ----------

// spark-shell
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord

val tableName = "hudi_trips_cow"
val basePath = "/FileStore/vr/hudi"
val dataGen = new DataGenerator

// COMMAND ----------

// MAGIC %md # Create Hudi table

// COMMAND ----------

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)

// COMMAND ----------

// MAGIC %md # Read table

// COMMAND ----------

val tripsSnapshotDF = spark
  .read
  .format("hudi")
  .load(basePath+"/*/*/*")

// COMMAND ----------

display(tripsSnapshotDF)

// COMMAND ----------

// MAGIC %md # Temp View

// COMMAND ----------

tripsSnapshotDF.createOrReplaceTempView("tripsSnapshotDF")

// COMMAND ----------

display(spark.sql("select * from tripsSnapshotDF"))

// COMMAND ----------

// MAGIC %sql select count(*) from tripsSnapshotDF

// COMMAND ----------

// MAGIC %sql select * from tripsSnapshotDF where partitionpath = 'americas/brazil/sao_paulo'

// COMMAND ----------

// MAGIC %sql select partitionpath, sum(fare) as total from tripsSnapshotDF group by partitionpath

// COMMAND ----------

// MAGIC %md # Metastore

// COMMAND ----------

// MAGIC %sql 
// MAGIC create table if not exists vr_fraud_dev.hudi (
// MAGIC   begin_lat double,
// MAGIC   begin_lon double,
// MAGIC   driver string,
// MAGIC   end_lat double,
// MAGIC   end_lon double,
// MAGIC   fare double,
// MAGIC   partitionpath string,
// MAGIC   rider string,
// MAGIC   ts long,
// MAGIC   uuid string
// MAGIC ) using hudi
// MAGIC location '/FileStore/vr/hudi/*/*/*'
// MAGIC options (
// MAGIC   type = 'cow',
// MAGIC   primaryKey = 'uuid',
// MAGIC   preCombineField = 'ts'
// MAGIC  ) 
// MAGIC partitioned by (partitionpath);

// COMMAND ----------

// MAGIC %sql select count(*) from vr_fraud_dev.hudi

// COMMAND ----------

// MAGIC %sql select partitionpath, sum(fare) as total from vr_fraud_dev.hudi group by partitionpath
