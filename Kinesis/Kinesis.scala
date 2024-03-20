// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC <img src="https://miro.medium.com/max/595/0*UQBKjEff1uIsXH8W" width="400">
// MAGIC 
// MAGIC | Specs                               |
// MAGIC |----------------------|--------------|
// MAGIC | Data Stream Name     | oetrta       |
// MAGIC | Shards               | 1            |
// MAGIC | Region               | us-west-2    | 
// MAGIC 
// MAGIC <br>
// MAGIC 1. Attach IAM role `oetrta-IAM-access` to this cluster
// MAGIC 2. (Optional) Run the Kinesis Data Generator (producer) notebook [here](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/1039385502021748/command/1039385502021749)
// MAGIC 
// MAGIC Docs: https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html#amazon-kinesis

// COMMAND ----------

// DBTITLE 1,Get connection info from secrets
val kinesisRegion     = dbutils.secrets.get( "oetrta", "kinesis-region"      ) 
val kinesisStreamName = dbutils.secrets.get( "oetrta", "kinesis-stream-name" )

// COMMAND ----------

// DBTITLE 1,Set Configuration for Kinesis Streams
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import java.nio.ByteBuffer
import scala.util.Random

// COMMAND ----------

// DBTITLE 1,Structured Streaming Query that reads words from Kinesis and counts them up
val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", kinesisStreamName )
  .option("region",     kinesisRegion     )
  .option("initialPosition", "TRIM_HORIZON")
  .load()

val result = kinesis.selectExpr("lcase(CAST(data as STRING)) as word")
  .groupBy($"word")
  .count()
display(result)

// COMMAND ----------

// DBTITLE 1,Write words to Kinesis
// Create the low-level Kinesis Client from the AWS Java SDK.
val kinesisClient = AmazonKinesisClientBuilder.standard()
  .withRegion(kinesisRegion)
  .build()

println(s"Putting words onto stream $kinesisStreamName")
var lastSequenceNumber: String = null

for (i <- 0 to 10) {
  val time = System.currentTimeMillis
  // Generate words: fox in sox
  for (word <- Seq("Through", "three", "cheese", "trees", "three", "free", "fleas", "flew", "While", "these", "fleas", "flew", "freezy", "breeze", "blew", "Freezy", "breeze", "made", "these", "three", "trees", "freeze", "Freezy", "trees", "made", "these", "trees", "cheese", "freeze", "That's", "what", "made", "these", "three", "free", "fleas", "sneeze")) {
    val data = s"$word"
    val partitionKey = s"$word"
    val request = new PutRecordRequest()
        .withStreamName(kinesisStreamName)
        .withPartitionKey(partitionKey)
        .withData(ByteBuffer.wrap(data.getBytes()))
    if (lastSequenceNumber != null) {
      request.setSequenceNumberForOrdering(lastSequenceNumber)
    }    
    val result = kinesisClient.putRecord(request)
    lastSequenceNumber = result.getSequenceNumber()
  }
  Thread.sleep(math.max(10000 - (System.currentTimeMillis - time), 0)) // loop around every ~10 seconds 
}
