-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify ETL with Delta Live Table
-- MAGIC
-- MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
-- MAGIC
-- MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-1.png" width="700"/>
-- MAGIC
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC
-- MAGIC ## Our Delta Live Table pipeline
-- MAGIC
-- MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
-- MAGIC
-- MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
-- MAGIC
-- MAGIC **Your DLT Pipeline is ready!** Your pipeline was started using this notebook and is <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/460f840c-9ecc-4d19-a661-f60fd3a88297">available here</a>.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_sql&dt=DLT">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC Our datasets are coming from 3 different systems and saved under a cloud storage folder (S3/ADLS/GCS): 
-- MAGIC
-- MAGIC * `loans/raw_transactions` (loans uploader here in every few minutes)
-- MAGIC * `loans/ref_accounting_treatment` (reference table, mostly static)
-- MAGIC * `loans/historical_loans` (loan from legacy system, new data added every week)
-- MAGIC
-- MAGIC Let's ingest this data incrementally, and then compute a couple of aggregates that we'll need for our final Dashboard to report our KPI.

-- COMMAND ----------

-- DBTITLE 1,Let's review the incoming data
-- MAGIC %fs ls /demos/dlt/loans/raw_transactions

-- COMMAND ----------

-- MAGIC %fs head dbfs:/demos/dlt/loans/raw_transactions/part-00000-tid-8519183599634130094-3767a22a-3f0e-49fc-b913-e83e0566058a-130-1-c000.json

-- COMMAND ----------

-- MAGIC %fs cp dbfs:/demos/dlt/loans/raw_transactions/part-00000-tid-8519183599634130094-3767a22a-3f0e-49fc-b913-e83e0566058a-130-1-c000.json /FileStore/tmp_json/part-00000-tid-8519183599634130094-3767a22a-3f0e-49fc-b913-e83e0566058a-130-1-c000.json

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-2.png" width="600"/>
-- MAGIC
-- MAGIC Our raw data is being sent to a blob storage. 
-- MAGIC
-- MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
-- MAGIC
-- MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
-- MAGIC
-- MAGIC For more detail on Autoloader, you can see `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC #### STREAMING LIVE TABLE 
-- MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. Without `STREAMING`, you will scan and ingest all the data available at once. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) for more details

-- COMMAND ----------

-- DBTITLE 1,Capture new incoming transactions
CREATE STREAMING LIVE TABLE raw_txs COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone" AS
SELECT
  *
FROM
  cloud_files(
    '/demos/dlt/loans/raw_transactions',
    'json',
    map("cloudFiles.inferColumnTypes", "true")
  )

-- COMMAND ----------

-- DBTITLE 1,Reference table - metadata (small & almost static)
CREATE LIVE TABLE ref_accounting_treatment COMMENT "Lookup mapping for accounting codes" AS
SELECT
  *
FROM
  delta.`/demos/dlt/loans/ref_accounting_treatment`

-- COMMAND ----------

-- DBTITLE 1,Historical transaction from legacy system
-- as this is only refreshed at a weekly basis, we can lower the interval
CREATE STREAMING LIVE TABLE raw_historical_loans
  TBLPROPERTIES ("pipelines.trigger.interval"="6 hour")
  COMMENT "Raw historical transactions"
AS SELECT * FROM cloud_files('/demos/dlt/loans/historical_loans', 'csv', map("cloudFiles.inferColumnTypes", "true"))
