-- Databricks notebook source
-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-3.png" width="600"/>
-- MAGIC
-- MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
-- MAGIC
-- MAGIC To consume only increment from the Bronze layer like `BZ_raw_txs`, we'll be using the `stream` keyworkd: `stream(LIVE.BZ_raw_txs)`
-- MAGIC
-- MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
-- MAGIC
-- MAGIC #### Expectations
-- MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

-- COMMAND ----------

-- DBTITLE 1,Enrich transactions with metadata
CREATE STREAMING LIVE VIEW new_txs 
  COMMENT "Livestream of new transactions"
AS SELECT txs.*, ref.accounting_treatment as accounting_treatment FROM stream(LIVE.raw_txs) txs
  INNER JOIN live.ref_accounting_treatment ref ON txs.accounting_treatment_id = ref.id

-- COMMAND ----------

-- DBTITLE 1,Keep only the proper transactions. Fail if cost center isn't correct, discard the others.
CREATE STREAMING LIVE TABLE cleaned_new_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
  COMMENT "Livestream of new transactions, cleaned and compliant"
AS SELECT * from STREAM(live.new_txs)

-- COMMAND ----------

-- DBTITLE 1,Let's quarantine the bad transaction for further analysis
-- This is the inverse condition of the above statement to quarantine incorrect data for further analysis.
CREATE STREAMING LIVE TABLE quarantine_bad_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date <= date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance <= 0 OR arrears_balance <= 0) ON VIOLATION DROP ROW
)
  COMMENT "Incorrect transactions requiring human analysis"
AS SELECT * from STREAM(live.new_txs)

-- COMMAND ----------

-- DBTITLE 1,Enrich all historical transactions
CREATE LIVE TABLE historical_txs
  COMMENT "Historical loan transactions"
AS SELECT l.*, ref.accounting_treatment as accounting_treatment FROM LIVE.raw_historical_loans l
  INNER JOIN LIVE.ref_accounting_treatment ref ON l.accounting_treatment_id = ref.id

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Gold layer
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-4.png" width="600"/>
-- MAGIC
-- MAGIC Our last step is to materialize the Gold Layer.
-- MAGIC
-- MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per cost location
CREATE LIVE TABLE total_loan_balances
  COMMENT "Combines historical and new loan data for unified rollup of loan balances"
  TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols" = "location_code")
AS SELECT sum(revol_bal) AS bal, addr_state AS location_code FROM live.historical_txs GROUP BY addr_state
  UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per cost center
CREATE STREAMING LIVE TABLE new_loan_balances_by_cost_center
  COMMENT "New loan balances for consumption by different cost centers"
AS SELECT sum(balance) AS bal, cost_center_code FROM stream(live.cleaned_new_txs)
  GROUP BY cost_center_code

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per country
CREATE STREAMING LIVE TABLE new_loan_balances_by_country
  COMMENT "New loan balances per country"
AS SELECT sum(count) AS tot_count, country_code FROM stream(live.cleaned_new_txs)
  GROUP BY country_code

-- COMMAND ----------

-- MAGIC %md ## Next steps
-- MAGIC
-- MAGIC Your DLT pipeline is ready to be started. <a href="https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/d9705de0-de09-4e05-a055-2a6854bb039b/updates/bb727076-b0cd-407c-8600-35b5eb96b73d">Click here to access the pipeline</a> created for you using this notebook.
-- MAGIC
-- MAGIC To create a new one, Open the DLT menu, create a pipeline and select this notebook to run it. To generate sample data, please run the [companion notebook]($./_resources/00-Loan-Data-Generator) (make sure the path where you read and write the data are the same!)
-- MAGIC
-- MAGIC Datas Analyst can start using DBSQL to analyze data and track our Loan metrics.  Data Scientist can also access the data to start building models to predict payment default or other more advanced use-cases.

-- COMMAND ----------

-- MAGIC %md ## Tracking data quality
-- MAGIC
-- MAGIC Expectations stats are automatically available as system table.
-- MAGIC
-- MAGIC This information let you monitor your data ingestion quality. 
-- MAGIC
-- MAGIC You can leverage DBSQL to request these table and build custom alerts based on the metrics your business is tracking.
-- MAGIC
-- MAGIC
-- MAGIC See [how to access your DLT metrics]($./03-Log-Analysis)
-- MAGIC
-- MAGIC <img width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC
-- MAGIC <a href="/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357" target="_blank">Data Quality Dashboard example</a>
