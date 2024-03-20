# Databricks notebook source
# MAGIC %md https://s3.console.aws.amazon.com/s3/buckets/databricks-vr?region=us-west-2&tab=objects

# COMMAND ----------

# dbutils.widgets.text('s3_path', '', 'S3 Path')

# COMMAND ----------

s3_path = dbutils.widgets.get('s3_path')

# COMMAND ----------

# MAGIC %md
# MAGIC # Securing access to External Tables / Files with Unity Catalog
# MAGIC
# MAGIC By default, Unity Catalog will create managed tables in your primary storage, providing a secured table access for all your users.
# MAGIC
# MAGIC In addition to these managed tables, you can manage access to External tables and files, located in another cloud storage (S3/ADLS/GCS). 
# MAGIC
# MAGIC This give you capabilities to ensure a full data governance, storing your main tables in the managed catalog/storage while ensuring secure access for for specific cloud storage.
# MAGIC
# MAGIC <br><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-global.png" style="float:right; margin-left:10px" width="1100"/>
# MAGIC
# MAGIC <!-- tracking, please do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Fexternal_location%2Faws&dt=FEATURE_UC_EXTERNAL_LOC_AZURE">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cluster
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
# MAGIC
# MAGIC
# MAGIC To use Unity Catalog, make sure you create a cluster with the security mode enabled.
# MAGIC
# MAGIC Go in the compute page, create a new cluster.
# MAGIC
# MAGIC Select either `Single User` or `Shared` mode.

# COMMAND ----------

# MAGIC %md-sandbox ## Files
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location.png" style="float:right; margin-left:10px" width="800"/>
# MAGIC
# MAGIC
# MAGIC Accessing external cloud storage is easily done using `External locations`.
# MAGIC
# MAGIC This can be done using 3 simple SQL command:
# MAGIC
# MAGIC
# MAGIC 1. First, create a Storage credential. It'll contain the IAM role/SP required to access your cloud storage
# MAGIC 1. Create an External location using your Storage credential. It can be any cloud location (a sub folder)
# MAGIC 1. Finally, Grant permissions to your users to access this Storage Credential

# COMMAND ----------

# MAGIC %md-sandbox ### Storage Credential
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-1.png" style="float:right; margin-left:10px" width="700px"/>
# MAGIC
# MAGIC The first step is to create the `STORAGE CREDENTIAL`.
# MAGIC
# MAGIC To do that, we'll use Databricks Unity Catalog UI:
# MAGIC
# MAGIC 1. Open the Data Explorer in DBSQL
# MAGIC 1. Select the "Storage Credential" menu
# MAGIC 1. Click on "Create Credential"
# MAGIC 1. Fill your credential information: the name and IAM role you will be using
# MAGIC
# MAGIC Because you need to be ADMIN, this step has been created for you.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-cred.png" width="400"/>

# COMMAND ----------

# MAGIC %md ### External Location

# COMMAND ----------

# MAGIC %sql CREATE EXTERNAL LOCATION IF NOT EXISTS vr_fraud_location
# MAGIC URL '${s3_path}/fraud'
# MAGIC WITH (STORAGE CREDENTIAL field_demos_credential)

# COMMAND ----------

# MAGIC %md ### Grant Access

# COMMAND ----------

# MAGIC %sql GRANT READ FILES ON EXTERNAL LOCATION `vr_fraud_location` TO `account users`;

# COMMAND ----------

# MAGIC %sql SHOW GRANTS ON EXTERNAL LOCATION vr_fraud_location

# COMMAND ----------

# MAGIC %md ### Accessing data

# COMMAND ----------

# DBTITLE 1,List files in an allowed location
display(dbutils.fs.ls(f'{s3_path}/fraud/dev/customers_silver'))

# COMMAND ----------

# DBTITLE 1,Read file in an allowed location
spark.read.json(f'{s3_path}/fraud/dev/raw/atm_visits/part-00000-tid-1188815250474084220-34e2faeb-925c-4c10-9613-80195e843fb9-44-1-c000.json').display()

# COMMAND ----------

# DBTITLE 1,List files in a forbidden location
display(dbutils.fs.ls(f'{s3_path}/customers'))

# COMMAND ----------

# DBTITLE 1,Read file in a forbidden location
spark.read.csv(f'{s3_path}/customers/olist_customers_dataset.csv').display()

# COMMAND ----------

# MAGIC %md ## Tables

# COMMAND ----------

# MAGIC %md-sandbox ### Catalog
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
# MAGIC
# MAGIC The first step is to create a new catalog.
# MAGIC
# MAGIC Unity Catalog works with 3 layers:
# MAGIC
# MAGIC * CATALOG
# MAGIC * SCHEMA (or DATABASE)
# MAGIC * TABLE
# MAGIC
# MAGIC To access one table, you can specify the full path: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`
# MAGIC
# MAGIC Note that the tables created before Unity Catalog are saved under the catalog named `hive_metastore`. Unity Catalog features are not available for this catalog.
# MAGIC
# MAGIC Note that Unity Catalog comes in addition to your existing data, not hard change required!

# COMMAND ----------

# MAGIC %sql CREATE CATALOG IF NOT EXISTS vr_fraud

# COMMAND ----------

# MAGIC %md ### Database

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS vr_fraud.dev
# MAGIC LOCATION '${s3_path}/fraud/vr_fraud_dev'

# COMMAND ----------

# MAGIC %md ### Managed Table

# COMMAND ----------

# MAGIC %sql CREATE TABLE IF NOT EXISTS vr_fraud.dev.visits_bronze (
# MAGIC   amount BIGINT,
# MAGIC   atm_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   day BIGINT,
# MAGIC   fraud_report STRING,
# MAGIC   hour BIGINT,
# MAGIC   min BIGINT,
# MAGIC   month BIGINT,
# MAGIC   sec BIGINT,
# MAGIC   visit_id BIGINT,
# MAGIC   withdrawl_or_deposit STRING,
# MAGIC   year BIGINT,
# MAGIC   _rescued_data STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md ### External Table

# COMMAND ----------

# MAGIC %sql CREATE TABLE IF NOT EXISTS vr_fraud.dev.customers_silver (
# MAGIC   customer_id BIGINT,
# MAGIC   card_number BIGINT,
# MAGIC   checking_savings STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   customer_since_date DATE)
# MAGIC LOCATION '${s3_path}/fraud/dev/customers_silver'

# COMMAND ----------

# MAGIC %md ### Grant Access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's grant all users a SELECT
# MAGIC GRANT SELECT ON TABLE vr_fraud.dev.customers_silver TO `account users`;
# MAGIC
# MAGIC -- We'll grant an extra MODIFY to our Data Engineers
# MAGIC GRANT SELECT, MODIFY ON TABLE vr_fraud.dev.customers_silver TO `dataengineers`;

# COMMAND ----------

# MAGIC %sql SHOW GRANTS ON TABLE vr_fraud.dev.customers_silver

# COMMAND ----------

# MAGIC %md ## Delta Sharing
# MAGIC
# MAGIC Delta Sharing let you share data with external recipient without creating copy of the data. Once they're authorized, recipients can access and download your data directly.
# MAGIC
# MAGIC In Delta Sharing, it all starts with a Delta Lake table registered in the Delta Sharing Server by the data provider. <br/>
# MAGIC This is done with the following steps:
# MAGIC - Create a RECIPIENT and share activation link with your recipient 
# MAGIC - Create a SHARE
# MAGIC - Add your Delta tables to the given SHARE
# MAGIC - GRANT SELECT on your SHARE to your RECIPIENT
# MAGIC  
# MAGIC Once this is done, your customer will be able to download the credential files and use it to access the data directly.
# MAGIC
# MAGIC <br>
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow.png" width="1000" />

# COMMAND ----------

# MAGIC %md ### Share

# COMMAND ----------

# MAGIC %sql
# MAGIC drop share if exists vr_fraud_share;
# MAGIC drop recipient if exists vr_fraud_recipient;

# COMMAND ----------

# MAGIC %sql CREATE SHARE IF NOT EXISTS vr_fraud_share

# COMMAND ----------

# MAGIC %md ### Add Tables

# COMMAND ----------

# MAGIC %sql -- Add complete tables
# MAGIC ALTER SHARE vr_fraud_share
# MAGIC ADD TABLE vr_fraud.dev.visits_gold;

# COMMAND ----------

# MAGIC %sql -- Add subsets of tables
# MAGIC ALTER SHARE vr_fraud_share
# MAGIC ADD TABLE vr_fraud.dev.visits_gold
# MAGIC PARTITION (bank = 'wellsfargo') AS vr_fraud_share.visits_wells;

# COMMAND ----------

# MAGIC %sql SHOW ALL IN SHARE vr_fraud_share

# COMMAND ----------

# MAGIC %md ### Recipient

# COMMAND ----------

# MAGIC %sql CREATE RECIPIENT IF NOT EXISTS vr_fraud_recipient;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow-5.png" width="600" style="float:right" />
# MAGIC
# MAGIC Each Recipient has an activation link that the consumer can use to download it's credential.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/kanonymity_share_activation.png" width=600>
# MAGIC
# MAGIC The credentials are typically saved as a file containing. The Delta Server identify and authorize consumer based on these identifiants.<br/>
# MAGIC Note that the activation link is single use. You can only access it once (it'll return null if already used)

# COMMAND ----------

# MAGIC %md ### Grant Access

# COMMAND ----------

# MAGIC %sql GRANT SELECT ON SHARE vr_fraud_share TO RECIPIENT vr_fraud_recipient

# COMMAND ----------

# MAGIC %sql SHOW GRANT ON SHARE vr_fraud_share

# COMMAND ----------

# MAGIC %md ### Accessing data

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/shares/vr_fraud.share')

# COMMAND ----------

# MAGIC %pip install delta-sharing

# COMMAND ----------

# DBTITLE 1,Listing data in a share
import delta_sharing

# Let's re-use it directly to access our data. If you get access error, please re-run the previous notebook
share_profile = '/dbfs/FileStore/shares/config__3_.share'

# Create a SharingClient
client = delta_sharing.SharingClient(share_profile)

# List all shared tables.
display(client.list_all_tables())

# COMMAND ----------

# DBTITLE 1,Accessing data in a share
# Use delta sharing client to load data
df = delta_sharing.load_as_spark(f"{share_profile}#vr_fraud_share.vr_fraud_share.visits_wells")
display(df.head(10))

# COMMAND ----------

# Use delta sharing client to load data
df = delta_sharing.load_as_pandas(f"{share_profile}#vr_fraud_share.vr_fraud_share.visits_wells")
df.head(10)

# COMMAND ----------

# MAGIC %sql CREATE TEMPORARY TABLE visits_wells
# MAGIC USING deltaSharing
# MAGIC LOCATION '/FileStore/shares/vr_fraud.share#vr_fraud_share.vr_fraud_share.visits_wells';
# MAGIC     
# MAGIC SELECT * FROM visits_wells

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Lineage
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/uc-lineage-slide.png" style="float:right; margin-left:10px" width="700"/>
# MAGIC
# MAGIC Unity Catalog captures runtime data lineage for any table to table operation executed on a Databricks cluster or SQL endpoint. Lineage operates across all languages (SQL, Python, Scala and R) and it can be visualized in the Data Explorer in near-real-time, and also retrieved via REST API.
# MAGIC
# MAGIC Lineage is available at two granularity levels:
# MAGIC - Tables
# MAGIC - Columns: ideal to track GDPR dependencies
# MAGIC
# MAGIC Lineage takes into account the Table ACLs present in Unity Catalog. If a user is not allowed to see a table at a certain point of the graph, its information are redacted, but they can still see that a upstream or downstream table is present.
# MAGIC
# MAGIC ## Working with Lineage
# MAGIC
# MAGIC No modifications are needed to the existing code to generate the lineage. As long as you operate with tables saved in the Unity Catalog, Databricks will capture all lineage informations for you.
# MAGIC
# MAGIC Requirements:
# MAGIC - Make sure you set `spark.databricks.dataLineage.enabled true`in your cluster setup
# MAGIC - Source and target tables must be registered in a Unity Catalog metastore to be eligible for lineage capture
# MAGIC - The data manipulation must be performed using Spark DataFrame language (python/SQL)
# MAGIC - To view lineage, users must have the SELECT privilege on the table
# MAGIC
# MAGIC Existing Limitations:
# MAGIC - Streaming operations are not yet supported
# MAGIC - Lineage will not be captured when data is written directly to files in cloud storage even if a table is defined at that location (eg spark.write.save(“s3:/mybucket/mytable/”) will not produce lineage)
# MAGIC - Lineage is not captured across workspaces (eg if a table A > table B transformation is performed in workspace 1 and table B > table C in workspace 2, each workspace will show a partial view of the lineage for table B)
# MAGIC - Lineage is computed on a 30 day rolling window, meaning that lineage will not be displayed for tables that have not been modified in more than 30 days ago
# MAGIC
# MAGIC <!-- tracking, please do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Flineage%2Flineage&dt=FEATURE_UC_LINAGE">
