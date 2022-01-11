# Databricks notebook source
# MAGIC %md # Prepare to download

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %sh mkdir ~/.kaggle

# COMMAND ----------

# MAGIC %%file ~/.kaggle/kaggle.json
# MAGIC {"username":"vorodrigues","key":"0927afdb2cdff7695c4ad65e44e3a8e2"}

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install p7zip
# MAGIC apt-get install p7zip-full

# COMMAND ----------

# MAGIC %md # Download and Extract

# COMMAND ----------

# MAGIC %sh
# MAGIC chmod 600 ~/.kaggle/kaggle.json
# MAGIC kaggle competitions download -c kkbox-churn-prediction-challenge
# MAGIC unzip kkbox-churn-prediction-challenge.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC 7z x members_v3.csv.7z
# MAGIC 7z x transactions.csv.7z
# MAGIC 7z x transactions_v2.csv.7z
# MAGIC 7z x user_logs.csv.7z
# MAGIC 7z x user_logs_v2.csv.7z
# MAGIC ls

# COMMAND ----------

# MAGIC %md # Create folders

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/FileStore/kkbox/members

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/FileStore/kkbox/transactions

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/FileStore/kkbox/user_logs

# COMMAND ----------

# MAGIC %md # Copy files to DBFS

# COMMAND ----------

# MAGIC %fs cp file:/databricks/driver/members_v3.csv dbfs:/FileStore/kkbox/members/members_v3.csv

# COMMAND ----------

# MAGIC %fs cp file:/databricks/driver/transactions.csv dbfs:/FileStore/kkbox/transactions/transactions.csv

# COMMAND ----------

# MAGIC %fs cp file:/databricks/driver/user_logs.csv dbfs:/FileStore/kkbox/user_logs/user_logs.csv

# COMMAND ----------

# MAGIC %fs cp file:/databricks/driver/data/churn_comp_refresh/transactions_v2.csv dbfs:/FileStore/kkbox/transactions/transactions_v2.csv

# COMMAND ----------

# MAGIC %fs cp file:/databricks/driver/data/churn_comp_refresh/user_logs_v2.csv dbfs:/FileStore/kkbox/user_logs/user_logs_v2.csv
