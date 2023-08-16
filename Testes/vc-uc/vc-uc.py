# Databricks notebook source
# MAGIC %sql CREATE TABLE test_ext_tbl using delta
# MAGIC LOCATION 's3://databricks-vr/fraud/dev/customers_silver'

# COMMAND ----------

# MAGIC %sql CREATE TABLE vr_tests.uc.test_ext_tbl using delta
# MAGIC LOCATION 's3://databricks-vr/fraud/dev/customers_silver'

# COMMAND ----------

# MAGIC %sql drop table vr_tests.uc.test_ext_tbl

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql create table `vr_tests.uc.test_ext_tbl` as select * from vr_fraud.dev.customers_silver limit 100

# COMMAND ----------

# MAGIC %sql create table `vr_tests`.`uc`.`test_ext_tbl` as select * from vr_fraud.dev.customers_silver limit 100
