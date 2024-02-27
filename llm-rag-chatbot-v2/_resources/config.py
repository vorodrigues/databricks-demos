# Databricks notebook source
# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-llm-rag-chatbot-victor_rodrigues` from the dropdown menu ([open cluster configuration](https://e2-demo-field-eng.cloud.databricks.com/#setting/clusters/1201-143005-dxhedgri/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('llm-rag-chatbot')` or re-install the demo: `dbdemos.install('llm-rag-chatbot')`*

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=local&notebook=%2Fconfig&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2Fconfig&version=1">

# COMMAND ----------

VECTOR_SEARCH_ENDPOINT_NAME="dbdemos_vs_endpoint"

DATABRICKS_SITEMAP_URL = "https://docs.databricks.com/en/doc-sitemap.xml"

catalog = "vr_demo"

#email = spark.sql('select current_user() as user').collect()[0]['user']
#username = email.split('@')[0].replace('.', '_')
#dbName = db = f"rag_chatbot_{username}"
dbName = db = "chatbot2"
