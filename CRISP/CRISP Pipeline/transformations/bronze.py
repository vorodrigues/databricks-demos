import dlt

# Ingere incrementalmente arquivos JSON usando o Databricks Auto Loader
@dlt.table(comment="Dados de transações de vendas crus ingeridos incrementalmente a partir do storage da landing zone")
def sales_bronze():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://one-env/vr/crisp/sales"))
  
@dlt.view()
def product():
  return spark.table("vr_demo.crisp.dim_product")

@dlt.view()
def store():
  return spark.table("vr_demo.crisp.dim_store")