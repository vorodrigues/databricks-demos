import dlt
from pyspark.sql.functions import col

# Remove registros com problemas de qualidade de dados e deduplica os registros e mantém o mais recente
@dlt.table(comment="Dados de transações de vendas limpos")
@dlt.expect_or_drop("Chave primária válida", "sales_id IS NOT NULL")
@dlt.expect_or_drop("Schema válido", "_rescued_data IS NULL")
def sales_silver():
  return (dlt.read("sales_bronze")
    .orderBy("date_key", ascending=False)
    .dropDuplicates(["sales_id"])
    .withColumn('sales_id', col('sales_id').cast('long'))
    .withColumn('product_id', col('product_id').cast('long'))
    .withColumn('store_id', col('store_id').cast('long'))
    .withColumn('date_key', col('date_key').cast('date'))
    .withColumn('sales_quantity', col('sales_quantity').cast('long'))
    .withColumn('sales_amount', col('sales_amount').cast('double'))
  )