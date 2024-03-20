# Databricks notebook source
import dlt

# COMMAND ----------

def expect_or_quarantine(dst, rules, date_col, retention):

  def wrapper(func):
    
    # Prepara regras da quarentena
    quarantine_rules = {}
    for name, rule in rules.items():
      quarantine_rules[name] = f"NOT ({rule})"

    # Cria a tabela com os registros válidos
    @dlt.table(name=dst)
    @dlt.expect_all_or_drop(rules)
    def clean():
      return func()

    # Cria a tabela com os registros inválidos
    @dlt.table(name=f'{dst}_quarantine')
    @dlt.expect_all_or_drop(quarantine_rules)
    def quarantined():
      return func()
      
    # Remove registros antigos da quarentena
    spark.sql(f'DELETE FROM {dst}_quarantine WHERE {date_col} < TODAY() - INTERVAL {retention} DAYS')
  
  return wrapper


# COMMAND ----------

@dlt.view()
def raw():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://one-env/vr/crisp/sales"))

# COMMAND ----------

rules = {
  "Chave primária válida" : "sales_id IS NOT NULL",
  "Schema válido" : "_rescued_data IS NULL"
}

@expect_or_quarantine(
  dst='test_quarantine', 
  rules=rules,
  date_col='date_key',
  retention=7
)
def run_quarantine():
  return dlt.readStream('raw')
