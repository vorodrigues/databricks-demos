# Databricks notebook source
dbutils.widgets.text('streaming_queries','')
streaming_queries = int(dbutils.widgets.get('streaming_queries'))
print(f'streaming_queries: {streaming_queries}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Template Job - Ingestão dados Raw - Trusted
# MAGIC
# MAGIC Nesse template é construído uma script de ingestão para o workflow do tipo (Job) para o consumo de dados da camada Raw e ingestão dos dados como Managed Tables no Databricks utilizando Unity Catalog. (Cluster Policies definidas pelo time de D&A garantem a escrita no unity catalog)

# COMMAND ----------

import os
import json
import pandas as pd
# import xmltodict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, from_json, lit, pandas_udf, schema_of_json, to_json, when, regexp_replace
from pyspark.sql.types import _parse_datatype_json_string
# from sparkaid import flatten

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.dataLineage.enabled","true")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")
spark.conf.set("spark.sql.shuffle.partitions", 16)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial
# MAGIC
# MAGIC Na célula abaixo configurar o dicionario: `KAFKA_TOPICS_CONFIG` com os tópicos que serão necessário ingestão do Kafka para camada Raw
# MAGIC - tópicos Kafka 
# MAGIC - metadados das tabelas a serem criadas
# MAGIC - STREAMING
# MAGIC
# MAGIC A Variavel ambiente para escrita no Catálogo de dados (dev ou prd) é atribuida automaticamente baseado nas variaveis de ambiente do tipo de cluster (prd ou dev)

# COMMAND ----------

dbutils.widgets.dropdown("streaming_mode", "continuous", ["continuous", "triggered"])
dbutils.widgets.dropdown("load_mode", "full_repair_load", ["from_last_checkpoint", "full_repair_load"])
#dbutils.widgets.removeAll()

# OUTPUT DATABRICKS TABLE PREFIX GLOBAL VARIABLE CONFIG
TABLE_NAME_PREFIX = "tb"

def get_tags_from_cluster_spark_cfg() -> dict:
    all_tags = {}
    for tag in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")):
        all_tags[tag['key']] = tag['value']
    return all_tags

all_tags = get_tags_from_cluster_spark_cfg()

JOB_NAME = all_tags.get('RunName')
COST_CENTER = all_tags.get('costCenter')
DOMAIN = all_tags.get('businessUnit') #.lower()
REQUESTER_PRODUCT_OWNER = all_tags.get('productOwner')
REQUESTER_PROJECT_NAME = all_tags.get('productName')

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

ENV = os.getenv("databricks_env")
# CATALOG_TRUSTED = f"{DOMAIN}-trusted-{ENV}"
# CATALOG_RAW = f"{DOMAIN}-raw-{ENV}"
CATALOG_TRUSTED = f"vr_tests"
CATALOG_RAW = f"vr_tests"
STREAMING = True if dbutils.widgets.get('streaming_mode') == 'continuous' else False


TRUSTED_TABLES_CONFIG = [
    {
        "table_name_trusted":f"delete_{i}",
        "table_name_raw":"delete_source",
        "description":"""Finalidade: 
                         Area que o Sistema atende: Aciaria da planta de Charqueadas. 
                         Dado que o Sistema Trata: Dados de produção da Aciaria de Charqueadas""",
        "source_system":"Oracle (MES Aciaria)",
        "data_owner":"NELAEP",
        "quality":"Bronze",
        "primary_keys":["visit_id"],
        "refresh_rate":"streaming",
        "si_classification":"Interno",
        "ingestion_system":"Delta",
        "critical_job_level": "P1",
        "data_retention_policy_days": "999999",
        "data_retention_policy_timestamp_column": "teste",
        "database": "uc"
    }
    for i in range(1,streaming_queries+1)
]

for idx, table in enumerate(TRUSTED_TABLES_CONFIG):
    TRUSTED_TABLES_CONFIG[idx]["product_owner_requester"] = REQUESTER_PRODUCT_OWNER
    TRUSTED_TABLES_CONFIG[idx]["workflow_job_name"] = JOB_NAME
    TRUSTED_TABLES_CONFIG[idx]["notebook_path"] = NOTEBOOK_PATH
    TRUSTED_TABLES_CONFIG[idx]["project"] = REQUESTER_PROJECT_NAME
    TRUSTED_TABLES_CONFIG[idx]["cost_center"] = COST_CENTER


# COMMAND ----------

TRUSTED_TABLES_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

# def _xml_to_json(xml_str: str) -> str:
#     """Python function to convert XML to JSON structure"""
#     parsed_xml = None
#     if xml_str:
#         o = xmltodict.parse(xml_str)
#         parsed_xml = json.dumps(o)
#         
#     return parsed_xml

@pandas_udf("string")
def extract_xml_series(s: pd.Series) -> pd.Series:
    """Spark UDF function to apply XML conversion to JSON structure"""
    #return s.apply(_xml_to_json)
    return s


# Preprocessing Data
# def _flatten_dataframe(micro_batch_df: DataFrame, ingestion_system: str) -> DataFrame:
#     """Flatten spark structtype column"""
#     data = flatten(micro_batch_df)
#     if ingestion_system == "Qlik":
#         data_columns = [column for column in data.columns if (column.startswith("data") or column == "headers_operation" or column == "headers_timestamp" or column == "surrogate_key")]
#         data = data.select(*data_columns)
#         for column in data.columns:
#             if column.startswith("data_"):
#                 column_parsed = column[5:]
#                 data = data.withColumnRenamed(column, column_parsed)
#     return data


def _get_condition_merge(merge_keys: list) -> str:
        """
        Get string to define merge into deltatable
        keys is a set of column names to use as condition for merge
        new data into delta table

        Args:
            keys: list

        Return:
            str
        """

        condition_string = f"updates.{merge_keys[0]} = actual.{merge_keys[0]}"

        for f in merge_keys[1:]:
            condition_string += f" AND updates.{str(f)} = actual.{str(f)}"

        return condition_string

def _optional_functions_apply(df: DataFrame, trusted_table_config: dict) -> DataFrame:
    """Apply optional functions (filter, drop or deserialize XML) when it is configured"""
    optional_functions = trusted_table_config.get("optional_functions", {})
    
    is_filter_requested = optional_functions.get("apply_data_filter", None)
    is_drop_columns_requested = optional_functions.get("apply_drop_columns", None)
    is_xml_deserialization_requested = optional_functions.get("apply_deserialize_xml_column", None)
    
    df_opt_func = df
    if is_filter_requested:
        filter_column_name = optional_functions["apply_data_filter"]["column_name"]
        value_to_filter = optional_functions["apply_data_filter"]["value_to_filter"]
        df_opt_func = df_opt_func.filter(col(filter_column_name) == value_to_filter)
    if is_drop_columns_requested:
        drop_columns = optional_functions["apply_drop_columns"]
        df_opt_func = df_opt_func.drop(drop_columns)
    if is_xml_deserialization_requested:
        xml_column_name = optional_functions["apply_deserialize_xml_column"]["column_name"]
        xml_schema_sample = optional_functions["apply_deserialize_xml_column"]["xml_schema_sample"]
        df_opt_func = df_opt_func.withColumn(xml_column_name, regexp_replace(extract_xml_series(xml_column_name), "@", ""))
        df_opt_func = df_opt_func.withColumn(xml_column_name, from_json(xml_column_name, schema_of_json(lit(xml_schema_sample))))
        df_opt_func = flatten(df_opt_func)
    
    return df_opt_func
    
    
def _merge_microbatch_to_trusted_layer(micro_batch: DataFrame, batch_id: int, trusted_table_config: dict) -> None:
    """process data at each microbatch"""
    database = trusted_table_config["database"]
    table_name = trusted_table_config['table_name_trusted']
    ingestion_system = trusted_table_config['ingestion_system']
    
    p_keys = trusted_table_config["primary_keys"]
    condition_merge = _get_condition_merge(p_keys)
    
    full_table_name_trusted = f"`{CATALOG_TRUSTED}`.`{database}`.`{table_name}`"
    trusted_delta_table = DeltaTable.forName(spark, full_table_name_trusted)
    
#     micro_batch_flattened = _flatten_dataframe(micro_batch, ingestion_system)
#     micro_batch_flattened = micro_batch_flattened.withColumn("headers_operation_key", when(col("headers_operation") == "DELETE", 1)
#                                                                                         .when(col("headers_operation") == "UPDATE", 2)
#                                                                                         .when(col("headers_operation") == "INSERT", 3)
#                                                                                         .when(col("headers_operation") == "REFRESH", 4))
    micro_batch_flattened = micro_batch.withColumn("headers_operation", lit('INSERT')).withColumn("headers_operation_key", lit(3))
    micro_batch_deduplicated = micro_batch_flattened.sort("headers_operation_key").dropDuplicates(p_keys)
    micro_batch_opt_functions = _optional_functions_apply(micro_batch_deduplicated, trusted_table_config)
    
    micro_batch_table_name = f"micro_batch_deduplicated_{table_name}"
    micro_batch_opt_functions.createOrReplaceTempView(micro_batch_table_name)
    
    is_first_load = True if len(trusted_delta_table.toDF().columns) == 1 else False
    if(is_first_load):
        micro_batch_opt_functions.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name_trusted)
    
    # Utilizando _jdf.sparkSession() referência: https://medium.com/analytics-vidhya/spark-session-and-the-singleton-misconception-1aa0eb06535a
    # Com a nova versão do runtime 11.0 + existe o dataFrame.SparkSession, porém não funcionou corretamente.
    micro_batch_opt_functions._jdf.sparkSession().sql(f"""MERGE INTO {full_table_name_trusted} as actual
                 USING {micro_batch_table_name} as updates
                 ON {condition_merge}
                 WHEN MATCHED AND updates.headers_operation = "DELETE" THEN DELETE
                 WHEN MATCHED AND updates.headers_operation <> "DELETE" THEN UPDATE SET *
                 WHEN NOT MATCHED AND updates.headers_operation <> "DELETE" THEN INSERT *""")
    

def _delta_streaming_ingestion_base(table_name: str) -> DataFrame:
    """"Read from Delta Table Streaming Base"""
    table_delta_raw = spark.readStream.format("delta").table(table_name).writeStream
    
    return table_delta_raw


def ingestion_environment_preparation(trusted_table_config: dict) -> None:
    """Check and Execute Full Load if it specified and also set tables metadata regardless load mode."""
    database = trusted_table_config['database']
    table_name_raw = trusted_table_config['table_name_raw']
    table_name_trusted = trusted_table_config['table_name_trusted']
    full_table_name_trusted = f"`{CATALOG_TRUSTED}`.`{database}`.`{trusted_table_config['table_name_trusted']}`"
    checkpoint_path = f"/AutoLoader/delta/{ENV}/{database}/{table_name_raw}_to_{table_name_trusted}"
    
    is_full_load = True if dbutils.widgets.get("load_mode") == "full_repair_load" else False
    tables_exists = spark.catalog.tableExists(full_table_name_trusted)
    if is_full_load and tables_exists:
#        spark.sql(f"DELETE FROM {full_table_name_trusted}")
#        spark.sql(f"VACUUM {full_table_name_trusted} RETAIN 0 HOURS")
        spark.sql(f"DROP TABLE {full_table_name_trusted}")
        dbutils.fs.rm(f"{checkpoint_path}/", True)
    _set_table_metadata(trusted_table_config)
#     if is_full_load and tables_exists:
#         spark.sql(f"FSCK REPAIR TABLE {full_table_name_trusted}")
#         spark.sql(f"REFRESH TABLE {full_table_name_trusted}")

def send_raw_data_to_trusted_layer(trusted_table_config: dict, streaming: bool, pool_num: int) -> None:
    """"Get data from a Raw delta Table and write it to a Databricks Managed Table using table metadata to write in the TBLPROPERTIES based on streaming or triggered job"""
    database = trusted_table_config['database']
    full_table_name_raw = f"`vr_tests`.`uc`.`{trusted_table_config['table_name_raw']}`"
    table_name_raw = trusted_table_config['table_name_raw']
    table_name_trusted = trusted_table_config['table_name_trusted']
    checkpoint_path = f"/AutoLoader/delta/{ENV}/{database}/{table_name_raw}_to_{table_name_trusted}"
    pool_id = f"raw_ingestion_{pool_num}_{table_name_trusted}"
    
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_id)
    
    delta_streaming = _delta_streaming_ingestion_base(full_table_name_raw)
    
    if not streaming:
        delta_streaming.trigger(Trigger.AvailableNow)
    spark.sql(f"REFRESH TABLE {full_table_name_raw}")
    
    (delta_streaming.queryName(pool_id).foreachBatch(lambda df, epoch_id: _merge_microbatch_to_trusted_layer(df, epoch_id, trusted_table_config))
                                       .option("nullValue", "null")
                                       .option("checkpointLocation", checkpoint_path)
                                       .outputMode("update")
                                       .start())
    

def _set_table_metadata(trusted_table_config: dict) -> None:
    """Configure Table Metadata on Unity Catalog"""
    #table_owner_group = f"{DOMAIN.lower()}-{REQUESTER_PROJECT_NAME.lower()}"
    table_owner_group = f"victor.rodrigues@databricks.com"
    table_name = f"`{CATALOG_TRUSTED}`.`{trusted_table_config['database']}`.`{trusted_table_config['table_name_trusted']}`"
    table_description = trusted_table_config['description']
    table_metadata = f"""
                     'source_system' = '{trusted_table_config['source_system']}',
                     'data_owner' = '{trusted_table_config['data_owner']}',
                     'quality' = '{trusted_table_config['quality']}',
                     'primary_keys' = '{','.join(trusted_table_config['primary_keys'])}',
                     'refresh_rate' = '{trusted_table_config['refresh_rate']}',
                     'si_classification' = '{trusted_table_config['si_classification']}',
                     'ingestion_system' = '{trusted_table_config['ingestion_system']}',
                     'critical_job_level' = '{trusted_table_config['critical_job_level']}',
                     'data_retention_policy_days' = '{trusted_table_config['data_retention_policy_days']}',
                     'data_retention_policy_timestamp_column' = '{trusted_table_config['data_retention_policy_timestamp_column']}',
                     'product_owner_requester' = '{trusted_table_config['product_owner_requester']}',
                     'workflow_job_name' = '{trusted_table_config['workflow_job_name']}',
                     'notebook_path' = '{trusted_table_config['notebook_path']}',
                     'project' = '{trusted_table_config['project']}',
                     'cost_center' = '{trusted_table_config['cost_center']}'                 
                     """
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name}(surrogate_key long) TBLPROPERTIES({table_metadata}); ")
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES({table_metadata}); ")
    spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_description}';")  
    spark.sql(f"ALTER TABLE {table_name} OWNER TO `{table_owner_group}`;")
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    
# TODO incluir na lib global .whl
# class ClusterJobs:
#     def __init__(self, kafka_topics_config: dict, streaming: bool = True):
#         topics = 
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Captura dos dados Raw e criação dinâmica das tabelas
# MAGIC No bloco abaixo será criado as tabelas a partir dos tópicos Kafka conforme configuração anterior. 
# MAGIC
# MAGIC O Kafka por padrão retorna os campos: `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`
# MAGIC
# MAGIC O campo `value` retorna no formato `Avro` com o ID em cada mensagem do Schema-Registry, por isso a necessidade da deserialização e leitura com a configuração do schema-registry no trecho: `.withColumn('parsed_value', from_avro("value", from_avro_abris_settings))`. Caso a `key` venha serializada com `Avro` é necessário passar pelo mesmo processo. Por padrão as ingestões via QLIK não estão serializando a `key`

# COMMAND ----------

# Check Full Load and Clean Data/Checkpoint if full_load_repair
# for table_config in TRUSTED_TABLES_CONFIG:
#     print(f'Dropping table {table_config["table_name_trusted"]}...')
#     ingestion_environment_preparation(table_config)

# Start Streamings (Using separate Spark Fair Pool)
for idx, table_config in enumerate(TRUSTED_TABLES_CONFIG):
    print(f'Streaming to table {table_config["table_name_trusted"]}...')
    send_raw_data_to_trusted_layer(table_config, STREAMING, idx)

# COMMAND ----------

# MAGIC %md # Tests

# COMMAND ----------

# MAGIC %md ## Setup environment
# MAGIC - r5.2xlarge (32 GB)
# MAGIC - single-node
# MAGIC - S3 policies

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists vr_tests;
# MAGIC create database if not exists vr_tests.uc;
# MAGIC -- create or replace table
# MAGIC create table if not exists 
# MAGIC vr_tests.uc.delete_source as select * from vr_fraud_dev.visits_gold limit 10000;

# COMMAND ----------

# MAGIC %md ## Reset environment

# COMMAND ----------

for i in range(1,21):
    tbl = f'vr_tests.uc.delete_{i}'
    print(f'Dropping table {tbl}...')
    spark.sql(f'drop table if exists {tbl}')
    dbutils.fs.rm(f'/tmp/{tbl}', recurse=True)
print('Done!')

# COMMAND ----------

# MAGIC %md ## Validate environment

# COMMAND ----------

display(spark.sql('show tables from vr_tests.uc').where("tableName like '%delete%'"))

# COMMAND ----------

[f for f in dbutils.fs.ls('/tmp/') if f.name.startswith('vr_tests.uc')]

# COMMAND ----------

# MAGIC %md # Other tests 

# COMMAND ----------

# MAGIC %md ## Stream to nonexistent table with `foreachBatch` / `saveAsTable`

# COMMAND ----------

from pyspark.sql import DataFrame
def _merge_microbatch_to_trusted_layer(micro_batch: DataFrame, batch_id: int, trusted_table_config: dict) -> None:
    full_table_name_trusted = trusted_table_config['full_table_name_trusted']
    micro_batch.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name_trusted)

# COMMAND ----------

def send_raw_data_to_trusted_layer(trusted_table_config: dict, streaming: bool, pool_num: int) -> None:
    table_name_trusted = trusted_table_config['full_table_name_trusted']
    pool_id = f"raw_ingestion_{pool_num}_{table_name_trusted}"
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_id)
    checkpoint_path = f'/tmp/{table_name_trusted}'
    (spark.readStream.table('vr_tests.uc.delete_source')
         .writeStream
         .foreachBatch(lambda df, epoch_id: _merge_microbatch_to_trusted_layer(df, epoch_id, trusted_table_config))
         .option("nullValue", "null")
         .option("checkpointLocation", checkpoint_path)
         .outputMode("update")
         .start())

# COMMAND ----------

for i in range(1,21):
    tbl = f'vr_tests.uc.delete_{i}'
    print(f'Dropping table {tbl}...')
    spark.sql(f'drop table if exists {tbl}')
    dbutils.fs.rm(f'/tmp/{tbl}', recurse=True)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tbl} (surrogate_key long); ")
    #spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES({table_metadata}); ")
    #spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_description}';")  
    #spark.sql(f"ALTER TABLE {table_name} OWNER TO `{table_owner_group}`;")
    spark.sql(f"ALTER TABLE {tbl} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    
for i in range(1,21):
    tbl = f'vr_tests.uc.delete_{i}'
    print(f'Starting streaming for {tbl}...')
    trusted_table_config = {"full_table_name_trusted":tbl}
    send_raw_data_to_trusted_layer(trusted_table_config, True, i)

# COMMAND ----------

# MAGIC %md ## Try / Exception

# COMMAND ----------

# Create functions to merge turbine and weather data into their target Delta tables
def _merge_microbatch_to_trusted_layer(micro_batch: DataFrame, batch_id: int, trusted_table_config: dict) -> None:
    
    full_table_name_trusted = trusted_table_config['full_table_name_trusted']
    
    try:
        # If the target table exists, write to it
        micro_batch.write.format("delta").mode("append").saveAsTable(full_table_name_trusted)
    except:
        # If the target table does not exist, create one
        micro_batch.write.format("delta").mode("append").saveAsTable(full_table_name_trusted)
