# Databricks notebook source
# MAGIC %md
# MAGIC # Testes para documentação das tabelas em Portugues
# MAGIC
# MAGIC - Autor: Rodrigo Oliveira
# MAGIC - Criado em: 10.06.2024
# MAGIC - Update: 02.01.2025
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### AI Assistent (Databricks UI)
# MAGIC The 'loyalty_segments' table contains information about the different loyalty segments in our system. It includes details such as the segment description, the threshold for each segment, and the validity period. This data can be used to understand the different levels of customer engagement and tailor marketing strategies accordingly. For instance, you can identify high-value customers based on their loyalty segment and target them with personalized offers. Additionally, you can monitor the effectiveness of your marketing campaigns by tracking changes in customer behavior and loyalty segment membership over time.
# MAGIC
# MAGIC #### AI Assistent (Databricks UI) traduzindo com `argostranslate`
# MAGIC A tabela "loyalty_segments" contém informações sobre os diferentes segmentos de fidelidade em nosso sistema. Inclui detalhes como a descrição do segmento, o limiar para cada segmento e o período de validade. Esses dados podem ser usados para entender os diferentes níveis de engajamento do cliente e adaptar estratégias de marketing de acordo. Por exemplo, você pode identificar clientes de alto valor com base em seu segmento de fidelidade e segmentá-los com ofertas personalizadas. Além disso, você pode monitorar a eficácia de suas campanhas de marketing, rastreando mudanças no comportamento do cliente e na adesão ao segmento de fidelidade ao longo do t
# MAGIC
# MAGIC #### Usando `AI_GEN` traduzindo com `argostranslate`
# MAGIC Esta tabela categoriza os clientes em segmentos de fidelidade. Provavelmente inclui vários atributos e comportamentos do cliente, usados para determinar seu nível de fidelidade. Cada linha pode representar um cliente único, com colunas para diferentes atributos, como histórico de compras, métricas de engajamento ou informações demográficas. Os dados nesta tabela auxiliam na compreensão do comportamento do cliente, permitindo estratégias de marketing direcionadas e experiências personalizadas do cliente. A estrutura e o conteúdo da tabela são cruciais para a condução de programas de retenção e lealdade do cliente.
# MAGIC

# COMMAND ----------

!pip install argostranslate

# COMMAND ----------

import argostranslate.package
import argostranslate.translate

from_code = 'en'
to_code = 'pt'

catalog = 'rodrigo' # aqui_vai_o_nome_do_catalogo'
 
argostranslate.package.update_package_index()
available_packages = argostranslate.package.get_available_packages()
package_to_install = next(
    filter(
        lambda x: x.from_code == from_code and x.to_code == to_code, available_packages
    )
)
argostranslate.package.install_from_path(package_to_install.download())

# COMMAND ----------

def descriptionTables(catalog, schema, table):
    table_desc = spark.sql(f"""
    SELECT ai_gen("Human: {catalog}.{schema}.{table_name} is a single table in a data warehouse. 

    As an industry-leading expert Data Scientist, generate a one-paragraph summary of the provided table. This summary will be added next to the table as a description in the data explorer UI. Your description should be succinct and written in an objective and decisive tone. Start with one sentence summarizing what the table is, followed by a detailed description. Ensure the paragraph is less than 100 words. The summary should only contain English characters, commas, and periods, with each sentence being direct and straightforward. Do not include quotation marks. Avoid flowery language, descriptions of the schema itself, decorative tone, specific examples, parentheses, and single or double quotes in your answer. Do not show table name or schema name or catalog name.") as table_description;
    """).collect()[0][0]
    return table_desc


def descriptionColumns(catalog, schema, table_name):
    columns = spark.catalog.listColumns(f"{catalog}.{schema}.{table_name}")

    column_descriptions = {}

    for column in columns:
        # Gera a descrição para cada coluna
        column_desc = spark.sql(f"""
        SELECT ai_gen("Human: {column.name} is a column in the table {catalog}.{schema}.{table_name}. 

        As an industry-leading expert Data Scientist, generate a concise description for this column. Start with one sentence summarizing what the column represents, followed by more details about its significance or role in the table. Keep the description under 25 words. Ensure the paragraph is in English, with each sentence being clear and precise. Avoid mentioning the table name, column name, column type, or using parentheses, quotation marks, or examples.") as column_description;
        """).collect()[0][0]
        
        # Adiciona a descrição ao dicionário
        column_descriptions[column.name] = column_desc

    return column_descriptions

def descriptionTranslateBr(table_desc_english):
    table_desc_ptbr = argostranslate.translate.translate(table_desc_english, from_code, to_code)
    return table_desc_ptbr    

def descriptionUpdate(catalog, schema, table_name, new_description):
    new_description = new_description.replace("'", "")
    update_description = f"ALTER TABLE {catalog}.{schema}.{table_name} SET TBLPROPERTIES ('comment' = '{new_description}')"
    spark.sql(update_description)

def descriptionUpdateColumn(catalog, schema, table_name, column_name, column_desc_ptbr):
    query = f"""
    ALTER TABLE {catalog}.{schema}.{table_name} ALTER COLUMN {column_name} COMMENT '{column_desc_ptbr}'
    """
    spark.sql(query)

# COMMAND ----------

schemas = ['dbdemos_agent_tools']

for schema in schemas:
    print(f"Iniciando tradução das descrições das tabelas catálogo {catalog} do schema {schema}...")

    tables = spark.catalog.listTables(f"{catalog}.{schema}")
    
    table_names = [table.name for table in tables]

    for table_name in table_names:

        if spark.catalog.tableExists(f"{catalog}.{schema}.{table_name}"):
            print(f"{catalog}.{schema}.{table_name}")

            assistant_ai_desc = spark.sql(f"DESCRIBE DETAIL {catalog}.{schema}.{table_name}").collect()[0]["description"]

            ## Esse if é pra ver se há uma descrição ou não, se tiver descrição ele vai traduzir, se não ele usa a descrição que já tem
            if assistant_ai_desc is None or assistant_ai_desc == "":
                table_desc = descriptionTables(catalog, schema, table_name)
            else:
                table_desc = assistant_ai_desc

            table_desc_ptbr = descriptionTranslateBr(table_desc)
            descriptionUpdate(catalog, schema, table_name, table_desc_ptbr)

            # Gera e processa descrições para as colunas da tabela
            print(f"Gerando descrições para as colunas da tabela {table_name}...")
            column_descriptions = descriptionColumns(catalog, schema, table_name)

            for column_name, column_desc in column_descriptions.items():
                # Recupera o comentário atual da coluna
                column_comment_query = f"DESCRIBE TABLE {catalog}.{schema}.{table_name}"
                column_metadata = spark.sql(column_comment_query).filter(f"`col_name` = '{column_name}'").collect()

                current_comment = None
                if len(column_metadata) > 0:
                    current_comment = column_metadata[0]["comment"]

                # Verifica se a coluna já tem um comentário
                if current_comment is None or current_comment == "":
                    # Sem comentário: gera uma descrição e traduz
                    column_desc_ptbr = descriptionTranslateBr(column_desc)
                    descriptionUpdateColumn(catalog, schema, table_name, column_name, column_desc_ptbr)
                else:
                    # Com comentário: apenas traduz
                    column_desc_ptbr = descriptionTranslateBr(current_comment)
                    descriptionUpdateColumn(catalog, schema, table_name, column_name, column_desc_ptbr)

        else:
            print(f"Descrição não gerada para tabela: {catalog}.{schema}.{table_name}")
            print(100*'---')
