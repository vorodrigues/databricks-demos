# Databricks notebook source
# MAGIC %md # Validação de Contratos
# MAGIC
# MAGIC Nesta demonstração, construiremos um processo para validação de contratos, na forma de texto livre, ao fazer perguntas em linguagem natural aos modelos de IA Generativa disponíveis no Databricks.
# MAGIC
# MAGIC Nosso objetivo é:
# MAGIC - Classificar esses contratos quanto ao seu tipo
# MAGIC - Extrair informações como contratante, contratada e objeto
# MAGIC - Analisar o cumprimento de uma série de regras 
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review.png" width="1200">

# COMMAND ----------

# MAGIC %md ## Conjunto de dados
# MAGIC
# MAGIC Nesta demonstração, vamos utilizar contratos reais publicamente disponíveis na internet.
# MAGIC
# MAGIC Esse conjunto consiste basicamente de uma tabela:
# MAGIC - **Contratos**: dados não-estruturados com o conteúdo dos contratos
# MAGIC
# MAGIC Agora, vamos visualizar estes dados

# COMMAND ----------

# MAGIC %sql
# MAGIC USE vr_demo.auditoria;
# MAGIC SELECT * EXCEPT (content) FROM contratos

# COMMAND ----------

# MAGIC %md ## Classificação de contratos e extração de informações
# MAGIC
# MAGIC Contratos de diferentes tipos podem ter diferentes regras. Além disso, também precisamos identificar uma série de informações no conteúdo desses contratos.
# MAGIC
# MAGIC Vamos ver como podemos aplicar modelos de IA Generativa para auxiliar nessas tarefas!

# COMMAND ----------

# MAGIC %md-sandbox ### > Foundation Models
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/serving-endpoints-list.png" style="float: right; padding-left: 10px" width=600>
# MAGIC
# MAGIC Primeiro, precisamos de um modelo capaz de interpretar o texto das avaliações e extrair as informações desejadas. Para isso, vamos utilizar **[Foundation Models](https://docs.databricks.com/en/machine-learning/foundation-models/index.html#pay-per-token-foundation-model-apis)**, que são grandes modelos de linguagem (LLMs) servidos pela Databricks e que podem ser consultados sob-demanda sem a necessidade de implantação ou gerenciamento desses recursos.
# MAGIC
# MAGIC Alguns modelos disponíveis são:
# MAGIC
# MAGIC - Llama 3.1 405B Instruct
# MAGIC - Llama 3.1 70B Instruct
# MAGIC - Llama 2 70B Chat
# MAGIC - DBRX Instruct
# MAGIC - Mixtral-8x7B Instruct
# MAGIC - GTE Large
# MAGIC - BGE Large

# COMMAND ----------

# MAGIC %md-sandbox ### > AI Playground
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/ai-playground.gif" style="float: right; padding-left: 10px" width=600>
# MAGIC
# MAGIC Para decidir qual o melhor modelo e instrução para o nosso caso de uso, podemos utilizar o **[Databricks AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html)**.
# MAGIC
# MAGIC Assim, podemos testar rapidamente diversas combinações de modelos e instruções através de uma interface intuitiva e escolher a melhor opção par utilizarmos no nosso projeto.
# MAGIC
# MAGIC Vamos testar a seguinte instrução:
# MAGIC
# MAGIC *Traduza para Português o texto a seguir (não adicione nenhum texto além da tradução):*
# MAGIC
# MAGIC *CONCLUSION: The administration of tissue plasminogen activator was responsible for the large extent of hemorrhage and should be considered in the differential diagnosis of hemorrhagic choroidal detachment.*

# COMMAND ----------

# MAGIC %md ### > AI Functions
# MAGIC
# MAGIC Por fim, para que possamos escalar a utilização dos modelos de IA Generativa, podemos utilizar as **[Databricks AI Functions](https://docs.databricks.com/en/large-language-models/ai-functions.html)**.
# MAGIC
# MAGIC Estas permitem a utilização de SQL, uma linguagem amplamente utiliza por analistas de dados e de negócio, para executar uma LLM sobre nossos bancos de dados corporativos. Com isso, também podemos criar novas tabelas com as informações extraídas para serem utilizadas em nossas análises mais facilmente.
# MAGIC
# MAGIC Existem funções nativas para executar tarefas pré-definidas ou enviar qualquer instrução desejada para ser executada. Seguem as descrições abaixo:
# MAGIC
# MAGIC | Gen AI SQL Function | Descrição |
# MAGIC | -- | -- |
# MAGIC | [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análise de Sentimento |
# MAGIC | [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Classifica o texto de acordo com as categorias definidas |
# MAGIC | [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extrai as entidades desejadas |
# MAGIC | [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrige a gramática do texto fornecido |
# MAGIC | [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Gera um novo texto conforme a instrução | 
# MAGIC | [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Marcara dados sensíveis |
# MAGIC | [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Envia instruções para o modelo desejado |
# MAGIC | [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Calcula a similaridade entre duas expressões |
# MAGIC | [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Sumariza o texto fornecido |
# MAGIC | [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traduz o texto fornecido |

# COMMAND ----------

# MAGIC %md ### Exemplo de contrato

# COMMAND ----------

# MAGIC %sql DECLARE OR REPLACE contrato = 'Contrato CVM n˚ 018/2020
# MAGIC  
# MAGIC TERMO DE CONTRATO DE AQUISIÇÃO DE
# MAGIC INFRAESTRUTURA DE ACESSO DE REDE
# MAGIC CABEADA (LAN), INCLUINDO EQUIPAMENTOS E
# MAGIC SERVIÇO DE IMPLEMENTAÇÃO DA SOLUÇÃO,
# MAGIC QUE ENTRE SI FAZEM 
# MAGIC A CVM - COMISSÃO DE
# MAGIC VALORES MOBILIÁRIOS 
# MAGIC E 
# MAGIC ZOOM
# MAGIC TECNOLOGIA LTDA.
# MAGIC  
# MAGIC A 
# MAGIC Comissão de Valores Mobiliários - CVM
# MAGIC , Autarquia Federal vinculada ao
# MAGIC Ministério da Economia, com sede na Rua Sete de Setembro, 111 - 28º andar -
# MAGIC Centro - Rio de Janeiro/RJ (CEP: 20.050-901), inscrita no CNPJ sob o nº
# MAGIC 29.507.878/0001-08, neste ato representada, com base na delegação de
# MAGIC competência conferida pela Portaria/CVM/PTE/nº 108, de 01 de novembro de
# MAGIC 2011, pela Superintendente Administrativo-Financeira em exercício, Sra. Cintia de
# MAGIC Miranda Moura, inscrita no CPF nº 018.962.037-44, portadora da Carteira de
# MAGIC Identidade nº 09785440-0, doravante denominada CONTRATANTE, e 
# MAGIC ZOOM
# MAGIC TECNOLOGIA LTDA
# MAGIC  inscrita no CNPJ/MF sob o nº 06.105.781/0001-65, sediada na
# MAGIC Avenida das Águias, nº 162, Pedra Branca, Palhoça/SC, CEP: 88.137-280,
# MAGIC doravante designada CONTRATADA, neste ato representada pelo Sr. Guilherme
# MAGIC Nunes Silva, portador da Carteira de Identidade nº 5300535, expedida pela SSP/SC,
# MAGIC e CPF nº 053.852.669-65, tendo em vista o que consta no Processo nº
# MAGIC 19957.006466/2020-08 e em observância às disposições da Lei nº 8.666, de 21 de
# MAGIC junho de 1993, da Lei nº 10.520, de 17 de julho de 2002, do Decreto nº 9.507, de
# MAGIC 21 de setembro de 2018, do Decreto nº 7.174, de 12 de maio de 2010, da
# MAGIC Instrução Normativa SGD/ME nº 1, de 4 de Abril de 2019 e da Instrução Normativa
# MAGIC SEGES/MP nº 5, de 26 de maio de 2017, resolvem celebrar o presente Termo de
# MAGIC Contrato, decorrente do Pregão nº 16/2020, mediante as cláusulas e condições a
# MAGIC seguir enunciadas.'

# COMMAND ----------

# MAGIC %md ### Classificação de contratos

# COMMAND ----------

# MAGIC %sql SELECT ai_classify(contrato, ARRAY('alimentos', 'materiais de escritório', 'software', 'infraestrutura de rede','outros')) as classe

# COMMAND ----------

# MAGIC %md ### Extração de informações

# COMMAND ----------

# MAGIC %sql SELECT ai_extract(contrato, ARRAY('contratante', 'contratada', 'objeto')) as infos

# COMMAND ----------

# MAGIC %md ## Validando regras de compliance

# COMMAND ----------

# MAGIC %md ### System Prompt

# COMMAND ----------

# MAGIC %sql DECLARE OR REPLACE system_prompt = 'Você é um validador de contratos que recebe um texto de contrato e responde apenas com um JSON com regras validadas. Você não conversa, nem fala contexto, apenas retorna o JSON. Com base no texto do seguinte contrato, valide o conteúdo de acordo com as seguintes regras: 
# MAGIC '

# COMMAND ----------

# MAGIC %md ### Instrução

# COMMAND ----------

# MAGIC %sql DECLARE OR REPLACE instruction = '
# MAGIC A sua resposta deve seguir EXATAMENTE o formato do JSON, assim como no exemplo.
# MAGIC
# MAGIC Retorne APENAS JSON. Nenhum outro texto fora do JSON. Formato JSON:
# MAGIC [{
# MAGIC   "regra_id":<número da regra>,
# MAGIC   "regra_desc":"<texto descrição da regra>",
# MAGIC   "resultado":"<se o contrato cumpre ou não a regra, com True|False>",
# MAGIC   "justificativa":"<motivo para que o contrato não passe na regra>",
# MAGIC   "trecho":"<trecho do contrato que contém o conteúdo utilizado para validação>",
# MAGIC }]
# MAGIC
# MAGIC Exemplo: 
# MAGIC [{
# MAGIC   "regra_id":1,
# MAGIC   "regra_desc":"O valor do contrato não deve ultrapassar 1 milhão de reais",
# MAGIC   "resultado":"False",
# MAGIC   "justificativa":"O valor desse contrato é R$ 1.313.567,52, acima de 1 milhão",
# MAGIC   "trecho":"4.1 O valor deste contrato está estimado em R$ 1.313.567,52 (um milhão, trezentos e treze mil, quinhentos e sessenta e sete reais e cinquenta e dois centavos), pelos primeiros 12 (doze) meses.",
# MAGIC }]
# MAGIC
# MAGIC NADA que não seja o JSON deve ser respondido. Apenas o texto no formato descrito. 
# MAGIC Se não encontrar o valor escreva "Não identificado", mas NUNCA invente nada que não esteja no contrato. Apenas informações de lá.
# MAGIC
# MAGIC CONTRATO: 
# MAGIC '

# COMMAND ----------

# MAGIC %md ### Regras

# COMMAND ----------

# MAGIC %sql DECLARE OR REPLACE rules = '
# MAGIC Regra 1: O valor do contrato não deve ultrapassar 1 milhão de reais;
# MAGIC Regra 2: A duração do contrato não deve ser superior a 12 meses;
# MAGIC Regra 3: O pagamento não pode ser realizado por boleto bancário;
# MAGIC Regra 4: O contrato não poderá conter renovação automática;
# MAGIC '

# COMMAND ----------

# MAGIC %md ### Executando a validação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT path, resultado.* FROM (
# MAGIC   SELECT *, 
# MAGIC     explode(from_json(
# MAGIC       ai_query(
# MAGIC         'databricks-meta-llama-3-1-70b-instruct',
# MAGIC         CONCAT(system_prompt, rules, instruction, parsed_content)),
# MAGIC       'ARRAY<STRUCT<regra_id INT, regra_desc STRING, resultado STRING, justificativa STRING, trecho STRING>>')) as resultado
# MAGIC   FROM contratos
# MAGIC   WHERE path ILIKE '%1_CT_10_2023%'
# MAGIC )

# COMMAND ----------

# MAGIC %md ## Leitura dos PDFs
# MAGIC
# MAGIC - Carga
# MAGIC - Leitura

# COMMAND ----------

# MAGIC %pip install pypdf

# COMMAND ----------

import io
import warnings
from pypdf import PdfReader
from pyspark.sql.functions import pandas_udf
from typing import Iterator
import pandas as pd

def parse_bytes_pypdf(raw_doc_contents_bytes: bytes):
    try:
        pdf = io.BytesIO(raw_doc_contents_bytes)
        reader = PdfReader(pdf)
        parsed_content = [page_content.extract_text() for page_content in reader.pages]
        return "\n".join(parsed_content)
    except Exception as e:
        warnings.warn(f"Exception {e} has been thrown during parsing")
        return None
    
@pandas_udf("string")
def parse_pdf(content: pd.Series) -> pd.Series:
    return content.apply(parse_bytes_pypdf)

df = (spark.read.format('binaryFile')
    .load('/Volumes/lucas_catalog/llm/contracts')
    .withColumn("parsed_content", parse_pdf("content"))
)

display(df)

# COMMAND ----------

df.write.saveAsTable("vr_demo.auditoria.contratos")
