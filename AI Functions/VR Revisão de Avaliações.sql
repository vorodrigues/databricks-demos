-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Aumentando a satisfação do cliente com análise automática de avaliações
-- MAGIC
-- MAGIC Nesta demonstração, construiremos um pipeline de dados que pega avaliações de clientes, na forma de texto livre, e as enriquece com informações extraídas ao fazer perguntas em linguagem natural aos modelos de IA Generativa disponíveis no Databricks. Também forneceremos recomendações para as próximas melhores ações à nossa equipe de atendimento ao cliente - ou seja, se um cliente requer acompanhamento, e uma mensagem de exemplo para seguir com o acompanhamento.
-- MAGIC
-- MAGIC Para cada avaliação, nós:
-- MAGIC
-- MAGIC - Determinamos o sentimento e se uma resposta é necessária ao cliente
-- MAGIC - Geramos uma resposta mencionando produtos alternativos que podem satisfazer o cliente
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review.png" width="1200">

-- COMMAND ----------

-- MAGIC %md ## 0/ Conjunto de dados
-- MAGIC
-- MAGIC Nesta demonstração, vamos utilizar avaliações de produto geradas usando IA Generativa.
-- MAGIC
-- MAGIC Esse conjunto consiste basicamente de duas tabelas:
-- MAGIC - **Avaliações**: dados não-estruturados com o conteúdo das avaliações
-- MAGIC - **Clientes**: dados estruturados como o cadastro e consumo dos clientes
-- MAGIC
-- MAGIC Agora, vamos visualizar estes dados

-- COMMAND ----------

-- MAGIC %md ### Avaliações

-- COMMAND ----------

USE vr_demo.aifunc;
SELECT * FROM avaliacoes

-- COMMAND ----------

-- MAGIC %md ### Clientes

-- COMMAND ----------

SELECT * FROM clientes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1/ Extraindo informações relevantes das avaliações
-- MAGIC
-- MAGIC Nosso objetivo é permitir a análise rápida de grandes volumes de avaliações de forma rápida e eficiente. Para isso, precisamos extrair as seguintes informações:
-- MAGIC - Produtos mencionados
-- MAGIC - Sentimento do cliente
-- MAGIC - Caso seja negativo, qual o motivo da insatisfação
-- MAGIC
-- MAGIC Vamos ver como podemos aplicar IA Generativa para acelerar nosso trabalho.

-- COMMAND ----------

-- MAGIC %md-sandbox ### > Foundation Models
-- MAGIC
-- MAGIC <img src="https://docs.databricks.com/en/_images/serving-endpoints-list.png" style="float: right; padding-left: 10px" width=600>
-- MAGIC
-- MAGIC Primeiro, precisamos de um modelo capaz de interpretar o texto das avaliações e extrair as informações desejadas. Para isso, vamos utilizar **[Foundation Models](https://docs.databricks.com/en/machine-learning/foundation-models/index.html#pay-per-token-foundation-model-apis)**, que são grandes modelos de linguagem (LLMs) servidos pela Databricks e que podem ser consultados sob-demanda sem a necessidade de implantação ou gerenciamento desses recursos.
-- MAGIC
-- MAGIC Alguns modelos disponíveis são:
-- MAGIC
-- MAGIC - Llama 3.1 405B Instruct
-- MAGIC - Llama 3.1 70B Instruct
-- MAGIC - Llama 2 70B Chat
-- MAGIC - DBRX Instruct
-- MAGIC - Mixtral-8x7B Instruct
-- MAGIC - GTE Large
-- MAGIC - BGE Large

-- COMMAND ----------

-- MAGIC %md-sandbox ### > AI Playground
-- MAGIC
-- MAGIC <img src="https://docs.databricks.com/en/_images/ai-playground.gif" style="float: right; padding-left: 10px" width=600>
-- MAGIC
-- MAGIC Para decidir qual o melhor modelo e instrução para o nosso caso de uso, podemos utilizar o **[Databricks AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html)**.
-- MAGIC
-- MAGIC Assim, podemos testar rapidamente diversas combinações de modelos e instruções através de uma interface intuitiva e escolher a melhor opção par utilizarmos no nosso projeto.
-- MAGIC
-- MAGIC Vamos testar a seguinte instrução:
-- MAGIC
-- MAGIC *Traduza para Português o texto a seguir (não adicione nenhum texto além da tradução):*
-- MAGIC
-- MAGIC *CONCLUSION: The administration of tissue plasminogen activator was responsible for the large extent of hemorrhage and should be considered in the differential diagnosis of hemorrhagic choroidal detachment.*

-- COMMAND ----------

-- MAGIC %md ### > AI Functions
-- MAGIC
-- MAGIC Por fim, para que possamos escalar a utilização dos modelos de IA Generativa, podemos utilizar as **[Databricks AI Functions](https://docs.databricks.com/en/large-language-models/ai-functions.html)**.
-- MAGIC
-- MAGIC Estas permitem a utilização de SQL, uma linguagem amplamente utiliza por analistas de dados e de negócio, para executar uma LLM sobre nossos bancos de dados corporativos. Com isso, também podemos criar novas tabelas com as informações extraídas para serem utilizadas em nossas análises mais facilmente.
-- MAGIC
-- MAGIC Existem funções nativas para executar tarefas pré-definidas ou enviar qualquer instrução desejada para ser executada. Seguem as descrições abaixo:
-- MAGIC
-- MAGIC | Gen AI SQL Function | Descrição |
-- MAGIC | -- | -- |
-- MAGIC | [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análise de Sentimento |
-- MAGIC | [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Classifica o texto de acordo com as categorias definidas |
-- MAGIC | [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extrai as entidades desejadas |
-- MAGIC | [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrige a gramática do texto fornecido |
-- MAGIC | [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Gera um novo texto conforme a instrução | 
-- MAGIC | [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Marcara dados sensíveis |
-- MAGIC | [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Envia instruções para o modelo desejado |
-- MAGIC | [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Calcula a similaridade entre duas expressões |
-- MAGIC | [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Sumariza o texto fornecido |
-- MAGIC | [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traduz o texto fornecido |

-- COMMAND ----------

-- MAGIC %md ### Testando a extração
-- MAGIC
-- MAGIC Para extrair as informações desejadas, vamos utilizar a função **`AI_QUERY()`**. Aqui só precisamos instruir ao modelo com qual informações nós gostaríamos que ele extraísse.
-- MAGIC
-- MAGIC Vamos ver como isso funciona!

-- COMMAND ----------

SELECT AI_QUERY('databricks-meta-llama-3-1-70b-instruct',
  'Um cliente fez uma avaliação. Nós respondemos todos que aparentem descontentes.
  Extraia as seguintes informações:
  - classifique o sentimento como ["POSITIVO","NEGATIVO","NEUTRO"]
  - retorne se o sentimento é NEGATIVO e precisa de responsta: S ou N
  - se o sentimento é NEGATIVO, explique quais os principais motivos
  Retorne somente um JSON. Nenhum outro texto fora o JSON. Formato do JSON:
  {
    "produto_nome": <entidade nome>,
    "sentimento": <entidade sentimento>,
    "resposta": <S ou N para resposta>,
    "motivo": <motivos de insatisfação>
  }
  Avaliação: Comprei o tablet GHI e estou muito insatisfeito com a qualidade da bateria. Ela dura muito pouco tempo e demora muito para carregar. Além disso, o desempenho é lento e trava com frequência. Não recomendo!'
) AS resultado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2/ Simplificando o acesso à IA
-- MAGIC
-- MAGIC Ter que especificar as instruções várias vezes acaba sendo trabalhoso, especialmente para Analistas de Dados que deveriam focar em analisar os resultados dessa extração.
-- MAGIC
-- MAGIC Para simplificar o acesso à essa inteligência, criaremos uma função SQL para encapsular esse processo e poder apenas informar em qual coluna do nosso conjunto de dados gostaríamos de aplicá-la.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review-wrapper.png" width="1200px">

-- COMMAND ----------

CREATE OR REPLACE FUNCTION REVISAR_AVALIACAO(avaliacao STRING)
RETURNS STRUCT<produto_nome: STRING, sentimento: STRING, resposta: STRING, resposta_motivo: STRING>
RETURN FROM_JSON(
  AI_QUERY(
    'databricks-meta-llama-3-1-70b-instruct',
    CONCAT(
      'Um cliente fez uma avaliação. Nós respondemos todos que aparentem descontentes.
      Extraia as seguintes informações:
      - classifique o sentimento como ["POSITIVO","NEGATIVO","NEUTRO"]
      - retorne se o sentimento é NEGATIVO e precisa de responsta: S ou N
      - se o sentimento é NEGATIVO, explique quais os principais motivos
      Retorne somente um JSON. Nenhum outro texto fora o JSON. Formato do JSON:
      {
        "produto_nome": <entidade nome>,
        "sentimento": <entidade sentimento>,
        "resposta": <S ou N para resposta>,
        "motivo": <motivos de insatisfação>
      }
      Avaliação: ', avaliacao
    )
  ),
  "STRUCT<produto_nome: STRING, sentimento: STRING, resposta: STRING, motivo: STRING>"
)

-- COMMAND ----------

-- MAGIC %md Agora, vamos testar nossa função

-- COMMAND ----------

SELECT revisar_avaliacao('Comprei o tablet GHI e estou muito insatisfeito com a qualidade da bateria. Ela dura muito pouco tempo e demora muito para carregar. Além disso, o desempenho é lento e trava com frequência. Não recomendo!') AS resultado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3/ Automação da análise e classificação das avaliações
-- MAGIC
-- MAGIC Agora, todos os nossos usuários podem aproveitar nossa função que foi cuidadosamente preparada para analisar nossas avaliações de produtos.
-- MAGIC
-- MAGIC E podemos escalar esse processo facilmente aplicando essa função sobre todo o nosso conjunto de dados!
-- MAGIC
-- MAGIC <br>
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-flow.png" width="1000">

-- COMMAND ----------

-- CREATE TABLE avaliacoes_revisadas AS
SELECT *, resultado.* FROM (
  SELECT *, revisar_avaliacao(avaliacao) as resultado FROM avaliacoes LIMIT 10)

-- COMMAND ----------

-- MAGIC %md ## 4/ Gerando uma sugestão de resposta
-- MAGIC
-- MAGIC Com todas as informações extraídas, também podemos aproveitá-las para gerar sugestões de respostas personalizadas para acelerar o trabalho dos nossos times de comunicação.
-- MAGIC
-- MAGIC Aqui usaremos a função **GERAR_RESPOSTA**, que já deixamos preparada com a definição abaixo:
-- MAGIC <br><br>
-- MAGIC ```
-- MAGIC AI_QUERY('databricks-meta-llama-3-1-70b-instruct',
-- MAGIC   CONCAT(
-- MAGIC     "Nosso cliente, ", nome, " ", sobrenome, " que comprou ", num_pedidos, " produtos este ano estava insatisfeito com o produto ", produto, 
-- MAGIC     ", pois ", motivo, ". Forneça uma mensagem empática para o cliente incluindo a oferta de uma ligação com o gerente de produto relevante.",
-- MAGIC     "Eu quero recuperar sua confiança e evitar que ele deixe de ser nosso cliente."
-- MAGIC   )
-- MAGIC )
-- MAGIC ```

-- COMMAND ----------

SELECT *, gerar_resposta(nome, sobrenome, num_pedidos, produto_nome, resposta_motivo) AS rascunho
FROM (SELECT * FROM avaliacoes_revisadas WHERE resposta='S' LIMIT 10)

-- COMMAND ----------

-- MAGIC %md ## 5/ Analisando os dados enriquecidos
-- MAGIC
-- MAGIC Por fim, também podemos usar os dados enriquecidos para construir análises ad-hoc, explorar e obter novos insights!
-- MAGIC
-- MAGIC Para democratizar ainda mais o acesso a esses dados por qualquer perfil de usuário, mesmo que não conheça nenhuma linguagem de programação, vamos usar a **Genie** para fazer perguntas para os nossos dados
-- MAGIC <br><br>
-- MAGIC <img src="https://www.databricks.com/en-website-assets/static/55039e16e0da85dc99f87f24a9cb8e24/intro-genie-web1-1-1718093249.gif" style="float: right; padding-left: 10px">
-- MAGIC
-- MAGIC
