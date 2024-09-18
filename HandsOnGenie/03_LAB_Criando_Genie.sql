-- Databricks notebook source
-- MAGIC %md 
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/genie_ai_bi/main/images/header_genie.png">
-- MAGIC
-- MAGIC # Hands-On LAB 03 - Criando o AI/BI Genie
-- MAGIC
-- MAGIC Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de perguntas e respostas usando linguagem natural.
-- MAGIC
-- MAGIC
-- MAGIC ## Objetivos do Exercício
-- MAGIC
-- MAGIC O objetivo desse laboratório é usar o AI/BI Genie para permitir a análise de dados de vendas utilizando somente **Português**.

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.00 - Preparação
-- MAGIC
-- MAGIC 1. Conecte este notebook ao seu SQL Warehouse
-- MAGIC 2. Preencha o parâmetro no topo da página com o nome do database que será utilizado neste laborátio
-- MAGIC 3. Execute a célula abaixo para ativar este database
-- MAGIC
-- MAGIC ***Caso não tenha feito ainda, carregue os dados conforme descrito no [Lab 01 - Importando os dados](https://github.com/Databricks-BR/genie_ai_bi/blob/main/01_LAB_importando_dados/README.md)***.

-- COMMAND ----------

USE ${db}

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.01 - Criar AI/BI Genie
-- MAGIC
-- MAGIC Vamos começar criando uma Genie para fazer nossas perguntas. Para isso, vamos seguir os passos abaixo:
-- MAGIC
-- MAGIC 1. No menu principal (à esquerda), clique em `New` > `Genie space`
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/genie_ai_bi/main/images/genie_01.png">
-- MAGIC
-- MAGIC 2. Configure sua Genie
-- MAGIC     - Crie um nome para a sua Genie, por exemplo `<suas iniciais> Genie de Vendas`
-- MAGIC     - Selecione seu SQL Warehouse
-- MAGIC     - Selecione as seguintes tabelas:
-- MAGIC         - vendas
-- MAGIC         - estoque
-- MAGIC         - dim_medicamento
-- MAGIC         - dim_loja
-- MAGIC     - Clique em `Save`
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/genie_ai_bi/main/images/genie_02.png" width=800>

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.02 - Fazer perguntas ao AI/BI Genie
-- MAGIC
-- MAGIC - Qual o faturamento em out/22?
-- MAGIC - Agora, quebre por produto
-- MAGIC - Mantenha somente os 10 produtos com maior faturamento
-- MAGIC - Monte um gráfico de barras

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.03 - Fazer perguntas avançadas
-- MAGIC
-- MAGIC - Qual o total de produtos vendidos em genéricos?
-- MAGIC - Qual o valor total vendido de ansiolíticos?
-- MAGIC - Quais produtos tiveram uma proporção de vendas por estoque maior que 0.8 em Outubro de 2022?

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.04 - Usando comentários
-- MAGIC
-- MAGIC - Faça a pergunta:
-- MAGIC   - Qual o valor total de venda por loja? Exiba o nome da loja
-- MAGIC - Use a célula abaixo para adicionar um comentário na tabela `dim_loja`
-- MAGIC - Faça novamente a pergunta anterior

-- COMMAND ----------

ALTER TABLE dim_loja ALTER COLUMN nlj COMMENT 'Nome da loja'

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.05 - Usando chaves primárias
-- MAGIC
-- MAGIC - Use a célula abaixo para adicionar as chaves primárias e estrangeiras nas tabelas `dim_loja` e `vendas`
-- MAGIC - Faça novamente a pergunta anterior

-- COMMAND ----------

ALTER TABLE dim_loja ADD CONSTRAINT pk_dim_loja PRIMARY KEY (cod);
ALTER TABLE vendas ADD CONSTRAINT fk_venda_dim_loja FOREIGN KEY (id_loja) REFERENCES dim_loja(cod);

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.06 - Usando instruções
-- MAGIC
-- MAGIC - Faça a pergunta:
-- MAGIC   - Calcule a quantidade de itens vendidos para prescrição
-- MAGIC - Adicione a instrução:
-- MAGIC   - `* para calcular indicadores sobre prescrição use categoria_regulatoria <> 'GENÉRICO'`
-- MAGIC - Faça novamente a pergunta anterior

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.07 - Usando exemplos de queries
-- MAGIC
-- MAGIC - Faça a pergunta:
-- MAGIC   - Calcule a quantidade de itens vendidos por janela móvel de 3 meses 
-- MAGIC - Adicione o exemplo de query abaixo:
-- MAGIC   - `SELECT window.end AS dt_venda, SUM(vl_venda) FROM vendas GROUP BY WINDOW(dt_venda, '90 days', '1 day') `
-- MAGIC - Faça novamente a pergunta anterior

-- COMMAND ----------

-- MAGIC %md ## Exercício 03.08 - Usando funções
-- MAGIC
-- MAGIC - Faça a pergunta:
-- MAGIC   - Qual o lucro projetado do AAS?
-- MAGIC - Crie a função da célula abaixo
-- MAGIC - Adicione esta função a sua Genie
-- MAGIC - Faça novamente a pergunta anterior

-- COMMAND ----------

CREATE OR REPLACE FUNCTION calc_lucro(medicamento STRING)
  RETURNS TABLE(nome_medicamento STRING, lucro_projetado DOUBLE)
  COMMENT 'Use esta função para calcular o lucro projetado de um medicamento'
  RETURN 
    SELECT
      m.nome_medicamento,
      sum(case when m.categoria_regulatoria == 'GENÉRICO' then 1 else 0.5 end * v.vl_venda) / sum(v.qt_venda) as lucro_projetado
    FROM vendas v
    LEFT JOIN dim_medicamento m
    ON v.id_produto = m.id_produto
    WHERE m.nome_medicamento = calc_lucro.medicamento
    GROUP BY ALL
