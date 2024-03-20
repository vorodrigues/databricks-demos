# Databricks notebook source
# MAGIC %md # Graph Use Case
# MAGIC 
# MAGIC <img src="files/shared_uploads/victor.rodrigues@databricks.com/0_initial.png" width=900><br><br>
# MAGIC 
# MAGIC Given the above directed acyclic graph, we need to apply the following rules:<br><br>
# MAGIC 
# MAGIC 1. A vertex X will only send a message to another vertex Y if it's property is smaller than Y's property
# MAGIC 2. A vertex X will only send a message to another vertex Y if Y's property is different than the edge's property
# MAGIC 3. The message value is the greatest of the X's and edge's properties
# MAGIC <br><br>
# MAGIC 
# MAGIC ## First Superstep
# MAGIC <img src="files/shared_uploads/victor.rodrigues@databricks.com/1_first_step.png" width=900><br><br>
# MAGIC 
# MAGIC * **Messages 1 and 2** are the only ones sent, because they match **rules 1 and 2**.
# MAGIC * According to **rule 3**, for **message 1** the vertex property is sent, while for **message 2** the edge property is sent.<br><br>
# MAGIC 
# MAGIC ## Second Superstep
# MAGIC <img src="files/shared_uploads/victor.rodrigues@databricks.com/2_second_step.png" width=900><br><br>
# MAGIC 
# MAGIC * **Message 1** is the only one sent, because it matches **rules 1 and 2**. As per **rule 3**, the vertex property is sent.
# MAGIC * **Message 2** is not sent, because it doesn't match **rule 2**.<br><br>
# MAGIC 
# MAGIC ## Third Superstep
# MAGIC <img src="files/shared_uploads/victor.rodrigues@databricks.com/3_final.png" width=900><br><br>
# MAGIC 
# MAGIC * There are no messages to be sent and the algorithm should stop.

# COMMAND ----------

# MAGIC %md ## Build graph

# COMMAND ----------

columns = ["id", "positivado"]
data = [
  (1, 40),
  (2, 1000),
  (3, 1000),
  (4, 50),
  (5, 1000)
]
vertices = spark.createDataFrame(data, columns)
vertices.cache()
display(vertices)

# COMMAND ----------

columns = ["src", "dst", "data"]
data = [
  (1, 2, 30),
  (2, 3, 20),
  (2, 4, 50),
  (4, 5, 60)
]
edges = spark.createDataFrame(data, columns)
edges.cache()
display(edges)

# COMMAND ----------

from graphframes import GraphFrame
graph = GraphFrame(vertices, edges)

# COMMAND ----------

# MAGIC %md ## Define the algorithm

# COMMAND ----------

from pyspark.sql.functions import col, lit, when
from graphframes import *
# sc.setCheckpointDir("dbfs:/FileStore/Didier/Grafos")
# spark.conf.set("spark.databricks.io.cache.enabled", "true")
import pyspark.sql.types as t
from graphframes.lib import AggregateMessages as AM
from pyspark.sql.functions import min as sqlmin
import pyspark.sql.functions as F
from datetime import datetime
import math
from pyspark.sql import functions as F
from graphframes.lib import AggregateMessages as AM         
from pyspark import StorageLevel

# COMMAND ----------

class Algo(object):
 
  def runWithGraphFrames(self, g, edges, v_total):
    
    frequencia_checkpoint = 12
    i = 0
    diff_timestamp = None
    comeco_timestamp = datetime.utcnow()
    while True:
      diff_timestamp = datetime.utcnow()
      print("SUPERSTEP ", (i+1))
      print(datetime.now().strftime("%Y-%m-%d %H:%M"))    
      print("*************************************")
      triplets = g.triplets
      #triplets.cache() # <-- REMOVED
      
      # define o valor da mensagem
      # - retorna o maior valor entre as propriedades do vértice e do relacionamento quando as regras 1 e 2 forem atendidas 
      # - retorna nulo quando as regras 1 e 2 não forem atendidas
                  
      msgToDst = F.when(
        (AM.src['positivado'] < AM.dst['positivado']) 
        & (AM.edge['data'] != AM.dst['positivado']), # <--- CHANGED
        F.greatest(AM.src['positivado'], AM.edge['data'])) # <--- CHANGED
      
      #print("Passando mensagem: ")     
      msgsToDst = triplets.select(msgToDst.alias("aggMsgs"), "dst.id")
      #msgsToDst.cache() <-- REMOVED
      sentMsgsToDst = msgsToDst.filter("aggMsgs is not NULL")
      #sentMsgsToDst.cache() <-- REMOVED
      
      #print("Agrupando: ")      
      m = sentMsgsToDst.groupBy("id").agg(F.min("aggMsgs").alias("aggMsgs"))
      m.cache()
      
      #if m.isEmpty(): # <-- ADDED
      #  break # <-- ADDED
      
      #if i ==0:
      #  print("COMEÇO - quantidade de nós que receberam mensagem: ")
      print('Mensagens: %i' % m.count()) # <--- ACTION / CHANGED
      
      
      #Atualiza propriedade "positivado" de vertices
      v_total = (v_total
        .join(F.broadcast(m).alias("m"), 'id', how='left_outer')
        .withColumn('positivado', F.coalesce(F.col("m.aggMsgs"), F.col('positivado')))
        .drop('aggMsgs'))
      v_total.cache()  # <--- ADDED
      
      #print(v_total.count())
      
      
      #Filtra só os edges de nós que receberam mensagens e que podem passar mensagens
      e = (edges.alias("e").join(F.broadcast(m).alias("m"), F.col("e.src")==F.col("m.id"), "inner").select("e.src", "e.dst","e.data")) # <--- CHANGED
      e = e.cache() # <--- MOVED
      #Filtra todos os nós envolvidos
      ids = e.alias("e").select("e.src").distinct().union(e.alias("e2").select("e2.dst").distinct()).withColumnRenamed("src","id")
      #Filtra vertices 
      v = (v_total.alias("a").join(F.broadcast(ids), 'id', how='inner').select("a.id", "a.positivado")) # <--- CHANGED
      v.cache() # <--- MOVED
      
      
      
      if i % frequencia_checkpoint == 0 and i > 0:    
              
          print("Checando se deve parar")          
          
          #print(m.count()) # <--- ACTION / REMOVED
          #if m.isEmpty():
          #   break

          print("Checkpoint")          
          
          #v_total = v_total.checkpoint()
          
          print("EXPLICANDO: ")
          #print(v_total.explain())
          out = spark.sparkContext.getCheckpointDir() + "/{}".format(i)
          
          
          print("Escrita ", out)
          v_total.write.mode("overwrite").parquet(out) # <--- ACTION
          print("Leitura")
          v_total = sqlContext.read.parquet(out) # <--- ACTION
          
          print("DELETANDO PATH ", spark.sparkContext.getCheckpointDir() + "/{}".format(i-frequencia_checkpoint))
          path = spark.sparkContext.getCheckpointDir() + "/{}".format(i-frequencia_checkpoint)
          dbutils.fs.rm(path, True)    
          
          
          print("Chamando GC")
          spark.sparkContext._jvm.System.gc()
          
          #spark.catalog.clearCache()
          
          
      #print("Unpersist")
      #m.unpersist()
      #g.vertices.unpersist()
      #g.edges.unpersist()
      # cache dos novos vértices usando o workaround para SPARK-1334
      
      print("Criando Dataframe")
      g = GraphFrame(AM.getCachedDataFrame(v), AM.getCachedDataFrame(e)) # <--- CHANGED
      #m.unpersist()
      
      print("Tempo de Superstep: " + str(datetime.utcnow() - diff_timestamp) + "s")
      print("Tempo desde o começo: " + str(datetime.utcnow() - comeco_timestamp) + "s")
      print("*************************************")
      #print(spark.sparkContext._jsc.getPersistentRDDs().items())
      print("")
      print("")
      print("")
      
      i = i + 1
      
      
      
    return v_total

# COMMAND ----------

# MAGIC %md ## Run the algorithm

# COMMAND ----------

results = Algo().runWithGraphFrames(graph, edges, vertices)
results.display()

# COMMAND ----------

# MAGIC %md ## Step-by-Step

# COMMAND ----------

g = graph
v_total = vertices

# COMMAND ----------

# Inicializa o grafo
v_total = v_total.withColumn('data', F.col('positivado'))
v_total.cache()
g = GraphFrame(v_total, g.edges)

# Controle do loop
frequencia_checkpoint = 12
i = 0
diff_timestamp = None
comeco_timestamp = datetime.utcnow()

# COMMAND ----------

# MAGIC %md ### Loop

# COMMAND ----------

triplets = g.triplets
triplets.cache()

# COMMAND ----------

# define o valor da mensagem
# - retorna o maior valor entre as propriedades do vértice e do relacionamento quando as regras 1 e 2 forem atendidas 
# - retorna nulo quando as regras 1 e 2 não forem atendidas
msgToDst = F.when(
  (AM.src['positivado'] < AM.dst['positivado']) 
  & (AM.edge['data'] != AM.dst['positivado']),
  F.greatest(AM.src['positivado'], AM.edge['data']))

# COMMAND ----------

#print("Passando mensagem: ")     
msgsToDst = triplets.select(msgToDst.alias("aggMsgs"), "dst.id")
msgsToDst.cache()
sentMsgsToDst = msgsToDst.filter("aggMsgs is not NULL")
sentMsgsToDst.cache()

# COMMAND ----------

#print("Agrupando: ")      
m = sentMsgsToDst.groupBy("id").agg(F.min("aggMsgs").alias("aggMsgs"))
m.cache()

# COMMAND ----------

#Atualiza propriedade "positivado" de vertices
v_total = (v_total
  .join(F.broadcast(m).alias("m"), 'id', how='left_outer')
  .withColumn('positivado', F.coalesce(F.col("m.aggMsgs"), F.col('positivado')))
  .drop('aggMsgs'))
v_total.cache()

# COMMAND ----------

#Filtra só os edges de nós que receberam mensagens e que podem passar mensagens
e = (edges.alias("e").join(F.broadcast(m).alias("m"), F.col("e.src")==F.col("m.id"), "inner").select("e.src", "e.dst", "e.data"))
e.cache()
#Filtra todos os nós envolvidos
ids = e.alias("e").select("e.src").distinct().union(e.alias("e2").select("e2.dst").distinct()).withColumnRenamed("src","id")
ids.cache()
#Filtra vertices 
v = (v_total.alias("a").join(F.broadcast(ids), 'id', how='inner').select("a.id", "a.positivado"))
v.cache()

# COMMAND ----------

# Atualizando o grafo
g = GraphFrame(AM.getCachedDataFrame(v), e)

# Controle do loop
i = i + 1

# COMMAND ----------

print(v.count())

# COMMAND ----------


