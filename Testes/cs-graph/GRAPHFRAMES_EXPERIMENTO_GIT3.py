# Databricks notebook source
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


edges = spark.sql(
"""
SELECT * FROM basesinteligentes.teste_graphframes_edge2
where data <= 1275897863
""").withColumnRenamed("BASECPFID_ATENDEU_REGRA", "dst").withColumnRenamed("BASECPFID_RELACIONADO", "src")


# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",64)

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
      triplets = g.triplets.cache()
      
      
      
      # define o valor da mensagem
      # - retorna o maior valor entre as propriedades do vértice e do relacionamento quando as regras 1 e 2 forem atendidas 
      # - retorna nulo quando as regras 1 e 2 não forem atendidas
                  
      msgToDst = F.when(
        (AM.src['positivado'] < AM.dst['positivado']) 
        & (AM.dst['data'] != AM.dst['positivado']),
        F.greatest(AM.src['positivado'], AM.dst['data']))
      
      #print("Passando mensagem: ")     
      msgsToDst = triplets.select(msgToDst.alias("aggMsgs"), "dst.id").cache()
      sentMsgsToDst = msgsToDst.filter("aggMsgs is not NULL").cache()
      
      #print("Agrupando: ")      
      m = sentMsgsToDst.groupBy("id").agg(F.min("aggMsgs").alias("aggMsgs"))
      m = m.cache()
      
      if i ==0:
        print("COMEÇO - quantidade de nós que receberam mensagem: ")
        print(m.count())    
      
      
      #Atualiza propriedade "positivado" de vertices
      v_total = (v_total
        .join(F.broadcast(m).alias("m"), 'id', how='left_outer')
        .withColumn('positivado', F.coalesce(F.col("m.aggMsgs"), F.col('positivado')))
        .drop('aggMsgs'))
      
      #print(v_total.count())
      
      
      #Filtra só os edges de nós que receberam mensagens e que podem passar mensagens
      e = (edges.alias("e").join(F.broadcast(m).alias("m"), F.col("e.src")==F.col("m.id"), "inner").select("e.src", "e.dst"))
      #Filtra todos os nós envolvidos
      ids = e.alias("e").select("e.src").distinct().union(e.alias("e2").select("e2.dst").distinct()).withColumnRenamed("src","id")
      #Filtra vertices 
      v = (v_total.alias("a").join(F.broadcast(ids), 'id', how='inner').select("a.id", "a.positivado", "a.data"))
      
      
      e = e.cache()
      
      
      if i % frequencia_checkpoint == 0 and i > 0:    
              
          print("Checando se deve parar")          
          
          print(m.count())          
          #if m.isEmpty():
          #   break

          print("Checkpoint")          
          
          #v_total = v_total.checkpoint()
          
          print("EXPLICANDO: ")
          #print(v_total.explain())
          out = spark.sparkContext.getCheckpointDir() + "/{}".format(i)
          
          
          print("Escrita ", out)
          v_total.write.mode("overwrite").parquet(out)
          print("Leitura")
          v_total = sqlContext.read.parquet(out)
          
          print("DELETANDO PATH ", spark.sparkContext.getCheckpointDir() + "/{}".format(i-frequencia_checkpoint))
          path = spark.sparkContext.getCheckpointDir() + "/{}".format(i-frequencia_checkpoint)
          dbutils.fs.rm(path, True)    
          
          
          print("Chamando GC")
          spark.sparkContext._jvm.System.gc()
          
          #spark.catalog.clearCache()
          
          v = v.cache()
      #print("Unpersist")
      #m.unpersist()
      #g.vertices.unpersist()
      #g.edges.unpersist()
      # cache dos novos vértices usando o workaround para SPARK-1334
      
      print("Criando Dataframe")
      g = GraphFrame(AM.getCachedDataFrame(v), e)
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

num_parts = 32


print(datetime.now().strftime("%Y-%m-%d %H:%M"))


vertices = spark.sql("""
 SELECT a.id, 
  case 
  when b.positivado < a.positivado then b.positivado 
  when a.positivado < b.positivado then a.positivado
  else coalesce(b.positivado, a.positivado) end as positivado,
  a.data
  FROM basesinteligentes.teste_graphframes_vertex as a 
  LEFT JOIN basesinteligentes.pregel_resultado as b on b.id = a.id
  where a.data <= {}
""".format(1275897863))

  
vertices2 = vertices.coalesce(num_parts).cache()
edges2 = edges.coalesce(num_parts).cache()
  
print("Cache")
#vertices = vertices.checkpoint()
#edges = edges.checkpoint()


graph = GraphFrame(vertices2, edges2)
print("Algoritmo iniciado")
results = Algo().runWithGraphFrames(graph, edges2, vertices2)
  

# COMMAND ----------


