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

columns = ["id", "vertex_property"]
data = [
  (1, 40),
  (2, 1000),
  (3, 1000),
  (4, 50),
  (5, 1000)
]
vertices = spark.createDataFrame(data, columns)
display(vertices)

# COMMAND ----------

columns = ["src", "dst", "edge_property"]
data = [
  (1, 2, 30),
  (2, 3, 20),
  (2, 4, 50),
  (4, 5, 60)
]
edges = spark.createDataFrame(data, columns)
display(edges)

# COMMAND ----------

from graphframes import GraphFrame

vertices.cache()
edges.cache()

graph = GraphFrame(vertices, edges)

# COMMAND ----------

# MAGIC %md ## Run the algorithm

# COMMAND ----------

from graphframes.lib import Pregel
from pyspark.sql.functions import when, min, greatest, lit, coalesce, col

def Algo(maxIter):
  return graph.pregel \
    .setMaxIter(maxIter) \
    .withVertexColumn('result', col('vertex_property'), coalesce(Pregel.msg(), col('result'))) \
    .sendMsgToDst(
      when(
        (Pregel.src('result') < Pregel.dst('result')) & (Pregel.edge('edge_property') != Pregel.dst('result')),
        greatest(Pregel.src('result'), Pregel.edge('edge_property'))
      )
    ) \
    .aggMsgs(min(Pregel.msg())) \
    .run()

# COMMAND ----------

# MAGIC %md ### Known number of iterations
# MAGIC 
# MAGIC If we knew the required number of iterations previously, the following code would provide the right results (duplicated in the below image).<br><br>
# MAGIC <img src="files/shared_uploads/victor.rodrigues@databricks.com/3_final.png" width=900>

# COMMAND ----------

result = Algo(2)
result.display()

# COMMAND ----------

# MAGIC %md ### Larger number of iterations
# MAGIC 
# MAGIC If we set a larger number of iterations, we get the following error:

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) 

# COMMAND ----------

result = Algo(5)

# COMMAND ----------

result.display()

# COMMAND ----------

result = algo(15)

# COMMAND ----------

result.display()

# COMMAND ----------

# MAGIC %md ### AggregateMessages API
# MAGIC 
# MAGIC Inspired by [Belief Propagation example](https://graphframes.github.io/graphframes/docs/_site/api/python/_modules/graphframes/examples/belief_propagation.html)

# COMMAND ----------

from pyspark.sql import functions as F
from graphframes.lib import AggregateMessages as AM

class Algo(object):

  def runWithGraphFrames(self, g):
    g = GraphFrame(g.vertices.withColumn('result', F.col('vertex_property')), g.edges)

    while True:
      msgForDst = F.when(
        (AM.src['result'] < AM.dst['result']) 
        & (AM.edge['edge_property'] != AM.dst['result']),
        F.greatest(AM.src['result'], AM.edge['edge_property']))
      
      m = g.aggregateMessages(
        F.min(AM.msg).alias("aggMsgs"), 
        sendToDst=msgForDst)
      
      # exit when no more messages
      if m.filter(m.aggMsgs.isNotNull()).isEmpty():
        break
        
      # receive messages and update result
      v = (
        g.vertices
        .join(m, 'id', how='left_outer')
        .withColumn('result', F.coalesce(m.aggMsgs, F.col('result')))
        .drop('aggMsgs'))
      
      # cache new vertices using workaround for SPARK-1334
      g = GraphFrame(AM.getCachedDataFrame(v), g.edges)

    # return final vertices with result
    return g.vertices

# COMMAND ----------

results = Algo().runWithGraphFrames(graph)
results.display()
