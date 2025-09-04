# Ingere dados da tabela
bronzeDF = spark.readStream.table("cat.sch.tbl")

# Escreve dados do stream para tabela Delta
bronzeDF.writeStream.format("delta") \
        .option("checkpointLocation", path+"/checkpoints/bronze") \
        .trigger(processingTime="10 seconds") \
        .toTable("visits_bronze")
