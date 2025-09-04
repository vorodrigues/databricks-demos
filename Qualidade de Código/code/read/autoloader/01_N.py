# Ingere dados do storage
bronzeDF = spark.readStream.format("json") \
                .load(path+"/raw/atm_visits")

# Escreve dados do stream para tabela Delta
bronzeDF.writeStream.format("delta") \
        .option("checkpointLocation", path+"/checkpoints/bronze") \
        .trigger(processingTime="10 seconds") \
        .toTable("visits_bronze")
