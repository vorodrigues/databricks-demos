# Ingere dados do storage de forma incremental
bronzeDF = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "json") \
                .option("cloudFiles.schemaLocation", path+"/schemas") \
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
                .option("cloudFiles.inferColumnTypes", True) \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .load(path+"/raw/atm_visits")

# Escreve dados do stream para tabela Delta
bronzeDF.writeStream.format("delta") \
        .option("checkpointLocation", path+"/checkpoints/bronze") \
        .trigger(processingTime="10 seconds") \
        .toTable("visits_bronze")
