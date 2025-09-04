# Sobreescreve os dados da tabela tbl com os dados do dataframe df
df.write.mode("overwrite") \
        .partitionBy("state") \
        .saveAsTable("cat.sch.tbl")
