# Sobreescreve os dados da tabela tbl com os dados do dataframe df
df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("cat.sch.tbl")
