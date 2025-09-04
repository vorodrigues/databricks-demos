# Sobreescreve os dados da tabela tbl com o dataframe df
df.write.mode("overwrite") \
        .saveAsTable("cat.sch.tbl")
