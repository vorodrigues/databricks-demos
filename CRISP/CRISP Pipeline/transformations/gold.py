import dlt

# Enriquece as transações com os dados estruturados de produto e loja
@dlt.table(
  cluster_by=["product_id","store_id","date_key"],
  comment="A **CRISP** se conecta aos dados de [mais de 40 varejistas e distribuidores dos EUA](https://www.gocrisp.com/catalog/tag/inbound-connectors) e provê informações de *supply chain*, vendas, dentre outras. Nesta tabela, temos os dados harmonizados de vendas no varejo por produto, vendedor e data.",
  schema="""
    sales_id LONG COMMENT 'Chave primária. Identificador da venda gerado pela CRISP',
    product_id LONG COMMENT 'Identificador do produto gerado pela CRISP',
    store_id LONG COMMENT 'Identificador da loja gerado pela CRISP',
    date_key DATE COMMENT 'Data da venda',
    supplier STRING COMMENT 'Fornecedor do produto',
    product STRING COMMENT 'Nome do produto',
    upc STRING COMMENT 'Código UPC/GTIN-13. 13 dígitos. Sem dígito verificador',
    retailer STRING COMMENT 'Nome do varejista',
    store STRING COMMENT 'Nome da loja',
    store_type STRING COMMENT 'Tipo de loja definido pelo varejista',
    store_zip STRING COMMENT 'Código postal da loja',
    store_lat_long STRING COMMENT 'Latitude e longitude da loja (separados por vírgula)',
    sales_quantity LONG COMMENT 'Quantidade de unidades vendidas',
    sales_amount DOUBLE COMMENT 'Valor total da venda'
  """
)
def sales_gold():
  transactions = dlt.read("sales_silver").drop("_rescued_data")
  product = dlt.read("product")
  store = dlt.read("store")
  return transactions.join(product, on='product_id', how='left').join(store, on='store_id', how='left')