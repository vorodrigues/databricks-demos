# Databricks notebook source
# MAGIC %%file /databricks/driver/cvm-hist-spyder.py
# MAGIC 
# MAGIC # USAGE:
# MAGIC #
# MAGIC # scrapy runspider cvm-spyder.py
# MAGIC 
# MAGIC 
# MAGIC import os
# MAGIC import scrapy
# MAGIC import zipfile
# MAGIC 
# MAGIC START_URL = 'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/'
# MAGIC   
# MAGIC class cvmInfsSpider(scrapy.Spider):
# MAGIC     
# MAGIC     name = 'cvmInfsHist'
# MAGIC     allowed_domains = ['cvm.gov.br']
# MAGIC     start_urls = [START_URL]
# MAGIC     
# MAGIC     def parse(self, response):
# MAGIC         rows = response.xpath('//div[@class="wrapper"]/pre/a')
# MAGIC         limit = int(self.limit)
# MAGIC         if limit > 0:
# MAGIC             rows = rows[limit*-1:]
# MAGIC         for row in rows:
# MAGIC             name = row.xpath('text()').extract_first()
# MAGIC             url = START_URL + name
# MAGIC             if name.startswith('inf_diario'):
# MAGIC                 print(url)
# MAGIC                 yield scrapy.Request(url, self.download_zip)
# MAGIC     
# MAGIC     def download_zip(self, response):
# MAGIC         name = response.url.split('/')[-1]
# MAGIC         with open(os.path.join(self.path_zip, name), 'wb') as f:
# MAGIC             f.write(response.body)
# MAGIC         with zipfile.ZipFile(os.path.join(self.path_zip,name), 'r') as zip_ref:
# MAGIC             zip_ref.extractall(self.path_csv)
