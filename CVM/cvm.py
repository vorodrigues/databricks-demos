# Databricks notebook source
import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT


def downloadInfs(path_zip, path_csv, path_dbfs, limit=0):
    command = f'scrapy runspider /databricks/driver/cvm-spyder.py -a limit={limit} -a path_zip={path_zip} -a path_csv={path_csv} -a path_dbfs={path_dbfs}'
    process = Popen(command, stdout=PIPE, shell=True, stderr=STDOUT, bufsize=1, close_fds=True)
    for line in iter(process.stdout.readline, b''):
        print(line.rstrip().decode('utf-8'))
    process.stdout.close()
    process.wait()
    print('Done!')


def comparacao(cad, infs, fundos):
    
    hist = cad.query('CNPJ_FUNDO in '+str(fundos))[['DENOM_SOCIAL']] \
              .join(infs[['DT_COMPTC','VL_QUOTA']])
    hist['DENOM_SOCIAL'] = hist['DENOM_SOCIAL'].str[0:40]
    
    # Retorno x Tempo
    ini = infs.groupby('CNPJ_FUNDO').head(1).rename(columns={'VL_QUOTA':'INI'})['INI']
    ret = hist.join(ini)
    ret['RET'] = (ret['VL_QUOTA'] / ret['INI'] - 1) * 100
    ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET') \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.show()
    
    # Clustermap de Correlação
    ret = hist
    ret['RET'] = hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(0) / hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(252)
    pvt = ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET')
    sns.clustermap(abs(pvt.corr(method='pearson')), 
                   annot=True, 
                   cmap=sns.diverging_palette(220, 20, as_cmap=True),
                   vmin=0,
                   vmax=1
                  )
    plt.show()


def comparacao2(cad, infs, fundos):
    
    hist = cad.query('CNPJ_FUNDO in '+str(fundos))[['DENOM_SOCIAL']] \
              .join(infs[['DT_COMPTC','VL_QUOTA']])
    hist['DENOM_SOCIAL'] = hist['DENOM_SOCIAL'].str[0:40]
    
    # Retorno x Tempo
    ini = infs.groupby('CNPJ_FUNDO').head(1).rename(columns={'VL_QUOTA':'INI'})['INI']
    ret = hist.join(ini)
    ret['RET'] = (ret['VL_QUOTA'] / ret['INI'] - 1) * 100
    ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET') \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.show()
    
    # Clustermap de Correlação
    ret = hist
    ret['RET'] = hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(0) / hist['VL_QUOTA'].groupby('CNPJ_FUNDO').shift(126)
    pvt = ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET')
    sns.clustermap(abs(pvt.corr(method='pearson')), 
                   annot=True, 
                   cmap=sns.diverging_palette(220, 20, as_cmap=True),
                   vmin=0,
                   vmax=1
                  )
    plt.show()


def acompanhamento(cad, infs, fundos, dtini, dtfim):

    fundos = pd.DataFrame(fundos, columns = ['CNPJ_FUNDO','COTAS'])
    fundos = fundos.set_index('CNPJ_FUNDO')

    hist = fundos \
              .join(cad[['DENOM_SOCIAL']]) \
              .join(infs[['DT_COMPTC','VL_QUOTA']]\
                    .loc[(str(dtini)[:10]<=infs['DT_COMPTC']) & (infs['DT_COMPTC']<=str(dtfim)[:10])])

    # Total x Tempo
    tot = hist
    tot['POS'] = tot['COTAS'] * tot['VL_QUOTA']
    tot.groupby('DT_COMPTC')['POS'].sum() \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.show()

    # Retorno x Tempo
    ini = hist.sort_values('DT_COMPTC').groupby('CNPJ_FUNDO').head(1).rename(columns={'VL_QUOTA':'INI'})['INI']
    ret = hist.join(ini)
    ret['RET'] = (ret['VL_QUOTA'] / ret['INI'] - 1) * 100
    ret.pivot(index='DT_COMPTC', columns='DENOM_SOCIAL', values='RET') \
       .plot(figsize=(14,7)) \
       .grid(axis='y')
    plt.legend(loc='upper left', bbox_to_anchor=(0, -0.1))
    plt.show()

    # Retorno por Fundo
    fin = pd.DataFrame(ret.groupby('CNPJ_FUNDO').tail(1))
    fin['POS'] = fin['COTAS'] * fin['VL_QUOTA']
    fin['VAR'] = fin['POS'] * (1 - 1 / (1 + fin['RET'] / 100))
    fin = fin.sort_values('RET', ascending=False)

    # Retorno Total
    res = fin[['POS','VAR']].sum()
    res['RET'] = res['VAR'] / (res['POS'] - res['VAR']) * 100
    res['DENOM_SOCIAL'] = 'Total'
    display(fin.append(pd.DataFrame([res]))[['DENOM_SOCIAL','POS','VAR','RET']])
