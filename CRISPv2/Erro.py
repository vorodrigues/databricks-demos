# Databricks notebook source
perf = dbutils.jobs.taskValues.get(taskKey = "Previsao", key = "perf", debugValue = 0)
raise Exception(f'ERRO: A performance da previsão foi de {perf} e ficou abaixo do esperado. Por favor, revise os dados e tente novamente.')
