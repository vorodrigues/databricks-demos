-- Databricks notebook source
create streaming live table dlt_uc as select * from stream(vr_fraud_dev.visits_gold)
