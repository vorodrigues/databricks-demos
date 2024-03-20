-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS ${db}

-- COMMAND ----------

USE ${db}

-- COMMAND ----------

CREATE OR REPLACE TABLE site_d(
  site_id INTEGER PRIMARY KEY NOT ENFORCED DEFERRABLE,
  longitude DOUBLE,
  latitude DOUBLE
);

CREATE TABLE IF NOT EXISTS asset_hierarchy(
  asset_id INTEGER PRIMARY KEY,
  level1 STRING, 
  level2 STRING, 
  level3 STRING
);

CREATE TABLE IF NOT EXISTS device_d(
  device_id INTEGER PRIMARY KEY,
  site_id INTEGER,
  asset_id INTEGER,
  CONSTRAINT site_device_fk FOREIGN KEY(site_id) REFERENCES site_d,
  CONSTRAINT asset_hierarchy_fk FOREIGN KEY(asset_id) REFERENCES asset_hierarchy

);

CREATE TABLE IF NOT EXISTS device_f(
  reading_id INTEGER PRIMARY KEY,
  device_id INTEGER, 
  CONSTRAINT device_f_d_fk FOREIGN KEY(device_id) REFERENCES device_d
);



-- COMMAND ----------

insert into site_d values
  (1, 10, 20),
  (1, 100, 200),
  (null, 10, 20)
