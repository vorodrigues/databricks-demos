-- SETUP
create catalog vr_iiot_azure;
create database vr_iiot_azure.dev;
create table vr_iiot_azure.dev.weather_raw as select * from vr_iiot_aws.dev.weather_raw;