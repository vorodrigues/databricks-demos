-- SIMPLE SELECT
select * from vr_iiot_aws.dev.turbine_raw;

-- TIMETRAVEL
select * from vr_iiot_aws.dev.turbine_raw version as of 678;

-- SIMPLE FILTER
select * from vr_iiot_aws.dev.turbine_raw where deviceId = 'WindTurbine-293';

-- COMPLEX FILTER
select * from vr_iiot_aws.dev.turbine_raw where deviceId = 'WindTurbine-293' and date(timestamp) = '2022-04-13';

-- AGGREGATION
select deviceId, avg(rpm) from vr_iiot_aws.dev.turbine_raw group by deviceId;

-- JOIN WITH LOCAL CATALOG
select a.date, a.`timestamp`, b.deviceId, a.temperature, b.rpm
from vr_iiot_azure.dev.weather_raw a 
left join vr_iiot_aws.dev.turbine_raw b
on a.`timestamp` = b.`timestamp`;

-- JOIN WITH ANOTHER SHARE
select a.date, a.`timestamp`, b.deviceId, a.temperature, b.rpm
from vr_iiot_aws.dev.weather_raw a 
left join vr_iiot_aws.dev.turbine_raw b
on a.`timestamp` = b.`timestamp`;

-- CDC
select * from table_changes('vr_iiot_aws.dev.weather_raw_cdc', '2023-06-23 12:27:00');