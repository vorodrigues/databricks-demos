-- UPDATE
update vr_iiot.dev.weather_raw_cdc set temperature = 200 where timestamp = '2022-01-01 00:00:00.000';

-- DELETE
delete from vr_iiot.dev.weather_raw_cdc where timestamp = '2022-01-01 00:00:15.000';

-- INSERT
insert into vr_iiot.dev.weather_raw_cdc values (
  '2024-01-01',
  '2024-01-01 00:00:00.000',
  'WeatherCapture',
  18.56,
  38.22,
  7.81,
  'W'
);