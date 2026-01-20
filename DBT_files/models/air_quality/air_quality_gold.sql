{{ config(materialized='table') }}

select
    date(s.event_time) as date,
    d.station_sk,

    avg(s.aqi) as avg_aqi,
    max(s.aqi) as max_aqi,

    avg(s.pm25) as avg_pm25,
    avg(s.pm10) as avg_pm10
from {{ ref('air_quality_silver') }} s
join {{ ref('dim_station') }} d
  on s.city = d.city
 and s.station_name = d.station_name
 and s.event_time between d.valid_from and coalesce(d.valid_to, timestamp('9999-12-31'))
group by 1,2