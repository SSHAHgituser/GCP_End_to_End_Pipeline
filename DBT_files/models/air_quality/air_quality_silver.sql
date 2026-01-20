{{
    config(
        materialized = "incremental",
        unique_Key = "event_hash"
    )
}}

with source as(
    select 
        event_date, 
        event_time,
        city,
        station_name,
        lat,
        lon,
        cast(aqi as int64) as aqi,
        dominant_pollutant,
        pm25, pm10, co, no2, so2,
        temperature, humidity, wind,
        bq_load_time,

        -- business hash
        to_hex(md5(concat(
            city,
            station_name,
            cast(event_time as string)
        ))) as event_hash,

        row_number() over (
            partition by city, station_name, event_time 
            order by bq_load_time desc
        ) as rn
        from {{ source('air_quality', 'waqi_hyd_bronze') }}
)

select * except(rn) from source where rn=1

{% if is_incremental() %}
    and bq_load_time > (select max(bq_load_time) from {{this}})
{% endif %}