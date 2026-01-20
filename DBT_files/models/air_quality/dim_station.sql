{{ config(
    materialized='incremental',
    unique_key='station_sk'
) }}

with base as (
    select city, station_name, lat, lon, event_time
    from {{ ref('air_quality_silver') }}
),

dedup as (
    select *,
           row_number() over (
               partition by city, station_name
               order by event_time
           ) as rn
    from base
)

select
    {{ dbt_utils.generate_surrogate_key(
        ['city', 'station_name', 'event_time']
    ) }} as station_sk,

    city,
    station_name,
    lat,
    lon,
    event_time as valid_from,
    lead(event_time) over (
        partition by city, station_name
        order by event_time
    ) as valid_to,

    case
        when lead(event_time) over (
            partition by city, station_name
            order by event_time
        ) is null then true
        else false
    end as is_current

from dedup
where rn = 1
