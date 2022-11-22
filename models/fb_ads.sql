{{ config(schema='base') }}

with ga_events as (

select * from {{ source('bq_init', 'fb_ads') }}

)

select
    *
from
    ga_events