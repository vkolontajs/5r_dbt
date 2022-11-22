{{ config(schema='base') }}

with ga_events as (

select * from {{ source('bq_init', 'ga_campaigns') }}

)

select
    *
from
    ga_events