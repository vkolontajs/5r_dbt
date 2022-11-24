
with ga_events_raw as (

select * from {{ source('bq_init', 'fb_ads') }}

)

select
    *
from
    ga_events_raw