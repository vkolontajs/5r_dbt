
with ga_events_raw as (

select * from {{ source('bq_init', 'ga_campaigns') }}

)

select
    *
from
    ga_events_raw