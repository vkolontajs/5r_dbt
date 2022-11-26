
with ga_ads_raw as (

select * from {{ source('bq_init', 'google_ads') }}

)

select
    *
from
    ga_ads_raw