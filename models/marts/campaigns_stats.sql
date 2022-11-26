
with campaign_stats as (

select
    *
from
    {{ ref('fb_ads') }}

union all

select
    *
from
    {{ ref('ga_ads') }}

)

select
    *
from
    campaign_stats
order by
    campaign_date desc, campaign_name desc