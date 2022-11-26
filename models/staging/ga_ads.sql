
with ga_ads_source as (

select
    cast(campaign as string) as campaign_name,
    cast(date as date) as campaign_date,
    sum(impressions) as impressions,
    sum(adclicks) as clicks,
    sum(adcost) as cost,
    'google' as campaign_source
from
    {{ ref('ga_ads_raw') }}
group by
    1, 2

),

ga_ads as (

select
    campaign_name,
    campaign_date,
    campaign_source,
    campaign_type,
    product,
    impressions,
    clicks,
    cost
from
    ga_ads_source
left join
    {{ ref('campaigns') }}
using
    (campaign_name)

)

select
    *
from
    ga_ads