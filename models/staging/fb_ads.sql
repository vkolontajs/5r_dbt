
with fb_ads_source as (

select
    cast(campaign_name as string) as campaign_name,
    cast(date as date) as campaign_date,
    cast(impressions as int) as impressions,
    cast(clicks as int) as clicks,
    cast(spend as float64) as cost,
    'facebook' as campaign_source
from
    {{ ref('fb_ads_raw') }}

),

fb_ads as (

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
    fb_ads_source
left join
    {{ ref('campaigns') }}
using
    (campaign_name)

)

select
    *
from
    fb_ads