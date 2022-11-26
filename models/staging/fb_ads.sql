with fb_ads as (

select
    cast(campaign_name as string) as campaign_name,
    cast(date as date) as campaign_date,
    cast(impressions as int) as impressions,
    cast(clicks as int) as clicks,
    cast(spend as int) as costs
from
    {{ ref('fb_ads_raw') }}

)

select * from fb_ads