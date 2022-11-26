with dirty_campaings as (

select
    cast(campaign_name as string) as campaign_name
from
    {{ ref('fb_ads_raw') }}

union all

select
    cast(campaign as string) as campaign_name
from
    {{ ref('ga_campaigns_raw') }}

)

select * from dirty_campaings