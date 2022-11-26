with campaings_union as (

select
    cast(campaign_name as string) as campaign_name
from
    {{ ref('fb_ads_raw') }}

union all

select
    cast(campaign as string) as campaign_name
from
    {{ ref('ga_campaigns_raw') }}

union all

select
    cast(campaign as string) as campaign_name
from
    {{ ref('ga_ads_raw') }}

),

deduped_campaigns as (

select distinct
    campaign_name
from
    campaings_union

),

campaings as (

select
    campaign_name,
    case when campaign_type is null then 'uknown' else campaign_type end as campaign_type,
    case
        when campaign_type is not null and product is not null then product
        when campaign_type = 'error/missing' then campaign_type
        when campaign_type is null then 'uknown'
        else 'other'
    end as product
from
    deduped_campaigns
left join
    {{ ref('mapping_campaigns') }}
using
    (campaign_name)

)

select
    *
from
    campaings