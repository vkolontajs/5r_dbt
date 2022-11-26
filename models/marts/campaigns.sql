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
    case when source is null then 'uknown' else source end as campaign_source,
    case when campaing_channel is null then 'uknown' else campaing_channel end as campaing_channel,
    case when product is null and campaing_channel is not null then 'other' when product is null and campaing_channel is null then 'uknown' else product end as campaing_product
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