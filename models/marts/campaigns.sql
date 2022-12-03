select
    campaign_date,
    case when campaign_type is null then 'Unknown' else campaign_type end as table_sub_group,
    campaign_name,
    sessions,
    sessions_duration,
    clicks,
    cost,
    impressions,
    bounces
from
    {{ ref('campaigns_data') }}
left join
    {{ ref('ga_campaigns_mappings_raw') }}
using
    (campaign_name)