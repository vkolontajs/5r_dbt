select
    campaign_date,
    campaign_type as table_sub_group,
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