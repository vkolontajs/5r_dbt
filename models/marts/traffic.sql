with raw as (

select
    campaign_date,
    table_name,
    amount
from
    {{ref('manual_inputs') }}
where
    table_name in (select table_name from {{ ref ('bi_mappings_raw') }} where table_group = 'Traffic')

union all

select
    campaign_date,
    'added_to_cart' as table_name,
    sum(added_to_cart) as added_to_cart
from
    {{ ref('campaigns_data') }}
group by
    1, 2

union all

select
    campaign_date,
    'online_traffic' as table_name,
    sum(sessions) as added_to_cart
from
    {{ ref('campaigns_data') }}
group by
    1, 2

)

select
    campaign_date,
    table_sub_group,
    table_name,
    amount
from
    raw
left join
    {{ ref ('bi_mappings_raw') }}
using
    (table_name)

