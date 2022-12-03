with raw as (

select
    campaign_date,
    table_name,
    amount
from
    {{ref('manual_inputs') }}
where
    table_name in (select table_name from {{ ref ('bi_mappings_raw') }} where table_group = 'Financials')

union all

select
    campaign_date,
    'total_cost' as table_name,
    sum(cost) as added_to_cart
from
    {{ ref('campaigns_data') }}
group by
    1, 2

union all

select
    campaign_date,
    'ga_sales' as table_name,
    sum(transactions_revenue) as ga_sales
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

