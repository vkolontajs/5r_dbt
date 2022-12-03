with raw as (

select
    `order` as table_order_index,
    `group` as table_group,
    sub_group as table_sub_group,
    key as table_name
from
    {{ ref('bi_mappings_raw') }}

)

select
    *
from
    raw
where
    table_group != 'Cost'