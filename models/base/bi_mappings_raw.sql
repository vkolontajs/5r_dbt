with mappings as (

select 
    `order` as table_order_index,
    `group` as table_group,
    sub_group as table_sub_group,
    key as table_name 
from {{ source('inputs_sheets', 'bi_mappings') }}

)

select
    *
from
    mappings