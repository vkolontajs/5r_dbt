with raw as (

select
    *
from
    {{ ref('bi_mappings_raw') }}

)

select
    table_order_index,
    table_group,
    table_sub_group
from
    raw