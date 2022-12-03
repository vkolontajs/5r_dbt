with mappings as (

select * from {{ source('inputs_sheets', 'bi_mappings') }}

)

select
    *
from
    mappings