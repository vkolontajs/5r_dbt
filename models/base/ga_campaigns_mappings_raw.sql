with mappings as (

select * from {{ source('inputs_sheets', 'ga_mappings') }}

)

select
    campaign_name,
    campaign_type
from
    mappings