with mappings as (

select * from {{ source('inputs_sheets', 'ga_mappings') }}

)

select
    campaign_name,
    campaign_type,
    {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} source_key
from
    mappings