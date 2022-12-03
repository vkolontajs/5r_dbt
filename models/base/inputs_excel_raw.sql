with inputs as (

select * from {{ source('inputs_sheets', 'manual_inputs') }}

)

select
    *
from
    inputs