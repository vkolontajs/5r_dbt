with dates as (

select
    *
from
    unnest(generate_date_array('2022-10-01', current_date, interval 1 day)) as campaign_date
)

select
    *
from
    dates