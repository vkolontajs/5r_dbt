with inputs as (

select
    cast(date_from as date) as date_from_fix,
    cast(date_to as date) as date_to_fix,
    *
from
    {{ ref('inputs_excel_raw') }}

),

inputs_added as (

select
    1 + -date_diff(date_from_fix, date_to_fix, DAY) as delta_days,
    *
from
    inputs

),

metrics as (

select
    cast(date_from as date) as date_from,
    cast(date_to as date) as date_to,
    cast(offline_traffic / delta_days as int) as offline_traffic,
    cast(fb_subscribers / delta_days as int) as fb_subscribers,
    cast(ig_subscribers / delta_days as int) as ig_subscribers,
    cast(total_items_sold / delta_days as int) as total_items_sold,
    cast(plan / delta_days as int) as plan,
    cast(cancelled_orders / delta_days as int) as cancelled_orders,
    cast(sales_with_VAT / delta_days as int) as sales_with_VAT
from
    inputs_added

),

table_metrics as (

select
    campaign_date,
    offline_traffic,
    fb_subscribers,
    ig_subscribers,
    total_items_sold,
    plan,
    cancelled_orders,
    sales_with_VAT
from
    {{ ref('campaigns_dates') }} as d
join
    metrics as m
on
    d.campaign_date between m.date_from and m.date_to

)

select campaign_date, key as table_name, value as amount  from table_metrics
unpivot(value for key in ( offline_traffic,
    fb_subscribers,
    ig_subscribers,
    total_items_sold,
    plan,
    cancelled_orders,
    sales_with_VAT))