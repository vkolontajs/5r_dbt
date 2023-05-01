{{
  config(
    materialized = "table",
    tags=["powerbi"]
  )
}}


with inputs as (

select
    cast(date_from as date) as date_from_fix,
    cast(date_to as date) as date_to_fix,
    *
from
    {{ ref('inputs_excel_raw') }}

),

wrk_dates as (

SELECT 
    cast(day as date) as campaign_date
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2022-10-03'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day

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
    cast(offline_traffic / delta_days as float64) as offline_traffic,
    cast(fb_subscribers / delta_days as float64) as fb_subscribers,
    cast(ig_subscribers / delta_days as float64) as ig_subscribers,
    cast(total_items_sold / delta_days as float64) as total_items_sold,
    cast(plan / delta_days as float64) as plan,
    cast(cancelled_orders / delta_days as float64) as cancelled_orders,
    cast(sales_with_VAT / delta_days as float64) as sales_with_VAT
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
    wrk_dates as d
join
    metrics as m
on
    d.campaign_date between m.date_from and m.date_to

)

select 
    campaign_date, 
    key as table_name, 
    value as amount  
from table_metrics
unpivot(value for key in ( offline_traffic,
    fb_subscribers,
    ig_subscribers,
    total_items_sold,
    plan,
    cancelled_orders,
    sales_with_VAT))

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


order by campaign_date desc 

