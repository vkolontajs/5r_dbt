with fb_ads as (

select
    cast(campaign_name as string) as campaign_name,
    cast(date as date) as campaign_date,
    cast(impressions as int) as impressions,
    cast(clicks as int) as clicks,
    cast(spend as float64) as cost
from
    {{ ref('fb_ads_raw') }}
    
),

ga_ads as (

select
    cast(campaign as string) as campaign_name,
    cast(date as date) as campaign_date,
    cast(impressions as int) as impressions,
    cast(adclicks as int) as clicks,
    cast(adcost as float64) as cost
from
    {{ ref('ga_ads_raw') }}

),

ads_combined as (

select
    campaign_name,
    campaign_date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(cost) as cost
 from (
    select
        *
    from
        ga_ads

    union all

    select
        *
    from
        fb_ads
)
group by
    1, 2

),

ga_campaigns as (

select
    cast(campaign as string) as campaign_name,
    cast(date as date) as campaign_date,
    cast(sessions as int) as sessions,
    cast(bounces as int) as bounces,
    cast(avgsessionduration * sessions as int) as sessions_duration,
    cast(quantityaddedtocart as int) as added_to_cart,
    cast(transactions * transactionrevenue as float64) as transactions_revenue
from
    {{ ref('ga_campaigns_raw') }}

)

select
    campaign_name,
    campaign_date,
    sessions,
    bounces,
    sessions_duration,
    clicks,
    cost,
    impressions,
    added_to_cart,
    transactions_revenue
from (
    select
        campaign_name,
        campaign_date,
        sum(sessions) as sessions,
        sum(bounces) as bounces,
        sum(sessions_duration) as sessions_duration,
        sum(clicks) as clicks,
        sum(cost) as cost,
        sum(impressions) as impressions,
        sum(added_to_cart) as added_to_cart,
        sum(transactions_revenue) as transactions_revenue
    from
        ads_combined
    full outer join
        ga_campaigns
    using
        (campaign_name, campaign_date)
    group by
        1, 2
)
where
    campaign_date in (select campaign_date from {{ ref('campaigns_dates') }})