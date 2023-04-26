{{
  config(
    materialized = "table",
    tags=["powerbi"]
  )
}}
with google_ads as (
    select 
        -- dimensions
        date,  
        'Google Ads' source,
        account_id,
        cast(campaign_name as string) as campaign_name,
        {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} source_key,
        client_name,
         -- metrics
        cast( impressions as int) impressions,
        cast( clicks as int) clicks,
        cast( cost as numeric) cost
    FROM  {{ source('bq_init', 'google_ads_raw') }}
    left join 
      (select client_name, account_id FROM {{ source('inputs_sheets', 'accounts') }} 
      where account_type = 'Google Ads' ) using (account_id)
),

facebook as (
    select 
        -- dimensions
        date,
        'Facebook Ads' source,
        account_id,
        cast(campaign_name as string) as campaign_name,
        {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} source_key,
        client_name,

        -- metrics
        cast( impressions as int) impressions,
        cast( clicks as int) clicks,
        cast( spend as numeric) cost
    from {{ source('bq_init', 'fb_ads') }}
    left join 
      (select client_name, account_id FROM {{ source('inputs_sheets', 'accounts') }} 
      where account_type = 'Facebook' ) using (account_id)
)

select * from google_ads
union all
select * from facebook