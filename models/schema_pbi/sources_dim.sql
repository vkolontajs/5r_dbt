{{
  config(
    materialized = "table",
    unique_key="source_key",
    tags=["powerbi"]
  )
}}

with google_ads as (
    select 
        distinct 
        -- 'Google Ads' source,
        -- cast(campaign_name as string) as campaign_name,
        {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} source_key
    FROM  {{ source('bq_init', 'google_ads_raw') }}
),

facebook as (
    select distinct
        -- 'Facebook Ads' source,
        -- cast(campaign_name as string) as campaign_name,
        {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} source_key
    from {{ source('bq_init', 'fb_ads') }}
),

google_analytics as (
  select distinct
    -- cast(campaign as string) as campaign,
    {{ dbt_utils.generate_surrogate_key(['campaign']) }} source_key
  FROM  {{ source('bq_init', 'ga_campaigns') }}
),

mappings as (

  select distinct
    *, 
    {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} source_key
  from {{ source('inputs_sheets', 'ga_mappings') }}

)

select distinct * from (
  select * from google_ads
  UNION DISTINCT
  select * from facebook
  UNION DISTINCT
  select * from google_analytics
)
left join mappings using(source_key)