-- traffic_fct.sql
{{
  config(
    materialized = "table",
    unique_key="source_key",
    tags=["powerbi"]
  )
}}

select
  *,
  Lower( SPLIT(sourcemedium, ' / ')[OFFSET(0)] ) as source,
  Lower( SPLIT(sourcemedium, ' / ')[OFFSET(1)] ) as medium,
  {{ dbt_utils.generate_surrogate_key(['campaign']) }} source_key
FROM  {{ source('bq_init', 'ga_campaigns') }}
left join 
  (select client_name, account_id FROM {{ source('inputs_sheets', 'accounts') }} 
  where account_type = 'Google Analytics' ) using (account_id)

