{{
  config(
    materialized = "table",
    tags=["powerbi"]
  )
}}

select client_name, account_id 
FROM {{ source('inputs_sheets', 'accounts') }}