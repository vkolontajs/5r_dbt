{{
  config(
    materialized = "table",
    tags=["powerbi"],
    schema='pbi'
  )
}}

select client_name, account_id 
FROM {{ source('inputs_sheets', 'accounts') }}