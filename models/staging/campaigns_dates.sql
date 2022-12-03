with wrk_dates as (

SELECT day
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2022-10-03'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day

)

select cast(day as date) as campaign_date from wrk_dates