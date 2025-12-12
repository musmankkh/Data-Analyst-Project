{{config(materialized='table',
    unique_key='date_key')}}

WITH date_spine AS (
    {{dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    )}}
)

SELECT 
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_key,
    date_day AS date,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(quarter FROM date_day) AS quarter,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    EXTRACT(dayofweek FROM date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    MONTHNAME(date_day) AS month_name,
    EXTRACT(week FROM date_day) AS week_of_year,
    CASE WHEN EXTRACT(dayofweek FROM date_day) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE    
        WHEN EXTRACT(month FROM date_day) IN (1,2,3) THEN 'Q1'
        WHEN EXTRACT(month FROM date_day) IN (4,5,6) THEN 'Q2'
        WHEN EXTRACT(month FROM date_day) IN (7,8,9) THEN 'Q3'
        ELSE 'Q4'
    END AS quarter_name
FROM date_spine        
        
        