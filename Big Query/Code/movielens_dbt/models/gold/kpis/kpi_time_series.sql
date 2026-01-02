{{ config(
    materialized = 'table',
    partition_by = {
      "field": "rating_month",
      "data_type": "timestamp",
      "granularity": "month"
    }
) }}

with daily_base as (
    select
        fr.rating_date,
        timestamp(date_trunc(fr.rating_date, month)) as rating_month,
        dd.date_key,
        dd.year,
        dd.quarter,
        dd.month,
        dd.month_name,
        dd.week_of_year,
        dd.day_of_week_num,
        dd.day_name,
        dd.is_weekend,
        dd.is_weekday,
        dd.season,
        dd.year_month,
        dd.year_quarter,
        
        count(*) as total_ratings,
        count(distinct fr.user_id) as active_users,
        count(distinct fr.movie_id) as unique_movies_rated,
        round(avg(fr.rating_value), 2) as avg_rating,
        countif(fr.rating_sentiment = 'Positive') as positive_ratings,
        countif(fr.rating_sentiment = 'Negative') as negative_ratings
        
    from {{ ref('fact_ratings') }} fr
    left join {{ ref('dim_date') }} dd
        on fr.rating_date = dd.date_day
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
),

daily_with_moving_avg as (
    select
        *,
        
        -- Moving averages (7-day and 30-day)
        round(avg(total_ratings) over (
            order by rating_date
            rows between 6 preceding and current row
        ), 1) as ma_7day_ratings,
        
        round(avg(total_ratings) over (
            order by rating_date
            rows between 29 preceding and current row
        ), 1) as ma_30day_ratings,
        
        round(avg(avg_rating) over (
            order by rating_date
            rows between 6 preceding and current row
        ), 2) as ma_7day_avg_rating,
        
        -- Period-over-period changes
        total_ratings - lag(total_ratings, 1) over (order by rating_date) as dod_rating_change,
        total_ratings - lag(total_ratings, 7) over (order by rating_date) as wow_rating_change,
        
        round(
            (total_ratings - lag(total_ratings, 1) over (order by rating_date)) * 100.0 /
            nullif(lag(total_ratings, 1) over (order by rating_date), 0),
            1
        ) as dod_rating_change_pct,
        
        round(
            (total_ratings - lag(total_ratings, 7) over (order by rating_date)) * 100.0 /
            nullif(lag(total_ratings, 7) over (order by rating_date), 0),
            1
        ) as wow_rating_change_pct
        
    from daily_base
),

monthly_aggregates as (
    select
        year_month,
        year,
        month,
        month_name,
        
        sum(total_ratings) as monthly_total_ratings,
        round(avg(avg_rating), 2) as monthly_avg_rating,
        sum(active_users) as monthly_active_users_sum,
        
        -- Month-over-month
        sum(total_ratings) - lag(sum(total_ratings)) over (order by year_month) as mom_rating_change,
        round(
            (sum(total_ratings) - lag(sum(total_ratings)) over (order by year_month)) * 100.0 /
            nullif(lag(sum(total_ratings)) over (order by year_month), 0),
            1
        ) as mom_rating_change_pct
        
    from daily_base
    group by 1,2,3,4
),

quarterly_aggregates as (
    select
        year_quarter,
        year,
        quarter,
        
        sum(total_ratings) as quarterly_total_ratings,
        round(avg(avg_rating), 2) as quarterly_avg_rating,
        
        -- Quarter-over-quarter
        sum(total_ratings) - lag(sum(total_ratings)) over (order by year_quarter) as qoq_rating_change,
        round(
            (sum(total_ratings) - lag(sum(total_ratings)) over (order by year_quarter)) * 100.0 /
            nullif(lag(sum(total_ratings)) over (order by year_quarter), 0),
            1
        ) as qoq_rating_change_pct
        
    from daily_base
    group by 1,2,3
)

select
    -- Date Identifiers
    db.rating_date,
    db.rating_month,
    db.date_key,
    db.year,
    db.quarter,
    db.month,
    db.month_name,
    db.week_of_year,
    db.day_of_week_num,
    db.day_name,
    db.year_month,
    db.year_quarter,
    
    -- Date Attributes
    db.is_weekend,
    db.is_weekday,
    db.season,
    
    -- Daily KPIs
    db.total_ratings as daily_ratings,
    db.active_users as daily_active_users,
    db.unique_movies_rated as daily_unique_movies,
    db.avg_rating as daily_avg_rating,
    db.positive_ratings as daily_positive_ratings,
    db.negative_ratings as daily_negative_ratings,
    
    -- Derived Daily Metrics
    round(db.positive_ratings * 100.0 / nullif(db.total_ratings, 0), 1) as daily_pct_positive,
    
    -- Moving Averages
    dma.ma_7day_ratings,
    dma.ma_30day_ratings,
    dma.ma_7day_avg_rating,
    
    -- Day-over-Day Changes
    dma.dod_rating_change,
    dma.dod_rating_change_pct,
    
    -- Week-over-Week Changes
    dma.wow_rating_change,
    dma.wow_rating_change_pct,
    
    -- Monthly Aggregates
    ma.monthly_total_ratings,
    ma.monthly_avg_rating,
    ma.mom_rating_change,
    ma.mom_rating_change_pct,
    
    -- Quarterly Aggregates
    qa.quarterly_total_ratings,
    qa.quarterly_avg_rating,
    qa.qoq_rating_change,
    qa.qoq_rating_change_pct,
    
    -- Trend Classification
    case
        when dma.ma_7day_ratings > dma.ma_30day_ratings * 1.1 then 'Strong Upward'
        when dma.ma_7day_ratings > dma.ma_30day_ratings * 1.05 then 'Upward'
        when dma.ma_7day_ratings < dma.ma_30day_ratings * 0.95 then 'Downward'
        when dma.ma_7day_ratings < dma.ma_30day_ratings * 0.9 then 'Strong Downward'
        else 'Stable'
    end as trend_direction,
    
    -- Activity Level
    case
        when db.total_ratings >= dma.ma_30day_ratings * 1.5 then 'Very High'
        when db.total_ratings >= dma.ma_30day_ratings * 1.2 then 'High'
        when db.total_ratings >= dma.ma_30day_ratings * 0.8 then 'Normal'
        when db.total_ratings >= dma.ma_30day_ratings * 0.5 then 'Low'
        else 'Very Low'
    end as activity_level,
    
    -- Metadata
    current_timestamp() as kpi_generated_at
    
from daily_base db
left join daily_with_moving_avg dma 
    on db.rating_date = dma.rating_date
left join monthly_aggregates ma 
    on db.year_month = ma.year_month
left join quarterly_aggregates qa 
    on db.year_quarter = qa.year_quarter