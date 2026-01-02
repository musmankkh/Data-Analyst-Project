{{
    config(
        materialized = 'table'
    )
}}

with date_spine as (
    select date_day
    from unnest(
            generate_date_array(
                '1995-01-01',
                date_add(current_date(), interval 5 year),
                interval 1 day
            )
        ) as date_day

), 

date_attritubes as (
    select 
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(quarter from date_day) as quarter,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dayofweek from date_day) as day_of_week_num,
        extract(dayofyear from date_day) as day_of_year,
        
        format_date('%Y%m%d', date_day) as date_key,
        format_date('%Y-%m', date_day) as year_month,
        format_date('%Y-Q%Q', date_day) as year_quarter,
        format_date('%B', date_day) as month_name,
        format_date('%A', date_day) as day_name,
        format_date('%b', date_day) as month_name_short,
        format_date('%a', date_day) as day_name_short,

         date_trunc(date_day, week(monday)) as week_start_date,
        date_trunc(date_day, month) as month_start_date,
        date_trunc(date_day, quarter) as quarter_start_date,
        date_trunc(date_day, year) as year_start_date,

        last_day(date_day, month) as month_end_date,
        last_day(date_day, quarter) as quarter_end_date,
        last_day(date_day, year) as year_end_date,

        date_diff(date_day, current_date(), day) as days_from_today,
        date_diff(date_day, current_date(), week) as weeks_from_today,
        date_diff(date_day, current_date(), month) as months_from_today,

         case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
        case when extract(dayofweek from date_day) between 2 and 6 then true else false end as is_weekday,
        case when date_day = date_trunc(date_day, month) then true else false end as is_month_start,
        case when date_day = last_day(date_day, month) then true else false end as is_month_end,
        case when date_day = date_trunc(date_day, quarter) then true else false end as is_quarter_start,
        case when date_day = last_day(date_day, quarter) then true else false end as is_quarter_end,
        case when date_day = date_trunc(date_day, year) then true else false end as is_year_start,
        case when date_day = last_day(date_day, year) then true else false end as is_year_end,
        case when date_day = current_date() then true else false end as is_today,
        case when date_day = date_sub(current_date(), interval 1 day) then true else false end as is_yesterday,

         case
            when date_day = current_date() then 'Today'
            when date_day = date_sub(current_date(), interval 1 day) then 'Yesterday'
            when date_day >= date_trunc(current_date(), week(monday)) then 'This Week'
            when date_day >= date_sub(date_trunc(current_date(), week(monday)), interval 1 week) 
                 and date_day < date_trunc(current_date(), week(monday)) then 'Last Week'
            when date_day >= date_trunc(current_date(), month) then 'This Month'
            when date_day >= date_sub(date_trunc(current_date(), month), interval 1 month)
                 and date_day < date_trunc(current_date(), month) then 'Last Month'
            when date_day >= date_trunc(current_date(), quarter) then 'This Quarter'
            when date_day >= date_trunc(current_date(), year) then 'This Year'
            when date_day < current_date() then 'Historical'
            else 'Future'
        end as period_label,


        case
            when extract(month from date_day) in (12, 1, 2) then 'Winter'
            when extract(month from date_day) in (3, 4, 5) then 'Spring'
            when extract(month from date_day) in (6, 7, 8) then 'Summer'
            when extract(month from date_day) in (9, 10, 11) then 'Fall'
        end as season,

        extract(quarter from date_day) as fiscal_quarter,
        extract(year from date_day) as fiscal_year,

        current_timestamp() as created_at,

     from date_spine   


)

select 

    cast(date_key as int64)  as date_key,
    date_day,

    year,
    month,
    quarter,
    week_of_year,
    day_of_month,
    day_of_week_num,
    day_of_year,

    year_month,
    year_quarter,
    month_name,
    day_name,
    month_name_short,
    day_name_short,

    week_start_date,
    month_start_date,
    quarter_start_date,
    year_start_date,
    month_end_date,
    quarter_end_date,
    year_end_date,

    days_from_today,
    weeks_from_today,
    months_from_today,

    is_weekend,
    is_weekday,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end,
    is_year_start,
    is_year_end,
    is_today,
    is_yesterday,

    period_label,
    season,
    fiscal_quarter,
    fiscal_year,
    created_at

from date_attritubes
order by date_day    
