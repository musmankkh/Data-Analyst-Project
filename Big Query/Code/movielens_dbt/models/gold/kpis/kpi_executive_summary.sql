{{ config(
    materialized = 'table'
) }}

WITH platform_overview AS (
    SELECT
        COUNT(DISTINCT user_id) AS total_users,
        COUNT(DISTINCT movie_id) AS total_movies,
        COUNT(*) AS total_ratings,
        ROUND(AVG(rating_value), 2) AS overall_avg_rating,
        MIN(rating_date) AS platform_start_date,
        MAX(rating_date) AS platform_last_date,
        date_diff(MAX(rating_date), MIN(rating_date), DAY) AS platform_lifetime_days
    FROM
        {{ ref('fact_ratings') }}
),
recent_activity AS (
    SELECT
        COUNT(
            DISTINCT CASE
                WHEN rating_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY) THEN user_id END
        ) AS active_users_30d,
        COUNT(
            DISTINCT CASE
                WHEN rating_date >= date_sub(CURRENT_DATE(), INTERVAL 90 DAY) THEN user_id END
        ) AS active_users_90d,
        COUNT(
            DISTINCT CASE
                WHEN rating_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END
        ) AS ratings_30d,
        COUNT(
            DISTINCT CASE
                WHEN rating_date >= date_sub(CURRENT_DATE(), INTERVAL 90 DAY) THEN 1 END
        ) AS ratings_90d,
        ROUND(
            AVG(
                CASE
                    WHEN rating_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY) THEN rating_value END
            ),
            2
        ) AS avg_rating_30d
    FROM
        {{ ref('fact_ratings') }}
),
user_segmentation AS (
    SELECT
        COUNT(DISTINCT CASE WHEN user_type = 'Super Power User' THEN 1 END) AS super_power_users,
        COUNT(DISTINCT CASE WHEN user_type = 'Power User' THEN 1 END) AS power_users,
        COUNT(DISTINCT CASE WHEN user_type = 'Heavy User' THEN 1 END) AS heavy_users,
        COUNT(DISTINCT CASE WHEN user_type = 'Regular User' THEN 1 END) AS regular_users,
        COUNT(DISTINCT CASE WHEN user_type = 'Casual User' THEN 1 END) AS casual_users,
        COUNT(DISTINCT CASE WHEN user_type = 'Light User' THEN 1 END) AS light_users
    FROM
        {{ ref('kpi_user_engagement') }}
),
content_quality AS (
    SELECT
        COUNT(
            CASE
                WHEN rating_tier IN ('5 Star (4.5+)', '4+ Star (4.0-4.5)') THEN 1
            END
        ) AS high_quality_movies,
        COUNT(CASE WHEN rating_tier = '3.5+ Star' THEN 1 END) AS good_quality_movies,
        COUNT(CASE WHEN rating_tier = '3+ Star' THEN 1 END) AS average_quality_movies,
        COUNT(
            CASE
                WHEN popularity_tier IN ('Blockbuster (10K+)', 'Major Hit (5K+)') THEN 1
            END
        ) AS blockbluster_movies,
        COUNT(CASE WHEN popularity_tier = 'Very Popular (1K+)' THEN 1 END) AS very_popular_movies
    FROM
        {{ ref('kpi_movie_performance') }}
),
top_genres AS (
    SELECT
        ARRAY_AGG(
            STRUCT(
                genre_name,
                total_ratings,
                avg_rating,
                genre_health_score
            )
            ORDER BY total_ratings DESC
            LIMIT 10
        ) AS top_10_genres_by_volume,
        ARRAY_AGG(
            STRUCT(
                genre_name,
                avg_rating,
                total_ratings,
                genre_health_score
            )
            ORDER BY avg_rating DESC
            LIMIT 10
        ) AS top_10_genres_by_quality
    FROM
        {{ ref('kpi_genre_performance') }}
),
latest_trends AS (
    SELECT
        MAX(
            CASE
                WHEN rating_date = (SELECT MAX(rating_date) FROM {{ ref('kpi_time_series') }})
                THEN daily_ratings
            END
        ) AS latest_daily_ratings,
        MAX(
            CASE
                WHEN rating_date = (SELECT MAX(rating_date) FROM {{ ref('kpi_time_series') }})
                THEN daily_avg_rating
            END
        ) AS latest_daily_avg_rating,
        MAX(mom_rating_change_pct) AS latest_mom_change_pct,
        MAX(qoq_rating_change_pct) AS latest_qoq_change_pct,
        AVG(ma_30day_ratings) AS avg_daily_ratings_30d
    FROM
        {{ ref('kpi_time_series') }}
    WHERE
        rating_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT
    po.total_users,
    po.total_movies,
    po.total_ratings,
    po.overall_avg_rating,
    po.platform_start_date,
    po.platform_last_date,
    po.platform_lifetime_days,
    ra.active_users_30d,
    ra.active_users_90d,
    ra.ratings_30d,
    ra.ratings_90d,
    ra.avg_rating_30d,
    ROUND(ra.ratings_30d * 1.0 / 30, 0) AS calc_avg_daily_ratings_30d,
    ROUND(ra.active_users_30d * 100.0 / po.total_users, 2) AS pct_users_active_30d,
    ROUND(po.total_ratings * 1.0 / po.platform_lifetime_days, 0) AS avg_daily_ratings_lifetime,
    ROUND(po.total_ratings * 1.0 / po.total_users, 1) AS avg_ratings_per_user,
    ROUND(po.total_ratings * 1.0 / po.total_movies, 1) AS avg_ratings_per_movie,
    us.super_power_users,
    us.power_users,
    us.heavy_users,
    us.regular_users,
    us.casual_users,
    us.light_users,
    cq.high_quality_movies,
    cq.good_quality_movies,
    cq.average_quality_movies,
    cq.blockbluster_movies,
    cq.very_popular_movies,
    tg.top_10_genres_by_quality,
    tg.top_10_genres_by_volume,
    lt.latest_daily_ratings,
    lt.latest_daily_avg_rating,
    lt.avg_daily_ratings_30d,
    lt.latest_qoq_change_pct,
    lt.latest_mom_change_pct,
    CURRENT_TIMESTAMP() AS kpi_generated_at
FROM
    platform_overview po
    CROSS JOIN recent_activity ra
    CROSS JOIN user_segmentation us
    CROSS JOIN content_quality cq
    CROSS JOIN top_genres tg
    CROSS JOIN latest_trends lt