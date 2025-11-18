-- =====================================
-- 1. WAU (Weekly Active Users) 분석
-- =====================================
WITH weekly_data AS (
    SELECT 
        DATE_TRUNC('week', t.date) AS week_start,
        YEAR(t.date) AS year,
        WEEK(t.date) AS week_number,
        'TOTAL' AS segment_type,
        'ALL' AS segment_value,
        COUNT(DISTINCT f.user_dim_key) AS wau
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    WHERE f.user_dim_key != 0
    GROUP BY DATE_TRUNC('week', t.date), YEAR(t.date), WEEK(t.date)

    UNION ALL

    SELECT 
        DATE_TRUNC('week', t.date) AS week_start,
        YEAR(t.date) AS year,
        WEEK(t.date) AS week_number,
        'USER_SEGMENT' AS segment_type,
        COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
        COUNT(DISTINCT f.user_dim_key) AS wau
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    WHERE f.user_dim_key != 0
    GROUP BY DATE_TRUNC('week', t.date), YEAR(t.date), WEEK(t.date), u.user_segment

    UNION ALL

    SELECT 
        DATE_TRUNC('week', t.date) AS week_start,
        YEAR(t.date) AS year,
        WEEK(t.date) AS week_number,
        'AB_TEST_GROUP' AS segment_type,
        COALESCE(u.ab_test_group, 'UNKNOWN') AS segment_value,
        COUNT(DISTINCT f.user_dim_key) AS wau
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    WHERE f.user_dim_key != 0
    GROUP BY DATE_TRUNC('week', t.date), YEAR(t.date), WEEK(t.date), u.ab_test_group
)
SELECT 
    week_start,
    year,
    week_number,
    segment_type,
    segment_value,
    wau,
    -- 전주 대비 성장률
    LAG(wau) OVER (PARTITION BY segment_type, segment_value ORDER BY week_start) AS prev_week_wau,
    CASE 
        WHEN LAG(wau) OVER (PARTITION BY segment_type, segment_value ORDER BY week_start) > 0 
        THEN ROUND((wau - LAG(wau) OVER (PARTITION BY segment_type, segment_value ORDER BY week_start)) * 100.0 / LAG(wau) OVER (PARTITION BY segment_type, segment_value ORDER BY week_start), 2)
        ELSE NULL
    END AS week_over_week_growth_pct
FROM weekly_data
ORDER BY week_start DESC, segment_type, segment_value;
