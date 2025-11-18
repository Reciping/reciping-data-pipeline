-- =====================================
-- 9. 검색 -> 레시피 조회 전환율 (일간)
-- =====================================
WITH daily_searches AS (
    SELECT 
        t.date,
        u.user_segment,
        COUNT(*) AS search_count,
        COUNT(DISTINCT f.user_dim_key) AS search_users
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    WHERE e.event_name = 'search_recipe'
    GROUP BY t.date, u.user_segment
),
daily_recipe_views AS (
    SELECT 
        t.date,
        u.user_segment,
        COUNT(*) AS view_count,
        COUNT(DISTINCT f.user_dim_key) AS view_users
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    WHERE e.event_name = 'click_recipe'
    GROUP BY t.date, u.user_segment
)
SELECT 
    s.date,
    COALESCE(s.user_segment, 'UNKNOWN') AS user_segment,
    s.search_count,
    COALESCE(v.view_count, 0) AS view_count,
    CASE 
        WHEN s.search_count > 0 THEN ROUND(COALESCE(v.view_count, 0) * 100.0 / s.search_count, 2)
        ELSE 0
    END AS conversion_rate_pct
FROM daily_searches s
LEFT JOIN daily_recipe_views v ON s.date = v.date AND s.user_segment = v.user_segment
ORDER BY s.date, user_segment;