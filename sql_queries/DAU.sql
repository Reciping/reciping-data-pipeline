-- =====================================
-- 1. 일간 활성 사용자 (DAU) - 세그먼트별 + 전체
-- =====================================
SELECT 
    t.date AS event_date,
    'TOTAL' AS segment_type,
    'ALL' AS segment_value,
    COUNT(DISTINCT f.user_dim_key) AS dau
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.date

UNION ALL

SELECT 
    t.date AS event_date,
    'USER_SEGMENT' AS segment_type,
    COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
    COUNT(DISTINCT f.user_dim_key) AS dau
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.date, u.user_segment

UNION ALL

SELECT 
    t.date AS event_date,
    'COOKING_STYLE' AS segment_type,
    COALESCE(u.cooking_style, 'UNKNOWN') AS segment_value,
    COUNT(DISTINCT f.user_dim_key) AS dau
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.date, u.cooking_style

UNION ALL

SELECT 
    t.date AS event_date,
    'AB_TEST_GROUP' AS segment_type,
    COALESCE(u.ab_test_group, 'UNKNOWN') AS segment_value,
    COUNT(DISTINCT f.user_dim_key) AS dau
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.date, u.ab_test_group

ORDER BY event_date, segment_type, segment_value;