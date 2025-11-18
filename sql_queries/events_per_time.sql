-- =====================================
-- 2. 시간대별 이벤트 발생량 - 세그먼트별 + 전체
-- =====================================
SELECT 
    t.hour,
    'TOTAL' AS segment_type,
    'ALL' AS segment_value,
    COUNT(*) AS event_count,
    COUNT(DISTINCT f.user_dim_key) AS unique_users
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.hour

UNION ALL

SELECT 
    t.hour,
    'USER_SEGMENT' AS segment_type,
    COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
    COUNT(*) AS event_count,
    COUNT(DISTINCT f.user_dim_key) AS unique_users
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.hour, u.user_segment

ORDER BY hour, segment_type, segment_value;