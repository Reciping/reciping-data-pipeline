-- =====================================
-- 8. 핵심 행동(이벤트) 분포 - 세그먼트별 + 전체
-- =====================================
SELECT 
    e.event_name,
    'TOTAL' AS segment_type,
    'ALL' AS segment_value,
    COUNT(*) AS event_count,
    COUNT(DISTINCT f.user_dim_key) AS unique_users,
    ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY e.event_name

UNION ALL

SELECT 
    e.event_name,
    'USER_SEGMENT' AS segment_type,
    COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
    COUNT(*) AS event_count,
    COUNT(DISTINCT f.user_dim_key) AS unique_users,
    ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY e.event_name, u.user_segment

ORDER BY event_name, segment_type, event_count DESC;
