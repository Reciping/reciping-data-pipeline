-- 차트 4: 히트맵 데이터 (참여도 vs 전환율)
-- 세그먼트별 성과 매트릭스
-- ========================================

SELECT
    u.user_segment,
    u.ab_test_group,
    COUNT(DISTINCT f.user_dim_key) AS total_users,
    ROUND(COUNT(DISTINCT CASE WHEN f.is_conversion THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key), 2) AS conversion_rate,
    ROUND(AVG(f.engagement_score), 2) AS engagement_score,

    -- 히트맵을 위한 추가 매트릭
    COUNT(*) AS total_events,
    ROUND(AVG(f.session_duration_seconds) / 60.0, 2) AS avg_session_minutes,

    -- 성과 등급 (히트맵 색상 강도용)
    CASE
        WHEN COUNT(DISTINCT CASE WHEN f.is_conversion THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key) >= 10 THEN 'High'
        WHEN COUNT(DISTINCT CASE WHEN f.is_conversion THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key) >= 8 THEN 'Medium'
        ELSE 'Low'
    END AS performance_tier

FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
    AND u.ab_test_group IN ('treatment', 'control')
GROUP BY u.user_segment, u.ab_test_group
HAVING COUNT(DISTINCT f.user_dim_key) >= 50
ORDER BY u.user_segment, u.ab_test_group;