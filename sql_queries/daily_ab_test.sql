-- 추가: 일별 A/B 테스트 성과 트렌드 분석
SELECT 
    t.date as event_date,
    COALESCE(u.ab_test_group, 'UNKNOWN') AS ab_test_group,
    
    COUNT(DISTINCT f.user_dim_key) AS daily_active_users,
    COUNT(*) AS daily_events,
    
    -- 일별 전환율
    ROUND(COUNT(DISTINCT CASE WHEN f.is_conversion THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key), 2) AS daily_user_conversion_rate_pct,
    
    -- 일별 평균 참여도
    ROUND(AVG(f.engagement_score), 2) AS daily_avg_engagement_score,
    
    -- 일별 평균 세션 시간
    ROUND(AVG(f.session_duration_seconds) / 60.0, 2) AS daily_avg_session_minutes

FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.date, u.ab_test_group
ORDER BY event_date DESC, ab_test_group;