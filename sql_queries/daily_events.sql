-- =====================================
-- 2. 일별 이벤트 타입별 발생량
-- =====================================
SELECT 
    t.date AS event_date,
    e.event_name,
    COUNT(*) AS event_count,
    COUNT(DISTINCT f.user_dim_key) AS unique_users,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY t.date), 2) AS daily_event_percentage
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
WHERE f.user_dim_key != 0
GROUP BY t.date, e.event_name
ORDER BY t.date DESC, event_count DESC;