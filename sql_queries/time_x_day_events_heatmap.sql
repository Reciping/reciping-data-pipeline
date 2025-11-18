-- =====================================
-- 시간별 x 요일별 이벤트 발생량 히트맵 데이터
-- =====================================
WITH hourly_weekly_data AS (
    SELECT 
        t.hour,
        CASE t.day_of_week
            WHEN 'Mon' THEN '1-Monday'
            WHEN 'Tue' THEN '2-Tuesday'  
            WHEN 'Wed' THEN '3-Wednesday'
            WHEN 'Thu' THEN '4-Thursday'
            WHEN 'Fri' THEN '5-Friday'
            WHEN 'Sat' THEN '6-Saturday'
            WHEN 'Sun' THEN '7-Sunday'
            ELSE t.day_of_week
        END AS day_of_week_ordered,
        COUNT(*) AS event_count,
        COUNT(DISTINCT f.user_dim_key) AS unique_users,
        ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    WHERE f.user_dim_key != 0
    GROUP BY t.hour, t.day_of_week
)
SELECT 
    hour,
    day_of_week_ordered,
    event_count,
    unique_users,
    avg_engagement_score,
    
    -- 히트맵 시각화를 위한 정규화된 값 (0-100 스케일)
    ROUND(
        (event_count - MIN(event_count) OVER()) * 100.0 / 
        (MAX(event_count) OVER() - MIN(event_count) OVER()), 2
    ) AS normalized_intensity
    
FROM hourly_weekly_data
ORDER BY day_of_week_ordered, hour;