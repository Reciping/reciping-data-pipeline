-- =====================================
-- 4. 요일별 평균 참여도 점수 - 세그먼트별 + 전체
-- =====================================
-- 수정된 요일별 평균 참여도 점수 쿼리
-- 더 간단한 방법: 숫자로 시작하는 요일명
SELECT 
    CASE t.day_of_week
        WHEN 'Mon' THEN '1-Mon'
        WHEN 'Tue' THEN '2-Tue'  
        WHEN 'Wed' THEN '3-Wed'
        WHEN 'Thu' THEN '4-Thu'
        WHEN 'Fri' THEN '5-Fri'
        WHEN 'Sat' THEN '6-Sat'
        WHEN 'Sun' THEN '7-Sun'
        ELSE t.day_of_week
    END AS day_of_week,
    'TOTAL' AS segment_type,
    'ALL' AS segment_value,
    ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score,
    COUNT(*) AS total_events
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.day_of_week

UNION ALL

SELECT 
    CASE t.day_of_week
        WHEN 'Mon' THEN '1-Mon'
        WHEN 'Tue' THEN '2-Tue'  
        WHEN 'Wed' THEN '3-Wed'
        WHEN 'Thu' THEN '4-Thu'
        WHEN 'Fri' THEN '5-Fri'
        WHEN 'Sat' THEN '6-Sat'
        WHEN 'Sun' THEN '7-Sun'
        ELSE t.day_of_week
    END AS day_of_week,
    'USER_SEGMENT' AS segment_type,
    COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
    ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score,
    COUNT(*) AS total_events
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.user_dim_key != 0
GROUP BY t.day_of_week, u.user_segment

ORDER BY day_of_week, segment_type, segment_value;