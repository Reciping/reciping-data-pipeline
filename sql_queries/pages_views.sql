-- =====================================
-- 10. 페이지별 조회수 - 세그먼트별 + 전체
-- =====================================
SELECT 
    p.page_name,
    'TOTAL' AS segment_type,
    'ALL' AS segment_value,
    COUNT(*) AS page_views,
    COUNT(DISTINCT f.user_dim_key) AS unique_visitors
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_page p ON f.page_dim_key = p.page_sk
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.page_dim_key != 0
GROUP BY p.page_name

UNION ALL

SELECT 
    p.page_name,
    'USER_SEGMENT' AS segment_type,
    COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
    COUNT(*) AS page_views,
    COUNT(DISTINCT f.user_dim_key) AS unique_visitors
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_page p ON f.page_dim_key = p.page_sk
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.page_dim_key != 0
GROUP BY p.page_name, u.user_segment

ORDER BY page_name, segment_type, page_views DESC;
