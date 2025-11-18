-- =====================================
-- 7. 요리 종류(Dish Type)별 인기도 - 세그먼트별 + 전체
-- =====================================
SELECT 
    r.dish_type,
    'TOTAL' AS segment_type,
    'ALL' AS segment_value,
    COUNT(*) AS total_interactions,
    COUNT(DISTINCT f.user_dim_key) AS unique_users,
    ROUND(AVG(f.engagement_score), 2) AS avg_engagement
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_recipe r ON f.recipe_dim_key = r.recipe_sk
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.recipe_dim_key != 0
GROUP BY r.dish_type

UNION ALL

SELECT 
    r.dish_type,
    'USER_SEGMENT' AS segment_type,
    COALESCE(u.user_segment, 'UNKNOWN') AS segment_value,
    COUNT(*) AS total_interactions,
    COUNT(DISTINCT f.user_dim_key) AS unique_users,
    ROUND(AVG(f.engagement_score), 2) AS avg_engagement
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_recipe r ON f.recipe_dim_key = r.recipe_sk
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
WHERE f.recipe_dim_key != 0
GROUP BY r.dish_type, u.user_segment

ORDER BY dish_type, segment_type, total_interactions DESC;
