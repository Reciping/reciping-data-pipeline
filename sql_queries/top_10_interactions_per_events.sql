SELECT 
    COALESCE(r.recipe_name, 'Unknown Recipe') as recipe_name,
    COALESCE(r.dish_type, 'Unknown') as dish_type,
    COALESCE(e.event_name, 'Unknown Event') as event_name,
    COUNT(*) AS interaction_count,
    COUNT(DISTINCT f.user_dim_key) AS unique_users
FROM gold_analytics.fact_user_events f
LEFT JOIN gold_analytics.dim_recipe r ON f.recipe_dim_key = r.recipe_sk
LEFT JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
WHERE f.recipe_dim_key != 0
  AND r.recipe_name IN (
    -- 상위 10개 레시피만 필터링
    SELECT recipe_name 
    FROM (
      SELECT r2.recipe_name, COUNT(*) as total_interactions
      FROM gold_analytics.fact_user_events f2
      LEFT JOIN gold_analytics.dim_recipe r2 ON f2.recipe_dim_key = r2.recipe_sk
      WHERE f2.recipe_dim_key != 0
      GROUP BY r2.recipe_name
      ORDER BY total_interactions DESC
      LIMIT 10
    ) top_recipes
  )
GROUP BY r.recipe_name, r.dish_type, e.event_name
ORDER BY r.recipe_name, interaction_count DESC;