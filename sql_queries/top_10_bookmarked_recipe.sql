SELECT
  r.recipe_name,
  COUNT(*) AS bookmark_count
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_recipe r ON f.recipe_dim_key = r.recipe_sk
JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
WHERE e.event_name = 'click_bookmark'
GROUP BY r.recipe_name
ORDER BY bookmark_count DESC
LIMIT 10;