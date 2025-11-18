WITH extended_ads_funnel AS (
    SELECT 
        f.user_dim_key,
        MAX(CASE WHEN e.event_name = 'view_page' THEN 1 ELSE 0 END) AS step_1_visit,
        MAX(CASE WHEN e.event_name = 'search_recipe' THEN 1 ELSE 0 END) AS step_2_search,
        MAX(CASE WHEN e.event_name = 'click_recipe' THEN 1 ELSE 0 END) AS step_3_click_recipe,
        MAX(CASE WHEN e.event_name = 'view_recipe_list' THEN 1 ELSE 0 END) AS step_4_view_recipe,
        MAX(CASE WHEN e.event_name = 'click_bookmark' THEN 1 ELSE 0 END) AS step_5_bookmark,
        MAX(CASE WHEN e.event_name = 'view_ads' THEN 1 ELSE 0 END) AS step_6_view_ads,
        MAX(CASE WHEN e.event_name = 'click_ads' THEN 1 ELSE 0 END) AS step_7_click_ads
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
    WHERE f.user_dim_key != 0
    GROUP BY f.user_dim_key
),
funnel_counts AS (
    SELECT 
        SUM(step_1_visit) AS users_visit,
        SUM(step_2_search) AS users_search,
        SUM(step_3_click_recipe) AS users_click_recipe,
        SUM(step_4_view_recipe) AS users_view_recipe,
        SUM(step_5_bookmark) AS users_bookmark,
        SUM(step_6_view_ads) AS users_view_ads,
        SUM(step_7_click_ads) AS users_click_ads
    FROM extended_ads_funnel
)
SELECT 
    '1. Visit Site' AS funnel_step,
    users_visit AS users,
    100.0 AS conversion_rate_pct,
    0.0 AS drop_off_rate_pct
FROM funnel_counts

UNION ALL

SELECT 
    '2. Search Recipe',
    users_search,
    CASE 
        WHEN users_visit > 0 THEN ROUND(users_search * 100.0 / users_visit, 2)
        ELSE 0.0
    END,
    CASE 
        WHEN users_visit > 0 THEN ROUND((users_visit - users_search) * 100.0 / users_visit, 2)
        ELSE 0.0
    END
FROM funnel_counts

UNION ALL

SELECT 
    '3. Click Recipe',
    users_click_recipe,
    CASE 
        WHEN users_search > 0 THEN ROUND(users_click_recipe * 100.0 / users_search, 2)
        ELSE 0.0
    END,
    CASE 
        WHEN users_search > 0 THEN ROUND((users_search - users_click_recipe) * 100.0 / users_search, 2)
        ELSE 0.0
    END
FROM funnel_counts

UNION ALL

-- SELECT 
--     '4. View Recipe Detail',
--     users_view_recipe,
--     CASE 
--         WHEN users_click_recipe > 0 THEN ROUND(users_view_recipe * 100.0 / users_click_recipe, 2)
--         ELSE 0.0
--     END,
--     CASE 
--         WHEN users_click_recipe > 0 THEN ROUND((users_click_recipe - users_view_recipe) * 100.0 / users_click_recipe, 2)
--         ELSE 0.0
--     END
-- FROM funnel_counts

-- UNION ALL

-- SELECT 
--     '4. Bookmark Recipe',
--     users_bookmark,
--     CASE 
--         WHEN users_click_recipe > 0 THEN ROUND(users_bookmark * 100.0 / users_click_recipe, 2)
--         ELSE 0.0
--     END,
--     CASE 
--         WHEN users_click_recipe > 0 THEN ROUND((users_click_recipe - users_bookmark) * 100.0 / users_click_recipe, 2)
--         ELSE 0.0
--     END
-- FROM funnel_counts

-- UNION ALL

SELECT 
    '4. View Ads (Impression)',
    users_view_ads,
    CASE 
        WHEN users_click_recipe > 0 THEN ROUND(users_view_ads * 100.0 / users_click_recipe, 2)
        ELSE 0.0
    END,
    CASE 
        WHEN users_click_recipe > 0 THEN ROUND((users_click_recipe - users_view_ads) * 100.0 / users_click_recipe, 2)
        ELSE 0.0
    END
FROM funnel_counts

UNION ALL

SELECT 
    '5. Click Ads (Revenue)',
    users_click_ads,
    CASE 
        WHEN users_view_ads > 0 THEN ROUND(users_click_ads * 100.0 / users_view_ads, 2)
        ELSE 0.0
    END,
    CASE 
        WHEN users_view_ads > 0 THEN ROUND((users_view_ads - users_click_ads) * 100.0 / users_view_ads, 2)
        ELSE 0.0
    END
FROM funnel_counts;