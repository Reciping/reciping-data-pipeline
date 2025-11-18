-- =====================================
-- 광고 성과 분석 (CTR 중심)
-- =====================================
WITH ads_performance AS (
    SELECT 
        u.user_segment,
        u.ab_test_group,
        COUNT(DISTINCT f.user_dim_key) AS total_users,
        SUM(CASE WHEN e.event_name = 'view_ads' THEN 1 ELSE 0 END) AS total_ad_views,
        SUM(CASE WHEN e.event_name = 'click_ads' THEN 1 ELSE 0 END) AS total_ad_clicks,
        COUNT(DISTINCT CASE WHEN e.event_name = 'view_ads' THEN f.user_dim_key END) AS users_viewed_ads,
        COUNT(DISTINCT CASE WHEN e.event_name = 'click_ads' THEN f.user_dim_key END) AS users_clicked_ads
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
    WHERE f.user_dim_key != 0
    GROUP BY u.user_segment, u.ab_test_group
)
SELECT 
    COALESCE(user_segment, 'UNKNOWN') AS user_segment,
    COALESCE(ab_test_group, 'UNKNOWN') AS ab_test_group,
    total_users,
    total_ad_views,
    total_ad_clicks,
    users_viewed_ads,
    users_clicked_ads,
    
    -- CTR (Click Through Rate): 광고 효과의 핵심 지표
    CASE 
        WHEN total_ad_views > 0 THEN ROUND(total_ad_clicks * 100.0 / total_ad_views, 2)
        ELSE 0.0
    END AS ctr_pct,
    
    -- 광고 노출률 (사용자 기준)
    ROUND(users_viewed_ads * 100.0 / total_users, 2) AS ad_exposure_rate_pct,
    
    -- 광고 전환률 (사용자 기준)
    CASE 
        WHEN users_viewed_ads > 0 THEN ROUND(users_clicked_ads * 100.0 / users_viewed_ads, 2)
        ELSE 0.0
    END AS ad_conversion_rate_pct
    
FROM ads_performance
WHERE total_ad_views > 0  -- 광고 노출이 있는 세그먼트만
ORDER BY ctr_pct DESC;