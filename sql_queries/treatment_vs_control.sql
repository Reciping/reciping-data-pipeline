-- ========================================
-- AB 테스트 성과 비교 (성과 기준: 광고 클릭률)
-- ========================================
WITH segment_performance AS (
    SELECT
        u.user_segment,
        u.ab_test_group,
        COUNT(DISTINCT f.user_dim_key) AS total_users,

        -- [변경됨] 성과 지표: 'is_conversion' 대신 '광고 클릭 사용자' 비율로 변경
        ROUND(COUNT(DISTINCT CASE WHEN e.event_name = 'click_ads' THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key), 2) AS ad_click_conversion_rate,

        -- 보조 지표로 여전히 활용 가능
        ROUND(AVG(f.engagement_score), 2) AS avg_engagement_score

    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk -- [추가됨] 이벤트 이름 확인을 위해 JOIN
    WHERE f.user_dim_key != 0
        AND u.ab_test_group IN ('treatment', 'control')  -- UNKNOWN 제외
    GROUP BY u.user_segment, u.ab_test_group
    HAVING COUNT(DISTINCT f.user_dim_key) >= 100  -- 충분한 표본 크기
)
SELECT
    user_segment,
    ab_test_group,
    ad_click_conversion_rate, -- [변경됨] 새로운 성과 지표
    avg_engagement_score,
    total_users,

    -- 색상 구분을 위한 필드
    CASE
        WHEN ab_test_group = 'treatment' THEN '#1f77b4'  -- 파란색
        WHEN ab_test_group = 'control' THEN '#ff7f0e'    -- 주황색
    END as chart_color
FROM segment_performance
ORDER BY user_segment, ab_test_group;