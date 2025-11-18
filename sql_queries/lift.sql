-- ========================================
-- Lift 분석 (Horizontal Bar Chart)
-- 성과 기준: 광고 클릭률 (Ad Click Conversion Rate)
-- ========================================
WITH segment_stats AS (
    SELECT
        u.user_segment,
        u.ab_test_group,
        COUNT(DISTINCT f.user_dim_key) AS total_users,

        -- [변경됨] 'is_conversion' 대신 '광고 클릭 사용자' 비율을 계산
        ROUND(COUNT(DISTINCT CASE WHEN e.event_name = 'click_ads' THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key), 2) AS avg_conversion,

        ROUND(AVG(f.engagement_score), 2) AS avg_engagement
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk -- [추가됨] 이벤트 이름 확인을 위해 JOIN
    WHERE f.user_dim_key != 0
        AND u.ab_test_group IN ('treatment', 'control')
    GROUP BY u.user_segment, u.ab_test_group
    HAVING COUNT(DISTINCT f.user_dim_key) >= 100
),
lift_calculation AS (
    SELECT
        t.user_segment,
        t.avg_conversion as treatment_conversion,
        c.avg_conversion as control_conversion,
        t.avg_engagement as treatment_engagement,
        c.avg_engagement as control_engagement,
        t.total_users as treatment_users,
        c.total_users as control_users,

        -- Lift 계산
        ROUND(t.avg_conversion - c.avg_conversion, 2) as absolute_lift,
        ROUND((t.avg_conversion - c.avg_conversion) / NULLIF(c.avg_conversion, 0) * 100, 1) as relative_lift_pct,

        -- 참여도 차이
        ROUND(t.avg_engagement - c.avg_engagement, 3) as engagement_diff,

        -- 승자 판정 (0.1%p 이상을 의미있는 차이로 판단)
        CASE
            WHEN ABS(t.avg_conversion - c.avg_conversion) < 0.1 THEN '무승부'
            WHEN t.avg_conversion > c.avg_conversion THEN 'Treatment 승리'
            ELSE 'Control 승리'
        END as winner,

        -- 통계적 유의성 (간단한 기준)
        CASE
            WHEN ABS(t.avg_conversion - c.avg_conversion) >= 0.3 THEN '매우 유의함'
            WHEN ABS(t.avg_conversion - c.avg_conversion) >= 0.1 THEN '유의함'
            ELSE '유의하지 않음'
        END as significance_level

    FROM segment_stats t
    JOIN segment_stats c ON t.user_segment = c.user_segment
    WHERE t.ab_test_group = 'treatment' AND c.ab_test_group = 'control'
)
SELECT
    user_segment,
    treatment_conversion,
    control_conversion,
    absolute_lift,
    relative_lift_pct,
    engagement_diff,
    winner,
    significance_level,
    treatment_users,
    control_users,

    -- 차트 색상 (승자에 따라)
    CASE
        WHEN winner = 'Treatment 승리' THEN '#2ca02c'  -- 녹색
        WHEN winner = 'Control 승리' THEN '#d62728'    -- 빨간색
        ELSE '#7f7f7f'  -- 회색
    END as result_color

FROM lift_calculation
ORDER BY relative_lift_pct DESC;