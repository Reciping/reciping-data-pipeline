-- =================================================================
-- 🔢 최종 차트 5: 핵심 KPI 메트릭 (정확한 테스트 기간 필터 적용)
-- =================================================================

-- 공통 CTE 1: 그룹별 광고 클릭 전환율 미리 계산
WITH segment_ad_conversion AS (
    SELECT
        u.user_segment,
        u.ab_test_group,
        COUNT(DISTINCT CASE WHEN e.event_name = 'click_ads' THEN f.user_dim_key END) * 100.0 / COUNT(DISTINCT f.user_dim_key) AS ad_click_conv_rate
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    JOIN gold_analytics.dim_event e ON f.event_dim_key = e.event_sk
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    WHERE u.ab_test_group IN ('treatment', 'control')
        AND t.date BETWEEN DATE '2025-08-08' AND DATE '2025-08-22' -- << 여기를 수정했습니다
    GROUP BY u.user_segment, u.ab_test_group
    HAVING COUNT(DISTINCT f.user_dim_key) >= 100
),

-- 공통 CTE 2: PIVOT 역할
pivoted_rates AS (
    SELECT
        user_segment,
        MAX(CASE WHEN ab_test_group = 'treatment' THEN ad_click_conv_rate END) as treatment_rate,
        MAX(CASE WHEN ab_test_group = 'control' THEN ad_click_conv_rate END) as control_rate
    FROM segment_ad_conversion
    GROUP BY user_segment
),

-- 공통 CTE 3: 각 세그먼트별 승자 결정
segment_winners AS (
    SELECT
        user_segment,
        CASE
            WHEN ABS(treatment_rate - control_rate) < 0.1 THEN '무승부'
            WHEN treatment_rate > control_rate THEN 'Treatment'
            ELSE 'Control'
        END as winner
    FROM pivoted_rates
)

-- KPI 1: Treatment 승률
SELECT
    'Treatment 승률' as metric_name,
    CAST(ROUND(COUNT(CASE WHEN winner = 'Treatment' THEN 1 END) * 100.0 / COUNT(*), 1) AS VARCHAR) as metric_value,
    '%' as unit
FROM segment_winners

UNION ALL

-- -- KPI 2: 평균 Lift
-- SELECT
--     '평균 Lift' as metric_name,
--     CAST(ROUND(AVG((treatment_rate - control_rate) / NULLIF(control_rate, 0) * 100), 1) AS VARCHAR) as metric_value,
--     '%' as unit
-- FROM pivoted_rates

-- UNION ALL

-- KPI 3: 총 테스트 사용자 수 (서식 및 비율 추가 - 최종 수정)
SELECT
    '총 테스트 사용자' as metric_name,
    -- format_number와 format 함수로 서식을 명확히 지정하여 문자열 생성
    format_number(test_users) || ' (' || format('%.1f', test_users * 100.0 / total_users) || '%)' as metric_value,
    '명' as unit
FROM (
    SELECT
        CAST(COUNT(DISTINCT f.user_dim_key) AS DOUBLE) AS test_users,
        (SELECT CAST(COUNT(DISTINCT user_sk) AS DOUBLE) FROM gold_analytics.dim_user) AS total_users
    FROM gold_analytics.fact_user_events f
    JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
    JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
    WHERE u.ab_test_group IN ('treatment', 'control')
        AND t.date BETWEEN DATE '2025-08-08' AND DATE '2025-08-22'
) stats

UNION ALL

-- KPI 4: 유의한 차이를 보이는 세그먼트 수
SELECT
    '유의한 세그먼트 수' as metric_name,
    CAST(COUNT(*) AS VARCHAR) as metric_value,
    '개' as unit
FROM (
    SELECT
        user_segment,
        ABS(treatment_rate - control_rate) as conv_diff
    FROM pivoted_rates
) segment_diffs
WHERE conv_diff >= 0.1

UNION ALL

-- KPI 5: AB 테스트 진행 기간
SELECT
    '테스트 기간' as metric_name,
    CAST(MIN(t.date) AS VARCHAR) || ' ~ ' || CAST(MAX(t.date) AS VARCHAR) as metric_value, -- << 여기를 수정했습니다
    '' as unit
FROM gold_analytics.fact_user_events f
JOIN gold_analytics.dim_user u ON f.user_dim_key = u.user_sk
JOIN gold_analytics.dim_time t ON f.time_dim_key = t.time_dim_key
WHERE u.ab_test_group IN ('treatment', 'control')
    AND t.date BETWEEN DATE '2025-08-08' AND DATE '2025-08-22'

UNION ALL

-- KPI 6: Treatment 우세 세그먼트 목록
SELECT
    'Treatment 우세 세그먼트' as metric_name,
    COALESCE(array_join(array_agg(user_segment), ', '), '없음') as metric_value,
    '' as unit
FROM segment_winners
WHERE winner = 'Treatment'

UNION ALL

-- KPI 7: Control 우세 세그먼트 목록
SELECT
    'Control 우세 세그먼트' as metric_name,
    COALESCE(array_join(array_agg(user_segment), ', '), '없음') as metric_value,
    '' as unit
FROM segment_winners
WHERE winner = 'Control';