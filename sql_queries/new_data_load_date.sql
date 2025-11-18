-- SELECT 
--     created_at
--     date_trunc('day', created_at) + INTERVAL, '1' DAY - INTERVAL '1' SECOND AS end_of_day
-- FROM gold_analytics.fact_user_events f
-- ORDER BY created_at DESC
-- LIMIT 1;

-- SELECT
--     created_at,
--     CASE
--         WHEN MONTH(created_at) < 9 THEN  -- created_at의 월(Month)이 9월 미만이면 (1월~8월)
--             date_trunc('day', created_at) + INTERVAL '1' DAY - INTERVAL '1' SECOND -- 해당 날짜의 마지막 시간(23:59:59)으로 변경
--         ELSE                             -- 그렇지 않으면 (9월이거나 그 이후이면)
--             created_at                   -- 원래 시간 그대로 표시
--     END AS modified_date
-- FROM gold_analytics.fact_user_events f
-- ORDER BY created_at DESC
-- LIMIT 1;

SELECT
    created_at AT TIME ZONE 'Asia/Seoul' as kst_time
    -- CASE
    --     WHEN MONTH(created_at) < 9 THEN  -- created_at의 월(Month)이 9월 미만이면 (1월~8월)
    --         date_trunc('day', created_at) + INTERVAL '1' DAY - INTERVAL '1' SECOND -- 해당 날짜의 마지막 시간(23:59:59)으로 변경
    --     ELSE                             -- 그렇지 않으면 (9월이거나 그 이후이면)
    --         created_at                   -- 원래 시간 그대로 표시
    -- END AS modified_date
FROM gold_analytics.fact_user_events f
ORDER BY created_at DESC
LIMIT 1;