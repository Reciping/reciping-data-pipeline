# 🥇 Gold Layer 10개 핵심 메트릭 + A/B 테스트 구현 가이드

## 📋 목차
1. [개요](#개요)
2. [10개 핵심 메트릭 설명](#10개-핵심-메트릭-설명)
3. [A/B 테스트 메트릭](#ab-테스트-메트릭)
4. [실행 방법](#실행-방법)
5. [결과 활용](#결과-활용)
6. [비즈니스 인사이트](#비즈니스-인사이트)

## 📊 개요

Silver Layer(`user_events_silver`)를 기반으로 **10개 핵심 비즈니스 메트릭 + A/B 테스트 분석**을 포함한 Gold Layer를 구축합니다.

### 🏗️ 아키텍처
```
🥈 Silver Layer: recipe_analytics.user_events_silver (1,000,001 rows)
           ↓
🥇 Gold Layer: gold_analytics.db/
├── 📐 Dimensions (5개)
│   ├── dim_users          # 사용자 차원
│   ├── dim_time           # 시간 차원
│   ├── dim_recipes        # 레시피 차원
│   ├── dim_pages          # 페이지 차원
│   └── dim_events         # 이벤트 차원
├── 📊 Facts (1개)
│   └── fact_user_events   # 이벤트 팩트
└── 📈 Metrics (12개)
    ├── metrics_active_users               # 1. DAU/WAU/MAU
    ├── metrics_event_type_time_distribution # 2. 이벤트 시간 분포
    ├── metrics_conversion_rate            # 3. 전환율
    ├── metrics_weekly_retention           # 4. 주간 리텐션
    ├── metrics_monthly_retention          # 5. 월간 리텐션
    ├── metrics_stickiness                 # 6. 사용자 고착성
    ├── metrics_funnel                     # 7. 퍼널 분석
    ├── metrics_count_visitors             # 8. 방문자 수
    ├── metrics_ree_segmentation           # 9. REE 세그먼트
    ├── metrics_recipe_performance         # 10. 레시피 성과
    ├── metrics_ab_test_results            # 11. A/B 테스트 결과
    └── metrics_ab_test_cohort             # 12. A/B 테스트 코호트
```

## 🎯 10개 핵심 메트릭 설명

### 1. 📈 ACTIVE_USERS (DAU/WAU/MAU)
**목적**: 일간/주간/월간 활성 사용자 추이 분석
```sql
-- 핵심 필드
date, dau, wau, mau, new_users, returning_users, 
dau_by_segment, dau_growth_rate, dau_7d_avg, dau_30d_avg

-- 비즈니스 활용
- 사용자 증가 추이 모니터링
- 세그먼트별 활성도 비교
- 성장률 트렌드 분석
```

### 2. ⏰ EVENT_TYPE_TIME_DISTRIBUTION
**목적**: 이벤트 유형별 시간대 분포 패턴 분석
```sql
-- 핵심 필드
event_time_month, event_time_day_name, event_time_hour, 
event_type, event_count, unique_users, peak_indicator

-- 비즈니스 활용
- 피크 타임 식별 및 서버 리소스 최적화
- 마케팅 캠페인 최적 시간대 결정
- 사용자 행동 패턴 이해
```

### 3. 📊 CONVERSION_RATE
**목적**: 퍼널 단계별 전환율 분석
```sql
-- 핵심 필드
date, funnel_stage, total_users, converted_users, 
conversion_rate, conversion_by_segment, benchmark_rate

-- 비즈니스 활용
- 퍼널 최적화 포인트 식별
- A/B 테스트 성과 측정
- 사용자 여정 개선
```

### 4. 🔄 WEEKLY_RETENTION
**목적**: 주간 코호트 리텐션 분석
```sql
-- 핵심 필드
cohort_week, retention_week, cohort_size, retained_users, 
retention_rate, retention_by_segment, churn_rate

-- 비즈니스 활용
- 제품 PMF(Product-Market Fit) 측정
- 코호트별 리텐션 비교
- 사용자 이탈 시점 파악
```

### 5. 📅 MONTHLY_RETENTION
**목적**: 월간 장기 리텐션 추이 분석
```sql
-- 핵심 필드
cohort_month, retention_month, cohort_size, retained_users, 
retention_rate, ltv_estimate, churn_probability

-- 비즈니스 활용
- LTV(고객 생애 가치) 추정
- 장기 사용자 충성도 측정
- 월별 코호트 성과 비교
```

### 6. 🎯 STICKINESS
**목적**: 사용자 고착성 및 참여도 분석
```sql
-- 핵심 필드
date, daily_active_users, monthly_active_users, 
stickiness_ratio, avg_sessions_per_user, power_user_ratio

-- 비즈니스 활용
- 제품 중독성 측정 (DAU/MAU 비율)
- 파워유저 비율 트래킹
- 참여도 개선 전략 수립
```

### 7. 🎪 FUNNEL
**목적**: 사용자 여정 퍼널 분석
```sql
-- 핵심 필드
date, funnel_stage, stage_order, total_users, 
conversion_from_previous, cumulative_conversion, drop_off_rate

-- 비즈니스 활용
- 이탈 구간 식별 및 개선
- 퍼널 최적화 우선순위 결정
- 사용자 여정 시각화
```

### 8. 🚶 COUNT_VISITORS
**목적**: 방문자 패턴 및 세션 품질 분석
```sql
-- 핵심 필드
date, total_visitors, unique_visitors, new_visitors, 
returning_visitors, avg_session_duration, bounce_rate

-- 비즈니스 활용
- 트래픽 품질 평가
- 신규/재방문 사용자 비율 모니터링
- 사용자 참여도 측정
```

### 9. 🏆 REE_SEGMENTATION (Recipe RFM)
**목적**: 레시피 도메인 특화 사용자 세그먼테이션
```sql
-- 핵심 필드
user_id, recency_days, engagement_score, expertise_level, 
user_segment, segment_description, recommended_actions

-- 세그먼트 유형
- Recipe_Champions: 최고 등급 사용자
- Loyal_Cooks: 충성 요리사
- New_Food_Lovers: 신규 음식 애호가
- Cooking_Experts: 요리 전문가
- At_Risk_Users: 위험 사용자
- Lost_Users: 이탈 사용자
```

### 10. 🍳 RECIPE_PERFORMANCE
**목적**: 레시피별 성과 및 인기도 분석
```sql
-- 핵심 필드
date, recipe_id, total_views, unique_viewers, 
recipe_saves, engagement_score, trending_score, 
viral_coefficient, recommendation_score

-- 비즈니스 활용
- 인기 레시피 식별 및 추천
- 콘텐츠 성과 최적화
- 바이럴 계수 분석
```

## 🧪 A/B 테스트 메트릭

### 11. AB_TEST_RESULTS
**목적**: A/B 테스트 통계적 유의성 분석
```sql
-- 핵심 필드
test_id, variant_group, metric_value, control_value, 
lift_percentage, statistical_significance, p_value, 
confidence_interval_lower, confidence_interval_upper
```

### 12. AB_TEST_COHORT
**목적**: A/B 테스트 코호트별 장기 효과 분석
```sql
-- 핵심 필드
test_id, variant_group, cohort_week, week_number, 
retention_rate, conversion_rate, revenue_per_user
```

## 🚀 실행 방법

### 1. Docker 환경에서 실행
```bash
# 1. Docker 환경 실행 (기존 Silver Layer 환경)
docker-compose up -d

# 2. 컨테이너 진입
docker exec -it reciping-spark bash

# 3. Gold Layer 파이프라인 실행
python gold_layer_star_schema.py

# 4. A/B 테스트 분석 (선택사항)
python -c "from gold_layer_star_schema import analyze_ab_test_results; analyze_ab_test_results()"

# 5. 대시보드 쿼리 생성 (선택사항)
python -c "from gold_layer_star_schema import generate_business_dashboard_queries; generate_business_dashboard_queries()"
```

### 2. 실행 결과 확인
```sql
-- 생성된 테이블 확인
SHOW TABLES IN iceberg_catalog.recipe_analytics LIKE '*metrics*';

-- 메트릭 데이터 확인
SELECT COUNT(*) FROM iceberg_catalog.recipe_analytics.metrics_active_users;
SELECT COUNT(*) FROM iceberg_catalog.recipe_analytics.metrics_weekly_retention;
SELECT COUNT(*) FROM iceberg_catalog.recipe_analytics.metrics_ree_segmentation;
```

## 📊 결과 활용

### 1. 비즈니스 대시보드 연결
```python
# BI 도구 연결 정보
HOST: localhost
PORT: 9083
TYPE: Hive Metastore
DATABASE: iceberg_catalog.recipe_analytics
```

### 2. 핵심 KPI 모니터링
```sql
-- 일간 핵심 지표 요약
SELECT 
    date,
    dau,
    dau_growth_rate,
    (SELECT AVG(retention_rate) FROM metrics_weekly_retention WHERE retention_week = 1) as week1_retention,
    (SELECT AVG(stickiness_ratio) FROM metrics_stickiness WHERE date = a.date) as stickiness
FROM metrics_active_users a
WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY date DESC;
```

### 3. 세그먼트별 인사이트
```sql
-- REE 세그먼트 분포 및 특성
SELECT 
    user_segment,
    COUNT(*) as user_count,
    AVG(engagement_score) as avg_engagement,
    AVG(expertise_level) as avg_expertise,
    AVG(recency_days) as avg_recency
FROM metrics_ree_segmentation
GROUP BY user_segment
ORDER BY user_count DESC;
```

## 💡 비즈니스 인사이트

### 📈 성장 지표
- **DAU 성장률**: 일간 사용자 증가 추이
- **리텐션 코호트**: 신규 사용자의 장기 정착률
- **Stickiness**: 제품 중독성 (DAU/MAU > 20% 권장)

### 🎯 최적화 포인트
- **퍼널 드롭오프**: 가장 큰 이탈 구간 개선
- **피크 타임**: 서버 리소스 및 마케팅 최적화
- **A/B 테스트**: 통계적 유의성 기반 의사결정

### 🏆 사용자 세그먼테이션
- **Recipe Champions**: VIP 대우 및 베타 기능 제공
- **At Risk Users**: 재참여 캠페인 타겟팅
- **Cooking Experts**: 멘토십 프로그램 운영

### 🍳 콘텐츠 전략
- **인기 레시피**: 홈페이지 추천 알고리즘 반영
- **바이럴 계수**: 공유 기능 개선 우선순위
- **참여도 점수**: 개인화 추천 시스템 가중치

## 🔄 정기 모니터링 주기

| 메트릭 | 모니터링 주기 | 알람 임계값 |
|--------|---------------|-------------|
| DAU | 일간 | 전일 대비 -10% |
| Week 1 Retention | 주간 | 40% 미만 |
| Conversion Rate | 일간 | 전주 대비 -15% |
| Stickiness | 주간 | 15% 미만 |
| A/B Test | 일간 | 통계적 유의성 달성 |

---

## 🎊 결론

이 구현을 통해 **레시피 서비스에 특화된 포괄적인 분석 플랫폼**을 구축할 수 있습니다:

1. **10개 핵심 메트릭**으로 비즈니스 현황 360도 분석
2. **A/B 테스트 자동화**로 데이터 기반 의사결정 지원  
3. **REE 세그먼테이션**으로 레시피 도메인 특화 사용자 분석
4. **실시간 대시보드**로 즉각적인 비즈니스 인사이트 제공

🚀 **Ready to Transform Your Recipe Business with Data!**
