# SQL 분석 쿼리 카탈로그

## 개요

이 디렉토리에는 **20개 이상의 사전 정의된 Trino SQL 쿼리**가 포함되어 있습니다. <br>각 쿼리는 레시피 서비스의 사용자 행동 데이터를 분석하여 비즈니스 인사이트를 도출하는 데 사용됩니다.

---

## Trino 접속

### Trino CLI 설치

```bash
# Trino CLI 다운로드 (최초 1회)
wget https://repo1.maven.org/maven2/io/trino/trino-cli/latest/trino-cli-latest-executable.jar
mv trino-cli-latest-executable.jar trino-cli
chmod +x trino-cli
```

### Iceberg 카탈로그 연결

```bash
# Trino 접속 (catalog 옵션으로 Iceberg 지정)
./trino-cli --server http://localhost:8080 --catalog iceberg
```

### 기본 탐색 명령어

```sql
-- 사용 가능한 데이터베이스 확인
SHOW SCHEMAS;

-- 데이터베이스 선택
USE recipe_analytics;

-- 테이블 목록 확인
SHOW TABLES;

-- 테이블 스키마 확인
DESCRIBE user_events_silver;

-- 샘플 데이터 확인
SELECT * FROM user_events_silver LIMIT 10;
```

---

### 주요 분석 카테고리

#### 1. 사용자 활동 지표
- `DAU.sql`: 일간 활성 사용자 (Daily Active Users)
- `WAU.sql`: 주간 활성 사용자 (Weekly Active Users)
- `event_distribution.sql`: 이벤트 타입별 분포
- `events_per_time.sql`: 시간대별 이벤트 발생량
- `time_x_day_events_heatmap.sql`: 요일 × 시간대 히트맵

#### 2. A/B 테스트 분석
- `ab_test_kpi_metric.sql`: Control vs Treatment 그룹 CTR 비교
- `treatment_vs_control.sql`: 세그먼트별 전환율 분석
- `lift.sql`: 통계적 유의성 검증 및 Lift 계산
- `ab_test_heatmap.sql`: 시간대별 그룹별 성과
- `daily_ab_test.sql`: 일별 A/B 테스트 추이

#### 3. 레시피 분석
- `top_10_clicked_recipe.sql`: 가장 많이 클릭된 레시피 순위
- `top_10_bookmarked_recipe.sql`: 가장 많이 북마크된 레시피
- `interactions_per_dish_type.sql`: 요리 타입별 인터랙션
- `top_10_interactions_per_events.sql`: 이벤트별 상위 인터랙션

#### 4. 참여도 분석
- `daily_engagement_score.sql`: 일별 사용자 참여도 점수
- `pages_views.sql`: 페이지별 조회수 및 체류 시간

#### 5. 전환 퍼널 분석
- `daily_search_to_recipe_click.sql`: 검색 → 레시피 클릭 전환율
- `click_ads_funnel.sql`: 광고 클릭 퍼널 분석
- `ctr_advertisement.sql`: 광고 클릭률 (CTR) 계산

---

## 참고 자료

- **메인 프로젝트 문서**: [../README.md](../README.md)
- **데이터 생성**: [../create_data/README.md](../create_data/README.md)
<!-- - **Trino 공식 문서**: https://trino.io/docs/current/
- **Apache Iceberg 문서**: https://iceberg.apache.org/docs/latest/ -->
