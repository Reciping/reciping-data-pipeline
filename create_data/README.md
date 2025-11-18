# 합성 사용자 행동 데이터 생성

## 개요

실제 사용자 데이터 없이 Python으로 현실적인 행동 패턴을 시뮬레이션하여 대규모 이벤트 로그를 생성합니다.

### 핵심 기술

**Dask 분산처리 프레임워크**
- 100만 건 이상의 이벤트 로그를 효율적으로 생성
- 사용자를 2,000명씩 배치로 나누고 4개 워커 프로세스에서 병렬 처리
- **순차 처리 대비 3-4배 빠른 생성 속도** 달성

---

## 현실적인 사용자 행동 패턴 시뮬레이션

실제 고객 행동과 유사한 데이터를 생성하기 위해 다음 요소들을 구현했습니다:

### 1. 시간대별 활동 패턴 (Time-of-Day Patterns)

사용자의 하루 일과에 따른 활동 가중치를 적용합니다.

```python
hour_weights = {
    0: 0.05, 1: 0.03, 2: 0.02, 3: 0.02, 4: 0.03, 5: 0.08,  # 새벽 (2-8%)
    6: 0.25, 7: 0.45, 8: 0.60, 9: 0.75, 10: 0.85, 11: 0.90,  # 오전 (25-90%)
    12: 1.00, 13: 0.95, 14: 0.85, 15: 0.80, 16: 0.85, 17: 0.90,  # 오후 (80-100%)
    18: 0.95, 19: 1.00, 20: 0.95, 21: 0.85, 22: 0.70, 23: 0.35   # 저녁~밤 (35-100%)
}
```

- **새벽 시간대 (0-5시)**: 2-8% 낮은 활동
- **오전 출근 시간 (6-11시)**: 25-90% 점진적 증가
- **점심 및 오후 (12-17시)**: 80-100% 최고 활동
- **저녁 퇴근 후 (18-21시)**: 85-100% 레시피 검색 피크
- **심야 (22-23시)**: 35-70% 점진적 감소

### 2. 요일별 활동 패턴 (Day-of-Week Patterns)

주중과 주말의 차이를 반영합니다.

```python
weekday_weights = {
    0: 0.8,   # 월요일: 80%
    1: 0.9,   # 화요일: 90%
    2: 0.95,  # 수요일: 95%
    3: 1.0,   # 목요일: 100%
    4: 1.1,   # 금요일: 110% (주말 준비)
    5: 1.3,   # 토요일: 130% (주말 피크)
    6: 1.2    # 일요일: 120% (여유로운 주말)
}
```

- **주중 (월-목)**: 80-100% 기본 활동
- **금요일**: 110% 주말 준비 증가
- **주말 (토-일)**: 120-130% 최대 피크 (요리 활동 증가)
- **주말 보너스**: 이벤트 수 10-20% 추가

### 3. 사용자 세그먼트별 행동 차이 (User Segmentation)

활동 수준에 따라 3개 그룹으로 분류합니다.

| 세그먼트 | 비율 | 일평균 이벤트 | 특징 |
|---------|------|--------------|------|
| **POWER_USER** | 10% | 40-50개 | 레시피 작성/댓글 등 고기여도 활동 |
| **ACTIVE_EXPLORER** | 60% | 15-20개 | 검색/필터 등 적극적 탐색 |
| **PASSIVE_BROWSER** | 30% | 5-10개 | 추천 목록 위주 가벼운 소비 |

### 4. 인구통계 세그먼트 (Demographic Segments)

성별 × 연령대 조합으로 6개 세그먼트를 생성합니다.

```python
DEMOGRAPHIC_DISTRIBUTION = {
    'FEMALE_20S': 0.142,    # 14.2%
    'FEMALE_30S': 0.207,    # 20.7%
    'FEMALE_40_PLUS': 0.356, # 35.6%
    'MALE_20S': 0.062,      # 6.2%
    'MALE_30S': 0.085,      # 8.5%
    'MALE_40_PLUS': 0.148   # 14.8%
}
```

실제 레시피 서비스 사용자 분포를 반영했습니다.

### 5. 요리 스타일 페르소나 (Cooking Style Personas)

사용자의 요리 선호도를 5가지 페르소나로 분류합니다.

| 페르소나 | 비율 | 특징 |
|---------|------|------|
| **DESSERT_FOCUSED** | 20% | 베이킹·디저트 제작 선호 |
| **HEALTHY_CONSCIOUS** | 25% | 다이어트·웰빙 요리 선호 |
| **COMFORT_FOOD** | 25% | 든든한 메인 요리 선호 |
| **QUICK_CONVENIENT** | 20% | 간편·시간절약 요리 선호 |
| **DIVERSE_EXPLORER** | 10% | 특정 패턴 없이 다양한 탐색 |

### 6. 자연스러운 이벤트 시퀀스 (Event Flow Logic)

`EVENT_SCHEMA` 기반으로 실제 사용자 여정을 구현합니다.

**이벤트 플로우 예시**:
```
view_page → search_recipe → view_recipe_list → click_recipe → click_bookmark
```

**구현 특징**:
- 각 이벤트 후 다음 이벤트 확률 분포 적용
- 이벤트 간 시간 간격: 5초 ~ 2분 랜덤 분산
- 실제 사용자 행동과 유사한 전환율 적용

### 7. 컨텍스트 기반 행동 연결 (Context-Aware Behavior)

이전 행동이 다음 행동에 영향을 줍니다.

- **검색 필터가 레시피 목록에 반영**: 요리 타입, 재료, 조리법 등
- **목록에 표시된 레시피 중에서 클릭**: 순위 정보 포함
- **클릭한 레시피에 대해 인터랙션**: 북마크/좋아요/댓글 작성
- **광고 노출 후 클릭 여부 결정**: CTR 기반 확률 적용

### 8. A/B 테스트 시뮬레이션

행동 타겟팅 광고의 효과를 측정하는 A/B 테스트를 구현했습니다.

**테스트 설정**:
- **기간**: 2025년 8월 8일 ~ 8월 22일 (2주)
- **시나리오**: `BEHAVIORAL_TARGETING_MVP_V1`
- **가설**: 사용자 행동 패턴 기반 광고 타겟팅이 랜덤 광고보다 효과적이다

**그룹 구성**:
- **Control Group** (50%): 기존 랜덤 광고 서빙 (CTR 1.8%)
- **Treatment Group** (50%): 행동 태그 기반 타겟팅 광고 (CTR 2.2%)

**사용자 할당**:
- `user_id`의 MD5 해시값 기반으로 일관되게 할당
- 동일 사용자는 항상 같은 그룹에 속함

**세그먼트별 차등 목표 CTR**:

```python
AB_TEST_SEGMENT_TARGETS = {
    ('FEMALE_30S', 'POWER_USER', 'DESSERT_FOCUSED'): {
        'control': 0.021,   # 2.1% CTR
        'treatment': 0.028  # 2.8% CTR (+33% 개선)
    },
    ('MALE_20S', 'ACTIVE_EXPLORER', 'QUICK_CONVENIENT'): {
        'control': 0.015,   # 1.5% CTR
        'treatment': 0.019  # 1.9% CTR (+27% 개선)
    },
    ('FEMALE_40_PLUS', 'ACTIVE_EXPLORER', 'HEALTHY_CONSCIOUS'): {
        'control': 0.018,   # 1.8% CTR
        'treatment': 0.023  # 2.3% CTR (+28% 개선)
    }
}
```

**A/B 테스트 데이터 구조**:

```json
{
  "user_id": "user_12345",
  "ab_test_group": "treatment",
  "ab_test_scenario": "BEHAVIORAL_TARGETING_MVP_V1",
  "event_type": "ad_click",
  "event_time": "2025-08-15T14:23:45.123+09:00",
  "demographic_segment": "FEMALE_30S",
  "activity_segment": "POWER_USER",
  "cooking_style_persona": "DESSERT_FOCUSED",
  "ad_targeting_method": "behavioral_targeting",
  "targeting_tags": ["dessert_lover", "baking_tools", "sweet_ingredients"],
  "personalization_score": 0.87
}
```

### 9. 실제 레시피 데이터 연동

S3에 저장된 실제 레시피 메타데이터를 활용합니다.

**데이터 소스**:
```python
# S3에서 Parquet 파일 읽기
recipes_df = pd.read_parquet('s3://reciping-user-event-logs/meta-data/total_recipes.parquet')
users_df = pd.read_parquet('s3://reciping-user-event-logs/meta-data/user.parquet')
profiles_df = pd.read_parquet('s3://reciping-user-event-logs/meta-data/user_profiles.parquet')
```

**활용 방법**:
- 1만개 이상의 실제 레시피 메타데이터 로드
- 레시피 속성 기반 필터링 (`dish_type`, `ingredient_type`, `method_type` 등)
- 사용자 페르소나에 맞는 레시피 추천 시뮬레이션
- 실제 재료 리스트, 조리 시간, 난이도 등 반영

---

## 데이터 생성 전략

### 6~8월: 대규모 배치 데이터 (Bulk Insert)

**목적**: 과거 데이터 백필 (Backfill)

- **기간**: 2025년 6월 1일 ~ 8월 31일 (3개월)
- **방식**: Dask로 대량 JSONL 파일 생성 후 일괄 처리
- **규모**: 약 **100만 건**의 이벤트
- **처리**: `bulk_runner.py`로 한 번에 Bronze → Silver → Gold
- **특징**: 
  - 시간대/요일별 가중치 적용
  - A/B 테스트 기간 (8/8-8/22) 포함
  - 성숙한 서비스 트래픽 시뮬레이션

### 9월: 실시간 스트리밍 데이터

**목적**: 실시간 파이프라인 검증

- **기간**: 2025년 9월 1일 ~ 9월 30일 (1개월)
- **방식**: Kafka Producer로 15분 간격 실시간 전송
- **규모**: 약 **30만 건**의 이벤트 (15분당 1만 건)
- **처리**: Airflow DAG가 15분마다 증분 처리
- **특징**:
  - Kafka Connect → S3 → Iceberg 파이프라인
  - 15분 윈도우 정확한 시간 경계 보장
  - 순차적 데이터 생성 (중복 방지)

---

## 사용법

### 환경 설정

#### 1. 의존성 설치

```bash
# Poetry로 datagen 그룹 포함 설치
poetry install --with datagen

# 또는 pip로 설치
pip install dask distributed kafka-python faker
```

#### 2. AWS 자격 증명 설정

S3에서 레시피 메타데이터를 읽기 위해 필요합니다.

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-northeast-2
```

### 데이터 생성 실행

#### 9월 실시간 스트리밍 데이터 생성

**기본 사용법**:

```bash
cd create_data
python create_event_logs.py \
    --start-date 2025-09-01-00-00 \
    --num-intervals 96 \
    --topic replay-user-events
```

**파라미터 설명**:
- `--start-date`: 시작 날짜 및 시간 (형식: `YYYY-MM-DD-HH-MM` 또는 `YYYY-MM-DD-HH`)
- `--num-intervals`: 생성할 15분 간격 수 (96 = 24시간, 2880 = 30일)
- `--topic`: Kafka 토픽 이름 (기본값: `replay-user-events`)

**예시: 9월 1일 전체 생성 (24시간)**

```bash
python create_event_logs.py \
    --start-date 2025-09-01-00-00 \
    --num-intervals 96 \
    --topic replay-user-events
```

**예시: 특정 시간대만 생성 (1시간)**

```bash
python create_event_logs.py \
    --start-date 2025-09-15-14-00 \
    --num-intervals 4 \
    --topic replay-user-events
```

#### 자동화 스크립트

30일치 데이터를 순차적으로 생성합니다.

```bash
bash replay_fast_sequential_producer.sh
```

**스크립트 내용** (`replay_fast_sequential_producer.sh`):
```bash
#!/bin/bash

# 9월 1일부터 30일까지 순차 실행
for day in {1..30}; do
    date_str=$(date -d "2025-09-${day}" +%Y-%m-%d)
    echo "생성 중: ${date_str}"
    
    python create_event_logs.py \
        --start-date ${date_str}-00-00 \
        --num-intervals 96 \
        --topic replay-user-events
    
    echo "완료: ${date_str}"
    sleep 5  # Kafka 안정화 대기
done
```

---

## 생성된 데이터 검증

### Kafka 메시지 확인

```bash
# Kafka Consumer로 토픽 확인
kafka-console-consumer.sh \
    --bootstrap-server 10.0.128.56:9092 \
    --topic replay-user-events \
    --from-beginning \
    --max-messages 10
```

### 이벤트 데이터 샘플

**생성된 JSONL 메시지 예시**:

```json
{
  "event_name": "view_page",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "user_1234",
  "anonymous_id": "anon_5678",
  "session_id": "session_9012",
  "timestamp": "2025-09-01T14:23:45.123+09:00",
  "context": "{\"page\": {\"name\": \"main\", \"url\": \"https://reciping.co.kr/main\"}, \"user_segment\": \"FEMALE_30S\", \"activity_level\": \"ACTIVE_EXPLORER\", \"cooking_style\": \"DESSERT_FOCUSED\"}",
  "event_properties": "{\"page_name\": \"main\", \"referrer\": \"https://google.com\"}"
}
```

### S3 확인

Kafka Connect가 S3에 저장한 파일을 확인합니다.

```bash
# S3 경로 확인
aws s3 ls s3://reciping-user-event-logs/staging_area/replay-user-events/ --recursive

# 예시 경로 구조
# s3://reciping-user-event-logs/staging_area/replay-user-events/
#   └── year=2025/month=09/day=01/hour=00/minute=00/
#       ├── replay-user-events+0+0000000000.json
#       └── replay-user-events+0+0000000001.json
```

---

## 성능 최적화

### Dask 분산처리 설정

`create_event_logs.py`에서 사용하는 Dask 클라이언트 설정:

```python
from dask.distributed import Client

# 로컬 클러스터 시작
client = Client(
    processes=True,      # 프로세스 기반 병렬화
    n_workers=4,         # 워커 수
    threads_per_worker=2,  # 워커당 스레드 수
    memory_limit='2GB'   # 워커당 메모리 제한
)

print(f"Dask Dashboard: {client.dashboard_link}")
```

**성능 지표**:
- **처리 속도**: 약 15,000 events/sec
- **100만 이벤트 생성 시간**: 약 60-70초
- **메모리 사용량**: 워커당 1.5-2GB

### 배치 크기 조정

사용자 수에 따라 배치 크기를 조정할 수 있습니다.

```python
# create_event_logs.py 내부
BATCH_SIZE = 2_000  # 사용자 수 (기본값)

# 메모리가 충분하면 증가
BATCH_SIZE = 5_000  # 처리 속도 향상

# 메모리가 부족하면 감소
BATCH_SIZE = 1_000  # 안정성 우선
```

---

## 참고 자료

- **메인 프로젝트 문서**: [../README.md](../README.md)
- **분석 쿼리**: [../sql_queries/README.md](../sql_queries/README.md)
- **Dask 공식 문서**: https://docs.dask.org/
- **Kafka Python 문서**: https://kafka-python.readthedocs.io/
