# Reciping Data Engineering Pipeline

## 프로젝트 개요

레시피 검색 서비스 Reciping의 유저 행동 이벤트를 수집·처리·분석하기 위한 End-to-End 데이터 파이프라인입니다.
Apache Iceberg 기반의 Medallion Architecture를 **AWS EC2 Self-Hosted 환경**에서 직접 구축하여
준실시간 이벤트 데이터를 효율적으로 처리하고, 비즈니스 의사결정에 필요한 Data Mart를 제공합니다.

| 항목 | 내용 |
|------|------|
| **개발 기간** | 2025.05 ~ 2025.10 (5개월) |
| **팀 구성** | 6명 (데이터 엔지니어링 기여도 100%) |
| **사용 언어** | Python |
| **핵심 기술** | Kafka · Spark · Airflow · Iceberg · Hive Metastore · Trino · AWS(S3, EC2) |

---

## 핵심 성과 요약

| 항목 | 목표 | 결과 |
|------|------|------|
| **인프라 비용 절감** | AWS 매니지드 서비스 대비 절감 | **75% 절감** (Self-Hosted EC2) |
| **SLO 1 — 파이프라인 지연** | End-to-End 지연 최대 20분 | **달성** (15분 주기 처리) |
| **SLO 2 — 데이터 정합성** | Bronze→Silver 정합성 95% 이상 | **99.31% 달성** (중복 데이터 0.69% 제거) |
| **SLO 3 — I/O 효율** | Daily 쿼리 전체 데이터셋 5% 이내 스캔 | **3.16% 달성** (I/O 비용 96.84% 절감) |

---

## 목차

1. [아키텍처 개요](#아키텍처-개요)
2. [아키텍처 의사결정 (ADR)](#아키텍처-의사결정-adr)
3. [SLO 설계 및 달성 결과](#slo-설계-및-달성-결과)
4. [트러블슈팅](#트러블슈팅)
5. [데이터 모델링](#데이터-모델링)
6. [대시보드](#대시보드)
7. [레포지토리 구조](#레포지토리-구조)
8. [로컬 실행 가이드](#로컬-실행-가이드)
9. [관련 문서](#관련-문서)

---

## 아키텍처 개요

![Architecture Overview](https://i.imgur.com/JO3tpsI.png)

### 데이터 흐름

```
[이벤트 생성] Python Producer (create_event_logs.py)
       ↓
[수집]  Kafka Cluster (3-Node) — replay-user-events Topic
       ↓
[전송]  Kafka Connect (S3 Sink Connector, 2-Node)
       ↓
[적재]  Amazon S3 — Staging Area (Raw JSON)
       ↓
[처리]  Airflow DAG (15분 간격 트리거)
       ↓
[변환]  Spark Jobs (Medallion Architecture)
       ├─ Staging → Bronze  (Raw Iceberg)
       ├─ Bronze  → Silver  (Cleaned & Deduplicated)
       ├─ Create Dimensions (Star Schema)
       └─ Silver  → Gold    (Fact & Dim Tables)
       ↓
[분석]  Trino Query Engine ← Hive Metastore (카탈로그)
       ↓
[시각화] Apache Superset Dashboard
```

### 인프라 구성

모든 데이터 처리 컴포넌트는 **AWS EC2 인스턴스에 직접 설치 및 구성**했습니다.

![EC2 Instances](https://i.imgur.com/vCVd5P1.png)

| EC2 인스턴스 | 타입 | 역할 | 선정 이유 |
|------------|------|------|----------|
| kafka-ec2 × 3 | t3.medium | Kafka 3-Node 클러스터 | 15분 이벤트 스트리밍 처리 및 인프라 균형 |
| kafka-connect-ec2 × 2 | t3.medium | S3 Sink Connector | 안정적인 S3 전송 및 장애 복구 |
| airflow-ec2 | t3.xlarge | Airflow + Spark | Spark OOM 방지를 위한 메모리 최적화 인스턴스 |
| trino-ec2 | t3.large | Trino 쿼리 엔진 | Iceberg 파티션 프루닝 기반 대화형 쿼리 최적화 |
| hive-metastore | t3.micro | Hive Metastore | Iceberg 테이블 카탈로그 및 스키마 정보 저장 |
| bastion-server | t3.micro | SSH 게이트웨이 | 보안 그룹 관리 및 접근 제어 |

### Airflow DAG 구성

**DAG ID**: `replay_september_15min_realtime`
- **실행 간격**: 15분 (`*/15 * * * *`)
- **Catchup**: True (과거 데이터 재처리 지원)
- **병렬 실행**: 1개 (max_active_runs=1, 데이터 순서 보장)

| Task | Operator | 주요 역할 |
|------|----------|----------|
| **staging_to_bronze** | SparkSubmitOperator | S3 Staging Area의 JSON 파일을 Bronze Iceberg 테이블로 적재, 15분 간격 파티션 경로 읽기, 원시 이벤트 문자열 보존 |
| **bronze_to_silver** | SparkSubmitOperator | Bronze의 원시 JSON 파싱 및 Silver 테이블 구조화, 30+ 필드 추출, 중복 제거 및 데이터 검증, 이벤트 발생 시간 기준 파티셔닝 |
| **silver_to_gold** | SparkSubmitOperator | Silver 데이터를 Gold Star Schema로 변환, Dimension 테이블 조인 및 Surrogate Key 매핑, 비즈니스 메트릭 계산(CTR, 참여도 점수), Fact 테이블 증분 적재 |

**의존성**: `staging_to_bronze >> bronze_to_silver >> silver_to_gold`

---

## 아키텍처 의사결정 (ADR)

Self-Hosted 선택 이유, Iceberg 도입 근거, Star Schema 설계 결정 등
상세한 아키텍처 의사결정 기록은 `docs/adr/`에서 확인할 수 있습니다.

- [001. Self-Hosted EC2 선택 — 인프라 비용 75% 절감](docs/adr/001-self-hosted-ec2.md)
- [002. Apache Iceberg Table Format 선택](docs/adr/002-apache-iceberg.md)
- [003. Star Schema 기반 Data Mart 설계](docs/adr/003-star-schema.md)

---

## SLO 설계 및 달성 결과

비즈니스·엔지니어링·운영 3개 관점에서 SLO를 설정하고 전 항목 달성했습니다.

| 관점 | Service Level Objective | SLI | 결과 |
|------|------------------------|-----|------|
| **비즈니스** | End-to-End 데이터 파이프라인 지연 시간 최대 20분 | 지연 시간 | ✅ 달성 |
| **엔지니어링** | Bronze→Silver 변환 데이터 정합성 95% 이상 보장 | 데이터 보존율 | ✅ **99.31%** |
| **운영** | Daily 필터링 분석 쿼리 전체 데이터셋 5% 이내 스캔 | 스캔된 파티션 비율 | ✅ **3.16%** |

### SLO 측정 쿼리 예시 (Trino)

```sql
-- SLO 2: Bronze → Silver 데이터 정합성 측정
SELECT
  'Bronze → Silver' AS transformation,
  (SELECT COUNT(*) FROM iceberg.recipe_analytics.bronze_events_iceberg) AS source_count,
  (SELECT COUNT(*) FROM iceberg.recipe_analytics.user_events_silver)    AS target_count,
  ROUND(
    100.0
    * (SELECT COUNT(*) FROM iceberg.recipe_analytics.user_events_silver)
    / (SELECT COUNT(*) FROM iceberg.recipe_analytics.bronze_events_iceberg),
    2
  ) AS success_rate_percent;
-- 결과: 1,070,312 → 1,062,969 (99.31%)

-- SLO 3: Daily 파티션 스캔 비율 측정 (Trino EXPLAIN)
EXPLAIN
SELECT COUNT(*)
FROM iceberg.recipe_analytics.user_events_silver
WHERE date = DATE '2025-09-01';
-- 스캔된 파티션: 33,598 / 전체: 1,062,969 → 3.16%
```

---

## 트러블슈팅

### 1. Spark Driver / Executor OOM 해결

**증상**
- AWS EC2 단일 노드(t3.xlarge)에서 Airflow + Spark 병행 실행 시 가용 메모리 부족
- Iceberg `MERGE INTO` 연산 시 전체 파일을 읽고 쓰는 과정에서 과도한 Shuffle · I/O 발생 → 빈번한 OOM

**원인 분석**
```
단일 EC2 내 메모리 경합:
  Airflow 프로세스 + Spark Driver + Spark Executor
  → 가용 메모리 한계 초과

Iceberg MERGE INTO:
  전체 파일 대상 Read-Modify-Write → Shuffle 폭증
```

**해결 방법**
```python
# Spark 메모리 설정 최적화
spark_conf = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",
    # Iceberg 벡터화 읽기 비활성화 → 메모리 안정성 우선
    "spark.sql.iceberg.vectorization.enabled": "false",
}

# MERGE INTO 대신 수동 중복 제거 + APPEND 방식으로 전환
# Bronze → Silver: event_id 기준 중복 제거 후 해당 날짜 파티션에 APPEND
silver_df = bronze_df.dropDuplicates(["event_id"])
silver_df.writeTo(self.silver_table).append()
```

**결과**
- OOM 해소 + 시스템 가용 리소스 확보
- Bronze→Silver 정합성 SLO **99.31%** 달성 (중복 데이터 0.69% 제거)

**향후 개선 방향**
- Iceberg `Dynamic Partition Overwrite` 도입
  → APPEND 방식의 재실행 멱등성 미달성 문제 해결 + 메모리 효율 유지

---

### 2. Silver → Gold DAU 5.5배 과다 집계 문제

**증상**
- Silver Layer: 4,538명 / Gold Layer `dim_user`: 25,060명 → **5.5배 불일치**
- 원인: `user_id` 속성 변화에 따라 증분 처리 시 동일 유저가 여러 개의 Surrogate Key로 중복 집계

**해결 방법**
```python
# 기존: 전체 user_dim 테이블을 재생성 (비효율)
# 변경: LEFT ANTI JOIN으로 신규 유저만 필터링하여 APPEND

new_users_df = silver_df.join(
    existing_dim_user,
    on="user_id",
    how="left_anti"   # dim_user에 없는 신규 유저만 선택
)

# Gold Layer dim_user에 신규 유저만 APPEND (기존 데이터 보존)
new_users_df.writeTo(self.dim_user_table).append()
```

**결과**
- user_id당 정확히 1개의 Surrogate Key 생성 → 유저 중복 집계 완전 제거
- 메모리 사용량 감소 (중간 결과 생성 불필요)

---

### 3. Small File 문제 (Gold Layer)

**증상**
- 15분 DAG × 30일 = 2,880회 DagRun + APPEND → 평균 파일 크기 **3.74MB** (권장: 128MB)
- 작은 파일 다수 생성 → Trino 쿼리 응답 속도 저하 우려

**해결 방법**
```sql
-- Iceberg Compaction (rewrite_data_files) 주기적 실행
-- Bin-packing 전략: 작은 파일들을 128MB 단위로 병합
ALTER TABLE iceberg.gold_analytics.fact_user_events
EXECUTE rewrite_data_files(
  strategy => 'binpack',
  options => map('target-file-size-bytes', '134217728')  -- 128MB
);
```

**결과**
- 메타데이터 오버헤드 최소화
- 쿼리 성능 저하 방지 + 비용 최적화

---

## 데이터 모델링

### Event Taxonomy 설계

유저의 서비스 이용 여정(유입→검색→수익화)을 데이터로 포착하기 위한 **순환 구조 이벤트 스키마**를 설계했습니다.

![Event Taxonomy](https://i.imgur.com/hRK3uRO.png)

- **유입 및 로그인**: `view_page` → `click_auth_button` → `auth_success`
- **검색 및 발견**: `search_recipe` → `view_recipe_list`
- **수익화**: `view_ads` → `click_ads`
- **레시피 클릭 및 상호작용**: `click_recipe` → `click_like` / `click_bookmark` / `create_comment`

### Gold Layer — Star Schema ERD

```
DIM_USER ──────────────────┐
DIM_RECIPE ─────────────── FACT_USER_EVENTS ─── DIM_TIME
DIM_PAGE ──────────────────┘         │
DIM_EVENT ───────────────────────────┘
```

| 테이블 | 타입 | 행 수 | 설명 |
|--------|------|-------|------|
| `fact_user_events` | Fact | ~1,280,000 | 모든 유저 행동 이벤트 + Surrogate Keys |
| `dim_user` | Dimension | ~2,000 | 유저 정보 (세그먼트, A/B 그룹) |
| `dim_recipe` | Dimension | ~10,000 | 레시피 메타데이터 |
| `dim_event` | Dimension | ~15 | 이벤트 타입 정보 |
| `dim_page` | Dimension | ~20 | 페이지 정보 |
| `dim_time` | Dimension | ~17,520 | 시간 차원 (년/월/일/시, 요일, 주말 여부) |

### 계층별 스키마

<details>
<summary>Bronze Layer (Raw Zone)</summary>

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| `raw_event_string` | STRING | 원시 JSON 이벤트 문자열 |
| `source_file` | STRING | 소스 파일 경로 |
| `ingestion_timestamp` | TIMESTAMP | 수집 시간 |
| `ingestion_date` | DATE | 수집 날짜 (파티션 키) |

</details>

<details>
<summary>Silver Layer (Cleaned Zone)</summary>

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| `event_id` | STRING | 이벤트 고유 ID (중복 제거 기준) |
| `user_id` | STRING | 유저 ID |
| `anonymous_id` | STRING | 비로그인 유저 식별자 |
| `session_id` | STRING | 세션 ID |
| `event_name` | STRING | 이벤트 타입 |
| `event_time` | TIMESTAMP | 이벤트 발생 시간 |
| `date` | DATE | 이벤트 발생 날짜 (파티션 키) |
| `year` / `month` / `day` | INT | 날짜 파생 컬럼 (디버깅 + Partition Pruning) |
| `recipe_id` | STRING | 레시피 ID |
| `ab_test_group` | STRING | A/B 테스트 그룹 |
| `event_properties` | JSON | 이벤트별 가변 속성 (Schemaless) |
| `... (30+ 필드)` | | |

</details>

<details>
<summary>Gold Layer — Fact Table</summary>

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| `event_id` | STRING | 이벤트 고유 ID |
| `user_dim_key` | BIGINT | dim_user Surrogate Key |
| `time_dim_key` | BIGINT | dim_time Surrogate Key |
| `recipe_dim_key` | BIGINT | dim_recipe Surrogate Key |
| `page_dim_key` | BIGINT | dim_page Surrogate Key |
| `event_dim_key` | BIGINT | dim_event Surrogate Key |
| `event_count` | INT | 이벤트 발생 수 |
| `session_duration_seconds` | BIGINT | 세션 지속 시간 |
| `page_view_duration_seconds` | BIGINT | 페이지 체류 시간 |
| `is_conversion` | BOOLEAN | 전환 여부 |
| `conversion_value` | DOUBLE | 전환 가치 |
| `engagement_score` | DOUBLE | 참여도 점수 |

</details>

---

## 대시보드

프로젝트는 **20개 이상의 사전 정의된 Trino SQL 쿼리**를 제공합니다.
모든 쿼리는 `sql_queries/` 디렉토리에서 확인할 수 있습니다.

### 고객 행동 분석 대시보드

![Superset Dashboard 1](https://i.imgur.com/2LAojEt.jpeg)

### 세부 행동 분석 대시보드

![Superset Dashboard 2](https://i.imgur.com/fkXPtSM.jpeg)

### 대시보드 기술 스택

- **데이터 소스**: Trino (Iceberg Catalog 연동)
- **시각화**: Apache Superset
- **실시간성**: 15분 간격 데이터 업데이트 (Airflow DAG 실행 후)

### 주요 인사이트

1. **피크 시간대**: 저녁 6~9시 (퇴근 후 저녁 준비)
2. **주말 효과**: 토요일 DAU 30% 증가
3. **A/B 테스트 결과**: Treatment 그룹 평균 22% CTR 개선
4. **세그먼트 특성**: FEMALE_40_PLUS가 가장 활성화된 세그먼트 (35.6%)
5. **레시피 선호도**: 베이킹/디저트 카테고리 인기

---

## 레포지토리 구조

```
reciping-data-pipeline/
├── spark/
│   ├── bulk_insert_jobs/          # 대규모 배치 처리 (8월 데이터 Bulk Insert)
│   │   ├── bulk_runner.py         # ELT 프로세스 순차 실행
│   │   ├── staging_to_bronze_iceberg.py
│   │   ├── bronze_to_silver_iceberg.py
│   │   ├── create_dims.py         # 5개 Dimension 테이블 생성
│   │   └── silver_to_gold_processor.py
│   │
│   └── replay_jobs/               # 준실시간 증분 처리 (9월, 15분 간격)
│       ├── replay_staging_to_bronze.py
│       ├── replay_bronze_to_silver.py
│       └── replay_silver_to_gold.py
│
├── dags/
│   └── replay_september_15min_dag.py  # Airflow DAG (15분 스케줄링)
│
├── create_data/                   # 유저 행동 이벤트 로그 생성 (Kafka Producer)
│   ├── create_event_logs.py
│   ├── replay_fast_sequential_producer.sh
│   └── README.md
│
├── sql_queries/                   # Trino 분석 쿼리 모음 (20개+)
│   ├── README.md
│   ├── DAU.sql
│   ├── WAU.sql
│   ├── ab_test_kpi_metric.sql
│   ├── daily_engagement_score.sql
│   └── ... (20개+ 쿼리)
│
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 로컬 실행 가이드

> 이 프로젝트는 AWS EC2 환경에서 운영되도록 설계되었습니다.
> 아래는 코드 탐색 및 로컬 테스트를 위한 환경 설정입니다.

### 1. 저장소 클론

```bash
git clone https://github.com/Reciping/reciping-data-pipeline.git
cd reciping-data-pipeline
```

### 2. Python 의존성 설치

```bash
# Poetry 사용 (권장)
poetry install --without datagen   # 파이프라인 실행 환경만
poetry install --with datagen      # 데이터 생성 도구 포함

# pip 사용
pip install -r requirements.txt
```

### 3. AWS 자격 증명 설정

```bash
aws configure
# 또는
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-northeast-2
```

### 4. 대규모 데이터 배치 처리 (벌크 로드)

```bash
cd spark/bulk_insert_jobs
python bulk_runner.py
```

**처리 단계**:
1. Staging → Bronze: 원시 JSON 파일을 Iceberg 테이블로 변환
2. Bronze → Silver: 파싱 및 중복 제거
3. Create Dimensions: 차원 테이블 생성
4. Silver → Gold: Star Schema로 변환

**설정 변경** (`bulk_runner.py`):
```python
BULK_INPUT_FILE = "dask_events_1m.jsonl"  # 처리할 파일명
TARGET_DATE = "2025-08-31"                # 대상 날짜
```

### 5. 준실시간 데이터 증분 처리 (Spark Job 수동 실행)

```bash
cd spark/replay_jobs
spark-submit replay_staging_to_bronze.py \
    --data-interval-start "2025-09-01T00:00:00+09:00" \
    --data-interval-end   "2025-09-01T00:15:00+09:00" \
    --test-mode false
```

### 6. 쿼리 엔진 (Trino)

```bash
# Trino 접속 (catalog 옵션으로 Iceberg 지정)
./trino-cli --server http://localhost:8080 --catalog iceberg
```

---

## 관련 문서

- **데이터 생성 상세**: [create_data/README.md](create_data/README.md)
  - 9가지 현실적인 행동 패턴 시뮬레이션
  - A/B 테스트 구현 상세
  - Dask 분산처리 최적화
  - 데이터 생성 명령어 및 트러블슈팅

- **SQL 쿼리 카탈로그**: [sql_queries/README.md](sql_queries/README.md)
  - 20개 이상의 분석 쿼리 전체 설명
  - Trino 접속 및 실행 방법
  - Iceberg Time Travel 상세
  - 쿼리 성능 최적화 팁
