# Reciping Data Engineering Pipeline

## 프로젝트 개요

레시피 검색 서비스 Reciping의 유저 행동 이벤트를 수집, 처리, 분석하기 위한 End-to-End 데이터 파이프라인입니다. <br>Apache Iceberg 기반의 Lakehouse 아키텍처를 **AWS EC2 클라우드 환경**에서 구축하여 대규모 이벤트 데이터를 효율적으로 처리합니다.
- 프로젝트 기간: 2025.05 ~ 2025.10
- 담당 역할: 데이터 엔지니어링 (기여도 100%)

### 주요 특징

| 특징 | 설명 |
|------|------|
| **AWS EC2 클라우드 인프라** | 완전한 클라우드 네이티브 환경에서 운영 |
| **Medallion Architecture** | Bronze → Silver → Gold 3계층 데이터 레이어 |
| **Apache Iceberg** | 스키마 진화, Time Travel, ACID 트랜잭션, Partition Pruning 지원 |
| **실제 유저 행동 시뮬레이션** | Python으로 가상의 유저 행동 로그 데이터 생성<br>8월: 대규모 배치 처리 / 9월: 준실시간 스트리밍 |
| **하이브리드 데이터 처리** | 배치 처리(Bulk Insert) + 준실시간 스트리밍(Kafka) |
| **데이터 품질 관리** | 중복 제거, 스키마 검증, 파티셔닝 전략 |
<!-- - **A/B 테스트 구현**: 행동 타겟팅 광고 효과 측정 (Control vs Treatment) -->

---

## 파이프라인 아키텍처

<!-- ![Architecture Overview](https://i.imgur.com/lrm8TTn.png) -->
![Architecture Overview](https://i.imgur.com/JO3tpsI.png)

### AWS EC2 클라우드 인프라 구성

프로젝트는 AWS EC2 기반의 클라우드 환경에서 구축되었습니다.

![EC2](https://i.imgur.com/vCVd5P1.png)

#### 인프라 컴포넌트

모든 데이터 처리 컴포넌트는 **AWS EC2 인스턴스에 직접 설치 및 구성**했습니다. <br>이러한 구성을 통해 **End-to-End 클라우드 네이티브 데이터 레이크하우스 아키텍처**를 구축했습니다.

| EC2 인스턴스 / 서비스 | 인스턴스 타입 | 설치 컴포넌트 | 주요 역할 |
|---------------------|--------------|--------------|----------|
| **kafka-test-ec2** | c5.xlarge | Python 이벤트 생성기<br>(Kafka Producer) | 유저 행동 이벤트 로그 데이터 생성<br>Kafka Producer로 replay-user-events 토픽 전송|
| **reciping-airflow-ec2** | t3.medium | Apache Airflow<br>Apache Spark | Workflow 오케스트레이션 (15분 간격 DAG 스케줄링)<br>Spark Client 모드로 대용량 데이터 처리 |
| **reciping-bastion-server** | t3.micro | - | SSH 접근 게이트웨이<br>보안 그룹 관리 및 접근 제어 |
| **reciping-hive-metastore** | t3.micro | Hive Metastore<br>(PostgreSQL RDS 백엔드) | Iceberg 카탈로그 메타데이터 관리<br>Thrift 서비스 (포트 9083) |
| **reciping-kafka-ec2-01, 02, 03** | t3.medium × 3 | Apache Kafka | 3노드 Kafka 클러스터<br>준실시간 이벤트 로그 스트리밍 |
| **reciping-kafka-connect-ec2-01, 02** | t3.medium × 2 | Kafka Connect<br>(S3 Sink Connector) | 준실시간 이벤트 데이터 S3 전송 |
| **reciping-trino-query-engine** | t3.large | Trino | Iceberg 테이블 분석 쿼리 엔진<br>BI 도구 연동 (Apache Superset) |
| **Amazon S3** | S3 버킷 | - | 데이터 레이크하우스 스토리지<br>버킷명: reciping-user-event-logs |



### 데이터 흐름

```
Python 이벤트 생성기 (create_event_logs.py)
    ↓
Kafka Cluster (User Events Topic)
    ↓
Kafka Connect (S3 Sink Connector)
    ↓
S3 Staging Area (JSON 파일)
    ↓
Airflow DAG (15분 간격 트리거)
    ↓
Spark Jobs (EC2)
    ├─ Staging → Bronze (Raw Iceberg)
    ├─ Bronze → Silver (Cleaned & Deduplicated)
    ├─ Create Dimensions (Star Schema)
    └─ Silver → Gold (Fact & Dim Tables)
    ↓
Trino Query Engine
    ↓
Analytics & Visualization (Apache Superset)
```
<!-- 
### 계층별 역할

#### Bronze Layer (Raw Zone)
- **역할**: 원시 데이터 저장소 (Raw Data Lake)
- **포맷**: JSON 문자열 형태의 이벤트 로그
- **데이터 규모**: 
  - 6~8월 배치: 약 **1,000,000 rows**
  - 9월 스트리밍: 약 **300,000 rows** (15분 간격 증분)
  - 총 약 **1,300,000 rows**
- **특징**:
  - 데이터 손실 방지를 위한 완전한 원본 보존 (Immutable)
  - 파티션: `ingestion_date` 기준 (날짜별 분할)
  - 스키마: `raw_event_string`, `source_file`, `ingestion_timestamp`, `ingestion_date`
  - 데이터 품질 검증 이전 단계
  - S3 JSON 파일을 Iceberg 포맷으로 변환

#### Silver Layer (Cleaned Zone)
- **역할**: 정제된 분석 준비 데이터 (Cleaned & Structured)
- **포맷**: 구조화된 컬럼 (파싱 완료)
- **데이터 규모**: 
  - 중복 제거 후 약 **1,280,000 rows** (중복률 ~1.5%)
  - 30개 이상의 구조화된 컬럼
- **특징**:
  - JSON 파싱 및 스키마 적용 (20+ 필드 추출)
  - **중복 제거**: `event_id` 기준으로 완전 중복 제거
  - **데이터 검증**: NULL 값 처리, 타입 검증, 범위 체크
  - 파티션: `event_date` (실제 이벤트 발생 일자 기준)
  - 타입 변환 및 데이터 정규화 (TIMESTAMP, DATE 등)
  - Bronze → Silver 변환 시 데이터 품질 메트릭 수집

#### Gold Layer (Business Zone)
- **역할**: 비즈니스 로직이 적용된 분석용 데이터 (Analytics-Ready)
- **포맷**: Star Schema (Fact + Dimension Tables)
- **데이터 규모**:
  - **Fact Table**: 약 **1,280,000 rows** (Silver와 동일, 조인 키 추가)
  - **Dimension Tables**: 총 **5개 테이블**
    - `dim_user`: 약 **2,000 rows** (고유 유저)
    - `dim_recipe`: 약 **10,000 rows** (레시피 마스터)
    - `dim_event`: 약 **15 rows** (이벤트 타입)
    - `dim_page`: 약 **20 rows** (페이지 정보)
    - `dim_time`: 약 **17,520 rows** (2025-2026년 시간별)
- **특징**:
  - **Fact Table**: `fact_user_events` - 모든 유저 행동 이벤트 + Surrogate Keys
  - **Dimension Tables**: 
    - `dim_user` - 유저 정보 (user_id, 인구통계 세그먼트, A/B 테스트 그룹, 활동 세그먼트)
    - `dim_recipe` - 레시피 메타데이터 (요리 타입, 재료, 조리법, 난이도, 시간)
    - `dim_event` - 이벤트 타입 정보 (event_name, category)
    - `dim_page` - 페이지 정보 (page_name, page_url)
    - `dim_time` - 시간 차원 (년/월/일/시, 요일, 주말 여부)
  - 파티션: `event_date`
  - 비즈니스 메트릭: CTR, 참여도 점수, 전환율 등
  - Surrogate Key (SK)로 각 차원 테이블 조인 최적화
  - SCD Type 1 적용 (유저 정보 최신 상태 유지) -->

---

## 레포지토리 구조

```
reciping-data-pipeline/
├── spark/                                  # Apache Spark 작업 디렉토리
│   ├── bulk_insert_jobs/                   # 8월 대규모 배치 데이터 처리 (Bulk Insert)
│   │   ├── bulk_runner.py                  # 벌크 데이터 ELT 프로세스 순차적 실행
│   │   ├── staging_to_bronze_iceberg.py
│   │   ├── bronze_to_silver_iceberg.py
│   │   ├── create_dims.py                  # 5개 Dimension 테이블 생성
│   │   └── silver_to_gold_processor.py
│   │
│   └── replay_jobs/                        # 9월 준실시간 스트리밍 처리 (15분 간격)
│       ├── replay_staging_to_bronze.py
│       ├── replay_bronze_to_silver.py
│       └── replay_silver_to_gold.py
│
├── dags/                                   # Airflow DAG 정의
│   └── replay_september_15min_dag.py       # 9월 증분 처리 DAG
│
├── create_data/                            # 유저 행동 이벤트 로그 데이터 생성
│   ├── create_event_logs.py                # Python 이벤트 로그 생성기 (Kafka Producer)
│   ├── replay_fast_sequential_producer.sh  # 데이터 생성 및 kafka topic으로 데이터 전송 스크립트
│   └── README.md                           # 데이터 생성 상세 문서
│
├── sql_queries/                            # Trino 분석 쿼리 모음 (20+)
│   ├── README.md                           # SQL 쿼리 설명
│   ├── DAU.sql                             # 일간 활성 유저
│   ├── WAU.sql                             # 주간 활성 유저
│   ├── ab_test_kpi_metric.sql              # A/B 테스트 전환율 분석
│   ├── daily_engagement_score.sql
│   └── ... (20+ 쿼리)
│
├── pyproject.toml                          # Poetry 의존성 관리
├── requirements.txt                        # Python 패키지 목록
└── README.md                               # 프로젝트 README 문서
```

---

## 시작하기

### 요구사항

해당 프로젝트는 **AWS EC2 클라우드 환경**에서 실행되도록 설계되었습니다.

### 설치

#### 1. 저장소 클론

```bash
git clone https://github.com/Reciping/reciping-data-pipeline.git
cd reciping-data-pipeline
```

#### 2. Python 의존성 설치

**Poetry 사용 (권장)**:
```bash
poetry install --without datagen
```

**pip 사용**:
```bash
pip install -r requirements.txt
```

#### 3. AWS 자격 증명 설정

```bash
aws configure
# 또는
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-northeast-2
```

---


## 사용법

### 1. 대규모 데이터 배치 처리 (벌크 로드)

8월의 대량 이벤트 데이터를 한 번에 처리하는 경우:

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

### 2. 준실시간 데이터 증분 처리 (Airflow)

9월부터 15분 간격으로 준실시간 데이터를 처리하는 경우:

#### Airflow DAG 구성

**DAG ID**: `replay_september_15min_realtime`

**실행 계획**:
<!-- - **총 DagRun 수**: 2,880개 (30일 × 96회/일) -->
- **실행 간격**: 15분 (`*/15 * * * *`)
- **Catchup**: True (과거 데이터 재처리 지원)
- **병렬 실행**: 1개 (max_active_runs=1, 데이터 순서 보장)

**Task 구성 및 역할**:

| Task | Operator | 주요 역할 |
|------|----------|----------|
| **staging_to_bronze** | SparkSubmitOperator | S3 Staging Area의 JSON 파일을 Bronze Iceberg 테이블로 적재<br>15분 간격 파티션 경로 읽기<br>원시 이벤트 문자열 보존 및 메타데이터 추가 |
| **bronze_to_silver** | SparkSubmitOperator | Bronze의 원시 JSON을 파싱하여 Silver 테이블에 구조화<br>JSON 스키마 적용 및 30+ 필드 추출<br>중복 제거 및 데이터 검증<br>실제 이벤트 발생 시간 기준 파티셔닝 |
| **silver_to_gold** | SparkSubmitOperator | Silver 데이터를 Gold Star Schema로 변환<br>Dimension 테이블 조인 및 Surrogate Key 매핑<br>비즈니스 메트릭 계산 (CTR, 참여도 점수)<br>Fact 테이블 증분 적재 |

**의존성**: `staging_to_bronze >> bronze_to_silver >> silver_to_gold`
<!-- 
**Airflow DAG 활성화**:
```bash
# DAG 파일 위치: dags/replay_september_15min_dag.py
airflow dags unpause replay_september_15min_realtime
``` -->

**수동 실행**:
```bash
cd spark/replay_jobs
spark-submit replay_staging_to_bronze.py \
    --data-interval-start "2025-09-01T00:00:00+09:00" \
    --data-interval-end "2025-09-01T00:15:00+09:00" \
    --test-mode false
```

### 3. 로그 스키마 기반 유저 행동 데이터 생성
<!-- 
**프로젝트의 핵심 특징**: 현실적인 행동 패턴을 시뮬레이션하여 유저 행동 이벤트 로그 데이터를 생성합니다.

**Dask 분산처리 프레임워크**:
- 100만 건 이상의 이벤트 로그를 효율적으로 생성
- 유저를 2,000명씩 배치로 나누고 4개 워커 프로세스에서 병렬 처리
- **순차 처리 대비 3-4배 빠른 생성 속도** 달성

**현실적인 유저 행동 패턴 시뮬레이션**:
- 시간대별 활동 패턴 (새벽 2% → 점심 피크 100% → 저녁 95%)
- 요일별 활동 패턴 (주중 80-100% → 주말 130% 피크)
- 유저 세그먼트 (POWER_USER 10%, ACTIVE_EXPLORER 60%, PASSIVE_BROWSER 30%)
- 인구통계 세그먼트 (성별 × 연령대 6개 조합)
- 요리 스타일 페르소나 (5개 요리 선호 타입)
- 자연스러운 이벤트 시퀀스 (EVENT_SCHEMA 기반)
- 컨텍스트 기반 행동 연결 (검색 → 목록 → 클릭 → 북마크)
- A/B 테스트 시뮬레이션 (세그먼트별 차등 CTR, 최대 +33% 개선)
- 실제 레시피 데이터 연동 (S3의 1만개+ 메타데이터) -->

> **상세 내용**: 9가지 행동 패턴 시뮬레이션, A/B 테스트 구현, 데이터 생성 명령어 등은 [create_data/README.md](create_data/README.md)를 참조하세요.


### 4. 쿼리 엔진 (Trino)

**Trino CLI 접속**:

```bash
# Trino 접속 (catalog 옵션으로 Iceberg 지정)
./trino-cli --server http://localhost:8080 --catalog iceberg
```
<!-- 
**분석 쿼리 카테고리** (총 20개 이상):
1. **유저 활동 지표**: DAU, WAU, 이벤트 분포, 시간대별 활동, 히트맵
2. **A/B 테스트 분석**: CTR 비교, 세그먼트별 전환율, Lift 계산, 일별 추이
3. **레시피 분석**: 인기 레시피, 북마크 순위, 요리 타입별 인터랙션
4. **참여도 분석**: 참여도 점수, 페이지별 조회수
5. **전환 퍼널 분석**: 검색→클릭 전환율, 광고 퍼널, CTR 계산

> **상세 내용**: 20개 쿼리의 전체 SQL, 사용 사례, Iceberg Time Travel 등은 [sql_queries/README.md](sql_queries/README.md)를 참조하세요. -->

<!-- ---

## 주요 설정

### Spark 설정

모든 Spark Job은 다음 공통 설정을 사용합니다:

```python
PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

CONF = {
    "spark.executor.memory": "3g",
    "spark.driver.memory": "3g",
    "spark.sql.shuffle.partitions": "100",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
    "spark.sql.catalog.iceberg_catalog.warehouse": "s3a://reciping-user-event-logs/iceberg/warehouse/"
}
```

### 환경별 데이터베이스

- **운영 환경**: `recipe_analytics`, `gold_analytics`
- **테스트 환경**: `recipe_analytics_test`

테스트 모드 활성화:
```python
--test-mode true  # CLI 인자
test_mode=True    # Python 코드
```

---

## 데이터 스키마

### Bronze Layer

| 컬럼명                | 타입      | 설명                    |
|-----------------------|-----------|-------------------------|
| raw_event_string      | STRING    | 원시 JSON 이벤트 문자열 |
| source_file           | STRING    | 소스 파일 경로          |
| ingestion_timestamp   | TIMESTAMP | 수집 시간               |
| ingestion_date        | DATE      | 수집 날짜 (파티션 키)   |

### Silver Layer

| 컬럼명        | 타입      | 설명                      |
|---------------|-----------|---------------------------|
| event_id      | STRING    | 이벤트 고유 ID (중복 제거)|
| user_id       | STRING    | 유저 ID                 |
| event_type    | STRING    | 이벤트 타입               |
| event_time    | TIMESTAMP | 이벤트 발생 시간          |
| event_date    | DATE      | 이벤트 발생 날짜 (파티션) |
| recipe_id     | STRING    | 레시피 ID                 |
| ab_test_group | STRING    | A/B 테스트 그룹           |
| device_type   | STRING    | 디바이스 타입             |
| ... (추가 필드)          |

### Gold Layer - Fact Table

| 컬럼명             | 타입      | 설명                    |
|--------------------|-----------|-------------------------|
| event_id           | STRING    | 이벤트 고유 ID          |
| user_id            | STRING    | 유저 ID (FK)          |
| recipe_id          | STRING    | 레시피 ID (FK)          |
| ab_test_group_id   | STRING    | A/B 테스트 그룹 ID (FK) |
| event_type         | STRING    | 이벤트 타입             |
| event_timestamp    | TIMESTAMP | 이벤트 시간             |
| event_date         | DATE      | 이벤트 날짜 (파티션)    |
| engagement_score   | DOUBLE    | 참여도 점수             |
| ... (비즈니스 메트릭)              |
 -->


---

## 데이터 분석 & 시각화 쿼리

프로젝트는 **20개 이상의 사전 정의된 Trino SQL 쿼리**를 제공합니다.
<br>모든 쿼리는 `sql_queries/` 디렉토리에서 확인할 수 있습니다.
> **상세 내용**: 20개 쿼리의 전체 SQL, 사용 사례, Iceberg Time Travel 등은 [sql_queries/README.md](sql_queries/README.md)를 참조하세요.
---

## Apache Superset 대시보드

<!-- 분석 쿼리를 기반으로 **Apache Superset**에 준실시간 대시보드를 구축했습니다. -->

### 대시보드 구성

#### 메인 대시보드: 고객 행동 분석

![Superset Dashboard 1](https://i.imgur.com/2LAojEt.jpeg)

<!-- **주요 차트**:

1. DAU 분포 (막대 차트)
2. 일일 DAU 변화 (KPI 카드)
3. 마인드맵 제로드맵 분석 (Sankey 다이어그램)
4. 유저 세그먼트별 비율 (도넛 차트)
5. 광고 클릭률 히트맵 (히트맵)
6. 가장 많이 상호작용한 레시피 Top 10 (가로 막대 차트)
7. WAU 트렌드 (라인 차트) -->

#### 세부 분석 대시보드: 세부 정보 분석

![Superset Dashboard 2](https://i.imgur.com/fkXPtSM.jpeg)
<!-- 
**주요 차트**:

1. 세그먼트별, 시간대별 이벤트 분포 (누적 막대 차트)
2. 세그먼트별 테마 일별 트렌드 (다중 라인 차트)
3. AB 테스트 지표 (테이블)
4. Lift 분석 결과 by 세그먼트별 AB 테스트 결과 (막대 차트)
5. 세그먼트별 AB 테스트 일별 CTR (히트맵)
6. 가장 많이 북마크된 레시피 Top 10 (가로 막대 차트)
7. 세그먼트별 AB 테스트 유저 분포 (버블 차트) -->

### 대시보드 기술 스택

- **데이터 소스**: Trino (Iceberg Catalog 연동)
- **시각화**: Apache Superset
- **실시간성**: 15분 간격 데이터 업데이트 (Airflow DAG 실행 후)
<!-- - **쿼리 성능**: Iceberg의 파티션 프루닝으로 빠른 응답 (<3초) -->

### 주요 인사이트

1. **피크 시간대**: 저녁 6-9시 (퇴근 후 저녁 준비)
2. **주말 효과**: 토요일 DAU 30% 증가
3. **A/B 테스트 결과**: Treatment 그룹 평균 22% CTR 개선
4. **세그먼트 특성**: FEMALE_40_PLUS가 가장 활성화된 세그먼트 (35.6%)
5. **레시피 선호도**: 베이킹/디저트 카테고리 인기

## 의존성 관리

**설치 방법**:
```bash
poetry install --with datagen  # 데이터 생성 도구 포함
poetry install --without datagen  # 실행 환경만
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

