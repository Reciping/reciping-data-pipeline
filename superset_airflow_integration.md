# 🚀 Apache Superset + Airflow 대시보드 아키텍처

## 📋 **전체 구조**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Apache        │    │   SQL Files     │    │   Apache        │
│   Airflow       │ ── │   Repository    │ ── │   Superset      │
│  (스케줄링)      │    │  (쿼리 관리)     │    │  (시각화)        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Gold Layer    │
                    │ fact_user_events│
                    │ (Star Schema)   │
                    └─────────────────┘
```

## 🎯 **1. Airflow DAG 구성**

### 📅 **Daily ETL + Dashboard Refresh DAG**
```python
# dags/reciping_dashboard_pipeline.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'reciping-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'reciping_dashboard_pipeline',
    default_args=default_args,
    description='Reciping 대시보드 파이프라인',
    schedule_interval='0 1 * * *',  # 매일 새벽 1시
    catchup=False
)

# Bronze → Silver 변환
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command='cd /opt/reciping && python bronze_to_silver_iceberg.py',
    dag=dag
)

# Silver → Gold 변환 (Star Schema)
silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command='cd /opt/reciping && python silver_to_gold_processor.py',
    dag=dag
)

# 대시보드 새로고침 함수
def refresh_superset_dashboard():
    """Superset 대시보드 캐시 새로고침"""
    import requests
    
    # Superset API 인증
    login_data = {
        'username': 'admin',
        'password': 'admin',
        'provider': 'db'
    }
    
    # 로그인
    session = requests.Session()
    login_response = session.post(
        'http://superset:8088/api/v1/security/login',
        json=login_data
    )
    
    if login_response.status_code == 200:
        # 대시보드 새로고침
        refresh_response = session.post(
            'http://superset:8088/api/v1/dashboard/1/refresh'
        )
        print(f"대시보드 새로고침 결과: {refresh_response.status_code}")
    
# Superset 대시보드 새로고침
refresh_dashboard = PythonOperator(
    task_id='refresh_superset_dashboard',
    python_callable=refresh_superset_dashboard,
    dag=dag
)

# 의존성 설정
bronze_to_silver >> silver_to_gold >> refresh_dashboard
```

## 🗂️ **2. SQL 파일 관리 구조**

### 📁 **SQL Repository 구조**
```
sql/
├── base_queries/                # 기본 쿼리
│   ├── kpi_metrics.sql         # 핵심 KPI
│   ├── hourly_activity.sql     # 시간대별 활동
│   └── conversion_funnel.sql   # 전환 퍼널
├── dashboard_queries/          # 대시보드별 쿼리
│   ├── executive_dashboard.sql # 경영진 대시보드
│   ├── product_analytics.sql   # 상품 분석
│   └── user_behavior.sql       # 사용자 행동
└── materialized_views/         # 집계 뷰
    ├── daily_summary.sql       # 일간 요약
    └── weekly_trends.sql       # 주간 트렌드
```

### 🔍 **핵심 KPI SQL 예시**
```sql
-- sql/base_queries/kpi_metrics.sql
-- 일간 핵심 지표 쿼리
SELECT 
    CURRENT_DATE() as report_date,
    
    -- DAU (Daily Active Users)
    COUNT(DISTINCT session_id) as dau,
    
    -- 이벤트 총 개수
    COUNT(*) as total_events,
    
    -- 전환율
    ROUND(
        SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as conversion_rate,
    
    -- 평균 참여도
    ROUND(AVG(engagement_score), 2) as avg_engagement,
    
    -- 세션당 평균 이벤트
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT session_id), 2) as events_per_session

FROM iceberg_catalog.gold_analytics.fact_user_events
WHERE time_dim_key >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd00') AS BIGINT)
  AND time_dim_key <= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd23') AS BIGINT);
```

### ⏰ **시간대별 활동 분석**
```sql
-- sql/base_queries/hourly_activity.sql
-- KST 시간대별 활동 히트맵 데이터
SELECT 
    (time_dim_key % 100) as hour,
    COUNT(*) as event_count,
    COUNT(DISTINCT session_id) as unique_sessions,
    ROUND(AVG(engagement_score), 2) as avg_engagement,
    
    -- 이벤트 타입별 분포
    SUM(CASE WHEN event_dim_key = 1 THEN 1 ELSE 0 END) as auth_events,
    SUM(CASE WHEN event_dim_key = 2 THEN 1 ELSE 0 END) as comment_events,
    SUM(CASE WHEN event_dim_key = 3 THEN 1 ELSE 0 END) as bookmark_events,
    SUM(CASE WHEN event_dim_key = 4 THEN 1 ELSE 0 END) as recipe_click_events,
    SUM(CASE WHEN event_dim_key = 5 THEN 1 ELSE 0 END) as search_events,
    SUM(CASE WHEN event_dim_key = 6 THEN 1 ELSE 0 END) as view_events

FROM iceberg_catalog.gold_analytics.fact_user_events
WHERE time_dim_key >= CAST(DATE_FORMAT(CURRENT_DATE() - INTERVAL 7 DAYS, 'yyyyMMdd00') AS BIGINT)
GROUP BY (time_dim_key % 100)
ORDER BY hour;
```

## 🎨 **3. Superset 대시보드 구성**

### 🏗️ **Docker Compose로 Superset 설정**
```yaml
# docker-compose.superset.yml
version: '3.8'
services:
  superset:
    image: apache/superset:latest
    ports:
      - "8088:8088"
    environment:
      - SUPERSET_CONFIG_PATH=/app/superset_config.py
    volumes:
      - ./superset_config.py:/app/superset_config.py
      - ./sql:/app/sql  # SQL 파일 마운트
    depends_on:
      - postgres
      
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: superset
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

### ⚙️ **Superset 설정**
```python
# superset_config.py
import os

# 데이터베이스 연결 (Spark/Hive)
SQLALCHEMY_DATABASE_URI = 'hive://localhost:9083/default'

# Spark SQL 연결 설정
SQLALCHEMY_BINDS = {
    'spark': 'spark://spark-master:7077',
    'hive': 'hive://localhost:9083/iceberg_catalog'
}

# 캐시 설정 (Redis)
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
}

# 스케줄된 쿼리 설정
ENABLE_SCHEDULED_EMAIL_REPORTS = True
SCHEDULED_EMAIL_DEBUG_MODE = True
```

## 🔄 **4. 자동화 워크플로**

### 📈 **실시간 대시보드 업데이트**
```python
# airflow/dags/superset_sync.py
def sync_sql_to_superset():
    """SQL 파일을 Superset 차트로 동기화"""
    import os
    import requests
    
    sql_directory = "/opt/reciping/sql"
    
    for root, dirs, files in os.walk(sql_directory):
        for file in files:
            if file.endswith('.sql'):
                sql_path = os.path.join(root, file)
                
                with open(sql_path, 'r') as f:
                    sql_content = f.read()
                
                # Superset API로 차트 생성/업데이트
                chart_data = {
                    'slice_name': file.replace('.sql', ''),
                    'query': sql_content,
                    'database_id': 1,  # Hive/Spark 데이터베이스 ID
                    'viz_type': 'table',  # 기본 테이블
                }
                
                # API 호출로 차트 생성
                response = requests.post(
                    'http://superset:8088/api/v1/chart/',
                    json=chart_data,
                    headers={'Authorization': f'Bearer {access_token}'}
                )
                
                print(f"차트 생성 결과: {file} - {response.status_code}")

# SQL 동기화 태스크
sync_sql_task = PythonOperator(
    task_id='sync_sql_to_superset',
    python_callable=sync_sql_to_superset,
    dag=dag
)
```

## 🎯 **5. 대시보드 템플릿**

### 📊 **경영진 대시보드**
```sql
-- sql/dashboard_queries/executive_dashboard.sql
-- 경영진용 핵심 지표 대시보드
WITH daily_metrics AS (
  SELECT 
    FLOOR(time_dim_key / 100) as date,
    COUNT(DISTINCT session_id) as dau,
    COUNT(*) as total_events,
    SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions
  FROM iceberg_catalog.gold_analytics.fact_user_events
  WHERE time_dim_key >= CAST(DATE_FORMAT(CURRENT_DATE() - INTERVAL 30 DAYS, 'yyyyMMdd00') AS BIGINT)
  GROUP BY FLOOR(time_dim_key / 100)
),
growth_metrics AS (
  SELECT 
    date,
    dau,
    LAG(dau, 1) OVER (ORDER BY date) as prev_dau,
    ROUND((dau - LAG(dau, 1) OVER (ORDER BY date)) * 100.0 / LAG(dau, 1) OVER (ORDER BY date), 2) as dau_growth,
    total_events,
    conversions,
    ROUND(conversions * 100.0 / total_events, 2) as conversion_rate
  FROM daily_metrics
)
SELECT * FROM growth_metrics
ORDER BY date DESC;
```

## 🚀 **6. 배포 및 운영**

### 🐳 **통합 Docker Compose**
```yaml
# docker-compose.full.yml
version: '3.8'
services:
  # Spark 환경 (기존)
  spark-dev:
    # ... 기존 설정
  
  # Airflow
  airflow-webserver:
    image: apache/airflow:2.7.0
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./sql:/opt/airflow/sql
  
  # Superset
  superset:
    image: apache/superset:latest
    ports:
      - "8088:8088"
    volumes:
      - ./sql:/app/sql
  
  # Redis (캐시)
  redis:
    image: redis:6.2
    ports:
      - "6379:6379"
```

### 📋 **실행 순서**
1. **데이터 파이프라인 실행**: `silver_to_gold_processor.py`
2. **Airflow DAG 활성화**: 스케줄된 ETL 실행  
3. **Superset 대시보드 구성**: SQL 파일 기반 차트 생성
4. **자동 새로고침**: Airflow → ETL → Superset 연계

이 구조로 **SQL 파일 중심의 유지보수가 쉬운 대시보드**를 구축할 수 있습니다! 🎯
