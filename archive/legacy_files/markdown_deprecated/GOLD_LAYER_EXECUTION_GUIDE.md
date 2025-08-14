# Gold Layer Star Schema 실행 가이드

## 🌟 **Gold Layer의 역할과 목적**

네, 정확히 맞습니다! **Gold Layer**는 데이터 레이크하우스에서 다음과 같은 역할을 담당합니다:

### **Gold Layer = 비즈니스 인텔리전스 & 분석 레이어**
- ✅ **Star Schema 구축**: 차원 모델링으로 분석 최적화
- ✅ **핵심 비즈니스 지표**: DAU, WAU, Retention, Conversion 등
- ✅ **집계된 데이터**: 즉시 사용 가능한 분석용 데이터
- ✅ **대시보드 피드**: BI 도구(Tableau, PowerBI)와 직접 연결

---

## 🏗️ **구축된 Star Schema 아키텍처**

```
🌟 Star Schema Gold Layer
├── 📊 Fact Table
│   └── fact_user_events (중심 테이블)
│
├── 🎯 Dimension Tables  
│   ├── dim_users (사용자 차원)
│   ├── dim_time (시간 차원) 
│   ├── dim_recipes (레시피 차원)
│   ├── dim_pages (페이지 차원)
│   └── dim_events (이벤트 차원)
│
└── 📈 Business Metrics
    ├── metrics_dau (일일 활성 사용자)
    ├── metrics_retention (주간 리텐션)
    └── metrics_recipe_performance (레시피 성과)
```

---

## 🚀 **실행 방법**

### **1. Docker 환경에서 실행**
```bash
# 프로젝트 디렉토리로 이동
cd c:\Users\aryij\Documents\DataStudy\reciping-data-pipeline

# Docker 환경 시작 (이미 실행 중이면 생략)
docker-compose up -d

# Gold Layer Star Schema 파이프라인 실행
docker-compose exec spark-dev python /app/gold_layer_star_schema.py
```

### **2. 실행 결과 확인**
```bash
# 생성된 테이블 확인
docker-compose exec spark-dev python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.iceberg_catalog.type', 'hive') \
    .config('spark.sql.catalog.iceberg_catalog.uri', 'thrift://metastore:9083') \
    .getOrCreate()

print('=== Gold Layer 테이블 목록 ===')
spark.sql('SHOW TABLES IN iceberg_catalog.recipe_analytics').show()

print('=== DAU 메트릭 샘플 ===')
spark.sql('SELECT * FROM iceberg_catalog.recipe_analytics.metrics_dau ORDER BY date DESC LIMIT 5').show()

print('=== Weekly Retention 샘플 ===')
spark.sql('SELECT cohort_week, retention_week, cohort_size, retained_users, retention_rate FROM iceberg_catalog.recipe_analytics.metrics_retention ORDER BY cohort_week DESC, retention_week LIMIT 10').show()

spark.stop()
"
```

---

## 📊 **생성되는 핵심 비즈니스 지표**

### **1. DAU (Daily Active Users)**
```sql
SELECT 
    date,
    dau,
    new_users,
    returning_users,
    dau_growth_rate,
    dau_7d_avg
FROM iceberg_catalog.recipe_analytics.metrics_dau
ORDER BY date DESC;
```

**예상 결과:**
```
+----------+----+---------+---------------+---------------+----------+
|      date| dau|new_users|returning_users|dau_growth_rate|dau_7d_avg|
+----------+----+---------+---------------+---------------+----------+
|2025-07-31|8543|      156|           8387|           2.3%|   8234.2 |
|2025-07-30|8341|      201|           8140|           1.8%|   8156.7 |
|2025-07-29|8194|      189|           8005|          -0.5%|   8089.1 |
+----------+----+---------+---------------+---------------+----------+
```

### **2. Weekly Retention**
```sql
SELECT 
    cohort_week,
    retention_week,
    cohort_size,
    retained_users,
    retention_rate
FROM iceberg_catalog.recipe_analytics.metrics_retention
WHERE cohort_week = '2025-07-28'
ORDER BY retention_week;
```

**예상 결과:**
```
+-----------+--------------+-----------+--------------+--------------+
|cohort_week|retention_week|cohort_size|retained_users|retention_rate|
+-----------+--------------+-----------+--------------+--------------+
| 2025-07-28|             0|       1245|          1245|        100.00|
| 2025-07-28|             1|       1245|           856|         68.75|
| 2025-07-28|             2|       1245|           623|         50.04|
| 2025-07-28|             3|       1245|           467|         37.51|
+-----------+--------------+-----------+--------------+--------------+
```

### **3. Recipe Performance**
```sql
SELECT 
    recipe_id,
    total_views,
    unique_viewers,
    engagement_score,
    trending_score
FROM iceberg_catalog.recipe_analytics.metrics_recipe_performance
WHERE date = CURRENT_DATE()
ORDER BY total_views DESC
LIMIT 10;
```

---

## 🎯 **비즈니스 가치**

### **즉시 활용 가능한 분석**
1. **📈 성장 지표**
   - DAU/WAU/MAU 트렌드
   - 사용자 증가율
   - 세그먼트별 성장

2. **🔄 리텐션 분석** 
   - 코호트별 리텐션 커브
   - 차우링 패턴 분석
   - 사용자 라이프사이클

3. **🍳 콘텐츠 성과**
   - 인기 레시피 순위
   - 콘텐츠 참여도
   - 추천 알고리즘 피드백

### **BI 도구 연결 준비**
```python
# Tableau/PowerBI 연결용 쿼리 예시
business_dashboard_query = """
SELECT 
    d.full_date,
    d.year,
    d.month,
    d.day_name,
    d.is_weekend,
    
    du.user_segment,
    du.cooking_style,
    du.user_tier,
    
    de.event_category,
    de.is_conversion_event,
    
    COUNT(*) as total_events,
    COUNT(DISTINCT f.user_dim_key) as unique_users,
    SUM(f.conversion_value) as total_conversion_value,
    AVG(f.engagement_score) as avg_engagement
    
FROM iceberg_catalog.recipe_analytics.fact_user_events f
JOIN iceberg_catalog.recipe_analytics.dim_time d ON f.time_dim_key = d.time_dim_key  
JOIN iceberg_catalog.recipe_analytics.dim_users du ON f.user_dim_key = du.user_dim_key
JOIN iceberg_catalog.recipe_analytics.dim_events de ON f.event_dim_key = de.event_dim_key

WHERE d.full_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY d.full_date DESC
"""
```

---

## 🔄 **정기 업데이트 스케줄링**

### **일일 배치 실행**
```bash
# cron job 설정 예시 (매일 오전 6시)
0 6 * * * cd /path/to/reciping-data-pipeline && docker-compose exec -T spark-dev python /app/gold_layer_star_schema.py
```

### **증분 업데이트 모드**
```python
# 새로운 데이터만 처리하는 증분 업데이트
def incremental_update():
    # 어제 날짜 데이터만 처리
    yesterday = datetime.now() - timedelta(days=1)
    
    # DAU 증분 계산
    spark.sql(f"""
        INSERT INTO iceberg_catalog.recipe_analytics.metrics_dau
        SELECT ... 
        FROM iceberg_catalog.recipe_analytics.user_events_silver
        WHERE date = '{yesterday.strftime('%Y-%m-%d')}'
    """)
```

---

## 🎊 **결론: Gold Layer의 핵심 가치**

**정확히 말씀하신 대로**, Gold Layer는:

1. **✅ Star Schema 기반 차원 모델링**
2. **✅ DAU, Weekly Retention 등 핵심 지표 생성**  
3. **✅ 비즈니스 분석가가 즉시 사용할 수 있는 데이터 제공**
4. **✅ BI 도구와 직접 연결되는 분석용 데이터 마트**

이것이 바로 **데이터 레이크하우스 Gold Layer의 본질**입니다!

**🎯 다음 단계**: 위의 `gold_layer_star_schema.py`를 실행하여 완전한 Star Schema와 비즈니스 지표를 구축해보세요!
