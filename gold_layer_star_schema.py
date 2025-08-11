# ================================================================================
# 🌟 Gold Layer Star Schema 구축 파이프라인
# ================================================================================
# 
# 목적: Silver Layer(user_events_silver)를 기반으로 Star Schema 형태의 
#      Gold Layer를 구축하여 비즈니스 분석과 BI 리포팅을 지원
#
# 아키텍처:
# 🥈 Silver: recipe_analytics.user_events_silver
# 🥇 Gold:   gold_analytics.{dim_users, dim_time, dim_recipes, fact_user_events}
#
# S3 구조:
# s3://reciping-user-event-logs/iceberg/warehouse/
# ├── recipe_analytics.db/user_events_silver/    # Silver Layer
# └── gold_analytics.db/                         # Gold Layer
#     ├── dim_users/                            # 👤 사용자 차원
#     ├── dim_time/                             # 📅 시간 차원  
#     ├── dim_recipes/                          # 🍳 레시피 차원
#     ├── fact_user_events/                     # 📊 이벤트 팩트
#     └── metrics_*/                            # 📈 비즈니스 메트릭
# ================================================================================

# gold_layer_star_schema.py
"""
Gold Layer Star Schema 구축 및 비즈니스 지표 생성
- Silver Layer → Star Schema 변환
- DAU, Weekly Retention, Recipe Analytics 지표 생성
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import os

class GoldLayerStarSchema:
    """Gold Layer Star Schema 구축 및 비즈니스 메트릭 생성"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.database_name = "recipe_analytics"
        self.spark = None
        
    def create_spark_session(self):
        """Iceberg + Hive Metastore SparkSession 생성"""
        print("🧊 Gold Layer SparkSession 생성 중...")
        
        self.spark = SparkSession.builder \
            .appName("GoldLayer_StarSchema_Analytics") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .config("spark.driver.memory", "3g") \
            .config("spark.executor.memory", "3g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ Gold Layer SparkSession 생성 완료!")
        
    def create_dimension_tables(self):
        """Star Schema Dimension 테이블들 생성"""
        print("\n🌟 Star Schema Dimension 테이블 생성 중...")
        
        # 1. Time Dimension 생성
        print("📅 Time Dimension 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.dim_time (
                time_dim_key BIGINT NOT NULL,
                full_date DATE NOT NULL,
                year INT,
                quarter INT,
                month INT,
                month_name STRING,
                week_of_year INT,
                day_of_month INT,
                day_of_week INT,
                day_name STRING,
                hour INT,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN,
                season STRING,
                business_quarter STRING,
                days_from_today INT,
                weeks_from_today INT,
                months_from_today INT,
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '67108864'
            )
        """)
        
        # 2. User Dimension 생성  
        print("👤 User Dimension 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.dim_users (
                user_dim_key BIGINT NOT NULL,
                user_id STRING NOT NULL,
                user_segment STRING,
                cooking_style STRING,
                ab_test_group STRING,
                first_seen_date DATE,
                last_activity_date DATE,
                total_sessions BIGINT,
                total_recipe_views BIGINT,
                user_tier STRING,
                lifetime_value DECIMAL(10,2),
                effective_date DATE NOT NULL,
                expiry_date DATE,
                is_current BOOLEAN,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (is_current)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '67108864'
            )
        """)
        
        # 3. Recipe Dimension 생성
        print("🍳 Recipe Dimension 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.dim_recipes (
                recipe_dim_key BIGINT NOT NULL,
                recipe_id BIGINT,
                recipe_category STRING,
                ingredient_count INT,
                difficulty_level STRING,
                cuisine_type STRING,
                prep_time_minutes INT,
                is_premium BOOLEAN,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 4. Page Dimension 생성
        print("📱 Page Dimension 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.dim_pages (
                page_dim_key BIGINT NOT NULL,
                page_name STRING NOT NULL,
                page_url STRING,
                page_category STRING,
                funnel_stage STRING,
                is_mobile BOOLEAN,
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 5. Event Dimension 생성
        print("🎬 Event Dimension 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.dim_events (
                event_dim_key BIGINT NOT NULL,
                event_name STRING NOT NULL,
                event_category STRING,
                conversion_value DECIMAL(5,2),
                is_conversion_event BOOLEAN,
                event_weight DECIMAL(3,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        print("✅ 모든 Dimension 테이블 생성 완료!")
        
    def create_fact_table(self):
        """Star Schema Fact 테이블 생성"""
        print("\n📊 Fact 테이블 생성 중...")
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.fact_user_events (
                event_id STRING NOT NULL,
                user_dim_key BIGINT NOT NULL,
                time_dim_key BIGINT NOT NULL,
                recipe_dim_key BIGINT,
                page_dim_key BIGINT,
                event_dim_key BIGINT NOT NULL,
                
                -- Additive Measures
                event_count BIGINT,
                session_duration_seconds BIGINT,
                page_view_duration_seconds BIGINT,
                
                -- Semi-Additive Measures
                is_conversion BOOLEAN,
                conversion_value DECIMAL(10,2),
                engagement_score DECIMAL(5,2),
                
                -- Degenerate Dimensions
                session_id STRING,
                anonymous_id STRING,
                
                -- ETL Metadata
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (time_dim_key)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '134217728'
            )
        """)
        
        print("✅ Fact 테이블 생성 완료!")
        
    def populate_time_dimension(self):
        """Time Dimension 데이터 채우기"""
        print("\n📅 Time Dimension 데이터 생성 중...")
        
        # 2025년 1월부터 2026년 12월까지 시간 데이터 생성
        time_data_query = """
        WITH date_range AS (
            SELECT SEQUENCE(
                TO_DATE('2025-01-01'), 
                TO_DATE('2026-12-31'), 
                INTERVAL 1 DAY
            ) as date_array
        ),
        time_expanded AS (
            SELECT 
                EXPLODE(date_array) as full_date
            FROM date_range
        ),
        time_with_hours AS (
            SELECT 
                full_date,
                hour_val
            FROM time_expanded
            CROSS JOIN (SELECT EXPLODE(SEQUENCE(0, 23)) as hour_val)
        )
        
        INSERT OVERWRITE iceberg_catalog.recipe_analytics.dim_time
        SELECT 
            CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS BIGINT) * 100 + hour_val as time_dim_key,
            full_date,
            YEAR(full_date) as year,
            QUARTER(full_date) as quarter,
            MONTH(full_date) as month,
            DATE_FORMAT(full_date, 'MMMM') as month_name,
            WEEKOFYEAR(full_date) as week_of_year,
            DAYOFMONTH(full_date) as day_of_month,
            DAYOFWEEK(full_date) as day_of_week,
            DATE_FORMAT(full_date, 'EEEE') as day_name,
            hour_val as hour,
            CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
            FALSE as is_holiday,  -- 추후 휴일 데이터 추가 가능
            CASE 
                WHEN MONTH(full_date) IN (12, 1, 2) THEN 'Winter'
                WHEN MONTH(full_date) IN (3, 4, 5) THEN 'Spring'
                WHEN MONTH(full_date) IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END as season,
            CONCAT('Q', QUARTER(full_date), '-', YEAR(full_date)) as business_quarter,
            DATEDIFF(full_date, CURRENT_DATE()) as days_from_today,
            CAST(DATEDIFF(full_date, CURRENT_DATE()) / 7 AS INT) as weeks_from_today,
            MONTHS_BETWEEN(full_date, CURRENT_DATE()) as months_from_today,
            CURRENT_TIMESTAMP() as created_at
        FROM time_with_hours
        """
        
        self.spark.sql(time_data_query)
        
        # 결과 확인
        time_count = self.spark.sql("SELECT COUNT(*) as cnt FROM iceberg_catalog.recipe_analytics.dim_time").collect()[0]['cnt']
        print(f"✅ Time Dimension 생성 완료: {time_count:,}개 레코드")
        
    def populate_dimensions_from_silver(self):
        """Silver Layer로부터 Dimension 데이터 생성"""
        print("\n🔄 Silver Layer에서 Dimension 데이터 추출 중...")
        
        # 1. User Dimension 채우기
        print("👤 User Dimension 데이터 생성...")
        user_dim_query = f"""
        WITH user_aggregates AS (
            SELECT 
                user_id,
                user_segment,
                cooking_style,
                ab_test_group,
                MIN(date) as first_seen_date,
                MAX(date) as last_activity_date,
                COUNT(DISTINCT session_id) as total_sessions,
                COUNT(CASE WHEN event_name = 'view_recipe' THEN 1 END) as total_recipe_views,
                COUNT(*) as total_events
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE user_id IS NOT NULL
            GROUP BY user_id, user_segment, cooking_style, ab_test_group
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.dim_users
        SELECT 
            ROW_NUMBER() OVER (ORDER BY first_seen_date, user_id) as user_dim_key,
            user_id,
            user_segment,
            cooking_style,
            ab_test_group,
            first_seen_date,
            last_activity_date,
            total_sessions,
            total_recipe_views,
            CASE 
                WHEN total_events >= 100 THEN 'Power User'
                WHEN total_events >= 20 THEN 'Active User'
                WHEN total_events >= 5 THEN 'Regular User'
                ELSE 'New User'
            END as user_tier,
            total_events * 0.1 as lifetime_value,  -- 간단한 LTV 계산
            first_seen_date as effective_date,
            NULL as expiry_date,
            TRUE as is_current,
            CURRENT_TIMESTAMP() as created_at,
            CURRENT_TIMESTAMP() as updated_at
        FROM user_aggregates
        """
        
        self.spark.sql(user_dim_query)
        
        # 2. Recipe Dimension 채우기
        print("🍳 Recipe Dimension 데이터 생성...")
        recipe_dim_query = f"""
        WITH recipe_info AS (
            SELECT DISTINCT
                prop_recipe_id as recipe_id
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE prop_recipe_id IS NOT NULL
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.dim_recipes
        SELECT 
            ROW_NUMBER() OVER (ORDER BY recipe_id) as recipe_dim_key,
            recipe_id,
            'Unknown' as recipe_category,  -- 추후 레시피 마스터 데이터와 조인
            5 as ingredient_count,  -- 기본값
            'Medium' as difficulty_level,  -- 기본값
            'Korean' as cuisine_type,  -- 기본값
            30 as prep_time_minutes,  -- 기본값
            FALSE as is_premium,
            CURRENT_TIMESTAMP() as created_at,
            CURRENT_TIMESTAMP() as updated_at
        FROM recipe_info
        WHERE recipe_id > 0
        
        UNION ALL
        
        SELECT 
            0 as recipe_dim_key,
            NULL as recipe_id,
            'N/A' as recipe_category,
            0 as ingredient_count,
            'N/A' as difficulty_level,
            'N/A' as cuisine_type,
            0 as prep_time_minutes,
            FALSE as is_premium,
            CURRENT_TIMESTAMP() as created_at,
            CURRENT_TIMESTAMP() as updated_at
        """
        
        self.spark.sql(recipe_dim_query)
        
        # 3. Page Dimension 채우기
        print("📱 Page Dimension 데이터 생성...")
        page_dim_query = f"""
        WITH page_info AS (
            SELECT DISTINCT
                page_name,
                page_url
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE page_name IS NOT NULL
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.dim_pages
        SELECT 
            ROW_NUMBER() OVER (ORDER BY page_name) as page_dim_key,
            page_name,
            page_url,
            CASE 
                WHEN page_name LIKE '%recipe%' THEN 'Recipe'
                WHEN page_name LIKE '%search%' THEN 'Search'
                WHEN page_name LIKE '%list%' THEN 'Browse'
                WHEN page_name LIKE '%auth%' THEN 'Authentication'
                ELSE 'Other'
            END as page_category,
            CASE 
                WHEN page_name = 'home' THEN 'Awareness'
                WHEN page_name LIKE '%list%' THEN 'Interest'
                WHEN page_name LIKE '%recipe%' THEN 'Consideration'
                WHEN page_name LIKE '%auth%' THEN 'Conversion'
                ELSE 'Other'
            END as funnel_stage,
            CASE WHEN page_url LIKE '%mobile%' THEN TRUE ELSE FALSE END as is_mobile,
            CURRENT_TIMESTAMP() as created_at
        FROM page_info
        
        UNION ALL
        
        SELECT 
            0 as page_dim_key,
            'Unknown' as page_name,
            'Unknown' as page_url,
            'Unknown' as page_category,
            'Unknown' as funnel_stage,
            FALSE as is_mobile,
            CURRENT_TIMESTAMP() as created_at
        """
        
        self.spark.sql(page_dim_query)
        
        # 4. Event Dimension 채우기
        print("🎬 Event Dimension 데이터 생성...")
        event_dim_query = f"""
        WITH event_info AS (
            SELECT DISTINCT event_name
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.dim_events
        SELECT 
            ROW_NUMBER() OVER (ORDER BY event_name) as event_dim_key,
            event_name,
            CASE 
                WHEN event_name LIKE '%view%' THEN 'Engagement'
                WHEN event_name LIKE '%click%' THEN 'Interaction'
                WHEN event_name LIKE '%search%' THEN 'Discovery'
                WHEN event_name LIKE '%auth%' THEN 'Conversion'
                ELSE 'Other'
            END as event_category,
            CASE 
                WHEN event_name = 'auth_success' THEN 10.0
                WHEN event_name = 'click_recipe' THEN 5.0
                WHEN event_name = 'search_recipe' THEN 3.0
                WHEN event_name = 'view_page' THEN 1.0
                ELSE 1.0
            END as conversion_value,
            CASE WHEN event_name IN ('auth_success', 'create_comment', 'click_bookmark') THEN TRUE ELSE FALSE END as is_conversion_event,
            1.0 as event_weight,
            CURRENT_TIMESTAMP() as created_at
        FROM event_info
        """
        
        self.spark.sql(event_dim_query)
        
        print("✅ 모든 Dimension 데이터 생성 완료!")
        
    def create_metrics_tables(self):
        """10개 핵심 비즈니스 메트릭 + A/B 테스트 메트릭 테이블들 생성"""
        print("\n📊 비즈니스 메트릭 테이블 생성 중...")
        
        # 1. EVENT_TYPE_TIME_DISTRIBUTION - 이벤트 유형별 시간 분포
        print("⏰ Event Type Time Distribution 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_event_type_time_distribution (
                event_time_month STRING NOT NULL,
                event_time_day_name STRING NOT NULL,
                event_time_hour INT NOT NULL,
                event_type STRING NOT NULL,
                event_count BIGINT NOT NULL,
                unique_users BIGINT,
                avg_session_duration DECIMAL(8,2),
                peak_indicator BOOLEAN,
                distribution_percentage DECIMAL(5,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (event_time_month)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 2. CONVERSION_RATE - 전환율 분석
        print("📈 Conversion Rate 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_conversion_rate (
                date DATE NOT NULL,
                funnel_stage STRING NOT NULL,
                total_users BIGINT NOT NULL,
                converted_users BIGINT NOT NULL,
                conversion_rate DECIMAL(5,2) NOT NULL,
                conversion_by_segment MAP<STRING, DECIMAL(5,2)>,
                benchmark_rate DECIMAL(5,2),
                improvement_target DECIMAL(5,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 3. WEEKLY_RETENTION - 주간 리텐션
        print("🔄 Weekly Retention 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_weekly_retention (
                cohort_week DATE NOT NULL,
                retention_week INT NOT NULL,
                cohort_size BIGINT NOT NULL,
                retained_users BIGINT NOT NULL,
                retention_rate DECIMAL(5,2) NOT NULL,
                retention_by_segment MAP<STRING, DECIMAL(5,2)>,
                cumulative_retention_rate DECIMAL(5,2),
                churn_rate DECIMAL(5,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (cohort_week)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 4. MONTHLY_RETENTION - 월간 리텐션
        print("📅 Monthly Retention 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_monthly_retention (
                cohort_month DATE NOT NULL,
                retention_month INT NOT NULL,
                cohort_size BIGINT NOT NULL,
                retained_users BIGINT NOT NULL,
                retention_rate DECIMAL(5,2) NOT NULL,
                ltv_estimate DECIMAL(10,2),
                churn_probability DECIMAL(5,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (cohort_month)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 5. ACTIVE_USERS - DAU/WAU/MAU
        print("👥 Active Users 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_active_users (
                date DATE NOT NULL,
                dau BIGINT NOT NULL,
                wau BIGINT,
                mau BIGINT,
                new_users BIGINT,
                returning_users BIGINT,
                dau_by_segment MAP<STRING, BIGINT>,
                wau_by_segment MAP<STRING, BIGINT>,
                mau_by_segment MAP<STRING, BIGINT>,
                dau_growth_rate DECIMAL(5,2),
                dau_7d_avg DECIMAL(10,2),
                dau_30d_avg DECIMAL(10,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 6. STICKINESS - 유저 고착성
        print("🎯 Stickiness 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_stickiness (
                date DATE NOT NULL,
                daily_active_users BIGINT NOT NULL,
                monthly_active_users BIGINT NOT NULL,
                stickiness_ratio DECIMAL(5,4) NOT NULL,
                avg_sessions_per_user DECIMAL(8,2),
                avg_events_per_user DECIMAL(8,2),
                power_user_ratio DECIMAL(5,2),
                engagement_depth_score DECIMAL(5,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 7. FUNNEL - 퍼널 분석
        print("🎪 Funnel 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_funnel (
                date DATE NOT NULL,
                funnel_stage STRING NOT NULL,
                stage_order INT NOT NULL,
                total_users BIGINT NOT NULL,
                conversion_from_previous DECIMAL(5,2),
                cumulative_conversion DECIMAL(5,2),
                drop_off_rate DECIMAL(5,2),
                avg_time_to_convert DECIMAL(10,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 8. COUNT_VISITORS - 방문자 수
        print("🚶 Count Visitors 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_count_visitors (
                date DATE NOT NULL,
                total_visitors BIGINT NOT NULL,
                unique_visitors BIGINT NOT NULL,
                new_visitors BIGINT NOT NULL,
                returning_visitors BIGINT NOT NULL,
                avg_session_duration DECIMAL(8,2),
                bounce_rate DECIMAL(5,2),
                pages_per_session DECIMAL(5,2),
                traffic_source_breakdown MAP<STRING, BIGINT>,
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 9. REE_SEGMENTATION - Recipe RFM (Recency, Engagement, Expertise)
        print("🏆 REE Segmentation 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_ree_segmentation (
                user_id STRING NOT NULL,
                analysis_date DATE NOT NULL,
                recency_days INT NOT NULL,
                engagement_score DECIMAL(8,2) NOT NULL,
                expertise_level DECIMAL(5,2) NOT NULL,
                recency_score INT NOT NULL,
                engagement_score_quintile INT NOT NULL,
                expertise_score_quintile INT NOT NULL,
                ree_total_score INT NOT NULL,
                user_segment STRING NOT NULL,
                segment_description STRING,
                recommended_actions ARRAY<STRING>,
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (analysis_date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 10. RECIPE_PERFORMANCE - 레시피 성과 분석
        print("🍳 Recipe Performance 메트릭 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_recipe_performance (
                date DATE NOT NULL,
                recipe_id BIGINT NOT NULL,
                total_views BIGINT,
                unique_viewers BIGINT,
                avg_view_duration DECIMAL(8,2),
                recipe_saves BIGINT,
                recipe_shares BIGINT,
                recipe_comments BIGINT,
                cooking_attempts BIGINT,
                success_rate DECIMAL(5,2),
                engagement_score DECIMAL(5,2),
                trending_score DECIMAL(5,2),
                recommendation_score DECIMAL(5,2),
                viral_coefficient DECIMAL(5,4),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (date)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 11. A/B TEST METRICS - A/B 테스트 결과 분석
        print("🧪 A/B Test Metrics 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_ab_test_results (
                test_id STRING NOT NULL,
                test_name STRING NOT NULL,
                variant_group STRING NOT NULL,
                metric_name STRING NOT NULL,
                analysis_date DATE NOT NULL,
                sample_size BIGINT NOT NULL,
                metric_value DECIMAL(15,6) NOT NULL,
                control_value DECIMAL(15,6),
                lift_percentage DECIMAL(8,4),
                confidence_interval_lower DECIMAL(15,6),
                confidence_interval_upper DECIMAL(15,6),
                p_value DECIMAL(10,8),
                statistical_significance BOOLEAN,
                confidence_level DECIMAL(5,2),
                test_status STRING,
                test_duration_days INT,
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (analysis_date, test_id)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        # 12. A/B TEST COHORT ANALYSIS - A/B 테스트 코호트 분석
        print("📊 A/B Test Cohort Analysis 테이블 생성...")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.database_name}.metrics_ab_test_cohort (
                test_id STRING NOT NULL,
                variant_group STRING NOT NULL,
                cohort_week DATE NOT NULL,
                week_number INT NOT NULL,
                initial_users BIGINT NOT NULL,
                active_users BIGINT NOT NULL,
                retention_rate DECIMAL(5,2) NOT NULL,
                avg_engagement_score DECIMAL(8,2),
                conversion_rate DECIMAL(5,2),
                revenue_per_user DECIMAL(10,2),
                created_at TIMESTAMP NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (test_id, cohort_week)
            TBLPROPERTIES (
                'format-version' = '2'
            )
        """)
        
        print("✅ 모든 메트릭 테이블 생성 완료!")
        
    def calculate_dau_metrics(self):
        """1. ACTIVE_USERS - DAU/WAU/MAU 메트릭 계산"""
        print("\n👥 Active Users 메트릭 계산 중...")
        
        active_users_query = f"""
        WITH daily_users AS (
            SELECT 
                date,
                user_id,
                user_segment,
                page_name,
                MIN(date) OVER (PARTITION BY user_id) as first_seen_date
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
        ),
        dau_calculations AS (
            SELECT 
                date,
                COUNT(DISTINCT user_id) as dau,
                COUNT(DISTINCT CASE WHEN first_seen_date = date THEN user_id END) as new_users,
                COUNT(DISTINCT CASE WHEN first_seen_date < date THEN user_id END) as returning_users
            FROM daily_users
            GROUP BY date
        ),
        dau_by_segment AS (
            SELECT 
                date,
                user_segment,
                COUNT(DISTINCT user_id) as segment_dau
            FROM daily_users
            WHERE user_segment IS NOT NULL
            GROUP BY date, user_segment
        ),
        dau_segment_map AS (
            SELECT 
                date,
                MAP_FROM_ARRAYS(
                    COLLECT_LIST(user_segment),
                    COLLECT_LIST(segment_dau)
                ) as dau_by_segment
            FROM dau_by_segment
            GROUP BY date
        ),
        wau_calculations AS (
            SELECT 
                DATE_TRUNC('week', date) as week_start,
                user_segment,
                COUNT(DISTINCT user_id) as segment_wau
            FROM daily_users
            WHERE user_segment IS NOT NULL
            GROUP BY DATE_TRUNC('week', date), user_segment
        ),
        wau_totals AS (
            SELECT 
                week_start,
                SUM(segment_wau) as wau
            FROM wau_calculations
            GROUP BY week_start
        ),
        wau_segment_map AS (
            SELECT 
                week_start,
                MAP_FROM_ARRAYS(
                    COLLECT_LIST(user_segment),
                    COLLECT_LIST(segment_wau)
                ) as wau_by_segment
            FROM wau_calculations
            GROUP BY week_start
        ),
        mau_calculations AS (
            SELECT 
                DATE_TRUNC('month', date) as month_start,
                user_segment,
                COUNT(DISTINCT user_id) as segment_mau
            FROM daily_users
            WHERE user_segment IS NOT NULL
            GROUP BY DATE_TRUNC('month', date), user_segment
        ),
        mau_totals AS (
            SELECT 
                month_start,
                SUM(segment_mau) as mau
            FROM mau_calculations
            GROUP BY month_start
        ),
        mau_segment_map AS (
            SELECT 
                month_start,
                MAP_FROM_ARRAYS(
                    COLLECT_LIST(user_segment),
                    COLLECT_LIST(segment_mau)
                ) as mau_by_segment
            FROM mau_calculations
            GROUP BY month_start
        ),
        combined_metrics AS (
            SELECT 
                d.date,
                d.dau,
                w.wau,
                m.mau,
                d.new_users,
                d.returning_users,
                ds.dau_by_segment,
                ws.wau_by_segment,
                ms.mau_by_segment,
                LAG(d.dau, 1) OVER (ORDER BY d.date) as prev_dau,
                AVG(d.dau) OVER (ORDER BY d.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as dau_7d_avg,
                AVG(d.dau) OVER (ORDER BY d.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as dau_30d_avg
            FROM dau_calculations d
            LEFT JOIN dau_segment_map ds ON d.date = ds.date
            LEFT JOIN wau_totals w ON d.date >= w.week_start AND d.date < w.week_start + INTERVAL 7 DAYS
            LEFT JOIN wau_segment_map ws ON d.date >= ws.week_start AND d.date < ws.week_start + INTERVAL 7 DAYS
            LEFT JOIN mau_totals m ON d.date >= m.month_start AND d.date < m.month_start + INTERVAL 1 MONTH
            LEFT JOIN mau_segment_map ms ON d.date >= ms.month_start AND d.date < ms.month_start + INTERVAL 1 MONTH
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_active_users
        SELECT 
            date,
            dau,
            wau,
            mau,
            new_users,
            returning_users,
            dau_by_segment,
            wau_by_segment,
            mau_by_segment,
            CASE WHEN prev_dau > 0 THEN ROUND(((dau - prev_dau) * 100.0 / prev_dau), 2) ELSE 0 END as dau_growth_rate,
            ROUND(dau_7d_avg, 2) as dau_7d_avg,
            ROUND(dau_30d_avg, 2) as dau_30d_avg,
            CURRENT_TIMESTAMP() as created_at
        FROM combined_metrics
        ORDER BY date
        """
        
        self.spark.sql(active_users_query)
        print("✅ Active Users 메트릭 계산 완료!")
        
    def calculate_event_time_distribution(self):
        """2. EVENT_TYPE_TIME_DISTRIBUTION 메트릭 계산"""
        print("\n⏰ Event Type Time Distribution 메트릭 계산 중...")
        
        event_time_query = f"""
        WITH event_time_analysis AS (
            SELECT
                DATE_FORMAT(utc_timestamp, 'yyyy-MM') as event_time_month,
                DATE_FORMAT(utc_timestamp, 'EEEE') as event_time_day_name,
                HOUR(utc_timestamp) as event_time_hour,
                event_name as event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(COALESCE(CAST(SPLIT(prop_action, ':')[1] AS DOUBLE), 0)) as avg_session_duration
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            GROUP BY 
                DATE_FORMAT(utc_timestamp, 'yyyy-MM'),
                DATE_FORMAT(utc_timestamp, 'EEEE'),
                HOUR(utc_timestamp),
                event_name
        ),
        peak_analysis AS (
            SELECT 
                *,
                MAX(event_count) OVER (PARTITION BY event_time_month, event_type) as max_count_for_type,
                SUM(event_count) OVER (PARTITION BY event_time_month) as total_events_month
            FROM event_time_analysis
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_event_type_time_distribution
        SELECT
            event_time_month,
            event_time_day_name,
            event_time_hour,
            event_type,
            event_count,
            unique_users,
            ROUND(avg_session_duration, 2) as avg_session_duration,
            CASE WHEN event_count = max_count_for_type THEN TRUE ELSE FALSE END as peak_indicator,
            ROUND((event_count * 100.0 / total_events_month), 2) as distribution_percentage,
            CURRENT_TIMESTAMP() as created_at
        FROM peak_analysis
        ORDER BY event_time_month, event_type, event_time_hour
        """
        
        self.spark.sql(event_time_query)
        print("✅ Event Type Time Distribution 메트릭 계산 완료!")
        
    def calculate_conversion_metrics(self):
        """3. CONVERSION_RATE 메트릭 계산"""
        print("\n📈 Conversion Rate 메트릭 계산 중...")
        
        conversion_query = f"""
        WITH user_funnel AS (
            SELECT 
                date,
                user_id,
                CASE 
                    WHEN event_name = 'view_page' THEN 'Awareness'
                    WHEN event_name LIKE '%search%' THEN 'Interest'
                    WHEN event_name = 'view_recipe' THEN 'Consideration'
                    WHEN event_name IN ('auth_success', 'create_comment', 'click_bookmark') THEN 'Conversion'
                    ELSE 'Other'
                END as funnel_stage,
                user_segment
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE event_name IN ('view_page', 'search_recipe', 'view_recipe', 'auth_success', 'create_comment', 'click_bookmark')
        ),
        funnel_analysis AS (
            SELECT 
                date,
                funnel_stage,
                user_segment,
                COUNT(DISTINCT user_id) as stage_users
            FROM user_funnel
            GROUP BY date, funnel_stage, user_segment
        ),
        conversion_calculations AS (
            SELECT 
                date,
                funnel_stage,
                SUM(stage_users) as total_users,
                SUM(CASE WHEN funnel_stage = 'Conversion' THEN stage_users ELSE 0 END) OVER (PARTITION BY date) as total_conversions,
                MAP_FROM_ARRAYS(
                    COLLECT_LIST(user_segment),
                    COLLECT_LIST(ROUND((stage_users * 100.0 / SUM(stage_users) OVER (PARTITION BY date, funnel_stage)), 2))
                ) as conversion_by_segment
            FROM funnel_analysis
            GROUP BY date, funnel_stage
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_conversion_rate
        SELECT 
            date,
            funnel_stage,
            total_users,
            CASE WHEN funnel_stage = 'Conversion' THEN total_users ELSE 0 END as converted_users,
            CASE 
                WHEN funnel_stage = 'Conversion' AND total_conversions > 0 
                THEN ROUND((total_users * 100.0 / total_conversions), 2)
                ELSE 0 
            END as conversion_rate,
            conversion_by_segment,
            2.5 as benchmark_rate,  -- 업계 평균 2.5%
            3.0 as improvement_target,  -- 목표 3%
            CURRENT_TIMESTAMP() as created_at
        FROM conversion_calculations
        ORDER BY date, funnel_stage
        """
        
        self.spark.sql(conversion_query)
        print("✅ Conversion Rate 메트릭 계산 완료!")
        
    def calculate_retention_metrics(self):
        """4. WEEKLY_RETENTION & 5. MONTHLY_RETENTION 메트릭 계산"""
        print("\n🔄 Weekly & Monthly Retention 메트릭 계산 중...")
        
        # Weekly Retention
        weekly_retention_query = f"""
        WITH user_cohorts AS (
            SELECT 
                user_id,
                DATE_TRUNC('week', MIN(date)) as cohort_week,
                user_segment
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            GROUP BY user_id, user_segment
        ),
        user_activity_weeks AS (
            SELECT 
                uc.user_id,
                uc.cohort_week,
                uc.user_segment,
                DATE_TRUNC('week', ues.date) as activity_week,
                DATEDIFF(DATE_TRUNC('week', ues.date), uc.cohort_week) / 7 as weeks_since_cohort
            FROM user_cohorts uc
            JOIN {self.catalog_name}.{self.database_name}.user_events_silver ues ON uc.user_id = ues.user_id
        ),
        weekly_retention_analysis AS (
            SELECT 
                cohort_week,
                weeks_since_cohort as retention_week,
                user_segment,
                COUNT(DISTINCT user_id) as active_users
            FROM user_activity_weeks
            WHERE weeks_since_cohort >= 0 AND weeks_since_cohort <= 12
            GROUP BY cohort_week, weeks_since_cohort, user_segment
        ),
        weekly_cohort_sizes AS (
            SELECT 
                cohort_week,
                user_segment,
                COUNT(DISTINCT user_id) as cohort_size
            FROM user_cohorts
            GROUP BY cohort_week, user_segment
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_weekly_retention
        SELECT 
            ra.cohort_week,
            ra.retention_week,
            SUM(cs.cohort_size) as total_cohort_size,
            SUM(ra.active_users) as total_retained_users,
            ROUND((SUM(ra.active_users) * 100.0 / SUM(cs.cohort_size)), 2) as retention_rate,
            MAP_FROM_ARRAYS(
                COLLECT_LIST(ra.user_segment),
                COLLECT_LIST(ROUND((ra.active_users * 100.0 / cs.cohort_size), 2))
            ) as retention_by_segment,
            LAG(ROUND((SUM(ra.active_users) * 100.0 / SUM(cs.cohort_size)), 2)) OVER (
                PARTITION BY ra.cohort_week ORDER BY ra.retention_week
            ) as cumulative_retention_rate,
            ROUND((100.0 - (SUM(ra.active_users) * 100.0 / SUM(cs.cohort_size))), 2) as churn_rate,
            CURRENT_TIMESTAMP() as created_at
        FROM weekly_retention_analysis ra
        JOIN weekly_cohort_sizes cs ON ra.cohort_week = cs.cohort_week AND ra.user_segment = cs.user_segment
        WHERE ra.cohort_week >= CURRENT_DATE() - INTERVAL 90 DAYS
        GROUP BY ra.cohort_week, ra.retention_week
        ORDER BY ra.cohort_week, ra.retention_week
        """
        
        self.spark.sql(weekly_retention_query)
        
        # Monthly Retention
        monthly_retention_query = f"""
        WITH monthly_user_cohorts AS (
            SELECT 
                user_id,
                DATE_TRUNC('month', MIN(date)) as cohort_month,
                user_segment
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            GROUP BY user_id, user_segment
        ),
        monthly_user_activity AS (
            SELECT 
                uc.user_id,
                uc.cohort_month,
                uc.user_segment,
                DATE_TRUNC('month', ues.date) as activity_month,
                MONTHS_BETWEEN(DATE_TRUNC('month', ues.date), uc.cohort_month) as months_since_cohort
            FROM monthly_user_cohorts uc
            JOIN {self.catalog_name}.{self.database_name}.user_events_silver ues ON uc.user_id = ues.user_id
        ),
        monthly_retention_analysis AS (
            SELECT 
                cohort_month,
                months_since_cohort as retention_month,
                COUNT(DISTINCT user_id) as active_users
            FROM monthly_user_activity
            WHERE months_since_cohort >= 0 AND months_since_cohort <= 12
            GROUP BY cohort_month, months_since_cohort
        ),
        monthly_cohort_sizes AS (
            SELECT 
                cohort_month,
                COUNT(DISTINCT user_id) as cohort_size
            FROM monthly_user_cohorts
            GROUP BY cohort_month
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_monthly_retention
        SELECT 
            ra.cohort_month,
            ra.retention_month,
            cs.cohort_size,
            ra.active_users as retained_users,
            ROUND((ra.active_users * 100.0 / cs.cohort_size), 2) as retention_rate,
            ra.active_users * 10.0 as ltv_estimate,  -- 간단한 LTV 추정
            ROUND((100.0 - (ra.active_users * 100.0 / cs.cohort_size)), 2) as churn_probability,
            CURRENT_TIMESTAMP() as created_at
        FROM monthly_retention_analysis ra
        JOIN monthly_cohort_sizes cs ON ra.cohort_month = cs.cohort_month
        WHERE ra.cohort_month >= CURRENT_DATE() - INTERVAL 12 MONTHS
        ORDER BY ra.cohort_month, ra.retention_month
        """
        
        self.spark.sql(monthly_retention_query)
        print("✅ Weekly & Monthly Retention 메트릭 계산 완료!")
        
    def calculate_stickiness_metrics(self):
        """6. STICKINESS 메트릭 계산"""
        print("\n🎯 Stickiness 메트릭 계산 중...")
        
        stickiness_query = f"""
        WITH daily_activity AS (
            SELECT 
                date,
                user_id,
                COUNT(*) as daily_events,
                COUNT(DISTINCT session_id) as daily_sessions
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            GROUP BY date, user_id
        ),
        monthly_activity AS (
            SELECT 
                DATE_TRUNC('month', date) as month,
                user_id,
                SUM(daily_events) as monthly_events,
                SUM(daily_sessions) as monthly_sessions,
                COUNT(DISTINCT date) as active_days_in_month
            FROM daily_activity
            GROUP BY DATE_TRUNC('month', date), user_id
        ),
        daily_aggregates AS (
            SELECT 
                date,
                COUNT(DISTINCT user_id) as daily_active_users,
                AVG(daily_sessions) as avg_sessions_per_user,
                AVG(daily_events) as avg_events_per_user
            FROM daily_activity
            GROUP BY date
        ),
        monthly_aggregates AS (
            SELECT 
                month,
                COUNT(DISTINCT user_id) as monthly_active_users,
                COUNT(DISTINCT CASE WHEN active_days_in_month >= 15 THEN user_id END) as power_users
            FROM monthly_activity
            GROUP BY month
        ),
        stickiness_calculation AS (
            SELECT 
                CAST(da.date AS DATE) as date,
                da.daily_active_users,
                ma.monthly_active_users,
                ROUND((da.daily_active_users * 1.0 / ma.monthly_active_users), 4) as stickiness_ratio,
                ROUND(da.avg_sessions_per_user, 2) as avg_sessions_per_user,
                ROUND(da.avg_events_per_user, 2) as avg_events_per_user,
                ROUND((ma.power_users * 100.0 / ma.monthly_active_users), 2) as power_user_ratio
            FROM daily_aggregates da
            JOIN monthly_aggregates ma ON DATE_TRUNC('month', da.date) = ma.month
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_stickiness
        SELECT 
            date,
            daily_active_users,
            monthly_active_users,
            stickiness_ratio,
            avg_sessions_per_user,
            avg_events_per_user,
            power_user_ratio,
            ROUND((stickiness_ratio * avg_sessions_per_user * power_user_ratio / 100), 2) as engagement_depth_score,
            CURRENT_TIMESTAMP() as created_at
        FROM stickiness_calculation
        ORDER BY date
        """
        
        self.spark.sql(stickiness_query)
        print("✅ Stickiness 메트릭 계산 완료!")
        
    def calculate_funnel_metrics(self):
        """7. FUNNEL 메트릭 계산"""
        print("\n🎪 Funnel 메트릭 계산 중...")
        
        funnel_query = f"""
        WITH funnel_stages AS (
            SELECT 
                date,
                user_id,
                session_id,
                timestamp,
                CASE 
                    WHEN event_name = 'view_page' AND page_name = 'home' THEN 'Landing'
                    WHEN event_name = 'search_recipe' THEN 'Search'
                    WHEN event_name = 'view_recipe' THEN 'Recipe_View'
                    WHEN event_name = 'click_bookmark' THEN 'Bookmark'
                    WHEN event_name = 'auth_success' THEN 'Signup'
                    WHEN event_name = 'create_comment' THEN 'Engagement'
                    ELSE NULL
                END as funnel_stage,
                ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY timestamp) as event_order
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE event_name IN ('view_page', 'search_recipe', 'view_recipe', 'click_bookmark', 'auth_success', 'create_comment')
        ),
        stage_order_mapping AS (
            SELECT 'Landing' as stage, 1 as stage_order
            UNION ALL SELECT 'Search', 2
            UNION ALL SELECT 'Recipe_View', 3
            UNION ALL SELECT 'Bookmark', 4
            UNION ALL SELECT 'Signup', 5
            UNION ALL SELECT 'Engagement', 6
        ),
        funnel_analysis AS (
            SELECT 
                date,
                funnel_stage,
                som.stage_order,
                COUNT(DISTINCT user_id) as stage_users,
                AVG(event_order) as avg_steps_to_reach
            FROM funnel_stages fs
            JOIN stage_order_mapping som ON fs.funnel_stage = som.stage
            WHERE funnel_stage IS NOT NULL
            GROUP BY date, funnel_stage, som.stage_order
        ),
        funnel_with_conversion AS (
            SELECT 
                date,
                funnel_stage,
                stage_order,
                stage_users,
                LAG(stage_users) OVER (PARTITION BY date ORDER BY stage_order) as previous_stage_users,
                FIRST_VALUE(stage_users) OVER (PARTITION BY date ORDER BY stage_order) as landing_users,
                avg_steps_to_reach
            FROM funnel_analysis
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_funnel
        SELECT 
            date,
            funnel_stage,
            stage_order,
            stage_users as total_users,
            CASE 
                WHEN previous_stage_users > 0 
                THEN ROUND((stage_users * 100.0 / previous_stage_users), 2)
                ELSE 100.0
            END as conversion_from_previous,
            CASE 
                WHEN landing_users > 0 
                THEN ROUND((stage_users * 100.0 / landing_users), 2)
                ELSE 0.0
            END as cumulative_conversion,
            CASE 
                WHEN previous_stage_users > 0 
                THEN ROUND(((previous_stage_users - stage_users) * 100.0 / previous_stage_users), 2)
                ELSE 0.0
            END as drop_off_rate,
            ROUND(avg_steps_to_reach, 2) as avg_time_to_convert,
            CURRENT_TIMESTAMP() as created_at
        FROM funnel_with_conversion
        ORDER BY date, stage_order
        """
        
        self.spark.sql(funnel_query)
        print("✅ Funnel 메트릭 계산 완료!")
        
    def calculate_visitor_metrics(self):
        """8. COUNT_VISITORS 메트릭 계산"""
        print("\n🚶 Count Visitors 메트릭 계산 중...")
        
        visitor_query = f"""
        WITH visitor_sessions AS (
            SELECT 
                date,
                user_id,
                session_id,
                anonymous_id,
                MIN(timestamp) as session_start,
                MAX(timestamp) as session_end,
                COUNT(*) as session_events,
                COUNT(DISTINCT page_name) as unique_pages_visited,
                MIN(date) OVER (PARTITION BY COALESCE(user_id, anonymous_id)) as first_visit_date
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            GROUP BY date, user_id, session_id, anonymous_id
        ),
        daily_visitor_metrics AS (
            SELECT 
                date,
                COUNT(DISTINCT session_id) as total_visitors,
                COUNT(DISTINCT COALESCE(user_id, anonymous_id)) as unique_visitors,
                COUNT(DISTINCT CASE WHEN first_visit_date = date THEN COALESCE(user_id, anonymous_id) END) as new_visitors,
                COUNT(DISTINCT CASE WHEN first_visit_date < date THEN COALESCE(user_id, anonymous_id) END) as returning_visitors,
                AVG(UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) as avg_session_duration_seconds,
                COUNT(DISTINCT CASE WHEN session_events = 1 THEN session_id END) as bounce_sessions,
                AVG(unique_pages_visited) as avg_pages_per_session
            FROM visitor_sessions
            GROUP BY date
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_count_visitors
        SELECT 
            date,
            total_visitors,
            unique_visitors,
            new_visitors,
            returning_visitors,
            ROUND(avg_session_duration_seconds / 60.0, 2) as avg_session_duration,
            ROUND((bounce_sessions * 100.0 / total_visitors), 2) as bounce_rate,
            ROUND(avg_pages_per_session, 2) as pages_per_session,
            MAP('Direct', unique_visitors * 0.4, 'Search', unique_visitors * 0.3, 'Social', unique_visitors * 0.2, 'Other', unique_visitors * 0.1) as traffic_source_breakdown,
            CURRENT_TIMESTAMP() as created_at
        FROM daily_visitor_metrics
        ORDER BY date
        """
        
        self.spark.sql(visitor_query)
        print("✅ Count Visitors 메트릭 계산 완료!")
        
    def calculate_ree_segmentation(self):
        """9. REE_SEGMENTATION (Recipe RFM) 메트릭 계산"""
        print("\n🏆 REE Segmentation 메트릭 계산 중...")
        
        ree_query = f"""
        WITH user_activity_summary AS (
            SELECT 
                user_id,
                MAX(date) as last_activity_date,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_id) as total_sessions,
                COUNT(CASE WHEN event_name = 'view_recipe' THEN 1 END) as recipe_views,
                COUNT(CASE WHEN event_name = 'create_comment' THEN 1 END) as comments_created,
                COUNT(CASE WHEN event_name = 'click_bookmark' THEN 1 END) as bookmarks_made,
                COUNT(DISTINCT prop_recipe_id) as unique_recipes_viewed,
                AVG(COALESCE(prop_session_duration, 0)) as avg_session_duration
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE user_id IS NOT NULL
            GROUP BY user_id
        ),
        ree_calculation AS (
            SELECT 
                user_id,
                CURRENT_DATE() as analysis_date,
                DATEDIFF(CURRENT_DATE(), last_activity_date) as recency_days,
                ROUND((total_events + recipe_views * 2 + comments_created * 5 + bookmarks_made * 3 + total_sessions), 2) as engagement_score,
                ROUND((unique_recipes_viewed * 1.5 + comments_created * 2 + bookmarks_made * 1.2 + avg_session_duration / 60), 2) as expertise_level
            FROM user_activity_summary
        ),
        ree_scores AS (
            SELECT 
                *,
                NTILE(5) OVER (ORDER BY recency_days ASC) as recency_score,
                NTILE(5) OVER (ORDER BY engagement_score DESC) as engagement_score_quintile,
                NTILE(5) OVER (ORDER BY expertise_level DESC) as expertise_score_quintile
            FROM ree_calculation
        ),
        ree_segmentation AS (
            SELECT 
                *,
                (recency_score + engagement_score_quintile + expertise_score_quintile) as ree_total_score,
                CASE 
                    WHEN recency_score >= 4 AND engagement_score_quintile >= 4 AND expertise_score_quintile >= 4 THEN 'Recipe_Champions'
                    WHEN recency_score >= 3 AND engagement_score_quintile >= 4 THEN 'Loyal_Cooks'
                    WHEN recency_score >= 4 AND engagement_score_quintile <= 2 THEN 'New_Food_Lovers'
                    WHEN recency_score <= 2 AND engagement_score_quintile >= 3 THEN 'At_Risk_Users'
                    WHEN recency_score <= 2 AND engagement_score_quintile <= 2 THEN 'Lost_Users'
                    WHEN expertise_score_quintile >= 4 THEN 'Cooking_Experts'
                    WHEN engagement_score_quintile >= 3 THEN 'Active_Users'
                    ELSE 'Casual_Browsers'
                END as user_segment
            FROM ree_scores
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_ree_segmentation
        SELECT 
            user_id,
            analysis_date,
            recency_days,
            engagement_score,
            expertise_level,
            recency_score,
            engagement_score_quintile,
            expertise_score_quintile,
            ree_total_score,
            user_segment,
            CASE 
                WHEN user_segment = 'Recipe_Champions' THEN 'Top users with high engagement and expertise'
                WHEN user_segment = 'Loyal_Cooks' THEN 'Regular users with consistent activity'
                WHEN user_segment = 'New_Food_Lovers' THEN 'Recent users showing interest'
                WHEN user_segment = 'At_Risk_Users' THEN 'Previously active users needing re-engagement'
                WHEN user_segment = 'Lost_Users' THEN 'Inactive users requiring win-back campaigns'
                WHEN user_segment = 'Cooking_Experts' THEN 'Skilled users who could mentor others'
                WHEN user_segment = 'Active_Users' THEN 'Regular engaged users'
                ELSE 'Light users browsing occasionally'
            END as segment_description,
            CASE 
                WHEN user_segment = 'Recipe_Champions' THEN ARRAY('VIP content access', 'Beta feature testing', 'Community leadership')
                WHEN user_segment = 'Loyal_Cooks' THEN ARRAY('Premium recipes', 'Cooking challenges', 'Social features')
                WHEN user_segment = 'New_Food_Lovers' THEN ARRAY('Onboarding sequence', 'Beginner recipes', 'Tutorial content')
                WHEN user_segment = 'At_Risk_Users' THEN ARRAY('Re-engagement email', 'Special offers', 'New content notification')
                WHEN user_segment = 'Lost_Users' THEN ARRAY('Win-back campaign', 'Survey feedback', 'Incentive offers')
                WHEN user_segment = 'Cooking_Experts' THEN ARRAY('Advanced recipes', 'Recipe creation tools', 'Mentorship program')
                WHEN user_segment = 'Active_Users' THEN ARRAY('Personalized recommendations', 'Social sharing', 'Achievement badges')
                ELSE ARRAY('Content discovery', 'Interest-based recommendations', 'Simple engagement features')
            END as recommended_actions,
            CURRENT_TIMESTAMP() as created_at
        FROM ree_segmentation
        ORDER BY ree_total_score DESC
        """
        
        self.spark.sql(ree_query)
        print("✅ REE Segmentation 메트릭 계산 완료!")
        
    def calculate_recipe_performance_metrics(self):
        """10. RECIPE_PERFORMANCE 메트릭 계산"""
        print("\n🍳 Recipe Performance 메트릭 계산 중...")
        
        recipe_performance_query = f"""
        WITH recipe_activity AS (
            SELECT 
                date,
                prop_recipe_id as recipe_id,
                user_id,
                session_id,
                event_name,
                prop_session_duration
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE prop_recipe_id IS NOT NULL AND prop_recipe_id > 0
        ),
        daily_recipe_metrics AS (
            SELECT 
                date,
                recipe_id,
                COUNT(*) as total_views,
                COUNT(DISTINCT user_id) as unique_viewers,
                AVG(COALESCE(prop_session_duration, 0)) as avg_view_duration,
                COUNT(CASE WHEN event_name = 'click_bookmark' THEN 1 END) as recipe_saves,
                COUNT(CASE WHEN event_name = 'share_recipe' THEN 1 END) as recipe_shares,
                COUNT(CASE WHEN event_name = 'create_comment' THEN 1 END) as recipe_comments,
                COUNT(CASE WHEN event_name = 'start_cooking' THEN 1 END) as cooking_attempts,
                COUNT(CASE WHEN event_name = 'complete_cooking' THEN 1 END) as cooking_completions
            FROM recipe_activity
            GROUP BY date, recipe_id
        ),
        recipe_performance_calculation AS (
            SELECT 
                *,
                CASE 
                    WHEN cooking_attempts > 0 
                    THEN ROUND((cooking_completions * 100.0 / cooking_attempts), 2)
                    ELSE 0
                END as success_rate,
                ROUND((total_views * 0.1 + unique_viewers * 0.3 + recipe_saves * 2 + recipe_comments * 3 + recipe_shares * 5), 2) as engagement_score,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_views DESC) as daily_view_rank,
                ROUND((recipe_shares * 1.0 / NULLIF(unique_viewers, 0)), 4) as viral_coefficient
            FROM daily_recipe_metrics
        ),
        trending_calculation AS (
            SELECT 
                *,
                CASE 
                    WHEN daily_view_rank <= 10 THEN 100
                    WHEN daily_view_rank <= 50 THEN 80
                    WHEN daily_view_rank <= 100 THEN 60
                    WHEN daily_view_rank <= 500 THEN 40
                    ELSE 20
                END as trending_score,
                ROUND((engagement_score * 0.4 + 
                       CASE WHEN daily_view_rank <= 100 THEN 100 - daily_view_rank ELSE 0 END * 0.3 + 
                       viral_coefficient * 1000 * 0.3), 2) as recommendation_score
            FROM recipe_performance_calculation
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_recipe_performance
        SELECT 
            date,
            recipe_id,
            total_views,
            unique_viewers,
            ROUND(avg_view_duration / 60.0, 2) as avg_view_duration,
            recipe_saves,
            recipe_shares,
            recipe_comments,
            cooking_attempts,
            success_rate,
            engagement_score,
            trending_score,
            recommendation_score,
            viral_coefficient,
            CURRENT_TIMESTAMP() as created_at
        FROM trending_calculation
        ORDER BY date, engagement_score DESC
        """
        
        self.spark.sql(recipe_performance_query)
        print("✅ Recipe Performance 메트릭 계산 완료!")
        
    def calculate_ab_test_metrics(self):
        """A/B 테스트 메트릭 계산"""
        print("\n🧪 A/B Test 메트릭 계산 중...")
        
        # A/B 테스트 결과 분석
        ab_test_query = f"""
        WITH ab_test_data AS (
            SELECT 
                COALESCE(ab_test_group, 'control') as variant_group,
                user_id,
                date,
                event_name,
                COUNT(*) as event_count,
                COUNT(DISTINCT session_id) as session_count,
                COUNT(CASE WHEN event_name IN ('auth_success', 'create_comment', 'click_bookmark') THEN 1 END) as conversion_events
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE ab_test_group IS NOT NULL
            GROUP BY COALESCE(ab_test_group, 'control'), user_id, date, event_name
        ),
        daily_ab_metrics AS (
            SELECT 
                'recipe_homepage_test' as test_id,
                'Recipe Homepage A/B Test' as test_name,
                variant_group,
                date as analysis_date,
                COUNT(DISTINCT user_id) as sample_size,
                SUM(event_count) as total_events,
                SUM(session_count) as total_sessions,
                SUM(conversion_events) as total_conversions,
                AVG(event_count) as avg_events_per_user,
                ROUND((SUM(conversion_events) * 100.0 / COUNT(DISTINCT user_id)), 4) as conversion_rate
            FROM ab_test_data
            GROUP BY variant_group, date
        ),
        control_baseline AS (
            SELECT 
                analysis_date,
                conversion_rate as control_conversion_rate,
                avg_events_per_user as control_avg_events
            FROM daily_ab_metrics
            WHERE variant_group = 'control'
        ),
        ab_test_results AS (
            SELECT 
                ab.test_id,
                ab.test_name,
                ab.variant_group,
                ab.analysis_date,
                ab.sample_size,
                ab.conversion_rate as metric_value,
                cb.control_conversion_rate as control_value,
                CASE 
                    WHEN cb.control_conversion_rate > 0 
                    THEN ROUND(((ab.conversion_rate - cb.control_conversion_rate) * 100.0 / cb.control_conversion_rate), 4)
                    ELSE 0
                END as lift_percentage,
                -- 간단한 통계적 유의성 계산 (정확한 계산을 위해서는 더 복잡한 통계 분석 필요)
                CASE 
                    WHEN ab.sample_size >= 1000 AND ABS(ab.conversion_rate - cb.control_conversion_rate) > 0.5 
                    THEN TRUE 
                    ELSE FALSE 
                END as statistical_significance,
                DATEDIFF(ab.analysis_date, MIN(ab.analysis_date) OVER (PARTITION BY ab.test_id)) as test_duration_days
            FROM daily_ab_metrics ab
            LEFT JOIN control_baseline cb ON ab.analysis_date = cb.analysis_date
            WHERE ab.variant_group != 'control'
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_ab_test_results
        SELECT 
            test_id,
            test_name,
            variant_group,
            'conversion_rate' as metric_name,
            analysis_date,
            sample_size,
            metric_value,
            control_value,
            lift_percentage,
            metric_value - 1.96 * SQRT(metric_value * (100 - metric_value) / sample_size) as confidence_interval_lower,
            metric_value + 1.96 * SQRT(metric_value * (100 - metric_value) / sample_size) as confidence_interval_upper,
            CASE WHEN statistical_significance THEN 0.05 ELSE 0.2 END as p_value,
            statistical_significance,
            95.0 as confidence_level,
            CASE 
                WHEN test_duration_days >= 14 AND statistical_significance THEN 'Complete'
                WHEN test_duration_days >= 14 THEN 'Inconclusive'
                ELSE 'Running'
            END as test_status,
            test_duration_days,
            CURRENT_TIMESTAMP() as created_at
        FROM ab_test_results
        ORDER BY analysis_date, variant_group
        """
        
        self.spark.sql(ab_test_query)
        
        # A/B 테스트 코호트 분석
        ab_cohort_query = f"""
        WITH ab_user_cohorts AS (
            SELECT 
                COALESCE(ab_test_group, 'control') as variant_group,
                user_id,
                DATE_TRUNC('week', MIN(date)) as cohort_week
            FROM {self.catalog_name}.{self.database_name}.user_events_silver
            WHERE ab_test_group IS NOT NULL
            GROUP BY COALESCE(ab_test_group, 'control'), user_id
        ),
        ab_weekly_activity AS (
            SELECT 
                uc.variant_group,
                uc.user_id,
                uc.cohort_week,
                DATE_TRUNC('week', ues.date) as activity_week,
                DATEDIFF(DATE_TRUNC('week', ues.date), uc.cohort_week) / 7 as week_number,
                COUNT(*) as weekly_events,
                COUNT(CASE WHEN ues.event_name IN ('auth_success', 'create_comment', 'click_bookmark') THEN 1 END) as weekly_conversions
            FROM ab_user_cohorts uc
            JOIN {self.catalog_name}.{self.database_name}.user_events_silver ues 
                ON uc.user_id = ues.user_id 
                AND COALESCE(ues.ab_test_group, 'control') = uc.variant_group
            GROUP BY uc.variant_group, uc.user_id, uc.cohort_week, DATE_TRUNC('week', ues.date)
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.database_name}.metrics_ab_test_cohort
        SELECT 
            'recipe_homepage_test' as test_id,
            variant_group,
            cohort_week,
            week_number,
            COUNT(DISTINCT user_id) OVER (PARTITION BY variant_group, cohort_week, 0) as initial_users,
            COUNT(DISTINCT user_id) as active_users,
            ROUND((COUNT(DISTINCT user_id) * 100.0 / 
                   COUNT(DISTINCT user_id) OVER (PARTITION BY variant_group, cohort_week, 0)), 2) as retention_rate,
            AVG(weekly_events) as avg_engagement_score,
            ROUND((SUM(weekly_conversions) * 100.0 / COUNT(DISTINCT user_id)), 2) as conversion_rate,
            SUM(weekly_conversions) * 1.5 as revenue_per_user,  -- 가상의 수익 계산
            CURRENT_TIMESTAMP() as created_at
        FROM ab_weekly_activity
        WHERE week_number >= 0 AND week_number <= 8
        GROUP BY variant_group, cohort_week, week_number
        ORDER BY variant_group, cohort_week, week_number
        """
        
        self.spark.sql(ab_cohort_query)
        print("✅ A/B Test 메트릭 계산 완료!")
        
    def run_full_pipeline(self):
        """전체 Gold Layer Star Schema + 10개 핵심 메트릭 파이프라인 실행"""
        print("🚀 Gold Layer Star Schema + 비즈니스 메트릭 파이프라인 시작!")
        print("=" * 70)
        
        try:
            # 1. SparkSession 생성
            self.create_spark_session()
            
            # 2. Dimension & Fact 테이블 생성
            self.create_dimension_tables()
            self.create_fact_table()
            
            # 3. Time Dimension 데이터 생성
            self.populate_time_dimension()
            
            # 4. Silver에서 Dimension 데이터 추출
            self.populate_dimensions_from_silver()
            
            # 5. 12개 메트릭 테이블 생성 (10개 핵심 + 2개 A/B 테스트)
            self.create_metrics_tables()
            
            # 6. 10개 핵심 비즈니스 메트릭 계산
            print("\n🎯 10개 핵심 비즈니스 메트릭 계산 시작...")
            self.calculate_dau_metrics()                    # 1. ACTIVE_USERS (DAU/WAU/MAU)
            self.calculate_event_time_distribution()        # 2. EVENT_TYPE_TIME_DISTRIBUTION
            self.calculate_conversion_metrics()             # 3. CONVERSION_RATE
            self.calculate_retention_metrics()              # 4. WEEKLY_RETENTION & 5. MONTHLY_RETENTION
            self.calculate_stickiness_metrics()             # 6. STICKINESS
            self.calculate_funnel_metrics()                 # 7. FUNNEL
            self.calculate_visitor_metrics()                # 8. COUNT_VISITORS
            self.calculate_ree_segmentation()               # 9. REE_SEGMENTATION (Recipe RFM)
            self.calculate_recipe_performance_metrics()     # 10. RECIPE_PERFORMANCE
            
            # 7. A/B 테스트 메트릭 계산
            print("\n🧪 A/B 테스트 메트릭 계산 시작...")
            self.calculate_ab_test_metrics()                # 11. A/B Test Results + Cohort
            
            # 8. 결과 요약 및 검증
            print("\n" + "=" * 70)
            print("🎉 Gold Layer Star Schema + 비즈니스 메트릭 구축 완료!")
            
            # 생성된 테이블 목록 확인
            print("\n📊 생성된 Gold Layer 테이블:")
            tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.database_name}")
            
            # 필터링을 위한 올바른 문법 사용
            gold_tables = tables_df.filter(
                (tables_df.tableName.contains("dim_")) |
                (tables_df.tableName.contains("fact_")) |
                (tables_df.tableName.contains("metrics_"))
            )
            gold_tables.show(50, truncate=False)
            
            # 각 테이블 행 수 확인
            print("\n📈 테이블별 데이터 현황:")
            table_summary = []
            
            for table_row in gold_tables.collect():
                table_name = table_row['tableName']
                try:
                    count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.database_name}.{table_name}").collect()[0]['cnt']
                    table_summary.append((table_name, count))
                    print(f"  📋 {table_name}: {count:,}개 행")
                except Exception as e:
                    print(f"  ❌ {table_name}: 오류 - {str(e)}")
            
            # 메트릭 샘플 확인
            print("\n🔍 주요 메트릭 샘플 확인:")
            
            # 1. DAU 메트릭 샘플
            try:
                dau_sample = self.spark.sql(f"""
                    SELECT date, dau, new_users, returning_users, dau_growth_rate 
                    FROM {self.catalog_name}.{self.database_name}.metrics_active_users 
                    ORDER BY date DESC LIMIT 3
                """)
                print("\n👥 Active Users (최근 3일):")
                dau_sample.show(truncate=False)
            except:
                print("👥 Active Users: 데이터 없음")
            
            # 2. Retention 메트릭 샘플
            try:
                retention_sample = self.spark.sql(f"""
                    SELECT cohort_week, retention_week, cohort_size, retained_users, retention_rate 
                    FROM {self.catalog_name}.{self.database_name}.metrics_weekly_retention 
                    WHERE retention_week <= 4
                    ORDER BY cohort_week DESC, retention_week 
                    LIMIT 5
                """)
                print("\n🔄 Weekly Retention (최근 코호트, 4주차까지):")
                retention_sample.show(truncate=False)
            except:
                print("🔄 Weekly Retention: 데이터 없음")
            
            # 3. REE Segmentation 샘플
            try:
                ree_sample = self.spark.sql(f"""
                    SELECT user_segment, COUNT(*) as user_count, 
                           AVG(engagement_score) as avg_engagement,
                           AVG(expertise_level) as avg_expertise
                    FROM {self.catalog_name}.{self.database_name}.metrics_ree_segmentation 
                    GROUP BY user_segment
                    ORDER BY user_count DESC
                """)
                print("\n🏆 REE Segmentation (사용자 세그먼트별):")
                ree_sample.show(truncate=False)
            except:
                print("🏆 REE Segmentation: 데이터 없음")
            
            # 4. A/B 테스트 결과 샘플
            try:
                ab_sample = self.spark.sql(f"""
                    SELECT variant_group, metric_name, 
                           AVG(metric_value) as avg_metric_value,
                           AVG(lift_percentage) as avg_lift,
                           COUNT(CASE WHEN statistical_significance THEN 1 END) as significant_days
                    FROM {self.catalog_name}.{self.database_name}.metrics_ab_test_results 
                    GROUP BY variant_group, metric_name
                    ORDER BY variant_group
                """)
                print("\n🧪 A/B Test Results (변형별 요약):")
                ab_sample.show(truncate=False)
            except:
                print("🧪 A/B Test Results: 데이터 없음")
            
            # 5. 총합 통계
            print("\n📊 전체 데이터 통계:")
            total_events = sum([count for name, count in table_summary if 'metrics_' in name])
            print(f"  🎯 총 메트릭 레코드: {total_events:,}개")
            print(f"  📅 생성된 테이블: {len(table_summary)}개")
            print(f"  💾 Gold Layer 데이터베이스: {self.catalog_name}.{self.database_name}")
            
            return True
            
        except Exception as e:
            print(f"❌ 파이프라인 실행 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if self.spark:
                self.spark.stop()
                print("\n🔚 SparkSession 종료")

def main():
    """메인 실행 함수"""
    pipeline = GoldLayerStarSchema()
    success = pipeline.run_full_pipeline()
    
    if success:
        print("\n🎊 파이프라인 실행 성공!")
        print("🔗 다음 단계 가이드:")
        print("  1. Docker 환경에서 실행: python gold_layer_star_schema.py")
        print("  2. BI 도구 연결: Hive Metastore (thrift://localhost:9083)")
        print("  3. 메트릭 대시보드 구축")
        print("  4. A/B 테스트 결과 모니터링")
    else:
        print("\n💥 파이프라인 실행 실패. 로그를 확인해주세요.")

def analyze_ab_test_results():
    """A/B 테스트 결과 심화 분석 유틸리티"""
    print("🧪 A/B 테스트 결과 심화 분석")
    print("=" * 50)
    
    spark = SparkSession.builder \
        .appName("AB_Test_Analysis") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
        .getOrCreate()
    
    try:
        # A/B 테스트 요약 분석
        summary_query = """
        SELECT 
            test_id,
            variant_group,
            COUNT(*) as analysis_days,
            AVG(sample_size) as avg_daily_sample_size,
            AVG(metric_value) as avg_conversion_rate,
            AVG(lift_percentage) as avg_lift,
            COUNT(CASE WHEN statistical_significance THEN 1 END) as significant_days,
            MAX(test_duration_days) as total_test_days,
            FIRST(test_status) as current_status
        FROM iceberg_catalog.recipe_analytics.metrics_ab_test_results
        GROUP BY test_id, variant_group
        ORDER BY test_id, variant_group
        """
        
        print("📊 A/B 테스트 요약:")
        spark.sql(summary_query).show(truncate=False)
        
        # 통계적 유의성 분석
        significance_query = """
        SELECT 
            test_id,
            variant_group,
            CASE 
                WHEN AVG(CASE WHEN statistical_significance THEN 1.0 ELSE 0.0 END) >= 0.8 
                THEN 'Highly Significant'
                WHEN AVG(CASE WHEN statistical_significance THEN 1.0 ELSE 0.0 END) >= 0.5 
                THEN 'Moderately Significant'
                ELSE 'Not Significant'
            END as significance_level,
            AVG(lift_percentage) as avg_lift,
            MIN(confidence_interval_lower) as min_ci_lower,
            MAX(confidence_interval_upper) as max_ci_upper
        FROM iceberg_catalog.recipe_analytics.metrics_ab_test_results
        GROUP BY test_id, variant_group
        ORDER BY avg_lift DESC
        """
        
        print("\n🎯 통계적 유의성 분석:")
        spark.sql(significance_query).show(truncate=False)
        
        # 권장사항 생성
        recommendation_query = """
        WITH test_performance AS (
            SELECT 
                test_id,
                variant_group,
                AVG(lift_percentage) as avg_lift,
                AVG(CASE WHEN statistical_significance THEN 1.0 ELSE 0.0 END) as significance_ratio,
                MAX(test_duration_days) as test_days
            FROM iceberg_catalog.recipe_analytics.metrics_ab_test_results
            GROUP BY test_id, variant_group
        )
        SELECT 
            test_id,
            variant_group,
            avg_lift,
            significance_ratio,
            test_days,
            CASE 
                WHEN avg_lift > 5 AND significance_ratio >= 0.8 THEN 'IMPLEMENT - Strong Winner'
                WHEN avg_lift > 2 AND significance_ratio >= 0.6 THEN 'IMPLEMENT - Moderate Winner'
                WHEN avg_lift > 0 AND significance_ratio >= 0.5 THEN 'MONITOR - Promising'
                WHEN avg_lift <= 0 AND significance_ratio >= 0.5 THEN 'REJECT - Clear Loser'
                WHEN test_days < 14 THEN 'CONTINUE - More Data Needed'
                ELSE 'INCONCLUSIVE - Consider Redesign'
            END as recommendation
        FROM test_performance
        WHERE variant_group != 'control'
        ORDER BY avg_lift DESC
        """
        
        print("\n💡 A/B 테스트 권장사항:")
        spark.sql(recommendation_query).show(truncate=False)
        
    finally:
        spark.stop()

def generate_business_dashboard_queries():
    """비즈니스 대시보드용 SQL 쿼리 생성"""
    queries = {
        "daily_active_users": """
        -- DAU 트렌드 (최근 30일)
        SELECT 
            date,
            dau,
            new_users,
            returning_users,
            dau_growth_rate,
            dau_7d_avg
        FROM iceberg_catalog.recipe_analytics.metrics_active_users
        WHERE date >= CURRENT_DATE() - INTERVAL 30 DAYS
        ORDER BY date DESC
        """,
        
        "retention_cohort_heatmap": """
        -- 리텐션 코호트 히트맵 데이터
        SELECT 
            cohort_week,
            retention_week,
            retention_rate
        FROM iceberg_catalog.recipe_analytics.metrics_weekly_retention
        WHERE cohort_week >= CURRENT_DATE() - INTERVAL 12 WEEKS
          AND retention_week <= 8
        ORDER BY cohort_week, retention_week
        """,
        
        "conversion_funnel": """
        -- 전환 퍼널 (최근 7일 평균)
        SELECT 
            funnel_stage,
            stage_order,
            AVG(total_users) as avg_users,
            AVG(conversion_from_previous) as avg_conversion_rate,
            AVG(drop_off_rate) as avg_drop_off_rate
        FROM iceberg_catalog.recipe_analytics.metrics_funnel
        WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS
        GROUP BY funnel_stage, stage_order
        ORDER BY stage_order
        """,
        
        "recipe_performance_top10": """
        -- 상위 10개 레시피 성과 (최근 7일)
        SELECT 
            r.recipe_id,
            AVG(r.total_views) as avg_daily_views,
            AVG(r.unique_viewers) as avg_daily_unique_viewers,
            AVG(r.engagement_score) as avg_engagement_score,
            AVG(r.viral_coefficient) as avg_viral_coefficient,
            MAX(r.trending_score) as max_trending_score
        FROM iceberg_catalog.recipe_analytics.metrics_recipe_performance r
        WHERE r.date >= CURRENT_DATE() - INTERVAL 7 DAYS
        GROUP BY r.recipe_id
        ORDER BY avg_engagement_score DESC
        LIMIT 10
        """,
        
        "user_segmentation_distribution": """
        -- REE 사용자 세그먼트 분포
        SELECT 
            user_segment,
            COUNT(*) as user_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
            AVG(engagement_score) as avg_engagement,
            AVG(expertise_level) as avg_expertise,
            AVG(recency_days) as avg_recency
        FROM iceberg_catalog.recipe_analytics.metrics_ree_segmentation
        WHERE analysis_date = CURRENT_DATE()
        GROUP BY user_segment
        ORDER BY user_count DESC
        """,
        
        "ab_test_dashboard": """
        -- A/B 테스트 대시보드 (최근 결과)
        SELECT 
            test_name,
            variant_group,
            sample_size,
            metric_value as conversion_rate,
            lift_percentage,
            statistical_significance,
            test_status,
            confidence_interval_lower,
            confidence_interval_upper
        FROM iceberg_catalog.recipe_analytics.metrics_ab_test_results
        WHERE analysis_date = (SELECT MAX(analysis_date) FROM iceberg_catalog.recipe_analytics.metrics_ab_test_results)
        ORDER BY test_name, variant_group
        """,
        
        "business_kpi_summary": """
        -- 핵심 비즈니스 KPI 요약 (최근 7일)
        WITH recent_metrics AS (
            SELECT 
                'DAU' as metric_name,
                AVG(dau) as current_value,
                AVG(dau_growth_rate) as growth_rate,
                'users' as unit
            FROM iceberg_catalog.recipe_analytics.metrics_active_users
            WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS
            
            UNION ALL
            
            SELECT 
                'Stickiness' as metric_name,
                AVG(stickiness_ratio) * 100 as current_value,
                0 as growth_rate,
                'percentage' as unit
            FROM iceberg_catalog.recipe_analytics.metrics_stickiness
            WHERE date >= CURRENT_DATE() - INTERVAL 7 DAYS
            
            UNION ALL
            
            SELECT 
                'Week 1 Retention' as metric_name,
                AVG(retention_rate) as current_value,
                0 as growth_rate,
                'percentage' as unit
            FROM iceberg_catalog.recipe_analytics.metrics_weekly_retention
            WHERE retention_week = 1 
              AND cohort_week >= CURRENT_DATE() - INTERVAL 4 WEEKS
        )
        SELECT 
            metric_name,
            ROUND(current_value, 2) as current_value,
            ROUND(growth_rate, 2) as growth_rate,
            unit
        FROM recent_metrics
        ORDER BY metric_name
        """
    }
    
    print("📊 비즈니스 대시보드 SQL 쿼리 모음")
    print("=" * 50)
    
    for query_name, query in queries.items():
        print(f"\n-- {query_name.upper().replace('_', ' ')}")
        print(query.strip())
        print("\n" + "-" * 50)

if __name__ == "__main__":
    main()
