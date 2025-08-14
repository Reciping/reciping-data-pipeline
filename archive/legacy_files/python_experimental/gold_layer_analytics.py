# gold_layer_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def main():
    """
    Silver Layer에서 Gold Layer 비즈니스 집계 테이블을 생성하는 
    고급 분석 파이프라인 (Iceberg + Hive Metastore).
    """
    try:
        # 시스템 환경 변수 설정
        os.environ['HADOOP_USER_NAME'] = 'root'
        os.environ['USER'] = 'root'
        os.environ['HOME'] = '/tmp'
        os.environ['JAVA_OPTS'] = '-Duser.name=root'
        os.environ['IVY_HOME'] = '/tmp/.ivy2'
        os.makedirs('/tmp/.ivy2', exist_ok=True)

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (Iceberg + Hive Metastore 설정)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession with Iceberg for Gold Layer 생성...")
        
        spark = SparkSession.builder \
            .appName("Gold_Layer_Analytics_Pipeline") \
            .master("local[*]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.spark_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://reciping-user-event-logs/warehouse") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
            .config("spark.hadoop.fs.s3a.region", "ap-northeast-2") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession with Iceberg for Gold Layer 생성 완료!")

        # -----------------------------------------------------------------------------
        # 2. Silver 데이터 로드
        # -----------------------------------------------------------------------------
        print("\n📊 Silver Layer 데이터 로드...")
        df_silver = spark.table("silver_db.cleaned_events")
        silver_count = df_silver.count()
        print(f"✅ Silver에서 {silver_count:,}행의 데이터를 로드했습니다.")

        # -----------------------------------------------------------------------------
        # 3. 🥇 Gold Layer 집계 테이블들 생성
        # -----------------------------------------------------------------------------
        
        # --- 3.1. 일별 이벤트 요약 테이블 ---
        print("\n🥇 Gold Layer - 일별 이벤트 요약 테이블 생성...")
        
        daily_events_summary = df_silver.groupBy(
            "year", "month", "day", "event_name"
        ).agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            countDistinct("anonymous_id").alias("unique_anonymous_users"),
            min("event_timestamp").alias("first_event_time"),
            max("event_timestamp").alias("last_event_time"),
            current_timestamp().alias("aggregation_timestamp")
        ).withColumn(
            "event_date", 
            to_date(concat_ws("-", col("year"), 
                             lpad(col("month"), 2, "0"), 
                             lpad(col("day"), 2, "0")))
        )
        
        # 일별 요약 테이블 저장
        daily_events_summary.writeTo("gold_db.daily_events_summary") \
            .partitionedBy("year", "month") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print("✅ 일별 이벤트 요약 테이블 'gold_db.daily_events_summary' 생성 완료")

        # --- 3.2. 사용자별 행동 프로파일 테이블 ---
        print("\n🥇 Gold Layer - 사용자별 행동 프로파일 테이블 생성...")
        
        user_behavior_profiles = df_silver.filter(col("user_id").isNotNull()).groupBy("user_id").agg(
            # 기본 통계
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("event_name").alias("unique_event_types"),
            
            # 시간 정보
            min("event_timestamp").alias("first_seen"),
            max("event_timestamp").alias("last_seen"),
            
            # 이벤트별 카운트
            sum(when(col("event_name") == "view_page", 1).otherwise(0)).alias("page_views"),
            sum(when(col("event_name") == "search_recipe", 1).otherwise(0)).alias("recipe_searches"),
            sum(when(col("event_name") == "click_recipe", 1).otherwise(0)).alias("recipe_clicks"),
            sum(when(col("event_name") == "view_recipe_list", 1).otherwise(0)).alias("list_views"),
            
            # 사용자 속성 (최신 값)
            last("user_segment", True).alias("latest_user_segment"),
            last("activity_level", True).alias("latest_activity_level"),
            last("cooking_style", True).alias("latest_cooking_style"),
            last("ab_test_group", True).alias("latest_ab_test_group"),
            
            # 집계 시점
            current_timestamp().alias("profile_updated_at")
        ).withColumn(
            "avg_events_per_session",
            round(col("total_events") / col("total_sessions"), 2)
        ).withColumn(
            "days_active",
            datediff(col("last_seen"), col("first_seen")) + 1
        )
        
        # 사용자 프로파일 테이블 저장
        user_behavior_profiles.writeTo("gold_db.user_behavior_profiles") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print("✅ 사용자별 행동 프로파일 테이블 'gold_db.user_behavior_profiles' 생성 완료")

        # --- 3.3. 레시피 인기도 분석 테이블 ---
        print("\n🥇 Gold Layer - 레시피 인기도 분석 테이블 생성...")
        
        recipe_popularity = df_silver.filter(
            col("prop_recipe_id").isNotNull()
        ).groupBy("prop_recipe_id").agg(
            # 기본 통계
            count("*").alias("total_interactions"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            
            # 이벤트별 분석
            sum(when(col("event_name") == "click_recipe", 1).otherwise(0)).alias("clicks"),
            sum(when(col("event_name") == "view_recipe", 1).otherwise(0)).alias("views"),
            sum(when(col("event_name") == "bookmark_recipe", 1).otherwise(0)).alias("bookmarks"),
            sum(when(col("event_name") == "share_recipe", 1).otherwise(0)).alias("shares"),
            
            # 시간 정보
            min("event_timestamp").alias("first_interaction"),
            max("event_timestamp").alias("last_interaction"),
            
            # 카테고리 정보 (최신 값)
            last("prop_category", True).alias("recipe_category"),
            
            # 평균 순위 (검색 결과에서)
            avg("prop_rank").alias("avg_search_rank"),
            
            # 집계 시점
            current_timestamp().alias("analysis_updated_at")
        ).withColumn(
            "click_through_rate",
            round(col("clicks") / col("total_interactions"), 4)
        ).withColumn(
            "engagement_score",
            col("clicks") * 1.0 + col("views") * 0.5 + col("bookmarks") * 2.0 + col("shares") * 3.0
        )
        
        # 레시피 인기도 테이블 저장
        recipe_popularity.writeTo("gold_db.recipe_popularity_analysis") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print("✅ 레시피 인기도 분석 테이블 'gold_db.recipe_popularity_analysis' 생성 완료")

        # --- 3.4. 검색 트렌드 분석 테이블 ---
        print("\n🥇 Gold Layer - 검색 트렌드 분석 테이블 생성...")
        
        search_trends = df_silver.filter(
            (col("event_name") == "search_recipe") & 
            col("prop_search_keyword").isNotNull()
        ).groupBy(
            "year", "month", "day", "prop_search_keyword"
        ).agg(
            count("*").alias("search_count"),
            countDistinct("user_id").alias("unique_searchers"),
            countDistinct("session_id").alias("unique_sessions"),
            avg("prop_result_count").alias("avg_results_returned"),
            
            # 검색 타입별 분석
            sum(when(col("prop_search_type") == "ingredient", 1).otherwise(0)).alias("ingredient_searches"),
            sum(when(col("prop_search_type") == "name", 1).otherwise(0)).alias("name_searches"),
            sum(when(col("prop_search_type") == "category", 1).otherwise(0)).alias("category_searches"),
            
            # 시간 정보
            min("event_timestamp").alias("first_search_time"),
            max("event_timestamp").alias("last_search_time"),
            
            current_timestamp().alias("trend_updated_at")
        ).withColumn(
            "search_date", 
            to_date(concat_ws("-", col("year"), 
                             lpad(col("month"), 2, "0"), 
                             lpad(col("day"), 2, "0")))
        )
        
        # 검색 트렌드 테이블 저장
        search_trends.writeTo("gold_db.search_trends_analysis") \
            .partitionedBy("year", "month") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print("✅ 검색 트렌드 분석 테이블 'gold_db.search_trends_analysis' 생성 완료")

        # --- 3.5. A/B 테스트 성과 분석 테이블 ---
        print("\n🥇 Gold Layer - A/B 테스트 성과 분석 테이블 생성...")
        
        ab_test_analysis = df_silver.filter(
            col("ab_test_group").isNotNull()
        ).groupBy(
            "ab_test_scenario", "ab_test_group", "event_name"
        ).agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            
            # 전환율 계산을 위한 기본 데이터
            sum(when(col("event_name") == "click_recipe", 1).otherwise(0)).alias("recipe_clicks"),
            sum(when(col("event_name") == "bookmark_recipe", 1).otherwise(0)).alias("bookmarks"),
            sum(when(col("event_name") == "purchase", 1).otherwise(0)).alias("purchases"),
            
            # 시간 정보
            min("event_timestamp").alias("analysis_period_start"),
            max("event_timestamp").alias("analysis_period_end"),
            
            current_timestamp().alias("ab_analysis_updated_at")
        )
        
        # A/B 테스트 분석 테이블 저장
        ab_test_analysis.writeTo("gold_db.ab_test_performance") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print("✅ A/B 테스트 성과 분석 테이블 'gold_db.ab_test_performance' 생성 완료")

        # -----------------------------------------------------------------------------
        # 4. 📊 Gold Layer 테이블 검증 및 샘플 조회
        # -----------------------------------------------------------------------------
        print("\n📊 Gold Layer 테이블 검증...")
        
        # 생성된 Gold 테이블 목록
        print("\n🗃️ 생성된 Gold Layer 테이블:")
        spark.sql("SHOW TABLES IN gold_db").show()
        
        # 각 테이블 샘플 데이터 확인
        print("\n📋 일별 이벤트 요약 (상위 10행):")
        spark.sql("SELECT * FROM gold_db.daily_events_summary ORDER BY event_date DESC, event_count DESC LIMIT 10").show()
        
        print("\n👤 사용자 행동 프로파일 (상위 5행):")
        spark.sql("SELECT user_id, total_events, total_sessions, page_views, recipe_searches, latest_user_segment FROM gold_db.user_behavior_profiles ORDER BY total_events DESC LIMIT 5").show()
        
        print("\n🍳 레시피 인기도 (상위 10행):")
        spark.sql("SELECT prop_recipe_id, total_interactions, unique_users, clicks, engagement_score FROM gold_db.recipe_popularity_analysis ORDER BY engagement_score DESC LIMIT 10").show()
        
        print("\n🔍 검색 트렌드 (상위 10행):")
        spark.sql("SELECT prop_search_keyword, search_count, unique_searchers, search_date FROM gold_db.search_trends_analysis ORDER BY search_count DESC LIMIT 10").show()

        # -----------------------------------------------------------------------------
        # 5. 💎 고급 분석 쿼리 예제 실행
        # -----------------------------------------------------------------------------
        print("\n💎 고급 분석 쿼리 실행...")
        
        # 예제 1: 가장 활성 사용자 세그먼트 분석
        print("\n📈 사용자 세그먼트별 활동 분석:")
        spark.sql("""
            SELECT 
                latest_user_segment,
                COUNT(*) as user_count,
                AVG(total_events) as avg_events_per_user,
                AVG(total_sessions) as avg_sessions_per_user,
                AVG(days_active) as avg_days_active
            FROM gold_db.user_behavior_profiles
            WHERE latest_user_segment IS NOT NULL
            GROUP BY latest_user_segment
            ORDER BY avg_events_per_user DESC
        """).show()
        
        # 예제 2: 일별 트렌드 분석
        print("\n📅 최근 일별 활동 트렌드:")
        spark.sql("""
            SELECT 
                event_date,
                SUM(event_count) as total_events,
                SUM(unique_users) as total_unique_users,
                SUM(unique_sessions) as total_sessions
            FROM gold_db.daily_events_summary
            GROUP BY event_date
            ORDER BY event_date DESC
            LIMIT 7
        """).show()

        # -----------------------------------------------------------------------------
        # 6. 검증 및 요약
        # -----------------------------------------------------------------------------
        total_gold_tables = spark.sql("SHOW TABLES IN gold_db").count()
        
        print("\n📈 Gold Layer 구축 완료 요약:")
        print(f"🥈 Silver 입력 데이터: {silver_count:,}행")
        print(f"🥇 생성된 Gold 테이블 수: {total_gold_tables}")
        print(f"📊 주요 집계 테이블:")
        print(f"   - daily_events_summary: 일별 이벤트 요약")
        print(f"   - user_behavior_profiles: 사용자 행동 프로파일")
        print(f"   - recipe_popularity_analysis: 레시피 인기도 분석")
        print(f"   - search_trends_analysis: 검색 트렌드 분석")
        print(f"   - ab_test_performance: A/B 테스트 성과 분석")
        print(f"🏗️ 메타스토어: Hive Metastore")
        print(f"💾 저장소: Iceberg Tables on S3")

        # -----------------------------------------------------------------------------
        # 7. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("✅ Gold Layer 구축이 완료되었습니다!")

    except Exception as e:
        print(f"❌ Gold Layer 구축 실패: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    main()
