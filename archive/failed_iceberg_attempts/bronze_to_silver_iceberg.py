# bronze_to_silver_iceberg.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, date_format, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType, TimestampType

def main():
    """
    S3 랜딩 존의 원본 파일을 읽어 Bronze, Silver 아이스버그 테이블을 구축하는
    데이터 레이크하우스 ETL 파이프라인 (Iceberg + Hive Metastore).
    """
    try:
        # 시스템 환경 변수를 먼저 설정 (성공한 설정 적용)
        import os
        import subprocess
        
        # 성공한 환경 변수 설정
        os.environ['HADOOP_USER_NAME'] = 'root'
        os.environ['USER'] = 'root'
        os.environ['HOME'] = '/tmp'
        os.environ['JAVA_OPTS'] = '-Duser.name=root'
        # Ivy 설정
        os.environ['IVY_HOME'] = '/tmp/.ivy2'
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.jars.ivy=/tmp/.ivy2 pyspark-shell'
        
        # 필요한 디렉토리 생성
        os.makedirs('/tmp/.ivy2', exist_ok=True)

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (Iceberg + Hive Metastore 설정)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession with Iceberg 생성을 시도합니다...")
        
        spark = SparkSession.builder \
            .appName("Bronze_to_Silver_Iceberg_Pipeline") \
            .master("local[*]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalogImplementation", "hive") \
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
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.block.size", "134217728") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession with Iceberg가 성공적으로 생성되었습니다!")

        # -----------------------------------------------------------------------------
        # 2. 🏗️ 데이터베이스 생성 및 관리
        # -----------------------------------------------------------------------------
        print("\n🏗️ 데이터베이스 스키마 구성...")
        
        # Bronze, Silver, Gold 데이터베이스 생성
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze_db COMMENT 'Raw data from landing zone'")
        spark.sql("CREATE DATABASE IF NOT EXISTS silver_db COMMENT 'Cleaned and transformed data'")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold_db COMMENT 'Business aggregated data'")
        
        print("✅ 데이터베이스 스키마 구성 완료")
        
        # 현재 데이터베이스 목록 확인
        print("\n📋 현재 데이터베이스 목록:")
        spark.sql("SHOW DATABASES").show()

        # -----------------------------------------------------------------------------
        # 3. 🥉 Bronze Layer - Iceberg 테이블 구축
        # -----------------------------------------------------------------------------
        print("\n🥉 Bronze Layer (Iceberg) 구축 시작...")
        
        # S3 랜딩 존에서 데이터 읽기
        landing_zone_path = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
        print(f"📂 랜딩 존에서 데이터 읽기: {landing_zone_path}")
        
        try:
            df_raw = spark.read.json(landing_zone_path)
            row_count = df_raw.count()
            print(f"✅ 랜딩 존 데이터 로드 성공! 행 수: {row_count:,}")
            
            # 처리 타임스탬프 추가
            df_raw_with_metadata = df_raw.withColumn("ingestion_timestamp", current_timestamp())
            
            # Bronze Iceberg 테이블 존재 여부 확인
            try:
                existing_bronze = spark.table("bronze_db.raw_events")
                existing_count = existing_bronze.count()
                print(f"📊 기존 Bronze 테이블 행 수: {existing_count:,}")
                
                # 기존 데이터와 중복 제거를 위한 증분 로드
                max_timestamp = spark.sql("SELECT MAX(ingestion_timestamp) as max_ts FROM bronze_db.raw_events").collect()[0]["max_ts"]
                if max_timestamp:
                    print(f"🔄 증분 데이터 로드 모드 (마지막 적재: {max_timestamp})")
                    # 실제 운영에서는 이벤트 타임스탬프 기반으로 필터링
                    insert_mode = "append"
                else:
                    insert_mode = "overwrite"
            except:
                print("📋 Bronze Iceberg 테이블이 존재하지 않습니다. 새로 생성합니다.")
                insert_mode = "overwrite"
            
            # Bronze Iceberg 테이블 생성/업데이트 (기존 테이블 확인 후 처리)
            try:
                # 기존 테이블이 있으면 append, 없으면 create
                spark.sql("DESCRIBE TABLE bronze_db.raw_events")
                print("📋 기존 Bronze 테이블 발견. 데이터를 추가합니다.")
                df_raw_with_metadata.writeTo("bronze_db.raw_events").append()
            except:
                print("📋 새로운 Bronze 테이블을 생성합니다.")
                df_raw_with_metadata.writeTo("bronze_db.raw_events").tableProperty("format-version", "2").create()
            
            print("✅ Bronze Iceberg 테이블 'bronze_db.raw_events' 생성/업데이트 완료")
            
        except Exception as e:
            print(f"❌ 랜딩 존에서 데이터를 읽을 수 없습니다: {e}")
            print("💡 upload_to_landing_zone.py를 먼저 실행하여 데이터를 업로드하세요.")
            spark.stop()
            return

        # -----------------------------------------------------------------------------
        # 4. 🥈 Silver Layer - Iceberg 테이블 구축 (고급 변환)
        # -----------------------------------------------------------------------------
        print("\n🥈 Silver Layer (Iceberg) 구축 시작...")
        
        # Bronze Iceberg 테이블에서 데이터 읽기
        df_bronze = spark.table("bronze_db.raw_events")
        bronze_count = df_bronze.count()
        print(f"📊 Bronze에서 {bronze_count:,}행의 데이터를 읽었습니다.")

        # --- 4.1. 스키마 정의 (개선된 버전) ---
        context_schema = StructType([
            StructField("page", StructType([
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("path", StringType(), True)
            ]), True),
            StructField("user_segment", StringType(), True),
            StructField("activity_level", StringType(), True),
            StructField("cooking_style", StringType(), True),
            StructField("ab_test", StructType([
                StructField("scenario", StringType(), True),
                StructField("group", StringType(), True),
                StructField("start_date", StringType(), True),
                StructField("end_date", StringType(), True)
            ]), True)
        ])

        event_properties_schema = StructType([
            StructField("page_name", StringType(), True), StructField("referrer", StringType(), True),
            StructField("path", StringType(), True), StructField("method", StringType(), True),
            StructField("type", StringType(), True), StructField("search_type", StringType(), True),
            StructField("search_keyword", StringType(), True), StructField("selected_filters", ArrayType(StringType()), True),
            StructField("result_count", IntegerType(), True), StructField("list_type", StringType(), True),
            StructField("displayed_recipe_ids", ArrayType(StringType()), True), StructField("recipe_id", StringType(), True),
            StructField("rank", IntegerType(), True), StructField("action", StringType(), True),
            StructField("comment_length", IntegerType(), True), StructField("category", StringType(), True),
            StructField("ingredient_count", IntegerType(), True), StructField("ad_id", StringType(), True),
            StructField("ad_type", StringType(), True), StructField("position", StringType(), True),
            StructField("target_url", StringType(), True)
        ])

        # --- 4.2. JSON 파싱 및 타임스탬프 변환 ---
        print("🔧 JSON 파싱 및 타임스탬프 변환 중...")
        df_transformed = df_bronze \
            .withColumn("parsed_context", from_json(col("context"), context_schema)) \
            .withColumn("parsed_properties", from_json(col("event_properties"), event_properties_schema)) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
            .withColumn("date_parsed", col("date").cast(DateType())) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .drop("context", "event_properties")

        # --- 4.3. 파티션 컬럼 생성 (KST 기준) ---
        print("📅 파티션 컬럼 생성 중...")
        df_with_partitions = df_transformed \
            .withColumn("year", year(col("timestamp_parsed"))) \
            .withColumn("month", month(col("timestamp_parsed"))) \
            .withColumn("day", dayofmonth(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed")))

        # --- 4.4. 컬럼 평탄화 (향상된 버전) ---
        print("🗂️ 컬럼 평탄화 중...")
        df_silver_flat = df_with_partitions.select(
            # 기본 이벤트 정보
            "event_id", "event_name", "user_id", "anonymous_id", "session_id",
            col("timestamp_parsed").alias("event_timestamp"), 
            col("date_parsed").alias("event_date"),
            col("ingestion_timestamp"),
            col("processing_timestamp"),
            
            # 파티션 컬럼
            "year", "month", "day", "hour",
            
            # Context 정보
            col("parsed_context.page.name").alias("page_name"),
            col("parsed_context.page.url").alias("page_url"),
            col("parsed_context.page.path").alias("page_path"),
            col("parsed_context.user_segment").alias("user_segment"),
            col("parsed_context.activity_level").alias("activity_level"),
            col("parsed_context.cooking_style").alias("cooking_style"),
            col("parsed_context.ab_test.group").alias("ab_test_group"),
            col("parsed_context.ab_test.scenario").alias("ab_test_scenario"),
            
            # Event Properties
            col("parsed_properties.page_name").alias("prop_page_name"),
            col("parsed_properties.referrer").alias("prop_referrer"),
            col("parsed_properties.path").alias("prop_path"),
            col("parsed_properties.method").alias("prop_method"),
            col("parsed_properties.type").alias("prop_type"),
            col("parsed_properties.search_type").alias("prop_search_type"),
            col("parsed_properties.search_keyword").alias("prop_search_keyword"),
            col("parsed_properties.selected_filters").alias("prop_selected_filters"),
            col("parsed_properties.result_count").alias("prop_result_count"),
            col("parsed_properties.list_type").alias("prop_list_type"),
            col("parsed_properties.displayed_recipe_ids").alias("prop_displayed_recipe_ids"),
            col("parsed_properties.recipe_id").cast(LongType()).alias("prop_recipe_id"),
            col("parsed_properties.rank").alias("prop_rank"),
            col("parsed_properties.action").alias("prop_action"),
            col("parsed_properties.comment_length").alias("prop_comment_length"),
            col("parsed_properties.category").alias("prop_category"),
            col("parsed_properties.ingredient_count").alias("prop_ingredient_count"),
            col("parsed_properties.ad_id").alias("prop_ad_id"),
            col("parsed_properties.ad_type").alias("prop_ad_type"),
            col("parsed_properties.position").alias("prop_position"),
            col("parsed_properties.target_url").alias("prop_target_url")
        )
        
        # --- 4.5. 데이터 품질 관리 (향상된 버전) ---
        print("🔍 데이터 품질 관리 중...")
        df_silver_clean = df_silver_flat \
            .filter(col("event_id").isNotNull()) \
            .filter(col("event_timestamp").isNotNull()) \
            .dropDuplicates(["event_id"])
        
        final_count = df_silver_clean.count()
        print(f"✅ 컬럼 평탄화 및 데이터 품질 관리 완료. 최종 행 수: {final_count:,}")
        
        # Silver 샘플 데이터 확인
        print("\n📊 Silver Layer 샘플 데이터 (상위 3행):")
        df_silver_clean.show(3, truncate=True)
        
        # 이벤트별 분포 확인
        print("\n📊 이벤트별 분포:")
        df_silver_clean.groupBy('event_name').count().orderBy('count', ascending=False).show(10)

        # --- 4.6. Silver Iceberg 테이블 생성 (파티션 적용) ---
        print("\n💾 Silver Iceberg 테이블로 저장 중...")
        
        # 성능 최적화: 파티션 수 조정
        df_silver_optimized = df_silver_clean.coalesce(4)
        
        # Iceberg 테이블 생성 (파티션 적용)
        try:
            # 기존 테이블이 있으면 덮어쓰기, 없으면 새로 생성
            spark.sql("DROP TABLE IF EXISTS silver_db.cleaned_events")
            df_silver_optimized.writeTo("silver_db.cleaned_events") \
                .partitionedBy("year", "month", "day") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.target-file-size-bytes", "134217728") \
                .create()
        except Exception as e:
            print(f"⚠️ Silver 테이블 생성 중 오류: {e}")
            # Fallback: 기존 테이블이 있다면 append
            try:
                df_silver_optimized.writeTo("silver_db.cleaned_events").append()
                print("✅ 기존 Silver 테이블에 데이터 추가 완료")
            except Exception as e2:
                print(f"❌ Silver 테이블 처리 실패: {e2}")
                return
        
        print("✅ Silver Iceberg 테이블 'silver_db.cleaned_events' 생성 완료")

        # -----------------------------------------------------------------------------
        # 5. 📊 테이블 메타데이터 및 통계 정보
        # -----------------------------------------------------------------------------
        print("\n📊 테이블 메타데이터 정보:")
        
        # 테이블 정보 조회
        print("\n🗃️ Bronze 테이블 정보:")
        spark.sql("DESCRIBE TABLE EXTENDED bronze_db.raw_events").show(truncate=False)
        
        print("\n🗃️ Silver 테이블 정보:")
        spark.sql("DESCRIBE TABLE EXTENDED silver_db.cleaned_events").show(truncate=False)
        
        # 파티션 정보 확인
        print("\n📁 Silver 테이블 파티션 정보:")
        spark.sql("SHOW PARTITIONS silver_db.cleaned_events").show()

        # -----------------------------------------------------------------------------
        # 6. 🧪 Iceberg 고급 기능 테스트
        # -----------------------------------------------------------------------------
        print("\n🧪 Iceberg 고급 기능 테스트...")
        
        # 스냅샷 이력 조회
        print("\n📸 Bronze 테이블 스냅샷 이력:")
        spark.sql("SELECT * FROM bronze_db.raw_events.snapshots").show(truncate=False)
        
        print("\n📸 Silver 테이블 스냅샷 이력:")
        spark.sql("SELECT * FROM silver_db.cleaned_events.snapshots").show(truncate=False)
        
        # 테이블 속성 확인
        print("\n⚙️ Silver 테이블 속성:")
        spark.sql("SHOW TBLPROPERTIES silver_db.cleaned_events").show(truncate=False)

        # -----------------------------------------------------------------------------
        # 7. 검증 및 요약
        # -----------------------------------------------------------------------------
        print("\n📈 Iceberg ETL 파이프라인 완료 요약:")
        print(f"🥉 Bronze 행 수: {bronze_count:,}")
        print(f"🥈 Silver 행 수: {final_count:,}")
        print(f"📊 생성된 데이터베이스: bronze_db, silver_db, gold_db")
        print(f"🗃️ Bronze 테이블: bronze_db.raw_events (Iceberg)")
        print(f"🗃️ Silver 테이블: silver_db.cleaned_events (Iceberg, 파티션: year/month/day)")
        print(f"🏗️ 메타스토어: Hive Metastore (thrift://metastore:9083)")
        print(f"💾 데이터 저장소: S3 (s3a://reciping-user-event-logs/warehouse)")

        # -----------------------------------------------------------------------------
        # 8. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("✅ SparkSession이 종료되었습니다.")

    except Exception as e:
        print(f"❌ 전체 프로세스 실패: {e}")
        import traceback
        traceback.print_exc()
        
        # 스파크 세션이 있다면 종료
        try:
            spark.stop()
        except:
            pass

# 이 스크립트가 직접 실행될 때만 main() 함수를 호출합니다.
if __name__ == "__main__":
    main()
