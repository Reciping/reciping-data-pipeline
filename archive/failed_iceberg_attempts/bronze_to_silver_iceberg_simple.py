# bronze_to_silver_iceberg_simple.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, date_format, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType, TimestampType

def main():
    """
    Iceberg 고급 기능을 활용하여 Bronze, Silver 레이어를 구축하는
    데이터 레이크하우스 ETL 파이프라인 (Hive Metastore 없이 Hadoop Catalog 사용).
    """
    try:
        # 시스템 환경 변수를 먼저 설정 (성공한 설정 적용)
        import os
        import subprocess
        
        # 성공한 환경 변수 설정 (JVM 최적화)
        os.environ['HADOOP_USER_NAME'] = 'root'
        os.environ['USER'] = 'root'
        os.environ['HOME'] = '/tmp'
        os.environ['JAVA_OPTS'] = '-Duser.name=root -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:MaxGCPauseMillis=200'
        # Ivy 설정
        os.environ['IVY_HOME'] = '/tmp/.ivy2'
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.jars.ivy=/tmp/.ivy2 pyspark-shell'
        
        # AWS 자격 증명 확인
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not aws_access_key or not aws_secret_key:
            print("⚠️ AWS 자격 증명이 설정되지 않았습니다. Docker Compose 환경 변수를 확인하세요.")
            print(f"AWS_ACCESS_KEY_ID: {'✅ 설정됨' if aws_access_key else '❌ 없음'}")
            print(f"AWS_SECRET_ACCESS_KEY: {'✅ 설정됨' if aws_secret_key else '❌ 없음'}")
        else:
            print("✅ AWS 자격 증명이 정상적으로 설정되었습니다.")
        
        # 필요한 디렉토리 생성
        os.makedirs('/tmp/.ivy2', exist_ok=True)

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (Iceberg + Hadoop Catalog)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession with Iceberg (Hadoop Catalog) 생성을 시도합니다...")
        
        spark = SparkSession.builder \
            .appName("Bronze_to_Silver_Iceberg_Simple_Pipeline") \
            .master("local[2]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/warehouse/iceberg") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
            .config("spark.hadoop.fs.s3a.region", "ap-northeast-2") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.block.size", "134217728") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession with Iceberg (Hadoop Catalog)가 성공적으로 생성되었습니다!")

        # -----------------------------------------------------------------------------
        # 2. 🏗️ 네임스페이스 생성 및 관리
        # -----------------------------------------------------------------------------
        print("\n🏗️ Iceberg 네임스페이스 구성...")
        
        # Bronze, Silver, Gold 네임스페이스 생성
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.bronze")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.silver") 
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.gold")
            print("✅ Iceberg 네임스페이스 구성 완료")
        except Exception as e:
            print(f"⚠️ 네임스페이스 생성 중 오류 (이미 존재할 수 있음): {e}")
        
        # 현재 네임스페이스 목록 확인
        print("\n📋 현재 Iceberg 네임스페이스 목록:")
        spark.sql("SHOW NAMESPACES IN iceberg_catalog").show()

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
            
            # Bronze Iceberg 테이블 생성 (안전한 방식)
            try:
                # 기존 테이블 삭제 후 새로 생성
                spark.sql("DROP TABLE IF EXISTS iceberg_catalog.bronze.raw_events")
                
                # 작은 샘플부터 테스트 (메모리 부담 감소)
                sample_size = min(50000, row_count)  # 최대 5만 개 레코드로 시작
                df_sample = df_raw_with_metadata.limit(sample_size)
                
                print(f"📊 테스트용 샘플 데이터 생성: {sample_size:,}행")
                
                df_sample.writeTo("iceberg_catalog.bronze.raw_events") \
                    .tableProperty("format-version", "2") \
                    .tableProperty("write.target-file-size-bytes", "67108864") \
                    .tableProperty("write.parquet.compression-codec", "snappy") \
                    .create()
                
                print("✅ Bronze Iceberg 테이블 'iceberg_catalog.bronze.raw_events' 생성 완료")
                
            except Exception as e:
                print(f"❌ Bronze 테이블 생성 실패: {e}")
                return
            
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
        df_bronze = spark.table("iceberg_catalog.bronze.raw_events")
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
            # 기존 테이블 삭제 후 새로 생성
            spark.sql("DROP TABLE IF EXISTS iceberg_catalog.silver.cleaned_events")
            
            df_silver_optimized.writeTo("iceberg_catalog.silver.cleaned_events") \
                .partitionedBy("year", "month", "day") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.target-file-size-bytes", "134217728") \
                .create()
            
            print("✅ Silver Iceberg 테이블 'iceberg_catalog.silver.cleaned_events' 생성 완료")
            
        except Exception as e:
            print(f"❌ Silver 테이블 생성 실패: {e}")
            return

        # -----------------------------------------------------------------------------
        # 5. 📊 Iceberg 테이블 메타데이터 및 고급 기능 시연
        # -----------------------------------------------------------------------------
        print("\n📊 Iceberg 테이블 메타데이터 정보:")
        
        # 테이블 정보 조회 (안전한 방식)
        print("\n🗃️ Bronze 테이블 정보:")
        try:
            bronze_info = spark.sql("DESCRIBE TABLE iceberg_catalog.bronze.raw_events")
            bronze_info.show(10, truncate=False)
        except Exception as e:
            print(f"⚠️ Bronze 테이블 정보 조회 실패: {e}")
        
        print("\n🗃️ Silver 테이블 정보:")
        try:
            silver_info = spark.sql("DESCRIBE TABLE iceberg_catalog.silver.cleaned_events")
            silver_info.show(10, truncate=False)
        except Exception as e:
            print(f"⚠️ Silver 테이블 정보 조회 실패: {e}")

        # 파티션 정보 확인
        print("\n📁 Silver 테이블 파티션 정보:")
        try:
            spark.sql("SHOW PARTITIONS iceberg_catalog.silver.cleaned_events").show()
        except Exception as e:
            print(f"⚠️ 파티션 정보 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 6. 🧪 Iceberg 고급 기능 테스트
        # -----------------------------------------------------------------------------
        print("\n🧪 Iceberg 고급 기능 테스트...")
        
        # 스냅샷 이력 조회 (안전한 방식)
        print("\n📸 Bronze 테이블 스냅샷 이력:")
        try:
            bronze_snapshots = spark.sql("SELECT snapshot_id, committed_at, summary FROM iceberg_catalog.bronze.raw_events.snapshots ORDER BY committed_at DESC LIMIT 3")
            bronze_snapshots.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Bronze 스냅샷 조회 실패: {e}")
        
        print("\n📸 Silver 테이블 스냅샷 이력:")
        try:
            silver_snapshots = spark.sql("SELECT snapshot_id, committed_at, summary FROM iceberg_catalog.silver.cleaned_events.snapshots ORDER BY committed_at DESC LIMIT 3")
            silver_snapshots.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Silver 스냅샷 조회 실패: {e}")
        
        # 파일 정보 확인
        print("\n📋 Silver 테이블 파일 정보:")
        try:
            spark.sql("SELECT * FROM iceberg_catalog.silver.cleaned_events.files LIMIT 5").show(truncate=False)
        except Exception as e:
            print(f"⚠️ 파일 정보 조회 실패: {e}")

        # 테이블 속성 확인
        print("\n⚙️ Silver 테이블 속성:")
        try:
            spark.sql("SHOW TBLPROPERTIES iceberg_catalog.silver.cleaned_events").show(truncate=False)
        except Exception as e:
            print(f"⚠️ 테이블 속성 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 7. 🔄 Iceberg Time Travel 기능 데모
        # -----------------------------------------------------------------------------
        print("\n🔄 Iceberg Time Travel 기능 데모...")
        
        try:
            # 최신 스냅샷 ID 조회
            snapshots = spark.sql("SELECT snapshot_id, committed_at FROM iceberg_catalog.silver.cleaned_events.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
            if snapshots:
                snapshot_id = snapshots[0]['snapshot_id']
                committed_at = snapshots[0]['committed_at']
                print(f"📸 최신 스냅샷: {snapshot_id} (생성시간: {committed_at})")
                
                # Time Travel 쿼리 (특정 스냅샷으로)
                time_travel_count = spark.sql(f"""
                    SELECT COUNT(*) as record_count 
                    FROM iceberg_catalog.silver.cleaned_events 
                    VERSION AS OF {snapshot_id}
                """).collect()[0]['record_count']
                
                print(f"⏰ Time Travel 쿼리 결과: {time_travel_count:,}행")
                
        except Exception as e:
            print(f"⚠️ Time Travel 기능 테스트 실패: {e}")

        # -----------------------------------------------------------------------------
        # 8. 검증 및 요약
        # -----------------------------------------------------------------------------
        print("\n📈 Iceberg ETL 파이프라인 완료 요약:")
        print(f"🥉 Bronze 행 수: {bronze_count:,}")
        print(f"🥈 Silver 행 수: {final_count:,}")
        print(f"📊 생성된 네임스페이스: iceberg_catalog.bronze, iceberg_catalog.silver, iceberg_catalog.gold")
        print(f"🗃️ Bronze 테이블: iceberg_catalog.bronze.raw_events (Iceberg v2)")
        print(f"🗃️ Silver 테이블: iceberg_catalog.silver.cleaned_events (Iceberg v2, 파티션: year/month/day)")
        print(f"🏗️ 카탈로그: Hadoop Catalog")
        print(f"💾 데이터 저장소: S3 (s3a://reciping-user-event-logs/warehouse/iceberg)")
        
        # 추가 Iceberg 기능들
        print(f"\n💎 지원되는 Iceberg 고급 기능:")
        print(f"   ✅ Time Travel: 특정 스냅샷으로 되돌아가기")
        print(f"   ✅ Schema Evolution: 스키마 변경 지원")
        print(f"   ✅ Partition Evolution: 파티션 구조 변경")
        print(f"   ✅ ACID Transactions: 원자성, 일관성, 격리성, 지속성")
        print(f"   ✅ Metadata Tables: 스냅샷, 파일, 매니페스트 조회")

        # -----------------------------------------------------------------------------
        # 9. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("✅ Iceberg ETL 파이프라인이 완료되었습니다!")

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
