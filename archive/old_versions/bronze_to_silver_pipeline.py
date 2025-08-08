# bronze_to_silver_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, date_format, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType, TimestampType

def main():
    """
    S3 랜딩 존의 원본 파일을 읽어 Bronze, Silver 아이스버그 테이블을 구축하는
    전체 데이터 레이크하우스 ETL 파이프라인.
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
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.jars.ivy=/tmp/.ivy2 --conf spark.jars.packages= pyspark-shell'
        
        # 필요한 디렉토리 생성
        os.makedirs('/tmp/.ivy2', exist_ok=True)

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (성공한 설정 + S3 + Iceberg 설정)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession 생성을 시도합니다...")
        
        spark = SparkSession.builder \
            .appName("Bronze_to_Silver_Lakehouse_Pipeline") \
            .master("local[*]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.hive_catalog.type", "hive") \
            .config("spark.sql.catalog.hive_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.hive_catalog.warehouse", "s3a://reciping-user-event-logs/warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
            .config("spark.hadoop.fs.s3a.region", "ap-northeast-2") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession이 성공적으로 생성되었습니다!")

        # -----------------------------------------------------------------------------
        # 2. 🥉 Bronze Layer 구축 - 데이터베이스 및 테이블 생성
        # -----------------------------------------------------------------------------
        print("\n🥉 Bronze Layer 구축 시작...")
        
        # 데이터베이스 생성 (존재하지 않을 경우)
        spark.sql("CREATE DATABASE IF NOT EXISTS hive_catalog.bronze_db")
        print("✅ Bronze 데이터베이스 생성/확인 완료")
        
        # S3 랜딩 존에서 데이터 읽기
        landing_zone_path = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
        print(f"📂 랜딩 존에서 데이터 읽기: {landing_zone_path}")
        
        try:
            df_raw = spark.read.json(landing_zone_path)
            row_count = df_raw.count()
            print(f"✅ 랜딩 존 데이터 로드 성공! 행 수: {row_count:,}")
            
            # Bronze 테이블이 이미 존재하는지 확인
            try:
                existing_df = spark.table("hive_catalog.bronze_db.raw_events")
                existing_count = existing_df.count()
                print(f"📊 기존 Bronze 테이블 행 수: {existing_count:,}")
            except:
                print("📋 Bronze 테이블이 존재하지 않습니다. 새로 생성합니다.")
                existing_count = 0
            
            # Bronze 아이스버그 테이블로 저장 (새 데이터만 추가)
            df_raw.write.mode("append").saveAsTable("hive_catalog.bronze_db.raw_events")
            print("✅ Bronze 테이블 'bronze_db.raw_events'에 데이터 추가 완료")
            
        except Exception as e:
            print(f"❌ 랜딩 존에서 데이터를 읽을 수 없습니다: {e}")
            print("💡 upload_to_landing_zone.py를 먼저 실행하여 데이터를 업로드하세요.")
            spark.stop()
            return

        # -----------------------------------------------------------------------------
        # 3. 🥈 Silver Layer 구축 (기존 변환 로직 전체 반영)
        # -----------------------------------------------------------------------------
        print("\n🥈 Silver Layer 구축 시작...")
        
        # 데이터베이스 생성 (존재하지 않을 경우)
        spark.sql("CREATE DATABASE IF NOT EXISTS hive_catalog.silver_db")
        print("✅ Silver 데이터베이스 생성/확인 완료")
        
        # Bronze 테이블에서 데이터를 읽습니다.
        df_bronze_table = spark.table("hive_catalog.bronze_db.raw_events")
        bronze_count = df_bronze_table.count()
        print(f"📊 Bronze 테이블에서 {bronze_count:,}행의 데이터를 읽었습니다.")

        # --- 3.1. 기존과 동일한 스키마 정의 ---
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

        # --- 3.2. JSON 파싱 및 타임스탬프 변환 ---
        print("🔧 JSON 파싱 및 타임스탬프 변환 중...")
        df_transformed = df_bronze_table \
            .withColumn("parsed_context", from_json(col("context"), context_schema)) \
            .withColumn("parsed_properties", from_json(col("event_properties"), event_properties_schema)) \
            .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
            .withColumn("date_parsed", col("date").cast(DateType())) \
            .drop("context", "event_properties")

        # --- 3.3. 파티션 컬럼 생성 (KST 기준) ---
        print("📅 파티션 컬럼 생성 중...")
        df_with_partitions = df_transformed \
            .withColumn("year", year(col("timestamp_parsed"))) \
            .withColumn("month", month(col("timestamp_parsed"))) \
            .withColumn("day", dayofmonth(col("timestamp_parsed"))) \
            .withColumn("hour", hour(col("timestamp_parsed")))

        # --- 3.4. 컬럼 평탄화 (기존 로직 전체 복원) ---
        print("🗂️ 컬럼 평탄화 중...")
        df_silver_flat = df_with_partitions.select(
            "event_id", "event_name", "user_id", "anonymous_id", "session_id",
            col("timestamp_parsed").alias("event_timestamp"), col("date_parsed").alias("event_date"),
            "year", "month", "day", "hour",
            col("parsed_context.page.name").alias("page_name"),
            col("parsed_context.page.url").alias("page_url"),
            col("parsed_context.page.path").alias("page_path"),
            col("parsed_context.user_segment").alias("user_segment"),
            col("parsed_context.activity_level").alias("activity_level"),
            col("parsed_context.cooking_style").alias("cooking_style"),
            col("parsed_context.ab_test.group").alias("ab_test_group"),
            col("parsed_context.ab_test.scenario").alias("ab_test_scenario"),
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
        
        # --- 3.5. 데이터 품질 관리 ---
        print("🔍 데이터 품질 관리 중...")
        df_silver_final = df_silver_flat.filter(col("event_id").isNotNull()).dropDuplicates(["event_id"])
        final_count = df_silver_final.count()
        print(f"✅ 컬럼 평탄화 및 데이터 품질 관리 완료. 최종 행 수: {final_count:,}")
        
        # Silver 샘플 데이터 확인
        print("\n📊 Silver Layer 샘플 데이터 (상위 3행):")
        df_silver_final.show(3, truncate=True)
        
        # 이벤트별 분포 확인
        print("\n📊 이벤트별 분포:")
        df_silver_final.groupBy('event_name').count().orderBy('count', ascending=False).show(10)

        # --- 3.6. Silver 아이스버그 테이블로 저장 ---
        print("\n💾 Silver 아이스버그 테이블로 저장 중...")
        (df_silver_final.write
         .mode("overwrite")
         .partitionBy("year", "month", "day") # 성능 최적화를 위해 hour 파티션 제거
         .saveAsTable("hive_catalog.silver_db.cleaned_events"))
        print("✅ Silver 테이블 'silver_db.cleaned_events' 생성 완료")

        # -----------------------------------------------------------------------------
        # 4. 스파크 세션 종료
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
