# main_bronze_to_silver.py (KST 파티셔닝 및 코드 최적화 최종본)
# 사용자 요청사항 반영:
# 1. event_name 컬럼명 유지 (event_type 리네임 안함)
# 2. archive/old_versions/bronze_to_silver.py 변환 로직 완전 적용
# 3. Asia/Seoul 타임존 설정으로 한국 시간 유지

# 필요한 라이브러리와 스파크의 내장 함수들을 가져옵니다.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, date_format, format_string
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType, TimestampType

def main():
    """
    Bronze Layer의 원본 이벤트 로그(JSONL)를 읽어 정제하고 구조화한 뒤,
    분석에 최적화된 Silver Layer(Parquet)로 변환하여 저장하는 PySpark ETL 작업.
    
    사용자 요청사항:
    1. event_name 컬럼명 그대로 유지
    2. archive/old_versions/bronze_to_silver.py의 변환 로직 완전 적용
    3. 한국 시간(KST) 타임존 유지
    """
    # -----------------------------------------------------------------------------
    # 1. 스파크 세션 생성 (ETL 작업의 시작점)
    # -----------------------------------------------------------------------------
    # SparkSession은 스파크의 모든 기능을 사용하기 위한 진입점입니다.
    spark = SparkSession.builder \
        .appName("BronzeToSilver-UserEvents-ETL-KST-Final") \
        .config("spark.sql.session.timeZone", "Asia/Seoul") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
        .getOrCreate()

    # 실행 로그를 WARN 레벨로 설정하여 정보성 로그는 줄이고 경고 및 에러만 표시합니다.
    spark.sparkContext.setLogLevel("WARN")
    print("✅ SparkSession이 성공적으로 생성되었습니다.")
    print(f"⏰ 타임존 설정: {spark.conf.get('spark.sql.session.timeZone')}")

    # -----------------------------------------------------------------------------
    # 2. 경로 변수 정의
    # -----------------------------------------------------------------------------
    # 원본 데이터가 있는 S3 경로를 지정합니다.
    bronze_s3_path = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
    # 결과물을 저장할 S3 경로를 지정합니다.
    silver_s3_path = "s3a://reciping-user-event-logs/silver/user-behavior-events/"

    print(f"Bronze 데이터 소스: {bronze_s3_path}")
    print(f"Silver 데이터 목적지: {silver_s3_path}")

    # -----------------------------------------------------------------------------
    # 3. 데이터 추출 (Extract)
    # -----------------------------------------------------------------------------
    # Bronze 경로의 모든 JSONL 파일을 읽어 스파크 데이터프레임으로 만듭니다.
    df_bronze = spark.read.json(bronze_s3_path)
    print("✅ Bronze Layer로부터 데이터 읽기 완료.")
    
    row_count = df_bronze.count()
    print(f"📊 원본 데이터 행 수: {row_count:,}")
    
    # -----------------------------------------------------------------------------
    # 4. 데이터 변환 (Transform)
    # -----------------------------------------------------------------------------

    # --- 4.1. 중첩된 JSON 문자열을 파싱하기 위한 스키마를 명시적으로 정의합니다. ---
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
        StructField("page_name", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("path", StringType(), True),
        StructField("method", StringType(), True),
        StructField("type", StringType(), True),
        StructField("search_type", StringType(), True),
        StructField("search_keyword", StringType(), True),
        StructField("selected_filters", ArrayType(StringType()), True),
        StructField("result_count", IntegerType(), True),
        StructField("list_type", StringType(), True),
        StructField("displayed_recipe_ids", ArrayType(StringType()), True),
        StructField("recipe_id", StringType(), True),
        StructField("rank", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("comment_length", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("ingredient_count", IntegerType(), True),
        StructField("ad_id", StringType(), True),
        StructField("ad_type", StringType(), True),
        StructField("position", StringType(), True),
        StructField("target_url", StringType(), True)
    ])

    # --- 4.2. JSON 파싱 및 타임스탬프 자료형 변환 ---
    df_transformed = df_bronze \
        .withColumn("parsed_context", from_json(col("context"), context_schema)) \
        .withColumn("parsed_properties", from_json(col("event_properties"), event_properties_schema)) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .withColumn("date", col("date").cast(DateType())) \
        .drop("context", "event_properties")

    print("✅ JSON 파싱 및 타임스탬프 변환 완료.")

    # --- 4.3. Silver Layer 저장을 위한 파티션 컬럼 생성 (KST 기준) ---
    # 세션 시간대가 "Asia/Seoul"이므로, year(), month() 등 함수가 KST 기준으로 값을 추출합니다.
    df_with_partitions = df_transformed \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_of_week", date_format(col("timestamp"), "E")) # 'Mon', 'Tue' 등 요일 추출

    print("✅ KST 기준 파티션 컬럼(year, month, day, hour) 생성 완료.")

    # --- 4.4. 최종 컬럼 선택 및 정리 (평탄화) ---
    df_silver = df_with_partitions.select(
        # 기본 이벤트 정보 (event_name 컬럼명 유지)
        "event_id", 
        "event_name", 
        "user_id", 
        "anonymous_id", 
        "session_id", 
        
        # 시간 관련 컬럼
        col("timestamp").alias("utc_timestamp"), 
        "date",
        
        # KST 기준 파생 컬럼들
        "year", 
        "month", 
        "day", 
        "hour",
        "day_of_week",
        
        # Context에서 파생된 컬럼들
        col("parsed_context.page.name").alias("page_name"),
        col("parsed_context.page.url").alias("page_url"),
        col("parsed_context.page.path").alias("page_path"),
        col("parsed_context.user_segment").alias("user_segment"),
        col("parsed_context.activity_level").alias("activity_level"),
        col("parsed_context.cooking_style").alias("cooking_style"),
        col("parsed_context.ab_test.group").alias("ab_test_group"),
        col("parsed_context.ab_test.scenario").alias("ab_test_scenario"),
        
        # Event Properties에서 파생된 컬럼들
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
    
    # --- 4.5. 데이터 품질 관리 ---
    df_silver = df_silver.filter(col("event_id").isNotNull()).dropDuplicates(["event_id"])
    print("✅ 컬럼 평탄화 및 데이터 품질 관리 완료.")

    clean_count = df_silver.count()
    print(f"📊 정제된 데이터 행 수: {clean_count:,}")

    # --- 4.6. 타임존 확인을 위한 샘플 데이터 출력 ---
    print("\n📅 타임존 확인을 위한 샘플 데이터:")
    df_silver.select("utc_timestamp", "year", "month", "day", "hour").show(5)

    # -----------------------------------------------------------------------------
    # 5. 데이터 적재 (Load) - Silver Layer에 최종 결과를 저장
    # -----------------------------------------------------------------------------
    print("Silver Layer로 데이터 저장을 시작합니다...")
    # KST 기준으로 생성한 year, month, day, hour 컬럼을 사용해 디렉토리 구조를 만듭니다.
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "hour") \
        .parquet(silver_s3_path)

    print(f"✅✅✅ 데이터가 성공적으로 {silver_s3_path} 에 저장되었습니다! ✅✅✅")

    # --- 파티션 정보 확인 ---
    print("\n📅 파티션별 데이터 분포:")
    df_silver.groupBy("year", "month", "day").count().orderBy("year", "month", "day").show()

    # -----------------------------------------------------------------------------
    # 6. 스파크 세션 종료
    # -----------------------------------------------------------------------------
    spark.stop()
    print("✅ SparkSession이 종료되었습니다.")


# 이 스크립트가 직접 실행될 때만 main() 함수를 호출합니다.
if __name__ == "__main__":
    main()
