#!/usr/bin/env python3
"""
🧊 Iceberg + Hive Metastore 기반 Bronze to Silver ETL Pipeline (Stateless & Idempotent)
=======================================================================================
Bronze Iceberg 테이블에서 신규 데이터를 읽어 Silver Iceberg 테이블로 변환/정제합니다.
Airflow로부터 data_interval_start를 받아 처리할 파티션을 동적으로 선택합니다.
"""
import logging
import argparse
from datetime import datetime
import pytz
from dateutil.parser import isoparse
from typing import Optional 

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    year, month, dayofmonth, hour, date_format, expr, to_date, coalesce, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType, DateType, LongType, ArrayType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BronzeToSilverETL:
    """Bronze Iceberg Table에서 데이터를 읽어 Silver Table로 변환하는 ETL 파이프라인"""

    def __init__(self, test_mode: bool = True):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083" # 자신의 Hive Metastore URI로 변경

        if test_mode:
            print("=== 테스트 모드로 실행 ===")
            self.database_name = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.table_suffix = "_test"
        else:
            print("=== 운영 모드로 실행 ===")
            self.database_name = "recipe_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.table_suffix = ""

        self.bronze_table_name = f"{self.catalog_name}.{self.database_name}.bronze_events_iceberg{self.table_suffix}"
        self.silver_table_name = f"{self.catalog_name}.{self.database_name}.user_events_silver{self.table_suffix}"

        print(f"데이터베이스: {self.database_name}")
        print(f"입력(Bronze) 테이블: {self.bronze_table_name}")
        print(f"출력(Silver) 테이블: {self.silver_table_name}")

    def create_spark_session(self):
        """SparkSession 생성"""
        print("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("BronzeToSilverETL") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        print("SparkSession 생성 완료")

    def create_silver_table_if_not_exists(self):
        """Silver Iceberg 테이블 생성 (기존 코드의 스키마 활용)"""
        print(f"Silver Iceberg 테이블 생성 확인: {self.silver_table_name}")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.silver_table_name} (
            event_id STRING, event_name STRING, user_id STRING, anonymous_id STRING, session_id STRING,
            kst_timestamp TIMESTAMP, utc_timestamp TIMESTAMP, date DATE,
            year INT, month INT, day INT, hour INT, day_of_week STRING,
            page_name STRING, page_url STRING, user_segment STRING, cooking_style STRING, ab_test_group STRING,
            prop_recipe_id BIGINT, prop_list_type STRING, prop_action STRING,
            prop_search_keyword STRING, prop_result_count INT,
            processed_at TIMESTAMP, data_source STRING, pipeline_version STRING
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        """
        self.spark.sql(create_table_sql)
        print("Silver Iceberg 테이블 준비 완료")

    def read_new_data_from_bronze(self, execution_ts: str = None) -> Optional[DataFrame]:
        """수정된 버전: execution_ts가 없으면 Bronze 테이블 전체 읽기"""
        if execution_ts is None:
            print("Bronze 테이블에서 전체 데이터 읽기 (벌크 모드)")
            bronze_df = self.spark.read.table(self.bronze_table_name)
        else:
            print(f"Bronze 테이블에서 특정 파티션 읽기: {execution_ts}")
            # 기존 로직
            kst_tz = pytz.timezone('Asia/Seoul')
            try:
                dt_obj = datetime.strptime(execution_ts, '%Y-%m-%d %H:%M')
                kst_dt = kst_tz.localize(dt_obj)
            except ValueError:
                utc_dt = isoparse(execution_ts)
                kst_dt = utc_dt.astimezone(kst_tz)
            
            target_date = kst_dt.strftime('%Y-%m-%d')
            bronze_df = self.spark.read.table(self.bronze_table_name).where(f"ingestion_date = '{target_date}'")
        
        if bronze_df.rdd.isEmpty():
            print("처리할 데이터가 없습니다.")
            return None
            
        count = bronze_df.count()
        print(f"총 {count:,} 건의 데이터를 읽었습니다.")
        return bronze_df

    def transform_bronze_to_silver(self, bronze_df: DataFrame) -> DataFrame:
        """Bronze 데이터를 Silver 스키마에 맞게 변환합니다."""
        print("Bronze to Silver 데이터 변환 시작...")
        
        # 1. 파싱에 필요한 스키마 정의 (기존 코드 재사용)
        json_event_schema = StructType([
            StructField("anonymous_id", StringType(), True), StructField("context", StringType(), True),
            StructField("date", StringType(), True), StructField("event_id", StringType(), True),
            StructField("event_name", StringType(), True), StructField("event_properties", StringType(), True),
            StructField("session_id", StringType(), True), StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True)
        ])
        context_schema = StructType([
            StructField("page", StructType([
                StructField("name", StringType(), True), StructField("url", StringType(), True),
                StructField("path", StringType(), True)
            ]), True),
            StructField("user_segment", StringType(), True), StructField("activity_level", StringType(), True),
            StructField("cooking_style", StringType(), True),
            StructField("ab_test", StructType([
                StructField("scenario", StringType(), True), StructField("group", StringType(), True),
                StructField("start_date", StringType(), True), StructField("end_date", StringType(), True)
            ]), True)
        ])
        event_properties_schema = StructType([
            StructField("page_name", StringType(), True), StructField("referrer", StringType(), True),
            StructField("recipe_id", StringType(), True), StructField("list_type", StringType(), True),
            StructField("action", StringType(), True), StructField("search_keyword", StringType(), True),
            StructField("result_count", IntegerType(), True)
        ])

        # 2. raw_event_string 컬럼을 JSON으로 파싱
        parsed_df = bronze_df.withColumn("event_data", from_json(col("raw_event_string"), json_event_schema))

        # 3. 파싱된 데이터를 기반으로 변환 수행
        df_transformed = parsed_df \
            .withColumn("parsed_context", from_json(col("event_data.context"), context_schema)) \
            .withColumn("parsed_properties", from_json(col("event_data.event_properties"), event_properties_schema)) \
            .withColumn("raw_timestamp_str", col("event_data.timestamp")) \
            .withColumn("kst_timestamp", 
                # ISO 8601 형식의 timestamp를 올바르게 파싱
                to_timestamp(col("raw_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
            .withColumn("utc_timestamp", 
                # KST에서 UTC로 변환 (9시간 빼기)
                expr("kst_timestamp - INTERVAL 9 HOURS")) \
            .withColumn("date", 
                # KST 기준으로 날짜 추출 (이게 핵심!)
                to_date(col("kst_timestamp"))) \
            .withColumn("year", year(col("kst_timestamp"))) \
            .withColumn("month", month(col("kst_timestamp"))) \
            .withColumn("day", dayofmonth(col("kst_timestamp"))) \
            .withColumn("hour", hour(col("kst_timestamp"))) \
            .withColumn("day_of_week", date_format(col("kst_timestamp"), "E"))

        # 최종 컬럼 선택
        df_final = df_transformed.select(
            col("event_data.event_id").alias("event_id"),
            col("event_data.event_name").alias("event_name"),
            col("event_data.user_id").alias("user_id"),
            col("event_data.anonymous_id").alias("anonymous_id"),
            col("event_data.session_id").alias("session_id"),
            "kst_timestamp", "utc_timestamp", "date",
            "year", "month", "day", "hour", "day_of_week",
            col("parsed_context.page.name").alias("page_name"),
            col("parsed_context.page.url").alias("page_url"),
            col("parsed_context.user_segment").alias("user_segment"),
            col("parsed_context.cooking_style").alias("cooking_style"),
            col("parsed_context.ab_test.group").alias("ab_test_group"),
            col("parsed_properties.recipe_id").cast(LongType()).alias("prop_recipe_id"),
            col("parsed_properties.list_type").alias("prop_list_type"),
            col("parsed_properties.action").alias("prop_action"),
            col("parsed_properties.search_keyword").alias("prop_search_keyword"),
            col("parsed_properties.result_count").alias("prop_result_count"),
            col("source_file").alias("data_source")
        ) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("pipeline_version", lit("bulk_corrected_v1.0")) \
        .dropDuplicates(["event_id"])

        print(f"데이터 변환 완료. 필터링 후 레코드 수: {df_final.count():,}")
        return df_final
    
#     def run_pipeline(self, execution_ts: Optional[str] = None, target_date: Optional[str] = None):
#         """인자에 따라 증분 또는 벌크 모드로 Bronze to Silver ETL을 실행합니다."""
#         try:
#             print("Bronze to Silver ETL 파이프라인 시작")
            
#             self.create_spark_session()
#             self.create_silver_table_if_not_exists()

#             # === 핵심 수정: 벌크 모드 처리 추가 ===
#             if target_date:
#                 # 증분 모드: 특정 날짜만 처리
#                 print(f"증분 모드로 실행 (대상 날짜: {target_date})")
#                 bronze_df = self.spark.read.table(self.bronze_table_name).where(f"ingestion_date = '{target_date}'")
                
#             elif execution_ts:
#                 # 증분 모드: Airflow에서 호출
#                 print(f"증분 모드로 실행 (입력 시간: {execution_ts})")
#                 kst_tz = pytz.timezone('Asia/Seoul')
#                 try:
#                     dt_obj = datetime.strptime(execution_ts, '%Y-%m-%d %H:%M')
#                     kst_dt = kst_tz.localize(dt_obj)
#                 except ValueError:
#                     utc_dt = isoparse(execution_ts)
#                     kst_dt = utc_dt.astimezone(kst_tz)
#                 target_date_str = kst_dt.strftime('%Y-%m-%d')
#                 bronze_df = self.spark.read.table(self.bronze_table_name).where(f"ingestion_date = '{target_date_str}'")
                
#             else:
#                 # 벌크 모드: 전체 Bronze 데이터 처리
#                 print("벌크 모드로 실행 (Bronze 테이블 전체 처리)")
#                 print("주의: ingestion_date와 관계없이 모든 데이터를 실제 이벤트 날짜별로 재파티셔닝합니다.")
#                 bronze_df = self.spark.read.table(self.bronze_table_name)
            
#             if bronze_df.rdd.isEmpty():
#                 print("처리할 데이터가 없습니다. 파이프라인을 종료합니다.")
#                 return
            
#             count = bronze_df.count()
#             print(f"총 {count:,} 건의 데이터를 처리합니다.")
            
#             # 데이터 변환 및 저장 (실제 이벤트 날짜별로 파티셔닝됨)
#             silver_data = self.transform_bronze_to_silver(bronze_df)
#             silver_data.writeTo(self.silver_table_name).append()
            
#             print("ETL 파이프라인 성공적으로 완료")
            
#         except Exception as e:
#             logger.error("파이프라인 실패", exc_info=True)
#             raise
#         finally:
#             if self.spark:
#                 self.spark.stop()

# def main():
#     parser = argparse.ArgumentParser(description="Unified Bronze to Silver ETL Job")
#     # 두 인자 모두 받되, 필수는 아니도록 설정
#     parser.add_argument("--execution-ts", required=False, help="For incremental runs")
#     parser.add_argument("--target-date", required=False, help="For bulk runs (YYYY-MM-DD)")
#     parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True)
#     args = parser.parse_args()

#     pipeline = BronzeToSilverETL(test_mode=args.test_mode)
#     pipeline.run_pipeline(execution_ts=args.execution_ts, target_date=args.target_date)


    def run_pipeline(self, data_interval_start: Optional[str] = None, data_interval_end: Optional[str] = None):
        """
        [수정됨] data_interval_start를 기반으로 Bronze 테이블의 특정 파티션을 읽어 처리합니다.
        """
        try:
            self.create_spark_session()
            self.create_silver_table_if_not_exists()

            if not data_interval_start:
                raise ValueError("증분 처리를 위해 --data-interval-start 인자가 반드시 필요합니다.")

            # === 증분 처리 모드: Airflow가 전달한 시간 구간을 명확히 사용 ===
            print(f"증분 처리 모드로 실행: {data_interval_start} ~ {data_interval_end}")
            
            # 1. data_interval_start(UTC)를 KST 기준으로 변환하여 'YYYY-MM-DD' 날짜 획득
            start_time_utc = isoparse(data_interval_start)
            kst_tz = pytz.timezone('Asia/Seoul')
            start_time_kst = start_time_utc.astimezone(kst_tz)
            target_date_str = start_time_kst.strftime('%Y-%m-%d')
            
            print(f"Bronze 테이블의 처리 대상 파티션 날짜: {target_date_str}")
            
            # 2. Bronze 테이블에서 해당 날짜 파티션만 정확히 읽어오기
            source_bronze_df = self.spark.read.table(self.bronze_table_name).where(
                f"ingestion_date = '{target_date_str}'"
            )
            
            if source_bronze_df.rdd.isEmpty():
                print("처리할 Bronze 데이터가 없습니다. 작업을 종료합니다.")
                return

            # --- 공통 실행 로직 ---
            print(f"총 {source_bronze_df.count():,} 건의 Bronze 데이터를 처리합니다.")
            silver_data = self.transform_bronze_to_silver(source_bronze_df)
            silver_data.createOrReplaceTempView("silver_updates")
            
            # MERGE INTO는 멱등성을 보장하는 좋은 방법입니다.
            merge_sql = f"""
            MERGE INTO {self.silver_table_name} t
            USING silver_updates s
            ON t.event_id = s.event_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            print("Silver Iceberg 테이블에 MERGE 실행...")
            self.spark.sql(merge_sql)
            print("Bronze to Silver ETL 파이프라인 성공적으로 완료")
                
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Stateless Bronze to Silver ETL Job")
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True)
    args = parser.parse_args()

    pipeline = BronzeToSilverETL(test_mode=args.test_mode)
    pipeline.run_pipeline(
        data_interval_start=args.data_interval_start, 
        data_interval_end=args.data_interval_end
    )

if __name__ == "__main__":
    main()