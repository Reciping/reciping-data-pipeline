#!/usr/bin/env python3
"""
🧊 Iceberg + Hive Metastore 기반 Bronze to Silver ETL Pipeline (Table-based)
============================================================================
Bronze Iceberg 테이블에서 신규 데이터를 읽어 Silver Iceberg 테이블로 변환/정제합니다.
Airflow 실행 시간에 따라 처리할 데이터 파티션을 동적으로 선택합니다.
"""
import logging
import argparse
from datetime import datetime
import pytz
from dateutil.parser import isoparse
from typing import Optional  # <--- [수정 1] Optional을 import 합니다.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    year, month, dayofmonth, hour, date_format, expr, to_date
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

    def read_new_data_from_bronze(self, execution_ts: str) -> Optional[DataFrame]:  # <--- [수정 2] "DataFrame | None"을 "Optional[DataFrame]"으로 변경
        """Bronze 테이블에서 특정 파티션의 신규 데이터를 읽어옵니다."""
        print(f"Bronze 테이블에서 신규 데이터 읽기 시작 (기준 시간: {execution_ts})")
        
        kst_tz = pytz.timezone('Asia/Seoul')
        try:
            dt_obj = datetime.strptime(execution_ts, '%Y-%m-%d %H:%M')
            kst_dt = kst_tz.localize(dt_obj)
        except ValueError:
            utc_dt = isoparse(execution_ts)
            kst_dt = utc_dt.astimezone(kst_tz)
        
        target_date = kst_dt.strftime('%Y-%m-%d')
        
        print(f"처리할 파티션 날짜: {target_date}")
        bronze_df = self.spark.read.table(self.bronze_table_name).where(f"ingestion_date = '{target_date}'")
        
        if bronze_df.rdd.isEmpty():
            print("처리할 신규 데이터가 없습니다. 파이프라인을 종료합니다.")
            return None
            
        count = bronze_df.count()
        print(f"총 {count:,} 건의 신규 데이터를 읽었습니다.")
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

        # 3. 파싱된 데이터를 기반으로 변환 수행 (기존 로직 활용)
        df_transformed = parsed_df \
            .withColumn("parsed_context", from_json(col("event_data.context"), context_schema)) \
            .withColumn("parsed_properties", from_json(col("event_data.event_properties"), event_properties_schema)) \
            .withColumn("kst_timestamp", col("event_data.timestamp").cast(TimestampType())) \
            .withColumn("utc_timestamp", expr("kst_timestamp - INTERVAL 9 HOURS")) \
            .withColumn("date", col("event_data.date").cast(DateType())) \
            .withColumn("year", year(col("kst_timestamp"))) \
            .withColumn("month", month(col("kst_timestamp"))) \
            .withColumn("day", dayofmonth(col("kst_timestamp"))) \
            .withColumn("hour", hour(col("kst_timestamp"))) \
            .withColumn("day_of_week", date_format(col("kst_timestamp"), "E"))
        
        # --- 이 부분이 핵심적인 수정입니다 ---
        # 4. 변환된 kst_timestamp를 기반으로 date 및 연/월/일/시 컬럼 생성
        df_with_date = df_transformed \
            .withColumn("utc_timestamp", expr("kst_timestamp - INTERVAL 9 HOURS")) \
            .withColumn("date", to_date(col("kst_timestamp"))) \
            .withColumn("year", year(col("kst_timestamp"))) \
            .withColumn("month", month(col("kst_timestamp"))) \
            .withColumn("day", dayofmonth(col("kst_timestamp"))) \
            .withColumn("hour", hour(col("kst_timestamp"))) \
            .withColumn("day_of_week", date_format(col("kst_timestamp"), "E"))

        # 5. 최종 컬럼 선택 및 정리
        df_final = df_with_date.select(
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
            col("source_file").alias("data_source") # Bronze의 source_file을 data_source로 활용
        ) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("pipeline_version", lit("table_based_v1.0")) \
        .dropDuplicates(["event_id"]) # 중복 이벤트 최종 제거

        print("데이터 변환 완료")
        return df_final
    
    def run_pipeline(self, execution_ts: str):
        """메인 파이프라인 실행"""
        try:
            print("Bronze to Silver ETL 파이프라인 시작")
            
            self.create_spark_session()
            self.create_silver_table_if_not_exists()

            # 1. Bronze 테이블에서 신규 데이터 읽기
            new_bronze_data = self.read_new_data_from_bronze(execution_ts)
            
            if new_bronze_data:
                # 2. 데이터 변환
                silver_data = self.transform_bronze_to_silver(new_bronze_data)
                
                # 3. Silver Iceberg 테이블에 저장
                print(f"Silver 테이블에 데이터 저장: {self.silver_table_name}")
                silver_data.writeTo(self.silver_table_name).append()
                print("Silver 테이블 저장 완료")

            print("ETL 파이프라인 성공적으로 완료")
            
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Bronze Iceberg to Silver Iceberg ETL Job")
    parser.add_argument("--execution-ts", required=True, help="Airflow execution timestamp (ISO or 'YYYY-MM-DD HH:MM')")
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True, help="Run in test mode (true/false)")
    args = parser.parse_args()

    pipeline = BronzeToSilverETL(test_mode=args.test_mode)
    pipeline.run_pipeline(execution_ts=args.execution_ts)

if __name__ == "__main__":
    main()