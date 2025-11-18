#!/usr/bin/env python3
"""
replay_bronze_to_silver.py (8월 호환 버전)
==========================================
8월 bulk와 동일한 로직으로 Bronze → Silver 변환
"""
import logging
import argparse
from datetime import datetime
import pytz
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, year, month, dayofmonth, 
    hour, date_format, to_timestamp, to_date, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReplayBronzeToSilver:
    def __init__(self, test_mode: bool = False):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083"
        
        if test_mode:
            self.database_name = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.table_suffix = "_test"
        else:
            self.database_name = "recipe_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.table_suffix = ""
        
        # 8월과 동일한 테이블명
        self.bronze_table = f"bronze_events_iceberg{self.table_suffix}"
        self.silver_table = f"user_events_silver{self.table_suffix}"
    
    def create_spark_session(self):
        logger.info("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("ReplayBronzeToSilver") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.sql(f"USE {self.catalog_name}")
        self.spark.sql(f"USE {self.database_name}")
        logger.info("SparkSession 준비 완료")
    
    def create_silver_table_if_not_exists(self):
        """8월과 동일한 Silver 테이블 구조"""
        logger.info(f"Silver 테이블 확인: {self.silver_table}")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.silver_table} (
            event_id STRING, event_name STRING, user_id STRING, 
            anonymous_id STRING, session_id STRING,
            kst_timestamp TIMESTAMP, utc_timestamp TIMESTAMP, date DATE,
            year INT, month INT, day INT, hour INT, day_of_week STRING,
            page_name STRING, page_url STRING, 
            user_segment STRING, cooking_style STRING, ab_test_group STRING,
            prop_recipe_id BIGINT, prop_list_type STRING, prop_action STRING,
            prop_search_keyword STRING, prop_result_count INT,
            processed_at TIMESTAMP, data_source STRING, pipeline_version STRING
        ) USING ICEBERG 
        PARTITIONED BY (year, month, day)
        """
        self.spark.sql(create_sql)
        logger.info("Silver 테이블 준비 완료")
    
    def read_bronze_partition(self, data_interval_start: str):
        """
        ✅ 8월과 동일: ingestion_date 파티션으로 읽기
        """
        kst_tz = pytz.timezone('Asia/Seoul')
        start_dt = datetime.fromisoformat(data_interval_start.replace('Z', '+00:00')).astimezone(kst_tz)
        
        target_date = start_dt.strftime('%Y-%m-%d')
        
        logger.info(f"Bronze 파티션 읽기: ingestion_date = {target_date}")
        
        # ✅ 8월과 동일한 파티션 조건
        bronze_df = self.spark.read.table(self.bronze_table) \
            .where(f"ingestion_date = '{target_date}'")
        
        count = bronze_df.count()
        logger.info(f"읽은 레코드 수: {count:,}건")
        
        if count == 0:
            return None
        
        return bronze_df
    
    def transform_to_silver(self, bronze_df: DataFrame) -> DataFrame:
        """
        ✅ 8월 bulk와 완전히 동일한 변환 로직
        """
        logger.info("데이터 변환 시작...")
        
        # 8월 bulk의 스키마 정의 그대로 사용
        json_event_schema = StructType([
            StructField("anonymous_id", StringType(), True),
            StructField("context", StringType(), True),
            StructField("date", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_name", StringType(), True),
            StructField("event_properties", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True)
        ])
        
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
            StructField("recipe_id", StringType(), True),
            StructField("list_type", StringType(), True),
            StructField("action", StringType(), True),
            StructField("search_keyword", StringType(), True),
            StructField("result_count", IntegerType(), True)
        ])
        
        # ✅ 8월과 동일: raw_event_string 컬럼 사용
        parsed_df = bronze_df.withColumn("event_data", 
                                         from_json(col("raw_event_string"), json_event_schema))
        
        # ✅ 8월과 동일한 변환 로직
        df_transformed = parsed_df \
            .withColumn("parsed_context", from_json(col("event_data.context"), context_schema)) \
            .withColumn("parsed_properties", from_json(col("event_data.event_properties"), event_properties_schema)) \
            .withColumn("raw_timestamp_str", col("event_data.timestamp")) \
            .withColumn("kst_timestamp", 
                to_timestamp(col("raw_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
            .withColumn("utc_timestamp", 
                expr("kst_timestamp - INTERVAL 9 HOURS")) \
            .withColumn("date", 
                to_date(col("kst_timestamp"))) \
            .withColumn("year", year(col("kst_timestamp"))) \
            .withColumn("month", month(col("kst_timestamp"))) \
            .withColumn("day", dayofmonth(col("kst_timestamp"))) \
            .withColumn("hour", hour(col("kst_timestamp"))) \
            .withColumn("day_of_week", date_format(col("kst_timestamp"), "E"))
        
        # ✅ 8월과 동일한 최종 컬럼 선택
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
        .withColumn("pipeline_version", lit("september_replay_v1.0")) \
        .dropDuplicates(["event_id"])
        
        final_count = df_final.count()
        logger.info(f"변환 완료: {final_count:,}건")
        
        return df_final
    
    def run(self, data_interval_start: str, data_interval_end: str):
        try:
            self.create_spark_session()
            self.create_silver_table_if_not_exists()
            
            bronze_df = self.read_bronze_partition(data_interval_start)
            if bronze_df is None:
                logger.warning("처리할 데이터 없음")
                return
            
            silver_df = self.transform_to_silver(bronze_df)
            
            # ✅ 8월과 동일: Append만 사용
            logger.info("Silver 테이블에 Append...")
            silver_df.writeTo(self.silver_table).append()
            
            logger.info("Silver 적재 완료")
            
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Replay Bronze to Silver ETL")
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    parser.add_argument("--test-mode", type=lambda x: str(x).lower() == 'true', default=False)
    args = parser.parse_args()
    
    pipeline = ReplayBronzeToSilver(test_mode=args.test_mode)
    pipeline.run(args.data_interval_start, args.data_interval_end)

if __name__ == "__main__":
    main()