#!/usr/bin/env python3
"""
🧊 Staging to Bronze Iceberg ETL Pipeline (Stateless & Idempotent)
===================================================================
S3 Staging Area의 Raw JSONL 파일들을 Bronze Iceberg 테이블로 수집합니다.
Airflow로부터 data_interval_start를 받아 처리할 S3 경로를 동적으로 결정합니다.
"""

import logging
import argparse
from datetime import datetime, timedelta
import pytz
from dateutil.parser import isoparse
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, to_date, lit, col, from_json, to_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StagingToBronzeETL:
    def __init__(self, test_mode: bool = True):
        # ... (__init__ 메소드는 이전 답변의 최종본과 동일) ...
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083" 

        if test_mode:
            self.database_name = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            # self.s3_staging_area_bulk = "s3a://reciping-user-event-logs/bronze/landing-zone/events/event_logs_3m/"
            self.s3_staging_area_bulk = "s3a://reciping-user-event-logs/bronze/landing-zone/events/event_logs_1m/"
            self.s3_staging_area_incremental = "s3a://reciping-user-event-logs/bronze/landing-zone/events/staging-area/"
            self.table_suffix = "_test"
        else:
            self.database_name = "recipe_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.s3_staging_area_bulk = "s3a://reciping-user-event-logs/bronze/landing-zone/events/event_logs_1m/"
            self.s3_staging_area_incremental = "s3a://reciping-user-event-logs/bronze/landing-zone/events/staging-area/"
            self.table_suffix = ""

        self.bronze_table_simple_name = f"bronze_events_iceberg{self.table_suffix}"



    def create_spark_session(self):
        print("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("StagingToBronzeETL") \
            .config("spark.local.dir", "/home/ec2-user/spark-tmp") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        # --- 변경점 2: 사용할 카탈로그와 데이터베이스를 Spark 세션에 명시적으로 지정 ---
        print(f"현재 카탈로그를 '{self.catalog_name}'으로 설정합니다.")
        self.spark.sql(f"USE {self.catalog_name}")
        print(f"현재 데이터베이스를 '{self.database_name}'으로 설정합니다.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        self.spark.sql(f"USE {self.database_name}")
        
        print("SparkSession 생성 및 설정 완료")

    def create_bronze_table_if_not_exists(self):
        # --- 변경점 3: 테이블 이름에 더 이상 카탈로그와 데이터베이스 이름을 포함하지 않음 ---
        print(f"Bronze Iceberg 테이블 생성 확인: {self.bronze_table_simple_name}")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.bronze_table_simple_name} (
            raw_event_string STRING,
            source_file STRING,
            ingestion_timestamp TIMESTAMP,
            ingestion_date DATE
        ) USING ICEBERG PARTITIONED BY (ingestion_date)
        """
        self.spark.sql(create_table_sql)
        print("Bronze Iceberg 테이블 준비 완료")
        

    def run_pipeline(self, execution_ts: Optional[str] = None, input_file_name: Optional[str] = None, target_date: Optional[str] = None):
        """
        인자에 따라 벌크 또는 증분 모드로 ETL 파이프라인을 실행합니다.
        target_date 없는 벌크 모드 지원 추가.
        """
        try:
            self.create_spark_session()
            self.create_bronze_table_if_not_exists()

            specific_file_path = ""
            target_date_str = ""

            # --- [핵심] 인자에 따라 분기 처리 ---
            # 경로 1: 벌크 처리 모드 (target_date 지정)
            if input_file_name and target_date:
                print(f"벌크 처리 모드로 실행 (입력 파일: {input_file_name}, 파티션 날짜: {target_date})")
                specific_file_path = f"{self.s3_staging_area_bulk}{input_file_name}"
                target_date_str = target_date
            
            # 경로 1-2: 벌크 처리 모드 (target_date 없음 - 현재 날짜 사용)
            elif input_file_name and not target_date:
                current_date = datetime.now().strftime('%Y-%m-%d')
                print(f"벌크 처리 모드로 실행 (입력 파일: {input_file_name}, 파티션 날짜: {current_date} - 자동 설정)")
                specific_file_path = f"{self.s3_staging_area_bulk}{input_file_name}"
                target_date_str = current_date
            
            # 경로 2: 증분 처리 모드
            elif execution_ts:
                print(f"증분 처리 모드로 실행 (입력된 시간: {execution_ts})")
                kst_tz = pytz.timezone('Asia/Seoul')
                try:
                    dt_obj = datetime.strptime(execution_ts, '%Y-%m-%d %H:%M')
                    kst_dt = kst_tz.localize(dt_obj)
                except ValueError:
                    utc_dt = isoparse(execution_ts)
                    kst_dt = utc_dt.astimezone(kst_tz)
                
                target_filename = f"events_{kst_dt.strftime('%Y%m%d%H%M')}.jsonl"
                specific_file_path = f"{self.s3_staging_area_incremental}{target_filename}"
                target_date_str = kst_dt.strftime('%Y-%m-%d')
            
            # 잘못된 인자
            else:
                raise ValueError("실행 모드를 결정할 수 없습니다. --execution-ts 또는 --input-file-name 인자가 필요합니다.")

            # --- 공통 실행 로직 ---
            print(f"처리할 대상 파일 경로: {specific_file_path}")
            print(f"ingestion_date로 사용될 날짜: {target_date_str}")
            
            try:
                raw_df = self.spark.read.text(specific_file_path)
                if raw_df.rdd.isEmpty():
                    print(f"파일이 비어있습니다: {specific_file_path}. 작업을 종료합니다.")
                    return
            except Exception:
                print(f"파일을 찾을 수 없습니다: {specific_file_path}. 작업을 건너뜁니다.")
                return

            bronze_df = raw_df.withColumnRenamed("value", "raw_event_string") \
                .withColumn("source_file", lit(specific_file_path)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("ingestion_date", to_date(lit(target_date_str)))
            
            print(f"Bronze Iceberg 테이블의 '{target_date_str}' 파티션에 데이터 저장...")
            bronze_df.writeTo(self.bronze_table_simple_name).append()
            print("ETL 파이프라인 성공적으로 완료")

        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Unified Staging to Bronze ETL Job (Bulk or Incremental)")
    # 두 모드의 인자를 모두 받되, 필수는 아니도록 설정
    parser.add_argument("--execution-ts", required=False, help="For incremental runs (ISO 8601 or 'YYYY-MM-DD HH:MM')")
    parser.add_argument("--input-file-name", required=False, help="For bulk runs: The name of the file in S3 staging")
    parser.add_argument("--target-date", required=False, help="For bulk runs: The logical date for the data batch (YYYY-MM-DD)")
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True)
    args = parser.parse_args()

    pipeline = StagingToBronzeETL(test_mode=args.test_mode)
    pipeline.run_pipeline(
        execution_ts=args.execution_ts, 
        input_file_name=args.input_file_name, 
        target_date=args.target_date
    )

if __name__ == "__main__":
    main()
