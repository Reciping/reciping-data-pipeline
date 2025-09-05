#!/usr/bin/env python3
"""
🧊 Staging to Bronze Iceberg ETL Pipeline (Airflow-triggered, Flexible Time Parsing)
====================================================================================
S3 Staging Area의 Raw JSONL 파일들을 Bronze Iceberg 테이블로 안정적으로 수집합니다.
Airflow로부터 실행 시간(execution_ts)을 인자로 받아 해당 시간대의 파일만 처리하며,
자동 스케줄(ISO 형식)과 수동 테스트(간편 형식) 시간을 모두 지원합니다.
"""
import logging
import argparse
from datetime import datetime
import pytz
from dateutil.parser import isoparse  # 상단으로 이동

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, to_date, lit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StagingToBronzeETL:
    def __init__(self, test_mode: bool = True):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083" # 자신의 Hive Metastore URI로 변경

        if test_mode:
            self.database_name = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.s3_staging_area = "s3a://reciping-user-event-logs/bronze/landing-zone/events/staging-area/"
            self.table_suffix = "_test"
        else:
            self.database_name = "recipe_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.s3_staging_area = "s3a://reciping-user-event-logs/bronze/landing-zone/events/staging-area/"
            self.table_suffix = ""

        # --- 변경점 1: 테이블 전체 이름을 변수로 관리하지 않고, 각 부분만 관리 ---
        self.bronze_table_simple_name = f"bronze_events_iceberg{self.table_suffix}"
        
        print(f"카탈로그: {self.catalog_name}")
        print(f"데이터베이스: {self.database_name}")
        print(f"Staging 경로: {self.s3_staging_area}")
        print(f"Bronze 테이블: {self.bronze_table_simple_name}")

    def create_spark_session(self):
        print("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("StagingToBronzeETL") \
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

    def run_pipeline(self, execution_ts: str):
        try:
            print(f"Staging to Bronze ETL 파이프라인 시작 (입력된 시간: {execution_ts})")
            
            self.create_spark_session()
            self.create_bronze_table_if_not_exists()

            # --- 이 부분이 최종 수정된 시간 파싱 로직입니다 ---
            kst_tz = pytz.timezone('Asia/Seoul')
            kst_dt = None
            
            # 1. 수동 실행을 위한 간편 형식 ('YYYY-MM-DD HH:MM')으로 먼저 파싱 시도
            try:
                dt_obj = datetime.strptime(execution_ts, '%Y-%m-%d %H:%M')
                kst_dt = kst_tz.localize(dt_obj)
                print(f"입력값을 간편 형식(KST)으로 인식: {kst_dt}")
            except ValueError:
                # 2. 간편 형식 실패 시, 자동 스케줄을 위한 ISO 형식으로 파싱 시도
                print("간편 형식 파싱 실패. ISO 형식(UTC)으로 재시도합니다.")
                utc_dt = isoparse(execution_ts)
                kst_dt = utc_dt.astimezone(kst_tz)
                print(f"입력값을 ISO 형식으로 인식 후 KST로 변환: {kst_dt}")

            if kst_dt is None:
                raise ValueError(f"지원하지 않는 시간 형식입니다: {execution_ts}")
            # --- 로직 수정 끝 ---

            # KST 기준 시간에 맞는 파일명 동적 생성
            target_filename = f"events_{kst_dt.strftime('%Y%m%d%H%M')}.jsonl"
            specific_file_path = f"{self.s3_staging_area}{target_filename}"
            
            print(f"처리할 대상 파일 경로: {specific_file_path}")
            try:
                raw_df = self.spark.read.text(specific_file_path)
                if raw_df.rdd.isEmpty():
                    print(f"파일이 비어있습니다: {specific_file_path}. 작업을 종료합니다.")
                    return
            except Exception:
                print(f"파일을 찾을 수 없습니다: {specific_file_path}. 작업을 건너뜁니다.")
                return

            # Bronze 스키마에 맞게 변환
            # --- 변경점 시작 ---
            # 파티션 날짜를 현재 시간이 아닌, 처리 대상 시간(kst_dt) 기준으로 생성
            target_date_str = kst_dt.strftime('%Y-%m-%d')
            
            bronze_df = raw_df.withColumnRenamed("value", "raw_event_string") \
                .withColumn("source_file", input_file_name()) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("ingestion_date", to_date(lit(target_date_str))) # <-- 이 부분이 핵심입니다.
            # --- 변경점 끝 ---
            
            # Bronze Iceberg 테이블에 데이터 추가
            print(f"Bronze Iceberg 테이블의 '{target_date_str}' 파티션에 데이터 저장...")
            bronze_df.writeTo(self.bronze_table_simple_name).append()
            
            print("ETL 파이프라인 성공적으로 완료")
            
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                print("SparkSession 종료")
                self.spark.stop()

def main():
    # ... (이전과 동일한 main 함수) ...
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-ts", required=True, help="Airflow execution timestamp (ISO 8601 or 'YYYY-MM-DD HH:MM')")
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True, help="Run in test mode (true/false)")
    args = parser.parse_args()

    pipeline = StagingToBronzeETL(test_mode=args.test_mode)
    pipeline.run_pipeline(execution_ts=args.execution_ts)


if __name__ == "__main__":
    main()