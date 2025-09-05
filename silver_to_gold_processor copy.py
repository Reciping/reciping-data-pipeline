#!/usr/bin/env python3
"""
🧊 Silver to Gold ETL Processor (Incremental, Airflow-triggered)
================================================================
Silver Iceberg 테이블에서 특정 파티션의 데이터를 읽어 Gold Fact 테이블로 변환/집계합니다.
Airflow로부터 실행 시간을 받아 점진적으로 작업을 수행합니다.
"""
import logging
import argparse
from datetime import datetime
import pytz
from dateutil.parser import isoparse

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SilverToGoldProcessor:
    def __init__(self, test_mode: bool = True):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        
        if test_mode:
            print("=== 테스트 모드로 실행 ===")
            self.silver_database = "recipe_analytics_test"
            self.gold_database = "recipe_analytics_test" # Gold도 테스트 DB 사용
            self.table_suffix = "_test"
        else:
            print("=== 운영 모드로 실행 ===")
            self.silver_database = "recipe_analytics"
            self.gold_database = "gold_analytics"
            self.table_suffix = ""
            
        self.silver_table_name = f"{self.catalog_name}.{self.silver_database}.user_events_silver{self.table_suffix}"
        self.gold_table_name = f"{self.catalog_name}.{self.gold_database}.fact_user_events{self.table_suffix}"

    def create_spark_session(self):
        print("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("SilverToGold_ETL") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://10.0.11.86:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        print("SparkSession 생성 완료")

    # --- [신규 추가] Gold 테이블 생성 함수 ---
    def create_gold_table_if_not_exists(self):
        """Gold Fact 테이블이 없으면 생성합니다."""
        print(f"Gold Fact 테이블 생성 확인: {self.gold_table_name}")
        # Silver 테이블의 스키마를 기반으로 Gold 테이블 DDL 작성
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.gold_table_name} (
            event_id STRING,
            user_dim_key BIGINT,
            time_dim_key BIGINT,
            recipe_dim_key BIGINT,
            page_dim_key BIGINT,
            event_dim_key INT,
            event_count INT,
            session_duration_seconds BIGINT,
            page_view_duration_seconds BIGINT,
            is_conversion BOOLEAN,
            conversion_value DOUBLE,
            engagement_score DOUBLE,
            session_id STRING,
            anonymous_id STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(created_at)) -- Gold 테이블은 날짜 기준으로 파티셔닝
        """
        self.spark.sql(create_table_sql)
        print("Gold Fact 테이블 준비 완료")

    def transform_and_load_gold_data(self, target_date: str):
        """특정 날짜의 Silver 데이터를 Gold Fact 테이블로 변환 및 적재합니다."""
        print(f"Silver to Gold 처리 시작 (대상 날짜: {target_date})")
        
        # Gold 테이블에 데이터를 바로 INSERT 하는 SQL 쿼리
        # 기존 코드의 핵심 변환 로직을 그대로 사용
        insert_gold_sql = f"""
        INSERT INTO {self.gold_table_name}
        SELECT 
            s.event_id,
            0 as user_dim_key,
            CAST(DATE_FORMAT(s.kst_timestamp, 'yyyyMMddHH') AS BIGINT) as time_dim_key,
            COALESCE(s.prop_recipe_id, 0) as recipe_dim_key,
            0 as page_dim_key,
            CASE 
                WHEN s.event_name = 'auth_success' THEN 1 WHEN s.event_name = 'create_comment' THEN 2
                WHEN s.event_name = 'click_bookmark' THEN 3 WHEN s.event_name = 'click_recipe' THEN 4
                WHEN s.event_name = 'search_recipe' THEN 5 WHEN s.event_name = 'view_recipe' THEN 6
                WHEN s.event_name = 'view_page' THEN 7 ELSE 0
            END as event_dim_key,
            1 as event_count,
            CASE 
                WHEN s.prop_action IS NOT NULL AND SIZE(SPLIT(s.prop_action, ':')) >= 2
                THEN COALESCE(CAST(SPLIT(s.prop_action, ':')[1] AS BIGINT), 60)
                ELSE 60
            END as session_duration_seconds,
            30 as page_view_duration_seconds,
            CASE WHEN s.event_name IN ('auth_success', 'click_bookmark', 'create_comment') THEN TRUE ELSE FALSE END as is_conversion,
            1.0 as conversion_value,
            CASE 
                WHEN s.event_name = 'auth_success' THEN 10.0 WHEN s.event_name = 'create_comment' THEN 9.0
                WHEN s.event_name = 'click_bookmark' THEN 8.0 WHEN s.event_name = 'click_recipe' THEN 7.0
                WHEN s.event_name = 'search_recipe' THEN 5.0 WHEN s.event_name = 'view_recipe' THEN 4.0
                WHEN s.event_name = 'view_page' THEN 2.0 ELSE 1.0
            END as engagement_score,
            s.session_id,
            s.anonymous_id,
            s.kst_timestamp as created_at,
            s.kst_timestamp as updated_at
        FROM {self.silver_table_name} s
        WHERE s.date = '{target_date}' AND s.event_id IS NOT NULL
        """
        
        try:
            silver_count = self.spark.read.table(self.silver_table_name).where(f"date = '{target_date}'").count()
            if silver_count == 0:
                print(f"{target_date} 날짜의 Silver 데이터가 없습니다. 작업을 건너뜁니다.")
                return

            print(f"{target_date} 날짜의 Silver 데이터 {silver_count}건을 Gold 테이블로 변환합니다.")
            self.spark.sql(insert_gold_sql)
            print("Gold 테이블 적재 완료.")
        except Exception as e:
            logger.error("Gold 데이터 변환/적재 실패", exc_info=True)
            raise

    def run_pipeline(self, execution_ts: str):
        """메인 파이프라인 실행"""
        try:
            self.create_spark_session()
            
            # Airflow가 넘겨준 실행 시간을 기준으로 처리할 날짜(파티션) 결정
            kst_tz = pytz.timezone('Asia/Seoul')
            try:
                dt_obj = datetime.strptime(execution_ts, '%Y-%m-%d %H:%M')
                kst_dt = kst_tz.localize(dt_obj)
            except ValueError:
                utc_dt = isoparse(execution_ts)
                kst_dt = utc_dt.astimezone(kst_tz)
            
            target_date = kst_dt.strftime('%Y-%m-%d')
            
            # --- [신규 추가] Gold 테이블 생성 함수 호출 ---
            self.create_gold_table_if_not_exists()
            
            # --- [신규 추가] Silver 테이블의 최신 메타데이터 강제 새로고침 ---
            print(f"Silver 테이블의 최신 정보 새로고침: {self.silver_table_name}")
            self.spark.catalog.refreshTable(self.silver_table_name)
            # -----------------------------------------------------------
            
            # 이제 최신 상태가 보장된 Silver 테이블에서 데이터를 읽습니다.
            self.transform_and_load_gold_data(target_date)
            
            print("Silver to Gold ETL 파이프라인 성공적으로 완료")
            
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Silver to Gold Iceberg ETL Job (Incremental)")
    parser.add_argument("--execution-ts", required=True, help="Airflow execution timestamp")
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True, help="Run in test mode")
    args = parser.parse_args()

    processor = SilverToGoldProcessor(test_mode=args.test_mode)
    processor.run_pipeline(execution_ts=args.execution_ts)

if __name__ == "__main__":
    main()