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
from pyspark.sql.functions import when, size, split, coalesce, cast, col, lit, date_format

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
            
        # self.silver_table_name = f"{self.catalog_name}.{self.silver_database}.user_events_silver{self.table_suffix}"
        # self.gold_table_name = f"{self.catalog_name}.{self.gold_database}.fact_user_events{self.table_suffix}"
        self.silver_table_name = f"user_events_silver{self.table_suffix}" # <--- 단순 이름으로 변경
        self.gold_table_name = f"fact_user_events{self.table_suffix}" # <--- 단순 이름으로 변경

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

        # --- 이 부분이 핵심적인 수정입니다 ---
        print(f"현재 카탈로그를 '{self.catalog_name}'으로 설정합니다.")
        self.spark.sql(f"USE {self.catalog_name}")
        
        # Gold 테이블은 gold_database에, Silver 테이블은 silver_database에 있습니다.
        # 따라서 Gold 테이블을 만들기 전에 target database를 사용하도록 설정합니다.
        print(f"대상 데이터베이스 '{self.gold_database}' 생성 및 사용 설정합니다.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.gold_database}")
        self.spark.sql(f"USE {self.gold_database}")
        # --- 수정 끝 ---

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
        """Silver 및 Dimension 테이블들을 조인하여 Gold Fact 테이블로 변환 및 적재합니다."""
        
        print(f"Silver to Gold 처리 시작 (대상 날짜: {target_date})")
        
        try:
            # --- [변경점] 읽어올 테이블들의 전체 이름(DB.TABLE)을 명시적으로 생성 ---
            full_silver_table = f"{self.silver_database}.{self.silver_table_name}"
            dim_user_table = f"{self.gold_database}.dim_user{self.table_suffix}"
            dim_recipe_table = f"{self.gold_database}.dim_recipe{self.table_suffix}"
            dim_event_table = f"{self.gold_database}.dim_event{self.table_suffix}"
            dim_page_table = f"{self.gold_database}.dim_page{self.table_suffix}"

            # 1. Silver 테이블에서 해당 날짜의 데이터 읽기 (전체 이름 사용)
            silver_df = self.spark.read.table(full_silver_table).where(f"date = '{target_date}'")
            
            silver_count = silver_df.count()
            if silver_count == 0:
                print(f"{target_date} 날짜의 Silver 데이터가 없습니다. 작업을 건너뜁니다.")
                return

            print(f"{target_date} 날짜의 Silver 데이터 {silver_count}건을 Gold 테이블로 변환합니다.")

            # 2. 필요한 모든 Dimension 테이블 읽기 (전체 이름 사용)
            dim_user = self.spark.read.table(dim_user_table)
            dim_recipe = self.spark.read.table(dim_recipe_table)
            dim_event = self.spark.read.table(dim_event_table)
            dim_page = self.spark.read.table(dim_page_table)

            # 3. Silver 데이터와 모든 Dimension을 순차적으로 조인
            joined_df = silver_df \
                .join(dim_user, ["user_id", "anonymous_id", "user_segment", "cooking_style"], "left") \
                .join(dim_recipe, silver_df.prop_recipe_id == dim_recipe.recipe_id, "left") \
                .join(dim_event, "event_name", "left") \
                .join(dim_page, ["page_name", "page_url"], "left")

            # 4. 최종 Fact 테이블 형태 생성 (DataFrame API 사용)
            # from pyspark.sql.functions import when, size, split, coalesce, lit, date_format
            
            fact_df = joined_df.select(
                col("event_id"),
                coalesce(col("user_sk"), lit(0)).alias("user_dim_key"),
                date_format(col("kst_timestamp"), "yyyyMMddHH").cast("bigint").alias("time_dim_key"),
                coalesce(col("recipe_sk"), lit(0)).alias("recipe_dim_key"),
                coalesce(col("page_sk"), lit(0)).alias("page_dim_key"),
                coalesce(col("event_sk"), lit(0)).alias("event_dim_key"),
                lit(1).alias("event_count"),
                when(col("prop_action").isNotNull() & (size(split(col("prop_action"), ":")) >= 2), 
                     coalesce(split(col("prop_action"), ":")[1].cast("bigint"), lit(60)))
                .otherwise(60).alias("session_duration_seconds"),
                lit(30).cast("bigint").alias("page_view_duration_seconds"),
                when(col("event_name").isin('auth_success', 'click_bookmark', 'create_comment'), True).otherwise(False).alias("is_conversion"),
                lit(1.0).alias("conversion_value"),
                when(col("event_name") == 'auth_success', 10.0).when(col("event_name") == 'create_comment', 9.0)
                .when(col("event_name") == 'click_bookmark', 8.0).when(col("event_name") == 'click_recipe', 7.0)
                .when(col("event_name") == 'search_recipe', 5.0).when(col("event_name") == 'view_recipe', 4.0)
                .when(col("event_name") == 'view_page', 2.0).otherwise(1.0).alias("engagement_score"),
                col("session_id"),
                col("anonymous_id"),
                col("kst_timestamp").alias("created_at"),
                col("kst_timestamp").alias("updated_at")
            )

            # 5. Gold 테이블에 데이터 추가 (Append) (전체 이름 사용)
            print("Gold 테이블 적재 중...")
            fact_df.writeTo(f"{self.gold_database}.{self.gold_table_name}").append()
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