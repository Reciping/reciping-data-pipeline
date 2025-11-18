#!/usr/bin/env python3
"""
🧊 Silver to Gold ETL Processor (Stateless & Idempotent)
==========================================================
Silver Iceberg 테이블과 Dimension 테이블들을 조인하여 Gold Fact 테이블로 변환/집계합니다.
Airflow로부터 실행 시간을 받아 점진적으로 작업을 수행합니다.
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
    year, month, dayofmonth, hour, date_format, expr, when, size, split, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType, DateType, LongType, ArrayType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SilverToGoldProcessor:
    def __init__(self, test_mode: bool = True):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083" # 자신의 Hive Metastore URI

        if test_mode:
            self.silver_database = "recipe_analytics_test"
            self.gold_database = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.table_suffix = "_test"
        else:
            self.silver_database = "recipe_analytics"
            self.gold_database = "gold_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.table_suffix = ""
            
        # 변수에는 카탈로그/DB이름 없이 순수 테이블 이름만 저장
        self.silver_table_name = f"user_events_silver{self.table_suffix}"
        self.gold_table_name = f"fact_user_events{self.table_suffix}"

    def create_spark_session(self):
        """SparkSession 생성 및 카탈로그/DB 설정"""
        print("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("SilverToGold_ETL") \
            .config("spark.local.dir", "/home/ec2-user/spark-tmp") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        # 세션의 기본 카탈로그와 데이터베이스를 Gold 용으로 설정
        print(f"현재 카탈로그를 '{self.catalog_name}'으로 설정합니다.")
        self.spark.sql(f"USE {self.catalog_name}")
        
        print(f"대상 데이터베이스 '{self.gold_database}' 생성 및 사용 설정합니다.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.gold_database}")
        self.spark.sql(f"USE {self.gold_database}")
        
        print("SparkSession 생성 및 설정 완료")

    def create_gold_table_if_not_exists(self):
        """Gold Fact 테이블이 없으면 생성합니다."""
        print(f"Gold Fact 테이블 생성 확인: {self.gold_table_name}")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.gold_table_name} (
            event_id STRING, user_dim_key BIGINT, time_dim_key BIGINT, recipe_dim_key BIGINT,
            page_dim_key BIGINT, event_dim_key BIGINT, event_count INT,
            session_duration_seconds BIGINT, page_view_duration_seconds BIGINT,
            is_conversion BOOLEAN, conversion_value DOUBLE, engagement_score DOUBLE,
            session_id STRING, anonymous_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(created_at))
        """
        self.spark.sql(create_table_sql)
        print("Gold Fact 테이블 준비 완료")

    
    # def transform_and_load_gold_data(self, target_date: Optional[str] = None):
    #     """
    #     개선된 버전: JOIN 조건을 더 유연하게 처리하여 데이터 손실을 최소화합니다.
    #     """
    #     if target_date:
    #         print(f"Silver to Gold 증분 처리 시작 (대상 날짜: {target_date})")
    #     else:
    #         print("Silver to Gold 벌크 처리 시작 (전체 데이터)")

    #     full_silver_table = f"{self.silver_database}.{self.silver_table_name}"
    #     dim_user_table = f"dim_user{self.table_suffix}"
    #     dim_recipe_table = f"dim_recipe{self.table_suffix}"
    #     dim_event_table = f"dim_event{self.table_suffix}"
    #     dim_page_table = f"dim_page{self.table_suffix}"
        
    #     try:
    #         # Silver 데이터 읽기
    #         silver_df_reader = self.spark.read.table(full_silver_table)
    #         if target_date:
    #             silver_df = silver_df_reader.where(f"date = '{target_date}'")
    #         else:
    #             silver_df = silver_df_reader

    #         silver_count = silver_df.count()
    #         if silver_count == 0:
    #             print(f"처리할 Silver 데이터가 없습니다.")
    #             return

    #         print(f"Silver 데이터 {silver_count:,}건을 Gold 테이블로 변환합니다.")

    #         # Dimension 테이블들 읽기
    #         dim_user = self.spark.read.table(dim_user_table)
    #         dim_recipe = self.spark.read.table(dim_recipe_table)
    #         dim_event = self.spark.read.table(dim_event_table)
    #         dim_page = self.spark.read.table(dim_page_table)

    #         # ===== 핵심 수정 부분: 더 유연한 JOIN 조건 =====
            
    #         # 1. user_id와 anonymous_id만으로 JOIN (필수 키만 사용)
    #         joined_df = silver_df.alias("s") \
    #             .join(
    #                 dim_user.alias("du"), 
    #                 (col("s.user_id") == col("du.user_id")) & 
    #                 (col("s.anonymous_id") == col("du.anonymous_id")),
    #                 "left"
    #             ) \
    #             .join(
    #                 dim_recipe.alias("dr"), 
    #                 col("s.prop_recipe_id") == col("dr.recipe_id"), 
    #                 "left"
    #             ) \
    #             .join(
    #                 dim_event.alias("de"), 
    #                 col("s.event_name") == col("de.event_name"), 
    #                 "left"
    #             ) \
    #             .join(
    #                 dim_page.alias("dp"), 
    #                 (col("s.page_name") == col("dp.page_name")) & 
    #                 (col("s.page_url") == col("dp.page_url")), 
    #                 "left"
    #             )

    #         # 2. 최종 Fact 테이블 생성 (컬럼명 명시적 지정)
    #         fact_df = joined_df.select(
    #             col("s.event_id"),
    #             coalesce(col("du.user_sk"), lit(0)).alias("user_dim_key"),
    #             date_format(col("s.kst_timestamp"), "yyyyMMddHH").cast("bigint").alias("time_dim_key"),
    #             coalesce(col("dr.recipe_sk"), lit(0)).alias("recipe_dim_key"),
    #             coalesce(col("dp.page_sk"), lit(0)).alias("page_dim_key"),
    #             coalesce(col("de.event_sk"), lit(0)).alias("event_dim_key"),
    #             lit(1).alias("event_count"),
    #             when(col("s.prop_action").isNotNull() & (size(split(col("s.prop_action"), ":")) >= 2), 
    #                  coalesce(split(col("s.prop_action"), ":")[1].cast("bigint"), lit(60)))
    #             .otherwise(60).alias("session_duration_seconds"),
    #             lit(30).cast("bigint").alias("page_view_duration_seconds"),
    #             when(col("s.event_name").isin('auth_success', 'click_bookmark', 'create_comment'), True)
    #             .otherwise(False).alias("is_conversion"),
    #             lit(1.0).alias("conversion_value"),
    #             when(col("s.event_name") == 'auth_success', 10.0)
    #             .when(col("s.event_name") == 'create_comment', 9.0)
    #             .when(col("s.event_name") == 'click_bookmark', 8.0)
    #             .when(col("s.event_name") == 'click_recipe', 7.0)
    #             .when(col("s.event_name") == 'search_recipe', 5.0)
    #             .when(col("s.event_name") == 'view_recipe', 4.0)
    #             .when(col("s.event_name") == 'view_page', 2.0)
    #             .otherwise(1.0).alias("engagement_score"),
    #             col("s.session_id"),
    #             col("s.anonymous_id"),
    #             col("s.kst_timestamp").alias("created_at"),
    #             col("s.kst_timestamp").alias("updated_at")
    #         )

    #         # 데이터 적재
    #         if target_date:
    #             print("Gold 테이블에 증분 데이터 추가(Append)...")
    #             fact_df.writeTo(self.gold_table_name).append()
    #         else:
    #             print("Gold 테이블 전체 데이터 덮어쓰기(Overwrite)...")
    #             fact_df.write.format("iceberg").mode("overwrite").saveAsTable(self.gold_table_name)

    #         print("Gold 테이블 적재 완료.")

    #     except Exception as e:
    #         logger.error("Gold 데이터 변환/적재 실패", exc_info=True)
    #         raise


    def update_dim_user_if_needed(self, silver_df):
        """
        Silver의 신규 사용자를 dim_user에 자동 추가
        
        Args:
            silver_df: Silver DataFrame
        """
        try:
            logger.info("신규 사용자 확인 중...")
            
            dim_user_table = f"dim_user{self.table_suffix}"
            
            # 기존 dim_user 읽기
            dim_user = self.spark.read.table(dim_user_table)
            existing_user_ids = dim_user.select("user_id", "anonymous_id").where(col("user_id").isNotNull())
            
            # Silver의 고유 사용자 추출
            silver_users = silver_df.select(
                "user_id", 
                "anonymous_id", 
                "user_segment", 
                "cooking_style", 
                "ab_test_group"
            ).where(col("user_id").isNotNull()).distinct()
            
            # 신규 사용자 필터링 (user_id + anonymous_id 조합으로 확인)
            new_users = silver_users.alias("su").join(
                existing_user_ids.alias("eu"),
                (col("su.user_id") == col("eu.user_id")) & 
                (col("su.anonymous_id") == col("eu.anonymous_id")),
                "left_anti"
            )
            
            new_count = new_users.count()
            
            if new_count > 0:
                logger.info(f"신규 사용자 {new_count}명 발견 - dim_user 업데이트 중...")
                
                # 현재 최대 user_sk 조회
                max_sk_result = self.spark.sql(f"""
                    SELECT COALESCE(MAX(user_sk), 0) as max_sk 
                    FROM {dim_user_table}
                """).collect()
                current_max_sk = max_sk_result[0]['max_sk']
                
                logger.info(f"현재 최대 user_sk: {current_max_sk}")
                
                # user_sk 생성
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number
                
                window_spec = Window.orderBy("user_id")
                new_users_with_sk = new_users.withColumn(
                    "user_sk", 
                    row_number().over(window_spec) + lit(current_max_sk)
                ).select(
                    "user_sk", 
                    "user_id", 
                    "anonymous_id",
                    "user_segment", 
                    "cooking_style", 
                    "ab_test_group"
                )
                
                # dim_user에 APPEND
                new_users_with_sk.writeTo(dim_user_table).append()
                
                logger.info(f"✅ dim_user에 {new_count}명 추가 완료")
                
                # 검증
                new_max_sk = self.spark.sql(f"""
                    SELECT MAX(user_sk) as max_sk 
                    FROM {dim_user_table}
                """).collect()[0]['max_sk']
                
                logger.info(f"업데이트 후 최대 user_sk: {new_max_sk}")
            else:
                logger.info("신규 사용자 없음 - dim_user 업데이트 스킵")
                
        except Exception as e:
            logger.warning(f"dim_user 업데이트 실패 (계속 진행): {e}", exc_info=True)


    def transform_and_load_gold_data(self, target_date: Optional[str] = None):
        """개선된 Silver to Gold 변환 - 견고한 JOIN 처리"""
        if target_date:
            print(f"Silver to Gold 증분 처리 시작 (대상 날짜: {target_date})")
        else:
            print("Silver to Gold 벌크 처리 시작 (전체 데이터)")

        full_silver_table = f"{self.silver_database}.{self.silver_table_name}"
        dim_user_table = f"dim_user{self.table_suffix}"
        dim_recipe_table = f"dim_recipe{self.table_suffix}"
        dim_event_table = f"dim_event{self.table_suffix}"
        dim_page_table = f"dim_page{self.table_suffix}"
        
        try:
            # Silver 데이터 읽기
            silver_df_reader = self.spark.read.table(full_silver_table)
            if target_date:
                silver_df = silver_df_reader.where(f"date = '{target_date}'")
            else:
                silver_df = silver_df_reader

            silver_count = silver_df.count()
            if silver_count == 0:
                print(f"처리할 Silver 데이터가 없습니다.")
                return

            print(f"Silver 데이터 {silver_count:,}건을 Gold 테이블로 변환합니다.")

            # ✅ 신규 사용자 자동 업데이트 
            self.update_dim_user_if_needed(silver_df)

            # Dimension 테이블들 읽기 (dim_user 다시 읽기 - 방금 업데이트 반영)
            dim_user = self.spark.read.table(dim_user_table)
            dim_recipe = self.spark.read.table(dim_recipe_table)
            dim_event = self.spark.read.table(dim_event_table)
            dim_page = self.spark.read.table(dim_page_table)

            # === 개선된 JOIN 로직 ===
            
            # 1. User JOIN (user_id + anonymous_id 기준)
            joined_df = silver_df.alias("s") \
                .join(
                    dim_user.alias("du"), 
                    (col("s.user_id") == col("du.user_id")) & 
                    (col("s.anonymous_id") == col("du.anonymous_id")),
                    "left"
                )
            
            # 2. Recipe JOIN (데이터 타입 안전성 보장)
            joined_df = joined_df.join(
                dim_recipe.alias("dr"), 
                col("s.prop_recipe_id").cast("string") == col("dr.recipe_id").cast("string"), 
                "left"
            )
            
            # 3. Event JOIN (NULL 안전 처리)
            joined_df = joined_df.join(
                dim_event.alias("de"), 
                (col("s.event_name") == col("de.event_name")) & 
                col("s.event_name").isNotNull(), 
                "left"
            )
            
            # 4. Page JOIN (NULL 값 처리 개선)
            joined_df = joined_df.join(
                dim_page.alias("dp"), 
                (coalesce(col("s.page_name"), lit("")) == coalesce(col("dp.page_name"), lit(""))) & 
                (coalesce(col("s.page_url"), lit("")) == coalesce(col("dp.page_url"), lit(""))) &
                (col("s.page_name").isNotNull() | col("s.page_url").isNotNull()), 
                "left"
            )

            # 5. 최종 Fact 테이블 생성
            fact_df = joined_df.select(
                col("s.event_id"),
                coalesce(col("du.user_sk"), lit(0)).alias("user_dim_key"),
                date_format(col("s.kst_timestamp"), "yyyyMMddHH").cast("bigint").alias("time_dim_key"),
                coalesce(col("dr.recipe_sk"), lit(0)).alias("recipe_dim_key"),
                coalesce(col("dp.page_sk"), lit(0)).alias("page_dim_key"),
                coalesce(col("de.event_sk"), lit(0)).alias("event_dim_key"),
                lit(1).alias("event_count"),
                when(col("s.prop_action").isNotNull() & (size(split(col("s.prop_action"), ":")) >= 2), 
                    coalesce(split(col("s.prop_action"), ":")[1].cast("bigint"), lit(60)))
                .otherwise(60).alias("session_duration_seconds"),
                lit(30).cast("bigint").alias("page_view_duration_seconds"),
                when(col("s.event_name").isin('auth_success', 'click_bookmark', 'create_comment'), True)
                .otherwise(False).alias("is_conversion"),
                lit(1.0).alias("conversion_value"),
                when(col("s.event_name") == 'auth_success', 10.0)
                .when(col("s.event_name") == 'create_comment', 9.0)
                .when(col("s.event_name") == 'click_bookmark', 8.0)
                .when(col("s.event_name") == 'click_recipe', 7.0)
                .when(col("s.event_name") == 'search_recipe', 5.0)
                .when(col("s.event_name") == 'view_recipe', 4.0)
                .when(col("s.event_name") == 'view_page', 2.0)
                .otherwise(1.0).alias("engagement_score"),
                col("s.session_id"),
                col("s.anonymous_id"),
                col("s.kst_timestamp").alias("created_at"),
                col("s.kst_timestamp").alias("updated_at")
            )

            # 6. JOIN 성공률 로깅
            total_count = fact_df.count()
            user_join_success = fact_df.where(col("user_dim_key") != 0).count()
            recipe_join_success = fact_df.where(col("recipe_dim_key") != 0).count()
            page_join_success = fact_df.where(col("page_dim_key") != 0).count()
            event_join_success = fact_df.where(col("event_dim_key") != 0).count()
            
            print(f"JOIN 성공률:")
            print(f"  User: {user_join_success:,}/{total_count:,} ({user_join_success/total_count*100:.1f}%)")
            print(f"  Recipe: {recipe_join_success:,}/{total_count:,} ({recipe_join_success/total_count*100:.1f}%)")
            print(f"  Page: {page_join_success:,}/{total_count:,} ({page_join_success/total_count*100:.1f}%)")
            print(f"  Event: {event_join_success:,}/{total_count:,} ({event_join_success/total_count*100:.1f}%)")

            # 7. 데이터 적재
            if target_date:
                print("Gold 테이블에 증분 데이터 추가(Append)...")
                fact_df.writeTo(self.gold_table_name).append()
            else:
                print("Gold 테이블 전체 데이터 덮어쓰기(Overwrite)...")
                fact_df.write.format("iceberg").mode("overwrite").saveAsTable(self.gold_table_name)

            print("Gold 테이블 적재 완료.")

        except Exception as e:
            logger.error("Gold 데이터 변환/적재 실패", exc_info=True)
            raise

#     def run_pipeline(self, target_date: Optional[str] = None):
#         """
#         메인 파이프라인을 실행합니다.
#         target_date가 있으면 증분 모드, 없으면 벌크 모드로 동작합니다.
#         """
#         try:
#             self.create_spark_session()
#             self.create_gold_table_if_not_exists()
            
#             # Silver 테이블의 최신 메타데이터 정보를 불러옵니다.
#             # Airflow 등 외부에서 Silver 테이블이 업데이트된 직후 이 잡을 실행할 때 필요합니다.
#             full_silver_table_name = f"{self.silver_database}.{self.silver_table_name}"
#             print(f"Silver 테이블의 최신 정보 새로고침: {full_silver_table_name}")
#             self.spark.catalog.refreshTable(full_silver_table_name)
            
#             # target_date 인자를 transform 함수로 그대로 전달합니다.
#             self.transform_and_load_gold_data(target_date)
            
#             print("Silver to Gold ETL 파이프라인 성공적으로 완료")
            
#         except Exception as e:
#             logger.error("파이프라인 실패", exc_info=True)
#             raise
#         finally:
#             if self.spark:
#                 self.spark.stop()

# def main():
#     parser = argparse.ArgumentParser(description="Silver to Gold Iceberg ETL Job")
#     parser.add_argument("--target-date", required=False, help="Target date for incremental processing (YYYY-MM-DD)")
#     parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True)
#     args = parser.parse_args()

#     processor = SilverToGoldProcessor(test_mode=args.test_mode)
#     processor.run_pipeline(target_date=args.target_date)

    def run_pipeline(self, data_interval_start: Optional[str] = None, data_interval_end: Optional[str] = None):
        """
        [수정됨] data_interval_start를 기반으로 처리할 Silver 데이터의 날짜를 결정합니다.
        """
        try:
            self.create_spark_session()
            self.create_gold_table_if_not_exists()
            
            if not data_interval_start:
                raise ValueError("증분 처리를 위해 --data-interval-start 인자가 반드시 필요합니다.")

            full_silver_table_name = f"{self.silver_database}.{self.silver_table_name}"
            self.spark.catalog.refreshTable(full_silver_table_name)

            # === 증분 모드 ===
            print(f"증분 처리 모드로 실행: {data_interval_start} ~ {data_interval_end}")
            
            # 1. data_interval_start(UTC)를 KST 기준으로 변환하여 'YYYY-MM-DD' 날짜 획득
            start_time_utc = isoparse(data_interval_start)
            kst_tz = pytz.timezone('Asia/Seoul')
            start_time_kst = start_time_utc.astimezone(kst_tz)
            target_date_str = start_time_kst.strftime('%Y-%m-%d')
            
            print(f"Silver 테이블의 처리 대상 파티션 날짜: {target_date_str}")
            
            # 2. 기존 변환 함수에 target_date 인자를 전달하여 실행
            self.transform_and_load_gold_data(target_date=target_date_str)
            
            print("Silver to Gold ETL 파이프라인 성공적으로 완료")
                
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Stateless Silver to Gold ETL Job")
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True)
    args = parser.parse_args()

    processor = SilverToGoldProcessor(test_mode=args.test_mode)
    processor.run_pipeline(
        data_interval_start=args.data_interval_start,
        data_interval_end=args.data_interval_end
    )

if __name__ == "__main__":
    main()