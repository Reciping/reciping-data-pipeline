#!/usr/bin/env python3
"""
🧊 Dimension Tables Creation Script
====================================
Silver 테이블 및 S3 마스터 파일을 기반으로 Gold Layer의 모든 Dimension 테이블을 생성/업데이트합니다.
"""
import logging
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id, to_date, year, month, dayofmonth, hour, date_format, lit, when, expr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DimensionBuilder:
    def __init__(self, test_mode: bool = True):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.s3_master_path = "s3a://reciping-user-event-logs/meta-data/"

        if test_mode:
            self.source_database = "recipe_analytics_test"
            self.target_database = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.table_suffix = "_test"
        else:
            self.source_database = "recipe_analytics"
            self.target_database = "gold_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.table_suffix = ""
            
        self.silver_table_name = f"{self.catalog_name}.{self.source_database}.user_events_silver{self.table_suffix}"
        
    def create_spark_session(self):
        """SparkSession 생성 및 카탈로그/DB 설정"""
        print("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("DimensionBuilder") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://10.0.11.86:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        print(f"현재 카탈로그를 '{self.catalog_name}'으로 설정합니다.")
        self.spark.sql(f"USE {self.catalog_name}")
        
        print(f"대상 데이터베이스 '{self.target_database}' 생성 및 사용 설정합니다.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.target_database}")
        self.spark.sql(f"USE {self.target_database}")
        
        print("SparkSession 생성 및 설정 완료")

    def _create_dimension(self, table_name: str, source_df: DataFrame, id_cols: list, surrogate_key: str):
        """Silver 테이블 기반 Dimension 생성 헬퍼 함수"""
        print(f"Dimension 테이블 생성/업데이트 중: {table_name}")
        dim_df = source_df.select(*id_cols).where(col(id_cols[0]).isNotNull()).distinct()
        dim_df_with_sk = dim_df.withColumn(surrogate_key, monotonically_increasing_id())
        dim_df_with_sk.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        print(f"{table_name} 처리 완료. 총 {dim_df_with_sk.count():,} 건")

    # --- [핵심 수정] S3의 Parquet 마스터 파일을 읽어 dim_recipe를 생성하는 함수 ---
    def _create_dim_recipe_from_master(self):
        table_name = f"dim_recipe{self.table_suffix}"
        master_file_path = f"{self.s3_master_path}total_recipes.parquet"
        
        print(f"Dimension 테이블 생성/업데이트 중: {table_name} (Source: {master_file_path})")

        # 1. S3에 있는 레시피 마스터 Parquet 파일 읽기
        recipe_master_df = self.spark.read.parquet(master_file_path)

        # 2. 필요한 컬럼 선택 및 이름 변경
        dim_recipe_df = recipe_master_df.select(
            col("id").alias("recipe_id"),
            col("name").alias("recipe_name"),
            col("dish_type"),
            col("ingredient_type"),
            col("method_type"),
            col("situation_type"),
            col("difficulty"),
            col("cooking_time")
        )

        # 3. 고유 식별을 위한 대리 키(Surrogate Key) 생성
        dim_recipe_with_sk = dim_recipe_df.withColumn("recipe_sk", monotonically_increasing_id())

        # 4. 최종 스키마 선택 및 저장
        final_dim_df = dim_recipe_with_sk.select(
            "recipe_sk", "recipe_id", "recipe_name", "dish_type", 
            "ingredient_type", "method_type", "situation_type", "difficulty", "cooking_time"
        )
        
        final_dim_df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)
        print(f"{table_name} 처리 완료. 총 {final_dim_df.count():,} 건")

    def run(self):
        """메인 파이프라인 실행"""
        try:
            self.create_spark_session()
            
            print(f"Silver 테이블에서 데이터 읽기: {self.silver_table_name}")
            silver_df = self.spark.read.table(self.silver_table_name)
            # 성능을 위해 한번 캐싱
            silver_df.cache()

            # --- Silver 테이블 기반 Dimension 테이블 생성 ---
            # self._create_dimension(f"dim_user{self.table_suffix}", silver_df, ["user_id", "anonymous_id", "user_segment", "cooking_style"], "user_sk")
            # [수정 후] ab_test_group 컬럼을 리스트에 추가합니다.
            self._create_dimension(f"dim_user{self.table_suffix}", silver_df, ["user_id", "anonymous_id", "user_segment", "cooking_style", "ab_test_group"], "user_sk")
            self._create_dimension(f"dim_event{self.table_suffix}", silver_df, ["event_name"], "event_sk")
            self._create_dimension(f"dim_event{self.table_suffix}", silver_df, ["event_name"], "event_sk")
            self._create_dimension(f"dim_page{self.table_suffix}", silver_df, ["page_name", "page_url"], "page_sk")

            # --- S3 마스터 파일 기반 dim_recipe 생성 ---
            self._create_dim_recipe_from_master()
            
           # --- [수정] dim_time 테이블 생성 로직 ---
            print(f"Dimension 테이블 생성/업데이트 중: dim_time{self.table_suffix}")
            time_df = self.spark.sql("""
                SELECT explode(sequence(to_timestamp('2025-01-01 00:00:00'), 
                                       to_timestamp('2026-12-31 23:00:00'), 
                                       interval 1 hour)) as ts
            """)
            
            # 이 select 구문에 to_date(...)를 추가합니다.
            dim_time = time_df.select(
                date_format(col("ts"), "yyyyMMddHH").cast("bigint").alias("time_dim_key"),
                col("ts").alias("datetime_kst"),
                to_date(col("ts")).alias("date"), # <-- 이 라인을 추가합니다.
                year(col("ts")).alias("year"),
                month(col("ts")).alias("month"),
                dayofmonth(col("ts")).alias("day"),
                hour(col("ts")).alias("hour"),
                date_format(col("ts"), "E").alias("day_of_week"),
                when(date_format(col("ts"), "E").isin("Sat", "Sun"), True).otherwise(False).alias("is_weekend")
            )

            dim_time.write.format("iceberg").mode("overwrite").saveAsTable(f"dim_time{self.table_suffix}")
            print(f"dim_time{self.table_suffix} 처리 완료.")

            silver_df.unpersist() # 캐시 해제

        except Exception as e:
            logger.error("Dimension 테이블 생성 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Create All Dimension Tables for Gold Layer")
    parser.add_argument("--test-mode", type=lambda x: (str(x).lower() == 'true'), default=True)
    args = parser.parse_args()
    builder = DimensionBuilder(test_mode=args.test_mode)
    builder.run()

if __name__ == "__main__":
    main()