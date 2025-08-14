#!/usr/bin/env python3
"""
🧊 Iceberg + Hive Metastore 기반 Bronze to Silver ETL Pipeline
=============================================================

Apache Iceberg 테이블 형식을 사용하여 고급 데이터 레이크 기능들을 구현합니다:
- ACID 트랜잭션
- 스키마 진화 (Schema Evolution)
- 타임 트래블 (Time Travel)
- 스냅샷 관리
- Hive Metastore를 통한 테이블 메타데이터 관리

Author: Data Engineering Team
Date: 2025-08-08
"""

import os
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, 
    to_timestamp, date_format, year, month, dayofmonth, hour,
    expr, from_json, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, DateType, LongType, ArrayType
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IcebergETLPipeline:
    """Iceberg 기반 ETL 파이프라인 클래스"""
    
    def __init__(self):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.database_name = "recipe_analytics"
        
        # S3 경로 설정 - 운영 환경용 S3 사용
        self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
        self.s3_landing_zone = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
        
        # Hive Metastore 설정
        self.hive_metastore_uri = "thrift://metastore:9083"
        
    def create_spark_session(self) -> SparkSession:
        """Iceberg와 Hive Metastore를 지원하는 SparkSession 생성"""
        
        print("🧊 Iceberg + Hive Metastore SparkSession 생성 중...")
        
        try:
            spark = SparkSession.builder \
                .appName("IcebergETLPipeline") \
                .config("spark.sql.session.timeZone", "Asia/Seoul") \
                .config("spark.driver.memory", "3g") \
                .config("spark.executor.memory", "3g") \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.maxBatchSize", "128MB") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
                .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
                .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.block.size", "134217728") \
                .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
                .config("spark.hadoop.fs.s3a.fast.upload", "true") \
                .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
                .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
                .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # 로그 레벨 설정
            spark.sparkContext.setLogLevel("WARN")
            
            print("✅ Iceberg SparkSession 생성 완료!")
            print(f"📍 Warehouse 경로: {self.s3_warehouse_path}")
            print(f"🗄️  Hive Metastore URI: {self.hive_metastore_uri}")
            
            self.spark = spark
            return spark
            
        except Exception as e:
            print(f"❌ SparkSession 생성 실패: {str(e)}")
            raise
    
    def create_database_if_not_exists(self):
        """Iceberg 데이터베이스 생성"""
        try:
            print(f"🗃️  데이터베이스 생성: {self.catalog_name}.{self.database_name}")
            
            self.spark.sql(f"""
                CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}
                COMMENT 'Recipe Analytics Database for Iceberg Tables'
                LOCATION '{self.s3_warehouse_path}{self.database_name}.db/'
            """)
            
            print(f"✅ 데이터베이스 준비 완료!")
            
        except Exception as e:
            print(f"❌ 데이터베이스 생성 실패: {str(e)}")
            raise
    
    def define_event_schema(self) -> StructType:
        """이벤트 데이터 스키마 정의 - 실제 데이터 구조에 맞춤"""
        return StructType([
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
    
    def read_from_landing_zone(self) -> DataFrame:
        """랜딩 존에서 원시 데이터 읽기"""
        try:
            print("📂 랜딩 존에서 데이터 읽기 시작...")
            print(f"📍 경로: {self.s3_landing_zone}")
            
            # JSON 스키마 정의
            schema = self.define_event_schema()
            
            # 랜딩 존에서 JSON 파일 읽기
            df = self.spark.read \
                .option("multiline", "false") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(schema) \
                .json(self.s3_landing_zone)
            
            row_count = df.count()
            print(f"✅ 랜딩 존 데이터 로드 성공! 행 수: {row_count:,}")
            
            return df
            
        except Exception as e:
            print(f"❌ 랜딩 존 데이터 읽기 실패: {str(e)}")
            raise
    
    def clean_and_transform_data(self, df: DataFrame) -> DataFrame:
        """데이터 정제 및 변환 - archive/old_versions/bronze_to_silver.py 로직 완전 적용"""
        try:
            print("🧹 데이터 정제 및 변환 시작...")
            
            # --- 4.1. 중첩된 JSON 문자열을 파싱하기 위한 스키마를 명시적으로 정의 ---
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
            # Bronze timestamp는 KST로 되어 있음: "2025-07-07T08:40:12.782565795+09:00"
            df_transformed = df \
                .withColumn("parsed_context", from_json(col("context"), context_schema)) \
                .withColumn("parsed_properties", from_json(col("event_properties"), event_properties_schema)) \
                .withColumn("kst_timestamp", col("timestamp").cast(TimestampType())) \
                .withColumn("utc_timestamp", expr("kst_timestamp - INTERVAL 9 HOURS")) \
                .withColumn("date", col("date").cast(DateType())) \
                .drop("context", "event_properties", "timestamp")

            print("✅ JSON 파싱 및 KST/UTC 타임스탬프 변환 완료.")

            # --- 4.3. Silver Layer 저장을 위한 파티션 컬럼 생성 (KST 기준) ---
            # kst_timestamp를 기준으로 파티션 컬럼 생성
            df_with_partitions = df_transformed \
                .withColumn("year", year(col("kst_timestamp"))) \
                .withColumn("month", month(col("kst_timestamp"))) \
                .withColumn("day", dayofmonth(col("kst_timestamp"))) \
                .withColumn("hour", hour(col("kst_timestamp"))) \
                .withColumn("day_of_week", date_format(col("kst_timestamp"), "E")) # 'Mon', 'Tue' 등 요일 추출

            print("✅ KST 기준 파티션 컬럼(year, month, day, hour) 생성 완료.")

            # --- 4.4. 최종 컬럼 선택 및 정리 (평탄화) - 메모리 최적화를 위해 핵심 컬럼만 선택 ---
            df_silver = df_with_partitions.select(
                # 기본 이벤트 정보
                "event_id", 
                "event_name", 
                "user_id", 
                "anonymous_id", 
                "session_id", 
                
                # 시간 관련 컬럼 (KST와 UTC 모두 포함)
                "kst_timestamp",  # 한국 시간 (원본)
                "utc_timestamp",  # UTC 시간 (변환됨)
                "date",
                
                # KST 기준 파생 컬럼들
                "year", 
                "month", 
                "day", 
                "hour",
                "day_of_week",
                
                # Context에서 주요 컬럼들만 선택
                col("parsed_context.page.name").alias("page_name"),
                col("parsed_context.page.url").alias("page_url"),
                col("parsed_context.user_segment").alias("user_segment"),
                col("parsed_context.cooking_style").alias("cooking_style"),
                col("parsed_context.ab_test.group").alias("ab_test_group"),
                
                # Event Properties에서 주요 컬럼들만 선택
                col("parsed_properties.recipe_id").cast(LongType()).alias("prop_recipe_id"),
                col("parsed_properties.list_type").alias("prop_list_type"),
                col("parsed_properties.action").alias("prop_action"),
                col("parsed_properties.search_keyword").alias("prop_search_keyword"),
                col("parsed_properties.result_count").alias("prop_result_count")
            )
            
            # --- 4.5. 데이터 품질 관리 ---
            df_silver = df_silver.filter(col("event_id").isNotNull()).dropDuplicates(["event_id"])
            print("✅ 컬럼 평탄화 및 데이터 품질 관리 완료.")

            # --- 4.6. 처리 메타데이터 추가 ---
            df_final = df_silver \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("data_source", lit("landing_zone")) \
                .withColumn("pipeline_version", lit("iceberg_v2.0"))

            # --- 4.7. 메모리 최적화를 위한 파티션 수 조정 ---
            df_final = df_final.coalesce(2)  # 2개 파티션으로 줄여서 메모리 사용량 더욱 감소

            clean_count = df_final.count()
            print(f"✅ 데이터 정제 완료! 정제된 행 수: {clean_count:,}")
            
            return df_final
            
        except Exception as e:
            print(f"❌ 데이터 정제 실패: {str(e)}")
            raise
    
    def create_iceberg_table_if_not_exists(self):
        """Iceberg 테이블 생성 (존재하지 않는 경우) - archive/old_versions/bronze_to_silver.py 스키마 적용"""
        try:
            table_name = f"{self.catalog_name}.{self.database_name}.user_events_silver"
            
            print(f"🧊 Iceberg 테이블 생성: {table_name}")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_id STRING,
                event_name STRING,
                user_id STRING,
                anonymous_id STRING,
                session_id STRING,
                kst_timestamp TIMESTAMP,
                utc_timestamp TIMESTAMP,
                date DATE,
                year INT,
                month INT,
                day INT,
                hour INT,
                day_of_week STRING,
                page_name STRING,
                page_url STRING,
                user_segment STRING,
                cooking_style STRING,
                ab_test_group STRING,
                prop_recipe_id BIGINT,
                prop_list_type STRING,
                prop_action STRING,
                prop_search_keyword STRING,
                prop_result_count INT,
                processed_at TIMESTAMP,
                data_source STRING,
                pipeline_version STRING
            )
            USING ICEBERG
            PARTITIONED BY (year, month, day, hour)
            TBLPROPERTIES (
                'write.distribution-mode' = 'hash',
                'write.upsert.enabled' = 'true',
                'format-version' = '2'
            )
            """
            
            self.spark.sql(create_table_sql)
            print(f"✅ Iceberg 테이블 준비 완료!")
            
        except Exception as e:
            print(f"❌ Iceberg 테이블 생성 실패: {str(e)}")
            raise
    
    def write_to_iceberg_table(self, df: DataFrame):
        """Iceberg 테이블에 데이터 쓰기"""
        try:
            table_name = f"{self.catalog_name}.{self.database_name}.user_events_silver"
            
            print(f"🧊 Iceberg 테이블에 데이터 쓰기: {table_name}")
            
            # Iceberg 테이블에 append 모드로 쓰기
            df.writeTo(table_name) \
                .option("write-audit-publish", "true") \
                .append()
            
            print(f"✅ Iceberg 테이블 쓰기 완료!")
            
            # 테이블 정보 확인
            self.show_table_info(table_name)
            
        except Exception as e:
            print(f"❌ Iceberg 테이블 쓰기 실패: {str(e)}")
            raise
    
    def show_table_info(self, table_name: str):
        """Iceberg 테이블 정보 표시"""
        try:
            print(f"\n📊 Iceberg 테이블 정보: {table_name}")
            
            # 테이블 행 수 확인
            count_df = self.spark.sql(f"SELECT COUNT(*) as total_rows FROM {table_name}")
            total_rows = count_df.collect()[0]['total_rows']
            print(f"📈 총 행 수: {total_rows:,}")
            
            # 스냅샷 정보 확인
            snapshots_df = self.spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 5")
            print(f"\n📸 최근 스냅샷 (최대 5개):")
            snapshots_df.show(truncate=False)
            
            # 파티션 정보 확인
            partitions_df = self.spark.sql(f"SELECT year, month, day, hour, COUNT(*) as row_count FROM {table_name} GROUP BY year, month, day, hour ORDER BY year DESC, month DESC, day DESC, hour DESC")
            print(f"\n📅 파티션별 데이터 분포:")
            partitions_df.show()
            
        except Exception as e:
            print(f"⚠️  테이블 정보 조회 중 오류: {str(e)}")
    
    def run_pipeline(self):
        """전체 ETL 파이프라인 실행"""
        try:
            print("🚀 Iceberg ETL 파이프라인 시작!")
            print("=" * 60)
            
            # 1. SparkSession 생성
            self.create_spark_session()
            
            # 2. 데이터베이스 생성
            self.create_database_if_not_exists()
            
            # 3. 랜딩 존에서 데이터 읽기
            raw_df = self.read_from_landing_zone()
            
            # 4. 데이터 정제 및 변환
            clean_df = self.clean_and_transform_data(raw_df)
            
            # 5. Iceberg 테이블 생성
            self.create_iceberg_table_if_not_exists()
            
            # 6. Iceberg 테이블에 데이터 쓰기
            self.write_to_iceberg_table(clean_df)
            
            print("\n" + "=" * 60)
            print("🎉 Iceberg ETL 파이프라인 완료!")
            
        except Exception as e:
            print(f"\n❌ 파이프라인 실행 실패: {str(e)}")
            raise
        finally:
            if self.spark:
                print("🔚 SparkSession 종료")
                self.spark.stop()

def main():
    """메인 함수"""
    pipeline = IcebergETLPipeline()
    pipeline.run_pipeline()

if __name__ == "__main__":
    main()
