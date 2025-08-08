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
    to_timestamp, date_format, year, month, dayofmonth,
    expr, from_json, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
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
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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
        """데이터 정제 및 변환"""
        try:
            print("🧹 데이터 정제 및 변환 시작...")
            
            # 1. event_name을 event_type으로 리네임
            df_cleaned = df.withColumnRenamed("event_name", "event_type")
            
            # 2. 타임스탬프 변환 (ISO 8601 형식 처리)
            df_cleaned = df_cleaned.withColumn(
                "event_timestamp",
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX")
            )
            
            # 3. 파티셔닝을 위한 날짜 컬럼 추가
            df_cleaned = df_cleaned \
                .withColumn("event_year", year(col("event_timestamp"))) \
                .withColumn("event_month", month(col("event_timestamp"))) \
                .withColumn("event_day", dayofmonth(col("event_timestamp")))
            
            # 4. 데이터 품질 검증 및 정제 (실제 컬럼명 사용)
            df_cleaned = df_cleaned \
                .filter(col("user_id").isNotNull()) \
                .filter(col("event_type").isNotNull()) \
                .filter(col("event_timestamp").isNotNull())
            
            # 5. JSON 문자열에서 주요 정보 추출 (event_properties에서)
            df_cleaned = df_cleaned \
                .withColumn("recipe_id", expr("get_json_object(event_properties, '$.recipe_id')")) \
                .withColumn("list_type", expr("get_json_object(event_properties, '$.list_type')")) \
                .withColumn("comment_length", expr("get_json_object(event_properties, '$.comment_length')")) \
                .withColumn("rank", expr("get_json_object(event_properties, '$.rank')"))
            
            # 6. context JSON에서 페이지 정보 추출
            df_cleaned = df_cleaned \
                .withColumn("page_name", expr("get_json_object(context, '$.page.name')")) \
                .withColumn("page_url", expr("get_json_object(context, '$.page.url')")) \
                .withColumn("user_segment", expr("get_json_object(context, '$.user_segment')")) \
                .withColumn("cooking_style", expr("get_json_object(context, '$.cooking_style')"))
            
            # 7. 처리 메타데이터 추가
            df_cleaned = df_cleaned \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("data_source", lit("landing_zone")) \
                .withColumn("pipeline_version", lit("iceberg_v1.0"))
            
            # 8. 최종 컬럼 선택 (실제 존재하는 컬럼들로)
            final_columns = [
                "user_id", "session_id", "event_type", "event_timestamp", "event_id",
                "anonymous_id", "date", "recipe_id", "list_type", "comment_length", "rank",
                "page_name", "page_url", "user_segment", "cooking_style",
                "event_properties", "context",
                "event_year", "event_month", "event_day",
                "processed_at", "data_source", "pipeline_version"
            ]
            
            df_final = df_cleaned.select(*final_columns)
            
            clean_count = df_final.count()
            print(f"✅ 데이터 정제 완료! 정제된 행 수: {clean_count:,}")
            
            return df_final
            
        except Exception as e:
            print(f"❌ 데이터 정제 실패: {str(e)}")
            raise
    
    def create_iceberg_table_if_not_exists(self):
        """Iceberg 테이블 생성 (존재하지 않는 경우)"""
        try:
            table_name = f"{self.catalog_name}.{self.database_name}.user_events_silver"
            
            print(f"🧊 Iceberg 테이블 생성: {table_name}")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                user_id STRING,
                session_id STRING,
                event_type STRING,
                event_timestamp TIMESTAMP,
                event_id STRING,
                anonymous_id STRING,
                date STRING,
                recipe_id STRING,
                list_type STRING,
                comment_length STRING,
                rank STRING,
                page_name STRING,
                page_url STRING,
                user_segment STRING,
                cooking_style STRING,
                event_properties STRING,
                context STRING,
                event_year INT,
                event_month INT,
                event_day INT,
                processed_at TIMESTAMP,
                data_source STRING,
                pipeline_version STRING
            )
            USING ICEBERG
            PARTITIONED BY (event_year, event_month, event_day)
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
            partitions_df = self.spark.sql(f"SELECT event_year, event_month, event_day, COUNT(*) as row_count FROM {table_name} GROUP BY event_year, event_month, event_day ORDER BY event_year DESC, event_month DESC, event_day DESC")
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
