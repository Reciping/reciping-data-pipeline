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
Date: 2025-08-31

임시 스테이징을 활용한 청크 처리 Iceberg ETL Pipeline
메모리 효율성과 파일 최적화를 동시에 달성
"""

import os
import logging
from datetime import datetime
from typing import Optional, List
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, 
    to_timestamp, date_format, year, month, dayofmonth, hour,
    expr, from_json, lit, current_timestamp, monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, DateType, LongType, ArrayType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedChunkedETL:
    """임시 스테이징 활용 청크 처리 ETL"""
    
    def __init__(self, chunk_size: int = 100000, test_mode: bool = True):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083"
        
        # 청크 처리 설정
        self.chunk_size = chunk_size
        self.processed_chunks = 0
        self.failed_chunks = []
        
        # 테스트/운영 환경 분리
        if test_mode:
            print("=== 테스트 모드로 실행 ===")
            self.database_name = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.s3_landing_zone = "s3a://reciping-user-event-logs/bronze/landing-zone/events/test-sample/"
            self.s3_temp_path = "s3a://reciping-user-event-logs/temp/test_chunked_processing/"
            self.table_suffix = "_test"
        else:
            print("=== 운영 모드로 실행 ===")
            self.database_name = "recipe_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.s3_landing_zone = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
            self.s3_temp_path = "s3a://reciping-user-event-logs/temp/chunked_processing/"
            self.table_suffix = ""
        
        print(f"데이터베이스: {self.database_name}")
        print(f"웨어하우스 경로: {self.s3_warehouse_path}")
        print(f"임시 처리 경로: {self.s3_temp_path}")
        
    def create_spark_session(self) -> SparkSession:
        """SparkSession 생성"""
        print("SparkSession 생성 중...")
        
        try:
            spark = SparkSession.builder \
                .appName("OptimizedChunkedETL") \
                .config("spark.sql.session.timeZone", "Asia/Seoul") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
                .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
                .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            
            print("SparkSession 생성 완료")
            self.spark = spark
            return spark
            
        except Exception as e:
            print(f"SparkSession 생성 실패: {str(e)}")
            raise
    
    def create_database_if_not_exists(self):
        """데이터베이스 생성"""
        try:
            print(f"데이터베이스 생성: {self.catalog_name}.{self.database_name}")
            
            self.spark.sql(f"""
                CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}
                COMMENT 'Recipe Analytics Database - Optimized Chunked Processing'
                LOCATION '{self.s3_warehouse_path}{self.database_name}.db/'
            """)
            
            print("데이터베이스 준비 완료")
            
        except Exception as e:
            print(f"데이터베이스 생성 실패: {str(e)}")
            raise
    
    def define_event_schema(self) -> StructType:
        """JSONL 스키마 정의"""
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
    
    def cleanup_temp_directory(self):
        """S3 임시 디렉토리 정리"""
        try:
            print(f"임시 디렉토리 정리: {self.s3_temp_path}")
            
            try:
                test_df = self.spark.read.option("multiline", "false").text(self.s3_temp_path)
                print("기존 임시 파일이 존재할 수 있습니다 (덮어쓰기됩니다)")
            except:
                print("임시 디렉토리가 비어있음")
                
        except Exception as e:
            print(f"임시 디렉토리 정리 중 오류 (계속 진행): {str(e)}")
    
    def optimized_chunked_processing(self):
        """S3 파일 단위로 ETL을 수행하여 메모리 병목을 근본적으로 해결"""
        import json
        
        try:
            print("=== S3 파일 단위 병렬 처리 시작 ===")
            pipeline_start = time.time()
            
            # 1. S3 랜딩 존의 파일 목록을 가져옵니다.
            # Hadoop Path 객체를 사용하기 위해 sparkContext를 활용합니다.
            URI = self.spark.sparkContext._gateway.jvm.java.net.URI
            Path = self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
            fs = Path(self.s3_landing_zone).getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
            
            # listStatus를 사용하여 파일(및 디렉터리) 목록을 가져옵니다.
            file_statuses = fs.listStatus(Path(self.s3_landing_zone))
            
            # 실제 파일 경로만 필터링합니다. (디렉터리 제외)
            file_paths = [str(status.getPath()) for status in file_statuses if status.isFile()]
            
            if not file_paths:
                print("처리할 파일이 없습니다. 파이프라인을 종료합니다.")
                return
            
            print(f"총 {len(file_paths)}개의 파일을 처리합니다.")

            # 2. 각 파일을 순회하며 독립적으로 처리합니다.
            for i, file_path in enumerate(file_paths):
                print(f"\n--- 파일 {i+1}/{len(file_paths)} 처리 시작: {file_path.split('/')[-1]} ---")
                
                # 2.1. 파일 하나만 텍스트로 읽기
                lines_df = self.spark.read.text(file_path)
                
                # 2.2. RDD로 변환하여 병렬 JSON 파싱
                def safe_json_loads(line_str):
                    try:
                        return json.loads(line_str.value)
                    except (json.JSONDecodeError, AttributeError):
                        return None

                rdd = lines_df.rdd.map(safe_json_loads).filter(lambda x: x is not None)
                
                # 2.3. DataFrame으로 변환
                schema = self.define_event_schema()
                df = self.spark.createDataFrame(rdd, schema)
                
                # 2.4. 데이터 정제 및 변환 (중복 제거 포함)
                print("데이터 정제 및 변환 중...")
                transformed_df = self.transform_chunk_data(df, i + 1)
                transformed_df = transformed_df.dropDuplicates(["event_id"]) # 파일 단위 중복 제거
                
                # 2.5. Iceberg 테이블에 바로 저장
                print("Iceberg 테이블에 저장 중...")
                self.write_to_iceberg_table(transformed_df)
                
                print(f"--- 파일 {i+1} 처리 완료 ---")

            pipeline_elapsed = time.time() - pipeline_start
            print(f"\n🎉 모든 파일 처리 완료! (총 소요시간: {pipeline_elapsed:.1f}초)")

        except Exception as e:
            print(f"파일 단위 처리 파이프라인 실패: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise
    
    def process_chunk_and_save_temp(self, chunk_df: DataFrame, chunk_id: int) -> bool:
        """DataFrame 청크 처리 및 임시 저장"""
        try:
            print(f"청크 {chunk_id} 처리 중...")
            start_time = time.time()
            
            # 데이터 변환
            transformed_df = self.transform_chunk_data(chunk_df, chunk_id)
            
            # 임시 경로에 Parquet으로 저장
            temp_chunk_path = f"{self.s3_temp_path}/chunk_{chunk_id:03d}"
            
            transformed_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .parquet(temp_chunk_path)
            
            elapsed = time.time() - start_time
            print(f"청크 {chunk_id} 임시 저장 완료 (소요시간: {elapsed:.1f}초)")
            
            self.processed_chunks += 1
            return True
            
        except Exception as e:
            print(f"청크 {chunk_id} 처리 실패: {str(e)}")
            self.failed_chunks.append(chunk_id)
            return False
    
    def transform_chunk_data(self, chunk_df: DataFrame, chunk_id: int) -> DataFrame:
        """청크 데이터 변환 (완전한 스키마)"""
        try:
            # 완전한 스키마 정의
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

            # 변환 로직
            df_transformed = chunk_df \
                .withColumn("parsed_context", from_json(col("context"), context_schema)) \
                .withColumn("parsed_properties", from_json(col("event_properties"), event_properties_schema)) \
                .withColumn("kst_timestamp", col("timestamp").cast(TimestampType())) \
                .withColumn("utc_timestamp", expr("kst_timestamp - INTERVAL 9 HOURS")) \
                .withColumn("date", col("date").cast(DateType())) \
                .withColumn("year", year(col("kst_timestamp"))) \
                .withColumn("month", month(col("kst_timestamp"))) \
                .withColumn("day", dayofmonth(col("kst_timestamp"))) \
                .withColumn("hour", hour(col("kst_timestamp"))) \
                .withColumn("day_of_week", date_format(col("kst_timestamp"), "E")) \
                .drop("context", "event_properties", "timestamp")

            # 최종 컬럼 선택
            df_final = df_transformed.select(
                "event_id", "event_name", "user_id", "anonymous_id", "session_id",
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
                col("parsed_properties.result_count").alias("prop_result_count")
            ) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("data_source", lit("landing_zone")) \
            .withColumn("pipeline_version", lit("chunked_v1.0")) \
            .withColumn("chunk_id", lit(chunk_id))

            return df_final
            
        except Exception as e:
            print(f"청크 데이터 변환 실패: {str(e)}")
            raise
    
    def write_to_iceberg_table(self, df: DataFrame):
        """Iceberg 테이블에 직접 저장 - count() 없이"""
        try:
            table_name = f"{self.catalog_name}.{self.database_name}.user_events_silver{self.table_suffix}"
            
            print("Iceberg 테이블에 저장 중...")
            df.writeTo(table_name).append()
            print("저장 완료")
            
        except Exception as e:
            print(f"Iceberg 저장 실패: {str(e)}")
            raise
    
    def create_iceberg_table_if_not_exists(self):
        """Iceberg 테이블 생성"""
        try:
            table_name = f"{self.catalog_name}.{self.database_name}.user_events_silver{self.table_suffix}"
            
            print(f"Iceberg 테이블 생성: {table_name}")
            
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
                pipeline_version STRING,
                chunk_id INT
            )
            USING ICEBERG
            PARTITIONED BY (year, month, day)
            TBLPROPERTIES (
                'write.distribution-mode' = 'hash',
                'write.upsert.enabled' = 'true',
                'format-version' = '2'
            )
            """
            
            self.spark.sql(create_table_sql)
            print("Iceberg 테이블 준비 완료")
            
        except Exception as e:
            print(f"Iceberg 테이블 생성 실패: {str(e)}")
            raise
    
    def print_pipeline_summary(self, total_rows: int, num_chunks: int, elapsed_time: float):
        """파이프라인 실행 결과 요약"""
        print(f"\n{'='*60}")
        print("청크 처리 파이프라인 실행 결과")
        print(f"{'='*60}")
        print(f"총 처리 행 수: {total_rows:,}")
        print(f"총 청크 수: {num_chunks}")
        print(f"성공한 청크: {self.processed_chunks}")
        print(f"실패한 청크: {len(self.failed_chunks)}")
        print(f"전체 소요시간: {elapsed_time:.1f}초")
        if num_chunks > 0:
            print(f"평균 청크 처리시간: {elapsed_time/num_chunks:.1f}초")
        print(f"시간당 처리량: {total_rows / elapsed_time * 3600:.0f} 행/시간")
        
        if self.failed_chunks:
            print(f"실패한 청크 ID: {self.failed_chunks}")
        
        print(f"{'='*60}")
    
    def run_pipeline(self):
        """메인 파이프라인 실행"""
        try:
            print("청크 처리 기반 ETL 파이프라인 시작")
            print("=" * 50)
            
            # 1. SparkSession 생성
            self.create_spark_session()
            
            # 2. 데이터베이스 생성
            self.create_database_if_not_exists()
            
            # 3. Iceberg 테이블 생성
            self.create_iceberg_table_if_not_exists()
            
            # 4. 최적화된 청크 처리 실행
            self.optimized_chunked_processing()
            
            print("=" * 50)
            print("ETL 파이프라인 완료")
            
        except Exception as e:
            print(f"파이프라인 실패: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise
        finally:
            if self.spark:
                print("SparkSession 종료")
                self.spark.stop()

def main():
    # 환경변수로 테스트 모드 제어
    import os
    import sys  # <--- sys 모듈을 import 합니다.

    # [수정 전]
    # test_mode = os.getenv('TEST_MODE', 'true').lower() == 'true'
    # chunk_size = int(os.getenv('CHUNK_SIZE', '100000'))

    # [수정 후]
    # sys.argv[0]은 스크립트 이름 자체이므로, 인자는 1번부터 시작합니다.
    test_mode = sys.argv[1].lower() == 'true'
    chunk_size = int(sys.argv[2])
    
    print(f"실행 모드: {'테스트' if test_mode else '운영'}")
    print(f"청크 크기: {chunk_size:,}")
    
    pipeline = OptimizedChunkedETL(chunk_size=chunk_size, test_mode=test_mode)
    pipeline.run_pipeline()


if __name__ == "__main__":
    main()