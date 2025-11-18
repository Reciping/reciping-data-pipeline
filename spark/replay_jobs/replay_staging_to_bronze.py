"""
replay_staging_to_bronze.py (boto3 없는 버전)
==============================================
Spark의 파일 시스템 API를 사용하여 15분 단위 파일 필터링
"""
import logging
import argparse
from datetime import datetime, timedelta
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, to_date, lit, col

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReplayStagingToBronze:
    def __init__(self, test_mode: bool = False):
        self.spark = None
        self.catalog_name = "iceberg_catalog"
        self.hive_metastore_uri = "thrift://10.0.11.86:9083"
        
        self.s3_bucket = "reciping-user-event-logs"
        self.s3_base_prefix = "bronze/replay-user-behavior-event-logs/replay-user-events/"
        
        if test_mode:
            self.database_name = "recipe_analytics_test"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/test_warehouse/"
            self.table_suffix = "_test"
        else:
            self.database_name = "recipe_analytics"
            self.s3_warehouse_path = "s3a://reciping-user-event-logs/iceberg/warehouse/"
            self.table_suffix = ""
        
        self.bronze_table = f"bronze_events_iceberg{self.table_suffix}"
        
    def create_spark_session(self):
        logger.info("SparkSession 생성 중...")
        self.spark = SparkSession.builder \
            .appName("ReplayStagingToBronze") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", self.hive_metastore_uri) \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.s3_warehouse_path) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.sql(f"USE {self.catalog_name}")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        self.spark.sql(f"USE {self.database_name}")
        logger.info("SparkSession 준비 완료")
    
    def create_bronze_table_if_not_exists(self):
        logger.info(f"Bronze 테이블 확인: {self.bronze_table}")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.bronze_table} (
            raw_event_string STRING,
            source_file STRING,
            ingestion_timestamp TIMESTAMP,
            ingestion_date DATE
        ) USING ICEBERG 
        PARTITIONED BY (ingestion_date)
        """
        self.spark.sql(create_sql)
        logger.info("Bronze 테이블 준비 완료")
    
    def get_files_with_spark(self, s3_path: str, start_dt: datetime, end_dt: datetime) -> list:
        """
        Spark의 Hadoop FileSystem API를 사용하여 파일 목록 조회 및 시간 필터링
        """
        from pyspark import SparkFiles
        from py4j.java_gateway import java_import
        
        logger.info(f"Spark FileSystem으로 파일 목록 조회: {s3_path}")
        logger.info(f"필터링 시간 범위: {start_dt} ~ {end_dt}")
        
        # Hadoop FileSystem 접근
        sc = self.spark.sparkContext
        java_import(sc._jvm, "org.apache.hadoop.fs.Path")
        java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
        
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.FileSystem.get(sc._jvm.java.net.URI(s3_path), hadoop_conf)
        path = sc._jvm.Path(s3_path)
        
        filtered_files = []
        
        try:
            file_statuses = fs.listStatus(path)
            
            for file_status in file_statuses:
                file_path = file_status.getPath().toString()
                
                # 디렉토리는 스킵
                if file_status.isDirectory():
                    continue
                
                # 파일 수정 시간 (밀리초 단위)
                modification_time_ms = file_status.getModificationTime()
                modification_time = datetime.fromtimestamp(
                    modification_time_ms / 1000.0, 
                    tz=pytz.UTC
                ).astimezone(pytz.timezone('Asia/Seoul'))
                
                # 시간 범위 필터링
                if start_dt <= modification_time < end_dt:
                    filtered_files.append(file_path)
                    logger.info(f"  선택: {file_path.split('/')[-1]} (수정시간: {modification_time})")
            
            logger.info(f"필터링 결과: 총 {len(filtered_files)}개 파일")
            
        except Exception as e:
            logger.warning(f"파일 목록 조회 실패 (디렉토리가 비어있을 수 있음): {e}")
        
        return filtered_files
    
    # def load_data_by_time_range(self, data_interval_start: str, data_interval_end: str):
    #     """시간 필터링 없이 해당 hour의 모든 파일 읽기"""
    #     kst_tz = pytz.timezone('Asia/Seoul')
    #     start_dt = datetime.fromisoformat(data_interval_start.replace('Z', '+00:00')).astimezone(kst_tz)
        
    #     year = start_dt.year
    #     month = f"{start_dt.month:02d}"
    #     day = f"{start_dt.day:02d}"
    #     hour = f"{start_dt.hour:02d}"
        
    #     # 시간 단위 전체 경로
    #     s3_path = f"s3a://{self.s3_bucket}/{self.s3_base_prefix}year={year}/month={month}/day={day}/hour={hour}/*.json"
        
    #     ingestion_date_str = start_dt.strftime('%Y-%m-%d')
        
    #     logger.info(f"처리 대상 경로: {s3_path}")
    #     logger.info(f"시간 범위: {data_interval_start} ~ {data_interval_end}")
        
    #     try:
    #         # ✅ 필터링 없이 전체 읽기
    #         raw_df = self.spark.read.text(s3_path)
            
    #         if raw_df.rdd.isEmpty():
    #             logger.warning(f"데이터가 없습니다: {s3_path}")
    #             return
            
    #         count = raw_df.count()
    #         logger.info(f"읽은 레코드 수: {count:,}건")
            
    #         # Bronze 테이블 형식으로 변환
    #         bronze_df = raw_df.withColumnRenamed("value", "raw_event_string") \
    #             .withColumn("source_file", input_file_name()) \
    #             .withColumn("ingestion_timestamp", current_timestamp()) \
    #             .withColumn("ingestion_date", to_date(lit(ingestion_date_str)))
            
    #         logger.info(f"Bronze 테이블에 데이터 저장 중... (파티션: {ingestion_date_str})")
    #         bronze_df.writeTo(self.bronze_table).append()
            
    #         logger.info("Bronze 적재 완료")
            
    #     except Exception as e:
    #         logger.error(f"데이터 로드 실패", exc_info=True)
    #         raise

    def load_data_by_time_range(self, data_interval_start: str, data_interval_end: str):
        """15분 단위로 minute 경로 포함하여 데이터 로드"""
        kst_tz = pytz.timezone('Asia/Seoul')
        start_dt = datetime.fromisoformat(data_interval_start.replace('Z', '+00:00')).astimezone(kst_tz)
        
        year = start_dt.year
        month = f"{start_dt.month:02d}"
        day = f"{start_dt.day:02d}"
        hour = f"{start_dt.hour:02d}"
        minute = f"{start_dt.minute:02d}"  # ✅ minute 추가
        
        # ✅ minute 경로 포함
        s3_path = f"s3a://{self.s3_bucket}/{self.s3_base_prefix}year={year}/month={month}/day={day}/hour={hour}/minute={minute}/*.json"
        
        ingestion_date_str = start_dt.strftime('%Y-%m-%d')
        
        logger.info(f"처리 대상 경로: {s3_path}")
        logger.info(f"시간 범위: {data_interval_start} ~ {data_interval_end}")
        
        try:
            raw_df = self.spark.read.text(s3_path)
            
            if raw_df.rdd.isEmpty():
                logger.warning(f"데이터가 없습니다: {s3_path}")
                return
            
            count = raw_df.count()
            logger.info(f"읽은 레코드 수: {count:,}건")
            
            bronze_df = raw_df.withColumnRenamed("value", "raw_event_string") \
                .withColumn("source_file", input_file_name()) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("ingestion_date", to_date(lit(ingestion_date_str)))
            
            logger.info(f"Bronze 테이블에 데이터 저장 중... (파티션: {ingestion_date_str})")
            bronze_df.writeTo(self.bronze_table).append()
            
            logger.info("Bronze 적재 완료")
            
        except Exception as e:
            logger.error(f"데이터 로드 실패", exc_info=True)
            raise
    
    def run(self, data_interval_start: str, data_interval_end: str):
        try:
            self.create_spark_session()
            self.create_bronze_table_if_not_exists()
            self.load_data_by_time_range(data_interval_start, data_interval_end)
            
        except Exception as e:
            logger.error("파이프라인 실패", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Replay Staging to Bronze ETL (15min)")
    parser.add_argument("--data-interval-start", required=True)
    parser.add_argument("--data-interval-end", required=True)
    parser.add_argument("--test-mode", type=lambda x: str(x).lower() == 'true', default=False)
    args = parser.parse_args()
    
    pipeline = ReplayStagingToBronze(test_mode=args.test_mode)
    pipeline.run(args.data_interval_start, args.data_interval_end)

if __name__ == "__main__":
    main()