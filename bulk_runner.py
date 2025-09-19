# bulk_runner.py (최종 수정본)
import subprocess

# --- 설정 ---
SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs"
# SPARK_SUBMIT_CMD = "/home/ec2-user/.local/bin/spark-submit"
SPARK_SUBMIT_CMD = "spark-submit"

# --- [수정] 처리할 대상 파일 이름 변경 ---
# BULK_INPUT_FILE = "dask_events_3m.jsonl"
BULK_INPUT_FILE = "dask_events_1m.jsonl"
# 이 벌크 데이터의 논리적 날짜 (Bronze/Silver 파티션에 사용될 날짜)
# 6~8월 데이터이므로, 8월의 마지막 날로 지정
TARGET_DATE = "2025-08-31" 

# Airflow DAG와 동일한 공통 패키지 및 설정
PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.4"
# CONF = {
#     # # --- [추가] Spark 임시 디렉토리를 메인 디스크 경로로 변경 ---
#     # "spark.local.dir": "/home/ec2-user/spark_tmp",
#     # # --- [추가] Executor 메모리 설정 ---
#     # "spark.executor.memory": "4g",

#     # # --- [추가] 셔플 파티션 개수 설정 ---
#     # "spark.sql.shuffle.partitions": "400",

#     "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#     "spark.hadoop.fs.s3a.path.style.access": "true",
#     "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com", 
#     "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
#     "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
#     "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
# }

CONF = {
    # 메모리 설정 최적화
    "spark.executor.memory": "3g",
    "spark.driver.memory": "3g", 
    "spark.executor.memoryFraction": "0.6",
    "spark.storage.memoryFraction": "0.3",
    "spark.sql.adaptive.coalescePartitions.maxBatchSize": "128MB",
    
    # 파티션 개수 줄이기
    "spark.sql.shuffle.partitions": "100",  # 400에서 100으로 감소
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
    
    # GC 설정 개선 (Java 8용)
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:+UseStringDeduplication",
    "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:+UseStringDeduplication",
    
    # Iceberg 최적화
    "spark.sql.iceberg.vectorization.enabled": "false",  # 메모리 절약
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max": "128m",
    
    # S3A 설정 최적화
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "spark.hadoop.fs.s3a.connection.maximum": "10",  # 연결 수 제한
    "spark.hadoop.fs.s3a.threads.max": "5",          # 스레드 수 제한
    
    "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
}

def run_spark_job(script_name, args):
    """주어진 스크립트와 인자로 spark-submit을 실행하는 함수"""
    print(f"\n{'='*20}\nRunning Spark job: {script_name} with args: {args}\n{'='*20}")
    
    command = [SPARK_SUBMIT_CMD]
    command.extend(["--packages", PACKAGES])
    for key, value in CONF.items():
        command.extend(["--conf", f"{key}={value}"])
    
    command.append(f"{SPARK_SCRIPTS_DIR}/{script_name}")
    command.extend(args)
    
    subprocess.run(command, check=True)

# --- 메인 실행 로직 ---
if __name__ == "__main__":
    try:
        # --- [수정] staging_to_bronze_iceberg.py 호출 시 --target-date 인자 추가 ---
        run_spark_job(
            "staging_to_bronze_iceberg.py",
            ["--input-file-name", BULK_INPUT_FILE, "--target-date", TARGET_DATE, "--test-mode", "false"]
        )
        # --- 수정 끝 ---
        
        # 2. Bronze -> Silver (운영 모드로 실행)
        run_spark_job("bronze_to_silver_iceberg.py", ["--target-date", TARGET_DATE, "--test-mode", "false"])
        
        # 3. Create Dims (운영 모드로 실행)
        run_spark_job("create_dims.py", ["--test-mode", "false"])
        
        # 4. Silver -> Gold (운영 모드로 실행)
        run_spark_job("silver_to_gold_processor.py", ["--test-mode", "false"])
        
        print("\n🎉 Bulk data loading completed successfully!")
        
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")