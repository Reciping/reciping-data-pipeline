# iceberg_replay_test_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Spark 스크립트들이 위치한 경로
SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs"

# --- 공통 Spark 설정 (기존과 동일) ---
spark_common_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.5.4"
]

spark_common_conf = {
    # 1순위: Driver 메모리 확보 (SIGSEGV 해결 가능성 매우 높음)
    "spark.driver.memory": "2g",

    #  --- [핵심] JIT 컴파일러 버그 우회를 위한 옵션 추가 ---
    "spark.driver.extraJavaOptions": "-XX:-TieredCompilation",
    "spark.executor.extraJavaOptions": "-XX:-TieredCompilation",



    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com", 
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
}

with DAG(
    # --- [핵심] 테스트 목적에 맞게 DAG 설정 변경 ---
    dag_id='iceberg_replay_2_intervals_test',
    
    # 1. start_date: 테스트하고 싶은 첫 번째 구간의 시작 시간
    start_date=pendulum.datetime(2025, 9, 1, 0, 0, tz="Asia/Seoul"),
    
    # 2. end_date: 마지막으로 실행할 구간이 끝나는 시간 (00:10~00:19 구간 실행 후 종료)
    end_date=pendulum.datetime(2025, 9, 1, 0, 20, tz="Asia/Seoul"),
    
    # 3. schedule_interval: 처리할 데이터의 시간 간격
    schedule_interval='*/10 * * * *',
    
    # 4. catchup: start_date부터 end_date까지 밀린 스케줄을 모두 실행
    catchup=True,
    
    max_active_runs=1, # 데이터 순서 보장을 위해 동시 실행은 1개로 제한

    doc_md="[Test] 2025-09-01의 첫 20분(10분짜리 2개 구간) 데이터만 리플레이합니다.",
    tags=['iceberg', 'spark', 'test', 'replay'],
) as dag:
    
    # --- Task 정의 (기존과 동일) ---
    # 각 Task는 Airflow가 자동으로 생성해주는 data_interval 값을 인자로 받습니다.
    
    staging_to_bronze_task = SparkSubmitOperator(
        task_id='run_staging_to_bronze_test',
        application=f"{SPARK_SCRIPTS_DIR}/staging_to_bronze_iceberg.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--data-interval-start', '{{ data_interval_start.to_iso8601_string() }}',
            '--data-interval-end', '{{ data_interval_end.to_iso8601_string() }}',
            '--test-mode', 'false'
        ]
    )

    bronze_to_silver_task = SparkSubmitOperator(
        task_id='run_bronze_to_silver_test',
        application=f"{SPARK_SCRIPTS_DIR}/bronze_to_silver_iceberg.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--data-interval-start', '{{ data_interval_start.to_iso8601_string() }}',
            '--data-interval-end', '{{ data_interval_end.to_iso8601_string() }}',
            '--test-mode', 'false'
        ]
    )

    silver_to_gold_task = SparkSubmitOperator(
        task_id='run_silver_to_gold_test',
        application=f"{SPARK_SCRIPTS_DIR}/silver_to_gold_processor.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--data-interval-start', '{{ data_interval_start.to_iso8601_string() }}',
            '--data-interval-end', '{{ data_interval_end.to_iso8601_string() }}',
            '--test-mode', 'false'
        ]
    )

    # --- Task 종속성 설정 ---
    staging_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task