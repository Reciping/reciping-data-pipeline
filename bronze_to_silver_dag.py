from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_JOBS_DIR = "/home/ec2-user/spark_jobs"

with DAG(
    dag_id="bronze_to_silver_iceberg_chunked_test",
    start_date=pendulum.datetime(2025, 8, 23, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
    tags=["iceberg", "chunked", "bronze", "silver", "test"],
) as dag:

    bronze_to_silver_task = SparkSubmitOperator(
        task_id="run_bronze_to_silver_chunked_test",
        application=f"{SPARK_JOBS_DIR}/bronze_to_silver_iceberg.py",
        # conn_id="spark_default",
        conn_id="spark_local",  # 새 connection 사용
        verbose=True,

        # [수정] deploy_mode를 conf 밖의 독립적인 파라미터로 지정합니다.
        deploy_mode="client",

        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.iceberg:iceberg-aws-bundle:1.4.2,"  # <--- 이 부분을 추가하세요.
                 "org.apache.hadoop:hadoop-aws:3.3.4,"
                 "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                 "org.postgresql:postgresql:42.5.4",
        # 환경변수로 테스트 모드 전달
        # env_vars={
        #     'TEST_MODE': 'true',
        #     'CHUNK_SIZE': '100000',  # 테스트용 작은 청크
        # },

        # [추가] application_args 파라미터를 사용하여 값을 리스트 형태로 전달합니다.
        application_args=[
            'true',      # 첫 번째 인자: TEST_MODE
            '100000',    # 두 번째 인자: CHUNK_SIZE
        ],

        conf={
            # S3/Iceberg 설정만 유지
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com", 
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        },

        # conf={
        #     # [추가] Spark Driver가 실행될 위치(Airflow Worker)의 IP를 명시합니다.
        #     # 이 설정이 있어야 Executor들이 Driver를 찾아갈 수 있고, UI가 정상적으로 열립니다.
        #     "spark.driver.host": "10.0.128.199",

        #     # [수정] 주석을 현재 환경(t3.medium)에 맞게 변경하고, 메모리를 줄입니다.
        #     # [설명] Driver는 t3.medium(4GB RAM)에서 실행되므로 2g 할당
        #     "spark.driver.memory": "1g",
            
        #     # [설명] Worker(t3.medium, 4GB RAM)의 실제 가용 메모리를 고려하여 2g 할당
        #     "spark.executor.memory": "1g",
            
        #     # [설명] 사용 가능한 Worker 2대를 모두 활용
        #     "spark.executor.instances": "1",
            
        #     # [설명] 각 Worker는 1개의 코어만 사용 (OS 등 여유 자원 확보)
        #     "spark.executor.cores": "1",
            
        #     # [설명] 이 작업이 사용할 총 코어 수는 2개 (2 instances * 1 core)
        #     "spark.cores.max": "1",
            
        #     # [설명] 셔플 파티션 수를 적절하게 설정
        #     "spark.sql.shuffle.partitions": "100",

        #     # S3 파일시스템 및 자격 증명 설정 (필수)
        #     "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        #     "spark.hadoop.fs.s3a.path.style.access": "true",
        #     "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
        #     "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",

        #     # 기타 성능 최적화
        #     "spark.serializer": "org.apache.spark.serializer.KryoSerializer",

        #     # [핵심] Iceberg가 S3와 통신하기 위한 전용 I/O 구현체 설정
        #     "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

        # },
        execution_timeout=timedelta(hours=3),  # 청크 처리 시간 고려하여 증가
    )