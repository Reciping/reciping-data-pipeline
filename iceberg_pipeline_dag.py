# iceberg_pipeline_dag.py (Replay & Manual Test Version)
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Spark 스크립트들이 위치한 경로를 변수로 지정
SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs"

with DAG(
    # --- [수정 1] DAG의 목적을 명확히 하도록 ID 변경 ---
    dag_id='iceberg_data_replay_pipeline_final',
    
    # --- [수정 2] 데이터 리플레이 기간 설정 ---
    start_date=pendulum.datetime(2025, 8, 1, 9, 0, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2025, 8, 30, 23, 45, tz="Asia/Seoul"),
    schedule_interval='*/15 * * * *',
    catchup=True,  # <-- 핵심: True로 설정하여 start_date부터 end_date까지의 모든 놓친 스케줄을 순차적으로 실행
    # --- 수정 끝 ---
    
    doc_md="[Final] 2025년 8월 데이터 리플레이 및 수동 테스트를 위한 Medallion Architecture 파이프라인",
    tags=['iceberg', 'spark', 'replay', 'production'],
    # params는 더 이상 사용하지 않으므로 제거
) as dag:
    
    # --- [수정 3] 실행 타입을 기반으로 처리할 시간을 동적으로 결정하는 Jinja 템플릿 ---
    # Airflow 내장 변수인 `dag_run.run_type`을 사용하여 자동 실행과 수동 실행을 구분합니다.
    # - 자동 실행 시 (run_type == 'scheduled'): Airflow의 스케줄 시간(`ts`)을 사용합니다.
    # - 수동 실행 시 (run_type == 'manual'): DAG의 end_date에 15분을 더한 미래 시간을 사용합니다.
    target_ts = """
        {{ dag.end_date.add(minutes=15).to_iso8601_string() if dag_run.run_type == 'manual' else ts }}
    """
    # --- 수정 끝 ---

    # 공통 Spark 설정 (이전과 동일)
    spark_common_packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.5.4"
    ]
    spark_common_conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s_3a.endpoint": "s3.ap-northeast-2.amazonaws.com", 
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }

    # --- Task 1: Staging to Bronze ---
    staging_to_bronze_task = SparkSubmitOperator(
        task_id='run_staging_to_bronze_iceberg',
        application=f"{SPARK_SCRIPTS_DIR}/staging_to_bronze_iceberg.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--execution-ts', target_ts,
            '--test-mode', 'false'  # <-- 이제 운영 DB를 사용하도록 'false'로 변경
        ]
    )

    # --- Task 2: Bronze to Silver ---
    bronze_to_silver_task = SparkSubmitOperator(
        task_id='run_bronze_to_silver_iceberg',
        application=f"{SPARK_SCRIPTS_DIR}/bronze_to_silver_iceberg.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--execution-ts', target_ts,
            '--test-mode', 'false' # <-- 운영 DB 사용
        ]
    )

    # --- Task 3: Create Dimension Tables ---
    create_dims_task = SparkSubmitOperator(
        task_id='run_create_all_dims',
        application=f"{SPARK_SCRIPTS_DIR}/create_dims.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--test-mode', 'false' # <-- 운영 DB 사용
        ]
    )

    # --- Task 4: Silver to Gold Table ---
    silver_to_gold_task = SparkSubmitOperator(
        task_id='run_silver_to_gold',
        application=f"{SPARK_SCRIPTS_DIR}/silver_to_gold_processor.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--execution-ts', target_ts,
            '--test-mode', 'false' # <-- 운영 DB 사용
        ]
    )

    # --- Task 종속성 설정 (이전과 동일) ---
    staging_to_bronze_task >> bronze_to_silver_task >> create_dims_task >> silver_to_gold_task