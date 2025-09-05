# iceberg_pipeline_dag.py
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Spark 스크립트들이 위치한 경로를 변수로 지정
SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs" # 기존 경로를 따름

with DAG(
    dag_id='iceberg_data_pipeline_v1',
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule_interval='*/15 * * * *',
    catchup=False,
    doc_md="[Staging -> Bronze -> Silver -> Gold] 준실시간 데이터 수집 및 변환을 위한 Iceberg를 활용한 Medallion Architecture 파이프라인",
    tags=['iceberg', 'spark', 'medallion-architecture'],
    params={
        "override_execution_ts": Param(
            default=None, 
            type=["null", "string"],
            title="수동 실행 시간 (KST)",
            description="테스트 시 'YYYY-MM-DD HH:MM' 형식으로 한국 시간을 입력 (예: 2025-08-01 09:15). 비워두면 스케줄 시간을 따릅니다."
        )
    }
) as dag:
    # 처리할 시간을 결정하는 Jinja 템플릿
    target_ts = "{{ params.override_execution_ts or ts }}"
    
    # --- 변경점: PostgreSQL 드라이버를 포함한 공통 Spark 패키지 정의 ---
    spark_common_packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.5.4"  # <-- 핵심: Hive Metastore 백엔드용 드라이버
    ]
    
    # 공통 Spark 설정
    spark_common_conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com", 
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }

    # --- Task 1: Staging to Bronze ---
    staging_to_bronze_task = SparkSubmitOperator(
        task_id='run_staging_to_bronze_iceberg',
        application=f"{SPARK_SCRIPTS_DIR}/staging_to_bronze_iceberg.py",
        conn_id='spark_local',  # 기존 conn_id를 따름
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--execution-ts', target_ts,
            '--test-mode', 'true'
        ]
    )

    # --- Task 2: Bronze to Silver ---
    bronze_to_silver_task = SparkSubmitOperator(
        task_id='run_bronze_to_silver_iceberg',
        application=f"{SPARK_SCRIPTS_DIR}/bronze_to_silver_iceberg.py",
        conn_id='spark_local', # 기존 conn_id를 따름
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--execution-ts', target_ts,
            '--test-mode', 'true'
            # chunk-size는 argparse에서 default값을 갖도록 수정했으므로 생략 가능
        ]
    )

    # --- [신규 추가] Task 3: Create Dimension Tables ---
    # 여러 Dimension 테이블을 병렬로 실행할 수 있습니다.
    create_dims_task = SparkSubmitOperator(
        task_id='run_create_all_dims',
        application=f"{SPARK_SCRIPTS_DIR}/create_dims.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=['--test-mode', 'true']
    )
    

    # --- [신규 추가] Task 4: Silver to Gold Fact Table ---
    silver_to_gold_fact_task = SparkSubmitOperator(
        task_id='run_silver_to_gold_fact',
        application=f"{SPARK_SCRIPTS_DIR}/silver_to_gold_processor.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=[
            '--execution-ts', target_ts,
            '--test-mode', 'true'
        ]
    )


    # --- Task 종속성 설정 ---
    # staging_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_fact_task
    
    # --- [수정] Task 종속성 설정 ---
    staging_to_bronze_task >> bronze_to_silver_task >> create_dims_task >> silver_to_gold_fact_task
