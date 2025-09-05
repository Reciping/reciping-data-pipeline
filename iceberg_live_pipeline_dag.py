# iceberg_live_pipeline_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs"

with DAG(
    dag_id='iceberg_live_pipeline_from_8_22',
    start_date=pendulum.datetime(2025, 8, 22, 0, 0, tz="Asia/Seoul"),
    schedule_interval='*/15 * * * *',
    catchup=False, # <-- 핵심: 놓친 과거 스케줄(8/22)을 자동으로 실행하지 않음
    max_active_runs=3, # 실시간 파이프라인은 동시 실행 개수를 적게 유지
    doc_md="8월 22일 데이터의 수동 처리 및 그 이후 모든 실시간 데이터를 처리합니다.",
    tags=['iceberg', 'spark', 'production'],
    params={
        "override_execution_ts": Param(
            default=None, type=["null", "string"],
            title="수동 실행 시간 (KST)",
            description="8/22 또는 특정 과거 시간을 테스트할 때 'YYYY-MM-DD HH:MM' 형식으로 입력."
        )
    }
) as dag:
    # 수동 실행과 자동 실행을 모두 지원하는 Jinja 템플릿
    target_ts = "{{ params.override_execution_ts or ts }}"
    
    # 공통 Spark 설정 (위 DAG와 동일)
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
        "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com", 
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }
    
    # Task 정의 (위 DAG와 동일)
    staging_to_bronze_task = SparkSubmitOperator(
        task_id='run_staging_to_bronze',
        application=f"{SPARK_SCRIPTS_DIR}/staging_to_bronze_iceberg.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=['--execution-ts', target_ts, '--test-mode', 'false']
    )

    bronze_to_silver_task = SparkSubmitOperator(
        task_id='run_bronze_to_silver',
        application=f"{SPARK_SCRIPTS_DIR}/bronze_to_silver_iceberg.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=['--execution-ts', target_ts, '--test-mode', 'false']
    )
    
    create_dims_task = SparkSubmitOperator(
        task_id='run_create_all_dims',
        application=f"{SPARK_SCRIPTS_DIR}/create_dims.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=['--test-mode', 'false']
    )

    silver_to_gold_fact_task = SparkSubmitOperator(
        task_id='run_silver_to_gold_fact',
        application=f"{SPARK_SCRIPTS_DIR}/silver_to_gold_processor.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_common_packages),
        conf=spark_common_conf,
        application_args=['--execution-ts', target_ts, '--test-mode', 'false']
    )

    # Task 종속성 설정
    staging_to_bronze_task >> bronze_to_silver_task >> create_dims_task >> silver_to_gold_fact_task