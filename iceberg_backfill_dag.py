# iceberg_backfill_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs"

with DAG(
    dag_id='iceberg_backfill_8_8_to_8_8',
    start_date=pendulum.datetime(2025, 8, 8, 9, 0, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2025, 8, 8, 23, 45, tz="Asia/Seoul"),
    schedule_interval='*/15 * * * *',
    catchup=True,
    max_active_runs=1, # 동시에 실행될 DAG Run의 최대 개수 (서버 사양에 맞게 조절)
    doc_md="8월 8일부터 8일까지의 데이터를 자동으로 백필(Replay)합니다."
) as dag:
    # 이 DAG는 항상 Airflow의 스케줄 시간(ts)을 사용합니다.
    target_ts = "{{ ts }}"
    
    # 공통 Spark 설정
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

    # --- Task 정의 ---
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

    # --- Task 종속성 설정 ---
    staging_to_bronze_task >> bronze_to_silver_task >> create_dims_task >> silver_to_gold_fact_task