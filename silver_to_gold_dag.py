from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_JOBS_DIR = "/home/ec2-user/spark_jobs"

with DAG(
    dag_id="silver_to_gold_iceberg",
    start_date=pendulum.datetime(2025, 8, 23, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
    tags=["iceberg", "replay", "silver", "gold"],
    doc_md="Silver to Gold: Silver 테이블의 데이터를 집계하여 Gold 테이블(Fact Table)을 생성합니다.",
) as dag:

    wait_for_silver_completion = ExternalTaskSensor(
        task_id="wait_for_silver_completion",
        external_dag_id="bronze_to_silver_iceberg",
        external_task_id="run_bronze_to_silver",
        allowed_states=["success"],
        poke_interval=60,
        timeout=7200,
        mode="poke",
    )

    silver_to_gold_task = SparkSubmitOperator(
        task_id="run_silver_to_gold",
        application=f"{SPARK_JOBS_DIR}/silver_to_gold_processor.py",
        conn_id="spark_default",
        verbose=True,
        # [수정] 이 작업 역시 Iceberg/S3/PostgreSQL 라이브러리가 필요합니다.
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2," \
                 "org.apache.hadoop:hadoop-aws:3.3.4," \
                 "com.amazonaws:aws-java-sdk-bundle:1.12.262," \
                 "org.postgresql:postgresql:42.5.4",
        conf={
            "spark.driver.memory": "4g",
            "spark.executor.memory": "2g",
            "spark.sql.shuffle.partitions": "100",
            "spark.executor.instances": "2",  # 작업자 2명은 그대로 유지
            "spark.executor.cores": "1",      # <--- 각 작업자에게 1코어만 요구 (안정적)
            "spark.cores.max": "2"            # <--- 이 작업이 사용할 전체 코어 수도 2 (2 instances * 1 core)로 수정
        },
        execution_timeout=timedelta(hours=2),
    )

    wait_for_silver_completion >> silver_to_gold_task