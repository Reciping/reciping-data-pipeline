# replay_september_15min_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_SCRIPTS_DIR = "/home/ec2-user/spark_jobs"

spark_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

spark_conf = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.driver.memoryOverhead": "512m",
    "spark.executor.memoryOverhead": "512m",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "512m",
    "spark.executor.cores": "1",
    "spark.default.parallelism": "2",
    "spark.sql.shuffle.partitions": "2",
    "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200",
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.iceberg.vectorization.enabled": "false",
}

with DAG(
    dag_id='replay_september_15min_realtime',
    
    start_date=pendulum.datetime(2025, 9, 1, 0, 0, tz="Asia/Seoul"),
    # end_date=pendulum.datetime(2025, 10, 1, 0, 0, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2025, 9, 1, 1, 0, tz="Asia/Seoul"),
    
    # ✅ 15분 간격
    schedule_interval='*/15 * * * *',
    
    catchup=True,
    max_active_runs=1,
    
    tags=['replay', 'september', '15min', 'realtime'],
    
    doc_md="""
    # 9월 15분 간격 준실시간 리플레이 DAG
    
    ## 실행 계획
    - 총 DagRun 수: 2,880개 (30일 × 96회/일)
    - 간격: 15분
    - S3 경로: year=YYYY/month=MM/day=DD/hour=HH/minute=MM/
    """
) as dag:
    
    staging_to_bronze = SparkSubmitOperator(
        task_id='staging_to_bronze',
        application=f"{SPARK_SCRIPTS_DIR}/replay_staging_to_bronze.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_packages),
        conf=spark_conf,
        application_args=[
            '--data-interval-start', '{{ data_interval_start.to_iso8601_string() }}',
            '--data-interval-end', '{{ data_interval_end.to_iso8601_string() }}',
            '--test-mode', 'false'
        ]
    )
    
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=f"{SPARK_SCRIPTS_DIR}/replay_bronze_to_silver.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_packages),
        conf=spark_conf,
        application_args=[
            '--data-interval-start', '{{ data_interval_start.to_iso8601_string() }}',
            '--data-interval-end', '{{ data_interval_end.to_iso8601_string() }}',
            '--test-mode', 'false'
        ]
    )
    
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=f"{SPARK_SCRIPTS_DIR}/replay_silver_to_gold.py",
        conn_id='spark_local',
        verbose=True,
        deploy_mode="client",
        packages=",".join(spark_packages),
        conf=spark_conf,
        application_args=[
            '--data-interval-start', '{{ data_interval_start.to_iso8601_string() }}',
            '--data-interval-end', '{{ data_interval_end.to_iso8601_string() }}',
            '--test-mode', 'false'
        ]
    )
    
    staging_to_bronze >> bronze_to_silver >> silver_to_gold