# streaming_to_iceberg.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def main():
    """
    실시간 스트리밍 데이터를 Iceberg 테이블로 처리하는 
    고급 스트리밍 파이프라인 (Structured Streaming + Iceberg).
    """
    try:
        # 시스템 환경 변수 설정
        os.environ['HADOOP_USER_NAME'] = 'root'
        os.environ['USER'] = 'root'
        os.environ['HOME'] = '/tmp'
        os.environ['JAVA_OPTS'] = '-Duser.name=root'
        os.environ['IVY_HOME'] = '/tmp/.ivy2'
        os.makedirs('/tmp/.ivy2', exist_ok=True)

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (스트리밍 + Iceberg 설정)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession for Streaming + Iceberg 생성...")
        
        spark = SparkSession.builder \
            .appName("Streaming_to_Iceberg_Pipeline") \
            .master("local[*]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-streaming-checkpoints") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.spark_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://reciping-user-event-logs/warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
            .config("spark.hadoop.fs.s3a.region", "ap-northeast-2") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession for Streaming 생성 완료!")

        # 체크포인트 디렉토리 생성
        os.makedirs("/tmp/spark-streaming-checkpoints", exist_ok=True)

        # -----------------------------------------------------------------------------
        # 2. 🏗️ 실시간 테이블 스키마 준비
        # -----------------------------------------------------------------------------
        print("\n🏗️ 실시간 스트리밍 테이블 스키마 준비...")
        
        # 실시간 이벤트 스키마 정의
        streaming_event_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_name", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("anonymous_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("date", StringType(), False),
            StructField("context", StringType(), True),
            StructField("event_properties", StringType(), True)
        ])
        
        # 실시간 집계 테이블 생성 (존재하지 않는 경우)
        try:
            # 실시간 이벤트 스트림 테이블
            spark.sql("""
                CREATE TABLE IF NOT EXISTS bronze_db.streaming_events (
                    event_id STRING NOT NULL,
                    event_name STRING NOT NULL,
                    user_id STRING,
                    anonymous_id STRING,
                    session_id STRING,
                    event_timestamp TIMESTAMP NOT NULL,
                    event_date DATE NOT NULL,
                    context STRING,
                    event_properties STRING,
                    ingestion_timestamp TIMESTAMP NOT NULL,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL,
                    hour INT NOT NULL
                ) USING ICEBERG
                PARTITIONED BY (year, month, day, hour)
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.target-file-size-bytes' = '134217728'
                )
            """)
            print("✅ 실시간 이벤트 테이블 'bronze_db.streaming_events' 준비 완료")
            
            # 실시간 집계 테이블
            spark.sql("""
                CREATE TABLE IF NOT EXISTS gold_db.realtime_metrics (
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    event_name STRING NOT NULL,
                    event_count BIGINT NOT NULL,
                    unique_users BIGINT NOT NULL,
                    unique_sessions BIGINT NOT NULL,
                    processing_timestamp TIMESTAMP NOT NULL,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL,
                    hour INT NOT NULL
                ) USING ICEBERG
                PARTITIONED BY (year, month, day)
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.target-file-size-bytes' = '67108864'
                )
            """)
            print("✅ 실시간 집계 테이블 'gold_db.realtime_metrics' 준비 완료")
            
        except Exception as e:
            print(f"⚠️ 테이블 생성 중 오류 (이미 존재할 수 있음): {e}")

        # -----------------------------------------------------------------------------
        # 3. 📡 스트리밍 데이터 소스 시뮬레이션
        # -----------------------------------------------------------------------------
        print("\n📡 스트리밍 데이터 소스 설정...")
        
        # 파일 기반 스트리밍 소스 (S3 랜딩 존 모니터링)
        streaming_path = "s3a://reciping-user-event-logs/bronze/streaming-zone/events/"
        
        # 스트리밍 DataFrame 생성
        streaming_df = spark.readStream \
            .format("json") \
            .schema(streaming_event_schema) \
            .option("path", streaming_path) \
            .option("maxFilesPerTrigger", 1) \
            .load()
        
        print(f"✅ 스트리밍 소스 설정 완료: {streaming_path}")

        # -----------------------------------------------------------------------------
        # 4. 🔄 스트리밍 데이터 변환
        # -----------------------------------------------------------------------------
        print("\n🔄 스트리밍 데이터 변환 로직 설정...")
        
        # 타임스탬프 변환 및 파티션 컬럼 추가
        streaming_transformed = streaming_df \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("event_date", col("date").cast(DateType())) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", dayofmonth(col("event_timestamp"))) \
            .withColumn("hour", hour(col("event_timestamp"))) \
            .filter(col("event_timestamp").isNotNull()) \
            .drop("timestamp", "date")
        
        print("✅ 스트리밍 변환 로직 설정 완료")

        # -----------------------------------------------------------------------------
        # 5. 💾 실시간 데이터를 Iceberg 테이블로 저장
        # -----------------------------------------------------------------------------
        print("\n💾 실시간 Iceberg 저장 스트림 설정...")
        
        # Iceberg 테이블로 스트리밍 저장
        streaming_to_iceberg = streaming_transformed.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("table", "bronze_db.streaming_events") \
            .option("checkpointLocation", "/tmp/spark-streaming-checkpoints/streaming_events") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print("✅ 실시간 Iceberg 저장 스트림 시작됨")

        # -----------------------------------------------------------------------------
        # 6. 📊 실시간 집계 처리
        # -----------------------------------------------------------------------------
        print("\n📊 실시간 집계 스트림 설정...")
        
        # 5분 윈도우 집계
        windowed_aggregation = streaming_transformed \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                "event_name"
            ).agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "event_name",
                "event_count",
                "unique_users", 
                "unique_sessions",
                current_timestamp().alias("processing_timestamp"),
                year(col("window.start")).alias("year"),
                month(col("window.start")).alias("month"),
                dayofmonth(col("window.start")).alias("day"),
                hour(col("window.start")).alias("hour")
            )
        
        # 실시간 집계를 Iceberg 테이블로 저장
        aggregation_to_iceberg = windowed_aggregation.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("table", "gold_db.realtime_metrics") \
            .option("checkpointLocation", "/tmp/spark-streaming-checkpoints/realtime_metrics") \
            .trigger(processingTime="60 seconds") \
            .start()
        
        print("✅ 실시간 집계 스트림 시작됨")

        # -----------------------------------------------------------------------------
        # 7. 🖥️ 스트리밍 모니터링 및 콘솔 출력
        # -----------------------------------------------------------------------------
        print("\n🖥️ 스트리밍 모니터링 설정...")
        
        # 콘솔로 실시간 데이터 확인 (디버깅용)
        console_output = streaming_transformed.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .option("numRows", 5) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print("✅ 콘솔 모니터링 스트림 시작됨")

        # -----------------------------------------------------------------------------
        # 8. 📈 스트리밍 상태 모니터링
        # -----------------------------------------------------------------------------
        print("\n📈 스트리밍 파이프라인 실행 중...")
        print("💡 실시간 데이터 처리가 시작되었습니다.")
        print("📡 모니터링 중인 경로:", streaming_path)
        print("🔄 처리 주기: 30초 (데이터 저장), 60초 (집계)")
        print("⏹️  중지하려면 Ctrl+C를 누르세요.")
        
        try:
            # 스트림 상태 체크 루프
            import time
            runtime_minutes = 0
            max_runtime_minutes = 10  # 최대 10분 실행
            
            while runtime_minutes < max_runtime_minutes:
                time.sleep(60)  # 1분 대기
                runtime_minutes += 1
                
                print(f"\n⏰ 실행 시간: {runtime_minutes}분")
                
                # 스트림 상태 확인
                if streaming_to_iceberg.isActive:
                    progress = streaming_to_iceberg.lastProgress
                    if progress:
                        print(f"📊 Iceberg 저장 스트림 - 처리된 행: {progress.get('inputRowsPerSecond', 0)}/초")
                
                if aggregation_to_iceberg.isActive:
                    agg_progress = aggregation_to_iceberg.lastProgress
                    if agg_progress:
                        print(f"🔢 집계 스트림 - 처리된 행: {agg_progress.get('inputRowsPerSecond', 0)}/초")
                
                # 실제 데이터가 들어왔는지 확인
                try:
                    current_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze_db.streaming_events").collect()[0]['cnt']
                    print(f"💾 현재 저장된 스트리밍 이벤트 수: {current_count:,}")
                    
                    metrics_count = spark.sql("SELECT COUNT(*) as cnt FROM gold_db.realtime_metrics").collect()[0]['cnt']
                    print(f"📊 현재 저장된 집계 메트릭 수: {metrics_count:,}")
                except:
                    print("📊 테이블 조회 중 오류 (테이블이 아직 생성되지 않았을 수 있음)")
            
            print(f"\n✅ {max_runtime_minutes}분 실행 완료. 스트림을 종료합니다.")
            
        except KeyboardInterrupt:
            print("\n⏹️ 사용자에 의해 중단되었습니다.")
        
        # -----------------------------------------------------------------------------
        # 9. 스트림 정리 및 종료
        # -----------------------------------------------------------------------------
        print("\n🛑 스트리밍 작업 종료 중...")
        
        # 모든 스트림 종료
        streaming_to_iceberg.stop()
        aggregation_to_iceberg.stop()
        console_output.stop()
        
        print("✅ 모든 스트림이 종료되었습니다.")
        
        # 최종 결과 확인
        try:
            print("\n📊 최종 처리 결과:")
            
            final_events = spark.sql("SELECT COUNT(*) as total_events FROM bronze_db.streaming_events").collect()[0]['total_events']
            print(f"💾 총 처리된 스트리밍 이벤트: {final_events:,}")
            
            final_metrics = spark.sql("SELECT COUNT(*) as total_metrics FROM gold_db.realtime_metrics").collect()[0]['total_metrics']
            print(f"📊 총 생성된 집계 메트릭: {final_metrics:,}")
            
            if final_metrics > 0:
                print("\n📈 최근 집계 결과 (상위 5개):")
                spark.sql("""
                    SELECT window_start, window_end, event_name, event_count, unique_users
                    FROM gold_db.realtime_metrics 
                    ORDER BY window_start DESC 
                    LIMIT 5
                """).show()
            
        except Exception as e:
            print(f"⚠️ 최종 결과 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 10. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("✅ 실시간 스트리밍 파이프라인이 완료되었습니다!")

    except Exception as e:
        print(f"❌ 스트리밍 파이프라인 실패: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    main()
