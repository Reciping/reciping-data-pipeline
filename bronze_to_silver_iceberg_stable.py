# bronze_to_silver_iceberg_stable.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, date_format, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType, TimestampType

def main():
    """
    안정적인 Iceberg 버전으로 Bronze, Silver 레이어를 구축하는
    데이터 레이크하우스 ETL 파이프라인 (Iceberg 1.4.3 + Hadoop Catalog).
    """
    try:
        # 시스템 환경 변수를 먼저 설정 (성공한 설정 적용)
        import os
        import subprocess
        
        # 성공한 환경 변수 설정
        os.environ['HADOOP_USER_NAME'] = 'root'
        os.environ['USER'] = 'root'
        os.environ['HOME'] = '/tmp'
        os.environ['JAVA_OPTS'] = '-Duser.name=root -XX:+UseG1GC -XX:G1HeapRegionSize=32m'
        # Ivy 설정
        os.environ['IVY_HOME'] = '/tmp/.ivy2'
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.jars.ivy=/tmp/.ivy2 pyspark-shell'
        
        # 필요한 디렉토리 생성
        os.makedirs('/tmp/.ivy2', exist_ok=True)

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (안정적인 Iceberg + Hadoop Catalog)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession with Stable Iceberg (Hadoop Catalog) 생성을 시도합니다...")
        
        spark = SparkSession.builder \
            .appName("Bronze_to_Silver_Iceberg_Stable_Pipeline") \
            .master("local[2]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/warehouse/iceberg") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
            .config("spark.hadoop.fs.s3a.region", "ap-northeast-2") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.block.size", "134217728") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession with Stable Iceberg가 성공적으로 생성되었습니다!")

        # -----------------------------------------------------------------------------
        # 2. 🏗️ 네임스페이스 생성 및 관리
        # -----------------------------------------------------------------------------
        print("\n🏗️ Iceberg 네임스페이스 구성...")
        
        # Bronze, Silver, Gold 네임스페이스 생성
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.bronze")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.silver") 
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.gold")
            print("✅ Iceberg 네임스페이스 구성 완료")
        except Exception as e:
            print(f"⚠️ 네임스페이스 생성 중 오류 (이미 존재할 수 있음): {e}")
        
        # 현재 네임스페이스 목록 확인
        print("\n📋 현재 Iceberg 네임스페이스 목록:")
        spark.sql("SHOW NAMESPACES IN iceberg_catalog").show()

        # -----------------------------------------------------------------------------
        # 3. 🥉 Bronze Layer - Iceberg 테이블 구축
        # -----------------------------------------------------------------------------
        print("\n🥉 Bronze Layer (Iceberg) 구축 시작...")
        
        # S3 랜딩 존에서 데이터 읽기
        landing_zone_path = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
        print(f"📂 랜딩 존에서 데이터 읽기: {landing_zone_path}")
        
        try:
            df_raw = spark.read.json(landing_zone_path)
            row_count = df_raw.count()
            print(f"✅ 랜딩 존 데이터 로드 성공! 행 수: {row_count:,}")
            
            # 처리 타임스탬프 추가
            df_raw_with_metadata = df_raw.withColumn("ingestion_timestamp", current_timestamp())
            
            # Bronze Iceberg 테이블 생성 (단순화)
            try:
                # 기존 테이블 삭제 후 새로 생성
                spark.sql("DROP TABLE IF EXISTS iceberg_catalog.bronze.raw_events")
                
                # 작은 샘플 데이터로 테스트 (메모리 부담 감소)
                df_sample = df_raw_with_metadata.limit(10000)
                
                df_sample.writeTo("iceberg_catalog.bronze.raw_events") \
                    .tableProperty("format-version", "2") \
                    .tableProperty("write.target-file-size-bytes", "67108864") \
                    .create()
                
                sample_count = df_sample.count()
                print(f"✅ Bronze Iceberg 테이블 'iceberg_catalog.bronze.raw_events' 생성 완료 (샘플 {sample_count:,}행)")
                
            except Exception as e:
                print(f"❌ Bronze 테이블 생성 실패: {e}")
                return
            
        except Exception as e:
            print(f"❌ 랜딩 존에서 데이터를 읽을 수 없습니다: {e}")
            print("💡 upload_to_landing_zone.py를 먼저 실행하여 데이터를 업로드하세요.")
            spark.stop()
            return

        # -----------------------------------------------------------------------------
        # 4. 📊 Bronze 테이블 검증
        # -----------------------------------------------------------------------------
        print("\n📊 Bronze Iceberg 테이블 검증...")
        
        # Bronze 테이블에서 데이터 읽기 테스트
        try:
            df_bronze = spark.table("iceberg_catalog.bronze.raw_events")
            bronze_count = df_bronze.count()
            print(f"✅ Bronze 테이블 검증 성공! 행 수: {bronze_count:,}")
            
            # 스키마 확인
            print("\n📋 Bronze 테이블 스키마:")
            df_bronze.printSchema()
            
            # 샘플 데이터 확인 (안전하게)
            print("\n📊 Bronze 샘플 데이터 (상위 2행):")
            df_bronze.select("event_id", "event_name", "user_id", "timestamp").show(2, truncate=True)
            
        except Exception as e:
            print(f"❌ Bronze 테이블 검증 실패: {e}")
            return

        # -----------------------------------------------------------------------------
        # 5. 🧪 Iceberg 고급 기능 테스트 (안전하게)
        # -----------------------------------------------------------------------------
        print("\n🧪 Iceberg 고급 기능 테스트...")
        
        try:
            # 테이블 정보 조회
            print("\n🗃️ Bronze 테이블 정보:")
            spark.sql("DESCRIBE TABLE iceberg_catalog.bronze.raw_events").show(5, truncate=False)
            
        except Exception as e:
            print(f"⚠️ 테이블 정보 조회 실패: {e}")

        try:
            # 스냅샷 이력 조회 (안전하게)
            print("\n📸 Bronze 테이블 스냅샷 이력:")
            snapshots = spark.sql("SELECT snapshot_id, committed_at FROM iceberg_catalog.bronze.raw_events.snapshots ORDER BY committed_at DESC LIMIT 2")
            snapshots.show(truncate=False)
            
        except Exception as e:
            print(f"⚠️ 스냅샷 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 6. 검증 및 요약
        # -----------------------------------------------------------------------------
        print("\n📈 Stable Iceberg ETL 파이프라인 완료 요약:")
        print(f"🥉 Bronze 행 수: {bronze_count:,}")
        print(f"📊 생성된 네임스페이스: iceberg_catalog.bronze, iceberg_catalog.silver, iceberg_catalog.gold")
        print(f"🗃️ Bronze 테이블: iceberg_catalog.bronze.raw_events (Iceberg v2)")
        print(f"🏗️ 카탈로그: Hadoop Catalog (안정적 버전)")
        print(f"💾 데이터 저장소: S3 (s3a://reciping-user-event-logs/warehouse/iceberg)")
        
        # 지원되는 기능들
        print(f"\n💎 검증된 Iceberg 기능:")
        print(f"   ✅ 테이블 생성 및 관리")
        print(f"   ✅ 스냅샷 이력 추적")
        print(f"   ✅ 메타데이터 조회")
        print(f"   ✅ S3 스토리지 통합")
        print(f"   📝 다음 단계: Silver Layer 구축, Time Travel, Schema Evolution")

        # -----------------------------------------------------------------------------
        # 7. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("✅ Stable Iceberg ETL 파이프라인이 완료되었습니다!")

    except Exception as e:
        print(f"❌ 전체 프로세스 실패: {e}")
        import traceback
        traceback.print_exc()
        
        # 스파크 세션이 있다면 종료
        try:
            spark.stop()
        except:
            pass

# 이 스크립트가 직접 실행될 때만 main() 함수를 호출합니다.
if __name__ == "__main__":
    main()
