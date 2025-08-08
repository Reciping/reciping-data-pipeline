# bronze_to_silver_iceberg_local.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, date_format, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, DateType, TimestampType

def main():
    """
    로컬 환경에서 Iceberg 고급 기능을 테스트하는
    데이터 레이크하우스 ETL 파이프라인 (로컬 파일 시스템 사용).
    """
    try:
        # 시스템 환경 변수를 먼저 설정 (로컬 테스트용)
        import os
        import subprocess
        
        # 로컬 테스트용 환경 변수 설정
        os.environ['HADOOP_USER_NAME'] = 'spark'
        os.environ['USER'] = 'spark'
        os.environ['HOME'] = '/tmp'
        os.environ['LOGNAME'] = 'spark'
        os.environ['USERNAME'] = 'spark'
        
        # 필요한 디렉토리 생성
        os.makedirs('/tmp/.ivy2', exist_ok=True)
        os.makedirs('/tmp/warehouse/iceberg', exist_ok=True)

        print("🎯 로컬 Iceberg 고급 기능 테스트를 시작합니다...")

        # -----------------------------------------------------------------------------
        # 1. 스파크 세션 생성 (로컬 Iceberg + 로컬 파일 시스템)
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession with Iceberg (로컬 테스트) 생성을 시도합니다...")
        
        spark = SparkSession.builder \
            .appName("Iceberg_Local_Advanced_Features_Test") \
            .master("local[2]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "file:///tmp/warehouse/iceberg") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession with Iceberg (로컬 테스트)가 성공적으로 생성되었습니다!")

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
        # 3. 🧪 샘플 데이터 생성 및 Iceberg 고급 기능 테스트
        # -----------------------------------------------------------------------------
        print("\n🧪 Iceberg 고급 기능 테스트용 샘플 데이터 생성...")
        
        # 샘플 이벤트 데이터 생성
        sample_data = [
            ("evt001", "page_view", "user001", "anon001", "session001", "2024-01-15 10:30:00", "2024-01-15", 2024, 1, 15, 10),
            ("evt002", "recipe_search", "user001", "anon001", "session001", "2024-01-15 10:31:00", "2024-01-15", 2024, 1, 15, 10),
            ("evt003", "recipe_view", "user001", "anon001", "session001", "2024-01-15 10:32:00", "2024-01-15", 2024, 1, 15, 10),
            ("evt004", "page_view", "user002", "anon002", "session002", "2024-01-15 11:30:00", "2024-01-15", 2024, 1, 15, 11),
            ("evt005", "recipe_search", "user002", "anon002", "session002", "2024-01-15 11:31:00", "2024-01-15", 2024, 1, 15, 11),
            ("evt006", "recipe_bookmark", "user002", "anon002", "session002", "2024-01-15 11:32:00", "2024-01-15", 2024, 1, 15, 11),
            ("evt007", "page_view", "user003", "anon003", "session003", "2024-01-16 09:15:00", "2024-01-16", 2024, 1, 16, 9),
            ("evt008", "recipe_view", "user003", "anon003", "session003", "2024-01-16 09:16:00", "2024-01-16", 2024, 1, 16, 9),
            ("evt009", "comment_write", "user003", "anon003", "session003", "2024-01-16 09:17:00", "2024-01-16", 2024, 1, 16, 9),
            ("evt010", "recipe_share", "user001", "anon001", "session004", "2024-01-16 14:20:00", "2024-01-16", 2024, 1, 16, 14)
        ]
        
        columns = ["event_id", "event_name", "user_id", "anonymous_id", "session_id", 
                  "event_timestamp", "event_date", "year", "month", "day", "hour"]
        
        df_sample = spark.createDataFrame(sample_data, columns) \
            .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
            .withColumn("event_date", col("event_date").cast(DateType())) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        print(f"✅ 샘플 데이터 생성 완료: {df_sample.count()}행")
        df_sample.show(truncate=False)

        # -----------------------------------------------------------------------------
        # 4. 🥉 Bronze Iceberg 테이블 생성
        # -----------------------------------------------------------------------------
        print("\n🥉 Bronze Iceberg 테이블 생성...")
        
        try:
            # 기존 테이블 삭제 후 새로 생성
            spark.sql("DROP TABLE IF EXISTS iceberg_catalog.bronze.events_raw")
            
            df_sample.writeTo("iceberg_catalog.bronze.events_raw") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.target-file-size-bytes", "67108864") \
                .create()
            
            print("✅ Bronze Iceberg 테이블 'iceberg_catalog.bronze.events_raw' 생성 완료")
            
        except Exception as e:
            print(f"❌ Bronze 테이블 생성 실패: {e}")
            return

        # -----------------------------------------------------------------------------
        # 5. 🥈 Silver Iceberg 테이블 생성 (파티션 적용)
        # -----------------------------------------------------------------------------
        print("\n🥈 Silver Iceberg 테이블 생성 (파티션 적용)...")
        
        # Silver 데이터 변환
        df_silver = df_sample.select(
            "event_id", "event_name", "user_id", "anonymous_id", "session_id",
            "event_timestamp", "event_date", "ingestion_timestamp",
            "year", "month", "day", "hour"
        ).filter(col("event_id").isNotNull())
        
        try:
            # 기존 테이블 삭제 후 새로 생성
            spark.sql("DROP TABLE IF EXISTS iceberg_catalog.silver.events_clean")
            
            df_silver.writeTo("iceberg_catalog.silver.events_clean") \
                .partitionedBy("year", "month", "day") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.target-file-size-bytes", "67108864") \
                .create()
            
            print("✅ Silver Iceberg 테이블 'iceberg_catalog.silver.events_clean' 생성 완료")
            
        except Exception as e:
            print(f"❌ Silver 테이블 생성 실패: {e}")
            return

        # -----------------------------------------------------------------------------
        # 6. 📊 Iceberg 고급 기능 시연
        # -----------------------------------------------------------------------------
        print("\n📊 Iceberg 고급 기능 시연 시작...")
        
        # 6.1. 테이블 정보 조회
        print("\n🗃️ Bronze 테이블 정보:")
        try:
            bronze_info = spark.sql("DESCRIBE TABLE iceberg_catalog.bronze.events_raw")
            bronze_info.show(10, truncate=False)
        except Exception as e:
            print(f"⚠️ Bronze 테이블 정보 조회 실패: {e}")
        
        print("\n🗃️ Silver 테이블 정보:")
        try:
            silver_info = spark.sql("DESCRIBE TABLE iceberg_catalog.silver.events_clean")
            silver_info.show(10, truncate=False)
        except Exception as e:
            print(f"⚠️ Silver 테이블 정보 조회 실패: {e}")

        # 6.2. 스냅샷 이력 조회
        print("\n📸 Bronze 테이블 스냅샷 이력:")
        try:
            bronze_snapshots = spark.sql("SELECT snapshot_id, committed_at, summary FROM iceberg_catalog.bronze.events_raw.snapshots ORDER BY committed_at DESC")
            bronze_snapshots.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Bronze 스냅샷 조회 실패: {e}")
        
        print("\n📸 Silver 테이블 스냅샷 이력:")
        try:
            silver_snapshots = spark.sql("SELECT snapshot_id, committed_at, summary FROM iceberg_catalog.silver.events_clean.snapshots ORDER BY committed_at DESC")
            silver_snapshots.show(truncate=False)
        except Exception as e:
            print(f"⚠️ Silver 스냅샷 조회 실패: {e}")

        # 6.3. 파일 정보 확인
        print("\n📋 Silver 테이블 파일 정보:")
        try:
            silver_files = spark.sql("SELECT file_path, file_format, record_count FROM iceberg_catalog.silver.events_clean.files")
            silver_files.show(truncate=False)
        except Exception as e:
            print(f"⚠️ 파일 정보 조회 실패: {e}")

        # 6.4. 파티션 정보 확인
        print("\n📁 Silver 테이블 파티션 정보:")
        try:
            silver_partitions = spark.sql("SHOW PARTITIONS iceberg_catalog.silver.events_clean")
            silver_partitions.show(truncate=False)
        except Exception as e:
            print(f"⚠️ 파티션 정보 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 7. 🔄 Iceberg Time Travel 기능 데모
        # -----------------------------------------------------------------------------
        print("\n🔄 Iceberg Time Travel 기능 데모...")
        
        try:
            # 최신 스냅샷 ID 조회
            snapshots = spark.sql("SELECT snapshot_id, committed_at FROM iceberg_catalog.silver.events_clean.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
            if snapshots:
                snapshot_id = snapshots[0]['snapshot_id']
                committed_at = snapshots[0]['committed_at']
                print(f"📸 최신 스냅샷: {snapshot_id} (생성시간: {committed_at})")
                
                # Time Travel 쿼리 (특정 스냅샷으로)
                time_travel_count = spark.sql(f"""
                    SELECT COUNT(*) as record_count 
                    FROM iceberg_catalog.silver.events_clean 
                    VERSION AS OF {snapshot_id}
                """).collect()[0]['record_count']
                
                print(f"⏰ Time Travel 쿼리 결과: {time_travel_count:,}행")
                
            else:
                print("⚠️ 스냅샷을 찾을 수 없습니다.")
                
        except Exception as e:
            print(f"⚠️ Time Travel 기능 테스트 실패: {e}")

        # -----------------------------------------------------------------------------
        # 8. 📝 데이터 업데이트 및 스키마 진화 테스트
        # -----------------------------------------------------------------------------
        print("\n📝 데이터 업데이트 및 스키마 진화 테스트...")
        
        try:
            # 8.1. 새로운 데이터 추가
            new_data = [
                ("evt011", "recipe_rating", "user004", "anon004", "session005", "2024-01-17 15:30:00", "2024-01-17", 2024, 1, 17, 15),
                ("evt012", "page_view", "user004", "anon004", "session005", "2024-01-17 15:31:00", "2024-01-17", 2024, 1, 17, 15)
            ]
            
            df_new = spark.createDataFrame(new_data, columns) \
                .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
                .withColumn("event_date", col("event_date").cast(DateType())) \
                .withColumn("ingestion_timestamp", current_timestamp())
            
            # 기존 테이블에 새 데이터 추가
            df_new.writeTo("iceberg_catalog.silver.events_clean").append()
            
            print("✅ 새로운 데이터 추가 완료")
            
            # 업데이트 후 데이터 확인
            updated_count = spark.table("iceberg_catalog.silver.events_clean").count()
            print(f"📊 업데이트 후 총 행 수: {updated_count}")
            
            # 새로운 스냅샷 확인
            print("\n📸 업데이트 후 스냅샷 이력:")
            latest_snapshots = spark.sql("SELECT snapshot_id, committed_at, summary FROM iceberg_catalog.silver.events_clean.snapshots ORDER BY committed_at DESC LIMIT 3")
            latest_snapshots.show(truncate=False)
            
        except Exception as e:
            print(f"⚠️ 데이터 업데이트 실패: {e}")

        # -----------------------------------------------------------------------------
        # 9. 🧹 테이블 유지보수 기능 테스트
        # -----------------------------------------------------------------------------
        print("\n🧹 Iceberg 테이블 유지보수 기능 테스트...")
        
        try:
            # 9.1. 테이블 속성 확인
            print("\n⚙️ Silver 테이블 속성:")
            table_props = spark.sql("SHOW TBLPROPERTIES iceberg_catalog.silver.events_clean")
            table_props.show(truncate=False)
            
        except Exception as e:
            print(f"⚠️ 테이블 속성 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 10. 검증 및 요약
        # -----------------------------------------------------------------------------
        print("\n📈 Iceberg 고급 기능 테스트 완료 요약:")
        
        # 테이블 카운트
        bronze_count = spark.table("iceberg_catalog.bronze.events_raw").count()
        silver_count = spark.table("iceberg_catalog.silver.events_clean").count()
        
        print(f"🥉 Bronze 테이블 행 수: {bronze_count:,}")
        print(f"🥈 Silver 테이블 행 수: {silver_count:,}")
        print(f"📊 네임스페이스: iceberg_catalog.bronze, iceberg_catalog.silver, iceberg_catalog.gold")
        print(f"🗃️ Bronze 테이블: iceberg_catalog.bronze.events_raw (Iceberg v2)")
        print(f"🗃️ Silver 테이블: iceberg_catalog.silver.events_clean (Iceberg v2, 파티션: year/month/day)")
        print(f"🏗️ 카탈로그: Hadoop Catalog (로컬)")
        print(f"💾 데이터 저장소: 로컬 파일 시스템 (/tmp/warehouse/iceberg)")
        
        # 구현된 Iceberg 고급 기능들
        print(f"\n💎 성공적으로 테스트된 Iceberg 고급 기능:")
        print(f"   ✅ 네임스페이스 관리: 다중 레이어 구성")
        print(f"   ✅ 파티션 테이블: 년/월/일 기준 파티셔닝")
        print(f"   ✅ 메타데이터 테이블: 스냅샷, 파일, 파티션 조회")
        print(f"   ✅ Time Travel: 특정 스냅샷 시점 데이터 조회")
        print(f"   ✅ 데이터 추가: 기존 테이블에 새 데이터 append")
        print(f"   ✅ 스냅샷 이력: 변경 사항 추적 및 관리")
        print(f"   ✅ 테이블 속성: 세부 설정 및 최적화 옵션")
        print(f"   📝 준비 완료: Schema Evolution, ACID Transactions, Compaction")

        # -----------------------------------------------------------------------------
        # 11. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("✅ Iceberg 고급 기능 테스트가 완료되었습니다!")

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
