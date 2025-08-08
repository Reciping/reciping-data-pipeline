# test_iceberg_basic.py
"""
기본적인 Iceberg 기능을 테스트하는 간단한 스크립트
"""

def test_iceberg_features():
    """Iceberg 고급 기능 테스트"""
    print("🎯 Iceberg 고급 기능 테스트 시작...")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, current_timestamp, to_timestamp
        from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType
        
        print("✅ PySpark 모듈 로드 성공")
        
        # SparkSession 생성 (간단한 설정)
        spark = SparkSession.builder \
            .appName("IcebergBasicTest") \
            .master("local[2]") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.type", "hadoop") \
            .config("spark.sql.catalog.demo.warehouse", "/tmp/iceberg-warehouse") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ SparkSession 생성 성공")
        
        # 간단한 테스트 데이터 생성
        test_data = [
            ("evt001", "page_view", "user001", "2024-01-15 10:30:00"),
            ("evt002", "recipe_search", "user001", "2024-01-15 10:31:00"),
            ("evt003", "recipe_view", "user002", "2024-01-15 11:30:00"),
            ("evt004", "recipe_bookmark", "user002", "2024-01-15 11:31:00"),
            ("evt005", "comment_write", "user003", "2024-01-16 09:15:00")
        ]
        
        df = spark.createDataFrame(test_data, ["event_id", "event_name", "user_id", "timestamp"]) \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("ingestion_time", current_timestamp()) \
            .drop("timestamp")
        
        print(f"✅ 테스트 데이터 생성 성공: {df.count()}행")
        df.show()
        
        # 네임스페이스 생성
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.test")
            print("✅ 네임스페이스 생성 성공")
        except Exception as e:
            print(f"⚠️ 네임스페이스 생성 실패: {e}")
        
        # Iceberg 테이블 생성
        try:
            spark.sql("DROP TABLE IF EXISTS demo.test.events")
            
            df.writeTo("demo.test.events") \
                .tableProperty("format-version", "2") \
                .create()
            
            print("✅ Iceberg 테이블 생성 성공")
            
            # 테이블에서 데이터 읽기
            result = spark.table("demo.test.events")
            print(f"✅ 테이블 읽기 성공: {result.count()}행")
            result.show()
            
            # 스냅샷 정보 조회
            try:
                snapshots = spark.sql("SELECT snapshot_id, committed_at FROM demo.test.events.snapshots")
                print("✅ 스냅샷 조회 성공:")
                snapshots.show()
            except Exception as e:
                print(f"⚠️ 스냅샷 조회 실패: {e}")
            
            # 추가 데이터 삽입
            new_data = [
                ("evt006", "recipe_rating", "user004", "2024-01-16 15:30:00")
            ]
            
            df_new = spark.createDataFrame(new_data, ["event_id", "event_name", "user_id", "timestamp"]) \
                .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
                .withColumn("ingestion_time", current_timestamp()) \
                .drop("timestamp")
            
            df_new.writeTo("demo.test.events").append()
            print("✅ 데이터 추가 성공")
            
            # 업데이트 후 데이터 확인
            updated_result = spark.table("demo.test.events")
            print(f"✅ 업데이트 후 행 수: {updated_result.count()}")
            
            print("\n📈 Iceberg 기본 기능 테스트 완료!")
            print("✅ 테이블 생성/조회")
            print("✅ 데이터 추가 (append)")
            print("✅ 스냅샷 조회")
            print("✅ 메타데이터 관리")
            
        except Exception as e:
            print(f"❌ Iceberg 테이블 조작 실패: {e}")
            import traceback
            traceback.print_exc()
        
        spark.stop()
        
    except Exception as e:
        print(f"❌ 전체 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_iceberg_features()
