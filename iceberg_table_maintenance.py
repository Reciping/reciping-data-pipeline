# iceberg_table_maintenance.py
from pyspark.sql import SparkSession
import os

def main():
    """
    Iceberg 테이블의 고급 관리 및 유지보수 작업을 수행하는 스크립트.
    - 테이블 최적화 (Compaction)
    - 스냅샷 관리 및 정리
    - 테이블 스키마 진화
    - 성능 모니터링
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
        # 1. 스파크 세션 생성
        # -----------------------------------------------------------------------------
        print("🔧 SparkSession for Iceberg Maintenance 생성...")
        
        spark = SparkSession.builder \
            .appName("Iceberg_Table_Maintenance") \
            .master("local[*]") \
            .config("spark.sql.session.timeZone", "Asia/Seoul") \
            .config("spark.sql.adaptive.enabled", "true") \
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
        print("✅ SparkSession 생성 완료!")

        # -----------------------------------------------------------------------------
        # 2. 📊 테이블 상태 분석
        # -----------------------------------------------------------------------------
        print("\n📊 Iceberg 테이블 상태 분석...")
        
        # 모든 데이터베이스와 테이블 목록 조회
        print("\n🗃️ 전체 데이터베이스 및 테이블 목록:")
        spark.sql("SHOW DATABASES").show()
        
        databases = ["bronze_db", "silver_db", "gold_db"]
        for db in databases:
            try:
                print(f"\n📁 {db} 테이블 목록:")
                spark.sql(f"SHOW TABLES IN {db}").show()
            except Exception as e:
                print(f"⚠️ {db} 데이터베이스에 접근할 수 없습니다: {e}")

        # -----------------------------------------------------------------------------
        # 3. 🔧 테이블 최적화 (Compaction)
        # -----------------------------------------------------------------------------
        print("\n🔧 테이블 최적화 (Compaction) 수행...")
        
        # Silver 테이블 최적화
        try:
            print("\n🥈 Silver 테이블 최적화...")
            
            # 현재 파일 상태 확인
            print("📋 최적화 전 Silver 테이블 파일 상태:")
            spark.sql("SELECT * FROM silver_db.cleaned_events.files LIMIT 10").show(truncate=False)
            
            # Compaction 수행 (작은 파일들을 큰 파일로 합치기)
            spark.sql("CALL spark_catalog.system.rewrite_data_files('silver_db.cleaned_events')")
            print("✅ Silver 테이블 Compaction 완료")
            
            # 최적화 후 상태 확인
            print("📋 최적화 후 Silver 테이블 파일 상태:")
            spark.sql("SELECT * FROM silver_db.cleaned_events.files LIMIT 10").show(truncate=False)
            
        except Exception as e:
            print(f"⚠️ Silver 테이블 최적화 실패: {e}")

        # Gold 테이블들 최적화
        gold_tables = [
            "daily_events_summary",
            "user_behavior_profiles", 
            "recipe_popularity_analysis",
            "search_trends_analysis",
            "ab_test_performance"
        ]
        
        for table in gold_tables:
            try:
                print(f"\n🥇 {table} 테이블 최적화...")
                spark.sql(f"CALL spark_catalog.system.rewrite_data_files('gold_db.{table}')")
                print(f"✅ {table} 테이블 Compaction 완료")
            except Exception as e:
                print(f"⚠️ {table} 테이블 최적화 실패: {e}")

        # -----------------------------------------------------------------------------
        # 4. 🗂️ 스냅샷 관리 및 정리
        # -----------------------------------------------------------------------------
        print("\n🗂️ 스냅샷 관리 및 정리...")
        
        # Silver 테이블 스냅샷 정보 조회
        try:
            print("\n📸 Silver 테이블 스냅샷 이력:")
            snapshots_df = spark.sql("SELECT * FROM silver_db.cleaned_events.snapshots ORDER BY committed_at DESC")
            snapshots_df.show(10, truncate=False)
            
            # 오래된 스냅샷 정리 (7일 이전 스냅샷 삭제)
            print("\n🧹 오래된 스냅샷 정리 (7일 이전)...")
            spark.sql("CALL spark_catalog.system.expire_snapshots('silver_db.cleaned_events', INTERVAL 7 DAYS)")
            print("✅ 오래된 스냅샷 정리 완료")
            
        except Exception as e:
            print(f"⚠️ 스냅샷 관리 실패: {e}")

        # -----------------------------------------------------------------------------
        # 5. 📈 테이블 성능 모니터링
        # -----------------------------------------------------------------------------
        print("\n📈 테이블 성능 모니터링...")
        
        # 각 테이블의 크기 및 파일 수 확인
        try:
            print("\n📊 Silver 테이블 성능 지표:")
            
            # 파일 수 및 크기 정보
            files_info = spark.sql("""
                SELECT 
                    COUNT(*) as file_count,
                    SUM(file_size_in_bytes) as total_size_bytes,
                    AVG(file_size_in_bytes) as avg_file_size_bytes,
                    MIN(file_size_in_bytes) as min_file_size_bytes,
                    MAX(file_size_in_bytes) as max_file_size_bytes
                FROM silver_db.cleaned_events.files
            """)
            files_info.show()
            
            # 파티션별 정보
            print("\n📁 Silver 테이블 파티션 정보:")
            partition_info = spark.sql("""
                SELECT 
                    partition,
                    COUNT(*) as file_count,
                    SUM(file_size_in_bytes) as partition_size_bytes
                FROM silver_db.cleaned_events.files
                GROUP BY partition
                ORDER BY partition_size_bytes DESC
            """)
            partition_info.show(20, truncate=False)
            
        except Exception as e:
            print(f"⚠️ 성능 모니터링 실패: {e}")

        # -----------------------------------------------------------------------------
        # 6. 🔄 스키마 진화 예제
        # -----------------------------------------------------------------------------
        print("\n🔄 스키마 진화 기능 데모...")
        
        try:
            # 현재 Silver 테이블 스키마 확인
            print("\n📋 현재 Silver 테이블 스키마:")
            spark.sql("DESCRIBE silver_db.cleaned_events").show(50, truncate=False)
            
            # 스키마 진화 예제: 새로운 컬럼 추가 (데모용)
            print("\n🆕 스키마 진화 예제: 새 컬럼 추가 시뮬레이션")
            print("💡 실제 운영에서는 다음과 같은 방식으로 스키마를 안전하게 진화시킬 수 있습니다:")
            print("   - ALTER TABLE ADD COLUMN")
            print("   - 새로운 컬럼은 기존 데이터에 대해 NULL 값을 가짐")
            print("   - 하위 호환성 보장")
            
            # 예제 스키마 진화 쿼리 (실행하지 않고 출력만)
            schema_evolution_example = """
            -- 예제: 새로운 추적 컬럼 추가
            ALTER TABLE silver_db.cleaned_events 
            ADD COLUMN data_quality_score DOUBLE COMMENT 'Data quality score (0.0-1.0)';
            
            -- 예제: 새로운 메타데이터 컬럼 추가
            ALTER TABLE silver_db.cleaned_events 
            ADD COLUMN data_lineage_id STRING COMMENT 'Data lineage tracking ID';
            """
            print(f"예제 쿼리:\n{schema_evolution_example}")
            
        except Exception as e:
            print(f"⚠️ 스키마 조회 실패: {e}")

        # -----------------------------------------------------------------------------
        # 7. 🚀 고급 Iceberg 기능 활용
        # -----------------------------------------------------------------------------
        print("\n🚀 고급 Iceberg 기능 활용...")
        
        try:
            # Time Travel 쿼리 예제
            print("\n⏰ Time Travel 기능 데모:")
            
            # 최신 스냅샷 ID 조회
            latest_snapshot = spark.sql("SELECT snapshot_id FROM silver_db.cleaned_events.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
            if latest_snapshot:
                snapshot_id = latest_snapshot[0]['snapshot_id']
                print(f"📸 최신 스냅샷 ID: {snapshot_id}")
                
                # Time Travel 쿼리 예제 (특정 스냅샷으로)
                time_travel_query = f"""
                -- 특정 스냅샷의 데이터 조회
                SELECT COUNT(*) as record_count 
                FROM silver_db.cleaned_events 
                VERSION AS OF {snapshot_id}
                """
                print(f"Time Travel 쿼리 예제:\n{time_travel_query}")
                
                # 실제 실행
                result = spark.sql(time_travel_query)
                result.show()
            
        except Exception as e:
            print(f"⚠️ Time Travel 기능 데모 실패: {e}")

        # -----------------------------------------------------------------------------
        # 8. 📋 유지보수 리포트 생성
        # -----------------------------------------------------------------------------
        print("\n📋 유지보수 리포트 생성...")
        
        maintenance_report = {
            "maintenance_timestamp": spark.sql("SELECT current_timestamp()").collect()[0][0],
            "operations_performed": [
                "테이블 최적화 (Compaction)",
                "스냅샷 정리",
                "성능 모니터링",
                "스키마 진화 검토"
            ],
            "recommendations": [
                "정기적인 Compaction 수행 (주 1회)",
                "스냅샷 정리 자동화 (일 1회)",
                "성능 지표 모니터링 대시보드 구축",
                "스키마 변경 시 영향도 분석 수행"
            ]
        }
        
        print(f"\n📊 유지보수 완료 시점: {maintenance_report['maintenance_timestamp']}")
        print(f"🔧 수행된 작업:")
        for op in maintenance_report['operations_performed']:
            print(f"   ✅ {op}")
        
        print(f"\n💡 권장사항:")
        for rec in maintenance_report['recommendations']:
            print(f"   📝 {rec}")

        # -----------------------------------------------------------------------------
        # 9. 스파크 세션 종료
        # -----------------------------------------------------------------------------
        spark.stop()
        print("\n✅ Iceberg 테이블 유지보수가 완료되었습니다!")

    except Exception as e:
        print(f"❌ 유지보수 작업 실패: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    main()
