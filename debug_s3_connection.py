#!/usr/bin/env python3
"""
S3 연결 문제 디버깅 스크립트
Airflow DAG 실행 전에 S3 연결을 테스트합니다.
"""

from pyspark.sql import SparkSession
import time

def test_s3_connection():
    print("🔍 S3 연결 테스트 시작...")
    
    # SparkSession 생성
    spark = SparkSession.builder \
        .appName("S3ConnectionTest") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .getOrCreate()
    
    try:
        s3_path = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
        print(f"📂 테스트 경로: {s3_path}")
        
        # 1. 경로 존재 확인
        print("1️⃣ 경로 존재 여부 확인...")
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3_path)
        
        start_time = time.time()
        path_exists = fs.exists(path_obj)
        elapsed = time.time() - start_time
        
        print(f"   결과: {'존재함' if path_exists else '존재하지 않음'} (소요시간: {elapsed:.2f}초)")
        
        if not path_exists:
            print("❌ S3 경로가 존재하지 않습니다!")
            return False
        
        # 2. 파일 목록 확인
        print("2️⃣ 파일 목록 확인...")
        start_time = time.time()
        file_status = fs.listStatus(path_obj)
        elapsed = time.time() - start_time
        
        print(f"   찾은 파일 수: {len(file_status)} (소요시간: {elapsed:.2f}초)")
        
        for i, file_stat in enumerate(file_status[:5]):  # 처음 5개만
            file_path = file_stat.getPath().toString()
            file_size = file_stat.getLen()
            print(f"   {i+1}. {file_path.split('/')[-1]} ({file_size:,} bytes)")
        
        if len(file_status) == 0:
            print("❌ 읽을 파일이 없습니다!")
            return False
        
        # 3. 작은 샘플 데이터 읽기 테스트
        print("3️⃣ 샘플 데이터 읽기 테스트...")
        start_time = time.time()
        
        sample_df = spark.read \
            .option("multiline", "false") \
            .option("mode", "PERMISSIVE") \
            .json(s3_path) \
            .limit(10)
        
        sample_count = sample_df.count()
        elapsed = time.time() - start_time
        
        print(f"   읽은 샘플 행 수: {sample_count} (소요시간: {elapsed:.2f}초)")
        
        if sample_count > 0:
            print("✅ S3 연결 및 데이터 읽기 성공!")
            
            # 샘플 데이터 구조 확인
            print("4️⃣ 데이터 구조 확인...")
            sample_df.printSchema()
            print("\n샘플 데이터:")
            sample_df.show(1, truncate=False)
            
            return True
        else:
            print("❌ 데이터를 읽을 수 없습니다!")
            return False
            
    except Exception as e:
        print(f"❌ S3 연결 테스트 실패: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False
        
    finally:
        spark.stop()

def test_hive_metastore():
    print("\n🗄️ Hive Metastore 연결 테스트...")
    
    spark = SparkSession.builder \
        .appName("HiveMetastoreTest") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://10.0.11.86:9083") \
        .getOrCreate()
    
    try:
        # Hive Metastore 연결 테스트
        databases = spark.sql("SHOW DATABASES").collect()
        print(f"✅ Hive Metastore 연결 성공! 데이터베이스 수: {len(databases)}")
        
        for db in databases[:5]:  # 처음 5개만
            print(f"   - {db['databaseName']}")
            
        return True
        
    except Exception as e:
        print(f"❌ Hive Metastore 연결 실패: {str(e)}")
        return False
        
    finally:
        spark.stop()

if __name__ == "__main__":
    print("🔧 ETL 파이프라인 사전 진단 시작")
    print("=" * 50)
    
    s3_ok = test_s3_connection()
    hive_ok = test_hive_metastore()
    
    print("\n" + "=" * 50)
    print("📋 진단 결과 요약:")
    print(f"   S3 연결: {'✅ 정상' if s3_ok else '❌ 문제'}")
    print(f"   Hive Metastore: {'✅ 정상' if hive_ok else '❌ 문제'}")
    
    if s3_ok and hive_ok:
        print("\n🎉 모든 연결이 정상입니다! ETL 파이프라인을 실행할 수 있습니다.")
    else:
        print("\n⚠️ 문제가 발견되었습니다. 위의 오류를 해결 후 다시 시도해주세요.")
