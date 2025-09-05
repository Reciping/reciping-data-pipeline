#!/usr/bin/env python3
"""
개선된 S3 + Hive Metastore + Iceberg 연결 디버깅 스크립트
필요한 JAR 파일들을 포함하여 S3/Hive/Iceberg 연결을 테스트합니다.
"""

from pyspark.sql import SparkSession
import time

def test_s3_with_packages():
    print("🔍 S3 연결 테스트 시작 (필수 패키지 포함)...")
    
    # 필요한 패키지들을 명시적으로 지정하여 SparkSession 생성
    spark = SparkSession.builder \
        .appName("S3ConnectionTest") \
        .config("spark.jars.packages", 
               "org.apache.hadoop:hadoop-aws:3.3.4," \
               "com.amazonaws:aws-java-sdk-bundle:1.12.262," \
               "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
               "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.limit", "5") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .getOrCreate()
    
    try:
        s3_path = "s3a://reciping-user-event-logs/bronze/landing-zone/events/"
        print(f"📂 테스트 경로: {s3_path}")
        
        # 1. Spark의 FileSystem API를 사용한 경로 확인
        print("1️⃣ Spark FileSystem API를 통한 경로 확인...")
        start_time = time.time()
        
        try:
            # 실제 데이터는 jsonl 확장자를 사용하므로 패턴 수정
            files_df = spark.read.option("multiline", "false").text(s3_path + "*.jsonl")
            
            # lazy evaluation이므로 실제로 action을 수행해야 함
            file_count = files_df.count()
            elapsed = time.time() - start_time
            
            print(f"   ✅ S3 접근 성공! 파일 수: {file_count} (소요시간: {elapsed:.2f}초)")
            
            if file_count > 0:
                print("2️⃣ JSONL 파일 읽기 테스트...")
                start_time = time.time()
                
                # JSONL으로 읽어보기
                json_df = spark.read.option("multiline", "false").json(s3_path + "*.jsonl").limit(5)
                row_count = json_df.count()
                elapsed = time.time() - start_time
                
                print(f"   ✅ JSONL 읽기 성공! 행 수: {row_count} (소요시간: {elapsed:.2f}초)")
                
                print("3️⃣ 데이터 구조 확인...")
                json_df.printSchema()
                
                print("\n📄 샘플 데이터 (첫 번째 행):")
                json_df.show(1, truncate=False)
                
                return True
            else:
                print("   ⚠️  파일은 접근 가능하지만 읽을 수 있는 데이터가 없습니다.")
                return False
                
        except Exception as inner_e:
            print(f"   ❌ S3 접근 실패: {str(inner_e)}")
            return False
            
    except Exception as e:
        print(f"❌ S3 연결 테스트 실패: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False
        
    finally:
        spark.stop()

def test_aws_credentials():
    print("\n🔐 AWS 자격증명 테스트...")
    
    import subprocess
    import os
    
    try:
        # AWS CLI가 설치되어 있는지 확인
        result = subprocess.run(['aws', '--version'], capture_output=True, text=True)
        print(f"   AWS CLI 버전: {result.stdout.strip()}")
        
        # AWS 자격증명 확인
        result = subprocess.run(['aws', 'sts', 'get-caller-identity'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ AWS 자격증명 정상: {result.stdout.strip()}")
        else:
            print(f"   ❌ AWS 자격증명 문제: {result.stderr.strip()}")
            return False
        
        # S3 버킷 접근 테스트
        result = subprocess.run([
            'aws', 's3', 'ls', 
            's3://reciping-user-event-logs/bronze/landing-zone/events/',
            '--region', 'ap-northeast-2'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            file_count = len([line for line in lines if line.strip()])
            print(f"   ✅ S3 버킷 접근 성공! 파일 수: {file_count}")
            
            # 처음 5개 파일만 표시
            for line in lines[:5]:
                if line.strip():
                    print(f"      {line.strip()}")
            
            return True
        else:
            print(f"   ❌ S3 버킷 접근 실패: {result.stderr.strip()}")
            return False
            
    except FileNotFoundError:
        print("   ❌ AWS CLI가 설치되지 않았습니다.")
        return False
    except Exception as e:
        print(f"   ❌ AWS 자격증명 테스트 실패: {str(e)}")
        return False

def test_hive_metastore_improved():
    print("\n🗄️ Hive Metastore 연결 테스트 (개선됨)...")
    
    spark = SparkSession.builder \
        .appName("HiveMetastoreTest") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://10.0.11.86:9083") \
        .getOrCreate()
    
    try:
        # 1. 기본 데이터베이스 목록 확인
        print("   1️⃣ 데이터베이스 목록 확인...")
        databases = spark.sql("SHOW DATABASES").collect()
        print(f"      데이터베이스 수: {len(databases)}")
        
        for db in databases:
            db_name = db[0] if isinstance(db, (list, tuple)) else str(db)
            print(f"      - {db_name}")
        
        # 2. Iceberg 카탈로그 테스트
        print("   2️⃣ Iceberg 카탈로그 테스트...")
        try:
            iceberg_dbs = spark.sql("SHOW DATABASES IN iceberg_catalog").collect()
            print(f"      Iceberg 데이터베이스 수: {len(iceberg_dbs)}")
            for db in iceberg_dbs[:3]:
                db_name = db[0] if isinstance(db, (list, tuple)) else str(db)
                print(f"      - iceberg_catalog.{db_name}")
        except Exception as iceberg_e:
            print(f"      ⚠️  Iceberg 카탈로그 접근 중 오류: {str(iceberg_e)}")
        
        # 3. 테스트 테이블 생성해보기
        print("   3️⃣ 테스트 테이블 생성 시도...")
        try:
            spark.sql("""
                CREATE DATABASE IF NOT EXISTS iceberg_catalog.test_db
                COMMENT 'Test database for connection verification'
            """)
            print("      ✅ 테스트 데이터베이스 생성 성공!")
            return True
        except Exception as table_e:
            print(f"      ❌ 테스트 테이블 생성 실패: {str(table_e)}")
            return False
    except Exception as e:
        print(f"   ❌ Hive Metastore 연결 실패: {str(e)}")
        return False
    finally:
        spark.stop()

def check_network_connectivity():
    print("\n🌐 네트워크 연결 테스트...")
    
    import subprocess
    
    endpoints = [
        ("S3 엔드포인트", "s3.ap-northeast-2.amazonaws.com", 443),
        ("Hive Metastore", "10.0.11.86", 9083)
    ]
    
    for name, host, port in endpoints:
        try:
            result = subprocess.run([
                'timeout', '10', 'bash', '-c', f'echo > /dev/tcp/{host}/{port}'
            ], capture_output=True)
            if result.returncode == 0:
                print(f"   ✅ {name} ({host}:{port}) 연결 가능")
            else:
                print(f"   ❌ {name} ({host}:{port}) 연결 불가")
        except Exception as e:
            print(f"   ❌ {name} 연결 테스트 실패: {str(e)}")

if __name__ == "__main__":
    print("🔧 개선된 ETL 파이프라인 사전 진단 시작")
    print("=" * 60)
    
    aws_ok = test_aws_credentials()
    network_ok = check_network_connectivity()
    s3_ok = test_s3_with_packages()
    hive_ok = test_hive_metastore_improved()
    
    print("\n" + "=" * 60)
    print("📋 진단 결과 요약:")
    print(f"   AWS 자격증명: {'✅ 정상' if aws_ok else '❌ 문제'}")
    print(f"   S3 연결: {'✅ 정상' if s3_ok else '❌ 문제'}")
    print(f"   Hive Metastore: {'✅ 정상' if hive_ok else '❌ 문제'}")
    
    if aws_ok and s3_ok and hive_ok:
        print("\n🎉 모든 연결이 정상입니다! ETL 파이프라인을 실행할 수 있습니다.")
    else:
        print("\n⚠️ 문제가 발견되었습니다.")
        if not aws_ok:
            print("   🔧 AWS 자격증명을 확인하세요: aws configure 또는 IAM 역할 설정")
        if not s3_ok:
            print("   🔧 S3 접근 권한과 네트워크를 확인하세요")
        if not hive_ok:
            print("   🔧 Hive Metastore 서비스 상태를 확인하세요")
