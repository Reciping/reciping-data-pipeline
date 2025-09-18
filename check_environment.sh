#!/bin/bash
# 분산 EC2 환경 + RDS PostgreSQL + Hive Metastore 환경 진단 스크립트

echo "=== 분산 Iceberg 환경 진단 ==="

# 환경 변수 설정 (실제 값으로 수정 필요)
HIVE_METASTORE_HOST="10.0.11.86"  # Hive Metastore EC2의 Private IP
HIVE_METASTORE_PORT="9083"
RDS_ENDPOINT="your-rds-endpoint.region.rds.amazonaws.com"  # 실제 RDS 엔드포인트로 변경
TRINO_HOST="10.0.x.x"  # Trino EC2의 Private IP
AIRFLOW_HOST="10.0.x.x"  # Airflow EC2의 Private IP

echo "현재 실행 중인 EC2: $(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)"
echo "현재 EC2 역할: $(curl -s http://169.254.169.254/latest/meta-data/tags/instance/Name 2>/dev/null || echo 'Unknown')"

# 1. 네트워크 연결 테스트
echo -e "\n1. 네트워크 연결 테스트:"
echo "   Hive Metastore 연결 (${HIVE_METASTORE_HOST}:${HIVE_METASTORE_PORT}):"
if timeout 5 bash -c "cat < /dev/null > /dev/tcp/${HIVE_METASTORE_HOST}/${HIVE_METASTORE_PORT}"; then
    echo "   ✅ Hive Metastore 포트 접근 가능"
else
    echo "   ❌ Hive Metastore 포트 접근 불가"
    echo "      - Security Group 9083 포트 허용 확인"
    echo "      - Hive Metastore EC2에서 서비스 실행 상태 확인"
fi

echo "   RDS PostgreSQL 연결 (${RDS_ENDPOINT}:5432):"
if timeout 5 bash -c "cat < /dev/null > /dev/tcp/${RDS_ENDPOINT}/5432" 2>/dev/null; then
    echo "   ✅ RDS PostgreSQL 포트 접근 가능"
else
    echo "   ❌ RDS PostgreSQL 포트 접근 불가"
    echo "      - RDS Security Group 5432 포트 허용 확인"
    echo "      - RDS 인스턴스 상태 확인"
fi

# 2. S3 접근 테스트
echo -e "\n2. S3 접근 테스트:"
if aws s3 ls s3://reciping-user-event-logs/ > /dev/null 2>&1; then
    echo "   ✅ S3 버킷 접근 가능"
    echo "   S3 Iceberg 경로 확인:"
    aws s3 ls s3://reciping-user-event-logs/iceberg/ --recursive | head -5 || echo "   (Iceberg 경로 없음 - 정상)"
else
    echo "   ❌ S3 버킷 접근 불가"
    echo "      - IAM 역할/정책 확인"
    echo "      - AWS 자격증명 확인"
fi

# 3. Spark 환경 확인
echo -e "\n3. Spark 환경 확인:"
if command -v spark-submit &> /dev/null; then
    echo "   ✅ spark-submit 사용 가능"
    echo "   Spark 버전: $(spark-submit --version 2>&1 | grep version | head -1 || echo 'Unknown')"
else
    echo "   ❌ spark-submit 찾을 수 없음"
    echo "      - Spark 설치 확인"
    echo "      - PATH 환경변수 확인"
fi

# 4. Python 및 PySpark 확인
echo -e "\n4. Python 환경 확인:"
python3 -c "
try:
    from pyspark.sql import SparkSession
    print('   ✅ PySpark 사용 가능')
    
    import boto3
    print('   ✅ boto3 사용 가능')
    
    # Spark 세션으로 Hive Metastore 연결 테스트 (간단한 테스트)
    spark = SparkSession.builder.appName('ConnectionTest').getOrCreate()
    print('   ✅ SparkSession 생성 가능')
    spark.stop()
    
except ImportError as e:
    print(f'   ❌ Python 패키지 누락: {e}')
except Exception as e:
    print(f'   ⚠️  Spark 연결 테스트 실패: {e}')
"

# 5. 디스크 및 메모리 상태
echo -e "\n5. 시스템 리소스:"
echo "   디스크 공간:"
df -h /home/ec2-user/ | head -2
echo "   메모리 상태:"
free -h

# 6. 현재 EC2 역할별 체크리스트 제공
echo -e "\n6. EC2 역할별 체크리스트:"
echo "   📋 Hive Metastore EC2에서 확인할 사항:"
echo "      - sudo systemctl status hive-metastore (또는 해당 서비스)"
echo "      - netstat -tlnp | grep 9083"
echo "      - tail -f /var/log/hive/hive-metastore.log"
echo
echo "   📋 RDS PostgreSQL에서 확인할 사항:"
echo "      - RDS 콘솔에서 인스턴스 상태 확인"
echo "      - Security Group에서 Hive Metastore EC2 IP 허용 확인"
echo "      - 'hive' 데이터베이스 존재 확인"
echo
echo "   📋 현재 EC2에서 실행할 ETL 작업:"
echo "      - Spark 작업은 여기서 실행"
echo "      - Hive Metastore와 RDS는 원격 접속"
echo "      - S3는 직접 접근"

echo -e "\n=== 진단 완료 ==="
echo
echo "🔍 문제 해결 가이드:"
echo "1. Hive Metastore 연결 실패시 → Hive Metastore EC2 로그인 후 서비스 확인"
echo "2. RDS 연결 실패시 → AWS 콘솔에서 RDS 상태 및 Security Group 확인"
echo "3. S3 접근 실패시 → IAM 역할 및 정책 확인"
echo "4. Spark 없음 → pip install pyspark 또는 Spark 설치"