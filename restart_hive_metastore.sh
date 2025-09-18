#!/bin/bash
# Hive Metastore 재시작 스크립트

echo "=== Hive Metastore 재시작 ==="

# 기존 Metastore 프로세스 종료
echo "1. 기존 Hive Metastore 프로세스 종료..."
pkill -f HiveMetaStore
sleep 3

# PostgreSQL 재시작 (Metastore 백엔드)
echo "2. PostgreSQL 재시작..."
sudo systemctl restart postgresql
sleep 5

# PostgreSQL 상태 확인
if ! systemctl is-active --quiet postgresql; then
    echo "❌ PostgreSQL 시작 실패"
    exit 1
fi
echo "✅ PostgreSQL 재시작 완료"

# Hive Metastore 데이터베이스 스키마 초기화 (필요시)
echo "3. Hive Metastore 스키마 확인/초기화..."

# Hive 환경변수 설정 (실제 경로에 맞게 수정 필요)
export HIVE_HOME=/opt/hive  # 실제 Hive 설치 경로로 변경
export HADOOP_HOME=/opt/hadoop  # 실제 Hadoop 설치 경로로 변경
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # 실제 Java 경로로 변경

# PATH 설정
export PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

# Metastore 스키마 초기화 (처음 실행시에만 필요)
# 주의: 이 명령은 기존 메타데이터를 모두 삭제합니다!
echo "Metastore 스키마를 초기화하시겠습니까? (기존 메타데이터가 모두 삭제됩니다)"
read -p "y/N: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "스키마 초기화 중..."
    $HIVE_HOME/bin/schematool -dbType postgres -initSchema
    if [ $? -eq 0 ]; then
        echo "✅ 스키마 초기화 완료"
    else
        echo "❌ 스키마 초기화 실패"
    fi
fi

# Hive Metastore 서비스 시작
echo "4. Hive Metastore 서비스 시작..."
nohup $HIVE_HOME/bin/hive --service metastore > /var/log/hive-metastore.log 2>&1 &

# 서비스 시작 대기
sleep 10

# 상태 확인
if pgrep -f "HiveMetaStore" > /dev/null; then
    echo "✅ Hive Metastore 시작 완료"
    echo "로그 확인: tail -f /var/log/hive-metastore.log"
else
    echo "❌ Hive Metastore 시작 실패"
    echo "로그 확인: cat /var/log/hive-metastore.log"
    exit 1
fi

# 포트 확인
sleep 5
if netstat -tlnp | grep :9083 > /dev/null; then
    echo "✅ 포트 9083에서 리스닝 중"
else
    echo "❌ 포트 9083 리스닝 실패"
    exit 1
fi

echo "=== Hive Metastore 재시작 완료 ==="