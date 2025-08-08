#!/bin/bash

echo "🔧 Hive Metastore S3 지원 초기화 시작..."

# curl 사용 가능 확인 (wget 대신)
if ! command -v curl &> /dev/null; then
    echo "❌ curl 명령어를 찾을 수 없습니다."
    exit 1
fi

# S3 JAR 파일들을 기존 lib 디렉토리에 다운로드
cd /opt/hive/lib

echo "📦 AWS S3 JAR 파일들 다운로드 중..."

# AWS SDK Bundle
echo "  - AWS SDK Bundle 다운로드..."
curl -s -L -o aws-java-sdk-bundle-1.12.262.jar 
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Hadoop AWS
echo "  - Hadoop AWS 다운로드..."
curl -s -L -o hadoop-aws-3.3.4.jar 
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Wildfly OpenSSL (SSL/TLS 지원)
echo "  - Wildfly OpenSSL 다운로드..."
curl -s -L -o wildfly-openssl-1.0.7.Final.jar 
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

echo "✅ JAR 파일 다운로드 완료!"

# 다운로드된 파일들 확인
echo "� 다운로드된 S3 JAR 파일들:"
ls -la /opt/hive/lib/*aws* /opt/hive/lib/*wildfly* 2>/dev/null || echo "  일부 파일 다운로드 실패"

echo "🎉 Hive Metastore S3 초기화 완료!"

echo "ℹ️  S3 JAR 파일들이 Hive classpath에 자동으로 포함됩니다."
