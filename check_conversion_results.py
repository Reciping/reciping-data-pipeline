#!/usr/bin/env python3
"""
Conversion Rate 메트릭 결과 확인 스크립트
"""

from pyspark.sql import SparkSession
import sys

def main():
    print("🚀 Conversion Rate 메트릭 결과 확인 시작!")
    
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("CheckConversionRate") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .getOrCreate()
    
    try:
        print("\n📊 1. 메트릭 테이블 목록 확인")
        tables_df = spark.sql("SHOW TABLES IN iceberg_catalog.recipe_analytics")
        print("테이블 목록:")
        tables_df.filter(tables_df.tableName.startswith("metrics_")).show(20, False)
        
        print("\n📈 2. Conversion Rate 메트릭 데이터 확인")
        conversion_df = spark.sql("""
        SELECT 
            date,
            funnel_stage,
            total_users,
            converted_users,
            conversion_rate,
            benchmark_rate,
            improvement_target
        FROM iceberg_catalog.recipe_analytics.metrics_conversion_rate 
        ORDER BY date DESC, funnel_stage 
        LIMIT 15
        """)
        
        print("Conversion Rate 메트릭 결과:")
        conversion_df.show(15, False)
        
        print("\n📊 3. 통계 요약")
        total_count = spark.sql("SELECT COUNT(*) as total FROM iceberg_catalog.recipe_analytics.metrics_conversion_rate").collect()[0][0]
        print(f"총 레코드 수: {total_count}")
        
        funnel_stages = spark.sql("""
        SELECT funnel_stage, COUNT(*) as count, AVG(conversion_rate) as avg_rate
        FROM iceberg_catalog.recipe_analytics.metrics_conversion_rate 
        GROUP BY funnel_stage 
        ORDER BY funnel_stage
        """)
        print("\n퍼널 단계별 통계:")
        funnel_stages.show(10, False)
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False
    
    finally:
        spark.stop()
    
    print("✅ Conversion Rate 메트릭 확인 완료!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
