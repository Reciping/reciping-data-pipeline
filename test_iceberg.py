#!/usr/bin/env python3
print('🧊 Iceberg 테스트 시작!')

try:
    from pyspark.sql import SparkSession
    print('✅ PySpark 임포트 성공')
    
    # Iceberg + Hive Metastore 설정을 포함한 SparkSession 생성
    spark = SparkSession.builder \
        .appName('IcebergTest') \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.iceberg_catalog.type', 'hive') \
        .config('spark.sql.catalog.iceberg_catalog.uri', 'thrift://metastore:9083') \
        .config('spark.sql.catalog.iceberg_catalog.warehouse', 's3a://reciping-user-event-logs/iceberg/warehouse/') \
        .config('spark.sql.catalog.iceberg_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO') \
        .config('spark.hadoop.fs.s3a.access.key', '') \
        .config('spark.hadoop.fs.s3a.secret.key', '') \
        .getOrCreate()
    
    print('✅ Spark + Iceberg + Hive Metastore 세션 생성 성공!')
    
    # 카탈로그 확인
    spark.sql('SHOW CATALOGS').show()
    
    spark.stop()
    print('�� Iceberg 테스트 완료!')
    
except Exception as e:
    print(f'❌ 오류 발생: {e}')
    import traceback
    traceback.print_exc()
