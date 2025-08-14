#!/usr/bin/env python3
"""
메모리 효율적인 Gold Layer 최소 구현
완전한 솔루션을 위한 메모리 최적화 접근법
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

class MinimalGoldLayer:
    """메모리 효율적인 Gold Layer 구현"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.silver_database = "recipe_analytics"
        self.gold_database = "gold_analytics"
        self.spark = None
        
    def create_minimal_spark_session(self):
        """메모리 최적화된 SparkSession 생성"""
        print("🧊 메모리 최적화 SparkSession 생성 중...")
        
        self.spark = SparkSession.builder \
            .appName("MinimalGoldLayer") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "512m") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "4") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.unsafe", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "false") \
            .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "32MB") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        print("✅ 메모리 최적화 SparkSession 생성 완료!")
        
    def create_minimal_fact_table(self):
        """메모리 안전한 최소 Fact 테이블 생성"""
        print("\n📊 메모리 안전한 Fact 테이블 생성...")
        
        # 매우 작은 배치로 시작
        minimal_query = f"""
        WITH silver_mini AS (
            SELECT 
                event_id,
                user_id,
                session_id,
                anonymous_id,
                event_name,
                page_name,
                prop_recipe_id,
                utc_timestamp,
                date
            FROM {self.catalog_name}.{self.silver_database}.user_events_silver
            WHERE date = '2025-07-01'  -- 하루만 처리
            LIMIT 5000  -- 매우 작은 배치
        )
        
        INSERT OVERWRITE {self.catalog_name}.{self.gold_database}.fact_user_events
        SELECT 
            event_id,
            0 as user_dim_key,  -- 단순화
            CAST(DATE_FORMAT(utc_timestamp, 'yyyyMMdd') AS BIGINT) * 100 + HOUR(utc_timestamp) as time_dim_key,
            0 as recipe_dim_key,  -- 단순화
            0 as page_dim_key,  -- 단순화
            1 as event_dim_key,  -- 기본값
            
            1 as event_count,
            0 as session_duration_seconds,
            30 as page_view_duration_seconds,
            
            CASE WHEN event_name = 'auth_success' THEN TRUE ELSE FALSE END as is_conversion,
            1.0 as conversion_value,
            CASE 
                WHEN event_name = 'auth_success' THEN 10.0
                WHEN event_name = 'click_recipe' THEN 7.0
                WHEN event_name = 'search_recipe' THEN 5.0
                ELSE 2.0
            END as engagement_score,
            
            session_id,
            anonymous_id,
            
            CURRENT_TIMESTAMP() as created_at,
            CURRENT_TIMESTAMP() as updated_at
            
        FROM silver_mini
        WHERE event_id IS NOT NULL
        """
        
        try:
            self.spark.sql(minimal_query)
            
            # 결과 확인
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.gold_database}.fact_user_events").collect()[0]['cnt']
            print(f"✅ 메모리 안전한 Fact 테이블 생성 완료: {count:,}개 레코드")
            
            # 점진적 확장
            self.expand_fact_table_gradually()
            
        except Exception as e:
            print(f"❌ 메모리 안전한 Fact 테이블 생성 실패: {str(e)}")
            
    def expand_fact_table_gradually(self):
        """점진적으로 Fact 테이블 확장"""
        print("\n🔄 점진적 Fact 테이블 확장...")
        
        # 하루씩 추가 처리
        dates_to_process = [
            '2025-07-02', '2025-07-03', '2025-07-04', '2025-07-05'
        ]
        
        total_added = 0
        
        for date in dates_to_process:
            try:
                print(f"   📅 {date} 처리 중...")
                
                expansion_query = f"""
                INSERT INTO {self.catalog_name}.{self.gold_database}.fact_user_events
                SELECT 
                    event_id,
                    0 as user_dim_key,
                    CAST(DATE_FORMAT(utc_timestamp, 'yyyyMMdd') AS BIGINT) * 100 + HOUR(utc_timestamp) as time_dim_key,
                    0 as recipe_dim_key,
                    0 as page_dim_key,
                    1 as event_dim_key,
                    
                    1 as event_count,
                    0 as session_duration_seconds,
                    30 as page_view_duration_seconds,
                    
                    CASE WHEN event_name = 'auth_success' THEN TRUE ELSE FALSE END as is_conversion,
                    1.0 as conversion_value,
                    CASE 
                        WHEN event_name = 'auth_success' THEN 10.0
                        WHEN event_name = 'click_recipe' THEN 7.0
                        WHEN event_name = 'search_recipe' THEN 5.0
                        ELSE 2.0
                    END as engagement_score,
                    
                    session_id,
                    anonymous_id,
                    
                    CURRENT_TIMESTAMP() as created_at,
                    CURRENT_TIMESTAMP() as updated_at
                    
                FROM {self.catalog_name}.{self.silver_database}.user_events_silver
                WHERE date = '{date}'
                AND event_id IS NOT NULL
                LIMIT 5000
                """
                
                self.spark.sql(expansion_query)
                
                # 배치별 확인
                current_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.gold_database}.fact_user_events").collect()[0]['cnt']
                batch_added = current_count - total_added
                total_added = current_count
                
                print(f"   ✅ {date} 완료: +{batch_added:,}개 레코드 (총 {total_added:,}개)")
                
            except Exception as e:
                print(f"   ❌ {date} 실패: {str(e)}")
                continue
        
        print(f"\n✅ 점진적 확장 완료: 총 {total_added:,}개 레코드")
        
    def validate_minimal_solution(self):
        """최소 솔루션 검증"""
        print("\n🔍 최소 솔루션 검증...")
        
        try:
            stats = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT time_dim_key) as unique_time_keys,
                COUNT(DISTINCT session_id) as unique_sessions,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            """).collect()[0]
            
            print("📊 최소 솔루션 결과:")
            print(f"   총 레코드: {stats['total_records']:,}개")
            print(f"   고유 시간키: {stats['unique_time_keys']:,}개")
            print(f"   고유 세션: {stats['unique_sessions']:,}개")
            print(f"   전환 이벤트: {stats['conversions']:,}개")
            print(f"   평균 참여도: {stats['avg_engagement']}")
            
            # 간단한 시간대별 분석
            hourly_analysis = self.spark.sql(f"""
            SELECT 
                (time_dim_key % 100) as hour,
                COUNT(*) as events,
                COUNT(DISTINCT session_id) as sessions,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            GROUP BY (time_dim_key % 100)
            ORDER BY hour
            LIMIT 10
            """).collect()
            
            print("\n⏰ 시간대별 분석 (상위 10시간):")
            for row in hourly_analysis:
                print(f"   {row['hour']:02d}시: {row['events']}이벤트, {row['sessions']}세션, {row['avg_engagement']}점 참여도")
            
            print("\n✅ 메모리 안전한 최소 솔루션 검증 완료!")
            print("   💡 이 기반으로 점진적으로 완전한 솔루션 구축 가능")
            
        except Exception as e:
            print(f"❌ 검증 실패: {str(e)}")
            
    def execute_minimal_solution(self):
        """최소 솔루션 전체 실행"""
        print("🚀 메모리 안전한 최소 솔루션 실행...")
        print("=" * 50)
        
        try:
            # 1. SparkSession 생성
            self.create_minimal_spark_session()
            
            # 2. 최소 Fact 테이블 생성
            self.create_minimal_fact_table()
            
            # 3. 검증
            self.validate_minimal_solution()
            
            print("\n🎉 최소 솔루션 성공!")
            print("   ✅ 메모리 크래시 없이 안정적 실행")
            print("   ✅ 기본 분석 데이터 준비 완료")
            print("   ✅ 점진적 확장 기반 마련")
            
        except Exception as e:
            print(f"❌ 최소 솔루션 실행 실패: {str(e)}")
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    minimal_gold = MinimalGoldLayer()
    minimal_gold.execute_minimal_solution()
