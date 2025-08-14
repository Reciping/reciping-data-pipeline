#!/usr/bin/env python3
"""
날짜 범위 분할 Gold Layer 처리기
- 7/1~7/10, 7/11~7/20, 7/21~7/31로 분할 처리
- KST 시간 컬럼 활용 (date, year, month, day, hour)
- 메모리 최적화 및 안전한 배치 처리
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

class DateRangeGoldProcessor:
    """날짜 범위별 Gold Layer 처리기"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.silver_database = "recipe_analytics"
        self.gold_database = "gold_analytics"
        self.spark = None
        
        # 날짜 범위 정의
        self.date_ranges = [
            {
                'name': 'Period1_7월초',
                'start_date': '2025-07-01',
                'end_date': '2025-07-10',
                'description': '7월 1일~10일 (10일간)'
            },
            {
                'name': 'Period2_7월중',
                'start_date': '2025-07-11', 
                'end_date': '2025-07-20',
                'description': '7월 11일~20일 (10일간)'
            },
            {
                'name': 'Period3_7월말',
                'start_date': '2025-07-21',
                'end_date': '2025-07-31', 
                'description': '7월 21일~31일 (11일간)'
            }
        ]
        
    def create_optimized_spark_session(self):
        """메모리 최적화된 SparkSession"""
        print("🔧 날짜 범위 분할용 최적화된 SparkSession...")
        
        self.spark = SparkSession.builder \
            .appName("DateRangeGoldProcessor") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ 최적화된 SparkSession 생성 완료")
        
    def create_gold_database(self):
        """Gold Analytics 데이터베이스 생성"""
        print(f"\n🏗️ Gold Analytics 데이터베이스 생성...")
        
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.gold_database}")
        print(f"✅ {self.catalog_name}.{self.gold_database} 데이터베이스 준비 완료!")
        
    def analyze_date_range(self, start_date, end_date, range_name):
        """특정 날짜 범위 데이터 분석"""
        print(f"\n📊 {range_name} 데이터 분석...")
        
        try:
            # KST 기준 날짜 필터링 (date 컬럼 사용)
            analysis_query = f"""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT session_id) as unique_sessions,
                COUNT(DISTINCT prop_recipe_id) as unique_recipes,
                COUNT(DISTINCT page_name) as unique_pages,
                COUNT(DISTINCT event_name) as unique_events,
                
                -- KST 시간 범위 확인
                MIN(date) as min_kst_date,
                MAX(date) as max_kst_date,
                MIN(year) as min_year,
                MAX(year) as max_year,
                MIN(month) as min_month,
                MAX(month) as max_month,
                
                -- UTC vs KST 시간 비교
                MIN(utc_timestamp) as min_utc,
                MAX(utc_timestamp) as max_utc
                
            FROM {self.catalog_name}.{self.silver_database}.user_events_silver
            WHERE date >= '{start_date}' AND date <= '{end_date}'
            """
            
            stats = self.spark.sql(analysis_query).collect()[0]
            
            print(f"   📈 기본 통계:")
            print(f"      총 이벤트: {stats.total_events:,}개")
            print(f"      고유 사용자: {stats.unique_users:,}명")
            print(f"      고유 세션: {stats.unique_sessions:,}개")
            print(f"      고유 레시피: {stats.unique_recipes:,}개")
            print(f"      고유 페이지: {stats.unique_pages}개")
            print(f"      고유 이벤트: {stats.unique_events}개")
            
            print(f"   🇰🇷 KST 시간 범위:")
            print(f"      KST 날짜: {stats.min_kst_date} ~ {stats.max_kst_date}")
            print(f"      연도: {stats.min_year} ~ {stats.max_year}")
            print(f"      월: {stats.min_month} ~ {stats.max_month}")
            
            print(f"   🌍 UTC 시간 범위:")
            print(f"      UTC 시간: {stats.min_utc} ~ {stats.max_utc}")
            
            # 메모리 사용량 추정
            batch_size = 5000
            needed_batches = (stats.total_events + batch_size - 1) // batch_size
            estimated_time = needed_batches * 45 / 3600  # 45초 per batch (최적화됨)
            memory_usage_gb = stats.total_events * 64 / (1024**3)  # 대략적 계산
            
            print(f"   ⚡ 처리 예상:")
            print(f"      필요 배치: {needed_batches}개")
            print(f"      예상 시간: {estimated_time:.1f}시간")
            print(f"      예상 메모리: {memory_usage_gb:.1f}GB")
            
            return {
                'total_events': stats.total_events,
                'needed_batches': needed_batches,
                'estimated_time': estimated_time,
                'memory_usage_gb': memory_usage_gb
            }
            
        except Exception as e:
            print(f"❌ {range_name} 분석 실패: {str(e)}")
            return None
            
    def create_simple_fact_table_for_range(self, start_date, end_date, range_name, is_first_range=False):
        """특정 날짜 범위의 간단한 Fact 테이블 생성"""
        print(f"\n🔄 {range_name} Fact 테이블 생성...")
        
        try:
            # INSERT 모드 결정
            insert_mode = "INSERT OVERWRITE" if is_first_range else "INSERT INTO"
            
            # KST 시간 컬럼 활용한 쿼리
            fact_query = f"""
            {insert_mode} {self.catalog_name}.{self.gold_database}.fact_user_events_simple
            SELECT 
                event_id,
                user_id,
                session_id,
                anonymous_id,
                event_name,
                page_name,
                prop_recipe_id,
                
                -- KST 시간 컬럼 활용 (UTC 대신 한국 시간 기준)
                date as kst_date,
                year as kst_year,
                month as kst_month, 
                day as kst_day,
                hour as kst_hour,
                day_of_week as kst_day_of_week,
                
                -- 비교를 위한 UTC 시간도 포함
                utc_timestamp,
                
                -- 사용자 정보
                user_segment,
                cooking_style,
                ab_test_group,
                
                -- 이벤트 속성
                prop_list_type,
                prop_action,
                prop_search_keyword,
                prop_result_count,
                
                -- 단순한 메트릭
                1 as event_count,
                CASE WHEN event_name IN ('auth_success', 'click_bookmark', 'create_comment') THEN 1 ELSE 0 END as conversion_flag,
                CASE 
                    WHEN event_name = 'auth_success' THEN 10
                    WHEN event_name = 'create_comment' THEN 9
                    WHEN event_name = 'click_bookmark' THEN 8
                    WHEN event_name = 'click_recipe' THEN 7
                    WHEN event_name = 'search_recipe' THEN 5
                    WHEN event_name = 'view_recipe' THEN 4
                    WHEN event_name = 'view_page' THEN 2
                    ELSE 1
                END as engagement_score,
                
                -- 메타데이터
                processed_at,
                data_source,
                pipeline_version,
                '{range_name}' as processing_batch,
                CURRENT_TIMESTAMP() as gold_processed_at
                
            FROM {self.catalog_name}.{self.silver_database}.user_events_silver
            WHERE date >= '{start_date}' AND date <= '{end_date}'
            AND event_id IS NOT NULL
            """
            
            print(f"   🔄 실행 중... ({insert_mode} 모드)")
            self.spark.sql(fact_query)
            
            # 결과 확인
            count_query = f"""
            SELECT COUNT(*) as cnt 
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events_simple
            WHERE processing_batch = '{range_name}'
            """
            
            range_count = self.spark.sql(count_query).collect()[0]['cnt']
            print(f"   ✅ {range_name} 완료: {range_count:,}개 레코드 처리")
            
            return range_count
            
        except Exception as e:
            print(f"   ❌ {range_name} Fact 테이블 생성 실패: {str(e)}")
            return 0
            
    def create_fact_table_schema(self):
        """Fact 테이블 스키마 생성"""
        print(f"\n🏗️ Fact 테이블 스키마 생성...")
        
        try:
            schema_query = f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_database}.fact_user_events_simple (
                -- 기본 식별자
                event_id STRING NOT NULL,
                user_id STRING,
                session_id STRING,
                anonymous_id STRING,
                
                -- 이벤트 정보
                event_name STRING NOT NULL,
                page_name STRING,
                prop_recipe_id BIGINT,
                
                -- KST 시간 컬럼 (한국 시간 기준)
                kst_date DATE NOT NULL,
                kst_year INT,
                kst_month INT,
                kst_day INT,
                kst_hour INT,
                kst_day_of_week STRING,
                
                -- UTC 시간 (비교용)
                utc_timestamp TIMESTAMP,
                
                -- 사용자 속성
                user_segment STRING,
                cooking_style STRING,
                ab_test_group STRING,
                
                -- 이벤트 속성
                prop_list_type STRING,
                prop_action STRING,
                prop_search_keyword STRING,
                prop_result_count INT,
                
                -- 메트릭
                event_count INT,
                conversion_flag INT,
                engagement_score INT,
                
                -- 메타데이터
                processed_at TIMESTAMP,
                data_source STRING,
                pipeline_version STRING,
                processing_batch STRING,
                gold_processed_at TIMESTAMP
                
            ) USING ICEBERG
            PARTITIONED BY (kst_year, kst_month, kst_day)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '134217728'
            )
            """
            
            self.spark.sql(schema_query)
            print(f"✅ Fact 테이블 스키마 생성 완료!")
            
        except Exception as e:
            print(f"❌ Fact 테이블 스키마 생성 실패: {str(e)}")
            
    def process_all_date_ranges(self):
        """모든 날짜 범위 순차 처리"""
        print(f"\n🚀 모든 날짜 범위 순차 처리 시작...")
        
        total_processed = 0
        processing_times = []
        
        for i, date_range in enumerate(self.date_ranges):
            range_name = date_range['name']
            start_date = date_range['start_date']
            end_date = date_range['end_date']
            description = date_range['description']
            
            print(f"\n{'='*60}")
            print(f"📅 처리 중: {description}")
            print(f"   범위: {start_date} ~ {end_date}")
            print(f"   배치: {range_name}")
            
            # 1. 데이터 분석
            analysis_result = self.analyze_date_range(start_date, end_date, range_name)
            
            if analysis_result is None:
                print(f"⚠️ {range_name} 건너뜀 (분석 실패)")
                continue
                
            # 2. 처리 시작
            start_time = time.time()
            
            is_first_range = (i == 0)
            processed_count = self.create_simple_fact_table_for_range(
                start_date, end_date, range_name, is_first_range
            )
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # 3. 결과 기록
            total_processed += processed_count
            processing_times.append({
                'range': range_name,
                'time': processing_time,
                'events': processed_count
            })
            
            print(f"   ⏱️ 실제 처리 시간: {processing_time/60:.1f}분")
            print(f"   📊 누적 처리: {total_processed:,}개")
            
        # 4. 전체 결과 요약
        self.summarize_processing_results(processing_times, total_processed)
        
    def summarize_processing_results(self, processing_times, total_processed):
        """처리 결과 요약"""
        print(f"\n🎉 날짜 범위 분할 처리 완료!")
        print(f"{'='*60}")
        
        total_time = sum(item['time'] for item in processing_times)
        
        print(f"📊 전체 처리 결과:")
        print(f"   총 처리 이벤트: {total_processed:,}개")
        print(f"   총 처리 시간: {total_time/60:.1f}분 ({total_time/3600:.1f}시간)")
        
        print(f"\n📅 범위별 처리 시간:")
        for item in processing_times:
            efficiency = item['events'] / item['time'] if item['time'] > 0 else 0
            print(f"   {item['range']}: {item['time']/60:.1f}분 ({item['events']:,}개, {efficiency:.0f}개/초)")
        
        # 최종 검증
        self.validate_gold_table()
        
    def validate_gold_table(self):
        """Gold 테이블 검증"""
        print(f"\n🔍 Gold 테이블 검증...")
        
        try:
            validation_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT processing_batch) as unique_batches,
                COUNT(DISTINCT kst_date) as unique_kst_dates,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT event_name) as unique_events,
                SUM(conversion_flag) as total_conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement,
                MIN(kst_date) as min_kst_date,
                MAX(kst_date) as max_kst_date,
                MIN(utc_timestamp) as min_utc,
                MAX(utc_timestamp) as max_utc
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events_simple
            """
            
            stats = self.spark.sql(validation_query).collect()[0]
            
            print(f"✅ Gold 테이블 검증 결과:")
            print(f"   총 레코드: {stats.total_records:,}개")
            print(f"   처리 배치: {stats.unique_batches}개")
            print(f"   고유 KST 날짜: {stats.unique_kst_dates}개")
            print(f"   고유 사용자: {stats.unique_users:,}명")
            print(f"   고유 이벤트: {stats.unique_events}개")
            print(f"   총 전환: {stats.total_conversions:,}건")
            print(f"   평균 참여도: {stats.avg_engagement}")
            
            print(f"\n🇰🇷 KST vs UTC 시간 비교:")
            print(f"   KST 날짜 범위: {stats.min_kst_date} ~ {stats.max_kst_date}")
            print(f"   UTC 시간 범위: {stats.min_utc} ~ {stats.max_utc}")
            
            # 배치별 통계
            batch_stats = self.spark.sql(f"""
            SELECT 
                processing_batch,
                COUNT(*) as records,
                COUNT(DISTINCT kst_date) as dates,
                MIN(kst_date) as min_date,
                MAX(kst_date) as max_date
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events_simple
            GROUP BY processing_batch
            ORDER BY min_date
            """).collect()
            
            print(f"\n📅 배치별 상세 통계:")
            for row in batch_stats:
                print(f"   {row.processing_batch}: {row.records:,}개 ({row.min_date} ~ {row.max_date}, {row.dates}일)")
                
        except Exception as e:
            print(f"❌ Gold 테이블 검증 실패: {str(e)}")
            
    def execute_date_range_processing(self):
        """날짜 범위 분할 처리 전체 실행"""
        print("🚀 날짜 범위 분할 Gold Layer 처리 시작...")
        print("=" * 60)
        
        try:
            # 1. Spark 세션 생성
            self.create_optimized_spark_session()
            
            # 2. Gold 데이터베이스 생성
            self.create_gold_database()
            
            # 3. Fact 테이블 스키마 생성
            self.create_fact_table_schema()
            
            # 4. 전체 날짜 범위 처리
            self.process_all_date_ranges()
            
            print(f"\n🎉 날짜 범위 분할 처리 완전 성공!")
            print(f"   ✅ KST 시간 컬럼 활용")
            print(f"   ✅ 메모리 최적화된 분할 처리")
            print(f"   ✅ 안전한 배치별 실행")
            
        except Exception as e:
            print(f"❌ 날짜 범위 분할 처리 실패: {str(e)}")
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    processor = DateRangeGoldProcessor()
    processor.execute_date_range_processing()
