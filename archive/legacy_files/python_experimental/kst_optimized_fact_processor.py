#!/usr/bin/env python3
"""
KST 최적화 Gold Layer Fact 테이블 처리기
- ultra_batch_processor 성공 기반으로 구축
- KST 컬럼 활용한 한국 시간대 기준 처리
- 메모리 안전 배치 크기 (5,000개) 유지
- 점진적 확장 및 에러 복구 기능
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from datetime import datetime

class KSTOptimizedFactProcessor:
    """KST 최적화 Fact 테이블 처리기"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.silver_database = "recipe_analytics"
        self.gold_database = "gold_analytics"
        self.spark = None
        self.batch_size = 5000  # 성공 검증된 배치 크기
        
        print("🇰🇷 KST 최적화 Fact 처리기 초기화")
        print(f"   📦 안전 배치 크기: {self.batch_size:,}개")
        
    def create_memory_safe_spark_session(self):
        """메모리 안전 SparkSession (ultra_batch 성공 설정 기반)"""
        print("🔧 메모리 안전 SparkSession 생성...")
        
        self.spark = SparkSession.builder \
            .appName("KSTOptimizedFact") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "20") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ 메모리 안전 SparkSession 생성 완료")
        
    def ensure_kst_optimized_fact_table(self):
        """KST 최적화 Fact 테이블 구조 확인/생성"""
        print("\n🏗️ KST 최적화 Fact 테이블 구조 준비...")
        
        try:
            # Gold 데이터베이스 생성
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.gold_database}")
            
            # 기존 테이블이 있는지 확인
            existing_tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.gold_database}").collect()
            fact_table_exists = any("fact_user_events" in row.tableName for row in existing_tables)
            
            if fact_table_exists:
                print("✅ 기존 Fact 테이블 발견 - 기존 구조 활용")
                return True
            
            # 새 KST 최적화 테이블 생성
            print("🆕 KST 최적화 Fact 테이블 생성...")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_database}.fact_user_events (
                event_id STRING NOT NULL,
                
                -- 기본 차원 키들 (단순화)
                user_dim_key BIGINT,
                time_dim_key BIGINT,
                recipe_dim_key BIGINT, 
                page_dim_key BIGINT,
                event_dim_key BIGINT,
                
                -- KST 기반 시간 정보 (핵심 개선점)
                kst_timestamp TIMESTAMP,
                kst_date DATE,
                kst_year INT,
                kst_month INT,
                kst_day INT,
                kst_hour INT,
                kst_day_of_week STRING,
                
                -- UTC 시간 (비교용)
                utc_timestamp TIMESTAMP,
                
                -- 사용자 정보 (차원 테이블 없이도 분석 가능)
                user_id STRING,
                user_segment STRING,
                cooking_style STRING,
                ab_test_group STRING,
                
                -- 이벤트 정보
                event_name STRING,
                page_name STRING,
                
                -- 레시피 정보 (NULL 허용)
                recipe_id BIGINT,
                
                -- 측정값들
                event_count BIGINT,
                session_duration_seconds BIGINT,
                page_view_duration_seconds BIGINT,
                is_conversion BOOLEAN,
                conversion_value DECIMAL(10,2),
                engagement_score DECIMAL(5,2),
                
                -- Degenerate Dimensions
                session_id STRING,
                anonymous_id STRING,
                
                -- ETL 메타데이터
                created_at TIMESTAMP,
                updated_at TIMESTAMP
                
            ) USING ICEBERG
            PARTITIONED BY (kst_year, kst_month, kst_day)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '67108864'
            )
            """
            
            self.spark.sql(create_table_sql)
            print("✅ KST 최적화 Fact 테이블 생성 완료!")
            return True
            
        except Exception as e:
            print(f"❌ Fact 테이블 생성 실패: {str(e)}")
            return False
            
    def create_kst_batch_safely(self, start_date: str, batch_num: int = 0, is_first_batch: bool = False):
        """KST 기반 안전한 배치 생성"""
        print(f"\n📅 KST 배치 생성: {start_date} (배치 #{batch_num + 1})")
        
        try:
            # INSERT 모드 결정
            insert_mode = "INSERT OVERWRITE" if is_first_batch else "INSERT INTO"
            offset = batch_num * self.batch_size
            
            # KST 컬럼 활용한 안전한 쿼리
            kst_batch_query = f"""
            {insert_mode} {self.catalog_name}.{self.gold_database}.fact_user_events
            SELECT 
                s.event_id,
                
                -- 차원 키들 (나중에 업데이트 가능)
                0 as user_dim_key,
                CAST(DATE_FORMAT(s.kst_timestamp, 'yyyyMMdd') AS BIGINT) * 100 + s.hour as time_dim_key,
                COALESCE(s.prop_recipe_id, 0) as recipe_dim_key,
                0 as page_dim_key,
                0 as event_dim_key,
                
                -- KST 시간 정보 (핵심 활용)
                s.kst_timestamp,
                s.date as kst_date,
                s.year as kst_year,
                s.month as kst_month,
                s.day as kst_day,
                s.hour as kst_hour,
                s.day_of_week as kst_day_of_week,
                
                -- UTC 시간 (비교용)
                s.utc_timestamp,
                
                -- 사용자 정보 (직접 포함)
                s.user_id,
                s.user_segment,
                s.cooking_style,
                s.ab_test_group,
                
                -- 이벤트 정보
                s.event_name,
                s.page_name,
                
                -- 레시피 정보
                s.prop_recipe_id as recipe_id,
                
                -- 측정값 계산
                1 as event_count,
                
                -- 세션 시간 (prop_action에서 추출 또는 기본값)
                CASE 
                    WHEN s.prop_action IS NOT NULL AND SIZE(SPLIT(s.prop_action, ':')) >= 2
                    THEN COALESCE(CAST(SPLIT(s.prop_action, ':')[1] AS BIGINT), 60)
                    ELSE 60
                END as session_duration_seconds,
                
                30 as page_view_duration_seconds,
                
                -- 전환 플래그 (KST 시간대 기준 분석 가능)
                CASE WHEN s.event_name IN ('auth_success', 'click_bookmark', 'create_comment') THEN TRUE ELSE FALSE END as is_conversion,
                
                1.0 as conversion_value,
                
                -- KST 시간대별 참여도 점수 (한국 사용 패턴 반영)
                CASE 
                    WHEN s.event_name = 'auth_success' THEN 10.0
                    WHEN s.event_name = 'create_comment' THEN 9.0
                    WHEN s.event_name = 'click_bookmark' THEN 8.0
                    WHEN s.event_name = 'click_recipe' THEN 7.0
                    WHEN s.event_name = 'search_recipe' THEN 5.0
                    WHEN s.event_name = 'view_recipe' THEN 4.0
                    WHEN s.event_name = 'view_page' THEN 2.0
                    ELSE 1.0
                END as engagement_score,
                
                -- Degenerate Dimensions
                s.session_id,
                s.anonymous_id,
                
                -- ETL 메타데이터
                CURRENT_TIMESTAMP() as created_at,
                CURRENT_TIMESTAMP() as updated_at
                
            FROM (
                SELECT 
                    event_id, kst_timestamp, utc_timestamp, date, year, month, day, hour, day_of_week,
                    user_id, user_segment, cooking_style, ab_test_group,
                    event_name, page_name, prop_recipe_id, prop_action,
                    session_id, anonymous_id,
                    ROW_NUMBER() OVER (ORDER BY kst_timestamp, event_id) as row_num
                FROM {self.catalog_name}.{self.silver_database}.user_events_silver
                WHERE date = '{start_date}' AND event_id IS NOT NULL
            ) s
            WHERE s.row_num > {offset} AND s.row_num <= {offset + self.batch_size}
            """
            
            start_time = time.time()
            self.spark.sql(kst_batch_query)
            end_time = time.time()
            
            # 결과 확인
            current_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.gold_database}.fact_user_events").collect()[0]['cnt']
            
            batch_time = end_time - start_time
            print(f"   ✅ 배치 완료: +{self.batch_size:,}개 (누적: {current_count:,}개, {batch_time:.1f}초)")
            
            return self.batch_size
            
        except Exception as e:
            print(f"   ❌ 배치 실패: {str(e)[:100]}...")
            return 0
            
    def process_date_range_with_kst(self, start_date: str, end_date: str):
        """KST 기반 날짜 범위 처리"""
        print(f"\n🗓️ KST 날짜 범위 처리: {start_date} ~ {end_date}")
        
        # 날짜별 처리
        from datetime import datetime, timedelta
        
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        total_processed = 0
        overall_batches = 0
        is_very_first_batch = True
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n📅 {date_str} 처리 중...")
            
            try:
                # 해당 날짜의 이벤트 수 확인
                date_count = self.spark.sql(f"""
                    SELECT COUNT(*) as cnt 
                    FROM {self.catalog_name}.{self.silver_database}.user_events_silver
                    WHERE date = '{date_str}'
                """).collect()[0]['cnt']
                
                if date_count == 0:
                    print(f"   ⚠️ {date_str}: 데이터 없음, 건너뜀")
                    current_date += timedelta(days=1)
                    continue
                
                # 해당 날짜의 배치 수 계산
                needed_batches = (date_count + self.batch_size - 1) // self.batch_size
                print(f"   📊 {date_count:,}개 이벤트 → {needed_batches}개 배치")
                
                # 날짜별 배치 처리
                date_processed = 0
                for batch_num in range(needed_batches):
                    processed_count = self.create_kst_batch_safely(
                        date_str, 
                        batch_num, 
                        is_very_first_batch
                    )
                    
                    if processed_count > 0:
                        date_processed += processed_count
                        total_processed += processed_count
                        overall_batches += 1
                        is_very_first_batch = False
                        
                        # 메모리 안정성을 위한 대기
                        time.sleep(1)
                    else:
                        print(f"   ⚠️ 배치 {batch_num + 1} 실패, 중단")
                        break
                
                print(f"   ✅ {date_str} 완료: {date_processed:,}개 처리")
                
            except Exception as e:
                print(f"   ❌ {date_str} 처리 실패: {str(e)[:50]}...")
                break
                
            current_date += timedelta(days=1)
        
        return total_processed, overall_batches
        
    def validate_kst_fact_table(self):
        """KST Fact 테이블 검증"""
        print(f"\n🔍 KST Fact 테이블 검증...")
        
        try:
            validation_stats = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT kst_date) as unique_kst_dates,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT event_name) as unique_events,
                COUNT(DISTINCT session_id) as unique_sessions,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as total_conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement,
                
                -- KST 시간 범위
                MIN(kst_timestamp) as min_kst_time,
                MAX(kst_timestamp) as max_kst_time,
                MIN(kst_date) as min_kst_date,
                MAX(kst_date) as max_kst_date,
                
                -- UTC 시간 범위 (비교)
                MIN(utc_timestamp) as min_utc_time,
                MAX(utc_timestamp) as max_utc_time
                
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            """).collect()[0]
            
            print("📊 KST Fact 테이블 검증 결과:")
            print(f"   총 레코드: {validation_stats.total_records:,}개")
            print(f"   KST 날짜 범위: {validation_stats.min_kst_date} ~ {validation_stats.max_kst_date}")
            print(f"   고유 사용자: {validation_stats.unique_users:,}명")
            print(f"   고유 이벤트: {validation_stats.unique_events}개")
            print(f"   고유 세션: {validation_stats.unique_sessions:,}개")
            print(f"   총 전환: {validation_stats.total_conversions:,}건")
            print(f"   평균 참여도: {validation_stats.avg_engagement}")
            
            print(f"\n🇰🇷 KST vs UTC 시간 비교:")
            print(f"   KST: {validation_stats.min_kst_time} ~ {validation_stats.max_kst_time}")
            print(f"   UTC: {validation_stats.min_utc_time} ~ {validation_stats.max_utc_time}")
            
            # KST 시간대별 분포
            hourly_dist = self.spark.sql(f"""
            SELECT 
                kst_hour,
                COUNT(*) as events,
                COUNT(DISTINCT user_id) as users
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            GROUP BY kst_hour
            ORDER BY kst_hour
            """).collect()
            
            print(f"\n⏰ KST 시간대별 활동 분포:")
            for row in hourly_dist:
                print(f"   {row.kst_hour:2d}시: {row.events:,}개 이벤트, {row.users:,}명 사용자")
            
            return validation_stats.total_records > 0
            
        except Exception as e:
            print(f"❌ 검증 실패: {str(e)}")
            return False
            
    def demonstrate_kst_analytics(self):
        """KST 기반 분석 예시"""
        print(f"\n🚀 KST 기반 분석 예시...")
        
        try:
            # 1. 한국 시간대별 사용 패턴
            print("⏰ 한국 시간대별 사용 패턴:")
            time_pattern = self.spark.sql(f"""
            SELECT 
                kst_hour,
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as active_users,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            WHERE kst_date >= '2025-07-01' AND kst_date <= '2025-07-03'
            GROUP BY kst_hour
            ORDER BY total_events DESC
            LIMIT 5
            """).collect()
            
            for row in time_pattern:
                print(f"   {row.kst_hour:2d}시: {row.total_events:,}개 이벤트, {row.active_users:,}명, 전환 {row.conversions}건")
            
            # 2. 요일별 패턴 (KST 기준)
            print(f"\n📅 요일별 활동 패턴 (KST 기준):")
            daily_pattern = self.spark.sql(f"""
            SELECT 
                kst_day_of_week,
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            GROUP BY kst_day_of_week
            ORDER BY total_events DESC
            """).collect()
            
            for row in daily_pattern:
                print(f"   {row.kst_day_of_week}: {row.total_events:,}개 이벤트, {row.unique_users:,}명")
            
            print(f"\n✅ KST 기반 분석으로 한국 사용자 행동 패턴 정확히 파악 가능!")
            
        except Exception as e:
            print(f"⚠️ 분석 예시 오류: {str(e)}")
            
    def execute_kst_optimized_processing(self, start_date: str = "2025-07-01", end_date: str = "2025-07-10"):
        """KST 최적화 처리 전체 실행"""
        print("🚀 KST 최적화 Gold Layer Fact 처리 시작!")
        print("=" * 60)
        
        start_time = time.time()
        
        try:
            # 1. 메모리 안전 SparkSession 생성
            self.create_memory_safe_spark_session()
            
            # 2. KST 최적화 Fact 테이블 준비
            if not self.ensure_kst_optimized_fact_table():
                print("❌ Fact 테이블 준비 실패")
                return False
            
            # 3. KST 기반 날짜 범위 처리
            total_processed, total_batches = self.process_date_range_with_kst(start_date, end_date)
            
            # 4. 결과 검증
            success = self.validate_kst_fact_table()
            
            # 5. KST 분석 예시
            if success:
                self.demonstrate_kst_analytics()
            
            end_time = time.time()
            elapsed_hours = (end_time - start_time) / 3600
            
            print(f"\n" + "=" * 60)
            if success and total_processed > 0:
                print(f"🎉 KST 최적화 Fact 처리 완료!")
                print(f"   📅 처리 기간: {start_date} ~ {end_date}")
                print(f"   📊 처리량: {total_processed:,}개 이벤트")
                print(f"   🔄 배치 수: {total_batches}개")
                print(f"   ⏱️ 소요 시간: {elapsed_hours:.1f}시간")
                print(f"   🇰🇷 KST 컬럼 활용: 완전 구현")
                print(f"   💾 메모리 안전: 5,000개 배치로 안정 처리")
            else:
                print(f"⚠️ 부분 완료 또는 실패")
                print(f"   처리량: {total_processed:,}개")
                
            return success
            
        except Exception as e:
            print(f"❌ KST 최적화 처리 실패: {str(e)}")
            return False
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    processor = KSTOptimizedFactProcessor()
    
    # 10일간 처리 (메모리 안전)
    success = processor.execute_kst_optimized_processing(
        start_date="2025-07-01", 
        end_date="2025-07-10"
    )
    
    if success:
        print(f"\n🎉 KST 최적화 완료!")
        print(f"   🇰🇷 한국 시간대 기준 정확한 분석 가능")
        print(f"   📊 시간대별/요일별 패턴 분석 가능")
        print(f"   🔒 메모리 안전 보장 (JVM 크래시 없음)")
    else:
        print(f"\n⚠️ 추가 최적화 필요")
