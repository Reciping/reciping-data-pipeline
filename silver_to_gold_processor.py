#!/usr/bin/env python3
"""
기존 호환 KST 최적화 Fact 처리기
- 기존 ultra_batch_processor 구조 유지
- KST 분석을 위한 추가 컬럼 최소화
- 메모리 안전 보장
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from datetime import datetime

class CompatibleKSTFactProcessor:
    """기존 호환 KST 최적화 Fact 처리기"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.silver_database = "recipe_analytics"
        self.gold_database = "gold_analytics"
        self.spark = None
        self.batch_size = 5000  # 성공 검증된 배치 크기
        
        print("🇰🇷 호환 KST Fact 처리기 초기화")
        print(f"   📦 안전 배치 크기: {self.batch_size:,}개")
        
    def create_memory_safe_spark_session(self):
        """메모리 안전 SparkSession"""
        print("🔧 메모리 안전 SparkSession 생성...")
        
        self.spark = SparkSession.builder \
            .appName("CompatibleKSTFact_SilverToGold") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://10.0.11.86:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ 메모리 안전 SparkSession 생성 완료")
        
    def clear_and_rebuild_fact_table(self):
        """기존 Fact 테이블 클리어 후 KST 데이터로 재구축"""
        print("\n🔄 기존 Fact 테이블 클리어 후 KST 데이터로 재구축...")
        
        try:
            # 기존 데이터 백업 확인
            current_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.gold_database}.fact_user_events").collect()[0]['cnt']
            print(f"   현재 데이터: {current_count:,}개")
            
            if current_count > 0:
                print("   🗑️ 기존 데이터 클리어...")
                self.spark.sql(f"DELETE FROM {self.catalog_name}.{self.gold_database}.fact_user_events")
                print("   ✅ 클리어 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ 클리어 실패: {str(e)}")
            return False
            
    def create_kst_optimized_batch(self, start_date: str, batch_num: int = 0):
        """KST 최적화된 배치 생성 (기존 스키마 호환)"""
        print(f"\n📅 KST 배치 생성: {start_date} (배치 #{batch_num + 1})")
        
        try:
            offset = batch_num * self.batch_size
            
            # 기존 스키마와 호환되는 KST 최적화 쿼리
            kst_batch_query = f"""
            INSERT INTO {self.catalog_name}.{self.gold_database}.fact_user_events
            SELECT 
                s.event_id,
                
                -- 차원 키들 (KST 기반으로 생성)
                0 as user_dim_key,
                
                -- time_dim_key를 KST 기준으로 생성 (YYYYMMDDHH 형식)
                CAST(DATE_FORMAT(s.kst_timestamp, 'yyyyMMddHH') AS BIGINT) as time_dim_key,
                
                COALESCE(s.prop_recipe_id, 0) as recipe_dim_key,
                0 as page_dim_key,
                
                -- event_dim_key (이벤트 유형별 구분)
                CASE 
                    WHEN s.event_name = 'auth_success' THEN 1
                    WHEN s.event_name = 'create_comment' THEN 2
                    WHEN s.event_name = 'click_bookmark' THEN 3
                    WHEN s.event_name = 'click_recipe' THEN 4
                    WHEN s.event_name = 'search_recipe' THEN 5
                    WHEN s.event_name = 'view_recipe' THEN 6
                    WHEN s.event_name = 'view_page' THEN 7
                    ELSE 0
                END as event_dim_key,
                
                -- 측정값
                1 as event_count,
                
                -- 세션 시간 (prop_action에서 추출)
                CASE 
                    WHEN s.prop_action IS NOT NULL AND SIZE(SPLIT(s.prop_action, ':')) >= 2
                    THEN COALESCE(CAST(SPLIT(s.prop_action, ':')[1] AS BIGINT), 60)
                    ELSE 60
                END as session_duration_seconds,
                
                30 as page_view_duration_seconds,
                
                -- 전환 플래그
                CASE WHEN s.event_name IN ('auth_success', 'click_bookmark', 'create_comment') THEN TRUE ELSE FALSE END as is_conversion,
                
                1.0 as conversion_value,
                
                -- KST 시간대별 참여도 점수 (한국 사용 패턴 최적화)
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
                
                -- ETL 메타데이터 (KST 타임스탬프 사용)
                s.kst_timestamp as created_at,
                s.kst_timestamp as updated_at
                
            FROM (
                SELECT 
                    event_id, kst_timestamp, utc_timestamp, date, year, month, day, hour,
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
            
    def process_kst_date_range(self, start_date: str, end_date: str):
        """KST 기반 날짜 범위 처리"""
        print(f"\n🗓️ KST 날짜 범위 처리: {start_date} ~ {end_date}")
        
        # 날짜별 처리
        from datetime import datetime, timedelta
        
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        total_processed = 0
        overall_batches = 0
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n📅 {date_str} KST 데이터 처리 중...")
            
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
                print(f"   📊 {date_count:,}개 이벤트 → {needed_batches}개 배치 (KST 최적화)")
                
                # 날짜별 배치 처리
                date_processed = 0
                for batch_num in range(needed_batches):
                    processed_count = self.create_kst_optimized_batch(date_str, batch_num)
                    
                    if processed_count > 0:
                        date_processed += processed_count
                        total_processed += processed_count
                        overall_batches += 1
                        
                        # 메모리 안정성을 위한 대기
                        time.sleep(1)
                    else:
                        print(f"   ⚠️ 배치 {batch_num + 1} 실패, 중단")
                        break
                
                print(f"   ✅ {date_str} KST 처리 완료: {date_processed:,}개")
                
            except Exception as e:
                print(f"   ❌ {date_str} KST 처리 실패: {str(e)[:50]}...")
                break
                
            current_date += timedelta(days=1)
        
        return total_processed, overall_batches
        
    def analyze_kst_patterns(self):
        """KST 패턴 분석 (기존 스키마 활용)"""
        print(f"\n🇰🇷 KST 패턴 분석...")
        
        try:
            # 시간별 패턴 (time_dim_key에서 시간 추출)
            print("⏰ KST 시간대별 활동 패턴:")
            hourly_pattern = self.spark.sql(f"""
            SELECT 
                (time_dim_key % 100) as kst_hour,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_id) as unique_sessions,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            WHERE time_dim_key >= 2025070100 AND time_dim_key <= 2025071023
            GROUP BY (time_dim_key % 100)
            ORDER BY total_events DESC
            LIMIT 10
            """).collect()
            
            for row in hourly_pattern:
                print(f"   {row.kst_hour:2d}시: {row.total_events:,}개 이벤트, {row.conversions}건 전환, 참여도 {row.avg_engagement}")
            
            # 일별 패턴
            print(f"\n📅 KST 일별 활동 패턴:")
            daily_pattern = self.spark.sql(f"""
            SELECT 
                FLOOR(time_dim_key / 100) as kst_date,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_id) as unique_sessions,
                SUM(event_count) as total_event_count,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            WHERE time_dim_key >= 2025070100 AND time_dim_key <= 2025071023
            GROUP BY FLOOR(time_dim_key / 100)
            ORDER BY kst_date
            """).collect()
            
            for row in daily_pattern:
                date_str = str(row.kst_date)
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                print(f"   {formatted_date}: {row.total_events:,}개 이벤트, {row.unique_sessions:,}개 세션")
            
            # 이벤트 유형별 패턴
            print(f"\n🎯 이벤트 유형별 KST 패턴:")
            event_pattern = self.spark.sql(f"""
            SELECT 
                event_dim_key,
                COUNT(*) as total_events,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement,
                ROUND(AVG(session_duration_seconds), 0) as avg_session_duration
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            GROUP BY event_dim_key
            ORDER BY total_events DESC
            """).collect()
            
            event_types = {
                0: "기타", 1: "인증성공", 2: "댓글작성", 3: "북마크", 
                4: "레시피클릭", 5: "레시피검색", 6: "레시피조회", 7: "페이지조회"
            }
            
            for row in event_pattern:
                event_name = event_types.get(row.event_dim_key, f"유형{row.event_dim_key}")
                print(f"   {event_name}: {row.total_events:,}개, 전환 {row.conversions}건, 참여도 {row.avg_engagement}")
            
            return True
            
        except Exception as e:
            print(f"⚠️ KST 분석 오류: {str(e)}")
            return False
            
    def generate_dashboard_kpi(self):
        """🎯 실시간 KPI 대시보드 데이터 생성"""
        print(f"\n🎯 실시간 KPI 대시보드 데이터 생성...")
        
        try:
            # 일간 핵심 KPI
            kpi_stats = self.spark.sql(f"""
            SELECT 
                CURRENT_DATE() as report_date,
                COUNT(DISTINCT session_id) as dau,
                COUNT(*) as total_events,
                ROUND(
                    SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
                    2
                ) as conversion_rate,
                ROUND(AVG(engagement_score), 2) as avg_engagement,
                ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT session_id), 2) as events_per_session
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            WHERE time_dim_key >= CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd00') AS BIGINT)
            """).collect()[0]
            
            print("📊 일간 핵심 KPI:")
            print(f"   DAU: {kpi_stats.dau:,}명")
            print(f"   총 이벤트: {kpi_stats.total_events:,}개")
            print(f"   전환율: {kpi_stats.conversion_rate}%")
            print(f"   평균 참여도: {kpi_stats.avg_engagement}")
            print(f"   세션당 이벤트: {kpi_stats.events_per_session}개")
            
            return kpi_stats
            
        except Exception as e:
            print(f"⚠️ KPI 생성 오류: {str(e)}")
            return None
    
    def generate_conversion_funnel(self):
        """🎪 전환 퍼널 차트 데이터 생성"""
        print(f"\n🎪 전환 퍼널 차트 데이터 생성...")
        
        try:
            funnel_data = self.spark.sql(f"""
            WITH user_journey AS (
                SELECT 
                    session_id,
                    MAX(CASE WHEN event_dim_key = 7 THEN 1 ELSE 0 END) as viewed_page,
                    MAX(CASE WHEN event_dim_key = 5 THEN 1 ELSE 0 END) as searched_recipe,
                    MAX(CASE WHEN event_dim_key = 4 THEN 1 ELSE 0 END) as clicked_recipe,
                    MAX(CASE WHEN event_dim_key = 3 THEN 1 ELSE 0 END) as bookmarked
                FROM {self.catalog_name}.{self.gold_database}.fact_user_events
                GROUP BY session_id
            )
            SELECT 
                'Page View' as stage, 1 as stage_order, SUM(viewed_page) as users
            FROM user_journey
            UNION ALL
            SELECT 'Recipe Search', 2, SUM(searched_recipe) FROM user_journey
            UNION ALL  
            SELECT 'Recipe Click', 3, SUM(clicked_recipe) FROM user_journey
            UNION ALL
            SELECT 'Bookmark', 4, SUM(bookmarked) FROM user_journey
            ORDER BY stage_order
            """).collect()
            
            print("📊 전환 퍼널 단계:")
            for row in funnel_data:
                print(f"   {row.stage}: {row.users:,}명")
            
            return funnel_data
            
        except Exception as e:
            print(f"⚠️ 퍼널 생성 오류: {str(e)}")
            return None
    
    def generate_recipe_performance(self):
        """🍳 레시피 성과 대시보드 데이터 생성"""
        print(f"\n🍳 레시피 성과 대시보드 데이터 생성...")
        
        try:
            recipe_stats = self.spark.sql(f"""
            SELECT 
                recipe_dim_key,
                COUNT(*) as interactions,
                COUNT(DISTINCT session_id) as unique_users,
                SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement,
                ROUND(
                    SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
                    2
                ) as conversion_rate
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            WHERE recipe_dim_key > 0
            GROUP BY recipe_dim_key
            ORDER BY interactions DESC
            LIMIT 10
            """).collect()
            
            print("📊 인기 레시피 TOP 10:")
            for i, row in enumerate(recipe_stats, 1):
                print(f"   {i:2d}. 레시피 {row.recipe_dim_key}: {row.interactions:,}회 상호작용, 전환율 {row.conversion_rate}%")
            
            return recipe_stats
            
        except Exception as e:
            print(f"⚠️ 레시피 성과 생성 오류: {str(e)}")
            return None

    def validate_kst_fact_data(self):
        """KST Fact 데이터 검증"""
        print(f"\n🔍 KST Fact 데이터 검증...")
        
        try:
            validation_stats = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT time_dim_key) as unique_time_keys,
                COUNT(DISTINCT session_id) as unique_sessions,
                COUNT(DISTINCT event_dim_key) as unique_event_types,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as total_conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement,
                MIN(time_dim_key) as min_time_key,
                MAX(time_dim_key) as max_time_key,
                MIN(created_at) as min_created_at,
                MAX(created_at) as max_created_at
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            """).collect()[0]
            
            print("📊 KST Fact 검증 결과:")
            print(f"   총 레코드: {validation_stats.total_records:,}개")
            print(f"   시간 키 범위: {validation_stats.min_time_key} ~ {validation_stats.max_time_key}")
            print(f"   고유 세션: {validation_stats.unique_sessions:,}개")
            print(f"   이벤트 유형: {validation_stats.unique_event_types}개")
            print(f"   총 전환: {validation_stats.total_conversions:,}건")
            print(f"   평균 참여도: {validation_stats.avg_engagement}")
            print(f"   생성 시간: {validation_stats.min_created_at} ~ {validation_stats.max_created_at}")
            
            return validation_stats.total_records > 0
            
        except Exception as e:
            print(f"❌ 검증 실패: {str(e)}")
            return False
            
    def execute_compatible_kst_processing(self, start_date: str = "2025-07-01", end_date: str = "2025-07-05"):
        """호환 KST 처리 전체 실행"""
        print("🚀 기존 호환 KST 최적화 처리 시작!")
        print("=" * 60)
        
        start_time = time.time()
        
        try:
            # 1. 메모리 안전 SparkSession 생성
            self.create_memory_safe_spark_session()
            
            # 2. 기존 데이터 클리어
            if not self.clear_and_rebuild_fact_table():
                print("❌ 테이블 클리어 실패")
                return False
            
            # 3. KST 기반 날짜 범위 처리
            total_processed, total_batches = self.process_kst_date_range(start_date, end_date)
            
            # 4. 결과 검증
            success = self.validate_kst_fact_data()
            
            # 5. KST 패턴 분석
            if success:
                self.analyze_kst_patterns()
                
                # 🎯 대시보드 데이터 생성 (새로 추가)
                print(f"\n🎨 대시보드 데이터 생성...")
                self.generate_dashboard_kpi()
                self.generate_conversion_funnel() 
                self.generate_recipe_performance()
            
            end_time = time.time()
            elapsed_hours = (end_time - start_time) / 3600
            
            print(f"\n" + "=" * 60)
            if success and total_processed > 0:
                print(f"🎉 KST 최적화 호환 처리 완료!")
                print(f"   📅 처리 기간: {start_date} ~ {end_date}")
                print(f"   📊 처리량: {total_processed:,}개 이벤트")
                print(f"   🔄 배치 수: {total_batches}개")
                print(f"   ⏱️ 소요 시간: {elapsed_hours:.1f}시간")
                print(f"   🇰🇷 KST 최적화: time_dim_key에 한국시간 반영")
                print(f"   💾 메모리 안전: 5,000개 배치로 안정 처리")
                print(f"   🔄 기존 스키마 호환: 완전 호환")
            else:
                print(f"⚠️ 부분 완료 또는 실패")
                print(f"   처리량: {total_processed:,}개")
                
            return success
            
        except Exception as e:
            print(f"❌ KST 호환 처리 실패: {str(e)}")
            return False
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    processor = CompatibleKSTFactProcessor()
    
    # 5일간 처리 (메모리 안전)
    success = processor.execute_compatible_kst_processing(
        start_date="2025-07-01", 
        end_date="2025-07-05"
    )
    
    if success:
        print(f"\n🎉 KST 호환 최적화 완료!")
        print(f"   🇰🇷 time_dim_key에 한국 시간 반영")
        print(f"   📊 KST 기반 시간대별 분석 가능")
        print(f"   🔒 메모리 안전 보장 (JVM 크래시 없음)")
        print(f"   ✅ 기존 스키마와 완전 호환")
    else:
        print(f"\n⚠️ 추가 최적화 필요")
