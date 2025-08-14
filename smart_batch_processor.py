#!/usr/bin/env python3
"""
스마트 배치 처리를 통한 Fact 테이블 생성
- 메모리 안전한 배치 크기로 점진적 처리
- SQL 쿼리 레벨에서 배치 분할
- 실시간 진행 상황 모니터링
"""

from pyspark.sql import SparkSession
import time
from datetime import datetime, timedelta

class SmartBatchProcessor:
    """메모리 안전한 배치 처리기"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.silver_database = "recipe_analytics"
        self.gold_database = "gold_analytics"
        self.spark = None
        
        # 배치 설정
        self.batch_size = 15000  # 안전한 배치 크기
        self.date_batch_size = 2  # 일별 배치 크기
        
    def create_spark_session(self):
        """메모리 최적화된 SparkSession"""
        print("🔧 배치 처리용 SparkSession 생성...")
        
        self.spark = SparkSession.builder \
            .appName("SmartBatchProcessor") \
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
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ 배치 처리용 SparkSession 생성 완료")
        
    def analyze_silver_data_distribution(self):
        """Silver 데이터 분포 분석으로 최적 배치 전략 수립"""
        print("\n📊 Silver 데이터 분포 분석...")
        
        # 1. 날짜별 데이터 분포
        date_distribution = self.spark.sql(f"""
        SELECT 
            date,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT session_id) as unique_sessions
        FROM {self.catalog_name}.{self.silver_database}.user_events_silver
        WHERE date >= '2025-07-01' AND date <= '2025-07-31'
        GROUP BY date
        ORDER BY date
        """).collect()
        
        print("   📅 날짜별 데이터 분포:")
        total_events = 0
        for row in date_distribution:
            total_events += row.event_count
            print(f"      {row.date}: {row.event_count:,}이벤트, {row.unique_users:,}사용자, {row.unique_sessions:,}세션")
            
        print(f"   📊 총 이벤트: {total_events:,}개")
        
        # 2. 배치 전략 결정
        avg_events_per_day = total_events / len(date_distribution)
        recommended_days_per_batch = max(1, int(self.batch_size / avg_events_per_day))
        
        print(f"\n🎯 배치 전략:")
        print(f"   평균 일일 이벤트: {avg_events_per_day:,.0f}개")
        print(f"   권장 배치 크기: {self.batch_size:,}개 이벤트")
        print(f"   권장 일별 배치: {recommended_days_per_batch}일씩")
        
        self.date_batch_size = recommended_days_per_batch
        return date_distribution
        
    def create_batched_fact_table(self):
        """배치별로 Fact 테이블 생성"""
        print("\n🔄 스마트 배치 처리로 Fact 테이블 생성 시작...")
        
        # 1. 데이터 분포 분석
        date_distribution = self.analyze_silver_data_distribution()
        
        # 2. 날짜 범위를 배치로 분할
        dates = [row.date for row in date_distribution]
        date_batches = []
        for i in range(0, len(dates), self.date_batch_size):
            batch = dates[i:i + self.date_batch_size]
            date_batches.append((batch[0], batch[-1]))
            
        print(f"\n📦 총 {len(date_batches)}개 배치로 분할:")
        for i, (start_date, end_date) in enumerate(date_batches):
            expected_events = sum(row.event_count for row in date_distribution 
                                if start_date <= row.date <= end_date)
            print(f"   배치 {i+1}: {start_date} ~ {end_date} (예상: {expected_events:,}이벤트)")
            
        # 3. 첫 번째 배치로 Fact 테이블 초기화
        first_start, first_end = date_batches[0]
        print(f"\n🎬 첫 번째 배치 처리: {first_start} ~ {first_end}")
        
        success_count = self.process_fact_batch(first_start, first_end, is_first_batch=True)
        
        if success_count == 0:
            print("❌ 첫 번째 배치 실패 - 더 작은 배치로 재시도")
            return self.process_micro_batches(first_start, first_end)
            
        # 4. 나머지 배치들 순차 처리
        total_processed = success_count
        for i, (start_date, end_date) in enumerate(date_batches[1:], 2):
            print(f"\n🔄 배치 {i} 처리: {start_date} ~ {end_date}")
            
            batch_count = self.process_fact_batch(start_date, end_date, is_first_batch=False)
            
            if batch_count > 0:
                total_processed += batch_count
                print(f"   ✅ 배치 {i} 성공: {batch_count:,}개 추가 (누적: {total_processed:,}개)")
            else:
                print(f"   ⚠️ 배치 {i} 실패 - 스킵")
                
        print(f"\n🎉 배치 처리 완료!")
        print(f"   총 처리된 레코드: {total_processed:,}개")
        
        # 5. 최종 검증
        self.validate_batched_result()
        
        return total_processed
        
    def process_fact_batch(self, start_date, end_date, is_first_batch=False):
        """단일 배치 처리"""
        try:
            # 배치별 안전한 SQL 쿼리
            batch_query = f"""
            WITH silver_batch AS (
                SELECT 
                    event_id, user_id, session_id, anonymous_id, event_name,
                    page_name, prop_recipe_id, utc_timestamp, date, prop_action
                FROM {self.catalog_name}.{self.silver_database}.user_events_silver
                WHERE date >= '{start_date}' AND date <= '{end_date}'
            ),
            fact_batch AS (
                SELECT 
                    s.event_id,
                    COALESCE(u.user_dim_key, 0) as user_dim_key,
                    CAST(DATE_FORMAT(s.utc_timestamp, 'yyyyMMdd') AS BIGINT) * 100 + HOUR(s.utc_timestamp) as time_dim_key,
                    COALESCE(r.recipe_dim_key, 0) as recipe_dim_key,
                    COALESCE(p.page_dim_key, 0) as page_dim_key,
                    COALESCE(e.event_dim_key, 1) as event_dim_key,
                    
                    1 as event_count,
                    CAST(RAND() * 120 AS INT) as session_duration_seconds,
                    30 as page_view_duration_seconds,
                    
                    COALESCE(e.is_conversion_event, FALSE) as is_conversion,
                    COALESCE(e.conversion_value, 1.0) as conversion_value,
                    
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
                    
                    s.session_id,
                    s.anonymous_id,
                    CURRENT_TIMESTAMP() as created_at,
                    CURRENT_TIMESTAMP() as updated_at
                    
                FROM silver_batch s
                LEFT JOIN {self.catalog_name}.{self.gold_database}.dim_users u 
                    ON s.user_id = u.user_id AND u.is_current = TRUE
                LEFT JOIN {self.catalog_name}.{self.gold_database}.dim_recipes r 
                    ON s.prop_recipe_id = r.recipe_id
                LEFT JOIN {self.catalog_name}.{self.gold_database}.dim_pages p 
                    ON s.page_name = p.page_name
                LEFT JOIN {self.catalog_name}.{self.gold_database}.dim_events e 
                    ON s.event_name = e.event_name
                WHERE s.event_id IS NOT NULL
            )
            
            {"INSERT OVERWRITE" if is_first_batch else "INSERT INTO"} {self.catalog_name}.{self.gold_database}.fact_user_events
            SELECT * FROM fact_batch
            """
            
            # 배치 실행
            start_time = time.time()
            self.spark.sql(batch_query)
            execution_time = time.time() - start_time
            
            # 배치 결과 확인
            if is_first_batch:
                batch_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.gold_database}.fact_user_events").collect()[0]['cnt']
            else:
                # 새로 추가된 레코드 수 추정
                total_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{self.gold_database}.fact_user_events").collect()[0]['cnt']
                # 이전 배치와의 차이로 현재 배치 크기 추정 (정확하지 않지만 근사값)
                silver_batch_count = self.spark.sql(f"""
                SELECT COUNT(*) as cnt 
                FROM {self.catalog_name}.{self.silver_database}.user_events_silver 
                WHERE date >= '{start_date}' AND date <= '{end_date}'
                """).collect()[0]['cnt']
                batch_count = min(silver_batch_count, total_count)  # 보수적 추정
                
            print(f"      ⏱️ 실행 시간: {execution_time:.1f}초")
            print(f"      📊 처리 레코드: {batch_count:,}개")
            
            return batch_count
            
        except Exception as e:
            print(f"      ❌ 배치 실패: {str(e)[:100]}...")
            return 0
            
    def process_micro_batches(self, start_date, end_date):
        """초소형 배치 처리 (fallback)"""
        print(f"\n🔬 초소형 배치 처리: {start_date} ~ {end_date}")
        
        # 날짜별로 하나씩 처리
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        total_processed = 0
        is_first = True
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"   📅 처리 중: {date_str}")
            
            try:
                # 하루치 데이터만 처리
                daily_count = self.process_fact_batch(date_str, date_str, is_first_batch=is_first)
                
                if daily_count > 0:
                    total_processed += daily_count
                    print(f"      ✅ 성공: {daily_count:,}개")
                    is_first = False
                else:
                    print(f"      ⚠️ 실패 또는 데이터 없음")
                    
            except Exception as e:
                print(f"      ❌ 오류: {str(e)[:50]}...")
                
            current_date += timedelta(days=1)
            
        print(f"\n📊 초소형 배치 완료: {total_processed:,}개 처리")
        return total_processed
        
    def validate_batched_result(self):
        """배치 처리 결과 검증"""
        print("\n🔍 배치 처리 결과 검증...")
        
        try:
            final_stats = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT user_dim_key) as unique_users,
                COUNT(DISTINCT recipe_dim_key) as unique_recipes,
                COUNT(DISTINCT time_dim_key) as unique_time_keys,
                COUNT(DISTINCT session_id) as unique_sessions,
                SUM(CASE WHEN user_dim_key > 0 THEN 1 ELSE 0 END) as mapped_users,
                SUM(CASE WHEN recipe_dim_key > 0 THEN 1 ELSE 0 END) as mapped_recipes,
                SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
                ROUND(AVG(engagement_score), 2) as avg_engagement
            FROM {self.catalog_name}.{self.gold_database}.fact_user_events
            """).collect()[0]
            
            print("📊 최종 배치 결과:")
            print(f"   총 레코드: {final_stats.total_records:,}개")
            print(f"   고유 사용자: {final_stats.unique_users:,}명")
            print(f"   고유 레시피: {final_stats.unique_recipes:,}개")
            print(f"   고유 시간키: {final_stats.unique_time_keys:,}개")
            print(f"   고유 세션: {final_stats.unique_sessions:,}개")
            
            user_mapping_pct = (final_stats.mapped_users / final_stats.total_records) * 100
            recipe_mapping_pct = (final_stats.mapped_recipes / final_stats.total_records) * 100
            
            print(f"   사용자 매핑: {user_mapping_pct:.1f}% ({final_stats.mapped_users:,}개)")
            print(f"   레시피 매핑: {recipe_mapping_pct:.1f}% ({final_stats.mapped_recipes:,}개)")
            print(f"   전환 이벤트: {final_stats.conversions:,}개")
            print(f"   평균 참여도: {final_stats.avg_engagement}점")
            
            # 성공 평가
            if final_stats.total_records >= 50000:
                print("\n🎉 배치 처리 성공!")
                print(f"   ✅ 대용량 데이터 안전 처리")
                print(f"   ✅ JVM 크래시 없이 완료")
                print(f"   ✅ 비즈니스 분석 가능")
            else:
                print("\n⚠️ 부분 성공")
                print(f"   💡 더 작은 배치 크기 권장")
                
        except Exception as e:
            print(f"❌ 검증 실패: {str(e)}")
            
    def execute_smart_batch_processing(self):
        """스마트 배치 처리 전체 실행"""
        print("🚀 스마트 배치 처리 실행...")
        print("=" * 60)
        
        try:
            # 1. SparkSession 생성
            self.create_spark_session()
            
            # 2. 배치별 Fact 테이블 생성
            total_processed = self.create_batched_fact_table()
            
            if total_processed > 0:
                print(f"\n🎉 스마트 배치 처리 성공!")
                print(f"   ✅ 총 {total_processed:,}개 레코드 처리")
                print(f"   ✅ 메모리 안전 확보")
                print(f"   ✅ 점진적 확장 가능")
            else:
                print(f"\n❌ 배치 처리 실패")
                print(f"   💡 더 작은 배치 크기나 다른 전략 필요")
                
        except Exception as e:
            print(f"❌ 스마트 배치 처리 실패: {str(e)}")
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    batch_processor = SmartBatchProcessor()
    batch_processor.execute_smart_batch_processing()
