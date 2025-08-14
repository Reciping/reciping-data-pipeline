#!/usr/bin/env python3
"""
메모리 제약 환경에서 안전한 메트릭 계산
- JVM 크래시 없는 단순 쿼리만 사용
- 기존 25,000개 레코드로 의미있는 분석 제공
- 즉시 비즈니스 의사결정 지원
"""

from pyspark.sql import SparkSession

class SafeGoldAnalytics:
    """안전한 Gold Layer 분석"""
    
    def __init__(self):
        self.catalog_name = "iceberg_catalog"
        self.silver_database = "recipe_analytics"
        self.gold_database = "gold_analytics"
        self.spark = None
        
    def create_minimal_spark_session(self):
        """최소한의 안전한 SparkSession"""
        print("🔧 안전한 SparkSession 생성...")
        
        self.spark = SparkSession.builder \
            .appName("SafeAnalytics") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog.uri", "thrift://metastore:9083") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://reciping-user-event-logs/iceberg/warehouse/") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ 안전한 SparkSession 생성 완료")
        
    def analyze_current_state(self):
        """현재 데이터 상태 분석"""
        print("\n📊 현재 Gold Layer 데이터 분석...")
        
        # 1. 기본 통계
        print("   🔍 기본 통계:")
        basic_stats = self.spark.sql(f"""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT session_id) as total_sessions,
            COUNT(DISTINCT time_dim_key) as time_periods,
            COUNT(DISTINCT page_dim_key) as pages_visited,
            COUNT(DISTINCT event_dim_key) as event_types,
            SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
            ROUND(AVG(engagement_score), 2) as avg_engagement,
            ROUND(AVG(session_duration_seconds), 2) as avg_session_time
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events
        """).collect()[0]
        
        print(f"      총 이벤트: {basic_stats.total_events:,}개")
        print(f"      총 세션: {basic_stats.total_sessions:,}개")
        print(f"      시간 구간: {basic_stats.time_periods}개")
        print(f"      방문 페이지: {basic_stats.pages_visited}개")
        print(f"      이벤트 타입: {basic_stats.event_types}개")
        print(f"      전환 이벤트: {basic_stats.conversions:,}개")
        print(f"      평균 참여도: {basic_stats.avg_engagement}점")
        print(f"      평균 세션 시간: {basic_stats.avg_session_time}초")
        
        # 전환율 계산
        conversion_rate = (basic_stats.conversions / basic_stats.total_events) * 100
        print(f"      전환율: {conversion_rate:.2f}%")
        
    def calculate_safe_metrics(self):
        """JVM 크래시 없는 안전한 메트릭 계산"""
        print("\n📈 안전한 메트릭 계산...")
        
        # 1. 이벤트 타입별 분석 (간단한 GROUP BY)
        print("   🎬 이벤트 타입별 성과:")
        event_analysis = self.spark.sql(f"""
        SELECT 
            e.event_name,
            e.event_category,
            COUNT(*) as event_count,
            SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
            ROUND(SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate,
            ROUND(AVG(f.engagement_score), 2) as avg_engagement
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_events e ON f.event_dim_key = e.event_dim_key
        GROUP BY e.event_name, e.event_category
        ORDER BY event_count DESC
        """).collect()
        
        for row in event_analysis:
            print(f"      {row.event_name}: {row.event_count:,}건 ({row.conversion_rate}% 전환율, {row.avg_engagement}점 참여도)")
            
        # 2. 페이지별 분석
        print("   📱 페이지별 성과:")
        page_analysis = self.spark.sql(f"""
        SELECT 
            p.page_name,
            p.funnel_stage,
            COUNT(*) as visits,
            COUNT(DISTINCT f.session_id) as unique_sessions,
            SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
            ROUND(AVG(f.engagement_score), 2) as avg_engagement
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_pages p ON f.page_dim_key = p.page_dim_key
        WHERE p.page_name != 'Unknown'
        GROUP BY p.page_name, p.funnel_stage
        ORDER BY visits DESC
        """).collect()
        
        for row in page_analysis:
            conversion_rate = (row.conversions / row.visits) * 100 if row.visits > 0 else 0
            print(f"      {row.page_name} ({row.funnel_stage}): {row.visits:,}방문, {row.unique_sessions:,}세션, {conversion_rate:.2f}% 전환율")
            
        # 3. 시간대별 분석
        print("   ⏰ 시간대별 사용 패턴:")
        time_analysis = self.spark.sql(f"""
        SELECT 
            t.hour,
            COUNT(*) as events,
            COUNT(DISTINCT f.session_id) as sessions,
            SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
            ROUND(AVG(f.engagement_score), 2) as avg_engagement
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_time t ON f.time_dim_key = t.time_dim_key
        GROUP BY t.hour
        ORDER BY t.hour
        """).collect()
        
        for row in time_analysis:
            conversion_rate = (row.conversions / row.events) * 100 if row.events > 0 else 0
            print(f"      {row.hour:02d}시: {row.events:,}이벤트, {row.sessions:,}세션, {conversion_rate:.2f}% 전환율, {row.avg_engagement}점 참여도")
            
    def provide_business_insights(self):
        """현재 데이터로 제공 가능한 비즈니스 인사이트"""
        print("\n🎯 비즈니스 인사이트 및 권장사항...")
        
        # 1. 최고 성과 이벤트 식별
        top_event = self.spark.sql(f"""
        SELECT 
            e.event_name,
            COUNT(*) as event_count,
            ROUND(SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_events e ON f.event_dim_key = e.event_dim_key
        GROUP BY e.event_name
        HAVING COUNT(*) >= 100
        ORDER BY conversion_rate DESC
        LIMIT 1
        """).collect()[0]
        
        print(f"   🏆 최고 전환 이벤트: {top_event.event_name} ({top_event.conversion_rate}% 전환율)")
        
        # 2. 최적 사용 시간대
        peak_hour = self.spark.sql(f"""
        SELECT 
            t.hour,
            COUNT(*) as events,
            ROUND(AVG(f.engagement_score), 2) as avg_engagement
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_time t ON f.time_dim_key = t.time_dim_key
        GROUP BY t.hour
        ORDER BY avg_engagement DESC
        LIMIT 1
        """).collect()[0]
        
        print(f"   📈 최적 활동 시간: {peak_hour.hour}시 (평균 {peak_hour.avg_engagement}점 참여도)")
        
        # 3. 전환 퍼널 분석
        funnel_analysis = self.spark.sql(f"""
        SELECT 
            p.funnel_stage,
            COUNT(*) as stage_events,
            SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) as conversions,
            ROUND(SUM(CASE WHEN f.is_conversion = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as stage_conversion_rate
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_pages p ON f.page_dim_key = p.page_dim_key
        WHERE p.funnel_stage != 'Unknown'
        GROUP BY p.funnel_stage
        ORDER BY stage_events DESC
        """).collect()
        
        print(f"   🔄 전환 퍼널 성과:")
        for row in funnel_analysis:
            print(f"      {row.funnel_stage}: {row.stage_events:,}이벤트 → {row.conversions:,}전환 ({row.stage_conversion_rate}%)")
            
        # 4. 전체 성과 요약
        total_stats = self.spark.sql(f"""
        SELECT 
            COUNT(DISTINCT session_id) as total_sessions,
            SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) as total_conversions,
            ROUND(SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT session_id), 2) as session_conversion_rate,
            ROUND(AVG(engagement_score), 2) as overall_engagement
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events
        """).collect()[0]
        
        print(f"\n📊 전체 성과 요약:")
        print(f"   세션 수: {total_stats.total_sessions:,}개")
        print(f"   전환 수: {total_stats.total_conversions:,}개")
        print(f"   세션 전환율: {total_stats.session_conversion_rate}%")
        print(f"   전체 참여도: {total_stats.overall_engagement}점")
        
    def suggest_improvements(self):
        """현재 데이터 기반 개선 제안"""
        print("\n💡 데이터 기반 개선 제안...")
        
        # 1. 낮은 성과 페이지 식별
        low_performance_pages = self.spark.sql(f"""
        SELECT 
            p.page_name,
            COUNT(*) as visits,
            ROUND(AVG(f.engagement_score), 2) as avg_engagement
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_pages p ON f.page_dim_key = p.page_dim_key
        WHERE p.page_name != 'Unknown'
        GROUP BY p.page_name
        HAVING COUNT(*) >= 100
        ORDER BY avg_engagement ASC
        LIMIT 2
        """).collect()
        
        print(f"   📉 개선 필요 페이지:")
        for row in low_performance_pages:
            print(f"      {row.page_name}: {row.avg_engagement}점 참여도 (평균 이하)")
            
        # 2. 비활성 시간대 식별
        low_activity_hours = self.spark.sql(f"""
        SELECT 
            t.hour,
            COUNT(*) as events
        FROM {self.catalog_name}.{self.gold_database}.fact_user_events f
        JOIN {self.catalog_name}.{self.gold_database}.dim_time t ON f.time_dim_key = t.time_dim_key
        GROUP BY t.hour
        ORDER BY events ASC
        LIMIT 3
        """).collect()
        
        print(f"   ⏰ 활성화 필요 시간대:")
        for row in low_activity_hours:
            print(f"      {row.hour:02d}시: {row.events}이벤트 (마케팅 기회)")
            
        print(f"\n🎯 즉시 실행 가능한 액션:")
        print(f"   1. 고전환 이벤트 프로모션 강화")
        print(f"   2. 피크 시간대 컨텐츠 최적화")  
        print(f"   3. 저성과 페이지 UX 개선")
        print(f"   4. 비활성 시간대 타겟 마케팅")
        
    def execute_safe_analysis(self):
        """안전한 분석 전체 실행"""
        print("🛡️ 안전한 Gold Layer 분석 실행...")
        print("=" * 60)
        
        try:
            # 1. SparkSession 생성
            self.create_minimal_spark_session()
            
            # 2. 현재 상태 분석
            self.analyze_current_state()
            
            # 3. 안전한 메트릭 계산
            self.calculate_safe_metrics()
            
            # 4. 비즈니스 인사이트 제공
            self.provide_business_insights()
            
            # 5. 개선 제안
            self.suggest_improvements()
            
            print(f"\n🎉 안전한 분석 완료!")
            print(f"   ✅ JVM 크래시 없음")
            print(f"   ✅ 즉시 사용 가능한 인사이트 제공")
            print(f"   ✅ 비즈니스 의사결정 지원 데이터 확보")
            print(f"   ✅ 개선 방향 명확화")
            
        except Exception as e:
            print(f"❌ 안전한 분석 실패: {str(e)}")
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    safe_analytics = SafeGoldAnalytics()
    safe_analytics.execute_safe_analysis()
