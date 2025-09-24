# ===================================================================
# 1️⃣ 필수 라이브러리 Import
# ===================================================================

import os
import uuid
import json
import random
import gc

import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

# 데이터 처리
import numpy as np
import pandas as pd

# Dask (분산 처리)
import dask
import dask.dataframe as dd
from dask import delayed

from confluent_kafka import Producer
import socket

# 설정
pd.set_option('display.max_columns', None)

print("✅ 모든 라이브러리가 성공적으로 로드되었습니다.")



# ===================================================================
# 2️⃣ 데이터 로딩 및 기본 설정
# ===================================================================

# S3 경로 정의
s3_base_path = 's3://reciping-user-event-logs/meta-data'

# S3에서 Parquet 파일 읽기
print("☁️ S3에서 메타데이터 로딩 시작...")
recipes_df = pd.read_parquet(f'{s3_base_path}/total_recipes.parquet')
users_df = pd.read_parquet(f'{s3_base_path}/user.parquet')
profiles_df = pd.read_parquet(f'{s3_base_path}/user_profiles.parquet')

print(f"✅ 데이터 로딩 완료:")
print(f"   - 레시피: {len(recipes_df):,}개")
print(f"   - 사용자: {len(users_df):,}명")
print(f"   - 프로필: {len(profiles_df):,}개")

# Demographic Segment 분포 정의
DEMOGRAPHIC_DISTRIBUTION = {
    'FEMALE_20S': 0.142,    # 14.2%
    'FEMALE_30S': 0.207,    # 20.7%
    'FEMALE_40_PLUS': 0.356, # 35.6%
    'MALE_20S': 0.062,      # 6.2%
    'MALE_30S': 0.085,      # 8.5%
    'MALE_40_PLUS': 0.148   # 14.8%
}

# 행동 태그 정의
INTENSITY_PERSONAS = {
    'POWER_USER': {'ratio': 0.15, 'description': '파워_유저, 주 5회 이상 활동'},
    'ACTIVE_USER': {'ratio': 0.55, 'description': '활성_유저, 주 2-4회 활동'},
    'CASUAL_USER': {'ratio': 0.30, 'description': '캐주얼_유저, 주 1회 이하 활동'}
}

COOKING_STYLE_PERSONAS = {
    'DESSERT_FOCUSED': {'ratio': 0.20, 'description': '디저트_중심, 베이킹 디저트 제작 선호'},
    'HEALTHY_CONSCIOUS': {'ratio': 0.25, 'description': '건강식_지향, 다이어트 웰빙 요리 선호'},
    'COMFORT_FOOD': {'ratio': 0.25, 'description': '든든한_식사, 메인 요리 한 끼 식사 선호'},
    'QUICK_CONVENIENT': {'ratio': 0.20, 'description': '간편_요리, 시간절약 간단 요리 선호'},
    'DIVERSE_EXPLORER': {'ratio': 0.10, 'description': '다양한_탐험, 특별한 패턴 없이 다양하게 탐색'}
}


# ===================================================================
# 3️⃣ 성숙 단계 서비스 시뮬레이션 환경 설정 (월 1억 건 대용량)
# ===================================================================

from datetime import timezone, timedelta

# 한국시간(KST) 설정 
KST = timezone(timedelta(hours=9))

# 시뮬레이션 기간: 2025년 6월 (성수기 1개월)
SIMULATION_START_DATE = datetime(2025, 6, 1, tzinfo=KST)
# SIMULATION_END_DATE = datetime(2025, 7, 31, 23, 59, 59, tzinfo=KST)
# SIMULATION_START_DATE = datetime(2025, 8, 1, tzinfo=KST)
SIMULATION_END_DATE = datetime(2025, 8, 31, 23, 59, 59, tzinfo=KST)

# 대용량 목표 지표 (성숙한 서비스)
TARGET_MONTHLY_EVENTS = 100_000_000  # 월 1억 건
# TARGET_DAU_AVERAGE = 160_000         # 평균 일간 활성 사용자
TARGET_DAU_AVERAGE = 1000         # 평균 일간 활성 사용자
# TARGET_MAU = 700_000                 # 월간 활성 사용자
TARGET_MAU = 2000                 # 월간 활성 사용자
TARGET_EVENTS_PER_USER_DAY = 20      # 1인당 일평균 이벤트

# 주간 패턴 (성숙한 서비스의 주기적 패턴)
WEEKDAY_MULTIPLIER = {
    0: 0.85,  # 월요일 (낮음)
    1: 0.90,  # 화요일
    2: 0.95,  # 수요일 
    3: 0.95,  # 목요일
    4: 1.20,  # 금요일 (주말 준비로 증가)
    5: 1.30,  # 토요일 (주말 피크)
    6: 1.25   # 일요일 (주말 피크)
}

# 사용자 세그먼트 재정의 (3개 그룹)
USER_SEGMENTS = {
    'POWER_USER': {
        'ratio': 0.10,  # 10%
        'daily_events': (40, 50),
        'description': '파워유저: 레시피 작성, 댓글 등 높은 기여도'
    },
    'ACTIVE_EXPLORER': {
        'ratio': 0.60,  # 60% 
        'daily_events': (15, 20),
        'description': '적극적 탐색 유저: 검색, 필터 등 다양한 기능 활용'
    },
    'PASSIVE_BROWSER': {
        'ratio': 0.30,  # 30%
        'daily_events': (5, 10), 
        'description': '소극적 탐색 유저: 추천 목록 위주 가벼운 소비'
    }
}

# KPI 목표 수준
TARGET_KPI = {
    'ad_ctr': 0.015,              # 광고 클릭률 1.5%
    'recipe_detail_conversion': 0.10,  # 상세 페이지 전환율 10%
    'retention_day1': 0.30,      # Day 1 유지율 30%
    'retention_day7': 0.15,      # Day 7 유지율 15%
    'retention_day30': 0.08      # Day 30 유지율 8%
}

# AB 테스트 설정 (한국시간 적용)
# AB_TEST_START_DATE = datetime(2025, 7, 8, tzinfo=KST)
# AB_TEST_END_DATE = datetime(2025, 7, 22, tzinfo=KST)
AB_TEST_START_DATE = datetime(2025, 8, 8, tzinfo=KST)
AB_TEST_END_DATE = datetime(2025, 8, 22, tzinfo=KST)

AB_TEST_SCENARIO_CODE = 'BEHAVIORAL_TARGETING_MVP_V1'
AB_TEST_CONTROL_CTR = 0.018      # Control: 기존 랜덤 광고 서빙 1.8%
AB_TEST_TREATMENT_CTR = 0.022    # Treatment: 행동 태그 기반 타겟팅 2.2%

# 세그먼트별 AB 테스트 목표 CTR
AB_TEST_SEGMENT_TARGETS = {
    ('FEMALE_30S', 'POWER_USER', 'DESSERT_FOCUSED'): {'current': 0.021, 'target': 0.028},
    ('MALE_20S', 'ACTIVE_EXPLORER', 'QUICK_CONVENIENT'): {'current': 0.015, 'target': 0.019},
    ('FEMALE_40_PLUS', 'ACTIVE_EXPLORER', 'HEALTHY_CONSCIOUS'): {'current': 0.018, 'target': 0.023},
    ('MALE_30S', 'PASSIVE_BROWSER', 'DIVERSE_EXPLORER'): {'current': 0.014, 'target': 0.017}
}

print("✅ 성숙 단계 서비스 시뮬레이션 환경 설정 완료")
print(f"📅 시뮬레이션 기간: {SIMULATION_START_DATE.strftime('%Y-%m-%d %H:%M %Z')} ~ {SIMULATION_END_DATE.strftime('%Y-%m-%d %H:%M %Z')}")
print(f"🎯 목표 월간 이벤트: {TARGET_MONTHLY_EVENTS:,}건")
print(f"👥 평균 DAU: {TARGET_DAU_AVERAGE:,}명, MAU: {TARGET_MAU:,}명")
print(f"📊 1인당 일평균 이벤트: {TARGET_EVENTS_PER_USER_DAY}개")
print(f"⏰ 시간대: 한국시간(KST, UTC+9)")
print(f"� 패턴: 주기적 (주말 피크: {max(WEEKDAY_MULTIPLIER.values()):.1f}x)")


# ===================================================================
# 3️⃣-1 성숙 단계 서비스의 주기적 DAU 계산 함수
# ===================================================================

def calculate_cyclical_dau(target_date):
    """
    성숙한 서비스의 주기적 DAU 계산 (S-커브 대신 주간/일일 패턴)
    
    Args:
        target_date: 계산할 날짜 (datetime 객체)
    
    Returns:
        int: 해당 날짜의 DAU
    """
    
    # 기본 DAU (평균값)
    base_dau = TARGET_DAU_AVERAGE
    
    # 요일별 가중치 적용
    weekday = target_date.weekday()  # 0=월요일, 6=일요일
    weekday_multiplier = WEEKDAY_MULTIPLIER.get(weekday, 1.0)
    
    # 월별 계절성 (7월은 성수기로 가정)
    month_multiplier = 1.1  # 7월 성수기 10% 증가
    
    # 최종 DAU 계산
    final_dau = int(base_dau * weekday_multiplier * month_multiplier)
    
    return final_dau

def calculate_daily_events_target(dau):
    """일별 목표 이벤트 수 계산"""
    return dau * TARGET_EVENTS_PER_USER_DAY

def get_korean_timestamp(dt):
    """datetime 객체를 한국시간 ISO8601 문자열로 변환"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    elif dt.tzinfo != KST:
        dt = dt.astimezone(KST)
    
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+09:00'

# 테스트: 7월 첫 주 DAU 패턴 확인
# print("\n📊 7월 첫 주 DAU 패턴 미리보기:")
# test_start = datetime(2025, 7, 1)
print("\n📊 8월 첫 주 DAU 패턴 미리보기:")
test_start = datetime(2025, 8, 1)
for i in range(7):
    test_date = test_start + timedelta(days=i)
    dau = calculate_cyclical_dau(test_date)
    events = calculate_daily_events_target(dau)
    weekday_name = ['월', '화', '수', '목', '금', '토', '일'][test_date.weekday()]
    
    print(f"   {test_date.strftime('%m/%d')} ({weekday_name}): DAU {dau:,}명 → 목표 이벤트 {events:,}건")

print(f"\n✅ 주기적 DAU 계산 함수 준비 완료")
print(f"📈 주말 피크: 토요일 {int(TARGET_DAU_AVERAGE * WEEKDAY_MULTIPLIER[5] * 1.1):,}명")
print(f"📉 주중 최저: 월요일 {int(TARGET_DAU_AVERAGE * WEEKDAY_MULTIPLIER[0] * 1.1):,}명")


# ===================================================================
# 3️⃣-2 성숙 단계 서비스 AB 테스트 관련 함수 (세그먼트별 목표 CTR 적용)
# ===================================================================

import hashlib

def is_ab_test_period(target_date):
    """AB 테스트 기간인지 확인 (한국시간 기준)"""
    return AB_TEST_START_DATE.date() <= target_date <= AB_TEST_END_DATE.date()

def assign_ab_test_group(user_id):
    """사용자를 AB 테스트 그룹에 할당 (일관성 있게)"""
    user_hash = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16)
    return 'treatment' if user_hash % 2 == 0 else 'control'

def get_segment_combination_key(user_data):
    """사용자 데이터에서 세그먼트 조합 키 생성"""
    demographic = user_data.get('demographic_segment', '')
    activity = user_data.get('activity_segment', '')
    cooking_style = user_data.get('cooking_style_persona', '')
    
    return (demographic, activity, cooking_style)

def get_target_ctr_for_segment(segment_key, ab_group):
    """세그먼트 조합별 목표 CTR 반환"""
    
    # 정의된 세그먼트 조합인 경우 목표 CTR 사용
    if segment_key in AB_TEST_SEGMENT_TARGETS:
        targets = AB_TEST_SEGMENT_TARGETS[segment_key]
        if ab_group == 'treatment':
            return targets['target']
        else:
            return targets['current']
    
    # 기본 CTR 사용
    if ab_group == 'treatment':
        return AB_TEST_TREATMENT_CTR
    else:
        return AB_TEST_CONTROL_CTR

def apply_ab_test_logic_v2(event_name, properties, user_data, session_time):
    """성숙 단계 서비스 AB 테스트 로직 적용"""
    
    # AB 테스트 기간이 아니면 원래 속성 반환
    if not is_ab_test_period(session_time.date()):
        return properties
    
    # 광고 관련 이벤트에만 AB 테스트 적용
    if event_name not in ['view_ads', 'click_ads']:
        return properties
    
    # 사용자 그룹 결정
    ab_group = assign_ab_test_group(user_data['id'])
    segment_key = get_segment_combination_key(user_data)
    
    # AB 테스트 속성 추가
    properties['ab_test_scenario'] = AB_TEST_SCENARIO_CODE
    properties['ab_test_group'] = ab_group
    properties['user_segment_combination'] = f"{segment_key[0]}_{segment_key[1]}_{segment_key[2]}"
    
    # 광고 타겟팅 방식 적용
    if event_name == 'view_ads':
        if ab_group == 'treatment':
            # Treatment 그룹: 행동 태그 기반 타겟팅
            properties['ad_targeting_method'] = 'behavioral_targeting'
            
            # 사용자의 요리 스타일에 맞는 태그 생성
            cooking_style = user_data.get('cooking_style_persona', '')
            if cooking_style == 'DESSERT_FOCUSED':
                properties['targeting_tags'] = ['dessert_lover', 'baking_tools', 'sweet_ingredients']
            elif cooking_style == 'HEALTHY_CONSCIOUS':
                properties['targeting_tags'] = ['healthy_food', 'diet_conscious', 'organic_ingredients']
            elif cooking_style == 'QUICK_CONVENIENT':
                properties['targeting_tags'] = ['quick_meal', 'time_saving', 'easy_cooking']
            elif cooking_style == 'COMFORT_FOOD':
                properties['targeting_tags'] = ['hearty_meals', 'family_cooking', 'comfort_food']
            else:  # DIVERSE_EXPLORER
                properties['targeting_tags'] = ['premium_ingredients', 'exotic_recipes', 'cooking_challenge']
            
            properties['personalization_score'] = round(random.uniform(0.7, 0.95), 2)
        else:
            # Control 그룹: 랜덤 광고 서빙
            properties['ad_targeting_method'] = 'random_serving'
            properties['targeting_tags'] = []
            properties['personalization_score'] = round(random.uniform(0.1, 0.3), 2)
    
    elif event_name == 'click_ads':
        # 세그먼트별 목표 CTR 적용
        target_ctr = get_target_ctr_for_segment(segment_key, ab_group)
        
        if random.random() < target_ctr:
            properties['click_predicted'] = True
            properties['targeting_success'] = (ab_group == 'treatment')
            
            if ab_group == 'treatment':
                properties['relevance_score'] = round(random.uniform(0.8, 0.95), 2)
                properties['targeting_method_used'] = 'behavioral_targeting'
            else:
                properties['relevance_score'] = round(random.uniform(0.3, 0.6), 2)
                properties['targeting_method_used'] = 'random_serving'
        else:
            properties['click_predicted'] = False
            properties['targeting_success'] = False
    
    return properties

print("✅ 성숙 단계 서비스 AB 테스트 관련 함수 준비 완료")
print(f"🧪 AB 테스트 정보:")
print(f"   - 테스트 기간: {AB_TEST_START_DATE.strftime('%Y-%m-%d')} ~ {AB_TEST_END_DATE.strftime('%Y-%m-%d')} (한국시간)")
print(f"   - 시나리오: {AB_TEST_SCENARIO_CODE}")
print(f"   - 대상 이벤트: view_ads, click_ads")
print(f"   - 세그먼트별 차등 목표 CTR 적용")

print(f"\n📊 세그먼트별 목표 CTR:")
for segment_combo, targets in AB_TEST_SEGMENT_TARGETS.items():
    demographic, activity, cooking = segment_combo
    current_ctr = targets['current']
    target_ctr = targets['target']
    improvement = ((target_ctr - current_ctr) / current_ctr) * 100
    
    print(f"   - {demographic} × {activity} × {cooking}:")
    print(f"     Control: {current_ctr:.1%} → Treatment: {target_ctr:.1%} (+{improvement:.0f}%)")



# ===================================================================
# 4️⃣ 성숙 단계 서비스 사용자 세그먼트 할당 (3개 그룹)
# ===================================================================

def assign_mature_service_user_segments(users_df, profiles_df):
    """성숙 단계 서비스의 사용자 세그먼트 할당"""
    
    print("🎭 성숙 단계 서비스 사용자 세그먼트 할당 시작...")
    
    # 사용자와 프로필 병합
    merged_df = pd.merge(users_df, profiles_df, left_on='id', right_on='user_id', how='inner')
    
    # 기존 Demographic Segment 유지 (성별×연령대)
    segment_list = list(DEMOGRAPHIC_DISTRIBUTION.keys())
    segment_weights = list(DEMOGRAPHIC_DISTRIBUTION.values())
    
    merged_df['demographic_segment'] = np.random.choice(
        segment_list, 
        size=len(merged_df), 
        p=segment_weights
    )
    
    # 새로운 활동 수준 세그먼트 (3개 그룹)
    activity_segments = list(USER_SEGMENTS.keys())
    activity_weights = [segment['ratio'] for segment in USER_SEGMENTS.values()]
    
    merged_df['activity_segment'] = np.random.choice(
        activity_segments,
        size=len(merged_df),
        p=activity_weights
    )
    
    # 요리 스타일 선호도 유지 (기존 5개 그룹)
    cooking_list = list(COOKING_STYLE_PERSONAS.keys())
    cooking_weights = [persona['ratio'] for persona in COOKING_STYLE_PERSONAS.values()]
    
    merged_df['cooking_style_persona'] = np.random.choice(
        cooking_list,
        size=len(merged_df),
        p=cooking_weights
    )
    
    print(f"✅ 세그먼트 할당 완료: {len(merged_df):,}명")
    
    # 분포 확인
    print(f"\n📊 Demographic Segment 분포:")
    demographic_dist = merged_df['demographic_segment'].value_counts(normalize=True)
    for segment, ratio in demographic_dist.items():
        print(f"   - {segment}: {ratio:.1%}")
    
    print(f"\n⚡ 활동 수준 세그먼트 분포:")
    activity_dist = merged_df['activity_segment'].value_counts(normalize=True)
    for segment, ratio in activity_dist.items():
        desc = USER_SEGMENTS[segment]['description']
        daily_events = USER_SEGMENTS[segment]['daily_events']
        print(f"   - {segment}: {ratio:.1%} (일평균 {daily_events[0]}-{daily_events[1]}개)")
        print(f"     └ {desc}")
    
    print(f"\n🍳 요리 스타일 분포:")
    cooking_dist = merged_df['cooking_style_persona'].value_counts(normalize=True)
    for cooking, ratio in cooking_dist.items():
        print(f"   - {cooking}: {ratio:.1%}")
    
    return merged_df

print("✅ 성숙 단계 서비스 사용자 세그먼트 할당 함수 준비 완료")
print("🔄 변경사항:")
print("   - 이용 강도 → 활동 수준 (3개 그룹)")
print("   - POWER_USER(10%), ACTIVE_EXPLORER(60%), PASSIVE_BROWSER(30%)")
print("   - 각 그룹별 일평균 이벤트 수 차등 적용")



# ===================================================================
# 🗂️ EVENT_SCHEMA 정의 (다음 이벤트 로직)
# ===================================================================

EVENT_SCHEMA = {
    'view_page': {
        'next_events': ['search_recipe', 'view_recipe_list', 'view_ads', 'click_auth_button']
    },
    'click_auth_button': {
        'next_events': ['auth_success', 'view_page']
    },
    'auth_success': {
        'next_events': ['view_page', 'view_recipe_list']
    },
    'search_recipe': {
        'next_events': ['view_recipe_list', 'click_recipe', 'view_page']
    },
    'view_recipe_list': {
        'next_events': ['click_recipe', 'search_recipe', 'view_page']
    },
    'click_recipe': {
        'next_events': ['click_bookmark', 'click_like', 'create_comment', 'view_page']
    },
    'click_bookmark': {
        'next_events': ['view_page', 'view_recipe_list', 'click_like']
    },
    'click_like': {
        'next_events': ['view_page', 'view_recipe_list', 'create_comment']
    },
    'create_comment': {
        'next_events': ['view_page', 'view_recipe_list']
    },
    'create_recipe_success': {
        'next_events': ['view_page', 'view_recipe_list']
    },
    'view_ads': {
        'next_events': ['click_ads', 'view_page', 'view_recipe_list']
    },
    'click_ads': {
        'next_events': ['view_page']
    }
}

print("✅ EVENT_SCHEMA 정의 완료")
print(f"📊 정의된 이벤트 수: {len(EVENT_SCHEMA)}")



# 🔍 데이터프레임 크기 진단
print("📊 데이터프레임 크기 확인:")
print(f"recipes_df 크기: {len(recipes_df):,} 행")
print(f"recipes_df 메모리 사용량: {recipes_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
print(f"users_df 크기: {len(users_df):,} 행")
print(f"profiles_df 크기: {len(profiles_df):,} 행")

print("\n🔍 recipes_df 컬럼 확인:")
print(f"컬럼들: {list(recipes_df.columns)}")
print(f"id 컬럼 데이터 타입: {recipes_df['id'].dtype}")
print(f"id 컬럼 샘플: {recipes_df['id'].head(3).tolist()}")

# 성능 테스트: 랜덤 선택 속도 측정
import time

print("\n⚡ 성능 테스트:")
start_time = time.time()
for i in range(100):
    if len(recipes_df) > 1000:
        random_idx = random.randint(0, len(recipes_df) - 1)
        test_id = recipes_df.iloc[random_idx]['id']
    else:
        test_id = recipes_df.sample(1)['id'].iloc[0]
end_time = time.time()
print(f"100번 랜덤 선택 시간: {(end_time - start_time)*1000:.1f} ms")



# ===================================================================
#  Kafka 전송 관련 함수들 (새로 추가)
# ===================================================================

def delivery_report(err, msg):
    """ 메시지 전송 완료 후 호출되는 콜백. 전송 성공/실패를 로그로 남깁니다. """
    if err is not None:
        print(f"❌ 메시지 전송 실패: {err}")

def send_df_to_kafka(df: pd.DataFrame, topic: str, bootstrap_servers: str):
    """ Pandas DataFrame을 JSON 메시지로 변환하여 Kafka 토픽으로 전송합니다. (confluent-kafka 사용) """
    conf = {'bootstrap.servers': bootstrap_servers, 'client.id': socket.gethostname()}
    producer = Producer(conf)
    
    print(f"🚀 Confluent Kafka Producer가 브로커({bootstrap_servers})에 연결을 시도합니다.")
    print(f"   - 토픽 '{topic}'으로 {len(df):,}개의 이벤트를 전송합니다...")

    records = df.to_dict('records')
    for record in records:
        try:
            producer.produce(
                topic,
                value=json.dumps(record, ensure_ascii=False).encode('utf-8'),
                callback=delivery_report
            )
        except BufferError:
            producer.flush()
            producer.produce(
                topic,
                value=json.dumps(record, ensure_ascii=False).encode('utf-8'),
                callback=delivery_report
            )
    
    remaining = producer.flush()
    if remaining > 0:
         print(f"⚠️ {remaining}개의 메시지가 아직 전송 대기 중입니다.")
    
    print(f"✅ {len(df):,}개의 이벤트 전송 요청 완료!")



# ===================================================================
# 5️⃣ 성숙 단계 서비스 이벤트 생성 핵심 함수들 (한국시간 적용)
# ===================================================================

def generate_event_properties_v2(event_name, context, recipes_df, user_data=None, session_time=None):
    """성숙 단계 서비스용 이벤트 속성 생성 (정확한 스키마 반영)"""
    
    properties = {}
    
    if event_name == 'view_page':
        pages = ['start', 'main', 'recipe_detail', 'profile', 'search_result']
        properties['page_name'] = context.get('page_name', random.choice(pages))
        
        if random.random() < 0.3:
            properties['referrer'] = random.choice(['https://google.com', 'https://naver.com', ''])
        
        if properties['page_name'] == 'recipe_detail' and context.get('recipe_id'):
            properties['path'] = f"/recipes/{context['recipe_id']}"
    
    elif event_name == 'click_auth_button':
        properties['type'] = random.choice(['signup', 'login'])
    
    elif event_name == 'auth_success':
        properties['method'] = random.choice(['email', 'kakao', 'google', 'naver'])
        properties['type'] = random.choice(['signup', 'login'])
    
    elif event_name == 'search_recipe':
        properties['search_type'] = random.choice(['category', 'ingredient', 'menu'])
        
        if random.random() < 0.7:
            keywords = ['치킨', '파스타', '샐러드', '스테이크', '케이크', '볶음밥', '국물요리']
            properties['search_keyword'] = random.choice(keywords)
        
        if random.random() < 0.4:
            # 실제 recipes_df 데이터 기반 필터링
            filters_config = {
                'dish_type': ['밑반찬', '메인반찬','국/탕', '찌개', '디저트', '면/만두', '밥/죽/떡', '퓨전', '김치/젓갈/장류', '양념/소스/잼', '양식', '샐러드', '스프', '빵', '과자', '차/음료/술', '기타'], 
                'situation_type': ['일상', '초스피드', '손님접대', '술안주', '다이어트', '도시락', '영양식', '간식', '야식', '푸드스타일링', '해장', '명절', '이유식', '기타'],
                'ingredient_type': ['소고기', '돼지고기', '닭고기', '육류', '채소류', '해물류', '달걀/유제품', '가공식품류', '쌀', '밀가루', '건어물류', '버섯류', '과일류', '콩/견과류', '곡류', '기타'],
                'method_type': ['볶음', '끓이기', '부침', '조림', '무침', '비빔', '찜', '절임', '튀김', '삶기', '굽기', '데치기', '회', '기타']
            }
            
            # 실제 recipes_df에서 사용 가능한 필터들만 선택
            available_filters = []
            for filter_type, filter_values in filters_config.items():
                if not recipes_df.empty and filter_type in recipes_df.columns:
                    # 해당 컬럼에 실제 존재하는 값들 중에서 선택
                    actual_values = recipes_df[filter_type].dropna().unique()
                    matching_values = [v for v in filter_values if v in actual_values]
                    if matching_values:
                        selected_value = random.choice(matching_values)
                        available_filters.append(f"{filter_type}:{selected_value}")
            
            # 필터가 있으면 1-2개 선택, 없으면 기본값 사용
            if available_filters:
                properties['selected_filters'] = random.sample(available_filters, min(random.randint(1, 4), len(available_filters)))
            else:
                # 폴백: recipes_df가 비어있거나 컬럼이 없을 때
                fallback_filters = ['한식', '양식', '중식', '돼지고기', '닭고기', '소고기', '간단요리', '복잡요리']
                properties['selected_filters'] = random.sample(fallback_filters, random.randint(1, 2))
        
        # result_count도 실제 필터링 결과에 기반하도록 개선
        if 'selected_filters' in properties and not recipes_df.empty:
            # 필터 조건에 맞는 레시피 수 계산 (시뮬레이션)
            estimated_results = random.randint(1, min(50, len(recipes_df) // 10))
            properties['result_count'] = max(1, estimated_results)  # 최소 1개는 보장
        else:
            properties['result_count'] = random.randint(5, 50)
    
    elif event_name == 'view_recipe_list':
        list_types = ['popular', 'recommended', 'search_result', 'trending']
        properties['list_type'] = random.choice(list_types)
        
        displayed_count = random.randint(5, 20)
        
        # context에서 이전 검색 필터 정보 활용
        context_filters = context.get('search_filters', [])
        
        if not recipes_df.empty and 'id' in recipes_df.columns and len(recipes_df) > 0:
            # 검색 필터가 있다면 해당 조건에 맞는 레시피들 우선 선택
            if context_filters and properties['list_type'] == 'search_result':
                filtered_recipes = recipes_df.copy()
                
                # 각 필터 조건 적용
                for filter_item in context_filters:
                    if ':' in filter_item:
                        filter_type, filter_value = filter_item.split(':', 1)
                        if filter_type in filtered_recipes.columns:
                            filtered_recipes = filtered_recipes[
                                filtered_recipes[filter_type].astype(str).str.contains(filter_value, na=False)
                            ]
                
                # 필터링된 결과가 있으면 그 중에서 선택
                if len(filtered_recipes) > 0:
                    if len(filtered_recipes) > displayed_count:
                        if len(filtered_recipes) > 1000:
                            sample_indices = random.sample(range(len(filtered_recipes)), displayed_count)
                            recipe_ids = filtered_recipes.iloc[sample_indices]['id'].tolist()
                        else:
                            recipe_sample = filtered_recipes.sample(n=displayed_count)
                            recipe_ids = recipe_sample['id'].tolist()
                    else:
                        recipe_ids = filtered_recipes['id'].tolist()
                    properties['displayed_recipe_ids'] = [str(x) for x in recipe_ids]
                else:
                    # 필터링 결과가 없으면 전체에서 랜덤 선택
                    if len(recipes_df) > 1000:
                        sample_indices = random.sample(range(len(recipes_df)), min(displayed_count, len(recipes_df)))
                        recipe_ids = recipes_df.iloc[sample_indices]['id'].tolist()
                    else:
                        recipe_sample = recipes_df.sample(n=min(displayed_count, len(recipes_df)))
                        recipe_ids = recipe_sample['id'].tolist()
                    properties['displayed_recipe_ids'] = [str(x) for x in recipe_ids]
            else:
                # 일반적인 목록 (인기, 추천 등) - 전체에서 랜덤 선택
                if len(recipes_df) > 1000:
                    sample_indices = random.sample(range(len(recipes_df)), min(displayed_count, len(recipes_df)))
                    recipe_ids = recipes_df.iloc[sample_indices]['id'].tolist()
                else:
                    recipe_sample = recipes_df.sample(n=min(displayed_count, len(recipes_df)))
                    recipe_ids = recipe_sample['id'].tolist()
                properties['displayed_recipe_ids'] = [str(x) for x in recipe_ids]
        else:
            # 빈 데이터프레임일 때 가상 ID 생성
            properties['displayed_recipe_ids'] = [f"recipe_{random.randint(1, 1000)}" for _ in range(displayed_count)]
    
    elif event_name == 'click_recipe':  # 스키마에 맞게 이벤트명 변경
        # 이전 view_recipe_list에서 표시된 레시피들 중에서 선택 (더 현실적)
        displayed_recipes = context.get('displayed_recipe_ids', [])
        
        if displayed_recipes:
            # 표시된 레시피 중 하나를 클릭
            properties['recipe_id'] = random.choice(displayed_recipes)
            # 클릭된 레시피의 목록 내 순위
            properties['rank'] = displayed_recipes.index(properties['recipe_id']) + 1
        elif context and context.get('recipe_id'):
            # context에 recipe_id가 있으면 사용
            properties['recipe_id'] = str(context['recipe_id']) if pd.notna(context['recipe_id']) else None
            properties['rank'] = random.randint(1, 20)
        elif not recipes_df.empty and 'id' in recipes_df.columns and len(recipes_df) > 0:
            # 성능 최적화: 큰 데이터프레임에서는 인덱스 기반 선택 사용
            if len(recipes_df) > 1000:
                random_idx = random.randint(0, len(recipes_df) - 1)
                recipe_id = recipes_df.iloc[random_idx]['id']
            else:
                recipe_id = recipes_df.sample(1)['id'].iloc[0]
            properties['recipe_id'] = str(recipe_id) if pd.notna(recipe_id) else None
            properties['rank'] = random.randint(1, 20)
        else:
            properties['recipe_id'] = f"recipe_{random.randint(1, 1000)}"
            properties['rank'] = random.randint(1, 20)
    
    elif event_name == 'click_bookmark':
        if context and context.get('recipe_id'):
            properties['recipe_id'] = str(context['recipe_id'])
        elif not recipes_df.empty and 'id' in recipes_df.columns and len(recipes_df) > 0:
            # 성능 최적화: 큰 데이터프레임에서는 인덱스 기반 선택 사용
            if len(recipes_df) > 1000:
                random_idx = random.randint(0, len(recipes_df) - 1)
                recipe_id = recipes_df.iloc[random_idx]['id']
            else:
                recipe_id = recipes_df.sample(1)['id'].iloc[0]
            properties['recipe_id'] = str(recipe_id)
        else:
            properties['recipe_id'] = f"recipe_{random.randint(1, 1000)}"
        
        properties['action'] = random.choice(['add', 'remove'])
    
    elif event_name == 'click_like':
        if context and context.get('recipe_id'):
            properties['recipe_id'] = str(context['recipe_id'])
        elif not recipes_df.empty and 'id' in recipes_df.columns and len(recipes_df) > 0:
            # 성능 최적화: 큰 데이터프레임에서는 인덱스 기반 선택 사용
            if len(recipes_df) > 1000:
                random_idx = random.randint(0, len(recipes_df) - 1)
                recipe_id = recipes_df.iloc[random_idx]['id']
            else:
                recipe_id = recipes_df.sample(1)['id'].iloc[0]
            properties['recipe_id'] = str(recipe_id)
        else:
            properties['recipe_id'] = f"recipe_{random.randint(1, 1000)}"
        
        properties['action'] = random.choice(['like', 'unlike'])
    
    elif event_name == 'create_comment':
        if context and context.get('recipe_id'):
            properties['recipe_id'] = str(context['recipe_id'])
        elif not recipes_df.empty and 'id' in recipes_df.columns and len(recipes_df) > 0:
            # 성능 최적화: 큰 데이터프레임에서는 인덱스 기반 선택 사용
            if len(recipes_df) > 1000:
                random_idx = random.randint(0, len(recipes_df) - 1)
                recipe_id = recipes_df.iloc[random_idx]['id']
            else:
                recipe_id = recipes_df.sample(1)['id'].iloc[0]
            properties['recipe_id'] = str(recipe_id)
        else:
            properties['recipe_id'] = f"recipe_{random.randint(1, 1000)}"
        
        properties['comment_length'] = random.randint(10, 200)
    
    elif event_name == 'create_recipe_success':
        # 새로 생성된 레시피 ID (실제로는 기존 레시피 참조)
        if not recipes_df.empty and 'id' in recipes_df.columns and len(recipes_df) > 0:
            if len(recipes_df) > 1000:
                random_idx = random.randint(0, len(recipes_df) - 1)
                recipe_id = recipes_df.iloc[random_idx]['id']
            else:
                recipe_id = recipes_df.sample(1)['id'].iloc[0]
            properties['recipe_id'] = str(recipe_id)
        else:
            properties['recipe_id'] = f"recipe_{random.randint(1000, 9999)}"
        
        # 실제 recipes_df의 dish_type 컬럼 활용
        if random.random() < 0.7:
            if not recipes_df.empty and 'dish_type' in recipes_df.columns:
                # 실제 데이터에서 사용되는 카테고리들 중 선택
                actual_categories = recipes_df['dish_type'].dropna().unique()
                if len(actual_categories) > 0:
                    properties['category'] = random.choice(actual_categories)
                else:
                    # 폴백 카테고리
                    properties['category'] = random.choice(['한식', '양식', '중식', '일식', '분식', '디저트', '음료'])
            else:
                # 폴백 카테고리
                properties['category'] = random.choice(['한식', '양식', '중식', '일식', '분식', '디저트', '음료'])
        
        # 재료 개수는 실제 ingredient_list 컬럼이 있다면 참조
        if not recipes_df.empty and 'ingredient_list' in recipes_df.columns:
            # 실제 레시피의 재료 개수 분포 참조
            sample_recipe = recipes_df.sample(1).iloc[0] if len(recipes_df) > 0 else None
            if sample_recipe is not None and pd.notna(sample_recipe.get('ingredient_list')):
                try:
                    # ingredient_list가 JSON 형태라면 파싱해서 개수 계산
                    import json
                    ingredients = json.loads(sample_recipe['ingredient_list'])
                    if isinstance(ingredients, list):
                        properties['ingredient_count'] = max(1, len(ingredients))
                    else:
                        properties['ingredient_count'] = random.randint(3, 15)
                except:
                    properties['ingredient_count'] = random.randint(3, 15)
            else:
                properties['ingredient_count'] = random.randint(3, 15)
        else:
            properties['ingredient_count'] = random.randint(3, 15)
    
    elif event_name == 'view_ads':
        properties['ad_id'] = f"ad_{random.randint(1000, 9999)}"
        properties['ad_type'] = random.choice(['banner', 'video', 'native', 'sponsored_recipe'])
        properties['position'] = random.choice(['top', 'middle', 'bottom', 'sidebar', 'recipe_detail'])
    
    elif event_name == 'click_ads':
        properties['ad_id'] = context.get('ad_id', f"ad_{random.randint(1000, 9999)}")
        properties['ad_type'] = random.choice(['banner', 'video', 'native', 'sponsored_recipe'])
        properties['position'] = random.choice(['top', 'middle', 'bottom', 'sidebar', 'recipe_detail'])
        properties['target_url'] = f"https://naver.com/promotion/{random.randint(1, 100)}"
    
    # AB 테스트 로직 적용 (user_data와 session_time이 있을 때)
    if user_data is not None and session_time is not None:
        properties = apply_ab_test_logic_v2(event_name, properties, user_data, session_time)
    
    return properties

def generate_mature_service_session_flow(user_data, session_time, recipes_df):
    """성숙 단계 서비스의 사용자 세션 플로우 생성"""
    
    session_id = str(uuid.uuid4())
    events = []
    current_time = session_time
    context = {}
    
    # 새로운 활동 수준 세그먼트별 세션 길이 결정
    activity_segment = user_data['activity_segment']
    daily_events_range = USER_SEGMENTS[activity_segment]['daily_events']
    
    # 세션당 이벤트 수 (일평균의 1/2 ~ 1/3 정도)
    session_lengths = {
        'POWER_USER': random.randint(15, 25),      # 40-50 일평균 → 15-25 세션당
        'ACTIVE_EXPLORER': random.randint(7, 12),  # 15-20 일평균 → 7-12 세션당
        'PASSIVE_BROWSER': random.randint(3, 6)    # 5-10 일평균 → 3-6 세션당
    }
    
    max_events = session_lengths.get(activity_segment, 5)
    
    # 세션 시작 이벤트
    start_events = ['view_page', 'click_auth_button']
    current_event = random.choice(start_events)
    
    for _ in range(max_events):
        # 이벤트 속성 생성 (새로운 함수 사용)
        properties = generate_event_properties_v2(
            current_event, 
            context, 
            recipes_df, 
            user_data=user_data,
            session_time=current_time
        )
        
        # 현재 페이지 정보 설정
        page_name = properties.get('page_name', 'main')
        page_url = f"https://reciping.co.kr/{page_name}"
        page_path = f"/{page_name}"
        
        # context 객체 구성 (한국시간 적용)
        context_obj = {
            "page": {
                "name": page_name,
                "url": page_url,
                "path": page_path
            },
            "user_segment": str(user_data['demographic_segment']),
            "activity_level": str(user_data['activity_segment']),
            "cooking_style": str(user_data['cooking_style_persona'])
        }
        
        # AB 테스트 기간이면 context에 AB 테스트 정보 추가
        if is_ab_test_period(current_time.date()):
            ab_group = assign_ab_test_group(user_data['id'])
            context_obj['ab_test'] = {
                "scenario": AB_TEST_SCENARIO_CODE,
                "group": ab_group,
                "start_date": AB_TEST_START_DATE.strftime('%Y-%m-%d'),
                "end_date": AB_TEST_END_DATE.strftime('%Y-%m-%d')
            }
        
        # anonymous_id 생성
        anonymous_id = str(user_data.get('anonymous_id', ''))
        if not anonymous_id or anonymous_id == '':
            anonymous_id = str(uuid.uuid4())
        
        # 이벤트 기록 (한국시간 적용)
        event = {
            'event_name': current_event,
            'event_id': str(uuid.uuid4()),
            'user_id': str(user_data['id']) if pd.notna(user_data['id']) else None,
            'anonymous_id': anonymous_id,
            'session_id': session_id,
            'context': json.dumps(context_obj, ensure_ascii=False),
            'event_properties': json.dumps(properties, default=str, ensure_ascii=False),
            'timestamp': get_korean_timestamp(current_time)
        }
        
        events.append(event)
        
        # 컨텍스트 업데이트 (다음 이벤트에서 활용)
        if 'recipe_id' in properties and properties['recipe_id'] is not None:
            context['recipe_id'] = str(properties['recipe_id'])
        if 'ad_id' in properties:
            context['ad_id'] = str(properties['ad_id'])
        
        # 검색 필터 정보 저장 (view_recipe_list에서 활용)
        if current_event == 'search_recipe' and 'selected_filters' in properties:
            context['search_filters'] = properties['selected_filters']
        
        # 표시된 레시피 목록 저장 (click_recipe에서 활용)
        if current_event == 'view_recipe_list' and 'displayed_recipe_ids' in properties:
            context['displayed_recipe_ids'] = properties['displayed_recipe_ids']
        
        # 다음 이벤트 결정 (스키마 기반)
        schema = EVENT_SCHEMA.get(current_event, {})
        next_events = schema.get('next_events', ['view_page'])
        
        # 레시피 클릭 후 이벤트 흐름 개선
        if current_event == 'view_recipe_list' and random.random() < 0.3:
            current_event = 'click_recipe'
        elif current_event == 'click_recipe' and random.random() < 0.4:
            current_event = random.choice(['click_bookmark', 'click_like', 'create_comment'])
        elif next_events and random.random() < 0.8:
            current_event = random.choice(next_events)
        else:
            current_event = random.choice(['view_page', 'search_recipe', 'view_recipe_list'])
        
        # 시간 증가 (5초 ~ 2분)
        current_time += timedelta(seconds=random.randint(5, 120))
    
    return events

print("✅ 성숙 단계 서비스 이벤트 생성 함수들 준비 완료")
print("📝 주요 변경사항:")
print("   - 새로운 활동 수준 세그먼트 적용")
print("   - 한국시간(KST) 타임스탬프 생성")
print("   - KPI 목표 수준 반영 (상세 페이지 전환율 10%)")
print("   - view_recipe_detail 이벤트 추가")



# ===================================================================
# 🧪 샘플 이벤트 로그 생성 테스트
# ===================================================================

def generate_sample_events_test(num_users=5, events_per_user=10):
    """개선된 로직 테스트를 위한 소량 샘플 이벤트 생성"""
    
    print("🧪 샘플 이벤트 로그 생성 테스트 시작")
    print("=" * 60)
    
    # 테스트용 사용자 샘플
    test_users = users_df.sample(n=min(num_users, len(users_df)))
    
    # 임시로 사용자 세그먼트 할당 (테스트용)
    test_profiles = []
    for _, user_row in test_users.iterrows():
        profile = {
            'id': user_row['id'],
            'activity_segment': random.choice(['POWER_USER', 'ACTIVE_EXPLORER', 'PASSIVE_BROWSER']),
            'demographic_segment': random.choice(['20대 남성', '30대 여성', '40대 남성']),
            'cooking_style_persona': random.choice(['간편요리족', '정통요리족', '실험요리족'])
        }
        test_profiles.append(profile)
    
    print(f"📊 테스트 설정:")
    print(f"   사용자 수: {len(test_users)}")
    print(f"   사용자당 이벤트 수: {events_per_user}")
    print(f"   recipes_df 크기: {len(recipes_df):,}개")
    print(f"   recipes_df 컬럼: {list(recipes_df.columns)}")
    
    # recipes_df 컬럼별 고유값 확인
    print(f"\n🔍 필터 관련 컬럼 데이터 확인:")
    filter_columns = ['dish_type', 'situation_type', 'ingredient_type', 'method_type']
    for col in filter_columns:
        if col in recipes_df.columns:
            unique_vals = recipes_df[col].dropna().unique()
            print(f"   {col}: {len(unique_vals)}개 고유값 - {list(unique_vals[:5])}{'...' if len(unique_vals) > 5 else ''}")
        else:
            print(f"   {col}: 컬럼 없음")
    
    all_events = []
    
    print(f"\n📝 이벤트 생성 중...")
    
    for idx, (user_row, user_profile) in enumerate(zip(test_users.itertuples(), test_profiles)):
        
        # 테스트용 간단한 세션 시간
        session_time = datetime.now(KST).replace(
            hour=random.randint(9, 21),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        
        print(f"   사용자 {user_row.id} ({user_profile['activity_segment']}) 이벤트 생성 중...")
        
        # 간단한 이벤트 시퀀스 생성
        events = []
        context = {}
        current_time = session_time
        
        # 다양한 이벤트 타입으로 테스트
        event_sequence = ['view_page', 'search_recipe', 'view_recipe_list', 'click_recipe', 'click_bookmark', 'view_ads', 'click_ads']
        selected_events = random.sample(event_sequence, min(events_per_user, len(event_sequence)))
        
        for event_name in selected_events:
            # 이벤트 속성 생성
            properties = generate_event_properties_v2(
                event_name, 
                context, 
                recipes_df, 
                user_data=user_profile,
                session_time=current_time
            )
            
            # 이벤트 기록
            event = {
                'event_name': event_name,
                'event_id': str(uuid.uuid4()),
                'user_id': str(user_row.id),
                'session_id': str(uuid.uuid4()),
                'timestamp': get_korean_timestamp(current_time),
                'properties': properties
            }
            
            events.append(event)
            
            # 컨텍스트 업데이트
            if 'recipe_id' in properties:
                context['recipe_id'] = properties['recipe_id']
            if 'selected_filters' in properties:
                context['search_filters'] = properties['selected_filters']
            if 'displayed_recipe_ids' in properties:
                context['displayed_recipe_ids'] = properties['displayed_recipe_ids']
            
            # 시간 증가
            current_time += timedelta(seconds=random.randint(10, 60))
        
        all_events.extend(events)
    
    print(f"\n✅ 총 {len(all_events)}개 이벤트 생성 완료!")
    
    return all_events

# 샘플 이벤트 생성 및 분석
sample_events = generate_sample_events_test(num_users=3, events_per_user=8)



# ===================================================================
# 🚀 Dask 활용 병렬처리 10만개 이벤트 로그 생성 시스템
# ===================================================================

import dask
from dask.distributed import Client, as_completed
from dask import delayed
import time
from datetime import datetime
import pandas as pd

print("🚀 Dask 활용 병렬처리 이벤트 생성 시스템")
print("=" * 60)

# 1. 사용자 세그먼트 할당 (실제 데이터 기반)
print("👥 사용자 세그먼트 할당 중...")
segmented_users_df = assign_mature_service_user_segments(users_df, profiles_df)
print(f"✅ {len(segmented_users_df):,}명 사용자 세그먼트 할당 완료")

@delayed
def generate_events_batch_optimized(user_batch, recipes_sample, batch_id, events_per_user=5):
    """
    Dask delayed 함수: 사용자 배치별 이벤트 생성 (최적화 + 시간대/날짜 가중치)
    """
    import random
    import uuid
    from datetime import datetime, timedelta
    import numpy as np
    
    # 배치별 고유 시드 설정
    random.seed(42 + batch_id)
    np.random.seed(42 + batch_id)
    
    # 시간대별 가중치 설정 (현실적인 사용자 활동 패턴 반영)
    hour_weights = {
        0: 0.05, 1: 0.03, 2: 0.02, 3: 0.02, 4: 0.03, 5: 0.08,  # 새벽 (2-8%)
        6: 0.25, 7: 0.45, 8: 0.60, 9: 0.75, 10: 0.85, 11: 0.90,  # 오전 (25-90%)
        12: 1.00, 13: 0.95, 14: 0.85, 15: 0.80, 16: 0.85, 17: 0.90,  # 오후 (80-100%)
        18: 0.95, 19: 1.00, 20: 0.95, 21: 0.85, 22: 0.70, 23: 0.35   # 저녁~밤 (35-100%)
    }
    
    # 요일별 가중치 설정 (주말 활성화)
    weekday_weights = {
        0: 0.8,   # 월요일: 80%
        1: 0.9,   # 화요일: 90%
        2: 0.95,  # 수요일: 95%
        3: 1.0,   # 목요일: 100%
        4: 1.1,   # 금요일: 110% (주말 준비)
        5: 1.3,   # 토요일: 130% (주말 피크)
        6: 1.2    # 일요일: 120% (주말 지속)
    }
    
    # 가중치 기반 시간 선택을 위한 확률 분포 생성
    hours = list(range(24))
    base_hour_weights = [hour_weights[hour] for hour in hours]
    
    batch_events = []
    
    # 각 사용자별로 세션 생성
    for _, user_row in user_batch.iterrows():
        try:
            # 사용자 데이터 준비
            user_data = user_row.to_dict()
            
            # 세션 날짜 생성 (6~8월, 요일별 가중치 적용)
            summer_dates_with_weights = []
            
            for month in [6, 7, 8]:  # 6월, 7월, 8월
                if month == 6:
                    days = range(1, 31)  # 6월: 30일
                elif month == 7:
                    days = range(1, 32)  # 7월: 31일
                else:  # month == 8
                    days = range(1, 32)  # 8월: 31일
                
                for day in days:
                    date_obj = datetime(2025, month, day)
                    weekday = date_obj.weekday()  # 0=월요일, 6=일요일
                    date_weight = weekday_weights[weekday]
                    
                    # 가중치만큼 날짜를 리스트에 추가 (확률적 선택을 위해)
                    repeat_count = int(date_weight * 10)  # 가중치를 정수로 변환
                    for _ in range(repeat_count):
                        summer_dates_with_weights.append(date_obj)
            
            session_date = random.choice(summer_dates_with_weights)
            weekday = session_date.weekday()
            
            # 요일에 따른 시간대 가중치 조정
            adjusted_hour_weights = []
            day_multiplier = weekday_weights[weekday]
            
            for hour_weight in base_hour_weights:
                # 주말에는 저녁 시간대(18-22시) 더 활성화
                if weekday >= 5 and 18 <= hours[base_hour_weights.index(hour_weight)] <= 22:
                    adjusted_weight = hour_weight * day_multiplier * 1.2  # 주말 저녁 추가 부스트
                else:
                    adjusted_weight = hour_weight * day_multiplier
                adjusted_hour_weights.append(adjusted_weight)
            
            # 가중치 기반 시간 선택
            total_weight = sum(adjusted_hour_weights)
            normalized_weights = [w/total_weight for w in adjusted_hour_weights]
            weighted_hour = np.random.choice(hours, p=normalized_weights)
            
            session_time = session_date.replace(
                hour=weighted_hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
                tzinfo=KST
            )
            
            # 활동 수준별 이벤트 수 결정 (주말에 추가 이벤트)
            activity_segment = user_data.get('activity_segment', 'ACTIVE_EXPLORER')
            base_events = 0
            
            if activity_segment == 'POWER_USER':
                base_events = random.randint(8, 12)
            elif activity_segment == 'ACTIVE_EXPLORER':
                base_events = random.randint(4, 8)
            else:  # PASSIVE_BROWSER
                base_events = random.randint(2, 5)
            
            # 주말 보너스 이벤트 (금~일요일에 10-20% 추가)
            if weekday >= 4:  # 금요일부터 일요일
                weekend_bonus = random.uniform(1.1, 1.2)
                user_events = int(base_events * weekend_bonus)
            else:
                user_events = base_events
            
            # 정교한 세션 플로우 생성
            session_events = generate_mature_service_session_flow(
                user_data, session_time, recipes_sample
            )
            
            # 목표 이벤트 수만큼 제한
            session_events = session_events[:user_events]
            batch_events.extend(session_events)
            
        except Exception as e:
            # 개별 사용자 오류는 로그만 남기고 계속 진행
            continue
    
    return {
        'batch_id': batch_id,
        'events': batch_events,
        'user_count': len(user_batch),
        'event_count': len(batch_events)
    }

def create_100k_events_with_dask(target_events=100_000, batch_size=2_000):
    """
    Dask를 활용한 병렬처리로 10만개 이벤트 생성
    """
    print(f"\n⚡ Dask 병렬처리로 {target_events:,}개 이벤트 생성 시작")
    print(f"📊 배치 크기: {batch_size:,}개씩 처리")
    
    start_time = time.time()
    
    # Dask 클라이언트 시작 (로컬 모드)
    try:
        client = Client(processes=True, n_workers=4, threads_per_worker=2, memory_limit='2GB')
        print(f"🔧 Dask 클러스터: {client}")
    except Exception as e:
        print(f"⚠️ Dask 클러스터 생성 실패, 스레드 모드로 대체: {e}")
        client = None
    
    # 레시피 샘플 준비 (메모리 효율성)
    recipes_sample = recipes_df.sample(n=min(10000, len(recipes_df)))
    print(f"🍽️ 레시피 샘플: {len(recipes_sample):,}개")
    
    # 사용자를 배치로 분할
    total_users_needed = target_events // 5  # 사용자당 평균 5개 이벤트
    users_sample = segmented_users_df.sample(n=min(total_users_needed, len(segmented_users_df)))
    
    # 사용자 배치 생성
    user_batches = []
    for i in range(0, len(users_sample), batch_size):
        batch = users_sample.iloc[i:i+batch_size]
        user_batches.append(batch)
    
    print(f"👥 총 사용자: {len(users_sample):,}명")
    print(f"📦 배치 수: {len(user_batches)}개")
    
    # Delayed 작업 생성
    print(f"⚙️ Delayed 작업 생성 중...")
    delayed_tasks = []
    for batch_id, user_batch in enumerate(user_batches):
        task = generate_events_batch_optimized(
            user_batch, 
            recipes_sample, 
            batch_id
        )
        delayed_tasks.append(task)
    
    # 병렬 실행
    print(f"🚀 {len(delayed_tasks)}개 배치 병렬 실행 시작...")
    
    if client:
        # Dask 클러스터 사용
        results = dask.compute(*delayed_tasks)
    else:
        # 로컬 스레드 사용
        with dask.config.set(scheduler='threads'):
            results = dask.compute(*delayed_tasks)
    
    # 결과 수집
    print(f"📊 결과 수집 및 정리 중...")
    all_events = []
    total_users = 0
    
    for result in results:
        all_events.extend(result['events'])
        total_users += result['user_count']
        
        # 진행상황 출력
        if len(all_events) % 10000 < 5000:  # 대략적인 진행상황
            print(f"   수집된 이벤트: {len(all_events):,}개...")
    
    # 목표 수만큼 제한
    all_events = all_events[:target_events]
    
    # DataFrame 변환
    print(f"🔄 DataFrame 변환 중...")
    events_df = pd.DataFrame(all_events)
    
    # 결과 정리
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n🎉 Dask 병렬처리 완료!")
    print(f"⏱️ 총 소요시간: {duration:.1f}초 ({duration/60:.1f}분)")
    print(f"📊 생성된 이벤트: {len(events_df):,}개")
    print(f"👥 참여 사용자: {total_users:,}명")
    print(f"⚡ 처리 속도: {len(events_df)/duration:.0f} events/sec")
    
    # 이벤트 타입 분포 확인
    if len(events_df) > 0:
        print(f"\n📈 이벤트 타입 분포:")
        event_dist = events_df['event_name'].value_counts()
        for event_type, count in event_dist.head(7).items():
            percentage = count / len(events_df) * 100
            print(f"   {event_type}: {count:,}개 ({percentage:.1f}%)")
    
    # 클라이언트 정리
    if client:
        client.close()
        print(f"🔧 Dask 클러스터 종료")
    
    return events_df

# 함수 준비 완료 메시지
print("✅ Dask 병렬처리 이벤트 생성 함수 준비 완료!")
print("💡 특징:")
print("   - 사용자 세그먼트별 차등 이벤트 수")
print("   - 실제 레시피 데이터 연동")
print("   - AB 테스트 로직 포함")
print("   - 한국시간 타임스탬프")
print("   - 여름 시즌(6~8월) 전체 커버")
print("   - 시간대별 가중치 적용 (현실적 활동 패턴)")
print("   - 📅 요일별 가중치 적용 (주말 활성화)")
print("   - 🎉 주말 보너스 이벤트 (금~일 10-20% 추가)")
print("   - 정교한 세션 플로우")
print("   - Dask 분산 처리로 성능 최적화")

# 시간대 및 요일별 가중치 패턴 설명
print("\n⏰ 시간대별 활동 가중치:")
print("   🌙 새벽 (0-5시): 2-8% (낮은 활동)")
print("   🌅 오전 (6-11시): 25-90% (점진적 증가)")
print("   ☀️ 점심 (12-17시): 80-100% (최고 활동)")
print("   🌆 저녁 (18-21시): 85-100% (높은 활동)")
print("   🌃 밤 (22-23시): 35-70% (점진적 감소)")

print("\n📅 요일별 활동 가중치:")
print("   📚 월요일: 80% (주초 낮은 활동)")
print("   💼 화-수요일: 90-95% (평일 보통)")
print("   🔥 목요일: 100% (평일 기준)")
print("   🍻 금요일: 110% (주말 준비 증가)")
print("   🎉 토요일: 130% (주말 최대 피크)")
print("   🛋️ 일요일: 120% (여유로운 주말)")



# ===================================================================
# 🎯 10분 간격 10만개 이벤트 로그 생성 시스템
# ===================================================================

def generate_events_by_time_window(window_start, window_end, target_events=100_000, batch_size=2_000):
    """
    특정 시간 윈도우(10분 간격) 내에서 정확히 지정된 개수의 이벤트 생성
    
    Args:
        window_start: 윈도우 시작 시간 (datetime)
        window_end: 윈도우 종료 시간 (datetime)
        target_events: 생성할 이벤트 수 (기본 10만개)
        batch_size: 배치 크기
    
    Returns:
        DataFrame: 생성된 이벤트들
    """
    import time
    import pandas as pd
    
    print(f"\n⏰ 시간 윈도우 이벤트 생성 시작")
    print(f"📅 기간: {window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {window_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 목표 이벤트: {target_events:,}개")
    
    start_time = time.time()
    
    # 레시피 샘플 준비
    recipes_sample = recipes_df.sample(n=min(5000, len(recipes_df)))
    
    # 사용자 샘플 준비 (목표 이벤트 수에 맞춰)
    users_needed = target_events // 8  # 사용자당 평균 8개 이벤트
    users_sample = segmented_users_df.sample(n=min(users_needed, len(segmented_users_df)))
    
    print(f"👥 사용 사용자: {len(users_sample):,}명")
    print(f"🍽️ 사용 레시피: {len(recipes_sample):,}개")
    
    all_events = []
    events_generated = 0
    
    # 시간 윈도우 내에서 이벤트 시간 분산
    window_duration_seconds = (window_end - window_start).total_seconds()
    
    for _, user_row in users_sample.iterrows():
        if events_generated >= target_events:
            break
            
        # 해당 윈도우 내 랜덤 시간 생성
        random_offset = random.uniform(0, window_duration_seconds)
        session_time = window_start + timedelta(seconds=random_offset)
        
        # 사용자 데이터 준비
        user_data = user_row.to_dict()
        
        # 활동 수준에 따른 이벤트 수 결정
        activity_segment = user_data.get('activity_segment', 'ACTIVE_EXPLORER')
        if activity_segment == 'POWER_USER':
            events_count = random.randint(12, 18)
        elif activity_segment == 'ACTIVE_EXPLORER':
            events_count = random.randint(6, 12)
        else:  # PASSIVE_BROWSER
            events_count = random.randint(3, 8)
        
        # 목표 수를 초과하지 않도록 조정
        remaining_events = target_events - events_generated
        if remaining_events <= 0:
            break
        events_count = min(events_count, remaining_events)
        
        # 세션 이벤트 생성
        session_events = generate_mature_service_session_flow(
            user_data, session_time, recipes_sample
        )
        
        # 필요한 만큼만 가져오기
        session_events = session_events[:events_count]
        all_events.extend(session_events)
        events_generated += len(session_events)
        
        # 진행 상황 출력
        if events_generated % 10000 == 0:
            progress = (events_generated / target_events) * 100
            print(f"   진행률: {progress:.1f}% ({events_generated:,}/{target_events:,})")
    
    # DataFrame 변환
    events_df = pd.DataFrame(all_events)
    
    # 정확한 개수로 자르기
    if len(events_df) > target_events:
        events_df = events_df.head(target_events)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"✅ 윈도우 이벤트 생성 완료!")
    print(f"⏱️ 소요시간: {duration:.1f}초")
    print(f"📊 생성 이벤트: {len(events_df):,}개")
    print(f"⚡ 처리 속도: {len(events_df)/duration:.0f} events/sec")
    
    return events_df


# 기존 generate_events_by_15min_intervals 함수를 아래 코드로 교체합니다.

def generate_events_by_15min_intervals(start_date, num_intervals, bootstrap_servers, topic):
    """
    [수정됨] 10분 간격으로 이벤트를 생성하고 Kafka로 직접 전송합니다. (시간 경계 포함)
    
    Args:
        start_date: 시작 날짜
        num_intervals: 생성할 10분 간격 수
        bootstrap_servers: 접속할 Kafka 브로커 주소
        topic: 데이터를 보낼 Kafka 토픽 이름
    """
    print(f"\n🕐 10분 간격 이벤트 생성 및 Kafka 전송 시작")
    print(f"📅 시작 시간: {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"🔢 생성 간격 수: {num_intervals}개 (총 {num_intervals * 15}분)")
    print(f"🔗 Kafka Broker: {bootstrap_servers}, Topic: {topic}")
    print("=" * 60)
    
    window_results = []
    
    for i in range(num_intervals):
        # 10분 윈도우 계산
        window_start = start_date + timedelta(minutes=i * 10)
        # [변경점] 윈도우 종료 시간에서 1초를 빼서 구간이 겹치지 않도록 조정
        window_end = window_start + timedelta(minutes=10) - timedelta(seconds=1)
        
        print(f"\n📍 윈도우 {i+1}/{num_intervals}: {window_start.strftime('%H:%M:%S')} ~ {window_end.strftime('%H:%M:%S')}")
        
        # 1. 해당 윈도우 이벤트 생성
        window_events_df = generate_events_by_time_window(
            window_start=window_start,
            window_end=window_end,
            target_events=100_000 # 10분당 10만개
        )
        
        # 2. Kafka로 전송
        if not window_events_df.empty:
            send_df_to_kafka(
                df=window_events_df,
                topic=topic,
                bootstrap_servers=bootstrap_servers
            )
        else:
            print("⚠️ 생성된 이벤트가 없어 Kafka 전송을 건너뜁니다.")
        
        # 결과 기록
        window_info = {
            'window_id': i + 1,
            'event_count': len(window_events_df)
        }
        window_results.append(window_info)
    
    print(f"\n🎉 모든 10분 간격 이벤트의 Kafka 전송 요청 완료!")
    total_events = sum([w['event_count'] for w in window_results])
    print(f"📊 총 전송 요청 이벤트: {total_events:,}개")
    
    return window_results


# print("🎯 10분 간격 이벤트 로그 생성 시스템")
# print("=" * 50)

# # 실행 전 준비상태 확인
# print("📋 실행 전 준비상태 확인:")
# print(f"   ✅ recipes_df: {len(recipes_df):,}개")
# print(f"   ✅ users_df: {len(users_df):,}명")
# print(f"   ✅ profiles_df: {len(profiles_df):,}개")

# # 핵심 함수들 존재 여부 확인
# core_functions = [
#     'assign_mature_service_user_segments',
#     'generate_mature_service_session_flow', 
#     'generate_event_properties_v2',
#     'apply_ab_test_logic_v2',
#     'get_korean_timestamp'
# ]

# print(f"\n🔧 핵심 함수 준비상태:")
# for func_name in core_functions:
#     if func_name in globals():
#         print(f"   ✅ {func_name}")
#     else:
#         print(f"   ❌ {func_name} - 함수가 정의되지 않음")

# # 핵심 변수들 존재 여부 확인
# core_variables = [
#     'EVENT_SCHEMA',
#     'USER_SEGMENTS', 
#     'KST',
#     'AB_TEST_START_DATE',
#     'AB_TEST_END_DATE'
# ]

# print(f"\n📊 핵심 변수 준비상태:")
# for var_name in core_variables:
#     if var_name in globals():
#         print(f"   ✅ {var_name}")
#     else:
#         print(f"   ❌ {var_name} - 변수가 정의되지 않음")

# print(f"\n🚀 10분 간격 이벤트 생성 시작...")

# # 10분 간격으로 4개 윈도우 (1시간 분량) 생성 실행
# # 2025-09-01 00:00:00 ~ 00:14:59, 00:15:00 ~ 00:29:59, 00:30:00 ~ 00:44:59, 00:45:00 ~ 00:59:59
# start_datetime = datetime(2025, 9, 1, 0, 0, 0, tzinfo=KST)
# window_results = generate_events_by_15min_intervals(start_date=start_datetime, num_intervals=4)

# print(f"\n� 최종 결과 요약:")
# for i, window in enumerate(window_results):
#     print(f"   📊 윈도우 {i+1}: {window['event_count']:,}개 이벤트")
#     print(f"       ⏰ {window['start_time'].strftime('%Y-%m-%d %H:%M:%S')} ~ {window['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")

# total_events = sum([w['event_count'] for w in window_results])
# print(f"\n📊 총 생성 이벤트: {total_events:,}개")
# print(f"📁 저장 위치: data/event_logs/events_window_*.parquet")
# print(f"⭐ 데이터 품질: 10분 간격별 정확한 시간 윈도우 적용")


# 파일의 맨 마지막 실행 부분을 아래 코드로 교체합니다.

if __name__ == "__main__":
    # ==================================
    # 1. 커맨드 라인 인자 파서 설정
    # ==================================
    parser = argparse.ArgumentParser(description="10분 간격 이벤트 로그를 생성하여 Kafka로 전송하는 스크립트")
    
    parser.add_argument(
        '--start-date', 
        type=str, 
        required=True, 
        help="데이터 생성을 시작할 날짜와 시간. 형식: 'YYYY-MM-DD-HH'"
    )
    parser.add_argument(
        '--num-intervals', 
        type=int, 
        default=4, 
        help="생성할 10분 간격의 수. 기본값: 4 (1시간 분량)"
    )
    # [추가된 부분] Kafka 토픽을 지정하는 인자
    parser.add_argument(
        '--topic',
        type=str,
        default='replay-user-events', # 기본값을 운영 토픽으로 설정
        help="데이터를 보낼 Kafka 토픽 이름. 기본값: 'replay-user-events'"
    )
    
    args = parser.parse_args()

    # ==================================
    # 2. Kafka 및 시뮬레이션 정보 설정
    # ==================================
    KAFKA_BOOTSTRAP_SERVERS = '10.0.128.56:9092,10.0.129.146:9092,10.0.79.163:9092'
    # [수정된 부분] 인자로부터 토픽 이름 가져오기
    kafka_topic = args.topic
    
    try:
        sim_start_datetime = datetime.strptime(args.start_date, '%Y-%m-%d-%H').replace(tzinfo=KST)
    except ValueError:
        print("❌ 오류: 날짜 형식이 잘못되었습니다. 'YYYY-MM-DD-HH' 형식으로 입력해주세요.")
        exit()
        
    sim_num_intervals = args.num_intervals

    print("=" * 50)
    print("🚀 10분 간격 이벤트 생성 및 Kafka 전송을 시작합니다.")
    print("=" * 50)
    print(f"   ▶️ 시작 시간: {sim_start_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"   ▶️ 구간 수: {sim_num_intervals}개")
    print(f"   ▶️ 전송 토픽: {kafka_topic}") # 현재 전송할 토픽 이름 출력

    # ==================================
    # 3. 변경된 메인 함수 호출
    # ==================================
    window_results = generate_events_by_15min_intervals(
        start_date=sim_start_datetime,
        num_intervals=sim_num_intervals,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # [수정된 부분] 인자로 받은 토픽 전달
        topic=kafka_topic
    )