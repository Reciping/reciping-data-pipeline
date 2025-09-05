# upload_to_staging_area.py
import boto3
import argparse
import os
from datetime import datetime

def upload_file_to_staging(input_dir, bucket_name, s3_prefix, execution_dt_str):
    """
    지정된 시간의 15분 단위 로컬 파일을 S3의 Staging Area에 업로드합니다.
    """
    # 1. 처리할 기준 시간을 datetime 객체로 변환
    target_dt = datetime.strptime(execution_dt_str, "%Y-%m-%d %H:%M")
    
    # 2. 업로드할 로컬 파일 경로 생성
    # 예: data\event_logs\event_1m_15min_chunks\events_202508010915.jsonl
    source_filename = f"events_{target_dt.strftime('%Y%m%d%H%M')}.jsonl"
    local_file_path = os.path.join(input_dir, source_filename)

    if not os.path.exists(local_file_path):
        print(f"파일을 찾을 수 없습니다: {local_file_path}")
        return

    # 3. S3 경로 및 키 생성 (Hive 파티션 없음)
    # 예: bronze/landing-zone/events/staging-area/events_202508010915.jsonl
    s3_key = f"{s3_prefix}/{source_filename}"

    # 4. S3에 파일 업로드
    print(f"'{local_file_path}' 파일을 S3 Staging Area에 업로드합니다...")
    print(f"대상 경로: s3://{bucket_name}/{s3_key}")
    
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"[SUCCESS] 성공: s3://{bucket_name}/{s3_key} 에 업로드했습니다.")
    except Exception as e:
        print(f"[ERROR] 오류: s3://{bucket_name}/{s3_key} 업로드 실패 - {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="15분 단위 로컬 파일을 S3 Staging Area에 업로드합니다.")
    parser.add_argument("--input-dir", required=True, help="15분 단위로 분할된 이벤트 로그 파일들이 있는 디렉토리")
    parser.add_argument("--bucket-name", default="reciping-user-event-logs", help="업로드할 S3 버킷 이름")
    # S3 prefix 기본값을 요청하신 경로로 변경
    parser.add_argument("--s3-prefix", default="bronze/landing-zone/events/staging-area", help="S3 내 Staging Area 경로")
    # 처리할 기준 시간을 인자로 받음 (예: "2025-08-01 09:15")
    parser.add_argument("--execution-dt", required=True, help="데이터 처리 기준 시간 (YYYY-MM-DD HH:MM 형식)")

    args = parser.parse_args()
    upload_file_to_staging(args.input_dir, args.bucket_name, args.s3_prefix, args.execution_dt)