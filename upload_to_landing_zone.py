# upload_to_landing_zone.py
import boto3
import argparse
import os
from datetime import datetime

def upload_file_to_s3(input_file_path, bucket_name, s3_prefix):
    """
    로컬 파일을 S3의 지정된 경로(랜딩 존)에 그대로 업로드합니다.
    """
    if not os.path.exists(input_file_path):
        print(f"오류: 파일을 찾을 수 없습니다 - {input_file_path}")
        return

    # 파일 이름을 그대로 사용 (타임스탬프 없이)
    # 프로덕션에서는 타임스탬프 추가 권장: timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    original_filename = os.path.basename(input_file_path)
    s3_key = f"{s3_prefix}/{original_filename}"

    print(f"'{input_file_path}' 파일을 S3에 업로드합니다...")
    print(f"대상 경로: s3://{bucket_name}/{s3_key}")

    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(input_file_path, bucket_name, s3_key)
        print(f"✅ 성공: s3://{bucket_name}/{s3_key} 에 업로드했습니다.")
    except Exception as e:
        print(f"❌ 오류: s3://{bucket_name}/{s3_key} 업로드 실패 - {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="로컬 파일을 S3 랜딩 존에 업로드합니다.")
    parser.add_argument("--input-file", required=True, help="입력 이벤트 로그 파일 경로 (.jsonl)")
    parser.add_argument("--bucket-name", default="reciping-user-event-logs", help="업로드할 S3 버킷 이름")
    # 랜딩 존 경로를 명확히 지정
    parser.add_argument("--s3-prefix", default="bronze/landing-zone/events/test-sample", help="S3 내 랜딩 존 경로")

    args = parser.parse_args()
    upload_file_to_s3(args.input_file, args.bucket_name, args.s3_prefix)