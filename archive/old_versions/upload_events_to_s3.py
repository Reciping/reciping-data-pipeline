import json
import boto3
import argparse
import os
from datetime import datetime
from collections import defaultdict
from zoneinfo import ZoneInfo

def upload_partitioned_jsonl_to_s3(input_file_path, bucket_name, s3_prefix):
    """
    JSONL 파일을 읽어, 내부 timestamp를 기준으로 데이터를 시간 단위로 재구성하여
    내용 변경 없이 S3에 파티셔닝하여 업로드합니다.

    :param input_file_path: 로컬에 있는 이벤트 로그 파일 경로 (.jsonl)
    :param bucket_name: 대상 S3 버킷 이름
    :param s3_prefix: S3 내 기본 경로 (예: 'bronze/events')
    """
    print(f"'{input_file_path}' 파일을 처리하여 파티셔닝을 시작합니다...")

    # 1. 파티션별로 로그 라인을 저장할 딕셔너리 준비
    # 예: {(2025, 8, 5, 9): ['{"ts":...}', '{"ts":...}']}
    partitioned_logs = defaultdict(list)

    # 2. 원본 파일을 한 줄씩 읽기
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    # 각 라인을 JSON으로 파싱하여 timestamp 값 추출
                    log_data = json.loads(line)
                    timestamp_str = log_data.get('timestamp')

                    if not timestamp_str:
                        print(f"경고: 'timestamp' 필드가 없는 라인을 건너뜁니다: {line}")
                        continue

                    # ISO 8601 형식의 timestamp를 datetime 객체로 변환
                    # 파이썬 3.7+ 에서는 fromisoformat으로 timezone 포함 파싱 가능
                    # dt_obj = datetime.fromisoformat(timestamp_str)

                    # # 파티션 키 생성 (년, 월, 일, 시)
                    # partition_key = (
                    #     dt_obj.year,
                    #     f"{dt_obj.month:02d}",
                    #     f"{dt_obj.day:02d}",
                    #     f"{dt_obj.hour:02d}"
                    # )

                    # 1. 서울 시간대 객체를 명시적으로 생성합니다.
                    seoul_tz = ZoneInfo("Asia/Seoul")
                    
                    # 2. ISO 형식 문자열을 timezone-aware datetime 객체로 변환합니다.
                    aware_dt_obj = datetime.fromisoformat(timestamp_str)
                    
                    # 3. 위 객체를 서울 시간대 기준으로 다시 한번 명확하게 변환합니다.
                    #    이것이 시간대 관련 모든 모호함을 제거하는 핵심 단계입니다.
                    kst_dt_obj = aware_dt_obj.astimezone(seoul_tz)

                    # 4. 서울 시간 기준이 보장된 객체에서 파티션 키를 추출합니다.
                    partition_key = (
                        kst_dt_obj.year,
                        f"{kst_dt_obj.month:02d}",
                        f"{kst_dt_obj.day:02d}",
                        f"{kst_dt_obj.hour:02d}"
                    )

                    # 생성된 파티션 키에 원본 라인(문자열)을 그대로 추가
                    partitioned_logs[partition_key].append(line)

                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    print(f"경고: 파싱할 수 없는 라인을 건너뜁니다: {line} (오류: {e})")
                    continue

    except FileNotFoundError:
        print(f"오류: 파일을 찾을 수 없습니다 - {input_file_path}")
        return

    # 3. 파티션별로 S3에 업로드
    if not partitioned_logs:
        print("업로드할 유효한 로그 데이터가 없습니다.")
        return

    s3_client = boto3.client('s3')
    original_filename = os.path.splitext(os.path.basename(input_file_path))[0]
    
    print(f"총 {len(partitioned_logs)}개의 파티션으로 나누어 S3에 업로드합니다.")

    for (year, month, day, hour), lines in partitioned_logs.items():
        # S3 객체 키(경로) 생성
        s3_key = (f"{s3_prefix}/year={year}/month={month}/day={day}/hour={hour}/"
                  f"{year}{month}{day}{hour}_{original_filename}.jsonl")

        # 해당 파티션의 라인들을 다시 하나의 .jsonl 파일 내용으로 합치기
        # 각 라인은 개행 문자(\n)로 구분
        output_content = "\n".join(lines)

        # S3에 업로드
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=output_content.encode('utf-8')
            )
            print(f"성공: {len(lines)}개 행을 s3://{bucket_name}/{s3_key} 에 업로드했습니다.")
        except Exception as e:
            print(f"오류: s3://{bucket_name}/{s3_key} 업로드 실패 - {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="JSONL 파일을 timestamp 기준으로 분할하여 원본 내용 그대로 S3에 업로드합니다."
    )
    parser.add_argument("--input-file", required=True, help="입력 이벤트 로그 파일 경로 (.jsonl)")
    parser.add_argument("--bucket-name", default="reciping-user-event-logs", help="업로드할 S3 버킷 이름")
    parser.add_argument("--s3-prefix", default="bronze/user-behavior-events", help="S3 내 파티션 이전의 기본 경로")

    args = parser.parse_args()
    upload_partitioned_jsonl_to_s3(args.input_file, args.bucket_name, args.s3_prefix)