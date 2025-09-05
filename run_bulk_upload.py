# run_bulk_upload.py
import os
import subprocess
import re
from datetime import datetime

def run_bulk_upload():
    """
    지정된 디렉토리의 모든 15분 단위 파일을 찾아
    upload_to_staging_area.py 스크립트를 순차적으로 호출합니다.
    """
    # 1. 설정
    # upload_to_staging_area.py가 있는 경로를 기준으로 상대 경로 설정
    source_dir = os.path.join("data", "event_logs", "event_1m_15min_chunks")
    uploader_script = "upload_to_staging_area.py"
    
    if not os.path.isdir(source_dir):
        print(f"오류: 소스 디렉토리를 찾을 수 없습니다 - {source_dir}")
        return

    # 2. 디렉토리 내 모든 파일 목록 가져오기
    all_files = os.listdir(source_dir)
    
    # 3. 파일명을 순회하며 업로드 스크립트 실행
    print(f"총 {len(all_files)}개의 파일을 S3에 업로드합니다.")
    for filename in sorted(all_files): # 시간 순서대로 처리하기 위해 정렬
        
        # 파일명 패턴(events_YYYYMMDDHHMM.jsonl)이 맞는지 확인
        match = re.match(r"events_(\d{12})\.jsonl", filename)
        if match:
            timestamp_str = match.group(1)
            # 'YYYYMMDDHHMM' 형식을 datetime 객체로 변환
            dt_obj = datetime.strptime(timestamp_str, "%Y%m%d%H%M")
            # '--execution-dt' 인자 형식으로 변환
            execution_dt = dt_obj.strftime("%Y-%m-%d %H:%M")
            
            # 4. 명령어 생성 및 실행
            command = [
                "python",
                uploader_script,
                "--input-dir", source_dir,
                "--execution-dt", execution_dt
            ]
            
            print(f"\n--- {filename} 처리 중... ---")
            # subprocess를 사용하여 외부 스크립트 실행
            result = subprocess.run(command, capture_output=True, text=True)
            
            # 실행 결과 출력
            print(result.stdout)
            if result.returncode != 0:
                print(f"오류 발생: {filename}")
                print(result.stderr)
        else:
            print(f"건너뛰기: 파일명 형식이 맞지 않습니다 - {filename}")

if __name__ == "__main__":
    run_bulk_upload()