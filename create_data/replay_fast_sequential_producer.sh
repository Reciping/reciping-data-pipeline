#!/bin/bash
# replay_fast_sequential_producer.sh

SCRIPT_DIR=$(dirname "$0")
SCRIPT_PATH="${SCRIPT_DIR}/create_event_logs.py"
TOPIC_NAME="replay-user-events"

START_YEAR=2025
START_MONTH=9
START_DAY=1
START_HOUR=0
START_MINUTE=0

END_DAY=1
END_HOUR=0
END_MINUTE=45

CURRENT_YEAR=$START_YEAR
CURRENT_MONTH=$START_MONTH
CURRENT_DAY=$START_DAY
CURRENT_HOUR=$START_HOUR
CURRENT_MINUTE=$START_MINUTE

WAIT_FOR_S3_SECONDS=60
WAIT_FOR_AIRFLOW_SECONDS=300

echo "======================================================"
echo " 9월 빠른 순차 리플레이"
echo "======================================================"

INTERVAL_COUNT=0

while true; do
    # ✅ 수정: 분까지 포함
    CURRENT_DATE=$(printf "%04d-%02d-%02d-%02d-%02d" $CURRENT_YEAR $CURRENT_MONTH $CURRENT_DAY $CURRENT_HOUR $CURRENT_MINUTE)
    #                                                                                              ^^^^^^^^^^^^^^^^
    #                                                                                              분 추가!
    
    CURRENT_TIME=$(printf "%02d:%02d" $CURRENT_HOUR $CURRENT_MINUTE)
    
    INTERVAL_COUNT=$((INTERVAL_COUNT + 1))
    
    echo -e "\n=========================================="
    echo "[실제: $(date +'%H:%M:%S')] 구간 #${INTERVAL_COUNT}"
    echo "처리 데이터: ${CURRENT_YEAR}-${CURRENT_MONTH}-${CURRENT_DAY} ${CURRENT_TIME}"
    echo "=========================================="
    
    # 데이터 생성
    python3 "${SCRIPT_PATH}" \
        --start-date ${CURRENT_DATE} \
        --num-intervals 1 \
        --topic ${TOPIC_NAME}
    
    if [ $? -ne 0 ]; then
        echo "❌ 실패"
        exit 1
    fi
    
    # S3 전송 대기
    echo "⏰ S3 전송 대기: ${WAIT_FOR_S3_SECONDS}초"
    sleep ${WAIT_FOR_S3_SECONDS}
    
    # Airflow 처리 대기
    echo "⏰ Airflow 처리 대기: ${WAIT_FOR_AIRFLOW_SECONDS}초"
    sleep ${WAIT_FOR_AIRFLOW_SECONDS}
    
    echo "✅ 완료, 다음 구간으로..."
    
    # 다음 15분
    CURRENT_MINUTE=$((CURRENT_MINUTE + 15))
    
    if [ $CURRENT_MINUTE -ge 60 ]; then
        CURRENT_MINUTE=0
        CURRENT_HOUR=$((CURRENT_HOUR + 1))
        
        if [ $CURRENT_HOUR -ge 24 ]; then
            CURRENT_HOUR=0
            CURRENT_DAY=$((CURRENT_DAY + 1))
            
            if [ $CURRENT_DAY -gt 30 ]; then
                echo -e "\n🎉 완료!"
                exit 0
            fi
        fi
    fi
    
    if [ $CURRENT_DAY -eq $END_DAY ] && \
       [ $CURRENT_HOUR -eq $END_HOUR ] && \
       [ $CURRENT_MINUTE -gt $END_MINUTE ]; then
        echo -e "\n🎉 완료!"
        exit 0
    fi
done