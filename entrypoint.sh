#!/bin/sh
# entrypoint.sh

# /home/spark 폴더의 소유권을 spark 유저(1001)에게 부여합니다.
# 이렇게 하면 poetry가 설정 파일을 쓸 수 있습니다.
chown -R 1001:0 /home/spark

# 원래 컨테이너가 실행하려 했던 명령을 이어서 실행합니다.
exec "$@"