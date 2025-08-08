# Dockerfile
# Bitnami에서 제공하는 Spark 3.5 버전을 기반으로 이미지를 만듭니다. 
FROM bitnami/spark:3.5

# 패키지 설치를 위해 root 권한으로 전환
USER root

# Python, pip, 그리고 기타 필요한 도구들을 설치합니다. 
RUN apt-get update && \
    apt-get install -y python3 python3-pip nano && \
    rm -r /var/lib/apt/lists/*

# Poetry 설치
RUN pip install poetry

# --- 수정된 부분 ---
# non-root 유저(1001)가 사용할 홈 디렉토리를 환경 변수로 지정합니다. 
# Poetry 등 애플리케이션이 이 경로를 참조하여 설정 파일을 생성합니다.
ENV HOME=/home/spark

# 작업 디렉토리를 /app으로 설정합니다. 
WORKDIR /app

# entrypoint 스크립트를 컨테이너에 복사하고 실행 권한을 부여합니다.
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# 의존성 파일만 먼저 복사하여 Docker 캐시를 효율적으로 사용합니다. 
COPY pyproject.toml poetry.lock* ./

# Poetry를 사용해 파이썬 라이브러리들을 설치합니다. 
# --without datagen: 데이터 생성에만 필요한 'datagen' 그룹은 제외하여 이미지를 가볍게 유지합니다.
# --no-root: 프로젝트 자체는 패키지로 설치하지 않고, 의존성만 설치합니다.
# RUN poetry config virtualenvs.create false && \
#     poetry install --no-interaction --no-ansi --without datagen --no-root
RUN poetry install --no-interaction --no-ansi --without datagen --no-root

# 나머지 프로젝트 소스 코드를 복사합니다. 
COPY . .

# 컨테이너의 기본 실행 사용자를 non-root 유저인 1001로 설정하여 보안을 강화합니다.
USER 1001