FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 패키지 업데이트 및 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    openssl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# 펌웨어 디렉토리 생성
RUN mkdir -p /app/firmware_files

# 포트 노출
EXPOSE 8080

# 헬스체크
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health')"

# 애플리케이션 실행
# Use a single worker because the server owns one MQTT subscription loop.
# Add threads so concurrent dashboard/API requests do not pile up on Flask dev server.
CMD ["sh", "-c", "exec gunicorn --worker-class gthread --workers ${GUNICORN_WORKERS:-1} --threads ${GUNICORN_THREADS:-8} --timeout ${GUNICORN_TIMEOUT:-120} --keep-alive ${GUNICORN_KEEPALIVE:-10} --max-requests ${GUNICORN_MAX_REQUESTS:-1000} --max-requests-jitter ${GUNICORN_MAX_REQUESTS_JITTER:-100} --bind 0.0.0.0:${PORT:-8080} wsgi:application"]
