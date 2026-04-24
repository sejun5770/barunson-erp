FROM node:20-slim

# Python + MSSQL 드라이버 + Chromium (PDF 생성용) 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    freetds-dev gcc g++ \
    chromium \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# Chromium 경로 설정 (puppeteer-core용)
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV PUPPETEER_SKIP_DOWNLOAD=true

WORKDIR /app

# Python 패키지 설치
COPY requirements.txt ./
RUN pip3 install --break-system-packages --no-cache-dir -r requirements.txt

# Node 의존성 설치
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# 캐시 무효화 (빌드마다 변경)
ARG CACHE_BUST=20260424-dd-resync-v2-creddiag
RUN echo "build:$CACHE_BUST" > /app/.buildstamp

# 앱 파일 복사
COPY . .

# 데이터/업로드 디렉토리 생성
RUN mkdir -p /app/data /app/uploads/invoices

# 환경변수 기본값
ENV PORT=12026
ENV DATA_DIR=/app/data
ENV UPLOAD_DIR=/app/uploads

# DB 초기화 (컨테이너 시작 시 실행하도록 entrypoint에서 처리)
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 12026

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["node", "serve_inv2.js"]
