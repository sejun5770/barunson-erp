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
COPY barunson-database-reference/user/requirements.txt ./
RUN pip3 install --break-system-packages --no-cache-dir -r requirements.txt

# Node 의존성 설치
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# 캐시 무효화 (빌드마다 다른 값)
ARG CACHE_BUST=20260508-ownership-migration

# 앱 파일 복사
RUN echo "build:$CACHE_BUST" > /app/.buildstamp
COPY . .

# 데이터/업로드 디렉토리 생성
RUN mkdir -p /app/data /app/uploads/invoices

# 환경변수 — Docker Manager 표준 포트 3000 사용
ENV PORT=3000
ENV DATA_DIR=/app/data
ENV UPLOAD_DIR=/app/uploads

EXPOSE 3000

CMD ["npm", "start"]
