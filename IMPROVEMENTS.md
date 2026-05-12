# Improvements

바른컴퍼니 ERP / 재고운영 시스템의 개선 내역입니다.

## 프로젝트 개요

- **스택**: Node.js 20 (CommonJS), 단일 파일 모놀리스 (`serve_inv2.js`, ~20K LOC)
- **데이터 소스**:
  - MSSQL (Azure SQL): `bar_shop1` (실물 카드), `XERP` (통합 ERP), `BHC` (디얼디어 재고)
  - MySQL: DD wedding (뚜비뚜비 쇼핑몰)
  - PostgreSQL (운영) / SQLite (로컬 폴백) — `pg-adapter.js`로 추상화
- **인증**: JWT (jsonwebtoken) + bcryptjs, Google OAuth 선택
- **주요 영역**: 발주/입고(`/po*`), 재고(`/inventory*`), 판매(`/sales*`), 후공정 관리, 거래처 포털
- **컨테이너**: `Dockerfile` (node:20-slim + Chromium + freetds), 기본 포트 **12026**

## 최근 개선 내역

### 1. 발주 메일 단위 재조정 (2026-05-11)
- **원재료 발주 메일·PDF·엑셀**: `발주수량(R)` 단일 표시. 거래처가 다발(R) 단위로 보길 원함
- **후공정 발주 메일·PDF·엑셀**: `입고수량(R)` 컬럼 제거, `생산수량(낱개)` 단일 표시로 단순화
- PDF 합계 행도 동일 정책으로 갱신 (원재료=R 합계, 후공정=생산수량 합계)
- `ream_qty` / `ordered_qty` 둘 다 enrichedItems 에 들어 있어 source 변경 없음
- (정책 변천: 2026-05-08 낱개 일원화 → 2026-05-11 위와 같이 재조정)

### 2. 발주 프로세스 개선 (v1.0.9)
- 후공정 "사용안함(패스)" + "기타업체 직접입력" 옵션
- 발주서 완료/복원 기능 (`PATCH /api/po-drafts/:id/status`)
- 발주서 법인 구분 표시 (DD=붉은색 / 바른컴퍼니=기본)
- 거래처 포탈 재고/긴급도 표시 (🚨긴급 / ⚠부족 / 안전)
- 한국 리드타임 기본값 5영업일 → 20영업일

### 3. DD 법인 재고/출고 분리 (v1.0.8)
- BHC DB 직접 연동 (SiteCode `BHC2`) — DD 재고를 별도 DB에서 조회
- `/api/xerp-inventory` 법인 분기: barunson(BK10) / dd(BHC2) / all(병렬 병합)
- DD 품목 `origin='한국'` 강제 UPDATE 제거 — 원본 생산지 보존

### 4. 품목 일괄 업로드 미리보기 (v1.0.7)
- 신규/기존/오류 건수 사전 표시 후 사용자가 명시 선택
- 옵션: 신규만 등록 / 전부 반영(덮어쓰기) / 취소
- API: `POST /api/products/bulk/preview` (저장 없이 분석만)

### 5. PostgreSQL 마이그레이션 (진행 중)
- SQLite → PostgreSQL 전환을 위한 `pg-adapter.js` 어댑터 도입
- `scripts/migrate-to-pg.js` 마이그레이션 스크립트
- 시작 시 권한/스키마 자동 복구 (admin onely 계정 사용)

## 남은 TODO

- **모놀리스 분할**: `serve_inv2.js` 20K줄 → 모듈별 분리 (현재 `routes/` 일부만 분리됨)
- **외래키 제약 도입**: 모든 DB에 FK 없음 → 데이터 정합성을 코드에서만 보장
- **PG 완전 전환**: SQLite 폴백 분기 제거, schema 일관성 (legal_entity 등 컬럼 부재 이슈 해결)
- **테스트 자동화**: 현재 수동 검증 중심 → API 단위/통합 테스트 도입
- **PII 마스킹 일괄 적용**: 화면/API 응답의 이름·이메일·전화·계좌 마스킹 누락 영역 점검
- **인덱스 미사용 쿼리 점검**: `LIKE '%...'`, 컬럼 함수 적용 등 Full Scan 유발 쿼리 색출

## 배포 참고

- **Node.js 20 이상** 필수 (Dockerfile 기준)
- **필수 환경변수** (`barunson-database-reference/.env`):
  - `DB_SERVER`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_DATABASE` — bar_shop1 (Azure MSSQL)
  - `DD_DB_SERVER`, `DD_DB_PORT`, `DD_DB_USER`, `DD_DB_PASSWORD` — DD wedding (MySQL)
- **선택 환경변수**:
  - `XERP_DB_*` — XERP 재무 데이터 권한 (조휘열 담당)
  - `SLACK_WEBHOOK_URL` — Slack 알림
  - `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM` — Gmail SMTP (메일 발송)
  - `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` — Google 로그인
  - `PORT` (기본 12026), `DATA_DIR`, `UPLOAD_DIR`
- **빌드**: `docker compose up --build`
- **실행**: `npm start` (= `node barunson-database-reference/user/serve_inv2.js`)
- **헬스체크**: `GET /api/health`
- **버전 확인**: `GET /api/version`
- **시드 데이터**: `barunson-database-reference/user/seed.db` (없으면 `init_db.js` 실행)
- **민감정보 주의**: `.env`, `*.db` 는 `.gitignore` 처리됨. 커밋 전 점검 필수
