# 운영 환경 DB 동기화 실패 — 진단 보고서

**발생일**: 2026-04-22  
**영향**: DB 동기화 버튼이 "live 리프레시 중..." 루프에 빠지고 "⚠️ 동기화 필요" 노란 배지가 영구 표시됨. 실제 XERP 데이터가 snapshot 에 저장되지 않음.

---

## 1. 증상

1. ERP 화면 상단에 **"⚠️ 동기화 필요"** 노란 배지가 계속 표시
2. DB 동기화 버튼 클릭 시 **"live 리프레시 중... (Ns)"** 로 바뀌며 계속 카운트만 올라감
3. 7분 후 타임아웃 또는 수동 취소까지 완료되지 않음
4. snapshot 테이블에 데이터가 쌓이지 않음

---

## 2. 원인 — Prod 컨테이너 로그 발췌

```
[pg-adapter exec ERROR] must be owner of table products
[pg-adapter exec FAILED STMT] ALTER TABLE products ADD COLUMN IF NOT EXISTS thomson TEXT DEFAULT ''
[pg-adapter exec ERROR] permission denied for schema public
[pg-adapter exec FAILED STMT] CREATE TABLE IF NOT EXISTS barcode_registry (...)
[pg-adapter exec ERROR] must be owner of table vat_reports
[pg-adapter exec FAILED STMT] ALTER TABLE vat_reports ADD COLUMN company TEXT DEFAULT 'barunson'
감사 로그 기록 실패: column "id" of relation "audit_log" does not exist
[sync] sync_log INSERT 실패 원문: relation "sync_log" does not exist
[sync] sync_log 테이블 자체가 없음 → snapshot 비활성, live 리프레시 모드로 진행
XERP 연결 실패 (시도 25): Login failed for user 'readonly_user'.
dd 조회 실패: Login failed for user 'readonly_user'.
[sync bg] 모든 데이터 fallback/empty — snapshot 갱신 스킵
```

### 근본 원인 3가지

| # | 원인 | 조치 필요 |
|---|---|---|
| 1 | **PG 계정(`sc_erp`) 이 기존 테이블 소유자가 아님 + public 스키마 CREATE 권한 없음** | PG 관리자가 권한 부여 필요 |
| 2 | **`sync_log` / `inventory_snapshot` 테이블이 prod PG 에 존재하지 않음** | PG 관리자가 테이블 생성 필요 |
| 3 | **XERP / DD 계정(`readonly_user`) 비밀번호 오류** | `.env` 의 XERP/DD 접속정보 수정 필요 |

~~**코드 레벨로는 해결 불가.** 앱 코드(`serve_inv2.js`) 에 방어적 CREATE/ALTER 가 이미 들어가 있지만 PG 권한 부족으로 모두 silent-fail 함.~~

**2026-04-22 업데이트**: 원인 #1/#2 는 **코드 prelude 로 해결**. `serve_inv2.js` 의 `db.connect()` 직후에 `PG_ADMIN_USER`(기본 `onely`) superuser 로 별도 접속해 (a) `GRANT ALL PRIVILEGES ON SCHEMA public TO <sc_erp>`, (b) 기존 public 테이블 `OWNER TO <sc_erp>` 일괄 이전, (c) `sync_log`/`inventory_snapshot` 직접 CREATE 를 수행. 다음 부팅부터 권한 문제 자동 해소됨. 단, `PG_ADMIN_USER`/`PG_ADMIN_PASSWORD` 가 실제 superuser 여야 하며 `.env` 로 오버라이드 가능. 원인 #3(XERP `readonly_user` 비밀번호) 은 여전히 `.env` 수정 필요.

**Prelude 가 동작하지 않을 때** (admin 자격 미설정/잘못됨): [`FIX-DB-PERMISSION.sql`](FIX-DB-PERMISSION.sql) 을 PG superuser 로 1회 실행 (`psql -U postgres -d sc_erp -f FIX-DB-PERMISSION.sql`). 멱등이라 여러 번 실행해도 안전.

---

## 3. 해결 방법

### A. PG superuser 로 아래 SQL 1회 실행

PG 관리자(postgres 또는 다른 superuser 계정) 로 접속해서:

```sql
-- 1) sc_erp 계정에 public 스키마 권한 부여
GRANT ALL PRIVILEGES ON SCHEMA public TO sc_erp;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sc_erp;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sc_erp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sc_erp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sc_erp;

-- 2) 기존 모든 테이블 소유권을 sc_erp 로 변경 (ALTER TABLE 이 가능하도록)
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP
    EXECUTE format('ALTER TABLE public.%I OWNER TO sc_erp', t.tablename);
  END LOOP;
END $$;

-- 3) 누락된 핵심 테이블 생성 (sync_log, inventory_snapshot)
CREATE TABLE IF NOT EXISTS sync_log (
  id SERIAL PRIMARY KEY,
  sync_type TEXT NOT NULL,
  started_at TEXT DEFAULT TO_CHAR(NOW() AT TIME ZONE 'Asia/Seoul','YYYY-MM-DD HH24:MI:SS'),
  finished_at TEXT DEFAULT '',
  success_count INTEGER DEFAULT 0,
  fail_count INTEGER DEFAULT 0,
  status TEXT DEFAULT 'running',
  error_msg TEXT DEFAULT '',
  triggered_by TEXT DEFAULT 'manual'
);
CREATE INDEX IF NOT EXISTS idx_synclog_type_time ON sync_log(sync_type, started_at);
CREATE INDEX IF NOT EXISTS idx_synclog_status ON sync_log(status) WHERE status='running';
ALTER TABLE sync_log OWNER TO sc_erp;

CREATE TABLE IF NOT EXISTS inventory_snapshot (
  product_code TEXT PRIMARY KEY,
  legal_entity TEXT DEFAULT 'barunson',
  site_code TEXT DEFAULT 'BK10',
  current_stock INTEGER DEFAULT 0,
  monthly_out INTEGER DEFAULT 0,
  daily_out INTEGER DEFAULT 0,
  total_3m INTEGER DEFAULT 0,
  item_name TEXT DEFAULT '',
  synced_at TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_invsnap_entity ON inventory_snapshot(legal_entity);
CREATE INDEX IF NOT EXISTS idx_invsnap_synced ON inventory_snapshot(synced_at);
ALTER TABLE inventory_snapshot OWNER TO sc_erp;
```

### B. `.env` 수정 (XERP/DD 접속)

배포 플랫폼의 환경변수 설정에서:

```env
# XERP (바른손 재고 MSSQL) — 현재 readonly_user 로그인 실패
DB_SERVER=<XERP 서버 호스트>
DB_USER=<올바른 계정>
DB_PASSWORD=<올바른 비번>

# DD (디얼디어 재고) — 미설정 상태
DD_DB_SERVER=<DD 서버 호스트>
DD_DB_USER=<계정>
DD_DB_PASSWORD=<비번>
```

### C. 컨테이너 재시작

배포 플랫폼의 **🔄 업데이트** 버튼 클릭 → 새 `.env` 로 재기동.

---

## 4. 검증 방법

재기동 후 **컨테이너 로그 startup 부분** 에서 이 줄 확인:

- ✅ `[init] sync_log 테이블 OK`
- ✅ `[init] inventory_snapshot 테이블 OK`
- ✅ `XERP 연결 완료` (또는 재시도 없음)

그 다음 ERP 페이지 Ctrl+Shift+R → DB 동기화 클릭 → **"동기화 중... (Ns 경과)"** → 완료 후 **"✅ 마지막 동기화: MM/DD HH:MM (N개)"** 녹색 배지.

---

## 5. 코드 레벨에서 이미 준비된 방어

운영 환경 권한만 해결되면 아래가 자동 동작:

- `serve_inv2.js:1389` — sync_log CREATE TABLE IF NOT EXISTS
- `serve_inv2.js:1367` — inventory_snapshot CREATE TABLE IF NOT EXISTS  
- `serve_inv2.js:1686` — onely pool 로 sync_log/inventory_snapshot 컬럼 보강 시도
- `serve_inv2.js:5904` — POST /api/sync/xerp-inventory 에 자가복구 로직 (INSERT 실패 시 테이블 재생성 + 재시도)
- `serve_inv2.js:5886` — TZ-agnostic stale lock 비교 (UTC 컨테이너 + KST 텍스트 저장 호환)
- `serve_inv2.js:1293` — startup 시 running lock 자동 해제

모든 방어 코드는 `sc_erp` (또는 `onely`) 가 CREATE/ALTER 권한을 가진 경우에만 작동. 현재 prod 는 권한 부족으로 무력화된 상태.

---

## 6. PG 관리자 접근 방법 (확인 필요)

- `.env` 의 `PG_HOST` 값:
  - `onely-postgres` → 같은 Docker 네트워크의 PG 컨테이너. 호스트에 SSH → `docker exec -it onely-postgres psql -U postgres`
  - `xxx.supabase.co` / `xxx.neon.tech` → 관리형 서비스 웹 콘솔에서 SQL 실행
  - `xxx.rds.amazonaws.com` → AWS RDS 콘솔 또는 psql CLI
  - 기타 → 별도 서버에 psql 로 접속

PG 관리자 권한이 없으면 담당자(DBA / 시스템 관리자) 에게 3번의 SQL 실행 요청.
