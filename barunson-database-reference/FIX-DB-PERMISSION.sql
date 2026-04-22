-- ============================================================================
-- FIX-DB-PERMISSION.sql
-- ============================================================================
-- ISSUE-2026-04-22-DB-PERMISSION.md 의 "Solution A" 를 한 파일로 정리.
-- PG superuser(postgres / onely 등) 로 1회 실행하면 sync_log/inventory_snapshot
-- 누락 + sc_erp 권한 부족 문제가 즉시 해소됨.
--
-- 사용:
--   psql -h <PG_HOST> -U postgres -d sc_erp -f FIX-DB-PERMISSION.sql
--   또는 Supabase/Neon/RDS 콘솔에 통째로 붙여넣기
--
-- 안전성: 모두 IF NOT EXISTS / DO 블록 안에서 처리되어 멱등(여러 번 실행 OK)
-- ============================================================================

\echo '[1/4] sc_erp 계정에 public 스키마 권한 부여...'
GRANT ALL PRIVILEGES ON SCHEMA public TO sc_erp;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sc_erp;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sc_erp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sc_erp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sc_erp;

\echo '[2/4] 기존 모든 public 테이블 owner 를 sc_erp 로 이전...'
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP
    EXECUTE format('ALTER TABLE public.%I OWNER TO sc_erp', t.tablename);
  END LOOP;
END $$;

\echo '[3/4] sync_log 테이블 생성...'
CREATE TABLE IF NOT EXISTS sync_log (
  id            SERIAL PRIMARY KEY,
  sync_type     TEXT NOT NULL,
  started_at    TEXT DEFAULT TO_CHAR(NOW() AT TIME ZONE 'Asia/Seoul','YYYY-MM-DD HH24:MI:SS'),
  finished_at   TEXT DEFAULT '',
  success_count INTEGER DEFAULT 0,
  fail_count    INTEGER DEFAULT 0,
  status        TEXT DEFAULT 'running',
  error_msg     TEXT DEFAULT '',
  triggered_by  TEXT DEFAULT 'manual'
);
CREATE INDEX IF NOT EXISTS idx_synclog_type_time ON sync_log(sync_type, started_at);
CREATE INDEX IF NOT EXISTS idx_synclog_status   ON sync_log(status) WHERE status='running';
ALTER TABLE sync_log OWNER TO sc_erp;

\echo '[4/4] inventory_snapshot 테이블 생성...'
CREATE TABLE IF NOT EXISTS inventory_snapshot (
  product_code   TEXT PRIMARY KEY,
  legal_entity   TEXT DEFAULT 'barunson',
  site_code      TEXT DEFAULT 'BK10',
  current_stock  INTEGER DEFAULT 0,
  monthly_out    INTEGER DEFAULT 0,
  daily_out      INTEGER DEFAULT 0,
  total_3m       INTEGER DEFAULT 0,
  item_name      TEXT DEFAULT '',
  synced_at      TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_invsnap_entity ON inventory_snapshot(legal_entity);
CREATE INDEX IF NOT EXISTS idx_invsnap_synced ON inventory_snapshot(synced_at);
ALTER TABLE inventory_snapshot OWNER TO sc_erp;

\echo '✅ 완료. 컨테이너 재기동 후 ERP 페이지에서 DB 동기화 버튼 클릭.'
