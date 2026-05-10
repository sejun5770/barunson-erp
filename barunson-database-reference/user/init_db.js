/**
 * SQLite DB 초기화 스크립트
 * 실행: node init_db.js
 */
const Database = require('better-sqlite3');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || __dirname;
const DB_PATH = path.join(DATA_DIR, 'orders.db');
const db = new Database(DB_PATH);

db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  -- 거래처 마스터
  CREATE TABLE IF NOT EXISTS vendors (
    vendor_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_code TEXT DEFAULT '',
    name        TEXT NOT NULL,
    type        TEXT DEFAULT '',
    contact     TEXT DEFAULT '',
    phone       TEXT DEFAULT '',
    email       TEXT DEFAULT '',
    kakao       TEXT DEFAULT '',
    memo        TEXT DEFAULT '',
    created_at  TEXT DEFAULT (datetime('now','localtime')),
    updated_at  TEXT DEFAULT (datetime('now','localtime'))
  );

  -- 발주 헤더
  CREATE TABLE IF NOT EXISTS po_header (
    po_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    po_number   TEXT UNIQUE NOT NULL,
    po_type     TEXT NOT NULL DEFAULT 'material',
    vendor_name TEXT DEFAULT '',
    po_date     TEXT DEFAULT (date('now','localtime')),
    status      TEXT DEFAULT 'draft',
    expected_date TEXT DEFAULT '',
    total_qty   INTEGER DEFAULT 0,
    notes       TEXT DEFAULT '',
    created_at  TEXT DEFAULT (datetime('now','localtime')),
    updated_at  TEXT DEFAULT (datetime('now','localtime'))
  );

  -- 발주 상세
  CREATE TABLE IF NOT EXISTS po_items (
    item_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id        INTEGER NOT NULL REFERENCES po_header(po_id) ON DELETE CASCADE,
    product_code TEXT NOT NULL,
    brand        TEXT DEFAULT '',
    process_type TEXT DEFAULT '',
    ordered_qty  INTEGER DEFAULT 0,
    received_qty INTEGER DEFAULT 0,
    spec         TEXT DEFAULT '',
    notes        TEXT DEFAULT ''
  );

  -- 수령 기록
  CREATE TABLE IF NOT EXISTS receipts (
    receipt_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id        INTEGER NOT NULL REFERENCES po_header(po_id),
    receipt_date TEXT DEFAULT (date('now','localtime')),
    received_by  TEXT DEFAULT '',
    notes        TEXT DEFAULT '',
    created_at   TEXT DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS receipt_items (
    receipt_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id      INTEGER NOT NULL REFERENCES receipts(receipt_id) ON DELETE CASCADE,
    po_item_id      INTEGER REFERENCES po_items(item_id),
    product_code    TEXT NOT NULL,
    received_qty    INTEGER DEFAULT 0,
    defect_qty      INTEGER DEFAULT 0,
    notes           TEXT DEFAULT ''
  );

  -- 거래명세서 (인보이스)
  CREATE TABLE IF NOT EXISTS invoices (
    invoice_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id        INTEGER REFERENCES po_header(po_id),
    vendor_name  TEXT DEFAULT '',
    invoice_no   TEXT DEFAULT '',
    invoice_date TEXT DEFAULT '',
    amount       REAL DEFAULT 0,
    file_path    TEXT DEFAULT '',
    file_name    TEXT DEFAULT '',
    status       TEXT DEFAULT 'received',
    notes        TEXT DEFAULT '',
    created_at   TEXT DEFAULT (datetime('now','localtime'))
  );

  -- 거래명세서 품목 상세
  CREATE TABLE IF NOT EXISTS invoice_items (
    inv_item_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_id    INTEGER NOT NULL REFERENCES invoices(invoice_id) ON DELETE CASCADE,
    product_code  TEXT NOT NULL DEFAULT '',
    product_name  TEXT DEFAULT '',
    qty           INTEGER DEFAULT 0,
    unit_price    REAL DEFAULT 0,
    amount        REAL DEFAULT 0,
    notes         TEXT DEFAULT ''
  );

  -- 거래처 미팅일지/특이사항
  CREATE TABLE IF NOT EXISTS vendor_notes (
    note_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id   INTEGER REFERENCES vendors(vendor_id),
    vendor_name TEXT DEFAULT '',
    title       TEXT NOT NULL,
    content     TEXT DEFAULT '',
    note_type   TEXT DEFAULT 'meeting',
    note_date   TEXT DEFAULT (date('now','localtime')),
    created_at  TEXT DEFAULT (datetime('now','localtime')),
    updated_at  TEXT DEFAULT (datetime('now','localtime'))
  );

  -- BOM 헤더
  CREATE TABLE IF NOT EXISTS bom_header (
    bom_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    product_code  TEXT NOT NULL UNIQUE,
    product_name  TEXT DEFAULT '',
    brand         TEXT DEFAULT '',
    version       INTEGER DEFAULT 1,
    notes         TEXT DEFAULT '',
    created_at    TEXT DEFAULT (datetime('now','localtime')),
    updated_at    TEXT DEFAULT (datetime('now','localtime'))
  );

  -- BOM 상세 (자재명세)
  CREATE TABLE IF NOT EXISTS bom_items (
    bom_item_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    bom_id        INTEGER NOT NULL REFERENCES bom_header(bom_id) ON DELETE CASCADE,
    item_type     TEXT NOT NULL DEFAULT 'material',
    material_code TEXT DEFAULT '',
    material_name TEXT DEFAULT '',
    vendor_name   TEXT DEFAULT '',
    process_type  TEXT DEFAULT '',
    qty_per       REAL DEFAULT 1,
    cut_spec      TEXT DEFAULT '',
    plate_spec    TEXT DEFAULT '',
    unit          TEXT DEFAULT 'EA',
    notes         TEXT DEFAULT '',
    sort_order    INTEGER DEFAULT 0
  );

  -- 생산계획
  CREATE TABLE IF NOT EXISTS production_plan (
    plan_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_month    TEXT NOT NULL,
    product_code  TEXT NOT NULL,
    product_name  TEXT DEFAULT '',
    brand         TEXT DEFAULT '',
    planned_qty   INTEGER DEFAULT 0,
    confirmed     INTEGER DEFAULT 0,
    notes         TEXT DEFAULT '',
    created_at    TEXT DEFAULT (datetime('now','localtime')),
    updated_at    TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(plan_month, product_code)
  );

  -- MRP 실행 결과
  CREATE TABLE IF NOT EXISTS mrp_result (
    result_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date      TEXT DEFAULT (datetime('now','localtime')),
    plan_month    TEXT NOT NULL,
    product_code  TEXT NOT NULL,
    material_code TEXT DEFAULT '',
    material_name TEXT DEFAULT '',
    vendor_name   TEXT DEFAULT '',
    process_type  TEXT DEFAULT '',
    gross_req     REAL DEFAULT 0,
    on_hand       REAL DEFAULT 0,
    on_order      REAL DEFAULT 0,
    net_req       REAL DEFAULT 0,
    order_qty     REAL DEFAULT 0,
    unit          TEXT DEFAULT 'EA',
    status        TEXT DEFAULT 'planned'
  );

  -- 인덱스
  CREATE INDEX IF NOT EXISTS idx_po_status ON po_header(status);
  CREATE INDEX IF NOT EXISTS idx_po_date ON po_header(po_date);
  CREATE INDEX IF NOT EXISTS idx_po_items_po ON po_items(po_id);
  CREATE INDEX IF NOT EXISTS idx_receipts_po ON receipts(po_id);
  CREATE INDEX IF NOT EXISTS idx_invoices_po ON invoices(po_id);
  CREATE INDEX IF NOT EXISTS idx_invoice_items_inv ON invoice_items(invoice_id);
  CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(invoice_date);
  CREATE INDEX IF NOT EXISTS idx_invoices_vendor ON invoices(vendor_name);
  CREATE INDEX IF NOT EXISTS idx_notes_vendor ON vendor_notes(vendor_id);
  CREATE INDEX IF NOT EXISTS idx_notes_date ON vendor_notes(note_date);
  CREATE INDEX IF NOT EXISTS idx_bom_items_bom ON bom_items(bom_id);
  CREATE INDEX IF NOT EXISTS idx_plan_month ON production_plan(plan_month);
  CREATE INDEX IF NOT EXISTS idx_mrp_month ON mrp_result(plan_month);
  CREATE INDEX IF NOT EXISTS idx_mrp_run ON mrp_result(run_date);

  -- 발주이력 (생산발주현황 시트 데이터)
  CREATE TABLE IF NOT EXISTS order_history (
    history_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    order_date     TEXT DEFAULT '',
    os_no          TEXT DEFAULT '',
    warehouse_order TEXT DEFAULT '',
    product_name   TEXT DEFAULT '',
    product_code   TEXT NOT NULL,
    actual_qty     INTEGER DEFAULT 0,
    material_code  TEXT DEFAULT '',
    material_name  TEXT DEFAULT '',
    paper_maker    TEXT DEFAULT '',
    vendor_code    TEXT DEFAULT '',
    qty            REAL DEFAULT 0,
    cut_spec       TEXT DEFAULT '',
    plate_spec     TEXT DEFAULT '',
    cutting        TEXT DEFAULT '',
    printing       TEXT DEFAULT '',
    foil_emboss    TEXT DEFAULT '',
    thomson        TEXT DEFAULT '',
    envelope_proc  TEXT DEFAULT '',
    seari          TEXT DEFAULT '',
    laser          TEXT DEFAULT '',
    silk           TEXT DEFAULT '',
    outsource      TEXT DEFAULT '',
    order_qty      INTEGER DEFAULT 0,
    product_spec   TEXT DEFAULT '',
    source_sheet   TEXT DEFAULT '',
    created_at     TEXT DEFAULT (datetime('now','localtime'))
  );
  CREATE INDEX IF NOT EXISTS idx_oh_product ON order_history(product_code);
  CREATE INDEX IF NOT EXISTS idx_oh_date ON order_history(order_date);
`);

console.log('DB 테이블 생성 완료:', DB_PATH);

// ── 시드 데이터 임포트 (빈 DB일 때만) ──────────────────────────────
const fs = require('fs');
const seedDir = __dirname;

function seedTable(fileName, tableName, uniqueCol) {
  const filePath = path.join(seedDir, fileName);
  if (!fs.existsSync(filePath)) { console.log(`  ⏭ ${fileName} 없음 — 건너뜀`); return 0; }
  const existing = db.prepare(`SELECT COUNT(*) AS cnt FROM ${tableName}`).get();
  if (existing.cnt > 0) { console.log(`  ⏭ ${tableName}: 이미 ${existing.cnt}건 존재 — 건너뜀`); return 0; }
  try {
    const rows = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    if (!rows.length) return 0;
    const cols = Object.keys(rows[0]).filter(c => c !== 'id' && c !== 'user_id' && c !== 'vendor_id' && c !== 'product_id');
    const placeholders = cols.map(c => '@' + c).join(',');
    const stmt = db.prepare(`INSERT OR IGNORE INTO ${tableName} (${cols.join(',')}) VALUES (${placeholders})`);
    const tx = db.transaction((data) => { for (const r of data) stmt.run(r); });
    tx(rows);
    console.log(`  ✅ ${tableName}: ${rows.length}건 임포트`);
    return rows.length;
  } catch (e) { console.log(`  ❌ ${tableName} 임포트 실패:`, e.message); return 0; }
}

console.log('시드 데이터 임포트 시작...');
seedTable('seed_products.json', 'products', 'product_code');
seedTable('seed_vendors.json', 'vendors', 'name');
seedTable('seed_users.json', 'users', 'email');
seedTable('seed_auto_order.json', 'auto_order_items', 'product_code');
seedTable('seed_menu_settings.json', 'menu_settings', 'page_id');
console.log('DB 초기화 완료!');

// localStorage 거래처 데이터 마이그레이션 헬퍼
const migrateVendors = db.prepare(`
  INSERT OR IGNORE INTO vendors (name, type, contact, phone, email, kakao, memo)
  VALUES (@name, @type, @contact, @phone, @email, @kakao, @memo)
`);

module.exports = { DB_PATH, migrateVendors };

db.close();
