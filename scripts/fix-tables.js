const { Client } = require('pg');
const Database = require('better-sqlite3');

(async () => {
  const pg = new Client({
    host: process.env.PG_HOST || 'onely-postgres',
    port: 5432, user: 'onely', password: 'onely', database: 'sc_erp'
  });
  await pg.connect();

  // 실패한 4개 테이블 수동 생성
  const sqls = [
    `CREATE TABLE IF NOT EXISTS incoming_inspections (
      id SERIAL PRIMARY KEY, po_id INTEGER, item_id INTEGER,
      product_code TEXT DEFAULT '', inspected_qty INTEGER DEFAULT 0,
      passed_qty INTEGER DEFAULT 0, failed_qty INTEGER DEFAULT 0,
      inspector TEXT DEFAULT '', inspect_date TEXT DEFAULT '',
      result TEXT DEFAULT 'pass', memo TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS po_header (
      po_id SERIAL PRIMARY KEY, po_number TEXT UNIQUE NOT NULL,
      po_date TEXT DEFAULT '', due_date TEXT DEFAULT '',
      vendor_id INTEGER, vendor_name TEXT DEFAULT '',
      vendor_contact TEXT DEFAULT '', vendor_phone TEXT DEFAULT '',
      vendor_email TEXT DEFAULT '', status TEXT DEFAULT 'draft',
      notes TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(), po_type TEXT DEFAULT '',
      process_chain TEXT DEFAULT '', process_step INTEGER DEFAULT 0,
      parent_po_id INTEGER, material_status TEXT DEFAULT '',
      process_status TEXT DEFAULT '', os_number TEXT DEFAULT '',
      post_vendor_email TEXT DEFAULT ''
    )`,
    `CREATE TABLE IF NOT EXISTS receipts (
      id SERIAL PRIMARY KEY, po_id INTEGER, receipt_no TEXT UNIQUE,
      receipt_date TEXT DEFAULT '',
      warehouse_id INTEGER DEFAULT 1, receiver TEXT DEFAULT '',
      notes TEXT DEFAULT '', status TEXT DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS vendor_notes (
      id SERIAL PRIMARY KEY, vendor_id INTEGER NOT NULL,
      note_date TEXT DEFAULT '',
      content TEXT NOT NULL DEFAULT '', author TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW()
    )`
  ];

  for (const s of sqls) {
    try {
      await pg.query(s);
      console.log('OK:', s.match(/CREATE TABLE[^(]+/)[0].trim());
    } catch (e) {
      console.error('FAIL:', e.message.split('\n')[0]);
    }
  }

  // po_header 데이터 이관
  const db = new Database('/app/data/orders.db', { readonly: true });
  const rows = db.prepare('SELECT * FROM po_header').all();
  for (const r of rows) {
    const cols = Object.keys(r);
    const vals = cols.map(c => r[c]);
    const ph = cols.map((_, i) => '$' + (i + 1)).join(', ');
    try {
      await pg.query(`INSERT INTO "po_header" (${cols.map(c => '"' + c + '"').join(', ')}) VALUES (${ph})`, vals);
    } catch (e) {
      // skip duplicates
    }
  }
  console.log('po_header data:', rows.length, 'rows');

  // 시퀀스 재설정
  try {
    await pg.query("SELECT setval(pg_get_serial_sequence('po_header', 'po_id'), COALESCE((SELECT MAX(po_id) FROM po_header), 0) + 1, false)");
  } catch (_) {}

  db.close();
  await pg.end();
  console.log('Done');
})();
