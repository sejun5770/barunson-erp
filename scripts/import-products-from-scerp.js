#!/usr/bin/env node
/**
 * s.c-erp 의 /api/products export → berp SQLite (orders.db) import
 *
 * 사용법:
 *   node scripts/import-products-from-scerp.js <scerp-products.json> [--apply] [--db-path /app/data/orders.db]
 *
 * 기본 동작 (dry-run):
 *   - INSERT/UPDATE 후보만 카운트해서 출력. 실제 변경 없음.
 * --apply 가 명시될 때만 INSERT/UPDATE 실행.
 *
 * 처리 규칙 (안전 위주):
 *   1. INSERT: product_code 가 berp 에 없는 행만 신규 추가
 *      - status 는 운영 export 의 값 그대로 (보통 active)
 *   2. UPDATE: 양쪽에 있고 berp 의 product_name 이 비어있는 경우만 s.c-erp 값으로 채움
 *      - 사용자가 의도적으로 비운 케이스 보존을 위해 덮어쓰기는 하지 않음
 *   3. UPDATE: origin 이 sc-erp 와 다르고 sc-erp 가 비어있지 않은 경우만 갱신
 *
 * 멱등: 같은 입력으로 다시 실행 시 INSERT 0 / UPDATE 0
 */
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const args = process.argv.slice(2);
const inputPath = args.find(a => !a.startsWith('--'));
const apply = args.includes('--apply');
const dbPathArg = args.find(a => a.startsWith('--db-path='))?.split('=')[1];
const dbPath = dbPathArg
  || process.env.DB_PATH
  || path.join(__dirname, '..', 'barunson-database-reference', 'user', 'orders.db');

if (!inputPath) {
  console.error('Usage: node scripts/import-products-from-scerp.js <scerp-products.json> [--apply] [--db-path=/path/to/orders.db]');
  process.exit(1);
}
if (!fs.existsSync(inputPath)) {
  console.error('입력 파일 없음:', inputPath);
  process.exit(1);
}
if (!fs.existsSync(dbPath)) {
  console.error('SQLite DB 파일 없음:', dbPath);
  console.error('  운영 dm-berp 컨테이너 안에서 실행하거나 --db-path 로 경로 지정 필요');
  process.exit(1);
}

console.log('입력:', inputPath);
console.log('DB  :', dbPath);
console.log('모드:', apply ? '!! APPLY (실제 변경) !!' : 'dry-run');
console.log();

const raw = JSON.parse(fs.readFileSync(inputPath, 'utf-8'));
const products = raw.data || (Array.isArray(raw) ? raw : []);
console.log('s.c-erp products:', products.length);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

const berpRows = db.prepare('SELECT product_code, product_name, origin FROM products').all();
const berpByCode = {};
berpRows.forEach(r => { berpByCode[r.product_code] = r; });
console.log('berp products :', berpRows.length);
console.log();

const inserts = [];
const nameUpdates = [];
const originUpdates = [];

for (const p of products) {
  const code = String(p.product_code || '').trim();
  if (!code) continue;
  const existing = berpByCode[code];
  if (!existing) {
    inserts.push(p);
    continue;
  }
  if (!existing.product_name && p.product_name) {
    nameUpdates.push({ code, name: String(p.product_name) });
  }
  if (p.origin && (existing.origin || '') !== p.origin) {
    originUpdates.push({ code, origin: String(p.origin), prev: existing.origin || '' });
  }
}

console.log('=== 분류 결과 ===');
console.log('INSERT 후보   :', inserts.length, '건');
console.log('UPDATE name   :', nameUpdates.length, '건 (berp 가 비어있고 sc-erp 에 값 있음)');
console.log('UPDATE origin :', originUpdates.length, '건');
console.log();

if (inserts.length) {
  console.log('--- INSERT 샘플 (5) ---');
  inserts.slice(0, 5).forEach(p => console.log('  ', p.product_code, '|', p.product_name || '(이름없음)', '|', 'brand:'+(p.brand||'-'), 'entity:'+(p.legal_entity||'barunson')));
  console.log();
}
if (nameUpdates.length) {
  console.log('--- UPDATE name 샘플 (5) ---');
  nameUpdates.slice(0, 5).forEach(u => console.log('  ', u.code, '→', u.name));
  console.log();
}
if (originUpdates.length) {
  console.log('--- UPDATE origin (전체) ---');
  originUpdates.forEach(u => console.log('  ', u.code, ':', u.prev || '(empty)', '→', u.origin));
  console.log();
}

if (!apply) {
  console.log('=== dry-run 모드 — 변경 없음. 실제 적용은 --apply 옵션 추가 ===');
  db.close();
  process.exit(0);
}

console.log('=== APPLY 시작 ===');

const insStmt = db.prepare(`
  INSERT OR IGNORE INTO products
    (product_code, product_name, brand, origin, category, status, legal_entity,
     unit, material_code, material_name, spec, product_spec, memo,
     created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          datetime('now','localtime'), datetime('now','localtime'))
`);

let inserted = 0, insertFailed = 0;
const insertTx = db.transaction(() => {
  for (const p of inserts) {
    try {
      const r = insStmt.run(
        p.product_code,
        p.product_name || '',
        p.brand || '',
        p.origin || '',
        p.category || '',
        p.status || 'active',
        p.legal_entity || 'barunson',
        p.unit || 'EA',
        p.material_code || '',
        p.material_name || '',
        p.spec || '',
        p.product_spec || '',
        p.memo || ''
      );
      if (r.changes > 0) inserted++;
    } catch (e) {
      insertFailed++;
      if (insertFailed <= 3) console.warn('  [INSERT 실패]', p.product_code, e.message);
    }
  }
});
insertTx();
console.log('INSERT 완료 :', inserted, '건 (실패', insertFailed + ')');

const updNameStmt = db.prepare(`UPDATE products SET product_name = ?, updated_at = datetime('now','localtime') WHERE product_code = ? AND (product_name IS NULL OR product_name = '')`);
let nameUpdated = 0;
const nameTx = db.transaction(() => {
  for (const u of nameUpdates) {
    const r = updNameStmt.run(u.name, u.code);
    if (r.changes > 0) nameUpdated++;
  }
});
nameTx();
console.log('UPDATE name :', nameUpdated, '건');

const updOriginStmt = db.prepare(`UPDATE products SET origin = ?, updated_at = datetime('now','localtime') WHERE product_code = ?`);
let originUpdated = 0;
const originTx = db.transaction(() => {
  for (const u of originUpdates) {
    const r = updOriginStmt.run(u.origin, u.code);
    if (r.changes > 0) originUpdated++;
  }
});
originTx();
console.log('UPDATE origin:', originUpdated, '건');

const total = db.prepare('SELECT COUNT(*) AS c FROM products').get().c;
console.log();
console.log('=== 결과 ===');
console.log('총 products :', total, '건');
console.log('INSERT       :', inserted, '/', insertFailed, '실패');
console.log('UPDATE name  :', nameUpdated);
console.log('UPDATE origin:', originUpdated);

db.close();
