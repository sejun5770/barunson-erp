#!/usr/bin/env node
/**
 * 재고현황 엑셀 → SQLite (orders.db) inventory_snapshot import
 * 사용법:
 *   node scripts/import-inventory-excel.js <엑셀파일경로>
 *
 * 엑셀 컬럼: 상태, 운영구분, 제품코드, 품목명, 생산지, 법인, 브랜드,
 *           가용재고, 월출고, 일평균, 가용일수, 안전재고, 미입고,
 *           OS번호, 예상재고, 발주필요, 메모
 *
 * 매핑:
 *   제품코드 → product_code (PK)
 *   법인     → legal_entity (바른컴퍼니→barunson, 디디→dd)
 *   가용재고 → current_stock
 *   월출고   → monthly_out
 *   일평균   → daily_out
 *   품목명   → item_name
 *
 * 멱등: product_code 가 이미 있으면 UPDATE (REPLACE).
 */
const path = require('path');
const xlsx = require('xlsx');
const Database = require('better-sqlite3');

const xlsxPath = process.argv[2];
if (!xlsxPath) {
  console.error('Usage: node import-inventory-excel.js <xlsx 경로>');
  process.exit(1);
}

const dbPath = process.env.DB_PATH || path.join(__dirname, '..', 'barunson-database-reference', 'user', 'orders.db');
console.log('DB:', dbPath);
console.log('XLSX:', xlsxPath);

const wb = xlsx.readFile(xlsxPath);
const ws = wb.Sheets['재고현황'] || wb.Sheets[wb.SheetNames[0]];
const rows = xlsx.utils.sheet_to_json(ws, { defval: '' });
console.log('읽은 행:', rows.length);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

// REPLACE 로 갱신 (스냅샷 시점 데이터로 덮어쓰기)
const upsert = db.prepare(`
  INSERT INTO inventory_snapshot
    (product_code, legal_entity, site_code, current_stock, monthly_out,
     daily_out, total_3m, item_name, synced_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
  ON CONFLICT(product_code) DO UPDATE SET
    legal_entity = excluded.legal_entity,
    site_code    = excluded.site_code,
    current_stock = excluded.current_stock,
    monthly_out  = excluded.monthly_out,
    daily_out    = excluded.daily_out,
    total_3m     = excluded.total_3m,
    item_name    = excluded.item_name,
    synced_at    = excluded.synced_at
`);

const ENTITY_MAP = { '바른컴퍼니': 'barunson', '디디': 'dd', '디얼디어': 'dd' };
const SITE_MAP = { 'barunson': 'BK10', 'dd': 'BHC2' };

let inserted = 0, updated = 0, failed = 0;
const tx = db.transaction(() => {
  for (const r of rows) {
    const code = String(r['제품코드'] || '').trim();
    if (!code) continue;
    try {
      const entity = ENTITY_MAP[r['법인']] || 'barunson';
      const siteCode = SITE_MAP[entity] || 'BK10';
      const monthly = Number(r['월출고']) || 0;
      const daily = Number(r['일평균']) || 0;
      const stock = Number(r['가용재고']) || 0;
      const total3m = monthly * 3; // 3개월 출고 추정 (월출고 x 3)

      upsert.run(
        code,
        entity,
        siteCode,
        stock,
        monthly,
        daily,
        total3m,
        String(r['품목명'] || '').trim()
      );
      inserted++;
    } catch (e) {
      failed++;
      if (failed <= 3) console.warn(`[fail] ${code}:`, e.message);
    }
  }
});

tx();

console.log('\n=== 결과 ===');
console.log(`  처리: ${inserted}건, 실패: ${failed}건`);
const total = db.prepare('SELECT COUNT(*) c FROM inventory_snapshot').get().c;
const totalStock = db.prepare('SELECT SUM(current_stock) s FROM inventory_snapshot').get().s;
const totalByEntity = db.prepare(
  `SELECT legal_entity, COUNT(*) c, SUM(current_stock) s FROM inventory_snapshot GROUP BY legal_entity`
).all();
console.log(`  총 inventory_snapshot 건수: ${total}`);
console.log(`  총 가용재고: ${(totalStock || 0).toLocaleString()}매`);
console.log('  법인별:');
for (const e of totalByEntity) {
  console.log(`    ${e.legal_entity}: ${e.c}건, ${(e.s || 0).toLocaleString()}매`);
}

db.close();
