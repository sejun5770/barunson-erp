#!/usr/bin/env node
/**
 * 발주현황 엑셀 → SQLite (orders.db) import
 * 사용법:
 *   node scripts/import-po-excel.js <엑셀파일경로>
 *
 * 엑셀 컬럼: 발주번호, 발주일, 입고예정일, 거래처, 발주유형, 상태, 생산지,
 *           제품코드, 브랜드, 공정, 원자재코드, 원재료용지명,
 *           발주수량, 발주(R), OS번호, 입고수량, 비고
 *
 * 멱등: po_number 가 이미 있으면 skip.
 */
const path = require('path');
const xlsx = require('xlsx');
const Database = require('better-sqlite3');

const xlsxPath = process.argv[2];
if (!xlsxPath) {
  console.error('Usage: node import-po-excel.js <xlsx 경로>');
  process.exit(1);
}

const dbPath = process.env.DB_PATH || path.join(__dirname, '..', 'barunson-database-reference', 'user', 'orders.db');
console.log('DB:', dbPath);
console.log('XLSX:', xlsxPath);

const wb = xlsx.readFile(xlsxPath);
const ws = wb.Sheets['발주현황'] || wb.Sheets[wb.SheetNames[0]];
const rows = xlsx.utils.sheet_to_json(ws, { defval: '' });
console.log('읽은 행:', rows.length);

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

const byPo = {};
for (const r of rows) {
  const num = r['발주번호'];
  if (!num) continue;
  if (!byPo[num]) byPo[num] = [];
  byPo[num].push(r);
}
console.log('발주번호 그룹:', Object.keys(byPo).length);

const insertHeader = db.prepare(`
  INSERT OR IGNORE INTO po_header
    (po_number, po_type, vendor_name, po_date, status, expected_date,
     total_qty, notes, os_number, origin, created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
`);
const getPoId = db.prepare(`SELECT po_id FROM po_header WHERE po_number = ?`);
const insertItem = db.prepare(`
  INSERT OR IGNORE INTO po_items
    (po_id, product_code, brand, process_type, ordered_qty, received_qty,
     spec, notes, os_number)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

let headersInserted = 0, headersSkipped = 0, itemsInserted = 0;

const tx = db.transaction(() => {
  for (const [poNum, items] of Object.entries(byPo)) {
    const first = items[0];
    const totalQty = items.reduce((s, r) => s + (Number(r['발주수량']) || 0), 0);
    const headerNotes = items.map(r => r['비고']).filter(Boolean).join(' / ');

    const headerResult = insertHeader.run(
      poNum,
      first['발주유형'] || '원재료',
      first['거래처'] || '',
      first['발주일'] || '',
      first['상태'] || '대기',
      first['입고예정일'] || '',
      totalQty,
      headerNotes,
      first['OS번호'] || '',
      first['생산지'] || ''
    );

    if (headerResult.changes > 0) headersInserted++;
    else headersSkipped++;

    const poRow = getPoId.get(poNum);
    if (!poRow) {
      console.warn(`[skip] po_id 못 찾음: ${poNum}`);
      continue;
    }
    const poId = poRow.po_id;

    if (headerResult.changes === 0) continue;

    for (const r of items) {
      const itemResult = insertItem.run(
        poId,
        r['제품코드'] || '',
        r['브랜드'] || '',
        r['공정'] || r['발주유형'] || '',
        Number(r['발주수량']) || 0,
        Number(r['입고수량']) || 0,
        r['원재료용지명'] || '',
        r['비고'] || '',
        r['OS번호'] || ''
      );
      if (itemResult.changes > 0) itemsInserted++;
    }
  }
});

tx();

console.log('\n=== 결과 ===');
console.log(`  발주서 신규: ${headersInserted}건`);
console.log(`  발주서 skip(이미 존재): ${headersSkipped}건`);
console.log(`  품목 신규: ${itemsInserted}건`);
console.log(`  현재 po_header 총 건수: ${db.prepare('SELECT COUNT(*) c FROM po_header').get().c}`);
console.log(`  현재 po_items 총 건수: ${db.prepare('SELECT COUNT(*) c FROM po_items').get().c}`);

db.close();
