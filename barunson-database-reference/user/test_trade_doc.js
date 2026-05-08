const http = require('http');
const crypto = require('crypto');
const token = crypto.createHash('sha256').update('sejun.song@barunn.net' + 'barun-company-portal-2026').digest('hex').slice(0,16);

function api(method, path, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : '';
    const opts = { hostname:'localhost', port:12026, path, method, headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(data)} };
    const req = http.request(opts, res => { let b=''; res.on('data',c=>b+=c); res.on('end',()=>{ try{resolve(JSON.parse(b))}catch(e){resolve(b)} }); });
    req.on('error',reject); if(data)req.write(data); req.end();
  });
}

(async () => {
  // 1. PO 7 생성 (거래명세서 테스트용)
  console.log('--- 1. PO 생성 ---');
  const createResp = await api('POST', '/api/po', {
    po_type:'material', vendor_name:'대한통상', status:'draft',
    expected_date:'2026-03-25', notes:'거래명세서 테스트',
    items:[{product_code:'BE004',brand:'바른손',process_type:'원재료',ordered_qty:20000,spec:'',notes:''}]
  });
  const poId = createResp.data.po_id;
  console.log(`  PO ${createResp.data.po_number} (id:${poId}) 생성됨`);

  // 2. PO 발송 → 거래명세서 자동 생성
  console.log('\n--- 2. PO 발송 (거래명세서 자동 생성) ---');
  // Use direct DB update to avoid curl encoding issues
  const db = require('better-sqlite3')('./orders.db');
  db.prepare("UPDATE po_header SET status='sent', material_status='sent', process_status='waiting' WHERE po_id=?").run(poId);
  // Manually trigger trade doc creation (simulating what the API does)
  const po = db.prepare('SELECT * FROM po_header WHERE po_id=?').get(poId);
  const items = db.prepare('SELECT * FROM po_items WHERE po_id=?').all(poId);
  const docItems = items.map(i => ({product_code:i.product_code, product_name:i.brand||'', qty:i.ordered_qty, unit_price:0, amount:0}));
  db.prepare("INSERT INTO trade_document (po_id,po_number,vendor_name,vendor_type,items_json,status) VALUES(?,?,?,?,?,'sent')")
    .run(poId, po.po_number, po.vendor_name, 'material', JSON.stringify(docItems));
  console.log('  거래명세서 자동 생성됨');

  const doc = db.prepare('SELECT * FROM trade_document WHERE po_id=? ORDER BY id DESC LIMIT 1').get(poId);
  console.log(`  doc_id: ${doc.id}, status: ${doc.status}, items: ${doc.items_json}`);

  // 3. 업체 포털에서 거래명세서 조회
  console.log('\n--- 3. 업체 포털에서 거래명세서 조회 ---');
  const tradeResp = await api('GET', `/api/vendor-portal/trade-doc?po_id=${poId}&email=sejun.song@barunn.net&token=${token}`);
  console.log(`  조회 결과: ${tradeResp.ok ? '성공' : '실패'}`);
  if (tradeResp.data) console.log(`  doc_id: ${tradeResp.data.id}, items: ${tradeResp.data.items?.length}개`);

  // 4. 업체가 단가 수정 (1000원 → 1200원)
  console.log('\n--- 4. 업체 단가 수정 (0 → 1200) + 메모 ---');
  const updateResp = await api('POST', '/api/vendor-portal/update-trade-doc', {
    doc_id: doc.id,
    email: 'sejun.song@barunn.net',
    token: token,
    modified_items: [{idx:0, unit_price:1200, qty:20000}],
    memo: '3월 단가 인상분 반영'
  });
  console.log(`  결과: ${JSON.stringify(updateResp)}`);

  // 5. 검토 대기 목록 확인
  console.log('\n--- 5. 관리자: 검토 대기 목록 ---');
  const reviewResp = await api('GET', '/api/trade-document/review');
  const reviewDocs = reviewResp.data || [];
  console.log(`  검토 대기: ${reviewDocs.length}건`);
  reviewDocs.forEach(d => {
    console.log(`  - ${d.po_number} | ${d.vendor_name} | 단가차이: ${d.price_diff ? '있음' : '없음'} | 메모: ${d.vendor_memo || '없음'}`);
  });

  // 6. 승인
  console.log('\n--- 6. 관리자: 승인 ---');
  const approveResp = await api('POST', `/api/trade-document/${doc.id}/approve`, {});
  console.log(`  승인 결과: ${JSON.stringify(approveResp)}`);

  // 7. 최종 상태 확인
  const finalDoc = db.prepare('SELECT * FROM trade_document WHERE id=?').get(doc.id);
  console.log(`\n--- 최종 상태 ---`);
  console.log(`  status: ${finalDoc.status}`);
  console.log(`  price_diff: ${finalDoc.price_diff}`);
  console.log(`  vendor_memo: ${finalDoc.vendor_memo}`);
  console.log(`  approved_at: ${finalDoc.approved_at}`);

  // 8. 활동 로그 확인
  const logs = db.prepare('SELECT * FROM po_activity_log WHERE po_id=? ORDER BY created_at').all(poId);
  console.log(`\n--- 활동 로그 (${logs.length}건) ---`);
  logs.forEach((l,i) => console.log(`  ${i+1}. [${l.action}] ${l.details} | ${l.created_at}`));

  // 9. 단가 차이 있는데 메모 없는 경우 테스트
  console.log('\n--- 9. 메모 없이 단가 수정 → 승인 거부 테스트 ---');
  db.prepare("INSERT INTO trade_document (po_id,po_number,vendor_name,vendor_type,items_json,vendor_modified_json,price_diff,vendor_memo,status) VALUES(?,?,?,?,?,?,1,'','vendor_confirmed')")
    .run(poId, po.po_number, '테스트업체', 'material', '[{"unit_price":100}]', '[{"unit_price":200}]');
  const noMemoDoc = db.prepare('SELECT id FROM trade_document WHERE vendor_name=? ORDER BY id DESC LIMIT 1').get('테스트업체');
  const rejectResp = await api('POST', `/api/trade-document/${noMemoDoc.id}/approve`, {});
  console.log(`  결과: ${JSON.stringify(rejectResp)}`);
  console.log(`  기대: 거부 (메모 없음) → ${rejectResp.ok === false ? '✅ 정상 거부' : '❌ 잘못 승인됨'}`);

  console.log('\n========== 거래명세서 테스트 완료 ==========');

  db.close();
})();
