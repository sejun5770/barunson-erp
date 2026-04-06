// routes/journal-auto.js — 자동 분개 생성 모듈
const Router = require('./_router');
const ctx = require('./_ctx');
const router = new Router();

// 계정코드 매핑 (KNOWN_ACCOUNTS 기반)
const ACCT = {
  RAW_MATERIAL: '11154101',  // 원재료
  ACCOUNTS_PAYABLE: '21140101', // 미지급금
  ACCOUNTS_RECEIVABLE: '11125101', // 외상매출금 (매출채권)
  SALES_REVENUE: '41110101', // 상품매출
};

function ensureTables() {
  const { db } = ctx;
  db.exec(`CREATE TABLE IF NOT EXISTS journal_auto_config (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT UNIQUE NOT NULL,
    enabled     INTEGER DEFAULT 1,
    debit_account TEXT NOT NULL,
    credit_account TEXT NOT NULL,
    description TEXT DEFAULT '',
    updated_at  TEXT DEFAULT (datetime('now','localtime'))
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS journal_auto_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type     TEXT NOT NULL,
    ref_id         INTEGER,
    ref_number     TEXT DEFAULT '',
    debit_account  TEXT NOT NULL,
    credit_account TEXT NOT NULL,
    amount         REAL DEFAULT 0,
    status         TEXT DEFAULT 'created',
    error_message  TEXT DEFAULT '',
    created_by     TEXT DEFAULT 'system',
    created_at     TEXT DEFAULT (datetime('now','localtime'))
  )`);
  db.exec("CREATE INDEX IF NOT EXISTS idx_jalog_event ON journal_auto_log(event_type)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_jalog_ref ON journal_auto_log(ref_id)");

  // 기본 설정 시드
  const cnt = db.prepare('SELECT COUNT(*) AS c FROM journal_auto_config').get().c;
  if (cnt === 0) {
    const ins = db.prepare('INSERT OR IGNORE INTO journal_auto_config (event_type, enabled, debit_account, credit_account, description) VALUES (?,?,?,?,?)');
    ins.run('goods_receipt', 1, ACCT.RAW_MATERIAL, ACCT.ACCOUNTS_PAYABLE, '입고 시 원재료/미지급금 분개');
    ins.run('sales_shipment', 1, ACCT.ACCOUNTS_RECEIVABLE, ACCT.SALES_REVENUE, '출하 시 매출채권/매출 분개');
  }
}

let tablesReady = false;
function init() {
  if (!tablesReady && ctx.db) { ensureTables(); tablesReady = true; }
}

function getConfig(eventType) {
  init();
  return ctx.db.prepare('SELECT * FROM journal_auto_config WHERE event_type=?').get(eventType);
}

function resolveUnitPrice(item, poId) {
  const { db } = ctx;
  if (item.unit_price && item.unit_price > 0) return item.unit_price;

  // po_items에서 단가 조회
  if (item.po_item_id) {
    const poItem = db.prepare('SELECT unit_price FROM po_items WHERE item_id=?').get(item.po_item_id);
    if (poItem && poItem.unit_price > 0) return poItem.unit_price;
  }

  // material_prices에서 최신 단가 조회
  if (item.product_code) {
    const mp = db.prepare('SELECT unit_price FROM material_prices WHERE product_code=? ORDER BY apply_month DESC LIMIT 1').get(item.product_code);
    if (mp && mp.unit_price > 0) return mp.unit_price;
  }

  // ctx.getLastVendorPrice 사용
  if (ctx.getLastVendorPrice && item.product_code && poId) {
    const po = db.prepare('SELECT vendor_name FROM po_header WHERE po_id=?').get(poId);
    if (po) {
      const price = ctx.getLastVendorPrice(po.vendor_name, item.product_code);
      if (price > 0) return price;
    }
  }

  return 0;
}

// 입고 자동 분개
function createReceiveJournal(poId, items, receivedBy) {
  init();
  const { db } = ctx;
  const config = getConfig('goods_receipt');
  if (!config || !config.enabled) return null;

  let totalAmount = 0;
  for (const it of items) {
    const qty = it.received_qty || 0;
    const price = resolveUnitPrice(it, poId);
    totalAmount += qty * price;
  }

  if (totalAmount <= 0) return null;

  const po = db.prepare('SELECT po_number FROM po_header WHERE po_id=?').get(poId);
  const poNumber = po ? po.po_number : `PO-${poId}`;
  const debitAcct = config.debit_account || ACCT.RAW_MATERIAL;
  const creditAcct = config.credit_account || ACCT.ACCOUNTS_PAYABLE;

  const result = db.prepare(`INSERT INTO journal_auto_log
    (event_type, ref_id, ref_number, debit_account, credit_account, amount, status, created_by)
    VALUES (?,?,?,?,?,?,?,?)`).run(
    'goods_receipt', poId, poNumber, debitAcct, creditAcct, totalAmount, 'created', receivedBy || 'system'
  );

  return {
    log_id: result.lastInsertRowid,
    event_type: 'goods_receipt',
    po_id: poId,
    po_number: poNumber,
    debit: debitAcct,
    credit: creditAcct,
    amount: totalAmount,
  };
}

// 출하 자동 분개
function createShipmentJournal(orderId, items) {
  init();
  const { db } = ctx;
  const config = getConfig('sales_shipment');
  if (!config || !config.enabled) return null;

  let totalAmount = 0;
  for (const it of items) {
    totalAmount += (it.qty || it.shipped_qty || 0) * (it.unit_price || it.price || 0);
  }
  if (totalAmount <= 0) return null;

  const debitAcct = config.debit_account || ACCT.ACCOUNTS_RECEIVABLE;
  const creditAcct = config.credit_account || ACCT.SALES_REVENUE;
  const refNumber = `SO-${orderId}`;

  const result = db.prepare(`INSERT INTO journal_auto_log
    (event_type, ref_id, ref_number, debit_account, credit_account, amount, status, created_by)
    VALUES (?,?,?,?,?,?,?,?)`).run(
    'sales_shipment', orderId, refNumber, debitAcct, creditAcct, totalAmount, 'created', 'system'
  );

  return {
    log_id: result.lastInsertRowid,
    event_type: 'sales_shipment',
    order_id: orderId,
    ref_number: refNumber,
    debit: debitAcct,
    credit: creditAcct,
    amount: totalAmount,
  };
}

// Hook 함수 (서버 receive 핸들러에서 호출)
function hookReceiveJournal(poId, items) {
  try {
    return createReceiveJournal(poId, items, 'auto-hook');
  } catch (e) {
    if (ctx.logError) ctx.logError('journal-auto', 'hookReceiveJournal failed', e);
    return null;
  }
}

function hookShipmentJournal(orderId, items) {
  try {
    return createShipmentJournal(orderId, items);
  } catch (e) {
    if (ctx.logError) ctx.logError('journal-auto', 'hookShipmentJournal failed', e);
    return null;
  }
}

// ── API Routes ──

// GET /api/journal/auto-entries
router.get('/api/journal/auto-entries', async (req, res) => {
  const { db, ok, fail, extractToken, verifyToken } = ctx;
  const token = extractToken(req);
  const decoded = token ? verifyToken(token) : null;
  if (!decoded) { fail(res, 401, '인증이 필요합니다.'); return; }
  init();

  const qs = new URL(req.url, 'http://localhost').searchParams;
  const eventType = qs.get('event_type') || '';
  const status = qs.get('status') || '';
  const offset = parseInt(qs.get('offset') || '0', 10);
  const limit = Math.min(parseInt(qs.get('limit') || '50', 10), 200);

  let where = '1=1';
  const params = [];
  if (eventType) { where += ' AND event_type=?'; params.push(eventType); }
  if (status) { where += ' AND status=?'; params.push(status); }

  const total = db.prepare(`SELECT COUNT(*) AS cnt FROM journal_auto_log WHERE ${where}`).get(...params).cnt;
  params.push(limit, offset);
  const entries = db.prepare(`SELECT * FROM journal_auto_log WHERE ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(...params);

  // 계정명 보강
  const accMap = {};
  try {
    db.prepare('SELECT acc_code, acc_name FROM gl_account_map').all().forEach(a => { accMap[a.acc_code] = a.acc_name; });
  } catch (_) {}

  const enriched = entries.map(e => ({
    ...e,
    debit_name: accMap[e.debit_account] || '',
    credit_name: accMap[e.credit_account] || '',
  }));

  ok(res, { entries: enriched, total, offset, limit });
});

// POST /api/journal/auto-entry — 수동 트리거
router.post('/api/journal/auto-entry', async (req, res) => {
  const { ok, fail, readJSON, extractToken, verifyToken } = ctx;
  const token = extractToken(req);
  const decoded = token ? verifyToken(token) : null;
  if (!decoded) { fail(res, 401, '인증이 필요합니다.'); return; }
  init();

  const body = await readJSON(req);
  if (!body.po_id) { fail(res, 400, 'po_id가 필요합니다.'); return; }

  const { db } = ctx;
  const poItems = db.prepare('SELECT item_id AS po_item_id, product_code, ordered_qty AS received_qty, unit_price FROM po_items WHERE po_id=?').all(body.po_id);
  if (!poItems.length) { fail(res, 404, '해당 발주의 품목을 찾을 수 없습니다.'); return; }

  const result = createReceiveJournal(body.po_id, poItems, decoded.username || decoded.name || 'manual');
  if (!result) { fail(res, 400, '분개 생성에 실패했습니다. 금액이 0이거나 설정이 비활성화되어 있습니다.'); return; }

  ok(res, { message: '자동 분개가 생성되었습니다.', journal: result });
});

// GET /api/journal/auto-config
router.get('/api/journal/auto-config', async (req, res) => {
  const { db, ok, fail, extractToken, verifyToken } = ctx;
  const token = extractToken(req);
  const decoded = token ? verifyToken(token) : null;
  if (!decoded) { fail(res, 401, '인증이 필요합니다.'); return; }
  init();

  const configs = db.prepare('SELECT * FROM journal_auto_config ORDER BY event_type').all();

  const accMap = {};
  try {
    db.prepare('SELECT acc_code, acc_name FROM gl_account_map').all().forEach(a => { accMap[a.acc_code] = a.acc_name; });
  } catch (_) {}

  const enriched = configs.map(c => ({
    ...c,
    debit_name: accMap[c.debit_account] || '',
    credit_name: accMap[c.credit_account] || '',
  }));

  ok(res, { configs: enriched });
});

// POST /api/journal/auto-config — 설정 저장
router.post('/api/journal/auto-config', async (req, res) => {
  const { db, ok, fail, readJSON, extractToken, verifyToken } = ctx;
  const token = extractToken(req);
  const decoded = token ? verifyToken(token) : null;
  if (!decoded) { fail(res, 401, '인증이 필요합니다.'); return; }
  init();

  const body = await readJSON(req);
  if (!body.event_type) { fail(res, 400, 'event_type이 필요합니다.'); return; }

  const existing = db.prepare('SELECT id FROM journal_auto_config WHERE event_type=?').get(body.event_type);
  if (existing) {
    const sets = [];
    const params = [];
    if (body.enabled !== undefined) { sets.push('enabled=?'); params.push(body.enabled ? 1 : 0); }
    if (body.debit_account) { sets.push('debit_account=?'); params.push(body.debit_account); }
    if (body.credit_account) { sets.push('credit_account=?'); params.push(body.credit_account); }
    if (body.description !== undefined) { sets.push('description=?'); params.push(body.description); }
    if (sets.length > 0) {
      sets.push("updated_at=datetime('now','localtime')");
      params.push(existing.id);
      db.prepare(`UPDATE journal_auto_config SET ${sets.join(',')} WHERE id=?`).run(...params);
    }
  } else {
    db.prepare(`INSERT INTO journal_auto_config (event_type, enabled, debit_account, credit_account, description)
      VALUES (?,?,?,?,?)`).run(
      body.event_type,
      body.enabled !== undefined ? (body.enabled ? 1 : 0) : 1,
      body.debit_account || '',
      body.credit_account || '',
      body.description || ''
    );
  }

  ok(res, { message: '자동 분개 설정이 저장되었습니다.' });
});

module.exports = { router, hookReceiveJournal, hookShipmentJournal };
