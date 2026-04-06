// routes/barcode.js — 바코드/QR코드 생성 및 스캔 모듈
const Router = require('./_router');
const ctx = require('./_ctx');
const router = new Router();

/* ────────────────────────────────────────────
   Code128B 인코딩 테이블 (순수 JS 구현)
   값 0~106 → 6개 바 패턴 (각 패턴은 bar/space 폭의 배열)
   ──────────────────────────────────────────── */
const CODE128_PATTERNS = [
  [2,1,2,2,2,2],[2,2,2,1,2,2],[2,2,2,2,2,1],[1,2,1,2,2,3],[1,2,1,3,2,2],
  [1,3,1,2,2,2],[1,2,2,2,1,3],[1,2,2,3,1,2],[1,3,2,2,1,2],[2,2,1,2,1,3],
  [2,2,1,3,1,2],[2,3,1,2,1,2],[1,1,2,2,3,2],[1,2,2,1,3,2],[1,2,2,2,3,1],
  [1,1,3,2,2,2],[1,2,3,1,2,2],[1,2,3,2,2,1],[2,2,3,2,1,1],[2,2,1,1,3,2],
  [2,2,1,2,3,1],[2,1,3,2,1,2],[2,2,3,1,1,2],[3,1,2,1,3,1],[3,1,1,2,2,2],
  [3,2,1,1,2,2],[3,2,1,2,2,1],[3,1,2,2,1,2],[3,2,2,1,1,2],[3,2,2,2,1,1],
  [2,1,2,1,2,3],[2,1,2,3,2,1],[2,3,2,1,2,1],[1,1,1,3,2,3],[1,3,1,1,2,3],
  [1,3,1,3,2,1],[1,1,2,3,1,3],[1,3,2,1,1,3],[1,3,2,3,1,1],[2,1,1,3,1,3],
  [2,3,1,1,1,3],[2,3,1,3,1,1],[1,1,2,1,3,3],[1,1,2,3,3,1],[1,3,2,1,3,1],
  [1,1,3,1,2,3],[1,1,3,3,2,1],[1,3,3,1,2,1],[3,1,3,1,2,1],[2,1,1,3,3,1],
  [2,3,1,1,3,1],[2,1,3,1,1,3],[2,1,3,3,1,1],[2,1,3,1,3,1],[3,1,1,1,2,3],
  [3,1,1,3,2,1],[3,3,1,1,2,1],[3,1,2,1,1,3],[3,1,2,3,1,1],[3,3,2,1,1,1],
  [3,1,4,1,1,1],[2,2,1,4,1,1],[4,3,1,1,1,1],[1,1,1,2,2,4],[1,1,1,4,2,2],
  [1,2,1,1,2,4],[1,2,1,4,2,1],[1,4,1,1,2,2],[1,4,1,2,2,1],[1,1,2,2,1,4],
  [1,1,2,4,1,2],[1,2,2,1,1,4],[1,2,2,4,1,1],[1,4,2,1,1,2],[1,4,2,2,1,1],
  [2,4,1,2,1,1],[2,2,1,1,1,4],[4,1,3,1,1,1],[2,4,1,1,1,2],[1,3,4,1,1,1],
  [1,1,1,2,4,2],[1,2,1,1,4,2],[1,2,1,2,4,1],[1,1,4,2,1,2],[1,2,4,1,1,2],
  [1,2,4,2,1,1],[4,1,1,2,1,2],[4,2,1,1,1,2],[4,2,1,2,1,1],[2,1,2,1,4,1],
  [2,1,4,1,2,1],[4,1,2,1,2,1],[1,1,1,1,4,3],[1,1,1,3,4,1],[1,3,1,1,4,1],
  [1,1,4,1,1,3],[1,1,4,3,1,1],[4,1,1,1,1,3],[4,1,1,3,1,1],[1,1,3,1,4,1],
  [1,1,4,1,3,1],[3,1,1,1,4,1],[4,1,1,1,3,1],[2,1,1,4,1,2],[2,1,1,2,1,4],
  [2,1,1,2,3,2],[2,3,3,1,1,1],[2,1,1,1,3,2],
];
// STOP pattern has 7 bars: [2,3,3,1,1,1,2]
const STOP_PATTERN = [2,3,3,1,1,1,2];
const START_B = 104;
const STOP_CODE = 106;

/**
 * Code128B 인코딩: text → 값 배열 (START + data + checksum + STOP)
 */
function encodeCode128B(text) {
  const values = [START_B];
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if (code < 32 || code > 127) {
      // Code128B는 ASCII 32-127만 지원, 범위 밖이면 '?' 대체
      values.push(31); // '?' = 63 - 32 = 31
    } else {
      values.push(code - 32);
    }
  }
  // 체크섬 계산
  let checksum = values[0]; // START_B = 104
  for (let i = 1; i < values.length; i++) {
    checksum += i * values[i];
  }
  checksum = checksum % 103;
  values.push(checksum);
  values.push(STOP_CODE);
  return values;
}

/**
 * 값 배열 → 바 너비 배열 (SVG 렌더링용)
 */
function valuesToBars(values) {
  const bars = [];
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (v === STOP_CODE) {
      // STOP은 7개 요소
      for (const w of STOP_PATTERN) bars.push(w);
    } else {
      const pattern = CODE128_PATTERNS[v];
      if (pattern) {
        for (const w of pattern) bars.push(w);
      }
    }
  }
  return bars;
}

/**
 * Code128 SVG 생성 (순수 JS)
 * @param {string} text - 인코딩할 텍스트
 * @param {object} options - { width: 모듈 폭(px), height: 바코드 높이, fontSize: 텍스트 크기 }
 * @returns {string} SVG 문자열
 */
function generateCode128SVG(text, options = {}) {
  const { width = 2, height = 80, fontSize = 14, showText = true } = options;
  const values = encodeCode128B(text);
  const bars = valuesToBars(values);

  // 조용한 영역(quiet zone) = 10 모듈
  const quietZone = 10 * width;
  let totalModules = 0;
  for (const w of bars) totalModules += w;
  const barcodeWidth = totalModules * width;
  const svgWidth = barcodeWidth + quietZone * 2;
  const textHeight = showText ? fontSize + 8 : 0;
  const svgHeight = height + textHeight + 4;

  let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${svgWidth}" height="${svgHeight}" viewBox="0 0 ${svgWidth} ${svgHeight}">`;
  svg += `<rect width="100%" height="100%" fill="white"/>`;

  let x = quietZone;
  for (let i = 0; i < bars.length; i++) {
    const barWidth = bars[i] * width;
    // 짝수 인덱스 = 바(검정), 홀수 인덱스 = 공백(흰색)
    if (i % 2 === 0) {
      svg += `<rect x="${x}" y="2" width="${barWidth}" height="${height}" fill="black"/>`;
    }
    x += barWidth;
  }

  if (showText) {
    const textX = svgWidth / 2;
    const textY = height + fontSize + 4;
    svg += `<text x="${textX}" y="${textY}" text-anchor="middle" font-family="monospace" font-size="${fontSize}" fill="black">${escapeXml(text)}</text>`;
  }

  svg += '</svg>';
  return svg;
}

function escapeXml(str) {
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

/* ────────────────────────────────────────────
   바코드 값 생성 (ref_type + ref_id 기반)
   ──────────────────────────────────────────── */
function generateBarcodeValue(refType, refId) {
  const prefixes = {
    product: 'P',
    po: 'O',
    lot: 'L',
    location: 'W',
  };
  const prefix = prefixes[refType] || 'X';
  // 깔끔한 바코드 값: 접두어 + 참조 ID (특수문자 제거)
  const cleanId = String(refId).replace(/[^A-Za-z0-9-]/g, '');
  return `${prefix}-${cleanId}`;
}

/* ────────────────────────────────────────────
   ref_name 조회 헬퍼
   ──────────────────────────────────────────── */
function lookupRefName(refType, refId) {
  const { db } = ctx;
  try {
    if (refType === 'product') {
      const row = db.prepare('SELECT product_name FROM products WHERE product_code = ?').get(refId);
      return row ? row.product_name : refId;
    }
    if (refType === 'po') {
      const row = db.prepare('SELECT supplier_name FROM po_header WHERE po_number = ?').get(refId);
      return row ? row.supplier_name : refId;
    }
    if (refType === 'lot') {
      const row = db.prepare('SELECT product_name FROM batch_master WHERE lot_number = ?').get(refId);
      return row ? row.product_name : refId;
    }
    if (refType === 'location') {
      return refId; // 로케이션은 코드 자체가 이름
    }
  } catch (_) { /* 테이블이 없을 수 있음 */ }
  return refId;
}

/* ────────────────────────────────────────────
   ERP 페이지 URL 매핑
   ──────────────────────────────────────────── */
function erpPageUrl(refType, refId) {
  const pages = {
    product: `/inventory?product_code=${encodeURIComponent(refId)}`,
    po: `/purchase?po_number=${encodeURIComponent(refId)}`,
    lot: `/inventory?lot=${encodeURIComponent(refId)}`,
    location: `/inventory?location=${encodeURIComponent(refId)}`,
  };
  return pages[refType] || '/';
}

/* ────────────────────────────────────────────
   테이블 초기화
   ──────────────────────────────────────────── */
function initTables() {
  const { db } = ctx;
  if (!db) return;
  db.exec(`CREATE TABLE IF NOT EXISTS barcode_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    barcode_value TEXT UNIQUE NOT NULL,
    barcode_type TEXT DEFAULT 'CODE128',
    ref_type TEXT NOT NULL,
    ref_id TEXT NOT NULL,
    ref_name TEXT DEFAULT '',
    metadata TEXT DEFAULT '{}',
    created_by TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime'))
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_bc_ref ON barcode_registry(ref_type, ref_id)`);
}

/* ────────────────────────────────────────────
   인증 헬퍼
   ──────────────────────────────────────────── */
function auth(req, res) {
  const token = ctx.extractToken(req);
  const decoded = token ? ctx.verifyToken(token) : null;
  if (!decoded) { ctx.fail(res, 401, '인증이 필요합니다'); return null; }
  return decoded;
}

/* ────────────────────────────────────────────
   1. GET /api/barcode/generate — Code128 바코드 SVG 생성
   ?type=product&id=PROD001&format=svg
   ──────────────────────────────────────────── */
router.get('/api/barcode/generate', async (req, res, parsed) => {
  const decoded = auth(req, res);
  if (!decoded) return;

  const qs = parsed.searchParams;
  const refType = qs.get('type');
  const refId = qs.get('id');
  const format = qs.get('format') || 'svg';

  if (!refType || !refId) {
    ctx.fail(res, 400, 'type과 id 파라미터가 필요합니다');
    return;
  }

  try {
    const barcodeValue = generateBarcodeValue(refType, refId);
    const refName = lookupRefName(refType, refId);

    // SVG 생성
    const svg = generateCode128SVG(barcodeValue, {
      width: 2,
      height: 80,
      fontSize: 14,
      showText: true,
    });

    // barcode_registry에 자동 등록 (이미 있으면 무시)
    try {
      ctx.db.prepare(`
        INSERT OR IGNORE INTO barcode_registry (barcode_value, barcode_type, ref_type, ref_id, ref_name, created_by)
        VALUES (?, 'CODE128', ?, ?, ?, ?)
      `).run(barcodeValue, refType, refId, refName, decoded.name || decoded.email || '');
    } catch (_) { /* 중복 무시 */ }

    ctx.ok(res, {
      barcode_value: barcodeValue,
      svg,
      ref_type: refType,
      ref_id: refId,
      ref_name: refName,
    });
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

/* ────────────────────────────────────────────
   2. GET /api/barcode/generate-qr — QR 데이터 생성
   ?type=po&id=PO-20260405-001
   (클라이언트가 qrcode.js로 렌더링)
   ──────────────────────────────────────────── */
router.get('/api/barcode/generate-qr', async (req, res, parsed) => {
  const decoded = auth(req, res);
  if (!decoded) return;

  const qs = parsed.searchParams;
  const refType = qs.get('type');
  const refId = qs.get('id');

  if (!refType || !refId) {
    ctx.fail(res, 400, 'type과 id 파라미터가 필요합니다');
    return;
  }

  try {
    const refName = lookupRefName(refType, refId);
    const url = erpPageUrl(refType, refId);
    const barcodeValue = generateBarcodeValue(refType, refId);

    // QR 데이터: JSON 문자열로 인코딩
    const qrData = JSON.stringify({
      type: refType,
      id: refId,
      name: refName,
      barcode: barcodeValue,
      url,
    });

    // barcode_registry에 QR 타입으로 등록
    try {
      ctx.db.prepare(`
        INSERT OR IGNORE INTO barcode_registry (barcode_value, barcode_type, ref_type, ref_id, ref_name, created_by)
        VALUES (?, 'QR', ?, ?, ?, ?)
      `).run(barcodeValue, refType, refId, refName, decoded.name || decoded.email || '');
    } catch (_) { /* 중복 무시 */ }

    ctx.ok(res, {
      qr_data: qrData,
      ref_type: refType,
      ref_id: refId,
      ref_name: refName,
      url,
    });
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

/* ────────────────────────────────────────────
   3. POST /api/barcode/scan — 바코드 스캔 처리
   Body: { barcode_value }
   ──────────────────────────────────────────── */
router.post('/api/barcode/scan', async (req, res) => {
  const decoded = auth(req, res);
  if (!decoded) return;

  try {
    const body = await ctx.readJSON(req);
    const { barcode_value } = body;

    if (!barcode_value) {
      ctx.fail(res, 400, 'barcode_value가 필요합니다');
      return;
    }

    const { db } = ctx;

    // 1) barcode_registry에서 검색
    const registered = db.prepare(
      'SELECT * FROM barcode_registry WHERE barcode_value = ?'
    ).get(barcode_value);

    if (registered) {
      // 등록된 바코드 → 상세 정보 조회
      const details = lookupDetails(registered.ref_type, registered.ref_id);
      ctx.ok(res, {
        found: true,
        source: 'registry',
        ref_type: registered.ref_type,
        ref_id: registered.ref_id,
        ref_name: registered.ref_name,
        details,
      });
      return;
    }

    // 2) 등록되지 않은 경우 → 제품코드, PO번호, LOT번호 매칭 시도
    const searchValue = barcode_value.trim();

    // 제품 테이블
    try {
      const product = db.prepare(
        'SELECT product_code, product_name FROM products WHERE product_code = ?'
      ).get(searchValue);
      if (product) {
        ctx.ok(res, {
          found: true,
          source: 'products',
          ref_type: 'product',
          ref_id: product.product_code,
          ref_name: product.product_name,
          details: product,
        });
        return;
      }
    } catch (_) { /* 테이블 없을 수 있음 */ }

    // PO 헤더
    try {
      const po = db.prepare(
        'SELECT po_number, supplier_name, status, order_date FROM po_header WHERE po_number = ?'
      ).get(searchValue);
      if (po) {
        ctx.ok(res, {
          found: true,
          source: 'po_header',
          ref_type: 'po',
          ref_id: po.po_number,
          ref_name: po.supplier_name,
          details: po,
        });
        return;
      }
    } catch (_) { /* 테이블 없을 수 있음 */ }

    // 배치/LOT
    try {
      const lot = db.prepare(
        'SELECT lot_number, product_code, product_name, qty FROM batch_master WHERE lot_number = ?'
      ).get(searchValue);
      if (lot) {
        ctx.ok(res, {
          found: true,
          source: 'batch_master',
          ref_type: 'lot',
          ref_id: lot.lot_number,
          ref_name: lot.product_name,
          details: lot,
        });
        return;
      }
    } catch (_) { /* 테이블 없을 수 있음 */ }

    // 못 찾음
    ctx.ok(res, {
      found: false,
      ref_type: null,
      ref_id: null,
      ref_name: null,
      details: null,
      message: '등록되지 않은 바코드입니다',
    });
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

/**
 * ref_type + ref_id로 상세 정보 조회
 */
function lookupDetails(refType, refId) {
  const { db } = ctx;
  try {
    if (refType === 'product') {
      return db.prepare('SELECT * FROM products WHERE product_code = ?').get(refId) || {};
    }
    if (refType === 'po') {
      return db.prepare('SELECT * FROM po_header WHERE po_number = ?').get(refId) || {};
    }
    if (refType === 'lot') {
      return db.prepare('SELECT * FROM batch_master WHERE lot_number = ?').get(refId) || {};
    }
  } catch (_) { /* 테이블 없을 수 있음 */ }
  return {};
}

/* ────────────────────────────────────────────
   4. POST /api/barcode/quick-receive — 바코드 빠른 입고
   Body: { barcode_value, qty, received_by }
   ──────────────────────────────────────────── */
router.post('/api/barcode/quick-receive', async (req, res) => {
  const decoded = auth(req, res);
  if (!decoded) return;

  try {
    const body = await ctx.readJSON(req);
    const { barcode_value, qty, received_by } = body;

    if (!barcode_value || !qty) {
      ctx.fail(res, 400, 'barcode_value와 qty가 필요합니다');
      return;
    }

    const { db } = ctx;

    // 바코드 → PO 찾기
    const registered = db.prepare(
      'SELECT * FROM barcode_registry WHERE barcode_value = ?'
    ).get(barcode_value);

    let poNumber = null;
    let refType = null;

    if (registered) {
      refType = registered.ref_type;
      if (refType === 'po') {
        poNumber = registered.ref_id;
      } else if (refType === 'product') {
        // 제품 바코드 → 해당 제품의 미완료 PO 검색
        try {
          const openPo = db.prepare(`
            SELECT h.po_number FROM po_header h
            JOIN po_items i ON h.po_number = i.po_number
            WHERE i.product_code = ? AND h.status IN ('approved','ordered','partial')
            ORDER BY h.order_date DESC LIMIT 1
          `).get(registered.ref_id);
          if (openPo) poNumber = openPo.po_number;
        } catch (_) { /* 테이블 없을 수 있음 */ }
      }
    } else {
      // 미등록 바코드 → PO 번호로 직접 매칭 시도
      try {
        const po = db.prepare('SELECT po_number FROM po_header WHERE po_number = ?').get(barcode_value);
        if (po) poNumber = po.po_number;
      } catch (_) {}
    }

    if (!poNumber) {
      ctx.fail(res, 404, '해당 바코드에 대한 발주를 찾을 수 없습니다');
      return;
    }

    // PO 입고 처리
    const receiver = received_by || decoded.name || decoded.email || '';
    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');

    // po_header 상태 업데이트
    try {
      db.prepare(`
        UPDATE po_header SET status = 'received', received_date = ?, received_by = ?
        WHERE po_number = ? AND status IN ('approved','ordered','partial')
      `).run(now, receiver, poNumber);
    } catch (_) { /* 컬럼 없을 수 있음 - 무시 */ }

    // po_receive 테이블에 입고 기록 (있는 경우)
    try {
      db.prepare(`
        INSERT INTO po_receive (po_number, received_qty, received_by, received_date)
        VALUES (?, ?, ?, ?)
      `).run(poNumber, qty, receiver, now);
    } catch (_) { /* 테이블 없을 수 있음 */ }

    // 감사 로그
    if (ctx.auditLog) {
      ctx.auditLog(decoded.name || decoded.email, 'BARCODE_QUICK_RECEIVE', `PO=${poNumber}, qty=${qty}`);
    }

    ctx.ok(res, {
      po_number: poNumber,
      qty: Number(qty),
      received_by: receiver,
      received_at: now,
      message: `발주 ${poNumber} 입고 처리 완료 (수량: ${qty})`,
    });
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

/* ────────────────────────────────────────────
   5. GET /api/barcode/registry — 등록된 바코드 목록
   ?type=product&search=&limit=50&offset=0
   ──────────────────────────────────────────── */
router.get('/api/barcode/registry', async (req, res, parsed) => {
  if (!auth(req, res)) return;

  const qs = parsed.searchParams;
  const refType = qs.get('type') || '';
  const search = qs.get('search') || '';
  const limit = parseInt(qs.get('limit') || '50', 10);
  const offset = parseInt(qs.get('offset') || '0', 10);

  try {
    const { db } = ctx;
    let where = '1=1';
    const params = [];

    if (refType) {
      where += ' AND ref_type = ?';
      params.push(refType);
    }
    if (search) {
      where += ' AND (barcode_value LIKE ? OR ref_id LIKE ? OR ref_name LIKE ?)';
      const like = `%${search}%`;
      params.push(like, like, like);
    }

    const countRow = db.prepare(`SELECT COUNT(*) AS cnt FROM barcode_registry WHERE ${where}`).get(...params);
    const total = countRow ? countRow.cnt : 0;

    const items = db.prepare(
      `SELECT * FROM barcode_registry WHERE ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`
    ).all(...params, limit, offset);

    ctx.ok(res, { items, total });
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

/* ────────────────────────────────────────────
   6. POST /api/barcode/register — 바코드 수동 등록
   Body: { barcode_value, barcode_type, ref_type, ref_id, ref_name }
   ──────────────────────────────────────────── */
router.post('/api/barcode/register', async (req, res) => {
  const decoded = auth(req, res);
  if (!decoded) return;

  try {
    const body = await ctx.readJSON(req);
    const { barcode_value, barcode_type, ref_type, ref_id, ref_name } = body;

    if (!barcode_value || !ref_type || !ref_id) {
      ctx.fail(res, 400, 'barcode_value, ref_type, ref_id는 필수입니다');
      return;
    }

    const { db } = ctx;

    // 중복 확인
    const existing = db.prepare('SELECT id FROM barcode_registry WHERE barcode_value = ?').get(barcode_value);
    if (existing) {
      ctx.fail(res, 409, '이미 등록된 바코드입니다');
      return;
    }

    const info = db.prepare(`
      INSERT INTO barcode_registry (barcode_value, barcode_type, ref_type, ref_id, ref_name, created_by)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      barcode_value,
      barcode_type || 'CODE128',
      ref_type,
      ref_id,
      ref_name || '',
      decoded.name || decoded.email || ''
    );

    ctx.ok(res, {
      id: info.lastInsertRowid,
      barcode_value,
      message: '바코드가 등록되었습니다',
    });
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

/* ────────────────────────────────────────────
   7. GET /api/barcode/print/:id — 인쇄용 바코드 HTML
   ──────────────────────────────────────────── */
router.getP(/^\/api\/barcode\/print\/(\d+)$/, async (req, res, parsed, match) => {
  if (!auth(req, res)) return;

  const id = parseInt(match[1], 10);

  try {
    const { db } = ctx;
    const row = db.prepare('SELECT * FROM barcode_registry WHERE id = ?').get(id);

    if (!row) {
      ctx.fail(res, 404, '바코드를 찾을 수 없습니다');
      return;
    }

    const svg = generateCode128SVG(row.barcode_value, {
      width: 3,
      height: 100,
      fontSize: 16,
      showText: true,
    });

    const html = `<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <title>바코드 인쇄 - ${escapeXml(row.barcode_value)}</title>
  <style>
    @media print {
      body { margin: 0; padding: 0; }
      .no-print { display: none; }
    }
    body {
      font-family: 'Malgun Gothic', sans-serif;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      margin: 0;
      background: #fff;
    }
    .barcode-label {
      border: 1px dashed #ccc;
      padding: 20px 30px;
      text-align: center;
      background: #fff;
    }
    .barcode-label svg {
      display: block;
      margin: 0 auto 10px;
    }
    .ref-info {
      font-size: 13px;
      color: #555;
      margin-top: 8px;
    }
    .ref-info strong {
      color: #222;
    }
    .ref-name {
      font-size: 16px;
      font-weight: bold;
      margin-top: 4px;
      color: #000;
    }
    .print-btn {
      margin-top: 20px;
      padding: 10px 30px;
      font-size: 14px;
      cursor: pointer;
      background: #4A90D9;
      color: #fff;
      border: none;
      border-radius: 4px;
    }
    .print-btn:hover { background: #357ABD; }
  </style>
</head>
<body>
  <div class="barcode-label">
    ${svg}
    <div class="ref-name">${escapeXml(row.ref_name || row.ref_id)}</div>
    <div class="ref-info">
      <strong>${escapeXml(row.ref_type.toUpperCase())}</strong> | ${escapeXml(row.ref_id)}
    </div>
  </div>
  <button class="print-btn no-print" onclick="window.print()">인쇄</button>
</body>
</html>`;

    // HTML 직접 응답
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  } catch (e) {
    ctx.fail(res, 500, e.message);
  }
});

module.exports = { router, initTables };
