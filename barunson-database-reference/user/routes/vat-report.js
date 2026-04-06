// routes/vat-report.js — 부가가치세 신고 모듈 (법인별 분리: 바른손 / DD)
const Router = require('./_router');
const ctx = require('./_ctx');
const router = new Router();

/* ────────────────────────────────────────────
   VAT 기간 매핑
   ──────────────────────────────────────────── */
const PERIOD_MAP = {
  '1q1': { label: '1기예정', startMM: '01', startDD: '01', endMM: '03', endDD: '31' },
  '1q2': { label: '1기확정', startMM: '04', startDD: '01', endMM: '06', endDD: '30' },
  '2q1': { label: '2기예정', startMM: '07', startDD: '01', endMM: '09', endDD: '30' },
  '2q2': { label: '2기확정', startMM: '10', startDD: '01', endMM: '12', endDD: '31' },
};

function periodDates(year, period) {
  const p = PERIOD_MAP[period];
  if (!p) return null;
  return {
    label: p.label,
    start: `${year}${p.startMM}${p.startDD}`,
    end:   `${year}${p.endMM}${p.endDD}`,
    startDash: `${year}-${p.startMM}-${p.startDD}`,
    endDash:   `${year}-${p.endMM}-${p.endDD}`,
  };
}

/* ────────────────────────────────────────────
   테이블 초기화
   ──────────────────────────────────────────── */
function initTables() {
  const { db } = ctx;
  if (!db) return;
  db.exec(`CREATE TABLE IF NOT EXISTS vat_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company TEXT DEFAULT 'barunson',
    report_type TEXT NOT NULL,
    period_type TEXT NOT NULL,
    year TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    sales_total REAL DEFAULT 0,
    sales_tax REAL DEFAULT 0,
    purchase_total REAL DEFAULT 0,
    purchase_tax REAL DEFAULT 0,
    vat_payable REAL DEFAULT 0,
    status TEXT DEFAULT 'draft',
    created_by TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime')),
    updated_at TEXT DEFAULT (datetime('now','localtime'))
  )`);
  // company 컬럼 추가 (기존 테이블 마이그레이션)
  try { db.exec("ALTER TABLE vat_reports ADD COLUMN company TEXT DEFAULT 'barunson'"); } catch(_) {}
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
   바른손(XERP) 세금계산서 집계
   ──────────────────────────────────────────── */
async function aggregateXerp(startDate, endDate) {
  const result = { sales: { count: 0, supply: 0, tax: 0 }, purchase: { count: 0, supply: 0, tax: 0 }, ok: false };
  try {
    const pool = await ctx.ensureXerpPool();
    const r = await pool.request()
      .input('from', startDate)
      .input('to', endDate)
      .query(`
        SELECT h.ArApGubun,
               COUNT(*) AS cnt,
               ISNULL(SUM(h.SupplyAmnt),0) AS supply,
               ISNULL(SUM(h.VatAmnt),0) AS vat
        FROM rpInvoiceHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10'
          AND h.InvoiceDate >= @from AND h.InvoiceDate <= @to
        GROUP BY h.ArApGubun
      `);
    for (const row of r.recordset) {
      const key = (row.ArApGubun || '').trim() === 'AR' ? 'sales' : 'purchase';
      result[key].count = row.cnt;
      result[key].supply = row.supply;
      result[key].tax = row.vat;
    }
    result.ok = true;
  } catch (_) {}
  return result;
}

/* ────────────────────────────────────────────
   DD(뚜비뚜비) 매출 집계 — orders 기반
   결제완료(P,F) 주문의 total_money에서 부가세 역산
   ──────────────────────────────────────────── */
async function aggregateDD(startDash, endDash) {
  const result = { sales: { count: 0, supply: 0, tax: 0 }, purchase: { count: 0, supply: 0, tax: 0 }, ok: false };
  try {
    const pool = ctx.getDdPool ? ctx.getDdPool() : null;
    if (!pool) return result;
    const [rows] = await pool.query(`
      SELECT COUNT(*) AS cnt,
             COALESCE(SUM(paid_money),0) AS total_paid
      FROM orders
      WHERE order_state IN ('P','F','D')
        AND pay_date >= ? AND pay_date <= ?
    `, [startDash.replace(/-/g,''), endDash.replace(/-/g,'')]);
    if (rows && rows[0]) {
      const totalPaid = rows[0].total_paid || 0;
      // 부가세 포함가 → 공급가액 = total / 1.1, 세액 = total - supply
      const supply = Math.round(totalPaid / 1.1);
      const tax = totalPaid - supply;
      result.sales.count = rows[0].cnt || 0;
      result.sales.supply = supply;
      result.sales.tax = tax;
    }
    result.ok = true;
  } catch (_) {}
  return result;
}

/* ────────────────────────────────────────────
   홈택스 업로드 세금계산서 집계 (법인 구분)
   ──────────────────────────────────────────── */
function aggregateHometax(startDate, endDate, company) {
  const { db } = ctx;
  const result = { sales: { count: 0, supply: 0, tax: 0 }, purchase: { count: 0, supply: 0, tax: 0 } };
  try {
    // company 컬럼이 있으면 필터, 없으면 전체
    let whereCompany = '';
    const params = [startDate, endDate];
    try {
      db.prepare('SELECT company FROM hometax_invoices LIMIT 1').get();
      if (company) { whereCompany = ' AND company = ?'; params.push(company); }
    } catch(_) {}

    const rows = db.prepare(`
      SELECT ar_ap,
             COUNT(*) AS cnt,
             COALESCE(SUM(supply_amt),0) AS supply,
             COALESCE(SUM(vat_amt),0) AS tax
      FROM hometax_invoices
      WHERE invoice_date >= ? AND invoice_date <= ?${whereCompany}
      GROUP BY ar_ap
    `).all(...params);
    for (const row of rows) {
      const key = (row.ar_ap || '').trim() === 'AR' ? 'sales' : 'purchase';
      result[key].count += row.cnt;
      result[key].supply += row.supply;
      result[key].tax += row.tax;
    }
  } catch (_) {}
  return result;
}

/* ────────────────────────────────────────────
   XERP 거래처별 집계
   ──────────────────────────────────────────── */
async function vendorListXerp(startDate, endDate, arAp) {
  const list = [];
  try {
    const pool = await ctx.ensureXerpPool();
    const r = await pool.request()
      .input('from', startDate)
      .input('to', endDate)
      .input('arAp', arAp)
      .query(`
        SELECT RTRIM(h.CsCode) AS cs_code, RTRIM(h.CsRegNo) AS cs_reg_no,
               COUNT(*) AS cnt,
               ISNULL(SUM(h.SupplyAmnt),0) AS supply,
               ISNULL(SUM(h.VatAmnt),0) AS vat
        FROM rpInvoiceHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10'
          AND h.InvoiceDate >= @from AND h.InvoiceDate <= @to
          AND h.ArApGubun = @arAp
        GROUP BY RTRIM(h.CsCode), RTRIM(h.CsRegNo)
        ORDER BY supply DESC
      `);
    list.push(...r.recordset);
  } catch (_) {}
  return list;
}

/* ────────────────────────────────────────────
   DD 거래처별 매출 집계 (제휴점 기준)
   ──────────────────────────────────────────── */
async function vendorListDD(startDash, endDash) {
  const list = [];
  try {
    const pool = ctx.getDdPool ? ctx.getDdPool() : null;
    if (!pool) return list;
    const [rows] = await pool.query(`
      SELECT COALESCE(ps.name, '직접주문') AS cs_name,
             '' AS cs_reg_no,
             COUNT(*) AS cnt,
             COALESCE(SUM(o.paid_money),0) AS total_paid
      FROM orders o
      LEFT JOIN partner_shops ps ON o.partner_shop_id = ps.id
      WHERE o.order_state IN ('P','F','D')
        AND o.pay_date >= ? AND o.pay_date <= ?
      GROUP BY COALESCE(ps.name, '직접주문')
      ORDER BY total_paid DESC
    `, [startDash.replace(/-/g,''), endDash.replace(/-/g,'')]);
    for (const r of rows) {
      const supply = Math.round((r.total_paid || 0) / 1.1);
      list.push({ cs_name: r.cs_name, cs_reg_no: r.cs_reg_no || '', cnt: r.cnt, supply, vat: (r.total_paid || 0) - supply });
    }
  } catch (_) {}
  return list;
}

function vendorListHometax(startDate, endDate, arAp, company) {
  const { db } = ctx;
  try {
    let whereCompany = '';
    const params = [startDate, endDate, arAp];
    try {
      db.prepare('SELECT company FROM hometax_invoices LIMIT 1').get();
      if (company) { whereCompany = ' AND company = ?'; params.push(company); }
    } catch(_) {}
    return db.prepare(`
      SELECT cs_name, cs_reg_no,
             COUNT(*) AS cnt,
             COALESCE(SUM(supply_amt),0) AS supply,
             COALESCE(SUM(vat_amt),0) AS vat
      FROM hometax_invoices
      WHERE invoice_date >= ? AND invoice_date <= ? AND ar_ap = ?${whereCompany}
      GROUP BY cs_name, cs_reg_no
      ORDER BY supply DESC
    `).all(...params);
  } catch (_) { return []; }
}

/* ────────────────────────────────────────────
   법인별 집계 통합 함수
   ──────────────────────────────────────────── */
async function getCompanyData(company, pd) {
  if (company === 'dd') {
    const dd = await aggregateDD(pd.startDash, pd.endDash);
    const hometax = aggregateHometax(pd.start, pd.end, 'dd');
    // DD는 B2C라 매입 세금계산서는 홈택스에서만
    const sales = dd.ok ? dd.sales : hometax.sales;
    const purchase = hometax.purchase; // DD 매입은 홈택스에서
    return { sales, purchase, sources: { dd: dd.ok ? 'ok' : 'unavailable', hometax: 'ok' } };
  } else {
    // barunson (XERP)
    const xerp = await aggregateXerp(pd.start, pd.end);
    const hometax = aggregateHometax(pd.start, pd.end, 'barunson');
    const sales = xerp.ok ? xerp.sales : hometax.sales;
    const purchase = xerp.ok ? xerp.purchase : hometax.purchase;
    return { sales, purchase, sources: { xerp: xerp.ok ? 'ok' : 'unavailable', hometax: 'ok' } };
  }
}

/* ════════════════════════════════════════════
   API 라우트
   ════════════════════════════════════════════ */

// 1. GET /api/vat/report — 기간별 부가세 신고서 (법인별)
router.get('/api/vat/report', async (req, res, parsed) => {
  if (!auth(req, res)) return;
  const qs = parsed.searchParams;
  const year = qs.get('year') || new Date().getFullYear().toString();
  const period = qs.get('period') || '1q1';
  const company = qs.get('company') || 'barunson'; // barunson | dd
  const pd = periodDates(year, period);
  if (!pd) { ctx.fail(res, 400, '유효하지 않은 기간입니다 (1q1/1q2/2q1/2q2)'); return; }

  const { sales, purchase, sources } = await getCompanyData(company, pd);
  const vatPayable = sales.tax - purchase.tax;
  const companyLabel = company === 'dd' ? '뚜비뚜비(DD)' : '바른손';

  ctx.ok(res, {
    year, period, company, company_label: companyLabel,
    period_label: pd.label,
    start_date: pd.start,
    end_date: pd.end,
    sales: { count: sales.count, supply_amount: sales.supply, tax_amount: sales.tax },
    purchase: { count: purchase.count, supply_amount: purchase.supply, tax_amount: purchase.tax },
    vat: {
      sales_tax: sales.tax,
      purchase_tax: purchase.tax,
      payable: vatPayable,
      refundable: vatPayable < 0 ? Math.abs(vatPayable) : 0,
      label: vatPayable >= 0 ? '납부세액' : '환급세액',
    },
    sources,
  });
});

// 2. GET /api/vat/summary — 연간 부가세 대시보드 (법인별)
router.get('/api/vat/summary', async (req, res, parsed) => {
  if (!auth(req, res)) return;
  const qs = parsed.searchParams;
  const year = qs.get('year') || new Date().getFullYear().toString();
  const company = qs.get('company') || 'barunson';

  const periods = [];
  let ytdSalesSupply = 0, ytdSalesTax = 0, ytdPurchaseSupply = 0, ytdPurchaseTax = 0;

  for (const [key] of Object.entries(PERIOD_MAP)) {
    const pd = periodDates(year, key);
    const { sales, purchase } = await getCompanyData(company, pd);
    const vatPayable = sales.tax - purchase.tax;

    ytdSalesSupply += sales.supply;
    ytdSalesTax += sales.tax;
    ytdPurchaseSupply += purchase.supply;
    ytdPurchaseTax += purchase.tax;

    const saved = ctx.db.prepare(
      'SELECT id, status FROM vat_reports WHERE year = ? AND period_type = ? AND company = ? ORDER BY id DESC LIMIT 1'
    ).get(year, pd.label, company);

    periods.push({
      period: key, label: pd.label, start_date: pd.start, end_date: pd.end,
      sales_supply: sales.supply, sales_tax: sales.tax,
      purchase_supply: purchase.supply, purchase_tax: purchase.tax,
      vat_payable: vatPayable, saved_report: saved || null,
    });
  }

  ctx.ok(res, {
    year, company,
    company_label: company === 'dd' ? '뚜비뚜비(DD)' : '바른손',
    periods,
    ytd: {
      sales_supply: ytdSalesSupply, sales_tax: ytdSalesTax,
      purchase_supply: ytdPurchaseSupply, purchase_tax: ytdPurchaseTax,
      vat_payable: ytdSalesTax - ytdPurchaseTax,
    },
  });
});

// 3. POST /api/vat/report/save — 신고서 저장 (법인별)
router.post('/api/vat/report/save', async (req, res) => {
  const decoded = auth(req, res);
  if (!decoded) return;
  try {
    const body = await ctx.readJSON(req);
    const { report_type, year, period, company, sales_total, sales_tax, purchase_total, purchase_tax, status } = body;
    if (!year || !period) { ctx.fail(res, 400, '연도와 기간은 필수입니다'); return; }
    const pd = periodDates(year, period);
    if (!pd) { ctx.fail(res, 400, '유효하지 않은 기간입니다'); return; }

    const vatPayable = (sales_tax || 0) - (purchase_tax || 0);
    const info = ctx.db.prepare(`
      INSERT INTO vat_reports (company, report_type, period_type, year, start_date, end_date,
        sales_total, sales_tax, purchase_total, purchase_tax, vat_payable, status, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      company || 'barunson',
      report_type || 'preliminary',
      pd.label, year, pd.start, pd.end,
      sales_total || 0, sales_tax || 0,
      purchase_total || 0, purchase_tax || 0,
      vatPayable, status || 'draft',
      decoded.name || decoded.email || ''
    );

    ctx.ok(res, { id: info.lastInsertRowid, message: '부가세 신고서가 저장되었습니다' });
  } catch (e) { ctx.fail(res, 500, e.message); }
});

// 4. GET /api/vat/report/:id — 저장된 신고서 상세
router.getP(/^\/api\/vat\/report\/(\d+)$/, async (req, res, parsed, match) => {
  if (!auth(req, res)) return;
  const id = parseInt(match[1], 10);
  const report = ctx.db.prepare('SELECT * FROM vat_reports WHERE id = ?').get(id);
  if (!report) { ctx.fail(res, 404, '해당 신고서를 찾을 수 없습니다'); return; }
  ctx.ok(res, report);
});

// 5. GET /api/vat/sales-list — 매출처별 합계 (법인별)
router.get('/api/vat/sales-list', async (req, res, parsed) => {
  if (!auth(req, res)) return;
  const qs = parsed.searchParams;
  const year = qs.get('year') || new Date().getFullYear().toString();
  const period = qs.get('period') || '1q1';
  const company = qs.get('company') || 'barunson';
  const pd = periodDates(year, period);
  if (!pd) { ctx.fail(res, 400, '유효하지 않은 기간입니다'); return; }

  let mainList = [], hometaxList = [];
  if (company === 'dd') {
    mainList = await vendorListDD(pd.startDash, pd.endDash);
    hometaxList = vendorListHometax(pd.start, pd.end, 'AR', 'dd');
  } else {
    mainList = await vendorListXerp(pd.start, pd.end, 'AR');
    hometaxList = vendorListHometax(pd.start, pd.end, 'AR', 'barunson');
  }

  const allItems = mainList.concat(hometaxList);
  const totalSupply = allItems.reduce((s, r) => s + (r.supply || 0), 0);
  const totalTax = allItems.reduce((s, r) => s + (r.vat || 0), 0);
  const totalCount = allItems.reduce((s, r) => s + (r.cnt || 0), 0);

  ctx.ok(res, {
    year, period, company, period_label: pd.label,
    company_label: company === 'dd' ? '뚜비뚜비(DD)' : '바른손',
    type: '매출',
    main: mainList, hometax: hometaxList,
    // backward compat
    xerp: company === 'barunson' ? mainList : [],
    totals: { count: totalCount, supply: totalSupply, tax: totalTax },
  });
});

// 6. GET /api/vat/purchase-list — 매입처별 합계 (법인별)
router.get('/api/vat/purchase-list', async (req, res, parsed) => {
  if (!auth(req, res)) return;
  const qs = parsed.searchParams;
  const year = qs.get('year') || new Date().getFullYear().toString();
  const period = qs.get('period') || '1q1';
  const company = qs.get('company') || 'barunson';
  const pd = periodDates(year, period);
  if (!pd) { ctx.fail(res, 400, '유효하지 않은 기간입니다'); return; }

  let mainList = [], hometaxList = [];
  if (company === 'dd') {
    // DD 매입은 홈택스에서만
    mainList = [];
    hometaxList = vendorListHometax(pd.start, pd.end, 'AP', 'dd');
  } else {
    mainList = await vendorListXerp(pd.start, pd.end, 'AP');
    hometaxList = vendorListHometax(pd.start, pd.end, 'AP', 'barunson');
  }

  const allItems = mainList.concat(hometaxList);
  const totalSupply = allItems.reduce((s, r) => s + (r.supply || 0), 0);
  const totalTax = allItems.reduce((s, r) => s + (r.vat || 0), 0);
  const totalCount = allItems.reduce((s, r) => s + (r.cnt || 0), 0);

  ctx.ok(res, {
    year, period, company, period_label: pd.label,
    company_label: company === 'dd' ? '뚜비뚜비(DD)' : '바른손',
    type: '매입',
    main: mainList, hometax: hometaxList,
    xerp: company === 'barunson' ? mainList : [],
    totals: { count: totalCount, supply: totalSupply, tax: totalTax },
  });
});

// 7. GET /api/vat/export — 내보내기 (법인별)
router.get('/api/vat/export', async (req, res, parsed) => {
  if (!auth(req, res)) return;
  const qs = parsed.searchParams;
  const year = qs.get('year') || new Date().getFullYear().toString();
  const period = qs.get('period') || '1q1';
  const company = qs.get('company') || 'barunson';
  const pd = periodDates(year, period);
  if (!pd) { ctx.fail(res, 400, '유효하지 않은 기간입니다'); return; }

  const companyLabel = company === 'dd' ? '뚜비뚜비(DD)' : '바른손';
  const { sales, purchase, sources } = await getCompanyData(company, pd);
  const vatPayable = sales.tax - purchase.tax;

  let salesVendors = [], purchaseVendors = [];
  if (company === 'dd') {
    salesVendors = await vendorListDD(pd.startDash, pd.endDash);
  } else {
    salesVendors = await vendorListXerp(pd.start, pd.end, 'AR');
    purchaseVendors = await vendorListXerp(pd.start, pd.end, 'AP');
  }
  const salesHometax = vendorListHometax(pd.start, pd.end, 'AR', company);
  const purchaseHometax = vendorListHometax(pd.start, pd.end, 'AP', company);

  ctx.ok(res, {
    title: `부가가치세 신고서 — ${companyLabel} (${year}년 ${pd.label})`,
    year, period, company, company_label: companyLabel,
    period_label: pd.label, start_date: pd.start, end_date: pd.end,
    section1_sales: {
      label: '과세표준 및 매출세액',
      세금계산서_매출: { count: sales.count, supply_amount: sales.supply, tax_amount: sales.tax },
    },
    section2_purchase: {
      label: '매입세액',
      세금계산서_매입: { count: purchase.count, supply_amount: purchase.supply, tax_amount: purchase.tax },
    },
    section3_calculation: {
      label: '납부(환급)세액 계산',
      매출세액: sales.tax, 매입세액: purchase.tax,
      납부세액: vatPayable >= 0 ? vatPayable : 0,
      환급세액: vatPayable < 0 ? Math.abs(vatPayable) : 0,
    },
    appendix: {
      매출처별_세금계산서합계표: { main: salesVendors, hometax: salesHometax },
      매입처별_세금계산서합계표: { main: purchaseVendors, hometax: purchaseHometax },
    },
    sources,
  });
});

module.exports = { router, initTables };
