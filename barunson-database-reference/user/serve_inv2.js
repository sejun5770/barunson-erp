const _startTime = Date.now();
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { URL } = require('url');
const Database = require('better-sqlite3');
const nodemailer = require('nodemailer');
const sql = require('mssql');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

// ── XERP MSSQL 연결 ─────────────────────────────────────────────────
let xerpPool = null;
let xerpUsageCache = null;
let xerpUsageCacheTime = 0;
let xerpInventoryCache = null;
let xerpInventoryCacheTime = 0;
let giftSetShipmentCache = {};    // { xerp_code: total_qty }
let giftSetShipmentCacheTime = 0;
// ── 매출관리 캐시 ──
let salesKpiCache = null, salesKpiCacheTime = 0;
const SALES_CACHE_TTL = 30 * 60 * 1000; // 30분
// ── 원가관리 캐시 ──
let costSummaryCache = null, costSummaryCacheTime = 0;
const COST_CACHE_TTL = 30 * 60 * 1000; // 30분
// ── 회계 모듈 캐시 ──
let acctStatsCache = null, acctStatsCacheTime = 0;
let trialBalanceCache = null, trialBalanceCacheTime = 0;
const ACCT_CACHE_TTL = 30 * 60 * 1000; // 30분
// ── 세금계산서 캐시 ──
let taxInvoiceSummaryCache = null, taxInvoiceSummaryCacheTime = 0;
const DEPT_GUBUN_LABELS = {'SB':'쇼핑몰B','BR':'바른손','ST':'스토어','SS':'쇼핑몰S','SA':'쇼핑몰A','OB':'기타B','DE':'기타'};
const BRAND_LABELS = {'B':'바른손카드','S':'비핸즈','C':'더카드','X':'디얼디어','W':'W카드','N':'네이처','I':'이니스','H':'비핸즈프리미엄','F':'플라워','D':'디자인카드','P':'프리미어','M':'모바일','G':'글로벌','U':'유니세프','Y':'유니크','K':'BK','T':'프리미어더카드','A':'기타'};
// .env를 여러 위치에서 탐색
const dotenvCandidates = [
  path.join(__dirname, '.env'),
  path.join(__dirname, '..', '.env'),
  path.join(__dirname, '..', '..', '.env')
];
const dotenvPath = dotenvCandidates.find(p => fs.existsSync(p)) || dotenvCandidates[1];
const envVars = {};
try {
  const envContent = fs.readFileSync(dotenvPath, 'utf8');
  envContent.replace(/\r/g, '').split('\n').forEach(line => {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) envVars[m[1].trim()] = m[2].trim();
  });
  console.log('환경변수 로드:', dotenvPath, '(DB_SERVER:', envVars.DB_SERVER ? 'OK' : 'missing', ')');
} catch (e) { console.warn('.env 로드 실패:', e.message); }

const xerpConfig = {
  server: envVars.DB_SERVER || process.env.DB_SERVER || '',
  port: parseInt(envVars.DB_PORT || process.env.DB_PORT || '1433'),
  user: envVars.DB_USER || process.env.DB_USER || '',
  password: envVars.DB_PASSWORD || process.env.DB_PASSWORD || '',
  database: 'XERP',
  options: { encrypt: true, trustServerCertificate: false, requestTimeout: 120000 },
  pool: { max: 5, min: 1, idleTimeoutMillis: 300000 }
};

let xerpReconnectTimer = null;
let xerpReconnectAttempts = 0;
const XERP_MAX_RECONNECT_DELAY = 300000; // 최대 5분

async function connectXERP() {
  if (!xerpConfig.server) { console.warn('XERP: DB_SERVER 미설정 → 출고현황 비활성'); return false; }
  try {
    // 기존 풀 정리
    if (xerpPool) { try { await xerpPool.close(); } catch(_){} xerpPool = null; }
    xerpPool = await sql.connect(xerpConfig);
    xerpReconnectAttempts = 0;
    console.log('XERP 데이터베이스 연결 완료');

    // 연결 에러 이벤트 → 자동 재연결
    xerpPool.on('error', (err) => {
      console.error('XERP 연결 끊김:', err.message);
      xerpPool = null;
      scheduleXerpReconnect();
    });
    return true;
  } catch (e) {
    console.warn(`XERP 연결 실패 (시도 ${xerpReconnectAttempts + 1}):`, e.message);
    xerpPool = null;
    return false;
  }
}

function scheduleXerpReconnect() {
  if (xerpReconnectTimer) return; // 이미 스케줄됨
  xerpReconnectAttempts++;
  // 지수 백오프: 5s, 10s, 20s, 40s ... 최대 5분
  const delay = Math.min(5000 * Math.pow(2, xerpReconnectAttempts - 1), XERP_MAX_RECONNECT_DELAY);
  console.log(`XERP 재연결 예약: ${Math.round(delay/1000)}초 후 (시도 #${xerpReconnectAttempts})`);
  xerpReconnectTimer = setTimeout(async () => {
    xerpReconnectTimer = null;
    const ok = await connectXERP();
    if (!ok) scheduleXerpReconnect();
  }, delay);
}

// 요청 시 풀 상태 확인 + 자동 복구 헬퍼
async function ensureXerpPool() {
  if (xerpPool && xerpPool.connected) return xerpPool;
  // 연결 없으면 즉시 재연결 시도
  const ok = await connectXERP();
  if (ok) return xerpPool;
  return null;
}

// 초기 연결
(async function initXERP() {
  const ok = await connectXERP();
  if (!ok) scheduleXerpReconnect();
})();

// ── DD (디얼디어) MySQL 연결 ─────────────────────────────────────────
let ddPool = null;
const ddConfig = {
  host: envVars.DD_DB_SERVER || process.env.DD_DB_SERVER || '',
  port: parseInt(envVars.DD_DB_PORT || process.env.DD_DB_PORT || '3306'),
  user: envVars.DD_DB_USER || process.env.DD_DB_USER || '',
  password: envVars.DD_DB_PASSWORD || process.env.DD_DB_PASSWORD || '',
  database: 'wedding',
  charset: 'utf8mb4',
  waitForConnections: true,
  connectionLimit: 3,
  queueLimit: 0
};

async function ensureDdPool() {
  if (ddPool) return ddPool;
  if (!ddConfig.host) { console.warn('DD: DD_DB_SERVER 미설정'); return null; }
  try {
    const mysql = require('mysql2/promise');
    ddPool = await mysql.createPool(ddConfig);
    // 연결 테스트
    const [rows] = await ddPool.query('SELECT 1');
    console.log('DD(디얼디어) MySQL 연결 완료');
    return ddPool;
  } catch(e) {
    console.warn('DD MySQL 연결 실패:', e.message);
    ddPool = null;
    return null;
  }
}

// DD 초기 연결 시도
(async function initDD() {
  if (ddConfig.host) await ensureDdPool();
  else console.log('ℹ DD_DB_SERVER 미설정 → DD 동기화 비활성');
})();

// ── Google Sheet 동기화 (Apps Script 웹앱 방식) ─────────────────────
const APPS_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbwwhOP9gtC-8l6BFSRRJFwv2UrnYCtXxvaD9_NBHcWTsKuGl3Hlk5qqdCxnla-gxHMa/exec';
let gAccessToken = null;
let gRefreshToken = null;
let gClientId = null;
let gClientSecret = null;
let gTokenExpiry = 0;

function refreshGoogleToken() {
  return new Promise((resolve) => {
    if (!gRefreshToken || !gClientId || !gClientSecret) { resolve(false); return; }
    const postData = `client_id=${gClientId}&client_secret=${gClientSecret}&refresh_token=${gRefreshToken}&grant_type=refresh_token`;
    const req = https.request({
      hostname: 'oauth2.googleapis.com', path: '/token', method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postData) }
    }, (resp) => {
      let data = '';
      resp.on('data', c => data += c);
      resp.on('end', () => {
        try {
          const d = JSON.parse(data);
          if (d.access_token) {
            gAccessToken = d.access_token;
            gTokenExpiry = Date.now() + (d.expires_in || 3600) * 1000 - 60000;
            console.log('Google 토큰 갱신 완료');
            resolve(true);
          } else { console.warn('토큰 갱신 실패:', data.substring(0, 200)); resolve(false); }
        } catch (e) { resolve(false); }
      });
    });
    req.on('error', () => resolve(false));
    req.write(postData);
    req.end();
  });
}

async function ensureGoogleToken() {
  if (!gRefreshToken) return false;
  if (gAccessToken && Date.now() < gTokenExpiry) return true;
  return await refreshGoogleToken();
}

(async function initGoogleAuth() {
  try {
    const clasprc = JSON.parse(fs.readFileSync(path.join(process.env.USERPROFILE || process.env.HOME, '.clasprc.json'), 'utf8'));
    const tokens = clasprc.tokens.default;
    gAccessToken = tokens.access_token;
    gRefreshToken = tokens.refresh_token;
    gClientId = tokens.client_id;
    gClientSecret = tokens.client_secret;
    gTokenExpiry = tokens.expiry_date || 0;
    // 토큰 만료 시 즉시 갱신
    if (Date.now() >= gTokenExpiry) {
      await refreshGoogleToken();
    }
    console.log('Google Sheet 동기화 준비 완료 (Apps Script 웹앱)');
  } catch (e) {
    console.warn('Google 인증 초기화 실패 (시트 동기화 비활성):', e.message);
  }
})();

async function appendToGoogleSheet(rows) {
  await ensureGoogleToken();
  return new Promise((resolve) => {
    if (!gAccessToken || !rows.length) { resolve({ ok: false, error: 'not ready' }); return; }
    const body = JSON.stringify({ rows });
    const bodyBuf = Buffer.from(body, 'utf8');
    const u = new URL(APPS_SCRIPT_URL);

    function doRequest(urlObj, redirectCount, method) {
      if (redirectCount > 5) { resolve({ ok: false, error: 'too many redirects' }); return; }
      const isPOST = method === 'POST';
      const options = {
        hostname: urlObj.hostname, path: urlObj.pathname + urlObj.search, method: method,
        headers: { 'Authorization': 'Bearer ' + gAccessToken }
      };
      if (isPOST) {
        options.headers['Content-Type'] = 'application/json; charset=utf-8';
        options.headers['Content-Length'] = bodyBuf.length;
      }
      const req = https.request(options, (resp) => {
        if (resp.statusCode >= 300 && resp.statusCode < 400 && resp.headers.location) {
          // Apps Script: POST→302→GET (standard HTTP redirect behavior)
          doRequest(new URL(resp.headers.location), redirectCount + 1, 'GET');
          resp.resume();
          return;
        }
        let data = '';
        resp.on('data', c => data += c);
        resp.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { resolve({ ok: false, error: 'parse error', status: resp.statusCode, raw: data.substring(0, 300) }); }
        });
      });
      req.on('error', e => resolve({ ok: false, error: e.message }));
      if (isPOST) req.write(bodyBuf);
      req.end();
    }
    doRequest(u, 0, 'POST');
  });
}

async function cancelInGoogleSheet(productCodes, orderDate) {
  await ensureGoogleToken();
  return new Promise((resolve) => {
    if (!gAccessToken || !productCodes.length) { resolve({ ok: false, error: 'not ready' }); return; }
    const body = JSON.stringify({ action: 'cancel', product_codes: productCodes, order_date: orderDate || '' });
    const bodyBuf = Buffer.from(body, 'utf8');
    const u = new URL(APPS_SCRIPT_URL);

    function doRequest(urlObj, redirectCount, method) {
      if (redirectCount > 5) { resolve({ ok: false, error: 'too many redirects' }); return; }
      const isPOST = method === 'POST';
      const options = {
        hostname: urlObj.hostname, path: urlObj.pathname + urlObj.search, method: method,
        headers: { 'Authorization': 'Bearer ' + gAccessToken }
      };
      if (isPOST) {
        options.headers['Content-Type'] = 'application/json; charset=utf-8';
        options.headers['Content-Length'] = bodyBuf.length;
      }
      const req = https.request(options, (resp) => {
        if (resp.statusCode >= 300 && resp.statusCode < 400 && resp.headers.location) {
          doRequest(new URL(resp.headers.location), redirectCount + 1, 'GET');
          resp.resume();
          return;
        }
        let data = '';
        resp.on('data', c => data += c);
        resp.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { resolve({ ok: false, error: 'parse error' }); }
        });
      });
      req.on('error', e => resolve({ ok: false, error: e.message }));
      if (isPOST) req.write(bodyBuf);
      req.end();
    }
    doRequest(u, 0, 'POST');
  });
}

// ── 이메일 발송 (nodemailer SMTP + Apps Script 폴백) ────────────────
const PORTAL_SECRET = 'barun-company-portal-2026';
const TEST_EMAIL = 'seungchan.back@barunn.net'; // 테스트용 — 모든 이메일 여기로
const BASE_URL = envVars.BASE_URL || process.env.BASE_URL || (() => {
  const ni = require('os').networkInterfaces();
  for (const addrs of Object.values(ni)) {
    for (const a of addrs) { if (a.family === 'IPv4' && !a.internal) return `http://${a.address}:12026`; }
  }
  return 'http://localhost:12026';
})();

// SMTP 설정 (.env에서 로드)
const SMTP_USER = envVars.SMTP_USER || process.env.SMTP_USER || '';       // Gmail 주소
const SMTP_PASS = envVars.SMTP_PASS || process.env.SMTP_PASS || '';       // Gmail 앱 비밀번호 (16자리)
const SMTP_FROM = envVars.SMTP_FROM || process.env.SMTP_FROM || SMTP_USER;

// Google OAuth 설정
const GOOGLE_CLIENT_ID = envVars.GOOGLE_CLIENT_ID || process.env.GOOGLE_CLIENT_ID || '';
const ALLOWED_EMAIL_DOMAINS = (envVars.ALLOWED_EMAIL_DOMAINS || process.env.ALLOWED_EMAIL_DOMAINS || 'barunn.net').split(',').map(d => d.trim());
if (GOOGLE_CLIENT_ID) console.log('✅ Google OAuth 설정됨 (허용 도메인:', ALLOWED_EMAIL_DOMAINS.join(', '), ')');
else console.log('ℹ️ GOOGLE_CLIENT_ID 미설정 — .env에 추가하면 Google 로그인 활성화');

let smtpTransporter = null;
if (SMTP_USER && SMTP_PASS) {
  smtpTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: SMTP_USER, pass: SMTP_PASS }
  });
  smtpTransporter.verify().then(() => {
    console.log('✅ SMTP(Gmail) 이메일 발송 준비 완료');
  }).catch(err => {
    console.warn('⚠️ SMTP 연결 실패:', err.message);
    smtpTransporter = null;
  });
} else {
  console.log('ℹ️ SMTP 미설정 — .env에 SMTP_USER, SMTP_PASS 추가 필요 (Gmail 앱 비밀번호)');
}

// P2-13: JWT 기반 거래처 토큰 (기존 해시 폴백 유지)
const VENDOR_JWT_SECRET = envVars.VENDOR_JWT_SECRET || (() => {
  const vendorSecretPath = path.join(__dirname, '.vendor_jwt_secret');
  try { return fs.readFileSync(vendorSecretPath, 'utf8').trim(); }
  catch {
    const s = crypto.randomBytes(32).toString('hex');
    try { fs.writeFileSync(vendorSecretPath, s, 'utf8'); } catch(_){}
    return s;
  }
})();

function generateVendorTokenLegacy(email) {
  return crypto.createHash('sha256').update(email + PORTAL_SECRET).digest('hex').slice(0, 16);
}

function generateVendorToken(email) {
  try {
    return jwt.sign({ email, type: 'vendor' }, VENDOR_JWT_SECRET, { expiresIn: '7d' });
  } catch (e) {
    console.warn('JWT 생성 실패, 레거시 토큰 사용:', e.message);
    return generateVendorTokenLegacy(email);
  }
}

function verifyVendorToken(email, token) {
  // 1. JWT 검증 시도
  try {
    const decoded = jwt.verify(token, VENDOR_JWT_SECRET);
    if (decoded && decoded.email === email && decoded.type === 'vendor') return true;
  } catch (_) {}
  // 2. 레거시 해시 토큰 폴백 (하위 호환)
  return generateVendorTokenLegacy(email) === token;
}

// product_info.json 로드 (원자재코드, 원재료명, 절 조회용)
let productInfoCache = null;
function getProductInfo() {
  if (productInfoCache) return productInfoCache;
  try {
    productInfoCache = JSON.parse(fs.readFileSync(path.join(__dir, 'product_info.json'), 'utf8'));
  } catch (e) { productInfoCache = {}; }
  return productInfoCache;
}

// 업체+제품코드 직전 단가 조회 (최신 승인/확인 거래명세서에서)
function getLastVendorPrice(vendorName, productCode) {
  try {
    const docs = db.prepare(`SELECT items_json, vendor_modified_json FROM trade_document
      WHERE vendor_name=? AND status IN ('vendor_confirmed','approved')
      ORDER BY id DESC LIMIT 10`).all(vendorName);
    for (const doc of docs) {
      const modified = doc.vendor_modified_json ? JSON.parse(doc.vendor_modified_json) : null;
      const items = JSON.parse(doc.items_json || '[]');
      for (let i = 0; i < items.length; i++) {
        if (items[i].product_code === productCode) {
          const price = modified && modified[i] ? modified[i].unit_price : items[i].unit_price;
          if (price && price > 0) return price;
        }
      }
    }
  } catch(e) {}
  // 후공정 단가 마스터에서도 조회
  try {
    const pp = db.prepare(`SELECT unit_price FROM post_process_price WHERE vendor_name=? AND unit_price>0 ORDER BY id DESC LIMIT 1`).get(vendorName);
    if (pp) return pp.unit_price;
  } catch(e) {}
  return 0;
}

// PO 활동 로그 기록
function logPOActivity(poId, action, opts = {}) {
  const po = db.prepare('SELECT po_number, status, material_status, process_status FROM po_header WHERE po_id=?').get(poId);
  if (!po) return;
  db.prepare(`INSERT INTO po_activity_log (po_id, po_number, action, actor, actor_type, from_status, to_status, from_material_status, to_material_status, from_process_status, to_process_status, details) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(
      poId, po.po_number, action,
      opts.actor || '', opts.actor_type || 'system',
      opts.from_status || po.status, opts.to_status || '',
      opts.from_mat || po.material_status || '', opts.to_mat || '',
      opts.from_proc || po.process_status || '', opts.to_proc || '',
      opts.details || ''
    );
}

// 이메일 발송: SMTP 우선, Apps Script 폴백
async function sendPOEmail(po, items, vendorEmail, vendorName, isPostProcess, emailCc) {

  const pInfo = getProductInfo();
  const token = generateVendorToken(vendorEmail);
  const portalUrl = `${BASE_URL}/?vendor_email=${encodeURIComponent(vendorEmail)}&vendor_name=${encodeURIComponent(vendorName)}&token=${token}`;

  const typeLabel = isPostProcess ? '후공정' : '원재료';
  const subject = `[바른컴퍼니] ${typeLabel} 발주서 - ${po.po_number} (${vendorName})`;

  // 품목별 product_info 매핑 + 연(R) 계산: 발주수량 / 500 / 절 / 조판
  const enrichedItems = items.map(it => {
    const pi = pInfo[it.product_code] || {};
    const cut = parseInt(pi['절']) || 1;
    const jopan = parseInt(pi['조판']) || 1;
    const qty = it.ordered_qty || 0;
    const reams = qty / 500 / cut / jopan;
    const reamsStr = reams % 1 === 0 ? String(reams) : reams.toFixed(1);
    return {
      ...it,
      material_code: pi['원자재코드'] || '',
      material_name: pi['원재료용지명'] || it.spec || '',
      cut_spec: pi['절'] || '',
      ream_qty: reamsStr,
    };
  });

  // 원재료/후공정 구분
  const isRawMaterial = !isPostProcess;

  // 원재료: 다음 입고처(후공정 업체) 조회
  let nextDestinations = [];
  if (isRawMaterial && po.po_date) {
    const postPOs = db.prepare(`SELECT DISTINCT vendor_name FROM po_header WHERE po_date = ? AND po_type = '후공정' AND status != 'cancelled'`).all(po.po_date);
    nextDestinations = postPOs.map(p => p.vendor_name);
  }

  const thStyle = 'border:1px solid #bbb;padding:8px 10px;text-align:left;background:#f3f4f6;font-weight:600;font-size:12px';
  const tdStyle = 'border:1px solid #ddd;padding:8px 10px;font-size:13px';

  let tableHeader, tableRows;
  if (isRawMaterial) {
    tableHeader = `<tr>
      <th style="${thStyle}">제품코드</th>
      <th style="${thStyle}">원재료코드</th>
      <th style="${thStyle}">원재료명</th>
      <th style="${thStyle};text-align:right">발주수량(R)</th>
      <th style="${thStyle};text-align:center">절</th>
    </tr>`;
    tableRows = enrichedItems.map(it => `<tr>
      <td style="${tdStyle};font-weight:600">${it.product_code || ''}</td>
      <td style="${tdStyle}">${it.material_code || ''}</td>
      <td style="${tdStyle}">${it.material_name || ''}</td>
      <td style="${tdStyle};text-align:right;font-weight:700;font-size:15px">${it.ream_qty || 0}R <span style="font-size:11px;color:#888;font-weight:400">(${(it.ordered_qty || 0).toLocaleString()}매)</span></td>
      <td style="${tdStyle};text-align:center">${it.cut_spec || ''}</td>
    </tr>`).join('');
  } else {
    // 후공정 발주서: 제품코드 | 공정 | 입고수량(R) | 생산수량(낱개) | 규격
    tableHeader = `<tr>
      <th style="${thStyle}">제품코드</th>
      <th style="${thStyle}">공정</th>
      <th style="${thStyle};text-align:right">입고수량(R)</th>
      <th style="${thStyle};text-align:right">생산수량(낱개)</th>
      <th style="${thStyle}">규격</th>
    </tr>`;
    tableRows = enrichedItems.map(it => `<tr>
      <td style="${tdStyle};font-weight:600">${it.product_code || ''}</td>
      <td style="${tdStyle}">${it.process_type || ''}</td>
      <td style="${tdStyle};text-align:right;font-weight:700">${it.ream_qty || '-'}R</td>
      <td style="${tdStyle};text-align:right;font-weight:600">${(it.ordered_qty || 0).toLocaleString()}</td>
      <td style="${tdStyle}">${it.spec || ''}</td>
    </tr>`).join('');
  }

  // 이메일 본문 HTML
  const html = `
    <div style="font-family:'맑은 고딕',sans-serif;max-width:700px;margin:0 auto">
      <div style="background:#f97316;color:#fff;padding:20px;border-radius:8px 8px 0 0">
        <h2 style="margin:0">바른컴퍼니 ${typeLabel} 발주서</h2>
      </div>
      <div style="padding:24px;border:1px solid #e5e7eb;border-top:none">
        <table style="width:100%;margin-bottom:20px;font-size:14px">
          <tr><td style="padding:6px 0;color:#666;width:100px">발주번호</td><td style="padding:6px 0;font-weight:600">${po.po_number}</td></tr>
          <tr><td style="padding:6px 0;color:#666">발주일</td><td style="padding:6px 0">${po.po_date || ''}</td></tr>
          <tr><td style="padding:6px 0;color:#666">거래처</td><td style="padding:6px 0;font-weight:600">${vendorName}</td></tr>
          <tr><td style="padding:6px 0;color:#666">납기예정일</td><td style="padding:6px 0">${po.expected_date || ''}</td></tr>
          ${nextDestinations.length ? `<tr><td style="padding:6px 0;color:#666">다음 입고처</td><td style="padding:6px 0;font-weight:600;color:#f97316">${nextDestinations.join(', ')}</td></tr>` : ''}
          ${po.notes ? `<tr><td style="padding:6px 0;color:#666">비고</td><td style="padding:6px 0">${po.notes}</td></tr>` : ''}
        </table>
        <h3 style="margin:20px 0 10px;font-size:15px">발주 품목</h3>
        <table style="width:100%;border-collapse:collapse">
          <thead>${tableHeader}</thead>
          <tbody>${tableRows}</tbody>
        </table>
        <div style="margin-top:30px;text-align:center">
          <a href="${portalUrl}" style="display:inline-block;background:#f97316;color:#fff;padding:14px 40px;border-radius:8px;text-decoration:none;font-weight:600;font-size:16px">
            발주 확인하기
          </a>
        </div>
        <p style="margin-top:20px;color:#888;font-size:12px;text-align:center">
          위 버튼을 클릭하면 발주현황 페이지로 이동합니다.
        </p>
      </div>
    </div>`;

  // 합계 계산
  const totalQty = enrichedItems.reduce((s, it) => s + (it.ordered_qty || 0), 0);
  const totalReams = enrichedItems.reduce((s, it) => s + (parseFloat(it.ream_qty) || 0), 0);

  // 중국 거래처 판별 (origin 기반)
  const isChinaVendor = items.some(it => {
    const prod = db.prepare('SELECT origin FROM products WHERE product_code=?').get(it.product_code);
    return prod && prod.origin === '중국';
  });

  // 첨부파일용 발주서 HTML (인쇄 최적화 — 프로페셔널 양식)
  const attachmentHtml = `<!DOCTYPE html><html><head><meta charset="utf-8">
    <title>${typeLabel} 발주서 - ${po.po_number}</title>
    <style>
      body{font-family:'Noto Sans KR','맑은 고딕','Microsoft YaHei',sans-serif;margin:0;padding:30px 40px;color:#333;font-size:13px}
      .header{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:3px solid #f97316;padding-bottom:16px;margin-bottom:20px}
      .header-left h1{font-size:20px;margin:0 0 2px;color:#f97316}
      .header-left .sub{color:#666;font-size:11px}
      .header-right{text-align:right;font-size:11px;color:#666;line-height:1.6}
      .doc-title{text-align:center;font-size:18px;font-weight:700;margin:16px 0;letter-spacing:2px}
      ${isChinaVendor ? '.doc-title .cn{font-size:14px;color:#666;font-weight:400}' : ''}
      .info-grid{display:flex;gap:20px;margin-bottom:20px}
      .info-box{flex:1;border:1px solid #ddd;border-radius:6px;padding:12px 14px}
      .info-box h4{font-size:11px;color:#f97316;margin:0 0 8px;text-transform:uppercase;letter-spacing:1px}
      .info-box table{width:100%}
      .info-box td{border:none;padding:3px 0;font-size:12px;vertical-align:top}
      .info-box td:first-child{color:#888;width:70px;white-space:nowrap}
      .info-box td:last-child{font-weight:500}
      .items-table{width:100%;border-collapse:collapse;margin-bottom:16px}
      .items-table th{background:#f8f9fa;border:1px solid #ccc;padding:7px 10px;text-align:left;font-size:11px;font-weight:600;color:#555}
      .items-table td{border:1px solid #ddd;padding:7px 10px;font-size:12px}
      .items-table .right{text-align:right}
      .items-table .center{text-align:center}
      .items-table .bold{font-weight:700}
      .items-table .total-row{background:#fff3e0;font-weight:700;font-size:13px}
      .terms{margin:20px 0;padding:14px;background:#fafafa;border:1px solid #eee;border-radius:6px;font-size:11px;color:#666;line-height:1.7}
      .terms h4{margin:0 0 6px;font-size:12px;color:#333}
      .sign-area{display:flex;justify-content:space-between;margin-top:30px}
      .sign-box{width:45%;border-top:2px solid #333;padding-top:8px;text-align:center}
      .sign-box .label{font-size:11px;color:#888}
      .sign-box .name{font-size:14px;font-weight:600;margin-top:4px}
      .footer{margin-top:30px;text-align:center;color:#bbb;font-size:10px;border-top:1px solid #eee;padding-top:10px}
      @media print{body{margin:15px 20px;padding:0}.footer{page-break-after:always}}
    </style></head><body>

    <div class="header">
      <div class="header-left">
        <h1>BARUN COMPANY</h1>
        <div class="sub">바른컴퍼니 | Premium Invitation & Stationery</div>
      </div>
      <div class="header-right">
        Tel: 02-6959-0750<br>
        Email: barun@baruncompany.com<br>
        서울특별시 금천구 가산디지털1로 168
      </div>
    </div>

    <div class="doc-title">
      ${typeLabel} 발주서 PURCHASE ORDER
      ${isChinaVendor ? '<br><span class="cn">采购订单</span>' : ''}
    </div>

    <div class="info-grid">
      <div class="info-box">
        <h4>발주 정보 ${isChinaVendor ? '/ 订单信息' : ''}</h4>
        <table>
          <tr><td>${isChinaVendor ? 'PO번호' : '발주번호'}</td><td style="font-weight:700">${po.po_number}</td></tr>
          <tr><td>${isChinaVendor ? '下单日期' : '발주일'}</td><td>${po.po_date || ''}</td></tr>
          <tr><td>${isChinaVendor ? '交货日期' : '납기예정'}</td><td style="color:#e65100;font-weight:600">${po.expected_date || '협의'}</td></tr>
          <tr><td>품목수</td><td>${enrichedItems.length}건</td></tr>
        </table>
      </div>
      <div class="info-box">
        <h4>거래처 ${isChinaVendor ? '/ 供应商' : ''}</h4>
        <table>
          <tr><td>${isChinaVendor ? '供应商' : '업체명'}</td><td style="font-weight:700;font-size:14px">${vendorName}</td></tr>
          <tr><td>이메일</td><td>${vendorEmail}</td></tr>
          ${nextDestinations.length ? `<tr><td>입고처</td><td style="color:#f97316;font-weight:600">${nextDestinations.join(', ')}</td></tr>` : ''}
          ${po.notes ? `<tr><td>비고</td><td>${po.notes}</td></tr>` : ''}
        </table>
      </div>
    </div>

    <table class="items-table">
      <thead>
        ${isRawMaterial ? `<tr>
          <th style="width:30px">#</th>
          <th>제품코드${isChinaVendor ? '<br><span style="font-weight:400;color:#999">产品编号</span>' : ''}</th>
          <th>원재료코드</th>
          <th>원재료명${isChinaVendor ? '<br><span style="font-weight:400;color:#999">材料名称</span>' : ''}</th>
          <th class="right">발주수량(R)${isChinaVendor ? '<br><span style="font-weight:400;color:#999">订购量</span>' : ''}</th>
          <th class="right">매수</th>
          <th class="center">절</th>
        </tr>` : `<tr>
          <th style="width:30px">#</th>
          <th>제품코드</th>
          <th>공정</th>
          <th class="right">입고수량(R)</th>
          <th class="right">생산수량</th>
          <th>규격</th>
        </tr>`}
      </thead>
      <tbody>
        ${enrichedItems.map((it, idx) => isRawMaterial ? `<tr>
          <td class="center" style="color:#999">${idx + 1}</td>
          <td class="bold">${it.product_code || ''}</td>
          <td>${it.material_code || ''}</td>
          <td>${it.material_name || ''}</td>
          <td class="right bold" style="font-size:14px">${it.ream_qty || 0}R</td>
          <td class="right" style="color:#888">${(it.ordered_qty || 0).toLocaleString()}</td>
          <td class="center">${it.cut_spec || ''}</td>
        </tr>` : `<tr>
          <td class="center" style="color:#999">${idx + 1}</td>
          <td class="bold">${it.product_code || ''}</td>
          <td>${it.process_type || ''}</td>
          <td class="right bold">${it.ream_qty || '-'}R</td>
          <td class="right">${(it.ordered_qty || 0).toLocaleString()}</td>
          <td>${it.spec || ''}</td>
        </tr>`).join('')}
        <tr class="total-row">
          <td colspan="${isRawMaterial ? 4 : 3}" style="text-align:right;border:1px solid #ccc">합계 ${isChinaVendor ? '/ 合计' : ''}</td>
          ${isRawMaterial ? `
            <td class="right" style="border:1px solid #ccc;font-size:14px">${totalReams % 1 === 0 ? totalReams : totalReams.toFixed(1)}R</td>
            <td class="right" style="border:1px solid #ccc">${totalQty.toLocaleString()}</td>
            <td style="border:1px solid #ccc"></td>
          ` : `
            <td class="right" style="border:1px solid #ccc">${totalReams % 1 === 0 ? totalReams : totalReams.toFixed(1)}R</td>
            <td class="right" style="border:1px solid #ccc">${totalQty.toLocaleString()}</td>
            <td style="border:1px solid #ccc"></td>
          `}
        </tr>
      </tbody>
    </table>

    <div class="terms">
      <h4>거래 조건 ${isChinaVendor ? '/ 交易条件' : ''}</h4>
      ${isChinaVendor ? `
        결제조건 / 付款方式: 월말 정산 (月末结算)<br>
        운송조건 / 运输方式: FOB 중국항 (FOB China Port)<br>
        포장기준 / 包装要求: 500매/연 기준 박스 포장 (500张/令 纸箱包装)<br>
        품질기준 / 质量标准: 주문사양 기준 ±2% 이내 (订单规格 ±2%)
      ` : `
        결제조건: 월말 정산<br>
        운송조건: 거래처 직배송<br>
        포장기준: 500매/연 기준 박스 포장<br>
        품질기준: 주문사양 기준
      `}
    </div>

    <div class="sign-area">
      <div class="sign-box">
        <div class="label">발주처 ${isChinaVendor ? '/ 采购方' : ''}</div>
        <div class="name">바른컴퍼니</div>
      </div>
      <div class="sign-box">
        <div class="label">공급처 ${isChinaVendor ? '/ 供应方' : ''}</div>
        <div class="name">${vendorName}</div>
      </div>
    </div>

    <div class="footer">바른컴퍼니 발주시스템 | Generated ${new Date().toISOString().slice(0, 10)} | ${po.po_number}</div>
  </body></html>`;

  const toEmail = vendorEmail; // 실제 거래처 이메일로 발송
  const ccEmails = emailCc ? emailCc.split(',').map(e => e.trim()).filter(e => e) : [];

  // HTML → PDF 변환
  let pdfBuffer = null;
  let attachmentFileName = `${typeLabel}_발주서_${po.po_number}.pdf`;
  let attachmentContentType = 'application/pdf';
  try {
    // puppeteer-core (경량) 우선, 없으면 puppeteer (풀) 사용
    let puppeteer;
    try { puppeteer = require('puppeteer-core'); } catch(_) { puppeteer = require('puppeteer'); }
    // 시스템 Chrome/Chromium 경로 자동 탐색
    const execPaths = [
      process.env.PUPPETEER_EXECUTABLE_PATH,
      '/usr/bin/chromium-browser', '/usr/bin/chromium', '/usr/bin/google-chrome',
      'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
      'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    ].filter(Boolean);
    let execPath = null;
    for (const p of execPaths) { try { if (require('fs').existsSync(p)) { execPath = p; break; } } catch(_){} }
    const launchOpts = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu'] };
    if (execPath) launchOpts.executablePath = execPath;
    const browser = await puppeteer.launch(launchOpts);
    const page = await browser.newPage();
    await page.setContent(attachmentHtml, { waitUntil: 'networkidle0' });
    pdfBuffer = await page.pdf({ format: 'A4', printBackground: true, margin: { top: '10mm', bottom: '10mm', left: '10mm', right: '10mm' } });
    await browser.close();
    console.log(`📄 PDF 변환 완료: ${attachmentFileName} (${pdfBuffer.length} bytes)`);
  } catch (pdfErr) {
    console.warn(`⚠️ PDF 변환 실패 (HTML 첨부로 대체): ${pdfErr.message}`);
    pdfBuffer = null;
    attachmentFileName = `${typeLabel}_발주서_${po.po_number}.html`;
    attachmentContentType = 'text/html';
  }

  // 방법 1: nodemailer SMTP (Gmail 앱 비밀번호)
  if (smtpTransporter) {
    try {
      const mailOptions = {
        from: `"바른컴퍼니 발주시스템" <${SMTP_FROM}>`,
        to: toEmail,
        subject,
        html,
        attachments: [{
          filename: attachmentFileName,
          content: pdfBuffer || attachmentHtml,
          contentType: attachmentContentType
        }]
      };
      if (ccEmails.length > 0) mailOptions.cc = ccEmails.join(', ');
      const info = await smtpTransporter.sendMail(mailOptions);
      console.log(`✅ SMTP 이메일 발송 성공: ${subject} → ${toEmail}${ccEmails.length ? ' (CC: ' + ccEmails.join(', ') + ')' : ''}`, info.messageId);
      return { ok: true, to: toEmail, cc: ccEmails, method: 'smtp', messageId: info.messageId };
    } catch (err) {
      console.error(`❌ SMTP 이메일 발송 실패: ${err.message}`);
      // SMTP 실패 → Apps Script 폴백 시도
    }
  }

  // 방법 2: Apps Script 폴백
  await ensureGoogleToken();
  if (!gAccessToken) {
    return { ok: false, error: 'SMTP 미설정 + Google 토큰 없음. .env에 SMTP_USER/SMTP_PASS 설정 필요', to: toEmail };
  }

  const emailPayload = {
    action: 'sendEmail', to: toEmail, subject, html,
    cc: ccEmails.length > 0 ? ccEmails.join(',') : '',
    attachment: { name: attachmentFileName, content: attachmentHtml, mimeType: 'text/html' }
  };

  return new Promise((resolve) => {
    const body = JSON.stringify(emailPayload);
    const bodyBuf = Buffer.from(body, 'utf8');
    const u = new URL(APPS_SCRIPT_URL);

    function doRequest(urlObj, redirectCount, method) {
      if (redirectCount > 5) { resolve({ ok: false, error: 'too many redirects', to: toEmail }); return; }
      const isPOST = method === 'POST';
      const options = {
        hostname: urlObj.hostname, path: urlObj.pathname + urlObj.search, method,
        headers: { 'Authorization': 'Bearer ' + gAccessToken }
      };
      if (isPOST) {
        options.headers['Content-Type'] = 'application/json; charset=utf-8';
        options.headers['Content-Length'] = bodyBuf.length;
      }
      const req = https.request(options, (resp) => {
        if (resp.statusCode >= 300 && resp.statusCode < 400 && resp.headers.location) {
          doRequest(new URL(resp.headers.location), redirectCount + 1, 'GET');
          resp.resume();
          return;
        }
        let data = '';
        resp.on('data', c => data += c);
        resp.on('end', () => {
          try {
            const result = JSON.parse(data);
            if (result.status === 'success' || result.ok) {
              console.log(`✅ Apps Script 이메일 발송 성공: ${subject} → ${toEmail}`, result);
              resolve({ ok: true, ...result, to: toEmail, method: 'apps_script' });
            } else {
              console.error(`❌ Apps Script 이메일 실패:`, result);
              resolve({ ok: false, error: result.error || 'Apps Script 실패', to: toEmail, method: 'apps_script' });
            }
          } catch (e) {
            console.error(`❌ Apps Script 응답 파싱 실패 (HTML 로그인 페이지 등): ${data.substring(0, 200)}`);
            resolve({ ok: false, error: 'Google 인증 만료 — SMTP 설정 필요', to: toEmail, method: 'apps_script', raw: data.substring(0, 100) });
          }
        });
      });
      req.on('error', e => resolve({ ok: false, error: e.message, to: toEmail }));
      if (isPOST) req.write(bodyBuf);
      req.end();
    }
    doRequest(u, 0, 'POST');
  });
}

const PORT = parseInt(process.env.PORT || '12026', 10);
const __dir = __dirname;
const DATA_DIR = process.env.DATA_DIR || __dir;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dir, 'uploads');

// ── SQLite ──────────────────────────────────────────────────────────
const DB_PATH = path.join(DATA_DIR, 'orders.db');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// ── Ensure order_history table ──────────────────────────────────────
db.exec(`
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

// ── product_notes 테이블 (품목별 특이사항) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS product_notes (
    product_code TEXT PRIMARY KEY,
    note_type    TEXT DEFAULT '',
    note_text    TEXT DEFAULT '',
    updated_at   TEXT DEFAULT (datetime('now','localtime'))
  );
`);

// ── 필수 자동발주 테이블 ─────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS auto_order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_code TEXT NOT NULL UNIQUE,
    min_stock INTEGER DEFAULT 0,
    order_qty INTEGER DEFAULT 0,
    vendor_name TEXT DEFAULT '',
    enabled INTEGER DEFAULT 1,
    last_ordered_at TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime'))
  );
`);
// ── 품목관리 테이블 ──
db.exec(`
  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_code TEXT NOT NULL UNIQUE,
    product_name TEXT DEFAULT '',
    brand TEXT DEFAULT '',
    origin TEXT DEFAULT '한국',
    category TEXT DEFAULT '',
    status TEXT DEFAULT 'active',
    material_code TEXT DEFAULT '',
    material_name TEXT DEFAULT '',
    cut_spec TEXT DEFAULT '',
    jopan TEXT DEFAULT '',
    paper_maker TEXT DEFAULT '',
    memo TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime')),
    updated_at TEXT DEFAULT (datetime('now','localtime'))
  );
`);

// ── BOM 조판 계산 확장 컬럼 ──
try { db.exec("ALTER TABLE bom_items ADD COLUMN material_type TEXT DEFAULT 'IMPOSITION'"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN paper_standard TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN paper_type TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN gsm INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN finished_w REAL DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN finished_h REAL DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN bleed REAL DEFAULT 3"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN grip REAL DEFAULT 10"); } catch {}
try { db.exec("ALTER TABLE bom_items ADD COLUMN loss_rate REAL DEFAULT 5"); } catch {}
try { db.exec("ALTER TABLE bom_header ADD COLUMN default_order_qty INTEGER DEFAULT 1000"); } catch {}
try { db.exec("ALTER TABLE bom_header ADD COLUMN finished_w REAL DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE bom_header ADD COLUMN finished_h REAL DEFAULT 0"); } catch {}

// ── os_number 컬럼 추가 (발주 프로세스 자동화) ──
try { db.exec("ALTER TABLE po_header ADD COLUMN os_number TEXT DEFAULT ''"); } catch(_) {}

// ── 신제품 관리 컬럼 추가 ──
try { db.exec("ALTER TABLE products ADD COLUMN is_new_product INTEGER DEFAULT 0"); } catch(e) {}
try { db.exec("ALTER TABLE products ADD COLUMN first_order_done INTEGER DEFAULT 0"); } catch(e) {}
try { db.exec("ALTER TABLE products ADD COLUMN die_cost INTEGER DEFAULT 0"); } catch(e) {}
try { db.exec("ALTER TABLE products ADD COLUMN lead_time_days INTEGER DEFAULT 0"); } catch(e) {}
try { db.exec("ALTER TABLE products ADD COLUMN post_vendor TEXT DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE products ADD COLUMN unit TEXT DEFAULT 'EA'"); } catch(e) {}

// ── 생산지별 기본 리드타임 (일) ──
const ORIGIN_LEAD_TIME = { '중국': 50, '한국': 7, '더기프트': 14 };

// 원지사(paper_maker) 약칭 → 등록된 거래처명 매핑
const PAPER_MAKER_TO_VENDOR = {
  '대한': '대한통상', '대한통상': '대한통상',
  '두성': '두성종이', '두성종이': '두성종이',
  '삼원': '삼원제지', '삼원제지': '삼원제지',
  '서경': '주식회사 서경', '주식회사 서경': '주식회사 서경',
  '한솔PNS': '한솔PNS', '한솔': '한솔PNS',
  '파주': '파주', // 거래처 미등록 시 매핑 안 됨
};

function resolveVendor(paperMaker) {
  if (!paperMaker) return null;
  return PAPER_MAKER_TO_VENDOR[paperMaker.trim()] || null;
}

// ── 2단계 파이프라인 추적 컬럼 추가 (원재료 + 후공정) ──
try { db.exec("ALTER TABLE po_header ADD COLUMN material_status TEXT DEFAULT 'sent'"); } catch(e) {}
try { db.exec("ALTER TABLE po_header ADD COLUMN process_status TEXT DEFAULT 'waiting'"); } catch(e) {}

// ── 발송처리 시점 기록 ──
try { db.exec("ALTER TABLE po_header ADD COLUMN shipped_at TEXT DEFAULT ''"); } catch(e) {}

// ── 불량 처리 발주 연결 컬럼 추가 ──
try { db.exec("ALTER TABLE po_header ADD COLUMN defect_id INTEGER DEFAULT NULL"); } catch(e) {}
try { db.exec("ALTER TABLE po_header ADD COLUMN defect_number TEXT DEFAULT ''"); } catch(e) {}

// ── 생산지(origin) 컬럼 추가 (한국/중국/더기프트 분리) ──
try { db.exec("ALTER TABLE po_header ADD COLUMN origin TEXT DEFAULT ''"); } catch(_) {}

// ── 납품 스케줄 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS vendor_shipment_schedule (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER NOT NULL,
  po_number TEXT DEFAULT '',
  vendor_name TEXT DEFAULT '',
  ship_date TEXT DEFAULT '',
  ship_time TEXT DEFAULT 'AM',
  post_vendor_name TEXT DEFAULT '',
  post_vendor_email TEXT DEFAULT '',
  auto_email_sent INTEGER DEFAULT 0,
  status TEXT DEFAULT 'scheduled',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 공정 리드타임 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS process_lead_time (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  process_type TEXT NOT NULL,
  default_days INTEGER DEFAULT 1,
  adjusted_days INTEGER DEFAULT NULL,
  adjusted_reason TEXT DEFAULT '',
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(vendor_name, process_type)
)`);

// ── 리드타임 수정 이력 ──
db.exec(`CREATE TABLE IF NOT EXISTS lead_time_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  process_type TEXT NOT NULL,
  old_days INTEGER,
  new_days INTEGER,
  changed_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_lt_hist_vendor ON lead_time_history(vendor_name)`);

// ── 거래명세서 테이블 (v2 - 기존 invoice 플로우 대체) ──
db.exec(`CREATE TABLE IF NOT EXISTS trade_document (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER NOT NULL,
  po_number TEXT DEFAULT '',
  doc_type TEXT DEFAULT 'purchase',
  vendor_name TEXT DEFAULT '',
  vendor_type TEXT DEFAULT 'material',
  items_json TEXT DEFAULT '[]',
  vendor_modified_json TEXT DEFAULT '',
  vendor_memo TEXT DEFAULT '',
  price_diff INTEGER DEFAULT 0,
  status TEXT DEFAULT 'sent',
  sent_at TEXT DEFAULT (datetime('now','localtime')),
  confirmed_at TEXT DEFAULT '',
  approved_at TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 신규 테이블 인덱스 ──
db.exec(`CREATE INDEX IF NOT EXISTS idx_shipment_po ON vendor_shipment_schedule(po_id)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_shipment_date ON vendor_shipment_schedule(ship_date)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_shipment_status ON vendor_shipment_schedule(status)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_doc_po ON trade_document(po_id)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_doc_status ON trade_document(status)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_doc_vendor ON trade_document(vendor_name)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_lead_time_vendor ON process_lead_time(vendor_name)`);

// ── 후공정 단가 마스터 ──
db.exec(`CREATE TABLE IF NOT EXISTS post_process_price (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  process_type TEXT NOT NULL,
  price_type TEXT DEFAULT 'per_unit',
  price_tier TEXT DEFAULT '',
  spec_condition TEXT DEFAULT '',
  unit_price REAL DEFAULT 0,
  effective_from TEXT DEFAULT '',
  effective_to TEXT DEFAULT '',
  source TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_price_vendor ON post_process_price(vendor_name)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_price_process ON post_process_price(process_type)`);

// ── 후공정 거래 이력 ──
db.exec(`CREATE TABLE IF NOT EXISTS post_process_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  month TEXT NOT NULL,
  date TEXT DEFAULT '',
  product_code TEXT NOT NULL,
  process_type TEXT NOT NULL,
  spec TEXT DEFAULT '',
  qty TEXT DEFAULT '',
  product_qty TEXT DEFAULT '',
  unit_price REAL DEFAULT 0,
  amount REAL DEFAULT 0,
  os_number TEXT DEFAULT '',
  po_number TEXT DEFAULT '',
  imported_from TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_vendor ON post_process_history(vendor_name)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_product ON post_process_history(product_code)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_month ON post_process_history(month)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_process ON post_process_history(process_type)`);

// ── 제품별 후공정 매핑 ──
db.exec(`CREATE TABLE IF NOT EXISTS product_process_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  process_type TEXT NOT NULL,
  default_spec TEXT DEFAULT '',
  default_price REAL DEFAULT 0,
  vendor_name TEXT DEFAULT '',
  occurrence INTEGER DEFAULT 1,
  last_amount REAL DEFAULT 0,
  last_month TEXT DEFAULT '',
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(product_code, process_type, vendor_name)
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_ppm_product ON product_process_map(product_code)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_ppm_vendor ON product_process_map(vendor_name)`);

// ── 품목 필드 변경 이력 ──
db.exec(`CREATE TABLE IF NOT EXISTS product_field_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  field_name TEXT NOT NULL,
  old_value TEXT DEFAULT '',
  new_value TEXT DEFAULT '',
  changed_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_pfh_code ON product_field_history(product_code)`);

// ── 중국 선적 이력 ──
db.exec(`CREATE TABLE IF NOT EXISTS china_shipment_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shipment_date TEXT DEFAULT '',
  file_name TEXT DEFAULT '',
  total_boxes REAL DEFAULT 0,
  total_items INTEGER DEFAULT 0,
  target_boxes INTEGER DEFAULT 500,
  items_json TEXT DEFAULT '[]',
  status TEXT DEFAULT 'draft',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 중국 선적 이력: BL번호/선적일/예상도착일 컬럼 추가 ──
try { db.exec("ALTER TABLE china_shipment_log ADD COLUMN bl_number TEXT DEFAULT ''"); } catch(_) {}
try { db.exec("ALTER TABLE china_shipment_log ADD COLUMN ship_date TEXT DEFAULT ''"); } catch(_) {}
try { db.exec("ALTER TABLE china_shipment_log ADD COLUMN eta_date TEXT DEFAULT ''"); } catch(_) {}

// ── 거래명세서 파일 관리 ──
db.exec(`CREATE TABLE IF NOT EXISTS trade_doc_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  period TEXT NOT NULL,
  file_name TEXT DEFAULT '',
  file_path TEXT DEFAULT '',
  total_amount REAL DEFAULT 0,
  item_count INTEGER DEFAULT 0,
  parsed_at TEXT DEFAULT '',
  status TEXT DEFAULT 'uploaded',
  uploaded_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 활동 로그 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS po_activity_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER NOT NULL,
  po_number TEXT DEFAULT '',
  action TEXT NOT NULL,
  actor TEXT DEFAULT '',
  actor_type TEXT DEFAULT '',
  from_status TEXT DEFAULT '',
  to_status TEXT DEFAULT '',
  from_material_status TEXT DEFAULT '',
  to_material_status TEXT DEFAULT '',
  from_process_status TEXT DEFAULT '',
  to_process_status TEXT DEFAULT '',
  details TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_activity_po ON po_activity_log(po_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_activity_date ON po_activity_log(created_at)");

// ── po_items에 ship_date 컬럼 추가 (품목별 출고일) ──
try { db.exec(`ALTER TABLE po_items ADD COLUMN ship_date TEXT DEFAULT ''`); } catch(e) {}
try { db.exec(`ALTER TABLE po_items ADD COLUMN os_number TEXT DEFAULT ''`); } catch(e) {}

// ── 불량/품질 관리 ──
db.exec(`CREATE TABLE IF NOT EXISTS defects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  defect_number TEXT NOT NULL UNIQUE,
  po_id INTEGER,
  po_number TEXT DEFAULT '',
  vendor_name TEXT NOT NULL,
  product_code TEXT NOT NULL,
  product_name TEXT DEFAULT '',
  defect_date TEXT NOT NULL,
  defect_type TEXT NOT NULL DEFAULT '',
  defect_qty INTEGER NOT NULL DEFAULT 0,
  order_qty INTEGER NOT NULL DEFAULT 0,
  severity TEXT NOT NULL DEFAULT 'minor',
  description TEXT NOT NULL DEFAULT '',
  photo_url TEXT DEFAULT '',
  claim_type TEXT DEFAULT '',
  claim_amount REAL DEFAULT 0,
  resolution TEXT DEFAULT '',
  resolved_date TEXT DEFAULT '',
  resolved_by TEXT DEFAULT '',
  status TEXT NOT NULL DEFAULT 'registered',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_defect_vendor ON defects(vendor_name)");
db.exec("CREATE INDEX IF NOT EXISTS idx_defect_product ON defects(product_code)");
db.exec("CREATE INDEX IF NOT EXISTS idx_defect_status ON defects(status)");
db.exec("CREATE INDEX IF NOT EXISTS idx_defect_date ON defects(defect_date)");

db.exec(`CREATE TABLE IF NOT EXISTS defect_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  defect_id INTEGER NOT NULL,
  defect_number TEXT DEFAULT '',
  action TEXT NOT NULL,
  from_status TEXT DEFAULT '',
  to_status TEXT DEFAULT '',
  actor TEXT DEFAULT '',
  details TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  FOREIGN KEY (defect_id) REFERENCES defects(id)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_defect_log_defect ON defect_logs(defect_id)");

// ── 생산요청 관리 ──
db.exec(`CREATE TABLE IF NOT EXISTS production_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_number TEXT NOT NULL UNIQUE,
  product_type TEXT NOT NULL DEFAULT '',
  product_name TEXT NOT NULL DEFAULT '',
  brand TEXT DEFAULT '',
  requested_qty INTEGER NOT NULL DEFAULT 0,
  spec_json TEXT DEFAULT '{}',
  requester TEXT DEFAULT '',
  designer TEXT DEFAULT '',
  printer_vendor TEXT DEFAULT '',
  post_vendor TEXT DEFAULT '',
  status TEXT NOT NULL DEFAULT 'requested',
  priority TEXT DEFAULT 'normal',
  due_date TEXT DEFAULT '',
  design_confirmed_at TEXT DEFAULT '',
  data_confirmed_at TEXT DEFAULT '',
  production_started_at TEXT DEFAULT '',
  completed_at TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_pr_status ON production_requests(status)");
db.exec("CREATE INDEX IF NOT EXISTS idx_pr_type ON production_requests(product_type)");
db.exec("CREATE INDEX IF NOT EXISTS idx_pr_date ON production_requests(created_at)");

db.exec(`CREATE TABLE IF NOT EXISTS production_request_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL,
  request_number TEXT DEFAULT '',
  action TEXT NOT NULL,
  from_status TEXT DEFAULT '',
  to_status TEXT DEFAULT '',
  actor TEXT DEFAULT '',
  details TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  FOREIGN KEY (request_id) REFERENCES production_requests(id)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_pr_log_req ON production_request_logs(request_id)");

// ── 제품 스펙 마스터 ──
db.exec(`CREATE TABLE IF NOT EXISTS product_spec_master (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_type TEXT NOT NULL DEFAULT '',
  spec_name TEXT NOT NULL DEFAULT '',
  brand TEXT DEFAULT '',
  paper_cover TEXT DEFAULT '',
  paper_inner TEXT DEFAULT '',
  print_method TEXT DEFAULT '',
  print_color TEXT DEFAULT '',
  binding TEXT DEFAULT '',
  post_process TEXT DEFAULT '',
  size TEXT DEFAULT '',
  pages INTEGER DEFAULT 0,
  weight TEXT DEFAULT '',
  extras TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  is_template INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_spec_type ON product_spec_master(product_type)");
db.exec("CREATE INDEX IF NOT EXISTS idx_spec_template ON product_spec_master(is_template)");

// ── 수입검사 (Incoming Inspection) ──
db.exec(`CREATE TABLE IF NOT EXISTS incoming_inspections (
  inspection_id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER,
  po_number TEXT DEFAULT '',
  vendor_name TEXT DEFAULT '',
  inspection_date TEXT DEFAULT (date('now','localtime')),
  inspector TEXT DEFAULT '',
  result TEXT NOT NULL DEFAULT 'pending',
  items_json TEXT DEFAULT '[]',
  total_qty INTEGER DEFAULT 0,
  pass_qty INTEGER DEFAULT 0,
  fail_qty INTEGER DEFAULT 0,
  pass_rate REAL DEFAULT 0,
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_insp_po ON incoming_inspections(po_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_insp_date ON incoming_inspections(inspection_date)");

// ── 부적합 처리 (Non-Conformance Report) ──
db.exec(`CREATE TABLE IF NOT EXISTS ncr (
  ncr_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ncr_number TEXT NOT NULL UNIQUE,
  defect_id INTEGER,
  inspection_id INTEGER,
  po_id INTEGER,
  vendor_name TEXT DEFAULT '',
  product_code TEXT DEFAULT '',
  ncr_type TEXT DEFAULT 'incoming',
  description TEXT DEFAULT '',
  root_cause TEXT DEFAULT '',
  corrective_action TEXT DEFAULT '',
  preventive_action TEXT DEFAULT '',
  status TEXT NOT NULL DEFAULT 'open',
  severity TEXT DEFAULT 'minor',
  responsible TEXT DEFAULT '',
  due_date TEXT DEFAULT '',
  closed_at TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_ncr_status ON ncr(status)");
db.exec("CREATE INDEX IF NOT EXISTS idx_ncr_vendor ON ncr(vendor_name)");

db.exec(`CREATE TABLE IF NOT EXISTS ncr_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ncr_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  from_status TEXT DEFAULT '',
  to_status TEXT DEFAULT '',
  actor TEXT DEFAULT '',
  details TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 협력사 평가 (Vendor Scorecard) ──
db.exec(`CREATE TABLE IF NOT EXISTS vendor_scorecard (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  eval_month TEXT NOT NULL,
  delivery_score REAL DEFAULT 0,
  quality_score REAL DEFAULT 0,
  price_score REAL DEFAULT 0,
  total_score REAL DEFAULT 0,
  total_po INTEGER DEFAULT 0,
  ontime_po INTEGER DEFAULT 0,
  total_defects INTEGER DEFAULT 0,
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(vendor_name, eval_month)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_vs_vendor ON vendor_scorecard(vendor_name)");

// ── 중국 상품별 단가 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS china_price_tiers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  product_type TEXT DEFAULT 'Card',
  qty_tier INTEGER NOT NULL,
  unit_price REAL NOT NULL,
  currency TEXT DEFAULT 'CNY',
  effective_date TEXT DEFAULT '2025-05-01',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(product_code, qty_tier)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_cpt_product ON china_price_tiers(product_code)");

// ── 인증/권한 테이블 (S1) ─────────────────────────────────────────
db.exec(`CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL DEFAULT '',
  display_name TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'viewer',
  email TEXT DEFAULT '',
  google_id TEXT DEFAULT '',
  profile_picture TEXT DEFAULT '',
  is_active INTEGER DEFAULT 1,
  last_login TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
// google_id, profile_picture, permissions 컬럼 마이그레이션
try { db.exec("ALTER TABLE users ADD COLUMN google_id TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE users ADD COLUMN profile_picture TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE users ADD COLUMN permissions TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE users ADD COLUMN favorites TEXT DEFAULT '[]'"); } catch {}
try { db.exec("ALTER TABLE vendors ADD COLUMN email_cc TEXT DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE product_post_vendor ADD COLUMN step_order INTEGER DEFAULT 1"); } catch {}
try { db.exec("ALTER TABLE po_header ADD COLUMN process_step INTEGER DEFAULT 0"); } catch {}
try { db.exec("ALTER TABLE po_header ADD COLUMN parent_po_id INTEGER DEFAULT NULL"); } catch {}
try { db.exec("ALTER TABLE po_header ADD COLUMN process_chain TEXT DEFAULT ''"); } catch {}

// ── po_header.origin 빈 값 backfill (품목 → products.origin 매핑) ──
try {
  const emptyOriginPOs = db.prepare("SELECT po_id FROM po_header WHERE origin='' OR origin IS NULL").all();
  let backfilled = 0;
  for (const po of emptyOriginPOs) {
    const item = db.prepare("SELECT pi.product_code FROM po_items pi WHERE pi.po_id=? LIMIT 1").get(po.po_id);
    if (item) {
      const prod = db.prepare("SELECT origin FROM products WHERE product_code=?").get(item.product_code);
      if (prod && prod.origin) {
        db.prepare("UPDATE po_header SET origin=? WHERE po_id=?").run(prod.origin, po.po_id);
        backfilled++;
      }
    }
  }
  if (backfilled > 0) console.log(`[origin backfill] ${backfilled}/${emptyOriginPOs.length} PO에 origin 설정 완료`);
} catch (e) { console.warn('origin backfill 실패:', e.message); }

// ── 업무관리 ──────────────────────────────────────────────────────
db.exec(`CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_number TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  description TEXT DEFAULT '',
  category TEXT DEFAULT '기타',
  status TEXT NOT NULL DEFAULT 'todo',
  priority TEXT NOT NULL DEFAULT 'normal',
  assignee TEXT DEFAULT '',
  due_date TEXT DEFAULT '',
  start_date TEXT DEFAULT '',
  completed_at TEXT DEFAULT '',
  related_po TEXT DEFAULT '',
  related_vendor TEXT DEFAULT '',
  tags TEXT DEFAULT '',
  created_by TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status)");
db.exec("CREATE INDEX IF NOT EXISTS idx_task_due ON tasks(due_date)");
db.exec("CREATE INDEX IF NOT EXISTS idx_task_assignee ON tasks(assignee)");
db.exec("CREATE INDEX IF NOT EXISTS idx_task_category ON tasks(category)");

db.exec(`CREATE TABLE IF NOT EXISTS task_comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id INTEGER NOT NULL,
  author TEXT DEFAULT '',
  content TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  FOREIGN KEY (task_id) REFERENCES tasks(id)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_tc_task ON task_comments(task_id)");

// ── 업무 단계 (Workflow Steps) ──
db.exec(`CREATE TABLE IF NOT EXISTS task_steps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id INTEGER NOT NULL,
  step_order INTEGER NOT NULL DEFAULT 0,
  step_name TEXT NOT NULL,
  step_type TEXT NOT NULL DEFAULT 'text',
  value TEXT DEFAULT '',
  is_done INTEGER DEFAULT 0,
  done_at TEXT DEFAULT '',
  note TEXT DEFAULT '',
  FOREIGN KEY (task_id) REFERENCES tasks(id)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_ts_task ON task_steps(task_id)");

// ── tasks 테이블에 template_id 컬럼 추가 ──
try { db.exec("ALTER TABLE tasks ADD COLUMN template_id TEXT DEFAULT ''"); } catch(_) {}

// ── 부속품 마스터 ──
db.exec(`CREATE TABLE IF NOT EXISTS accessories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  acc_code TEXT DEFAULT '',
  acc_name TEXT NOT NULL,
  acc_type TEXT DEFAULT '기타',
  current_stock INTEGER DEFAULT 0,
  min_stock INTEGER DEFAULT 0,
  unit TEXT DEFAULT '개',
  vendor TEXT DEFAULT '',
  memo TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

try { db.exec("ALTER TABLE accessories ADD COLUMN origin TEXT DEFAULT '한국'"); } catch(_) {}

// ── 제품↔부속품 연결 ──
db.exec(`CREATE TABLE IF NOT EXISTS product_accessories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  acc_id INTEGER NOT NULL,
  qty_per INTEGER DEFAULT 1,
  UNIQUE(product_code, acc_id)
)`);

db.exec(`CREATE TABLE IF NOT EXISTS po_drafts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_number TEXT NOT NULL,
  po_date TEXT,
  due_date TEXT,
  vendor_id INTEGER DEFAULT 0,
  vendor_name TEXT DEFAULT '',
  vendor_contact TEXT DEFAULT '',
  vendor_phone TEXT DEFAULT '',
  vendor_email TEXT DEFAULT '',
  issuer_name TEXT DEFAULT '바른컴퍼니',
  issuer_contact TEXT DEFAULT '',
  issuer_phone TEXT DEFAULT '',
  issuer_email TEXT DEFAULT '',
  payment_terms TEXT DEFAULT '',
  remark TEXT DEFAULT '',
  items TEXT DEFAULT '[]',
  total_supply INTEGER DEFAULT 0,
  total_tax INTEGER DEFAULT 0,
  total_amount INTEGER DEFAULT 0,
  status TEXT DEFAULT 'sent',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

try { db.exec("ALTER TABLE vendor_notes ADD COLUMN status TEXT DEFAULT 'open'"); } catch(_) {}

db.exec(`CREATE TABLE IF NOT EXISTS note_comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  note_id INTEGER NOT NULL,
  author TEXT DEFAULT '',
  content TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_nc_note ON note_comments(note_id)");

db.exec(`CREATE TABLE IF NOT EXISTS reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  subtitle TEXT DEFAULT '',
  report_type TEXT DEFAULT 'general',
  content TEXT DEFAULT '{}',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 워크플로우 템플릿 (하드코딩, DB 필요 없음) ──
const TASK_TEMPLATES = {
  'cn-order': {
    name: '🇨🇳 중국 발주 프로세스',
    category: '발주',
    steps: [
      { name: 'ERP 발주서 작성', type: 'text', placeholder: 'PO번호 입력 (예: PO-260001)' },
      { name: '발주서 이메일 발송', type: 'checkbox', placeholder: '' },
      { name: '업체 납기 확인', type: 'date', placeholder: '업체 확인 납기일' },
      { name: '예상 선적일', type: 'date', placeholder: '예상 선적일' },
      { name: 'BL번호 / 선적 완료', type: 'text', placeholder: 'BL번호 입력' },
      { name: '예상 통관일', type: 'date', placeholder: '예상 통관일' },
      { name: '실제 통관일', type: 'date', placeholder: '실제 통관 완료일' },
      { name: '입고 처리 완료', type: 'checkbox', placeholder: '' },
    ]
  },
  'kr-order': {
    name: '🇰🇷 한국 발주 프로세스',
    category: '발주',
    steps: [
      { name: 'ERP 발주서 작성', type: 'text', placeholder: 'PO번호 입력' },
      { name: '발주서 이메일 발송', type: 'checkbox', placeholder: '' },
      { name: '업체 납기 확인', type: 'date', placeholder: '납기일' },
      { name: '후공정 완료', type: 'date', placeholder: '후공정 완료일' },
      { name: '입고 예정일', type: 'date', placeholder: '입고 예정일' },
      { name: '실제 입고일', type: 'date', placeholder: '실제 입고 완료일' },
      { name: '검수 완료', type: 'checkbox', placeholder: '' },
    ]
  },
  'gift-order': {
    name: '🎁 더기프트 발주 프로세스',
    category: '발주',
    steps: [
      { name: 'ERP 발주서 작성', type: 'text', placeholder: 'PO번호 입력' },
      { name: '발주서 이메일 발송', type: 'checkbox', placeholder: '' },
      { name: '업체 납기 확인', type: 'date', placeholder: '납기일' },
      { name: '입고 예정일', type: 'date', placeholder: '입고 예정일' },
      { name: '실제 입고일', type: 'date', placeholder: '실제 입고 완료일' },
      { name: '검수 완료', type: 'checkbox', placeholder: '' },
    ]
  },
  'vendor-issue': {
    name: '🔴 거래처 이슈 처리',
    category: '거래처',
    steps: [
      { name: '이슈 내용 확인', type: 'text', placeholder: '이슈 요약' },
      { name: '거래처 연락', type: 'date', placeholder: '연락일' },
      { name: '거래처 답변 수신', type: 'text', placeholder: '답변 내용 요약' },
      { name: '내부 검토 완료', type: 'checkbox', placeholder: '' },
      { name: '처리 완료', type: 'date', placeholder: '처리 완료일' },
    ]
  },
  'custom': {
    name: '✏️ 직접 단계 입력',
    category: '',
    steps: []
  }
};

// password_hash NOT NULL 제거 불가능하므로 기본값 허용
try { db.exec("UPDATE users SET password_hash = '' WHERE password_hash IS NULL"); } catch {}

db.exec(`CREATE TABLE IF NOT EXISTS audit_log (
  log_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  username TEXT DEFAULT '',
  action TEXT NOT NULL,
  resource TEXT DEFAULT '',
  resource_id TEXT DEFAULT '',
  details TEXT DEFAULT '',
  ip_address TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)");

db.exec(`CREATE TABLE IF NOT EXISTS error_logs (
  error_id INTEGER PRIMARY KEY AUTOINCREMENT,
  level TEXT DEFAULT 'error',
  message TEXT NOT NULL,
  stack TEXT DEFAULT '',
  url TEXT DEFAULT '',
  method TEXT DEFAULT '',
  user_id INTEGER,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_error_created ON error_logs(created_at)");

// ── 더기프트 세트 생산재고 ─────────────────────────────────────────
db.exec(`CREATE TABLE IF NOT EXISTS gift_sets (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  set_code      TEXT NOT NULL UNIQUE,
  set_name      TEXT NOT NULL,
  description   TEXT DEFAULT '',
  base_stock    INTEGER DEFAULT 0,
  current_stock INTEGER DEFAULT 0,
  status        TEXT DEFAULT 'active',
  created_at    TEXT DEFAULT (datetime('now','localtime')),
  updated_at    TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_gs_code ON gift_sets(set_code)");

// xerp_code 컬럼 추가 (출고재고 XERP 연동용)
try { db.exec("ALTER TABLE gift_sets ADD COLUMN xerp_code TEXT DEFAULT ''"); } catch(e) { /* 이미 존재 */ }

db.exec(`CREATE TABLE IF NOT EXISTS gift_set_bom (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  set_id        INTEGER NOT NULL,
  item_type     TEXT NOT NULL,
  item_code     TEXT NOT NULL,
  item_name     TEXT DEFAULT '',
  qty_per       REAL DEFAULT 1,
  unit          TEXT DEFAULT 'EA',
  UNIQUE(set_id, item_type, item_code)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_gsb_set ON gift_set_bom(set_id)");

db.exec(`CREATE TABLE IF NOT EXISTS gift_set_transactions (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  set_id     INTEGER NOT NULL,
  tx_type    TEXT NOT NULL,
  qty        INTEGER NOT NULL,
  operator   TEXT DEFAULT '',
  memo       TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_gst_set ON gift_set_transactions(set_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_gst_created ON gift_set_transactions(created_at)");

// ── 매출관리 캐시 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS sales_daily_cache (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  sale_date       TEXT NOT NULL,
  source          TEXT NOT NULL,
  channel         TEXT NOT NULL DEFAULT '',
  total_orders    INTEGER DEFAULT 0,
  total_qty       INTEGER DEFAULT 0,
  total_sales     INTEGER DEFAULT 0,
  total_supply    INTEGER DEFAULT 0,
  total_vat       INTEGER DEFAULT 0,
  total_fee       INTEGER DEFAULT 0,
  cached_at       TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(sale_date, source, channel)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_sdc_date ON sales_daily_cache(sale_date)");
db.exec("CREATE INDEX IF NOT EXISTS idx_sdc_source ON sales_daily_cache(source)");

db.exec(`CREATE TABLE IF NOT EXISTS sales_monthly_cache (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  sale_month      TEXT NOT NULL,
  source          TEXT NOT NULL,
  channel         TEXT NOT NULL DEFAULT '',
  total_orders    INTEGER DEFAULT 0,
  total_qty       INTEGER DEFAULT 0,
  total_sales     INTEGER DEFAULT 0,
  total_supply    INTEGER DEFAULT 0,
  total_vat       INTEGER DEFAULT 0,
  total_fee       INTEGER DEFAULT 0,
  cached_at       TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(sale_month, source, channel)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_smc_month ON sales_monthly_cache(sale_month)");

db.exec(`CREATE TABLE IF NOT EXISTS sales_product_cache (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  sale_month      TEXT NOT NULL,
  source          TEXT NOT NULL,
  product_code    TEXT NOT NULL,
  product_name    TEXT DEFAULT '',
  brand           TEXT DEFAULT '',
  total_orders    INTEGER DEFAULT 0,
  total_qty       INTEGER DEFAULT 0,
  total_sales     INTEGER DEFAULT 0,
  cached_at       TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(sale_month, source, product_code)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_spc_month ON sales_product_cache(sale_month)");
db.exec("CREATE INDEX IF NOT EXISTS idx_spc_product ON sales_product_cache(product_code)");

db.exec(`CREATE TABLE IF NOT EXISTS sales_settings (
  key             TEXT PRIMARY KEY,
  value           TEXT NOT NULL,
  updated_at      TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("INSERT OR IGNORE INTO sales_settings (key, value) VALUES ('default_range_days', '30')");
db.exec("INSERT OR IGNORE INTO sales_settings (key, value) VALUES ('cache_ttl_minutes', '30')");

// ── 다중 창고 재고 관리 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS warehouses (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  code        TEXT NOT NULL UNIQUE,
  name        TEXT NOT NULL,
  location    TEXT DEFAULT '',
  description TEXT DEFAULT '',
  is_default  INTEGER DEFAULT 0,
  status      TEXT DEFAULT 'active',
  created_at  TEXT DEFAULT (datetime('now','localtime')),
  updated_at  TEXT DEFAULT (datetime('now','localtime'))
)`);

db.exec(`CREATE TABLE IF NOT EXISTS warehouse_inventory (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  warehouse_id  INTEGER NOT NULL,
  product_code  TEXT NOT NULL,
  product_name  TEXT DEFAULT '',
  quantity      INTEGER DEFAULT 0,
  memo          TEXT DEFAULT '',
  updated_at    TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(warehouse_id, product_code)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_wi_wh ON warehouse_inventory(warehouse_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_wi_pc ON warehouse_inventory(product_code)");

db.exec(`CREATE TABLE IF NOT EXISTS warehouse_transfers (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  from_warehouse  INTEGER NOT NULL,
  to_warehouse    INTEGER NOT NULL,
  product_code    TEXT NOT NULL,
  product_name    TEXT DEFAULT '',
  quantity        INTEGER NOT NULL,
  operator        TEXT DEFAULT '',
  memo            TEXT DEFAULT '',
  created_at      TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_wt_from ON warehouse_transfers(from_warehouse)");
db.exec("CREATE INDEX IF NOT EXISTS idx_wt_to ON warehouse_transfers(to_warehouse)");
db.exec("CREATE INDEX IF NOT EXISTS idx_wt_created ON warehouse_transfers(created_at)");

db.exec(`CREATE TABLE IF NOT EXISTS warehouse_adjustments (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  warehouse_id  INTEGER NOT NULL,
  product_code  TEXT NOT NULL,
  product_name  TEXT DEFAULT '',
  adj_type      TEXT NOT NULL,
  before_qty    INTEGER DEFAULT 0,
  after_qty     INTEGER DEFAULT 0,
  diff_qty      INTEGER DEFAULT 0,
  reason        TEXT DEFAULT '',
  operator      TEXT DEFAULT '',
  created_at    TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_wa_wh ON warehouse_adjustments(warehouse_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_wa_created ON warehouse_adjustments(created_at)");

// 기본 창고 초기화 (처음 실행 시)
const whCount = db.prepare("SELECT COUNT(*) as cnt FROM warehouses").get();
if (whCount.cnt === 0) {
  const insertWh = db.prepare("INSERT INTO warehouses (code, name, location, description, is_default) VALUES (?, ?, ?, ?, ?)");
  insertWh.run('WH-HQ', '본사창고', '본사', 'XERP 연동 기본 창고', 1);
  insertWh.run('WH-02', '제2창고', '', '', 0);
  insertWh.run('WH-03', '제3창고', '', '', 0);
  insertWh.run('WH-04', '제4창고', '', '', 0);
  console.log('[DB] 기본 창고 4개 초기화 완료');
}

// ── 공지/게시판 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS notices (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  title       TEXT NOT NULL,
  content     TEXT DEFAULT '',
  category    TEXT DEFAULT 'notice',
  is_popup    INTEGER DEFAULT 0,
  popup_start TEXT DEFAULT NULL,
  popup_end   TEXT DEFAULT NULL,
  is_pinned   INTEGER DEFAULT 0,
  author_id   INTEGER,
  author_name TEXT DEFAULT '',
  view_count  INTEGER DEFAULT 0,
  status      TEXT DEFAULT 'active',
  created_at  TEXT DEFAULT (datetime('now','localtime')),
  updated_at  TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_notices_status ON notices(status, created_at)");
db.exec("CREATE INDEX IF NOT EXISTS idx_notices_popup ON notices(is_popup, popup_start, popup_end)");

db.exec(`CREATE TABLE IF NOT EXISTS notice_reads (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  notice_id  INTEGER NOT NULL,
  user_id    INTEGER NOT NULL,
  read_at    TEXT DEFAULT (datetime('now','localtime')),
  popup_dismissed INTEGER DEFAULT 0
)`);
db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_nr_unique ON notice_reads(notice_id, user_id)");

// ── 회계 모듈 SQLite 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS gl_account_map (
  acc_code    TEXT PRIMARY KEY,
  acc_name    TEXT NOT NULL DEFAULT '',
  acc_type    TEXT NOT NULL DEFAULT '',
  acc_group   TEXT DEFAULT '',
  parent_code TEXT DEFAULT '',
  depth       INTEGER DEFAULT 0,
  sort_order  INTEGER DEFAULT 0,
  is_active   INTEGER DEFAULT 1,
  updated_at  TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE TABLE IF NOT EXISTS gl_balance_cache (
  acc_code   TEXT NOT NULL,
  year_month TEXT NOT NULL,
  opening_dr REAL DEFAULT 0, opening_cr REAL DEFAULT 0,
  period_dr  REAL DEFAULT 0, period_cr  REAL DEFAULT 0,
  closing_dr REAL DEFAULT 0, closing_cr REAL DEFAULT 0,
  cached_at  TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(acc_code, year_month)
)`);
db.exec(`CREATE TABLE IF NOT EXISTS accounting_settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL DEFAULT '',
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE TABLE IF NOT EXISTS cs_code_cache (
  cs_code   TEXT PRIMARY KEY,
  cs_name   TEXT DEFAULT '',
  cached_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// 계정과목 한글명 매핑 (XERP 주요 계정)
const KNOWN_ACCOUNTS = {
  '11110101':'현금','11110151':'보통예금','11110152':'보통예금2','11110153':'보통예금3',
  '11120101':'받을어음','11125101':'외상매출금','11125102':'외상매출금(수출)',
  '11130101':'단기대여금','11135101':'미수금','11135102':'미수수익',
  '11140101':'선급금','11145101':'선급비용','11150101':'재고자산',
  '11151101':'상품','11152101':'제품','11153101':'재공품','11154101':'원재료','11155101':'저장품',
  '11160101':'부가세대급금','11199101':'기타유동자산',
  '12110101':'토지','12120101':'건물','12125101':'건물감가상각누계액',
  '12130101':'기계장치','12135101':'기계장치감가상각누계액',
  '12140101':'차량운반구','12145101':'차량운반구감가상각누계액',
  '12150101':'비품','12155101':'비품감가상각누계액',
  '12160101':'건설중인자산','12170101':'무형자산','12175101':'영업권',
  '12180101':'장기투자증권','12190101':'장기대여금','12199101':'기타비유동자산',
  '21110101':'외상매입금','21120101':'지급어음','21130101':'단기차입금',
  '21140101':'미지급금','21145101':'미지급비용','21150101':'선수금',
  '21155101':'예수금','21160101':'부가세예수금','21170101':'유동성장기부채',
  '21199101':'기타유동부채','21210101':'미지급법인세',
  '21310101':'선수금(카드)','21320101':'선수금(현금)','21330101':'선수금',
  '22110101':'장기차입금','22120101':'사채','22130101':'퇴직급여충당부채',
  '22199101':'기타비유동부채',
  '31110101':'자본금','31120101':'자본잉여금','31130101':'이익잉여금',
  '31140101':'자본조정','31150101':'기타포괄손익누계액',
  '41110101':'상품매출','41110102':'상품매출(수출)','41120101':'제품매출',
  '51110101':'상품매출원가','51120101':'제품매출원가','51130101':'원재료비',
  '51140101':'노무비','51150101':'제조경비',
  '61110101':'상품매출','61110121':'상품매출(온라인)','61115101':'제품매출',
  '61120101':'임대수입','61130101':'수수료수입',
  '64110101':'이자수익','64120101':'배당금수익','64130101':'외환차익',
  '64140101':'외화환산이익','64150101':'유형자산처분이익','64199101':'잡이익',
  '71110101':'급여','71120101':'퇴직급여','71130101':'복리후생비',
  '71140101':'여비교통비','71150101':'접대비','71160101':'통신비',
  '71170101':'수도광열비','71180101':'세금과공과','71190101':'감가상각비',
  '71200101':'임차료','71210101':'수선비','71220101':'보험료',
  '71230101':'차량유지비','71240101':'운반비','71250101':'교육훈련비',
  '71260101':'도서인쇄비','71270101':'소모품비','71280101':'지급수수료',
  '71290101':'광고선전비','71300101':'대손상각비','71310101':'무형자산상각비',
  '71399101':'기타판관비',
  '73110101':'포장비','73120101':'운반비(판매)','73130101':'판매수수료',
  '73140101':'판매촉진비','73150101':'판매보증비',
  '73183104':'판매수수료',
  '74110101':'이자비용','74120101':'외환차손','74130101':'외화환산손실',
  '74140101':'유형자산처분손실','74199101':'잡손실',
  '81110101':'법인세비용'
};

function classifyAccount(code) {
  const c = code.replace(/\s/g, '');
  const first = c.charAt(0);
  const first2 = c.substring(0, 2);
  let acc_type = '', acc_group = '', sort_order = 0;
  if (first === '1') {
    acc_type = 'asset';
    if (first2 === '11') { acc_group = '유동자산'; sort_order = 1000; }
    else { acc_group = '비유동자산'; sort_order = 2000; }
  } else if (first === '2') {
    acc_type = 'liability';
    if (first2 === '21') { acc_group = '유동부채'; sort_order = 3000; }
    else { acc_group = '비유동부채'; sort_order = 4000; }
  } else if (first === '3') {
    acc_type = 'equity'; acc_group = '자본'; sort_order = 5000;
  } else if (first === '4' || first2 === '61') {
    acc_type = 'revenue'; acc_group = '매출'; sort_order = 6000;
  } else if (first === '5' || first2 === '51') {
    acc_type = 'expense'; acc_group = '매출원가'; sort_order = 7000;
  } else if (first2 === '64') {
    acc_type = 'revenue'; acc_group = '영업외수익'; sort_order = 6500;
  } else if (first === '6') {
    acc_type = 'revenue'; acc_group = '매출'; sort_order = 6000;
  } else if (first2 === '71' || first2 === '72' || first2 === '73') {
    acc_type = 'expense'; acc_group = '판매비와관리비'; sort_order = 8000;
  } else if (first2 === '74') {
    acc_type = 'expense'; acc_group = '영업외비용'; sort_order = 8500;
  } else if (first === '7') {
    acc_type = 'expense'; acc_group = '비용'; sort_order = 8000;
  } else if (first === '8') {
    acc_type = 'expense'; acc_group = '법인세비용'; sort_order = 9000;
  } else {
    acc_type = 'other'; acc_group = '기타'; sort_order = 9999;
  }
  const parent_code = c.substring(0, 5);
  const depth = c.length <= 5 ? 1 : (c.length <= 6 ? 2 : 3);
  const name = KNOWN_ACCOUNTS[c] || '';
  return { acc_type, acc_group, parent_code, depth, sort_order, acc_name: name };
}

// ── 작업지시 SQLite 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS work_orders (
  wo_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  wo_number   TEXT UNIQUE NOT NULL,
  request_id  INTEGER,
  product_code TEXT,
  product_name TEXT DEFAULT '',
  brand       TEXT DEFAULT '',
  ordered_qty INTEGER DEFAULT 0,
  produced_qty INTEGER DEFAULT 0,
  defect_qty  INTEGER DEFAULT 0,
  status      TEXT DEFAULT 'planned',
  priority    TEXT DEFAULT 'normal',
  start_date  TEXT,
  due_date    TEXT,
  completed_date TEXT,
  printer_vendor TEXT DEFAULT '',
  post_vendor TEXT DEFAULT '',
  paper_type  TEXT DEFAULT '',
  notes       TEXT DEFAULT '',
  cost_material REAL DEFAULT 0,
  cost_labor   REAL DEFAULT 0,
  cost_overhead REAL DEFAULT 0,
  cost_total   REAL DEFAULT 0,
  created_by  TEXT DEFAULT '',
  created_at  TEXT DEFAULT (datetime('now','localtime')),
  updated_at  TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE TABLE IF NOT EXISTS work_order_logs (
  log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  wo_id       INTEGER NOT NULL,
  wo_number   TEXT,
  action      TEXT,
  from_status TEXT,
  to_status   TEXT,
  qty_change  INTEGER DEFAULT 0,
  actor       TEXT DEFAULT '',
  details     TEXT DEFAULT '',
  created_at  TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 홈택스 세금계산서 업로드 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS hometax_invoices (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  invoice_no    TEXT DEFAULT '',
  invoice_date  TEXT NOT NULL DEFAULT '',
  ar_ap         TEXT DEFAULT 'AP',
  cs_name       TEXT DEFAULT '',
  cs_reg_no     TEXT DEFAULT '',
  supply_amt    REAL DEFAULT 0,
  vat_amt       REAL DEFAULT 0,
  total_amt     REAL DEFAULT 0,
  item_name     TEXT DEFAULT '',
  remark        TEXT DEFAULT '',
  electronic    TEXT DEFAULT 'Y',
  source        TEXT DEFAULT 'hometax',
  uploaded_by   TEXT DEFAULT '',
  uploaded_at   TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(invoice_no, invoice_date, cs_reg_no, supply_amt)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_hometax_date ON hometax_invoices(invoice_date)");
db.exec("CREATE INDEX IF NOT EXISTS idx_hometax_arap ON hometax_invoices(ar_ap, invoice_date)");

// ── 로트/배치 추적 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS batch_master (
  batch_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_number    TEXT NOT NULL DEFAULT '',
  product_code    TEXT NOT NULL DEFAULT '',
  product_name    TEXT DEFAULT '',
  vendor_name     TEXT DEFAULT '',
  vendor_lot      TEXT DEFAULT '',
  received_date   TEXT DEFAULT '',
  po_number       TEXT DEFAULT '',
  received_qty    REAL DEFAULT 0,
  current_qty     REAL DEFAULT 0,
  quality_status  TEXT DEFAULT 'GOOD',
  warehouse       TEXT DEFAULT '',
  mfg_date        TEXT DEFAULT '',
  exp_date        TEXT DEFAULT '',
  notes           TEXT DEFAULT '',
  created_by      TEXT DEFAULT '',
  created_at      TEXT DEFAULT (datetime('now','localtime')),
  updated_at      TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(batch_number, product_code)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_batch_product ON batch_master(product_code)");
db.exec("CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_master(quality_status)");
db.exec("CREATE INDEX IF NOT EXISTS idx_batch_date ON batch_master(received_date)");
db.exec("CREATE INDEX IF NOT EXISTS idx_batch_warehouse ON batch_master(warehouse)");

db.exec(`CREATE TABLE IF NOT EXISTS batch_transactions (
  txn_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id        INTEGER NOT NULL,
  batch_number    TEXT DEFAULT '',
  txn_type        TEXT NOT NULL DEFAULT '',
  txn_date        TEXT DEFAULT (datetime('now','localtime')),
  from_warehouse  TEXT DEFAULT '',
  to_warehouse    TEXT DEFAULT '',
  product_code    TEXT DEFAULT '',
  qty             REAL DEFAULT 0,
  qty_before      REAL DEFAULT 0,
  qty_after       REAL DEFAULT 0,
  reference_no    TEXT DEFAULT '',
  actor           TEXT DEFAULT '',
  notes           TEXT DEFAULT '',
  created_at      TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_btxn_batch ON batch_transactions(batch_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_btxn_date ON batch_transactions(txn_date)");
db.exec("CREATE INDEX IF NOT EXISTS idx_btxn_type ON batch_transactions(txn_type)");

db.exec(`CREATE TABLE IF NOT EXISTS batch_inspections (
  insp_id         INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id        INTEGER NOT NULL,
  batch_number    TEXT DEFAULT '',
  insp_date       TEXT DEFAULT (datetime('now','localtime')),
  inspector       TEXT DEFAULT '',
  insp_type       TEXT DEFAULT 'RECEIVING',
  sample_size     INTEGER DEFAULT 0,
  defects_found   INTEGER DEFAULT 0,
  defect_desc     TEXT DEFAULT '',
  result          TEXT DEFAULT 'PASS',
  next_action     TEXT DEFAULT '',
  notes           TEXT DEFAULT '',
  created_by      TEXT DEFAULT '',
  created_at      TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_binsp_batch ON batch_inspections(batch_id)");
db.exec("CREATE INDEX IF NOT EXISTS idx_binsp_date ON batch_inspections(insp_date)");

// ── 원재료 단가 테이블 ──
db.exec(`CREATE TABLE IF NOT EXISTS material_prices (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code    TEXT NOT NULL DEFAULT '',
  product_name    TEXT DEFAULT '',
  spec            TEXT DEFAULT '',
  unit            TEXT DEFAULT 'R',
  vendor_name     TEXT DEFAULT '',
  list_price      REAL DEFAULT 0,
  apply_price     REAL DEFAULT 0,
  discount_rate   REAL DEFAULT 0,
  apply_month     TEXT DEFAULT '',
  notes           TEXT DEFAULT '',
  uploaded_by     TEXT DEFAULT '',
  created_at      TEXT DEFAULT (datetime('now','localtime')),
  updated_at      TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(product_code, vendor_name, apply_month)
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_matprice_code ON material_prices(product_code)");
db.exec("CREATE INDEX IF NOT EXISTS idx_matprice_vendor ON material_prices(vendor_name)");
db.exec("CREATE INDEX IF NOT EXISTS idx_matprice_month ON material_prices(apply_month)");

// ── Phase 5: 알림센터 ──
db.exec(`CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY, user_id INTEGER, type TEXT NOT NULL DEFAULT 'system',
  title TEXT NOT NULL DEFAULT '', message TEXT DEFAULT '', link TEXT DEFAULT '',
  is_read INTEGER DEFAULT 0, is_email_sent INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec("CREATE INDEX IF NOT EXISTS idx_noti_user ON notifications(user_id, is_read)");

// ── Phase 1: 전자결재 ──
db.exec(`CREATE TABLE IF NOT EXISTS approvals (
  id INTEGER PRIMARY KEY, approval_no TEXT UNIQUE, doc_type TEXT NOT NULL DEFAULT 'general',
  doc_ref TEXT DEFAULT '', title TEXT NOT NULL DEFAULT '', content TEXT DEFAULT '',
  amount REAL DEFAULT 0, status TEXT DEFAULT 'draft',
  requester_id INTEGER, requester_name TEXT DEFAULT '',
  current_step INTEGER DEFAULT 1, total_steps INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
db.exec(`CREATE TABLE IF NOT EXISTS approval_lines (
  id INTEGER PRIMARY KEY, approval_id INTEGER NOT NULL, step_order INTEGER NOT NULL,
  approver_id INTEGER, approver_name TEXT DEFAULT '',
  role TEXT DEFAULT 'approver', status TEXT DEFAULT 'pending',
  comment TEXT DEFAULT '', acted_at TEXT,
  UNIQUE(approval_id, step_order)
)`);

// ── 동기화 메타 ──
db.exec(`CREATE TABLE IF NOT EXISTS sync_meta (
  key TEXT PRIMARY KEY, last_sync TEXT, record_count INTEGER DEFAULT 0, status TEXT DEFAULT 'ok', message TEXT DEFAULT ''
)`);

// ── Phase 2: 수주관리 ──
db.exec(`CREATE TABLE IF NOT EXISTS sales_orders (
  id INTEGER PRIMARY KEY, order_no TEXT UNIQUE, order_type TEXT DEFAULT 'quote',
  status TEXT DEFAULT 'draft', customer_name TEXT NOT NULL DEFAULT '',
  customer_contact TEXT DEFAULT '', customer_tel TEXT DEFAULT '',
  order_date TEXT, delivery_date TEXT, shipped_date TEXT,
  total_qty INTEGER DEFAULT 0, total_amount REAL DEFAULT 0, tax_amount REAL DEFAULT 0,
  notes TEXT DEFAULT '', created_by TEXT DEFAULT '',
  source TEXT DEFAULT 'manual', external_id TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
// source 컬럼 마이그레이션 (기존 테이블에 컬럼 없을 수 있음)
try { db.exec("ALTER TABLE sales_orders ADD COLUMN source TEXT DEFAULT 'manual'"); } catch(_){}
try { db.exec("ALTER TABLE sales_orders ADD COLUMN external_id TEXT DEFAULT ''"); } catch(_){}
db.exec(`CREATE TABLE IF NOT EXISTS sales_order_items (
  id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL,
  product_code TEXT DEFAULT '', product_name TEXT DEFAULT '', spec TEXT DEFAULT '',
  unit_price REAL DEFAULT 0, qty INTEGER DEFAULT 0, amount REAL DEFAULT 0, notes TEXT DEFAULT ''
)`);

// ── Phase 4: 예산관리 ──
db.exec(`CREATE TABLE IF NOT EXISTS budgets (
  id INTEGER PRIMARY KEY, year TEXT NOT NULL, month TEXT NOT NULL,
  acc_code TEXT DEFAULT '', acc_name TEXT DEFAULT '',
  budget_type TEXT DEFAULT 'expense', budget_amount REAL DEFAULT 0,
  actual_amount REAL DEFAULT 0, notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(year, month, acc_code)
)`);
db.exec(`CREATE TABLE IF NOT EXISTS daily_cash (
  id INTEGER PRIMARY KEY, cash_date TEXT NOT NULL,
  acc_code TEXT DEFAULT '', acc_name TEXT DEFAULT '',
  inflow REAL DEFAULT 0, outflow REAL DEFAULT 0, balance REAL DEFAULT 0,
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(cash_date, acc_code)
)`);

// ── Phase 6: 생산실적 ──
db.exec(`CREATE TABLE IF NOT EXISTS work_order_results (
  id INTEGER PRIMARY KEY, work_order_id INTEGER NOT NULL,
  result_date TEXT NOT NULL, good_qty INTEGER DEFAULT 0, defect_qty INTEGER DEFAULT 0,
  worker_name TEXT DEFAULT '', work_hours REAL DEFAULT 0, notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// 알림 생성 헬퍼
function createNotification(userId, type, title, message, link) {
  try {
    db.prepare("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)").run(userId, type, title, message || '', link || '');
  } catch(e) { console.error('알림 생성 실패:', e.message); }
}

// JWT 시크릿 (서버 고유 — 최초 생성 후 파일 저장)
const JWT_SECRET_PATH = path.join(DATA_DIR, '.jwt_secret');
let JWT_SECRET;
try {
  JWT_SECRET = fs.readFileSync(JWT_SECRET_PATH, 'utf8').trim();
} catch {
  JWT_SECRET = crypto.randomBytes(32).toString('hex');
  fs.writeFileSync(JWT_SECRET_PATH, JWT_SECRET, 'utf8');
  console.log('JWT 시크릿 키 생성 완료');
}
const JWT_EXPIRES = '24h';

// 마스터 관리자 계정 (seungchan.back@barunn.net)
const masterEmail = 'seungchan.back@barunn.net';
const masterUser = db.prepare("SELECT user_id FROM users WHERE email = ?").get(masterEmail);
if (!masterUser) {
  const hash = bcrypt.hashSync('1234', 10);
  const oldAdmin = db.prepare("SELECT user_id FROM users WHERE username = 'admin'").get();
  if (oldAdmin) {
    db.prepare("UPDATE users SET username = ?, email = ?, password_hash = ?, display_name = ?, role = 'admin' WHERE user_id = ?")
      .run('seungchan.back', masterEmail, hash, '백승찬', oldAdmin.user_id);
  } else {
    db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?, ?, ?, ?, ?)")
      .run('seungchan.back', hash, '백승찬', 'admin', masterEmail);
  }
  console.log('✅ 마스터 계정: seungchan.back@barunn.net / 1234');
} else {
  // 이미 존재하면 admin 역할만 보장 (비밀번호는 유지)
  db.prepare("UPDATE users SET role = 'admin' WHERE user_id = ?").run(masterUser.user_id);
}

// JWT 유틸
function signToken(user) {
  return jwt.sign({ userId: user.user_id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: JWT_EXPIRES });
}

function verifyToken(token) {
  try { return jwt.verify(token, JWT_SECRET); }
  catch { return null; }
}

function extractToken(req) {
  const auth = req.headers['authorization'];
  if (auth && auth.startsWith('Bearer ')) return auth.slice(7);
  const cookie = req.headers['cookie'];
  if (cookie) {
    const m = cookie.match(/token=([^;]+)/);
    if (m) return m[1];
  }
  return null;
}

// 감사 로그 기록
function auditLog(userId, username, action, resource, resourceId, details, ip) {
  try {
    db.prepare("INSERT INTO audit_log (user_id, username, action, resource, resource_id, details, ip_address) VALUES (?,?,?,?,?,?,?)")
      .run(userId || 0, username || '', action, resource || '', String(resourceId || ''), details || '', ip || '');
  } catch (e) { console.error('감사 로그 기록 실패:', e.message); }
}

// 에러 로그 기록
function logError(level, message, stack, url, method, userId) {
  try {
    db.prepare("INSERT INTO error_logs (level, message, stack, url, method, user_id) VALUES (?,?,?,?,?,?)")
      .run(level, message, stack || '', url || '', method || '', userId || null);
  } catch (e) { console.error('에러 로그 기록 실패:', e.message); }
}

// 전체 페이지 목록 (관리자 권한 UI용)
const ALL_PAGES = [
  { id: 'dashboard', name: '홈', group: '기본' },
  // 구매
  { id: 'auto-order', name: '자동발주', group: '구매' },
  { id: 'create-po', name: '발주생성', group: '구매' },
  { id: 'po-list', name: '발주현황', group: '구매' },
  { id: 'po-mgmt', name: '발주서 관리', group: '구매' },
  { id: 'china-shipment', name: '중국선적', group: '구매' },
  { id: 'delivery-schedule', name: '입고일정', group: '구매' },
  { id: 'receipts', name: '입고관리', group: '구매' },
  { id: 'os-register', name: 'OS등록', group: '구매' },
  // 판매
  { id: 'sales', name: '통합매출', group: '판매' },
  { id: 'sales-barun', name: '바른손매출', group: '판매' },
  { id: 'sales-dd', name: 'DD매출', group: '판매' },
  { id: 'sales-gift', name: '더기프트매출', group: '판매' },
  { id: 'customer-orders', name: '고객주문', group: '판매' },
  { id: 'shipping', name: '배송추적', group: '판매' },
  // 재고
  { id: 'inventory', name: '재고현황', group: '재고' },
  { id: 'warehouse', name: '창고별 재고', group: '재고' },
  { id: 'shipments', name: '입출고 현황', group: '재고' },
  // 생산
  { id: 'production-req', name: '생산요청', group: '생산' },
  { id: 'production-stock', name: '생산재고', group: '생산' },
  { id: 'mrp', name: 'MRP', group: '생산' },
  // 회계
  { id: 'invoices', name: '거래명세서', group: '회계' },
  { id: 'mat-purchase', name: '원재료 매입', group: '회계' },
  { id: 'cost-mgmt', name: '원가관리', group: '회계' },
  { id: 'closing', name: '마감현황', group: '회계' },
  { id: 'chart-of-accounts', name: '계정과목', group: '회계' },
  { id: 'journal', name: '분개장', group: '회계' },
  { id: 'general-ledger', name: '총계정원장', group: '회계' },
  { id: 'trial-balance', name: '시산표', group: '회계' },
  { id: 'financial-statements', name: '재무제표', group: '회계' },
  { id: 'ar-ap', name: '채권/채무', group: '회계' },
  { id: 'tax-invoice', name: '세금계산서', group: '회계' },
  // 생산
  { id: 'work-order', name: '작업지시', group: '생산' },
  { id: 'lot-tracking', name: '로트추적', group: '생산' },
  // 기준정보
  { id: 'vendors', name: '거래처 관리', group: '기준정보' },
  { id: 'product-mgmt', name: '품목관리', group: '기준정보' },
  { id: 'bom', name: '제품공정', group: '기준정보' },
  { id: 'post-process', name: '후공정 단가', group: '기준정보' },
  // 경영분석
  { id: 'analytics', name: '대시보드', group: '경영분석' },
  { id: 'exec-dashboard', name: '경영대시보드', group: '경영분석' },
  { id: 'report', name: '보고서', group: '경영분석' },
  { id: 'defects', name: '불량관리', group: '경영분석' },
  // 판매
  { id: 'sales-order', name: '수주관리', group: '판매' },
  // 재고
  { id: 'lot-tracking', name: 'Lot추적', group: '재고' },
  // 회계
  { id: 'budget', name: '예산관리', group: '회계' },
  // 업무
  { id: 'approval', name: '전자결재', group: '업무' },
  { id: 'tasks', name: '업무관리', group: '업무' },
  { id: 'notes', name: '미팅일지', group: '업무' },
  { id: 'board', name: '공지/게시판', group: '업무' },
  // 시스템
  { id: 'settings', name: '설정', group: '시스템' },
  { id: 'user-mgmt', name: '사용자 관리', group: '시스템' },
  { id: 'audit-log', name: '감사로그', group: '시스템' },
  { id: 'notification', name: '알림센터', group: '시스템' },
];

// 역할 기본 권한 맵 (개별 permissions가 없을 때 fallback)
const ROLE_PERMISSIONS = {
  admin: ['*'],  // 모든 권한
  purchase: ['dashboard', 'inventory', 'warehouse', 'shipments', 'auto-order', 'create-po', 'po-list', 'os-register',
    'delivery-schedule', 'receipts', 'invoices', 'notes', 'product-mgmt', 'bom', 'mrp', 'post-process', 'defects',
    'closing', 'report', 'po-mgmt', 'china-shipment', 'mat-purchase', 'tasks', 'meeting-log', 'sales', 'sales-barun', 'sales-dd', 'sales-gift', 'cost-mgmt', 'board', 'audit-log', 'exec-dashboard', 'customer-orders', 'shipping',
    'chart-of-accounts', 'journal', 'general-ledger', 'trial-balance', 'financial-statements', 'ar-ap', 'tax-invoice', 'work-order', 'lot-tracking',
    'approval', 'sales-order', 'budget', 'notification'],
  production: ['dashboard', 'inventory', 'warehouse', 'shipments', 'production-req', 'mrp', 'bom', 'post-process', 'defects', 'product-mgmt', 'notes', 'production-stock', 'tasks', 'approval', 'lot-tracking'],
  viewer: ['dashboard', 'inventory', 'warehouse', 'shipments', 'po-list', 'notes', 'sales', 'sales-barun', 'sales-gift', 'cost-mgmt', 'board', 'customer-orders', 'shipping',
    'chart-of-accounts', 'journal', 'general-ledger', 'trial-balance', 'financial-statements', 'ar-ap'],
};

function hasPermission(role, page, userPermissions) {
  if (!role) return false;
  // admin은 항상 전체 권한
  if (role === 'admin') return true;
  // 사용자별 개별 permissions가 있으면 그것 사용
  if (userPermissions && userPermissions.length > 0) {
    return userPermissions.includes(page);
  }
  // 없으면 역할 기본 권한
  const perms = ROLE_PERMISSIONS[role];
  if (!perms) return false;
  return perms.includes(page);
}

// GET /api/auth/pages — 전체 페이지 목록 (관리자 권한 UI용)
// (handleRequest 내에서 처리)

// ── DB 자동 백업 ──────────────────────────────────────────────────
const BACKUP_DIR = path.join(DATA_DIR, 'backups');
fs.mkdirSync(BACKUP_DIR, { recursive: true });

function backupDatabase() {
  const now = new Date();
  const stamp = now.toISOString().slice(0, 10).replace(/-/g, '');
  const backupPath = path.join(BACKUP_DIR, `orders_${stamp}.db`);
  try {
    if (fs.existsSync(backupPath)) return; // 오늘 이미 백업됨
    db.backup(backupPath).then(() => {
      console.log(`✅ DB 백업 완료: ${backupPath}`);
      // 7일 이전 백업 삭제
      const files = fs.readdirSync(BACKUP_DIR).filter(f => f.startsWith('orders_') && f.endsWith('.db'));
      const cutoff = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      files.forEach(f => {
        const dateStr = f.match(/orders_(\d{8})\.db/);
        if (dateStr) {
          const y = dateStr[1].slice(0, 4), m = dateStr[1].slice(4, 6), d = dateStr[1].slice(6, 8);
          if (new Date(`${y}-${m}-${d}`) < cutoff) {
            fs.unlinkSync(path.join(BACKUP_DIR, f));
            console.log(`🗑️ 오래된 백업 삭제: ${f}`);
          }
        }
      });
    }).catch(e => console.error('DB 백업 실패:', e.message));
  } catch (e) { console.error('DB 백업 실패:', e.message); }
}

// 서버 시작 시 즉시 백업 + 매일 자정 백업
backupDatabase();
setInterval(() => {
  const now = new Date();
  if (now.getHours() === 0 && now.getMinutes() < 5) backupDatabase();
}, 5 * 60 * 1000);

// 초기 필수발주 품목 4건
const aoInit = db.prepare('INSERT OR IGNORE INTO auto_order_items (product_code, min_stock, order_qty) VALUES (?, ?, ?)');
aoInit.run('BE004', 0, 0);
aoInit.run('BE005', 0, 0);
aoInit.run('2010wh_n', 0, 0);
aoInit.run('BE042', 0, 0);

// ── Uploads directory ───────────────────────────────────────────────
const UPLOAD_ROOT = path.join(UPLOAD_DIR, 'invoices');
fs.mkdirSync(UPLOAD_ROOT, { recursive: true });

// ── MIME map ────────────────────────────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.js':   'text/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif':  'image/gif',
  '.pdf':  'application/pdf',
  '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  '.xls':  'application/vnd.ms-excel',
};

// ── Helpers ─────────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function jsonRes(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8', ...CORS });
  res.end(JSON.stringify(payload));
}

function ok(res, data) { jsonRes(res, 200, { ok: true, data }); }
function fail(res, code, error) { jsonRes(res, code, { ok: false, error }); }

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function readJSON(req) {
  return readBody(req).then(buf => JSON.parse(buf.toString('utf8')));
}

// ── Simple multipart parser ─────────────────────────────────────────
function parseMultipart(buf, boundary) {
  const parts = {};
  const sep = Buffer.from('--' + boundary);
  let pos = 0;

  while (pos < buf.length) {
    const start = buf.indexOf(sep, pos);
    if (start === -1) break;
    const afterSep = start + sep.length;
    // check for closing --
    if (buf[afterSep] === 0x2D && buf[afterSep + 1] === 0x2D) break;

    // find header/body separator (double CRLF)
    const headerEnd = buf.indexOf('\r\n\r\n', afterSep);
    if (headerEnd === -1) break;
    const headerStr = buf.slice(afterSep, headerEnd).toString('utf8');

    // find end of this part
    const nextSep = buf.indexOf(sep, headerEnd + 4);
    const bodyEnd = nextSep !== -1 ? nextSep - 2 : buf.length; // -2 for trailing CRLF
    const body = buf.slice(headerEnd + 4, bodyEnd);

    // parse disposition
    const dispMatch = headerStr.match(/name="([^"]+)"/);
    const fileMatch = headerStr.match(/filename="([^"]+)"/);
    const ctMatch = headerStr.match(/Content-Type:\s*(.+)/i);
    const name = dispMatch ? dispMatch[1] : null;

    if (name) {
      if (fileMatch) {
        parts[name] = {
          filename: fileMatch[1],
          contentType: ctMatch ? ctMatch[1].trim() : 'application/octet-stream',
          data: body,
        };
      } else {
        parts[name] = body.toString('utf8');
      }
    }
    pos = nextSep !== -1 ? nextSep : buf.length;
  }
  return parts;
}

// ── PO number generator ─────────────────────────────────────────────
function generatePoNumber() {
  const today = new Date();
  const ymd = today.getFullYear().toString()
    + String(today.getMonth() + 1).padStart(2, '0')
    + String(today.getDate()).padStart(2, '0');
  const prefix = `PO-${ymd}-`;
  const row = db.prepare(`SELECT po_number FROM po_header WHERE po_number LIKE ? ORDER BY po_number DESC LIMIT 1`).get(prefix + '%');
  let seq = 1;
  if (row) {
    const last = parseInt(row.po_number.split('-')[2], 10);
    if (!isNaN(last)) seq = last + 1;
  }
  return prefix + String(seq).padStart(3, '0');
}

// ── Server ──────────────────────────────────────────────────────────
http.createServer(async (req, res) => {
  try {
    await handleRequest(req, res);
  } catch (e) {
    console.error('Server error:', e);
    logError('error', e.message, e.stack, req.url, req.method);
    fail(res, 500, e.message || 'Internal Server Error');
  }
}).listen(PORT, '0.0.0.0', () => {
  console.log(`스마트재고현황: http://localhost:${PORT}  (startup: ${((Date.now() - _startTime)/1000).toFixed(1)}s)`);
  console.log(`헬스체크: http://localhost:${PORT}/api/health`);
  scheduleAutoOrder();
  scheduleXerpSync();
});

// 미처리 예외/프라미스 거부 핸들러
process.on('uncaughtException', (e) => {
  console.error('Uncaught Exception:', e);
  logError('fatal', e.message, e.stack);
});
process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  const stack = reason instanceof Error ? reason.stack : '';
  console.error('Unhandled Rejection:', msg);
  logError('error', msg, stack);
});

async function handleRequest(req, res) {
  const parsed = new URL(req.url, `http://localhost:${PORT}`);
  const pathname = parsed.pathname;
  const method = req.method;

  // ── CORS preflight ──
  if (method === 'OPTIONS') {
    res.writeHead(204, CORS);
    res.end();
    return;
  }

  // GET /api/health — 시스템 헬스체크 (최상위 배치 — Docker 배포 안정성)
  if (pathname === '/api/health' && method === 'GET') {
    const health = { status: 'ok', timestamp: new Date().toISOString(), checks: {} };
    try { db.prepare('SELECT 1').get(); health.checks.sqlite = 'ok'; }
    catch (e) { health.checks.sqlite = 'error: ' + e.message; health.status = 'degraded'; }
    try {
      if (xerpPool && xerpPool.connected) { health.checks.xerp = 'ok'; }
      else if (xerpReconnectAttempts > 0) { health.checks.xerp = `reconnecting (attempt #${xerpReconnectAttempts})`; }
      else health.checks.xerp = 'not configured';
    } catch (e) { health.checks.xerp = 'error'; health.status = 'degraded'; }
    health.checks.smtp = smtpTransporter ? 'configured' : 'not configured';
    health.checks.google_sheet = gAccessToken ? 'ok' : (gRefreshToken ? 'token expired' : 'not configured');
    try {
      const backups = fs.readdirSync(BACKUP_DIR).filter(f => f.endsWith('.db'));
      health.checks.backup = `${backups.length} backups (latest: ${backups.sort().pop() || 'none'})`;
    } catch { health.checks.backup = 'error'; }
    try { health.checks.db_size = `${(fs.statSync(DB_PATH).size / 1024 / 1024).toFixed(1)} MB`; } catch {}
    ok(res, health);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  AUTH API (공개 — 토큰 불필요)
  // ════════════════════════════════════════════════════════════════════
  const clientIP = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';

  // GET /api/auth/local-bypass — 로컬 개발용 인증 우회 (localhost에서만 작동)
  if (pathname === '/api/auth/local-bypass' && method === 'GET') {
    const remoteAddr = req.socket.remoteAddress || '';
    const isLocal = remoteAddr === '127.0.0.1' || remoteAddr === '::1' || remoteAddr === '::ffff:127.0.0.1';
    if (!isLocal) { fail(res, 403, '로컬에서만 사용 가능합니다'); return; }
    const user = db.prepare("SELECT user_id, username, display_name, role, email, permissions, favorites FROM users WHERE role = 'admin' AND is_active = 1 LIMIT 1").get();
    if (!user) { fail(res, 404, '관리자 계정이 없습니다'); return; }
    const token = signToken(user);
    let favs = []; try { favs = JSON.parse(user.favorites || '[]'); } catch {}
    ok(res, { token, user: { user_id: user.user_id, username: user.username, display_name: user.display_name, role: user.role, email: user.email }, permissions: ['*'], favorites: favs });
    return;
  }

  // POST /api/auth/login
  if (pathname === '/api/auth/login' && method === 'POST') {
    const body = await readJSON(req);
    const { username, password } = body;
    if (!username || !password) { fail(res, 400, '이메일(또는 아이디)과 비밀번호를 입력하세요'); return; }
    // 이메일 또는 username으로 로그인 가능
    const user = db.prepare("SELECT * FROM users WHERE (username = ? OR email = ?) AND is_active = 1").get(username, username);
    if (!user || !bcrypt.compareSync(password, user.password_hash)) {
      auditLog(null, username, 'login_failed', 'auth', '', '로그인 실패', clientIP);
      fail(res, 401, '아이디 또는 비밀번호가 일치하지 않습니다');
      return;
    }
    const token = signToken(user);
    db.prepare("UPDATE users SET last_login = datetime('now','localtime') WHERE user_id = ?").run(user.user_id);
    auditLog(user.user_id, user.username, 'login', 'auth', '', '로그인 성공', clientIP);
    ok(res, {
      token, user: { user_id: user.user_id, username: user.username, display_name: user.display_name, role: user.role, email: user.email }
    });
    return;
  }

  // POST /api/auth/google — Google OAuth 로그인
  if (pathname === '/api/auth/google' && method === 'POST') {
    const body = await readJSON(req);
    const { credential } = body; // Google ID Token
    if (!credential) { fail(res, 400, 'Google 인증 토큰이 없습니다'); return; }
    try {
      // Google ID Token 검증 (Google tokeninfo endpoint 사용)
      const verifyUrl = `https://oauth2.googleapis.com/tokeninfo?id_token=${encodeURIComponent(credential)}`;
      const gRes = await new Promise((resolve, reject) => {
        https.get(verifyUrl, (r) => {
          let data = '';
          r.on('data', c => data += c);
          r.on('end', () => {
            try { resolve({ status: r.statusCode, data: JSON.parse(data) }); }
            catch { reject(new Error('Google 응답 파싱 실패')); }
          });
        }).on('error', reject);
      });
      if (gRes.status !== 200 || !gRes.data.email) {
        fail(res, 401, 'Google 인증 실패: 유효하지 않은 토큰');
        return;
      }
      // Client ID 검증 (설정된 경우)
      if (GOOGLE_CLIENT_ID && gRes.data.aud !== GOOGLE_CLIENT_ID) {
        fail(res, 401, 'Google Client ID 불일치');
        return;
      }
      const { email, sub: googleId, name, picture, hd } = gRes.data;
      // 이메일 도메인 검증
      const emailDomain = email.split('@')[1];
      if (!ALLOWED_EMAIL_DOMAINS.includes(emailDomain)) {
        auditLog(null, email, 'google_login_blocked', 'auth', '', `허용되지 않은 도메인: ${emailDomain}`, clientIP);
        fail(res, 403, `허용되지 않은 이메일 도메인입니다 (${emailDomain}). 회사 계정으로 로그인하세요.`);
        return;
      }
      // 기존 사용자 찾기 (google_id 또는 email)
      let user = db.prepare("SELECT * FROM users WHERE google_id = ? OR email = ?").get(googleId, email);
      if (user && !user.is_active) {
        fail(res, 403, '비활성화된 계정입니다. 관리자에게 문의하세요.');
        return;
      }
      if (!user) {
        // 첫 Google 로그인 → 자동 계정 생성 (기본 역할: viewer)
        const username = email.split('@')[0];
        // username 충돌 방지
        let finalUsername = username;
        let suffix = 1;
        while (db.prepare("SELECT user_id FROM users WHERE username = ?").get(finalUsername)) {
          finalUsername = username + suffix++;
        }
        const result = db.prepare("INSERT INTO users (username, password_hash, display_name, role, email, google_id, profile_picture) VALUES (?,?,?,?,?,?,?)")
          .run(finalUsername, '', name || email.split('@')[0], 'viewer', email, googleId, picture || '');
        user = db.prepare("SELECT * FROM users WHERE user_id = ?").get(result.lastInsertRowid);
        auditLog(user.user_id, finalUsername, 'google_register', 'auth', user.user_id, `Google 자동 등록: ${email}`, clientIP);
        console.log(`✅ Google 신규 사용자 등록: ${email} (${finalUsername})`);
      } else {
        // 기존 사용자 → google_id, profile_picture 업데이트
        db.prepare("UPDATE users SET google_id = ?, profile_picture = ?, display_name = CASE WHEN display_name = '' OR display_name = username THEN ? ELSE display_name END WHERE user_id = ?")
          .run(googleId, picture || '', name || user.display_name, user.user_id);
      }
      // JWT 발급
      const token = signToken(user);
      db.prepare("UPDATE users SET last_login = datetime('now','localtime') WHERE user_id = ?").run(user.user_id);
      auditLog(user.user_id, user.username, 'google_login', 'auth', '', `Google 로그인: ${email}`, clientIP);
      ok(res, {
        token,
        user: { user_id: user.user_id, username: user.username, display_name: user.display_name || name, role: user.role, email: user.email, profile_picture: picture || user.profile_picture }
      });
    } catch (e) {
      console.error('Google 인증 오류:', e.message);
      fail(res, 500, 'Google 인증 처리 중 오류: ' + e.message);
    }
    return;
  }

  // GET /api/auth/config — 클라이언트용 인증 설정 (Google Client ID 등)
  if (pathname === '/api/auth/config' && method === 'GET') {
    ok(res, {
      google_client_id: GOOGLE_CLIENT_ID,
      allowed_domains: ALLOWED_EMAIL_DOMAINS,
      auth_mode: GOOGLE_CLIENT_ID ? 'google' : 'password'
    });
    return;
  }

  // POST /api/auth/register — 회원가입 (@barunn.net만 허용)
  if (pathname === '/api/auth/register' && method === 'POST') {
    const body = await readJSON(req);
    const { email, password, display_name } = body;
    if (!email || !password) { fail(res, 400, '이메일과 비밀번호를 입력하세요'); return; }
    if (password.length < 4) { fail(res, 400, '비밀번호는 4자 이상이어야 합니다'); return; }
    // 이메일 도메인 검증
    const emailDomain = email.split('@')[1];
    if (!ALLOWED_EMAIL_DOMAINS.includes(emailDomain)) {
      fail(res, 403, `@${ALLOWED_EMAIL_DOMAINS.join(', @')} 이메일만 가입할 수 있습니다.`);
      return;
    }
    // 중복 검사
    const exists = db.prepare("SELECT user_id FROM users WHERE email = ?").get(email);
    if (exists) { fail(res, 409, '이미 등록된 이메일입니다. 로그인해주세요.'); return; }
    const username = email.split('@')[0];
    let finalUsername = username;
    let suffix = 1;
    while (db.prepare("SELECT user_id FROM users WHERE username = ?").get(finalUsername)) {
      finalUsername = username + suffix++;
    }
    const hash = bcrypt.hashSync(password, 10);
    const result = db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?,?,?,?,?)")
      .run(finalUsername, hash, display_name || username, 'viewer', email);
    const user = db.prepare("SELECT * FROM users WHERE user_id = ?").get(result.lastInsertRowid);
    const token = signToken(user);
    db.prepare("UPDATE users SET last_login = datetime('now','localtime') WHERE user_id = ?").run(user.user_id);
    auditLog(user.user_id, finalUsername, 'register', 'auth', user.user_id, `회원가입: ${email}`, clientIP);
    console.log(`✅ 신규 가입: ${email} (${finalUsername})`);
    ok(res, {
      token,
      user: { user_id: user.user_id, username: user.username, display_name: user.display_name, role: user.role, email: user.email }
    });
    return;
  }

  // GET /api/auth/me — 현재 사용자 정보
  if (pathname === '/api/auth/me' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const user = db.prepare("SELECT user_id, username, display_name, role, email, permissions, favorites, last_login FROM users WHERE user_id = ?").get(decoded.userId);
    if (!user) { fail(res, 401, '사용자를 찾을 수 없습니다'); return; }
    const userPerms = user.permissions ? JSON.parse(user.permissions) : [];
    const effectivePerms = user.role === 'admin' ? ['*'] : (userPerms.length > 0 ? userPerms : (ROLE_PERMISSIONS[user.role] || []));
    let favs = []; try { favs = JSON.parse(user.favorites || '[]'); } catch {}
    ok(res, { user: { ...user, permissions: undefined, favorites: undefined }, permissions: effectivePerms, favorites: favs });
    return;
  }

  // GET /api/auth/pages — 전체 페이지 목록 (관리자 권한 UI용)
  if (pathname === '/api/auth/pages' && method === 'GET') {
    ok(res, ALL_PAGES);
    return;
  }

  // POST /api/auth/change-password
  if (pathname === '/api/auth/change-password' && method === 'POST') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const body = await readJSON(req);
    const user = db.prepare("SELECT * FROM users WHERE user_id = ?").get(decoded.userId);
    if (!user) { fail(res, 404, '사용자 없음'); return; }
    if (!bcrypt.compareSync(body.current_password, user.password_hash)) { fail(res, 400, '현재 비밀번호가 일치하지 않습니다'); return; }
    if (!body.new_password || body.new_password.length < 4) { fail(res, 400, '새 비밀번호는 4자 이상이어야 합니다'); return; }
    const hash = bcrypt.hashSync(body.new_password, 10);
    db.prepare("UPDATE users SET password_hash = ?, updated_at = datetime('now','localtime') WHERE user_id = ?").run(hash, decoded.userId);
    auditLog(decoded.userId, decoded.username, 'password_change', 'auth', decoded.userId, '비밀번호 변경', clientIP);
    ok(res, { message: '비밀번호가 변경되었습니다' });
    return;
  }

  // GET /api/auth/favorites — 즐겨찾기 조회
  if (pathname === '/api/auth/favorites' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const row = db.prepare("SELECT favorites FROM users WHERE user_id = ?").get(decoded.userId);
    let favs = [];
    try { favs = JSON.parse(row?.favorites || '[]'); } catch {}
    ok(res, favs);
    return;
  }

  // PUT /api/auth/favorites — 즐겨찾기 저장
  if (pathname === '/api/auth/favorites' && method === 'PUT') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const body = await readJSON(req);
    const favs = Array.isArray(body.favorites) ? body.favorites : [];
    db.prepare("UPDATE users SET favorites = ? WHERE user_id = ?").run(JSON.stringify(favs), decoded.userId);
    ok(res, { message: '즐겨찾기 저장 완료', favorites: favs });
    return;
  }

  // GET /api/auth/users — 사용자 목록 (admin만)
  if (pathname === '/api/auth/users' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const users = db.prepare("SELECT user_id, username, display_name, role, email, permissions, is_active, last_login, created_at FROM users ORDER BY user_id").all();
    ok(res, users);
    return;
  }

  // POST /api/auth/users — 사용자 추가 (admin만)
  if (pathname === '/api/auth/users' && method === 'POST') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const body = await readJSON(req);
    if (!body.username || !body.password) { fail(res, 400, '아이디와 비밀번호 필수'); return; }
    const exists = db.prepare("SELECT user_id FROM users WHERE username = ?").get(body.username);
    if (exists) { fail(res, 409, '이미 존재하는 아이디입니다'); return; }
    const hash = bcrypt.hashSync(body.password, 10);
    const result = db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?,?,?,?,?)")
      .run(body.username, hash, body.display_name || body.username, body.role || 'viewer', body.email || '');
    auditLog(decoded.userId, decoded.username, 'user_create', 'users', result.lastInsertRowid, `사용자 생성: ${body.username} (${body.role || 'viewer'})`, clientIP);
    ok(res, { user_id: result.lastInsertRowid, username: body.username });
    return;
  }

  // PUT /api/auth/users/:id — 사용자 수정 (admin만)
  const userPut = pathname.match(/^\/api\/auth\/users\/(\d+)$/);
  if (userPut && method === 'PUT') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const uid = parseInt(userPut[1]);
    const body = await readJSON(req);
    const sets = [];
    const params = [];
    if (body.display_name !== undefined) { sets.push('display_name=?'); params.push(body.display_name); }
    if (body.role !== undefined) { sets.push('role=?'); params.push(body.role); }
    if (body.email !== undefined) { sets.push('email=?'); params.push(body.email); }
    if (body.is_active !== undefined) { sets.push('is_active=?'); params.push(body.is_active ? 1 : 0); }
    if (body.password) { sets.push('password_hash=?'); params.push(bcrypt.hashSync(body.password, 10)); }
    if (body.permissions !== undefined) { sets.push('permissions=?'); params.push(JSON.stringify(body.permissions)); }
    if (sets.length === 0) { fail(res, 400, '변경할 항목이 없습니다'); return; }
    sets.push("updated_at=datetime('now','localtime')");
    params.push(uid);
    db.prepare(`UPDATE users SET ${sets.join(',')} WHERE user_id=?`).run(...params);
    auditLog(decoded.userId, decoded.username, 'user_update', 'users', uid, `사용자 수정: ${JSON.stringify(body)}`, clientIP);
    ok(res, { updated: uid });
    return;
  }

  // (헬스체크는 최상위로 이동됨 — 위 참조)

  // GET /api/audit-log — 감사 로그 (admin만, 필터링/페이지네이션/통계 지원)
  if (pathname === '/api/audit-log' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const limit = Math.min(parseInt(parsed.searchParams.get('limit') || '100'), 500);
    const offset = parseInt(parsed.searchParams.get('offset') || '0');
    const user = parsed.searchParams.get('user') || '';
    const action = parsed.searchParams.get('action') || '';
    const resource = parsed.searchParams.get('resource') || '';
    const start = parsed.searchParams.get('start') || '';
    const end = parsed.searchParams.get('end') || '';
    const search = parsed.searchParams.get('q') || '';

    let where = [], params = [];
    if (user) { where.push("username LIKE ?"); params.push('%' + user + '%'); }
    if (action) { where.push("action = ?"); params.push(action); }
    if (resource) { where.push("resource = ?"); params.push(resource); }
    if (start) { where.push("created_at >= ?"); params.push(start); }
    if (end) { where.push("created_at <= ? || ' 23:59:59'"); params.push(end); }
    if (search) { where.push("(details LIKE ? OR username LIKE ? OR resource_id LIKE ?)"); params.push('%'+search+'%','%'+search+'%','%'+search+'%'); }

    const whereClause = where.length ? ' WHERE ' + where.join(' AND ') : '';
    const total = db.prepare("SELECT COUNT(*) as cnt FROM audit_log" + whereClause).get(...params).cnt;
    const rows = db.prepare("SELECT * FROM audit_log" + whereClause + " ORDER BY created_at DESC LIMIT ? OFFSET ?").all(...params, limit, offset);

    ok(res, { rows, total, limit, offset });
    return;
  }

  // GET /api/audit-log/stats — 감사 로그 통계 (admin만)
  if (pathname === '/api/audit-log/stats' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const days = parseInt(parsed.searchParams.get('days') || '30');
    const since = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);

    const totalLogs = db.prepare("SELECT COUNT(*) as cnt FROM audit_log WHERE created_at >= ?").get(since).cnt;
    const byAction = db.prepare("SELECT action, COUNT(*) as cnt FROM audit_log WHERE created_at >= ? GROUP BY action ORDER BY cnt DESC").all(since);
    const byUser = db.prepare("SELECT username, COUNT(*) as cnt FROM audit_log WHERE created_at >= ? GROUP BY username ORDER BY cnt DESC LIMIT 20").all(since);
    const byResource = db.prepare("SELECT resource, COUNT(*) as cnt FROM audit_log WHERE created_at >= ? GROUP BY resource ORDER BY cnt DESC").all(since);
    const byDay = db.prepare("SELECT DATE(created_at) as day, COUNT(*) as cnt FROM audit_log WHERE created_at >= ? GROUP BY DATE(created_at) ORDER BY day DESC LIMIT ?").all(since, days);
    const loginFailed = db.prepare("SELECT COUNT(*) as cnt FROM audit_log WHERE action='login_failed' AND created_at >= ?").get(since).cnt;
    const uniqueUsers = db.prepare("SELECT COUNT(DISTINCT username) as cnt FROM audit_log WHERE created_at >= ? AND action IN ('login','google_login')").get(since).cnt;
    const recentActions = db.prepare("SELECT action, COUNT(*) as cnt FROM audit_log WHERE created_at >= datetime('now','-1 hour','localtime') GROUP BY action ORDER BY cnt DESC").all();

    ok(res, { total: totalLogs, login_failed: loginFailed, unique_users: uniqueUsers, by_action: byAction, by_user: byUser, by_resource: byResource, by_day: byDay, recent_hour: recentActions, days });
    return;
  }

  // GET /api/audit-log/actions — 감사 로그 액션 유형 목록
  if (pathname === '/api/audit-log/actions' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const actions = db.prepare("SELECT DISTINCT action FROM audit_log ORDER BY action").all();
    const resources = db.prepare("SELECT DISTINCT resource FROM audit_log ORDER BY resource").all();
    const users = db.prepare("SELECT DISTINCT username FROM audit_log WHERE username IS NOT NULL ORDER BY username").all();
    ok(res, { actions: actions.map(a => a.action), resources: resources.map(r => r.resource), users: users.map(u => u.username) });
    return;
  }

  // GET /api/error-logs — 에러 로그 (admin만, 필터링 지원)
  if (pathname === '/api/error-logs' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const limit = Math.min(parseInt(parsed.searchParams.get('limit') || '100'), 500);
    const offset = parseInt(parsed.searchParams.get('offset') || '0');
    const level = parsed.searchParams.get('level') || '';
    const search = parsed.searchParams.get('q') || '';

    let where = [], params = [];
    if (level) { where.push("level = ?"); params.push(level); }
    if (search) { where.push("(message LIKE ? OR url LIKE ?)"); params.push('%'+search+'%','%'+search+'%'); }
    const whereClause = where.length ? ' WHERE ' + where.join(' AND ') : '';
    const total = db.prepare("SELECT COUNT(*) as cnt FROM error_logs" + whereClause).get(...params).cnt;
    const rows = db.prepare("SELECT * FROM error_logs" + whereClause + " ORDER BY created_at DESC LIMIT ? OFFSET ?").all(...params, limit, offset);
    ok(res, { rows, total, limit, offset });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  인증 미들웨어 — 여기서부터 모든 API는 토큰 필요
  // ════════════════════════════════════════════════════════════════════
  let currentUser = null;
  if (pathname.startsWith('/api/') && !pathname.startsWith('/api/vendor-portal')) {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (decoded) {
      currentUser = decoded;
    }
    // 인증 강제 모드: AUTH_REQUIRED=true 일 때만 차단 (기본: 선택적)
    const authRequired = (envVars.AUTH_REQUIRED || process.env.AUTH_REQUIRED || 'false') === 'true';
    if (authRequired && !decoded) {
      fail(res, 401, '인증이 필요합니다. 로그인하세요.');
      return;
    }
  }

  // ════════════════════════════════════════════════════════════════════
  //  EXISTING ROUTES (preserved)
  // ════════════════════════════════════════════════════════════════════

  // 제품정보 저장 API (POST)
  if (pathname === '/api/save-product-info' && method === 'POST') {
    const body = await readBody(req);
    const outPath = path.join(__dir, 'product_info.json');
    fs.writeFileSync(outPath, body, 'utf8');
    jsonRes(res, 200, { ok: true, size: body.length, path: outPath });
    return;
  }

  // 데이터 갱신 API
  if (pathname === '/api/refresh') {
    const xlsPath = process.env.ERP_EXCEL_PATH || path.join(DATA_DIR, '스마트재고현황.xls');
    if (fs.existsSync(xlsPath)) {
      const stat = fs.statSync(xlsPath);
      const script = path.join(__dir, 'read_erp_excel.py');
      execFile('python', [script, xlsPath], { encoding: 'utf8' }, (err, stdout, stderr) => {
        if (err) {
          jsonRes(res, 200, { ok: false, error: stderr || err.message });
          return;
        }
        const match = stdout.match(/제품 수: (\d+)개/);
        const count = match ? parseInt(match[1]) : 0;
        const fileTime = stat.mtime.toLocaleString('ko-KR');
        jsonRes(res, 200, { ok: true, count, fileTime, message: `${count}개 품목 갱신 완료 (엑셀: ${fileTime})` });
      });
      return;
    }
    // 엑셀 없으면 JSON 파일로 폴백
    const jsonPath = path.join(DATA_DIR, 'erp_smart_inventory.json');
    if (fs.existsSync(jsonPath)) {
      try {
        const d = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
        const products = d.products || d.data || d;
        const count = Array.isArray(products) ? products.length : 0;
        const stat = fs.statSync(jsonPath);
        const fileTime = stat.mtime.toLocaleString('ko-KR');
        jsonRes(res, 200, { ok: true, count, fileTime, message: `${count}개 품목 로드 완료 (JSON: ${fileTime})` });
      } catch(e) {
        jsonRes(res, 200, { ok: false, error: 'JSON 파싱 오류: ' + e.message });
      }
    } else {
      jsonRes(res, 200, { ok: false, error: '데이터 파일이 없습니다. erp_smart_inventory.json 또는 스마트재고현황.xls를 user/ 폴더에 넣어주세요.' });
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: VENDORS
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/vendors' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM vendors ORDER BY name').all();
    ok(res, rows);
    return;
  }

  if (pathname === '/api/vendors' && method === 'POST') {
    const body = await readJSON(req);
    const stmt = db.prepare(`INSERT INTO vendors (vendor_code, name, type, contact, phone, email, email_cc, kakao, memo) VALUES (@vendor_code, @name, @type, @contact, @phone, @email, @email_cc, @kakao, @memo)`);
    const info = stmt.run({
      vendor_code: body.vendor_code || '',
      name: body.name || '',
      type: body.type || '',
      contact: body.contact || '',
      phone: body.phone || '',
      email: body.email || '',
      email_cc: body.email_cc || '',
      kakao: body.kakao || '',
      memo: body.memo || '',
    });
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'vendor_create', 'vendors', info.lastInsertRowid, `거래처 등록: ${body.name}`, clientIP);
    ok(res, { vendor_id: info.lastInsertRowid });
    return;
  }

  if (pathname === '/api/vendors/migrate' && method === 'POST') {
    const vendors = await readJSON(req);
    if (!Array.isArray(vendors)) { fail(res, 400, 'Expected array'); return; }
    const stmt = db.prepare(`INSERT OR IGNORE INTO vendors (name, type, contact, phone, email, kakao, memo) VALUES (@name, @type, @contact, @phone, @email, @kakao, @memo)`);
    const tx = db.transaction((list) => {
      let count = 0;
      for (const v of list) {
        const info = stmt.run({
          name: v.name || '',
          type: v.type || '',
          contact: v.contact || '',
          phone: v.phone || '',
          email: v.email || '',
          kakao: v.kakao || '',
          memo: v.memo || '',
        });
        if (info.changes > 0) count++;
      }
      return count;
    });
    const count = tx(vendors);
    ok(res, { migrated: count, total: vendors.length });
    return;
  }

  // PUT /api/vendors/:id
  const vendorPut = pathname.match(/^\/api\/vendors\/(\d+)$/);
  if (vendorPut && method === 'PUT') {
    const id = parseInt(vendorPut[1]);
    const body = await readJSON(req);
    const fields = [];
    const params = { id };
    for (const col of ['vendor_code', 'name', 'type', 'contact', 'phone', 'email', 'email_cc', 'kakao', 'memo']) {
      if (body[col] !== undefined) {
        fields.push(`${col} = @${col}`);
        params[col] = body[col];
      }
    }
    if (fields.length === 0) { fail(res, 400, 'No fields to update'); return; }
    fields.push(`updated_at = datetime('now','localtime')`);
    db.prepare(`UPDATE vendors SET ${fields.join(', ')} WHERE vendor_id = @id`).run(params);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'vendor_update', 'vendors', id, `거래처 수정: ${body.name || id}`, clientIP);
    ok(res, { vendor_id: id });
    return;
  }

  // DELETE /api/vendors/:id
  const vendorDel = pathname.match(/^\/api\/vendors\/(\d+)$/);
  if (vendorDel && method === 'DELETE') {
    const id = parseInt(vendorDel[1]);
    db.prepare('DELETE FROM vendors WHERE vendor_id = ?').run(id);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'vendor_delete', 'vendors', id, `거래처 삭제`, clientIP);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCTS (품목관리)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/products' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM products ORDER BY origin, product_code').all();
    ok(res, rows);
    return;
  }

  if (pathname === '/api/products' && method === 'POST') {
    const b = await readJSON(req);
    if (!b.product_code) { fail(res, 400, 'product_code required'); return; }
    try {
      const info = db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, category, status, material_code, material_name, unit, cut_spec, jopan, paper_maker, memo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
        b.product_code, b.product_name||'', b.brand||'', b.origin||'한국', b.category||'', b.status||'active',
        b.material_code||'', b.material_name||'', b.unit||'EA', b.cut_spec||'', b.jopan||'', b.paper_maker||'', b.memo||''
      );
      ok(res, { id: info.lastInsertRowid });
    } catch(e) {
      fail(res, 400, e.message.includes('UNIQUE') ? '이미 등록된 품목코드입니다' : e.message);
    }
    return;
  }

  const prodPut = pathname.match(/^\/api\/products\/(\d+)$/);
  if (prodPut && method === 'PUT') {
    const id = parseInt(prodPut[1]);
    const b = await readJSON(req);
    db.prepare(`UPDATE products SET product_name=?, brand=?, origin=?, category=?, status=?, material_code=?, material_name=?, unit=?, cut_spec=?, jopan=?, paper_maker=?, memo=?, updated_at=datetime('now','localtime') WHERE id=?`).run(
      b.product_name||'', b.brand||'', b.origin||'한국', b.category||'', b.status||'active',
      b.material_code||'', b.material_name||'', b.unit||'EA', b.cut_spec||'', b.jopan||'', b.paper_maker||'', b.memo||'', id
    );
    ok(res, { id });
    return;
  }

  const prodDel = pathname.match(/^\/api\/products\/(\d+)$/);
  if (prodDel && method === 'DELETE') {
    const id = parseInt(prodDel[1]);
    db.prepare('DELETE FROM products WHERE id = ?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCTS BULK UPLOAD (품목관리 엑셀 일괄 업로드)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/products/bulk' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    if (!items.length) { fail(res, 400, 'items required'); return; }

    const upsert = db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, memo)
      VALUES (?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(product_code) DO UPDATE SET
        product_name=excluded.product_name, brand=excluded.brand, origin=excluded.origin,
        material_code=excluded.material_code, material_name=excluded.material_name,
        cut_spec=excluded.cut_spec, jopan=excluded.jopan, paper_maker=excluded.paper_maker,
        memo=excluded.memo, updated_at=datetime('now','localtime')`);

    let inserted = 0, updated = 0;
    const tx = db.transaction(() => {
      for (const it of items) {
        if (!it.product_code) continue;
        const existing = db.prepare('SELECT id FROM products WHERE product_code=?').get(it.product_code);
        upsert.run(
          it.product_code, it.product_name||'', it.brand||'', it.origin||'한국',
          it.material_code||'', it.material_name||'', it.cut_spec||'', it.jopan||'',
          it.paper_maker||'', it.memo||''
        );
        if (existing) updated++; else inserted++;
      }
    });
    tx();
    ok(res, { inserted, updated, total: inserted + updated });
    return;
  }

  // PUT /api/products/:code/field — 개별 필드 업데이트
  const prodFieldMatch = pathname.match(/^\/api\/products\/(.+)\/field$/);
  if (prodFieldMatch && method === 'PUT') {
    const code = decodeURIComponent(prodFieldMatch[1]);
    const body = await readJSON(req);
    const allowed = ['cut_spec','jopan','paper_maker','material_name','material_code','post_vendor'];
    if (!allowed.includes(body.field)) { fail(res, 400, '허용되지 않는 필드'); return; }
    // 이전 값 조회 후 이력 저장
    const prev = db.prepare(`SELECT ${body.field} as val FROM products WHERE product_code=?`).get(code);
    const oldVal = prev ? (prev.val || '') : '';
    if (String(oldVal) !== String(body.value)) {
      db.prepare('INSERT INTO product_field_history (product_code, field_name, old_value, new_value) VALUES (?,?,?,?)').run(code, body.field, String(oldVal), String(body.value));
    }
    db.prepare(`UPDATE products SET ${body.field}=?, updated_at=datetime('now','localtime') WHERE product_code=?`).run(body.value, code);
    ok(res, { updated: code, field: body.field });
    return;
  }

  // PATCH /api/products/:code/post-vendor — 후공정 업체 설정
  const postVendorMatch = pathname.match(/^\/api\/products\/(.+)\/post-vendor$/);
  if (postVendorMatch && method === 'PATCH') {
    const code = decodeURIComponent(postVendorMatch[1]);
    const body = await readJSON(req);
    db.prepare("UPDATE products SET post_vendor=?, updated_at=datetime('now','localtime') WHERE product_code=?").run(body.post_vendor || '', code);
    // 캐시 무효화
    xerpInventoryCacheTime = 0;
    ok(res, { ok: true, code, post_vendor: body.post_vendor });
    return;
  }

  // GET /api/products/:code/history — 필드 변경 이력 조회
  const prodHistMatch = pathname.match(/^\/api\/products\/(.+)\/history$/);
  if (prodHistMatch && method === 'GET') {
    const code = decodeURIComponent(prodHistMatch[1]);
    const rows = db.prepare('SELECT * FROM product_field_history WHERE product_code=? ORDER BY changed_at DESC LIMIT 50').all(code);
    ok(res, rows);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  품목별 후공정 업체 매핑 API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/product-post-vendor' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM product_post_vendor ORDER BY product_code, step_order, process_type').all();
    ok(res, rows);
    return;
  }

  if (pathname === '/api/product-post-vendor' && method === 'POST') {
    const body = await readJSON(req);
    const { mappings } = body; // [{product_code, process_type, vendor_name, step_order}, ...]
    if (!mappings || !mappings.length) { fail(res, 400, 'mappings 필요'); return; }
    const upsert = db.prepare(`INSERT INTO product_post_vendor (product_code, process_type, vendor_name, step_order, updated_at)
      VALUES (?, ?, ?, ?, datetime('now','localtime'))
      ON CONFLICT(product_code, process_type) DO UPDATE SET vendor_name=excluded.vendor_name, step_order=excluded.step_order, updated_at=datetime('now','localtime')`);
    const tx = db.transaction(() => {
      for (const m of mappings) {
        if (m.product_code && m.process_type && m.vendor_name) {
          upsert.run(m.product_code, m.process_type, m.vendor_name, m.step_order || 1);
        }
      }
    });
    tx();
    ok(res, { ok: true, saved: mappings.length });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  XERP 실시간 재고 API (mmInventory + 월출고 통합)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/xerp-inventory' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }

    // 10분 캐시 (refresh=1 파라미터로 강제 갱신)
    const now = Date.now();
    const forceRefresh = parsed.searchParams.get('refresh') === '1';
    if (!forceRefresh && xerpInventoryCache && now - xerpInventoryCacheTime < 600000) {
      ok(res, xerpInventoryCache);
      return;
    }

    try {
      // 품목관리 DB에서 등록된 제품코드 리스트 로드
      const registeredProducts = db.prepare("SELECT product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, post_vendor FROM products WHERE status = 'active'").all();
      if (!registeredProducts.length) {
        ok(res, { products: [], updated: new Date().toISOString(), count: 0, message: '품목관리에 등록된 제품이 없습니다. 먼저 품목을 등록해주세요.' });
        return;
      }
      const productCodes = registeredProducts.map(p => p.product_code);

      // IN절용 제품코드 리스트 (SQL Injection 방지: 영숫자_만 허용)
      const safeCodeList = productCodes.filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => `'${c}'`).join(',');
      if (!safeCodeList) { ok(res, { products: [], updated: new Date().toISOString(), count: 0 }); return; }

      // 1. 제품 현재고: mmInventory (등록 제품만 조회 — 속도 최적화)
      const invResult = await xerpPool.request().query(`
        SELECT RTRIM(ItemCode) AS item_code, SUM(OhQty) AS oh_qty
        FROM mmInventory WITH (NOLOCK)
        WHERE SiteCode = 'BK10' AND RTRIM(ItemCode) IN (${safeCodeList})
        GROUP BY RTRIM(ItemCode)
      `);
      const invMap = {};
      for (const r of invResult.recordset) {
        invMap[(r.item_code || '').trim().toUpperCase()] = Math.round(r.oh_qty || 0);
      }

      // 2. 제품 출고: mmInoutItem (최근 3개월, 등록 제품만)
      const today = new Date();
      const start3m = new Date(today); start3m.setMonth(start3m.getMonth() - 3);
      const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');

      const shipResult = await xerpPool.request()
        .input('start3m', sql.NChar(16), fmt(start3m))
        .input('today', sql.NChar(16), fmt(today))
        .query(`
          SELECT RTRIM(ItemCode) AS item_code, SUM(InoutQty) AS total_qty
          FROM mmInoutItem WITH (NOLOCK)
          WHERE SiteCode = 'BK10' AND InoutGubun = 'SO'
            AND InoutDate >= @start3m AND InoutDate < @today
            AND RTRIM(ItemCode) IN (${safeCodeList})
          GROUP BY RTRIM(ItemCode)
        `);
      const shipMap = {};
      for (const r of shipResult.recordset) {
        const code = (r.item_code || '').trim().toUpperCase();
        if (code) {
          const total = Math.round(r.total_qty || 0);
          shipMap[code] = { total, monthly: Math.round(total / 3), daily: Math.round(total / 90) };
        }
      }

      // 3. 품목명: bar_shop1.S2_Card (등록 제품만)
      let itemNames = {};
      try {
        const bar1Pool = new sql.ConnectionPool({ ...xerpConfig, database: 'bar_shop1' });
        await bar1Pool.connect();
        const nameResult = await bar1Pool.request().query(`SELECT Card_Code, Card_Name FROM S2_Card WHERE RTRIM(Card_Code) IN (${safeCodeList})`);
        nameResult.recordset.forEach(r => { itemNames[(r.Card_Code || '').trim().toUpperCase()] = (r.Card_Name || '').trim(); });
        await bar1Pool.close();
      } catch (e) { console.warn('품목명 로드 실패:', e.message); }

      // 4. 품목관리 DB 기준으로 통합 (등록된 제품만)
      const products = [];
      for (const p of registeredProducts) {
        const code = p.product_code;
        const codeUpper = code.toUpperCase();
        const ohQty = invMap[codeUpper] || 0;
        const ship = shipMap[codeUpper] || { total: 0, monthly: 0, daily: 0 };
        products.push({
          '제품코드': code,
          '품목명': p.product_name || itemNames[codeUpper] || '',
          '브랜드': p.brand || '',
          '생산지': p.origin || '',
          '현재고': ohQty,
          '가용재고': ohQty,
          '요청량': 0,
          '_xerpMonthly': ship.monthly,
          '_xerpDaily': ship.daily,
          '_xerpTotal3m': ship.total,
          '_원자재코드': p.material_code || '',
          '_원재료용지명': p.material_name || '',
          '_절': p.cut_spec || '',
          '_조판': p.jopan || '',
          '_원지사': p.paper_maker || '',
          '_후공정업체': p.post_vendor || ''
        });
      }

      xerpInventoryCache = { products, updated: new Date().toISOString(), count: products.length };
      xerpInventoryCacheTime = now;
      console.log(`XERP 제품 재고 로드: ${products.length}개 제품`);
      ok(res, xerpInventoryCache);
    } catch (e) {
      console.error('XERP 재고 조회 오류:', e.message);
      // 타임아웃/연결 끊김 시 자동 재연결
      if (e.message.includes('imeout') || e.message.includes('closed') || e.message.includes('ECONN')) {
        try { await xerpPool.close(); } catch(_){}
        try { xerpPool = await sql.connect(xerpConfig); console.log('XERP 재연결 완료'); } catch(re) { console.error('XERP 재연결 실패:', re.message); }
      }
      fail(res, 500, 'XERP 재고 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  품목별 미입고 잔량 API (발주 트래킹 — 중복발주 방지)
  // ════════════════════════════════════════════════════════════════════
  if (pathname === '/api/inventory/pending-orders' && method === 'GET') {
    try {
      const rows = db.prepare(`
        SELECT i.product_code,
               SUM(i.ordered_qty - COALESCE(i.received_qty,0)) as pending_qty,
               GROUP_CONCAT(DISTINCT h.os_number) as os_numbers,
               GROUP_CONCAT(DISTINCT h.po_number) as po_numbers,
               MIN(h.expected_date) as earliest_due,
               h.status as latest_status,
               MAX(h.po_date) as latest_po_date
        FROM po_items i
        JOIN po_header h ON h.po_id = i.po_id
        WHERE h.status NOT IN ('cancelled','completed','received')
          AND (i.ordered_qty - COALESCE(i.received_qty,0)) > 0
        GROUP BY i.product_code
      `).all();
      const map = {};
      rows.forEach(r => {
        map[r.product_code] = {
          pending_qty: r.pending_qty || 0,
          os_numbers: (r.os_numbers || '').split(',').filter(Boolean),
          po_numbers: (r.po_numbers || '').split(',').filter(Boolean),
          earliest_due: r.earliest_due || '',
          status: r.latest_status || '',
          last_order_date: r.latest_po_date || ''
        };
      });
      ok(res, map);
    } catch(e) { ok(res, {}); }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  XERP 입고이력 품목코드 API (발주이력 판별용)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/xerp-receiving-codes' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }
    const from = parsed.searchParams.get('from') || '20250801';
    const to = parsed.searchParams.get('to') || '20260228';
    try {
      const result = await xerpPool.request()
        .input('fromDate', sql.NChar(16), from.replace(/-/g, ''))
        .input('toDate', sql.NChar(16), to.replace(/-/g, ''))
        .query(`
          SELECT DISTINCT RTRIM(i.ItemCode) AS item_code
          FROM mmInoutHeader h WITH (NOLOCK)
          JOIN mmInoutItem i WITH (NOLOCK)
            ON h.SiteCode = i.SiteCode AND h.InoutNo = i.InoutNo AND h.InoutGubun = i.InoutGubun
          WHERE h.SiteCode = 'BK10'
            AND h.InoutGubun = 'SI'
            AND h.InoutDate >= @fromDate AND h.InoutDate <= @toDate
        `);
      const codes = result.recordset.map(r => (r.item_code || '').trim()).filter(Boolean);
      ok(res, { codes, count: codes.length, from, to });
    } catch (e) {
      console.error('XERP 입고이력 조회 오류:', e.message);
      fail(res, 500, 'XERP 입고이력 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  XERP 월평균 출고량 API (재고현황 발주 설계용)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/xerp-monthly-usage' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }

    // 1시간 캐시
    const now = Date.now();
    if (xerpUsageCache && now - xerpUsageCacheTime < 3600000) {
      ok(res, xerpUsageCache);
      return;
    }

    try {
      const today = new Date();
      const start3m = new Date(today); start3m.setMonth(start3m.getMonth() - 3);
      const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');

      // 등록 제품만 조회 (속도 최적화)
      const registeredProducts = db.prepare("SELECT product_code FROM products WHERE status = 'active'").all();
      const registeredCodes = new Set(registeredProducts.map(r => r.product_code));
      const safeCodeList = registeredProducts.map(p => p.product_code).filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => `'${c}'`).join(',');
      if (!safeCodeList) { ok(res, {}); return; }

      const result = await xerpPool.request()
        .input('start3m', sql.NChar(16), fmt(start3m))
        .input('today', sql.NChar(16), fmt(today))
        .query(`
          SELECT RTRIM(ItemCode) AS item_code, SUM(InoutQty) AS total_qty, COUNT(DISTINCT RTRIM(InoutDate)) AS ship_days
          FROM mmInoutItem WITH (NOLOCK)
          WHERE SiteCode = 'BK10' AND InoutGubun = 'SO'
            AND InoutDate >= @start3m AND InoutDate < @today
            AND RTRIM(ItemCode) IN (${safeCodeList})
          GROUP BY RTRIM(ItemCode)
        `);

      const usage = {};
      for (const row of result.recordset) {
        const code = (row.item_code || '').trim();
        if (!code || !registeredCodes.has(code)) continue;
        const total = Math.round(row.total_qty || 0);
        usage[code] = { total, monthly: Math.round(total / 3), daily: Math.round(total / 90) };
      }

      xerpUsageCache = usage;
      xerpUsageCacheTime = now;
      console.log(`XERP 월출고 데이터: ${Object.keys(usage).length}개 관리품목 로드`);
      ok(res, usage);
    } catch (e) {
      console.error('XERP 월출고 조회 오류:', e.message);
      if (e.message.includes('imeout') || e.message.includes('closed') || e.message.includes('ECONN')) {
        try { await xerpPool.close(); } catch(_){}
        try { xerpPool = await sql.connect(xerpConfig); console.log('XERP 재연결 완료'); } catch(re) { console.error('XERP 재연결 실패:', re.message); }
      }
      fail(res, 500, 'XERP 월출고 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  SHIPMENTS (출고현황 - XERP 실시간 조회)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/shipments' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }
    try {
      // 쿼리 파라미터: from, to (YYYYMMDD), gubun (SO,MO,SI,MI — 기본 전체)
      const qFrom = parsed.searchParams.get('from');
      const qTo = parsed.searchParams.get('to');
      const qGubun = parsed.searchParams.get('gubun'); // 'SO','MO','SI','MI' or null=all

      const today = new Date();
      const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
      const todayStr = fmt(today);

      // 기본 기간: 최근 1개월
      let startStr = qFrom || (() => { const d = new Date(today); d.setMonth(d.getMonth() - 1); return fmt(d); })();
      let endStr = qTo || fmt(today);
      // endDate는 해당 일자 포함을 위해 +1일
      const endNext = new Date(parseInt(endStr.slice(0,4)), parseInt(endStr.slice(4,6))-1, parseInt(endStr.slice(6,8))+1);
      const endNextStr = fmt(endNext);

      // 출고구분 필터
      const gubunList = qGubun ? qGubun.split(',').map(g => g.trim()) : ['SO','MO','SI','MI'];
      const gubunPlaceholders = gubunList.map((_, i) => `@gb${i}`).join(',');

      const req = xerpPool.request()
        .input('startDate', sql.NChar(16), startStr)
        .input('endDate', sql.NChar(16), endNextStr);
      gubunList.forEach((g, i) => req.input(`gb${i}`, sql.VarChar(4), g));

      const result = await req.query(`
        SELECT RTRIM(ItemCode) AS item_code, MAX(RTRIM(ItemName)) AS item_name,
               RTRIM(InoutDate) AS InoutDate, RTRIM(InoutGubun) AS gubun,
               SUM(InoutQty) AS qty, SUM(InoutAmnt) AS amnt
        FROM mmInoutItem WITH (NOLOCK)
        WHERE SiteCode = 'BK10'
          AND InoutGubun IN (${gubunPlaceholders})
          AND InoutDate >= @startDate AND InoutDate < @endDate
        GROUP BY RTRIM(ItemCode), RTRIM(InoutDate), RTRIM(InoutGubun)
        ORDER BY RTRIM(InoutDate) DESC, RTRIM(ItemCode)
      `);

      // bar_shop1에서 품목명 매핑 가져오기 (별도 연결)
      let itemNames = {};
      try {
        const bar1Pool = new sql.ConnectionPool({ ...xerpConfig, database: 'bar_shop1' });
        await bar1Pool.connect();
        const itemCodes = [...new Set(result.recordset.map(r => (r.item_code || '').trim()).filter(Boolean))];
        if (itemCodes.length) {
          for (let i = 0; i < itemCodes.length; i += 500) {
            const batch = itemCodes.slice(i, i + 500);
            const placeholders = batch.map((_, j) => `@c${i+j}`).join(',');
            const nameReq = bar1Pool.request();
            batch.forEach((c, j) => nameReq.input(`c${i+j}`, sql.VarChar(30), c));
            const nameResult = await nameReq.query(`SELECT Card_Code, Card_Name FROM S2_Card WHERE Card_Code IN (${placeholders})`);
            nameResult.recordset.forEach(r => { itemNames[(r.Card_Code || '').trim()] = (r.Card_Name || '').trim(); });
          }
        }
        await bar1Pool.close();
      } catch (nameErr) {
        console.warn('품목명 조회 실패:', nameErr.message);
      }

      // 구분별 한글 라벨
      const gubunLabel = { SO: '매출출고', SI: '매출입고(반품)', MO: '원자재출고', MI: '원자재입고' };
      const rows = [];
      for (const row of result.recordset) {
        const dateStr = (row.InoutDate || '').trim();
        const code = (row.item_code || '').trim();
        rows.push({
          item_code: code,
          item_name: itemNames[code] || (row.item_name || '').trim(),
          date: dateStr ? `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}` : '',
          gubun: row.gubun || 'SO',
          gubun_label: gubunLabel[row.gubun] || row.gubun,
          qty: row.qty || 0,
          amnt: row.amnt || 0
        });
      }

      // 구분별 집계
      const summary = {};
      for (const g of ['SO','MO','SI','MI']) {
        const items = rows.filter(r => r.gubun === g);
        summary[g] = { label: gubunLabel[g], count: items.length, totalQty: items.reduce((s,r) => s + r.qty, 0), totalAmnt: items.reduce((s,r) => s + r.amnt, 0) };
      }

      ok(res, { rows, summary, range: { start: startStr, end: endStr, today: todayStr } });
    } catch (e) {
      console.error('출고현황 조회 오류:', e.message);
      fail(res, 500, '출고현황 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  AUTO-ORDER (필수 자동발주)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/china-price-tiers — 전체 단가 조회
  if (pathname === '/api/china-price-tiers' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM china_price_tiers ORDER BY product_code, qty_tier').all();
    // product_code별로 그룹핑
    const map = {};
    for (const r of rows) {
      if (!map[r.product_code]) map[r.product_code] = { product_code: r.product_code, product_type: r.product_type, tiers: [] };
      map[r.product_code].tiers.push({ qty: r.qty_tier, price: r.unit_price });
    }
    ok(res, { products: Object.values(map), total: Object.keys(map).length });
    return;
  }

  // GET /api/china-price-tiers/:code — 특정 품목 단가 조회
  const cptMatch = pathname.match(/^\/api\/china-price-tiers\/(.+)$/);
  if (cptMatch && method === 'GET') {
    const code = decodeURIComponent(cptMatch[1]);
    const rows = db.prepare('SELECT * FROM china_price_tiers WHERE product_code=? ORDER BY qty_tier').all(code);
    if (!rows.length) {
      // 대소문자 무시 재시도
      const rows2 = db.prepare('SELECT * FROM china_price_tiers WHERE UPPER(product_code)=UPPER(?) ORDER BY qty_tier').all(code);
      ok(res, { product_code: code, tiers: rows2.map(r => ({ qty: r.qty_tier, price: r.unit_price })) });
    } else {
      ok(res, { product_code: code, tiers: rows.map(r => ({ qty: r.qty_tier, price: r.unit_price })) });
    }
    return;
  }

  // POST /api/china-price-tiers/import — 엑셀에서 단가 일괄 임포트
  if (pathname === '/api/china-price-tiers/import' && method === 'POST') {
    const b = await readJSON(req);
    const items = b.items || [];
    if (!items.length) { fail(res, 400, 'items 배열 필요'); return; }
    const insert = db.prepare('INSERT OR REPLACE INTO china_price_tiers (product_code, product_type, qty_tier, unit_price, currency, effective_date) VALUES (?,?,?,?,?,?)');
    const tx = db.transaction(() => {
      let cnt = 0;
      for (const item of items) {
        insert.run(item.product_code, item.product_type || 'Card', item.qty_tier, item.unit_price, item.currency || 'KRW', item.effective_date || '2025-05-01');
        cnt++;
      }
      return cnt;
    });
    const count = tx();
    ok(res, { imported: count });
    return;
  }

  // GET /api/china-price-tiers/optimal?code=XX&need=NNN — 최적 발주수량 계산
  if (pathname === '/api/china-price-tiers/optimal' && method === 'GET') {
    const code = parsed.searchParams.get('code') || '';
    const need = parseInt(parsed.searchParams.get('need')) || 0;
    const monthlyUsage = parseInt(parsed.searchParams.get('monthly')) || 0;
    const currentStock = parseInt(parsed.searchParams.get('stock')) || 0;
    const leadTimeDays = parseInt(parsed.searchParams.get('leadtime')) || 50; // 중국 기본 50일
    const boxLimit = parseInt(parsed.searchParams.get('boxlimit')) || 500; // 선적 상자 제한

    const tiers = db.prepare('SELECT qty_tier, unit_price FROM china_price_tiers WHERE UPPER(product_code)=UPPER(?) ORDER BY qty_tier').all(code);
    if (!tiers.length) { ok(res, { code, tiers: [], optimal: null, message: '단가 데이터 없음' }); return; }

    // 목표재고 = 월출고량에 따라 차등 (많으면 3개월, 적으면 2개월)
    const targetMonths = monthlyUsage > 10000 ? 3 : 2;
    const targetStock = monthlyUsage * targetMonths;
    const shortage = Math.max(targetStock - currentStock, 0);

    if (shortage <= 0) {
      ok(res, { code, tiers: tiers.map(t=>({qty:t.qty_tier,price:t.unit_price})), optimal: null, targetMonths, targetStock, shortage: 0, message: '재고 충분' });
      return;
    }

    // 각 단가 구간별 비용 비교
    const options = [];
    for (const t of tiers) {
      if (t.qty_tier < shortage * 0.5) continue; // 부족량의 50% 미만은 제외
      const qty = Math.max(t.qty_tier, Math.ceil(shortage / 1000) * 1000);
      const totalCost = qty * t.unit_price;
      const unitCost = t.unit_price;
      const coverMonths = monthlyUsage > 0 ? ((currentStock + qty) / monthlyUsage).toFixed(1) : '-';
      options.push({ qty: t.qty_tier, orderQty: qty, unitPrice: t.unit_price, totalCost, coverMonths });
    }
    // 부족수량 기준도 추가 (가장 가까운 구간 적용)
    let basePrice = tiers[0].unit_price;
    for (const t of tiers) { if (shortage >= t.qty_tier) basePrice = t.unit_price; }
    const baseQty = Math.ceil(shortage / 1000) * 1000;
    const baseOption = { qty: baseQty, orderQty: baseQty, unitPrice: basePrice, totalCost: baseQty * basePrice, coverMonths: monthlyUsage > 0 ? ((currentStock + baseQty) / monthlyUsage).toFixed(1) : '-', isBase: true };

    // 최적 = 단위당 비용이 가장 낮으면서 커버 기간이 적절한 것
    const allOpts = [baseOption, ...options].sort((a,b) => a.totalCost - b.totalCost);
    // 중복 제거
    const seen = new Set();
    const uniqueOpts = allOpts.filter(o => { const k = o.orderQty; if (seen.has(k)) return false; seen.add(k); return true; });
    uniqueOpts.sort((a,b) => a.orderQty - b.orderQty);

    // 최적 추천: 단가 * 수량이 가장 효율적이면서 커버 기간이 targetMonths 이상
    let optimal = uniqueOpts[0];
    for (const o of uniqueOpts) {
      if (parseFloat(o.coverMonths) >= targetMonths && o.unitPrice <= optimal.unitPrice) {
        optimal = o;
      }
    }

    ok(res, {
      code, shortage, targetMonths, targetStock, currentStock, monthlyUsage, leadTimeDays,
      tiers: tiers.map(t=>({qty:t.qty_tier,price:t.unit_price})),
      options: uniqueOpts,
      optimal,
    });
    return;
  }

  // GET /api/auto-order
  if (pathname === '/api/auto-order' && method === 'GET') {
    const rows = db.prepare(`SELECT a.*, COALESCE(p.origin,'') as origin FROM auto_order_items a LEFT JOIN products p ON a.product_code=p.product_code ORDER BY a.id`).all();
    ok(res, rows);
    return;
  }

  // GET /api/auto-order/search?q=... — 품목 검색 (XERP 재고+products DB)
  if (pathname === '/api/auto-order/search' && method === 'GET') {
    const q = (parsed.searchParams.get('q') || '').trim();
    if (!q) { ok(res, []); return; }
    // products DB에서 검색
    const dbRows = db.prepare(`SELECT product_code, product_name, brand, origin FROM products WHERE product_code LIKE ? OR product_name LIKE ? LIMIT 20`).all(`%${q}%`, `%${q}%`);
    // 이미 등록된 품목 체크
    const existingCodes = new Set(db.prepare('SELECT product_code FROM auto_order_items').all().map(r => r.product_code));
    const results = dbRows.map(r => ({ ...r, already_added: existingCodes.has(r.product_code) }));
    // XERP 캐시에서도 검색
    if (xerpInventoryCache && xerpInventoryCache.products) {
      const dbCodes = new Set(results.map(r => r.product_code));
      for (const p of xerpInventoryCache.products) {
        const code = p['제품코드'] || '';
        if (!dbCodes.has(code) && (code.toLowerCase().includes(q.toLowerCase()) || (p['품목명']||'').includes(q))) {
          results.push({ product_code: code, product_name: p['품목명']||'', brand: p['브랜드']||'', origin: p['생산지']||'', already_added: existingCodes.has(code) });
          dbCodes.add(code);
        }
      }
    }
    ok(res, results.slice(0, 30));
    return;
  }

  // POST /api/auto-order
  if (pathname === '/api/auto-order' && method === 'POST') {
    const b = await readJSON(req);
    if (!b.product_code) { fail(res, 400, 'product_code required'); return; }
    // origin 자동 판별: products DB → XERP 캐시 → 기본값
    let origin = b.origin || '';
    if (!origin) {
      const prod = db.prepare('SELECT origin FROM products WHERE product_code=?').get(b.product_code);
      if (prod) origin = prod.origin || '';
    }
    if (!origin && xerpInventoryCache && xerpInventoryCache.products) {
      const xp = xerpInventoryCache.products.find(p => (p['제품코드']||'') === b.product_code);
      if (xp) origin = xp['생산지'] || '';
    }
    if (!origin) origin = '한국'; // 기본값
    // products 테이블에 origin 없으면 업데이트
    const existProd = db.prepare('SELECT id, origin FROM products WHERE product_code=?').get(b.product_code);
    if (existProd && !existProd.origin) {
      db.prepare('UPDATE products SET origin=? WHERE id=?').run(origin, existProd.id);
    }
    try {
      const info = db.prepare('INSERT INTO auto_order_items (product_code, min_stock, order_qty, vendor_name) VALUES (?,?,?,?)').run(
        b.product_code, b.min_stock || 0, b.order_qty || 0, b.vendor_name || ''
      );
      ok(res, { id: info.lastInsertRowid, origin });
    } catch (e) {
      fail(res, 400, e.message.includes('UNIQUE') ? '이미 등록된 품목입니다' : e.message);
    }
    return;
  }

  // PUT /api/auto-order/:id
  const aoUpdate = pathname.match(/^\/api\/auto-order\/(\d+)$/);
  if (aoUpdate && method === 'PUT') {
    const id = parseInt(aoUpdate[1]);
    const b = await readJSON(req);
    const existing = db.prepare('SELECT * FROM auto_order_items WHERE id=?').get(id);
    if (!existing) { fail(res, 404, 'not found'); return; }
    db.prepare('UPDATE auto_order_items SET min_stock=?, order_qty=?, vendor_name=?, enabled=? WHERE id=?').run(
      b.min_stock !== undefined ? b.min_stock : existing.min_stock,
      b.order_qty !== undefined ? b.order_qty : existing.order_qty,
      b.vendor_name !== undefined ? b.vendor_name : existing.vendor_name,
      b.enabled !== undefined ? b.enabled : existing.enabled,
      id
    );
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/auto-order/:id
  if (aoUpdate && method === 'DELETE') {
    const id = parseInt(aoUpdate[1]);
    db.prepare('DELETE FROM auto_order_items WHERE id=?').run(id);
    ok(res, { deleted: true });
    return;
  }

  // POST /api/auto-order/bulk-add — 재고현황에서 특정 생산지 품목 일괄 추가
  if (pathname === '/api/auto-order/bulk-add' && method === 'POST') {
    const b = await readJSON(req);
    const origin = b.origin || '중국';
    // products DB에서 해당 origin 품목 가져오기
    const prodsByOrigin = db.prepare('SELECT product_code FROM products WHERE origin=?').all(origin);
    // XERP 재고에서도 (생산지가 있는 경우)
    const xerpCodes = new Set();
    let inv = xerpInventoryCache && xerpInventoryCache.products ? xerpInventoryCache.products : [];
    if (!inv.length) {
      // xerpInventoryCache에서 직접 참조 (self-HTTP 호출 제거)
    }
    for (const p of inv) { if ((p['생산지']||'') === origin) xerpCodes.add(p['제품코드']); }
    // XERP 재고에 있는 품목만 (재고 데이터가 있어야 의미)
    const invCodes = new Set(inv.map(p => p['제품코드'] || ''));
    const targetCodes = new Set([...prodsByOrigin.map(p => p.product_code).filter(c => invCodes.has(c)), ...xerpCodes]);

    const existing = new Set(db.prepare('SELECT product_code FROM auto_order_items').all().map(r => r.product_code));
    const insert = db.prepare('INSERT OR IGNORE INTO auto_order_items (product_code, min_stock, order_qty, vendor_name, enabled) VALUES (?,?,?,?,1)');
    let added = 0, skipped = 0;
    const tx = db.transaction(() => {
      for (const code of targetCodes) {
        if (!code) continue;
        if (existing.has(code)) { skipped++; continue; }
        insert.run(code, 0, 0, '');
        added++;
      }
    });
    tx();
    ok(res, { added, skipped, origin, total: targetCodes.size });
    return;
  }

  // ── 전략발주 최적수량 계산 공통 함수 ──
  function calculateOptimalOrder(productCode, invData, origin) {
    const avail = typeof invData['가용재고'] === 'number' ? invData['가용재고'] : 0;
    const daily = invData['_xerpDaily'] || 0;
    const monthly = invData['_xerpMonthly'] || (invData._xerpTotal3m ? Math.round(invData._xerpTotal3m / 3) : 0);
    if (monthly <= 0) return { skip: true, reason: '월출고량 없음' };

    // 1. 리드타임 결정: 품목별 > 생산지별 기본값 (중국 50일)
    const prod = db.prepare('SELECT lead_time_days FROM products WHERE product_code=?').get(productCode);
    const leadDays = (prod && prod.lead_time_days > 0) ? prod.lead_time_days : (ORIGIN_LEAD_TIME[origin] || 7);

    // 2. 리드타임 동안 소진량
    const leadTimeUsage = Math.round(daily * leadDays);

    // 3. 안전재고: 중국은 리드타임이 길어서 2개월분, 한국 0.5개월
    const safetyStock = origin === '중국' ? Math.round(monthly * 2) : Math.round(monthly * 0.5);

    // 4. 목표재고 = 리드타임소진 + 안전재고
    const targetStock = leadTimeUsage + safetyStock;

    // 5. 부족수량
    const shortage = Math.max(targetStock - avail, 0);
    if (shortage <= 0) return { skip: true, reason: `재고 충분 (${avail.toLocaleString()} >= 목표 ${targetStock.toLocaleString()})`, targetStock, leadDays };

    const remainDays = daily > 0 ? Math.round(avail / daily) : 9999;

    // 6. 단가 구간 최적화 (china_price_tiers가 있는 경우)
    const tiers = db.prepare('SELECT qty_tier, unit_price FROM china_price_tiers WHERE UPPER(product_code)=UPPER(?) ORDER BY qty_tier').all(productCode);
    let orderQty, unitPrice = 0, tierAnalysis = [];

    if (tiers.length && origin === '중국') {
      // 중국 전략발주: 총비용 효율 비교
      // 기본: 부족수량을 1,000 단위 올림
      const baseQty = Math.ceil(shortage / 1000) * 1000;

      // 각 단가 구간별 시나리오 분석
      for (const t of tiers) {
        // 해당 구간 이상이면서 부족수량을 충족하는 수량
        const qty = Math.max(t.qty_tier, baseQty);
        const cost = qty * t.unit_price;
        // 추가 주문 수량 (부족수량 대비 얼마나 더 주문하는지)
        const excess = qty - shortage;
        // 초과분이 소진되는 일수 (재고 소진 여유)
        const excessDays = daily > 0 ? Math.round(excess / daily) : 9999;
        // 초과분이 3개월분 이내면 허용 (너무 많이 주문하지 않도록)
        const maxExcess = monthly * 3;
        const isViable = excess <= maxExcess;

        tierAnalysis.push({
          tierQty: t.qty_tier, orderQty: qty, unitPrice: t.unit_price,
          totalCost: cost, excess, excessDays, isViable,
          costPerUnit: t.unit_price
        });
      }

      // 실현 가능한 옵션만 필터
      const viable = tierAnalysis.filter(t => t.isViable);
      if (viable.length) {
        // 총비용이 가장 낮은 옵션 선택 (같으면 수량 적은 것)
        viable.sort((a, b) => a.totalCost - b.totalCost || a.orderQty - b.orderQty);
        const best = viable[0];

        // 다음 구간이 총비용이 더 낮으면 올리기 (손익분기점 체크)
        // 예: 18,000 × ¥0.684 = ¥12,312 vs 20,000 × ¥0.615 = ¥12,300 → 올리는 게 이득
        let finalBest = best;
        for (const opt of viable) {
          if (opt.totalCost <= finalBest.totalCost && opt.orderQty >= finalBest.orderQty) {
            finalBest = opt;
          }
        }
        orderQty = finalBest.orderQty;
        unitPrice = finalBest.unitPrice;
      } else {
        // 모든 구간이 초과분 과다 → 최소 구간 사용
        orderQty = baseQty;
        unitPrice = tiers[0].unit_price; // 최소 구간 단가
      }
    } else if (tiers.length) {
      // 한국 등: 단가 구간이 있으면 해당 수량의 단가 적용
      const baseQty = Math.ceil(shortage / 1000) * 1000;
      orderQty = baseQty;
      for (const t of tiers) { if (baseQty >= t.qty_tier) unitPrice = t.unit_price; }
    } else {
      orderQty = Math.ceil(shortage / 1000) * 1000;
    }

    return {
      skip: false, orderQty, unitPrice, shortage, targetStock, leadDays,
      leadTimeUsage, safetyStock, monthly, daily, avail, remainDays,
      totalCost: orderQty * unitPrice, tierAnalysis
    };
  }

  // POST /api/auto-order/check — 자동발주 실행
  if (pathname === '/api/auto-order/check' && method === 'POST') {
    const items = db.prepare('SELECT * FROM auto_order_items WHERE enabled=1').all();
    // XERP 캐시 또는 API에서 재고+출고 데이터 로드
    let inv = [];
    if (xerpInventoryCache && xerpInventoryCache.products) {
      inv = xerpInventoryCache.products;
    } else {
      // 폴백: JSON 파일
      try {
        const raw = JSON.parse(fs.readFileSync(path.join(__dir, 'erp_smart_inventory.json'), 'utf8'));
        inv = raw.products || raw.data || (Array.isArray(raw) ? raw : []);
      } catch(e2) {}
    }
    const invMap = {};
    for (const p of inv) { invMap[p['제품코드'] || p['품목코드']] = p; }

    // 예상 소진일 기준 정렬 (빠른 순 = 긴급한 것 먼저)
    items.sort((a, b) => {
      const pa = invMap[a.product_code] || invMap[(a.product_code||'').toUpperCase()];
      const pb = invMap[b.product_code] || invMap[(b.product_code||'').toUpperCase()];
      const dailyA = pa ? (pa['_xerpDaily'] || 0) : 0;
      const dailyB = pb ? (pb['_xerpDaily'] || 0) : 0;
      const availA = pa ? (typeof pa['가용재고'] === 'number' ? pa['가용재고'] : 0) : 0;
      const availB = pb ? (typeof pb['가용재고'] === 'number' ? pb['가용재고'] : 0) : 0;
      const remainA = dailyA > 0 ? availA / dailyA : 9999;
      const remainB = dailyB > 0 ? availB / dailyB : 9999;
      return remainA - remainB;
    });

    const today = new Date().toISOString().slice(0, 10);
    const created = [];
    const skipped = [];

    // 이번 주 월요일 구하기
    const now = new Date();
    const dayOfWeek = now.getDay() || 7; // 일요일=7
    const monday = new Date(now); monday.setDate(now.getDate() - dayOfWeek + 1); monday.setHours(0,0,0,0);
    const mondayStr = monday.toISOString().slice(0, 10);
    // 거래처별 이번 주 발주 건수 캐시
    const weeklyVendorCount = {};

    for (const item of items) {
      const p = invMap[item.product_code] || invMap[(item.product_code||'').toUpperCase()];
      if (!p || typeof p['가용재고'] !== 'number') { skipped.push({ product_code: item.product_code, reason: '재고 데이터 없음' }); continue; }

      // 생산지 결정
      const origin = p['생산지'] || item.origin || '한국';

      // 전략발주 최적수량 계산
      const calc = calculateOptimalOrder(item.product_code, p, origin);
      if (calc.skip) { skipped.push({ product_code: item.product_code, reason: calc.reason }); continue; }

      const { orderQty, remainDays } = calc;
      const isUrgent = remainDays <= 14;
      const isDanger = remainDays <= 21;

      // 안전 품목은 발주 안 함 (잔여일 > 21일)
      if (!isDanger) { skipped.push({ product_code: item.product_code, reason: `안전 (잔여 ${Math.round(remainDays)}일, 목표재고 ${calc.targetStock.toLocaleString()})` }); continue; }

      // 거래처별 주간 6건 제한 (긴급은 한도 무시)
      const vendor = item.vendor_name || '';
      if (vendor && !isUrgent) {
        if (!(vendor in weeklyVendorCount)) {
          weeklyVendorCount[vendor] = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date>=? AND status!='cancelled' AND status!='취소'`).get(vendor, mondayStr).cnt;
        }
        if (weeklyVendorCount[vendor] >= 6) {
          skipped.push({ product_code: item.product_code, reason: `${vendor} 주간 한도 초과 (${weeklyVendorCount[vendor]}/6건)` });
          continue;
        }
      }

      // 미완료 PO가 있는 품목 스킵 (중복발주 방지)
      const pendingPO = db.prepare(`
        SELECT h.po_number, h.status FROM po_header h
        JOIN po_items i ON i.po_id = h.po_id
        WHERE i.product_code = ? AND h.status IN ('draft','발송','확인','수령중','OS등록대기')
        LIMIT 1
      `).get(item.product_code);
      if (pendingPO) { skipped.push({ product_code: item.product_code, reason: `미완료 PO (${pendingPO.po_number})` }); continue; }
      // 거래처 결정: auto_order_items.vendor_name > products.paper_maker 매핑
      let resolvedVendor = vendor;
      if (!resolvedVendor) {
        const prodInfo = db.prepare('SELECT paper_maker FROM products WHERE product_code=?').get(item.product_code);
        if (prodInfo && prodInfo.paper_maker) {
          resolvedVendor = resolveVendor(prodInfo.paper_maker) || '';
        }
      }

      // PO 생성
      const poNumber = generatePoNumber();
      // origin 결정
      const _aoOriginProd = db.prepare('SELECT origin FROM products WHERE product_code=?').get(item.product_code);
      const _aoOrigin = (_aoOriginProd && _aoOriginProd.origin) || '한국';
      const tx = db.transaction(() => {
        const hdr = db.prepare('INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, origin, po_date) VALUES (?,?,?,?,?,?,?,date(\'now\',\'localtime\'))').run(
          poNumber, '자동발주', resolvedVendor, 'draft', orderQty, '필수 자동발주', _aoOrigin
        );
        db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)').run(
          hdr.lastInsertRowid, item.product_code, p['브랜드'] || '', '', orderQty, '', '자동발주'
        );
        db.prepare('UPDATE auto_order_items SET last_ordered_at=? WHERE id=?').run(new Date().toISOString(), item.id);
        // auto_order_items에 vendor_name도 업데이트 (다음번부터 사용)
        if (resolvedVendor && !vendor) {
          db.prepare('UPDATE auto_order_items SET vendor_name=? WHERE id=?').run(resolvedVendor, item.id);
        }
        return { po_id: hdr.lastInsertRowid, po_number: poNumber };
      });
      const result = tx();
      if (resolvedVendor) weeklyVendorCount[resolvedVendor] = (weeklyVendorCount[resolvedVendor] || 0) + 1;

      // 거래처 이메일이 있으면 자동 발송
      let emailSent = false;
      if (resolvedVendor) {
        const vendorInfo = db.prepare('SELECT * FROM vendors WHERE name=?').get(resolvedVendor);
        if (vendorInfo && vendorInfo.email) {
          try {
            const po = db.prepare('SELECT * FROM po_header WHERE po_id=?').get(result.po_id);
            const poItems = db.prepare('SELECT * FROM po_items WHERE po_id=?').all(result.po_id);
            await sendPOEmail(po, poItems, vendorInfo.email, vendorInfo.name, false, vendorInfo.email_cc || '');
            db.prepare("UPDATE po_header SET status='sent' WHERE po_id=?").run(result.po_id);
            emailSent = true;
          } catch (emailErr) {
            console.warn(`자동발주 이메일 실패 (${item.product_code}):`, emailErr.message);
          }
        }
      }

      created.push({ product_code: item.product_code, ...result, order_qty: orderQty, vendor: resolvedVendor, email_sent: emailSent });
    }
    ok(res, { created, skipped, checked: items.length });
    return;
  }

  // POST /api/auto-order/run-scheduler — 자동발주 스케줄러 수동 즉시 실행
  if (pathname === '/api/auto-order/run-scheduler' && method === 'POST') {
    try {
      await runAutoOrderScheduler();
      ok(res, { success: true, message: '자동발주 스케줄러 수동 실행 완료' });
    } catch(e) {
      fail(res, 500, '스케줄러 실행 실패: ' + e.message);
    }
    return;
  }

  // POST /api/auto-order/run-shipment-check — 출고일 이메일 체크 수동 실행
  if (pathname === '/api/auto-order/run-shipment-check' && method === 'POST') {
    try {
      await runShipmentEmailCheck();
      ok(res, { success: true, message: '출고일 이메일 체크 완료' });
    } catch(e) {
      fail(res, 500, '출고일 체크 실패: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  중국 선적 API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/china-shipment/logs — 선적 이력 조회
  if (pathname === '/api/china-shipment/logs' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM china_shipment_log ORDER BY created_at DESC LIMIT 50').all();
    ok(res, rows);
    return;
  }

  // GET /api/china-shipment/logs/:id — 특정 선적 상세
  const csDetailMatch = pathname.match(/^\/api\/china-shipment\/logs\/(\d+)$/);
  if (csDetailMatch && method === 'GET') {
    const row = db.prepare('SELECT * FROM china_shipment_log WHERE id=?').get(csDetailMatch[1]);
    ok(res, row || null);
    return;
  }

  // POST /api/china-shipment/save — 선적 이력 저장
  if (pathname === '/api/china-shipment/save' && method === 'POST') {
    const body = await readJSON(req);
    const { shipment_date, file_name, total_boxes, total_items, target_boxes, items, notes, status, bl_number, ship_date } = body;
    // eta_date = ship_date + 50일 자동 계산
    let eta_date = '';
    if (ship_date) {
      const d = new Date(ship_date);
      d.setDate(d.getDate() + 50);
      eta_date = d.toISOString().slice(0, 10);
    }
    const result = db.prepare(`INSERT INTO china_shipment_log (shipment_date, file_name, total_boxes, total_items, target_boxes, items_json, notes, status, bl_number, ship_date, eta_date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(
      shipment_date || new Date().toISOString().slice(0,10),
      file_name || '',
      total_boxes || 0,
      total_items || 0,
      target_boxes || 500,
      JSON.stringify(items || []),
      notes || '',
      status || '전달',
      bl_number || '',
      ship_date || '',
      eta_date
    );
    ok(res, { id: result.lastInsertRowid });
    return;
  }

  // PUT /api/china-shipment/logs/:id/status — 상태 변경
  const csStatusMatch = pathname.match(/^\/api\/china-shipment\/logs\/(\d+)\/status$/);
  if (csStatusMatch && method === 'PUT') {
    const body = await readJSON(req);
    db.prepare('UPDATE china_shipment_log SET status=? WHERE id=?').run(body.status, csStatusMatch[1]);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/china-shipment/logs/:id — 선적 이력 삭제
  const csDelMatch = pathname.match(/^\/api\/china-shipment\/logs\/(\d+)$/);
  if (csDelMatch && method === 'DELETE') {
    db.prepare('DELETE FROM china_shipment_log WHERE id=?').run(csDelMatch[1]);
    ok(res, { deleted: true });
    return;
  }

  // PATCH /api/china-shipment/:id — BL번호/선적일/상태 업데이트
  const csPatchMatch = pathname.match(/^\/api\/china-shipment\/(\d+)$/);
  if (csPatchMatch && method === 'PATCH') {
    const body = await readJSON(req);
    const id = csPatchMatch[1];
    const updates = [];
    const params = [];
    if (body.status !== undefined) { updates.push('status=?'); params.push(body.status); }
    if (body.bl_number !== undefined) { updates.push('bl_number=?'); params.push(body.bl_number); }
    if (body.ship_date !== undefined) {
      updates.push('ship_date=?'); params.push(body.ship_date);
      // eta_date 자동 재계산 (ship_date + 50일)
      if (body.ship_date) {
        const d = new Date(body.ship_date);
        d.setDate(d.getDate() + 50);
        updates.push('eta_date=?'); params.push(d.toISOString().slice(0, 10));
      } else {
        updates.push('eta_date=?'); params.push('');
      }
    }
    if (updates.length > 0) {
      params.push(id);
      db.prepare(`UPDATE china_shipment_log SET ${updates.join(',')} WHERE id=?`).run(...params);
    }
    ok(res, { updated: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  VENDOR PORTAL API (업체 포털)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/vendor-portal — 업체 전용 PO 목록
  if (pathname === '/api/vendor-portal' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const email = qs.get('email') || '';
    const token = qs.get('token') || '';
    const vendorNameParam = qs.get('vendor_name') || '';
    if (!email || !token || !verifyVendorToken(email, token)) {
      fail(res, 403, '인증 실패'); return;
    }
    // vendor_name 파라미터가 있으면 이름으로 정확 매칭, 없으면 이메일로 조회
    let vendor;
    if (vendorNameParam) {
      vendor = db.prepare('SELECT * FROM vendors WHERE name = ? AND email = ?').get(vendorNameParam, email);
    }
    if (!vendor) {
      vendor = db.prepare('SELECT * FROM vendors WHERE email = ?').get(email);
    }
    if (!vendor) { fail(res, 404, '등록된 업체가 아닙니다'); return; }

    const enToKo = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'cancelled':'취소', 'os_pending':'OS등록대기', 'os_registered':'OS검증대기' };
    const materialStatusKo = { 'sent':'발주완료', 'confirmed':'확인', 'scheduled':'출고예정', 'shipped':'출고완료' };
    const processStatusKo = { 'waiting':'대기', 'sent':'발주완료', 'confirmed':'확인', 'working':'작업중', 'completed':'완료' };
    const rows = db.prepare('SELECT * FROM po_header WHERE vendor_name = ? ORDER BY po_date DESC, po_id DESC').all(vendor.name);
    for (const r of rows) {
      r.status = enToKo[r.status] || r.status;
      r.material_status_label = materialStatusKo[r.material_status] || r.material_status;
      r.process_status_label = processStatusKo[r.process_status] || r.process_status;
      r.items = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(r.po_id);
      // product_info 데이터 보강 (원자재코드, 원재료용지명, 절, 조판, 후공정체인)
      const pInfo = getProductInfo();
      const postCols = ['재단','인쇄','박/형압','톰슨','봉투가공','세아리','레이져','실크'];
      const steps = [];
      for (const it of r.items) {
        const info = pInfo[it.product_code] || {};
        it.material_code = info['원자재코드'] || info.material_code || '';
        it.material_name = info['원재료용지명'] || info.material_name || '';
        it.cut = info['절'] || '';
        it.imposition = info['조판'] || '';
        it.product_spec = info['제품사양'] || it.spec || '';
        // 품목별 후공정 체인 (순서대로)
        const itemSteps = [];
        postCols.forEach(c => {
          if (info[c] && info[c] !== '0') itemSteps.push({ p: c, v: info[c] });
        });
        it.first_process = itemSteps.length ? itemSteps[0].p : '';
        it.first_process_vendor = itemSteps.length ? itemSteps[0].v : '';
        it.process_chain_full = itemSteps.map(s => s.v + '(' + s.p + ')').join(' → ');
        // PO 전체 후공정 체인 수집
        itemSteps.forEach(s => {
          if (!steps.find(x => x.p === s.p && x.v === s.v)) steps.push(s);
        });
      }
      // 입고처 결정: 원재료→첫번째 후공정 업체, 후공정→다음단계 또는 파주(본사)
      if (vendor.type === '원재료' && steps.length > 0) {
        r.next_destination = steps[0].v + '(' + steps[0].p + ')';
        r.process_chain_label = steps.map(s => s.v + '(' + s.p + ')').join(' → ');
      } else if (vendor.type === '후공정') {
        const chain = r.process_chain ? JSON.parse(r.process_chain) : [];
        const curStep = r.process_step || 0;
        const nextInfo = chain.find(s => s.step === curStep + 1);
        r.next_destination = nextInfo ? nextInfo.vendor + '(' + nextInfo.process + ')' : '파주(본사)';
        r.process_chain_label = chain.length ? chain.map(s => s.vendor + '(' + s.process + ')').join(' → ') : '';
      }
      if (!r.next_destination) r.next_destination = '파주(본사)';
    }
    // 취소 제외 전체 표시, 처리 가능한 것 분리
    const activePOs = rows.filter(r => r.status !== '취소');
    const actionable = activePOs.filter(r => ['발송','확인'].includes(r.status));
    ok(res, { vendor, pos: actionable, allPos: activePOs });
    return;
  }

  // PATCH /api/vendor-portal/po/:id — 업체가 상태 변경
  const vpPatch = pathname.match(/^\/api\/vendor-portal\/po\/(\d+)$/);
  if (vpPatch && method === 'PATCH') {
    const poId = parseInt(vpPatch[1]);
    const body = await readJSON(req);
    const email = body.email || '';
    const token = body.token || '';
    if (!email || !token || !verifyVendorToken(email, token)) {
      fail(res, 403, '인증 실패'); return;
    }
    // 이메일로 등록된 업체 확인 (같은 이메일로 여러 업체 가능)
    const vendorsWithEmail = db.prepare('SELECT * FROM vendors WHERE email = ?').all(email);
    if (!vendorsWithEmail.length) { fail(res, 404, '등록된 업체가 아닙니다'); return; }

    const po = db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(poId);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    // PO의 vendor_name과 매칭되는 vendor 찾기
    const vendor = vendorsWithEmail.find(v => v.name === po.vendor_name || v.name.startsWith(po.vendor_name) || po.vendor_name.startsWith(v.name.slice(0,2)));
    if (!vendor) { fail(res, 403, '본인 발주서가 아닙니다'); return; }

    const action = body.action; // 'confirm' or 'ship'
    const enToKo = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'cancelled':'취소', 'os_pending':'OS등록대기', 'os_registered':'OS검증대기' };
    const koToEn = { '대기':'draft', '발송':'sent', '확인':'confirmed', '수령중':'partial', '완료':'received', '취소':'cancelled', 'OS등록대기':'os_pending', 'OS검증대기':'os_registered' };
    const currentStatus = enToKo[po.status] || po.status;

    let emailResult = null;

    if (action === 'confirm' && currentStatus === '발송') {
      // 업체가 발주 확인
      const beforeConfirm = { status: po.status, mat: po.material_status, proc: po.process_status };
      db.prepare(`UPDATE po_header SET status = 'confirmed', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
      // 파이프라인 서브상태 업데이트 (vendor.type 기준)
      if (vendor.type === '후공정') {
        db.prepare('UPDATE po_header SET process_status=? WHERE po_id=?').run('confirmed', poId);
      } else {
        // 원재료 또는 타입 미설정
        db.prepare('UPDATE po_header SET material_status=? WHERE po_id=?').run('confirmed', poId);
      }
      logPOActivity(poId, 'vendor_confirm', {
        actor: vendor.name, actor_type: vendor.type,
        from_status: beforeConfirm.status, to_status: 'confirmed',
        from_mat: beforeConfirm.mat, to_mat: vendor.type === '후공정' ? beforeConfirm.mat : 'confirmed',
        from_proc: beforeConfirm.proc, to_proc: vendor.type === '후공정' ? 'confirmed' : beforeConfirm.proc,
        details: `${vendor.name} 발주 확인`
      });
      ok(res, { po_id: poId, status: '확인', email: emailResult });
      return;

    } else if (action === 'ship' && currentStatus === '확인') {
      // 업체가 발송 처리 (vendor.type 기준으로 원재료/후공정 분기)
      if (vendor.type === '후공정') {
        // 후공정 업체 발송 → 다음 step 체크
        const currentStep = po.process_step || 0;
        const chain = po.process_chain ? JSON.parse(po.process_chain) : [];
        const nextStepInfo = chain.find(s => s.step === currentStep + 1);

        if (nextStepInfo) {
          // 다음 공정 단계가 있음 → 다음 step PO 자동 생성+발송
          db.prepare(`UPDATE po_header SET status = '확인', process_status='step_done', shipped_at=datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
          logPOActivity(poId, 'vendor_ship', {
            actor: vendor.name, actor_type: vendor.type,
            from_status: po.status, to_status: '확인',
            from_mat: po.material_status, to_mat: po.material_status,
            from_proc: po.process_status, to_proc: 'step_done',
            details: `${vendor.name} 공정 Step ${currentStep} 완료 → Step ${currentStep+1} (${nextStepInfo.process}@${nextStepInfo.vendor}) 자동 트리거`
          });

          // 다음 step PO가 이미 대기 중인지 확인
          const nextPO = db.prepare(`SELECT * FROM po_header WHERE parent_po_id = ? AND process_step = ? AND po_type = '후공정'`).get(po.parent_po_id || poId, currentStep + 1);
          if (nextPO) {
            const nextVendor = db.prepare('SELECT * FROM vendors WHERE name = ?').get(nextPO.vendor_name);
            if (nextVendor && nextVendor.email) {
              const nextItems = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(nextPO.po_id);
              db.prepare(`UPDATE po_header SET status = 'sent', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(nextPO.po_id);
              emailResult = await sendPOEmail(nextPO, nextItems, nextVendor.email, nextVendor.name, true, nextVendor.email_cc);
              console.log(`공정체인 Step ${currentStep}→${currentStep+1}: ${nextPO.po_number} → ${nextVendor.name}`);
            }
          }
          ok(res, { po_id: poId, status: '확인', next_step: currentStep + 1, next_vendor: nextStepInfo.vendor });
          return;
        }

        // 마지막 공정 → OS등록대기 상태로, process_status = completed
        db.prepare(`UPDATE po_header SET status = 'os_pending', process_status='completed', shipped_at=datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
        logPOActivity(poId, 'vendor_ship', {
          actor: vendor.name, actor_type: vendor.type,
          from_status: po.status, to_status: 'os_pending',
          from_mat: po.material_status, to_mat: po.material_status,
          from_proc: po.process_status, to_proc: 'completed',
          details: `${vendor.name} 후공정 최종 완료 발송`
        });
        ok(res, { po_id: poId, status: 'OS등록대기' });
        return;

      } else {
        // 원재료 업체 발송 → material_status = shipped, 같은 날짜 후공정 PO 체인 트리거
        db.prepare(`UPDATE po_header SET material_status='shipped', shipped_at=datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
        logPOActivity(poId, 'vendor_ship', {
          actor: vendor.name, actor_type: vendor.type,
          from_status: po.status, to_status: po.status,
          from_mat: po.material_status, to_mat: 'shipped',
          from_proc: po.process_status, to_proc: po.process_status,
          details: `${vendor.name} 원재료 출고`
        });
        // 후공정 PO 찾기 (같은 날짜, 대기 상태, 후공정 타입)
        const postPOs = db.prepare(`SELECT * FROM po_header WHERE po_date = ? AND status IN ('draft','sent') AND po_type = '후공정'`).all(po.po_date);
        for (const pp of postPOs) {
          const postVendor = db.prepare('SELECT * FROM vendors WHERE name = ?').get(pp.vendor_name);
          if (postVendor && postVendor.email) {
            const ppItems = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(pp.po_id);
            // 후공정 PO를 발송 상태로
            db.prepare(`UPDATE po_header SET status = 'sent', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(pp.po_id);
            emailResult = await sendPOEmail(pp, ppItems, postVendor.email, postVendor.name, true, postVendor.email_cc);
            console.log(`원재료→후공정 체인: ${pp.po_number} → ${postVendor.name} (${postVendor.email})`);
          }
        }
        ok(res, { po_id: poId, status: '확인', chain_triggered: postPOs.length, email: emailResult });
        return;
      }
    }

    fail(res, 400, `현재 상태(${currentStatus})에서 ${action} 처리 불가`);
    return;
  }

  // POST /api/vendor-portal/po/:id/reset-ship — 발송처리 수정 (shipped_at 초기화)
  const resetShipMatch = pathname.match(/^\/api\/vendor-portal\/po\/(\d+)\/reset-ship$/);
  if (resetShipMatch && method === 'POST') {
    const poId = parseInt(resetShipMatch[1]);
    const body = await readJSON(req);
    const email = body.email || '';
    const token = body.token || '';
    if (!verifyVendorToken(email, token)) { fail(res, 403, '인증 실패'); return; }
    const po = db.prepare('SELECT * FROM po_header WHERE po_id=?').get(poId);
    if (!po) { fail(res, 404, 'PO 없음'); return; }
    // shipped_at 초기화 (발송 처리 전 상태로)
    db.prepare(`UPDATE po_header SET shipped_at='', updated_at=datetime('now','localtime') WHERE po_id=?`).run(poId);
    logPOActivity(poId, 'reset_ship', { actor_type: 'vendor', details: '발송처리 수정 요청' });
    ok(res, { po_id: poId });
    return;
  }

  // POST /api/po/:id/reorder — 취소된 PO 재발주
  const reorderMatch = pathname.match(/^\/api\/po\/(\d+)\/reorder$/);
  if (reorderMatch && method === 'POST') {
    const oldPoId = parseInt(reorderMatch[1]);
    const oldPO = db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(oldPoId);
    if (!oldPO) { fail(res, 404, 'PO not found'); return; }
    if (oldPO.status !== 'cancelled') { fail(res, 400, '취소된 발주만 재발주 가능합니다'); return; }

    const oldItems = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(oldPoId);
    const today = new Date();
    const y = today.getFullYear(), m = String(today.getMonth()+1).padStart(2,'0'), d = String(today.getDate()).padStart(2,'0');
    const dateTag = `${y}${m}${d}`;
    const todayCount = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE po_number LIKE ?`).get(`PO-${dateTag}-%`).cnt;
    const poNumber = `PO-${dateTag}-${String(todayCount+1).padStart(3,'0')}`;

    // 새 PO 생성
    const totalQty = oldItems.reduce((s, it) => s + (it.ordered_qty || 0), 0);
    const info = db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, expected_date, total_qty, notes, origin, po_date)
      VALUES (?, ?, ?, 'sent', ?, ?, ?, ?, date('now','localtime'))`).run(
      poNumber, oldPO.po_type, oldPO.vendor_name, oldPO.expected_date || '', totalQty, `재발주 (원본: ${oldPO.po_number})`, oldPO.origin || ''
    );
    const newPoId = info.lastInsertRowid;

    // 품목 복사
    const insItem = db.prepare('INSERT INTO po_items (po_id, product_code, brand, ordered_qty, received_qty, process_type, spec) VALUES (?,?,?,?,0,?,?)');
    for (const it of oldItems) {
      insItem.run(newPoId, it.product_code, it.brand || '', it.ordered_qty || 0, it.process_type || '', it.spec || '');
    }

    // 이메일 발송
    let emailSent = false;
    const newPO = db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(newPoId);
    const newItems = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(newPoId);
    const vendor = db.prepare('SELECT * FROM vendors WHERE name = ?').get(newPO.vendor_name);
    if (vendor && vendor.email) {
      try {
        const isPost = vendor.type === '후공정';
        await sendPOEmail(newPO, newItems, vendor.email, vendor.name, isPost, vendor.email_cc);
        emailSent = true;
        console.log(`재발주 이메일 발송: ${poNumber} → ${vendor.name} (${vendor.email})`);
      } catch (e) { console.error('재발주 이메일 실패:', e.message); }
    }

    // Google Sheet 동기화
    try {
      await appendToGoogleSheet(newItems.map(it => ({
        order_date: newPO.po_date || '', product_code: it.product_code || '',
        product_name: it.brand || '', material_name: it.spec || '',
        paper_maker: newPO.vendor_name || '', order_qty: it.ordered_qty || 0, product_spec: it.spec || ''
      })));
    } catch(e) { console.warn('재발주 시트 동기화 실패:', e.message); }

    ok(res, { ok: true, new_po_id: newPoId, po_number: poNumber, email_sent: emailSent, old_po_number: oldPO.po_number });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  VENDOR SHIPMENT SCHEDULE API (출하 일정)
  // ════════════════════════════════════════════════════════════════════

  // POST /api/vendor-portal/material-shipped — 원재료 업체 출고 완료
  if (pathname === '/api/vendor-portal/material-shipped' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id } = body;
    if (!po_id) { fail(res, 400, 'po_id 필수'); return; }
    db.prepare("UPDATE po_header SET material_status='shipped' WHERE po_id=?").run(po_id);
    // 납품 스케줄 상태도 업데이트
    db.prepare("UPDATE vendor_shipment_schedule SET status='shipped' WHERE po_id=?").run(po_id);
    logPOActivity(po_id, 'material_shipped', { actor_type: 'material', to_mat: 'shipped', details: '원재료 출고 완료' });
    ok(res, { po_id, material_status: 'shipped' });
    return;
  }

  // POST /api/vendor-portal/set-shipment — 업체가 출하 일정 등록/수정
  if (pathname === '/api/vendor-portal/set-shipment' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, ship_date, ship_time, post_vendor_name } = body;
    if (!po_id || !ship_date) { fail(res, 400, '필수 항목 누락'); return; }
    const po = db.prepare('SELECT po_number, vendor_name FROM po_header WHERE po_id=?').get(po_id);
    if (!po) { fail(res, 404, 'PO 없음'); return; }
    const postVendor = db.prepare('SELECT email FROM vendors WHERE name=?').get(post_vendor_name || '');
    const postEmail = postVendor ? postVendor.email : '';
    const existing = db.prepare('SELECT id FROM vendor_shipment_schedule WHERE po_id=?').get(po_id);
    if (existing) {
      db.prepare(`UPDATE vendor_shipment_schedule SET ship_date=?, ship_time=?, post_vendor_name=?, post_vendor_email=?, updated_at=datetime('now','localtime') WHERE po_id=?`)
        .run(ship_date, ship_time || 'AM', post_vendor_name || '', postEmail, po_id);
    } else {
      db.prepare(`INSERT INTO vendor_shipment_schedule (po_id, po_number, vendor_name, ship_date, ship_time, post_vendor_name, post_vendor_email) VALUES (?,?,?,?,?,?,?)`)
        .run(po_id, po.po_number, po.vendor_name, ship_date, ship_time || 'AM', post_vendor_name || '', postEmail);
    }
    // 출하 일정 등록 시 원재료 파이프라인 상태를 '출고예정'으로 업데이트
    db.prepare("UPDATE po_header SET material_status='scheduled' WHERE po_id=?").run(po_id);
    logPOActivity(po_id, 'shipment_scheduled', {
      actor: po.vendor_name, actor_type: 'material',
      to_mat: 'scheduled',
      details: `출고일정: ${ship_date} ${ship_time || 'AM'} → ${post_vendor_name || ''}`
    });
    ok(res, { po_id, ship_date, ship_time: ship_time || 'AM', post_vendor_name });
    return;
  }

  // POST /api/vendor-portal/item-ship-date — 품목별 출고일 저장
  if (pathname === '/api/vendor-portal/item-ship-date' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, item_id, ship_date, email, token } = body;
    if (email && token && !verifyVendorToken(email, token)) { fail(res, 403, '인증 실패'); return; }
    if (!po_id || !ship_date) { fail(res, 400, '필수 항목 누락'); return; }
    if (item_id !== undefined && item_id !== null) {
      db.prepare('UPDATE po_items SET ship_date=? WHERE po_id=? AND item_id=?').run(ship_date, po_id, item_id);
    } else {
      db.prepare('UPDATE po_items SET ship_date=? WHERE po_id=?').run(ship_date, po_id);
    }
    ok(res, { saved: true });
    return;
  }

  // POST /api/vendor-portal/items-ship-dates — 품목별 출고일 일괄 저장
  if (pathname === '/api/vendor-portal/items-ship-dates' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, dates, email, token } = body;
    if (email && token && !verifyVendorToken(email, token)) { fail(res, 403, '인증 실패'); return; }
    if (!po_id || !Array.isArray(dates)) { fail(res, 400, '필수 항목 누락'); return; }
    const stmt = db.prepare('UPDATE po_items SET ship_date=? WHERE po_id=? AND item_id=?');
    const tx = db.transaction(() => { for (const d of dates) stmt.run(d.ship_date, po_id, d.item_id); });
    tx();
    ok(res, { saved: dates.length });
    return;
  }

  // GET /api/vendor-portal/shipment-schedule — 출하 일정 조회
  if (pathname === '/api/vendor-portal/shipment-schedule' && method === 'GET') {
    const poId = parsed.searchParams.get('po_id');
    if (poId) {
      const schedule = db.prepare('SELECT * FROM vendor_shipment_schedule WHERE po_id=?').get(poId);
      ok(res, schedule || null);
    } else {
      const all = db.prepare('SELECT * FROM vendor_shipment_schedule ORDER BY ship_date').all();
      ok(res, all);
    }
    return;
  }

  // GET /api/vendor-portal/lead-time — 벤더 포털 공정 리드타임 조회
  if (pathname === '/api/vendor-portal/lead-time' && method === 'GET') {
    const email = parsed.searchParams.get('email') || '';
    const token = parsed.searchParams.get('token') || '';
    const vendorName = parsed.searchParams.get('vendor_name') || '';
    if (!email || !token || !verifyVendorToken(email, token)) { fail(res, 403, '인증 실패'); return; }
    if (!vendorName) { fail(res, 400, 'vendor_name 필수'); return; }

    const DEFAULT_LEAD_TIMES = [
      { process_type: '재단', default_days: 1 },
      { process_type: '인쇄', default_days: 3 },
      { process_type: '박/형압', default_days: 2 },
      { process_type: '톰슨', default_days: 2 },
      { process_type: '봉투가공', default_days: 3 },
      { process_type: '세아리', default_days: 2 },
      { process_type: '레이져', default_days: 2 },
      { process_type: '실크', default_days: 3 },
    ];

    const saved = db.prepare('SELECT * FROM process_lead_time WHERE vendor_name=?').all(vendorName);
    const savedMap = {};
    for (const row of saved) savedMap[row.process_type] = row;

    const result = DEFAULT_LEAD_TIMES.map(d => {
      const s = savedMap[d.process_type];
      return {
        process_type: d.process_type,
        default_days: s ? (s.default_days ?? d.default_days) : d.default_days,
        adjusted_days: s ? s.adjusted_days : null,
        adjusted_reason: s ? (s.adjusted_reason || '') : '',
      };
    });
    // 수정 이력 (최근 20건)
    const history = db.prepare('SELECT process_type, old_days, new_days, changed_at FROM lead_time_history WHERE vendor_name=? ORDER BY changed_at DESC LIMIT 20').all(vendorName);
    ok(res, { rows: result, history });
    return;
  }

  // POST /api/vendor-portal/lead-time — 벤더 포털 공정 리드타임 저장
  if (pathname === '/api/vendor-portal/lead-time' && method === 'POST') {
    const body = await readJSON(req);
    const { email, token, vendor_name, lead_times } = body;
    if (!email || !token || !verifyVendorToken(email, token)) { fail(res, 403, '인증 실패'); return; }
    if (!vendor_name) { fail(res, 400, 'vendor_name 필수'); return; }
    if (!Array.isArray(lead_times) || lead_times.length === 0) { fail(res, 400, 'lead_times 필수'); return; }

    const upsert = db.prepare(`
      INSERT INTO process_lead_time (vendor_name, process_type, default_days, adjusted_days, adjusted_reason)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(vendor_name, process_type) DO UPDATE SET
        adjusted_days=excluded.adjusted_days,
        adjusted_reason=excluded.adjusted_reason,
        updated_at=datetime('now','localtime')
    `);

    // 변경 전 값 조회 (이력 기록용)
    const prevMap = {};
    const prevRows = db.prepare('SELECT process_type, adjusted_days, default_days FROM process_lead_time WHERE vendor_name=?').all(vendor_name);
    for (const r of prevRows) prevMap[r.process_type] = r.adjusted_days ?? r.default_days;

    const logStmt = db.prepare('INSERT INTO lead_time_history (vendor_name, process_type, old_days, new_days) VALUES (?,?,?,?)');

    const upsertAll = db.transaction((items) => {
      for (const lt of items) {
        const oldVal = prevMap[lt.process_type] ?? lt.default_days;
        const newVal = lt.adjusted_days ?? lt.default_days;
        if (oldVal !== newVal) {
          logStmt.run(vendor_name, lt.process_type, oldVal, newVal);
        }
        upsert.run(vendor_name, lt.process_type, lt.default_days ?? 1, lt.adjusted_days ?? null, lt.adjusted_reason || '');
      }
    });
    upsertAll(lead_times);

    console.log(`[vendor-portal/lead-time] ${vendor_name} (${email}) — ${lead_times.length}개 공정 리드타임 저장`);
    ok(res, { ok: true, vendor_name, saved: lead_times.length });
    return;
  }

  // GET /api/vendor-portal/trade-doc — 업체 포털 거래명세서 조회
  if (pathname === '/api/vendor-portal/trade-doc' && method === 'GET') {
    const poId = parsed.searchParams.get('po_id');
    const email = parsed.searchParams.get('email') || '';
    const token = parsed.searchParams.get('token') || '';
    if (!verifyVendorToken(email, token)) { fail(res, 403, '인증 실패'); return; }
    const doc = db.prepare('SELECT * FROM trade_document WHERE po_id=? ORDER BY id DESC LIMIT 1').get(poId);
    if (!doc) { ok(res, null); return; }
    doc.items = JSON.parse(doc.items_json || '[]');
    doc.vendor_modified = doc.vendor_modified_json ? JSON.parse(doc.vendor_modified_json) : null;
    ok(res, doc);
    return;
  }

  // POST /api/vendor-portal/update-trade-doc — 업체 포털 거래명세서 단가 수정
  if (pathname === '/api/vendor-portal/update-trade-doc' && method === 'POST') {
    const body = await readJSON(req);
    const { doc_id, email, token, modified_items, memo } = body;
    if (!verifyVendorToken(email || '', token || '')) { fail(res, 403, '인증 실패'); return; }
    if (!doc_id) { fail(res, 400, 'doc_id 필수'); return; }
    const doc = db.prepare('SELECT * FROM trade_document WHERE id=?').get(doc_id);
    if (!doc) { fail(res, 404, '문서 없음'); return; }

    const originalItems = JSON.parse(doc.items_json || '[]');
    const modifiedItems = modified_items || [];
    let hasDiff = false;
    for (let i = 0; i < modifiedItems.length; i++) {
      const orig = originalItems[i];
      const mod = modifiedItems[i];
      if (orig && mod && (orig.unit_price !== mod.unit_price || orig.qty !== mod.qty)) {
        hasDiff = true;
        break;
      }
    }

    db.prepare(`UPDATE trade_document SET vendor_modified_json=?, vendor_memo=?, price_diff=?, status='vendor_confirmed', confirmed_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?`)
      .run(JSON.stringify(modifiedItems), memo || '', hasDiff ? 1 : 0, doc_id);

    logPOActivity(doc.po_id, 'trade_doc_updated', {
      actor: doc.vendor_name, actor_type: doc.vendor_type,
      details: hasDiff ? `거래명세서 단가 수정 (사유: ${memo || '없음'})` : '거래명세서 확인 (수정 없음)'
    });

    ok(res, { doc_id, price_diff: hasDiff, status: 'vendor_confirmed' });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PROCESS LEAD TIME API (공정 리드타임)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/process-lead-time — 공정 리드타임 조회
  if (pathname === '/api/process-lead-time' && method === 'GET') {
    const vn = parsed.searchParams.get('vendor_name');
    if (vn) {
      const rows = db.prepare('SELECT * FROM process_lead_time WHERE vendor_name=?').all(vn);
      ok(res, rows);
    } else {
      const rows = db.prepare('SELECT * FROM process_lead_time ORDER BY vendor_name, process_type').all();
      ok(res, rows);
    }
    return;
  }

  // POST /api/process-lead-time — 공정 리드타임 등록/수정
  if (pathname === '/api/process-lead-time' && method === 'POST') {
    const body = await readJSON(req);
    const { vendor_name, process_type, default_days, adjusted_days, adjusted_reason } = body;
    if (!vendor_name || !process_type) { fail(res, 400, '필수 항목 누락'); return; }
    db.prepare(`INSERT INTO process_lead_time (vendor_name, process_type, default_days, adjusted_days, adjusted_reason)
      VALUES (?,?,?,?,?)
      ON CONFLICT(vendor_name, process_type) DO UPDATE SET
        default_days=COALESCE(excluded.default_days, default_days),
        adjusted_days=excluded.adjusted_days,
        adjusted_reason=excluded.adjusted_reason,
        updated_at=datetime('now','localtime')
    `).run(vendor_name, process_type, default_days || 1, adjusted_days || null, adjusted_reason || '');
    ok(res, { vendor_name, process_type });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  POST PROCESS COST API (후공정 단가 관리)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/post-process/prices — 단가 마스터 조회
  if (pathname === '/api/post-process/prices' && method === 'GET') {
    const vendor = parsed.searchParams.get('vendor_name');
    const process = parsed.searchParams.get('process_type');
    let sql = 'SELECT * FROM post_process_price WHERE 1=1';
    const params = [];
    if (vendor) { sql += ' AND vendor_name=?'; params.push(vendor); }
    if (process) { sql += ' AND process_type=?'; params.push(process); }
    sql += ' ORDER BY process_type, spec_condition, unit_price';
    ok(res, db.prepare(sql).all(...params));
    return;
  }

  // POST /api/post-process/prices — 단가 등록/수정
  if (pathname === '/api/post-process/prices' && method === 'POST') {
    const body = await readJSON(req);
    const { vendor_name, process_type, price_type, price_tier, spec_condition, unit_price, effective_from, notes } = body;
    if (!vendor_name || !process_type) { fail(res, 400, '필수 항목 누락'); return; }
    const info = db.prepare(`INSERT INTO post_process_price (vendor_name, process_type, price_type, price_tier, spec_condition, unit_price, effective_from, notes)
      VALUES (?,?,?,?,?,?,?,?)`).run(
      vendor_name, process_type, price_type||'per_unit', price_tier||'', spec_condition||'', unit_price||0, effective_from||'', notes||''
    );
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  // GET /api/post-process/history — 거래 이력 조회
  if (pathname === '/api/post-process/history' && method === 'GET') {
    const vendor = parsed.searchParams.get('vendor_name');
    const product = parsed.searchParams.get('product_code');
    const month = parsed.searchParams.get('month');
    const process = parsed.searchParams.get('process_type');
    let sql = 'SELECT * FROM post_process_history WHERE 1=1';
    const params = [];
    if (vendor) { sql += ' AND vendor_name=?'; params.push(vendor); }
    if (product) { sql += ' AND product_code=?'; params.push(product); }
    if (month) { sql += ' AND month=?'; params.push(month); }
    if (process) { sql += ' AND process_type=?'; params.push(process); }
    sql += ' ORDER BY month DESC, date DESC LIMIT 500';
    ok(res, db.prepare(sql).all(...params));
    return;
  }

  // GET /api/post-process/product-map — 제품별 후공정 매핑
  if (pathname === '/api/post-process/product-map' && method === 'GET') {
    const product = parsed.searchParams.get('product_code');
    const vendor = parsed.searchParams.get('vendor_name');
    let sql = 'SELECT * FROM product_process_map WHERE 1=1';
    const params = [];
    if (product) { sql += ' AND product_code=?'; params.push(product); }
    if (vendor) { sql += ' AND vendor_name=?'; params.push(vendor); }
    sql += ' ORDER BY product_code, process_type';
    ok(res, db.prepare(sql).all(...params));
    return;
  }

  // GET /api/post-process/summary — 후공정 단가 요약 (대시보드용)
  if (pathname === '/api/post-process/summary' && method === 'GET') {
    const vendor = parsed.searchParams.get('vendor_name') || '코리아패키지';
    const isAll = !parsed.searchParams.get('vendor_name');
    const whereVendor = isAll ? '1=1' : 'vendor_name=?';
    const vendorParam = isAll ? [] : [vendor];

    // 월별 총액
    const monthly = db.prepare(`SELECT month, SUM(amount) as total, COUNT(*) as cnt FROM post_process_history WHERE ${whereVendor} GROUP BY month ORDER BY month`).all(...vendorParam);

    // 공정별 총액
    const byProcess = db.prepare(`SELECT process_type, SUM(amount) as total, COUNT(*) as cnt, AVG(unit_price) as avg_price FROM post_process_history WHERE ${whereVendor} AND unit_price>0 GROUP BY process_type ORDER BY total DESC`).all(...vendorParam);

    // 단가 변동 감지 (같은 제품+공정인데 단가가 다른 경우)
    const priceChanges = db.prepare(`
      SELECT product_code, process_type,
        MIN(unit_price) as min_price, MAX(unit_price) as max_price,
        COUNT(DISTINCT unit_price) as price_count,
        GROUP_CONCAT(DISTINCT month) as months
      FROM post_process_history
      WHERE ${whereVendor} AND unit_price > 0
      GROUP BY product_code, process_type
      HAVING COUNT(DISTINCT unit_price) > 1
      ORDER BY (MAX(unit_price) - MIN(unit_price)) DESC
      LIMIT 20
    `).all(...vendorParam);

    // 제품별 후공정 원가 TOP 15
    const topProducts = db.prepare(`
      SELECT product_code, SUM(amount) as total, COUNT(DISTINCT process_type) as process_count,
        GROUP_CONCAT(DISTINCT process_type) as processes, GROUP_CONCAT(DISTINCT month) as months
      FROM post_process_history WHERE ${whereVendor}
      GROUP BY product_code ORDER BY total DESC LIMIT 15
    `).all(...vendorParam);

    // ── 추가 분석 데이터 ──────────────────────────────────────────────

    // 1. 전월 대비 변화율 (MoM)
    let momChange = null;
    if (monthly.length >= 2) {
      const cur = monthly[monthly.length - 1];
      const prev = monthly[monthly.length - 2];
      const changePct = prev.total > 0 ? Math.round(((cur.total - prev.total) / prev.total) * 1000) / 10 : 0;
      momChange = {
        current_month: cur.month,
        current_total: cur.total,
        prev_month: prev.month,
        prev_total: prev.total,
        change_pct: changePct,
        change_amount: cur.total - prev.total
      };
    } else if (monthly.length === 1) {
      const cur = monthly[0];
      momChange = { current_month: cur.month, current_total: cur.total, prev_month: null, prev_total: 0, change_pct: 0, change_amount: 0 };
    }

    // 2. 공정별 비중 (processShare)
    const grandTotal = byProcess.reduce((s, r) => s + (r.total || 0), 0);
    const processShare = byProcess.map(r => ({
      process_type: r.process_type,
      total: r.total,
      share_pct: grandTotal > 0 ? Math.round((r.total / grandTotal) * 1000) / 10 : 0
    }));

    // 3. 업체 비교 (전체 조회 시)
    let vendorComparison = null;
    if (isAll) {
      const vendorStats = db.prepare(`
        SELECT vendor_name, SUM(amount) as total, COUNT(DISTINCT month) as months
        FROM post_process_history
        GROUP BY vendor_name
        ORDER BY total DESC
      `).all();
      vendorComparison = vendorStats.map(v => ({
        vendor_name: v.vendor_name,
        total: v.total,
        months: v.months,
        avg_monthly: v.months > 0 ? Math.round(v.total / v.months) : 0
      }));
    }

    // 4. 자동 알림 생성 (alerts)
    const alerts = [];

    // price_up: 단가 10% 이상 인상된 제품
    priceChanges.forEach(r => {
      if (r.min_price > 0 && r.max_price > r.min_price * 1.1) {
        const pct = Math.round(((r.max_price - r.min_price) / r.min_price) * 1000) / 10;
        alerts.push({
          type: 'price_up',
          message: `${r.product_code} ${r.process_type} 단가 ${r.min_price.toLocaleString()}→${r.max_price.toLocaleString()}원 (${pct}% 인상)`,
          severity: pct >= 30 ? 'critical' : 'warning',
          action: '업체 단가 협의 필요'
        });
      }
    });

    // high_cost: 최근 월이 이전 월 평균 대비 20% 이상 높음
    if (monthly.length >= 3) {
      const recent = monthly[monthly.length - 1];
      const prevMonths = monthly.slice(0, -1);
      const prevAvg = prevMonths.reduce((s, r) => s + r.total, 0) / prevMonths.length;
      if (recent.total > prevAvg * 1.2) {
        const pct = Math.round(((recent.total - prevAvg) / prevAvg) * 1000) / 10;
        alerts.push({
          type: 'high_cost',
          message: `${recent.month} 후공정 비용 ${recent.total.toLocaleString()}원 (전월 평균 대비 +${pct}%)`,
          severity: pct >= 50 ? 'warning' : 'info',
          action: '비용 증가 원인 확인'
        });
      }
    }

    // vendor_gap: 동일 공정에서 업체간 단가 차이 20% 이상
    if (isAll) {
      const vendorGapRows = db.prepare(`
        SELECT process_type, vendor_name, AVG(unit_price) as avg_price
        FROM post_process_history
        WHERE unit_price > 0
        GROUP BY process_type, vendor_name
      `).all();
      const byProcVendor = {};
      vendorGapRows.forEach(r => {
        if (!byProcVendor[r.process_type]) byProcVendor[r.process_type] = [];
        byProcVendor[r.process_type].push(r);
      });
      Object.entries(byProcVendor).forEach(([proc, vendors]) => {
        if (vendors.length < 2) return;
        const prices = vendors.map(v => v.avg_price).sort((a, b) => a - b);
        const lo = prices[0], hi = prices[prices.length - 1];
        if (lo > 0 && hi > lo * 1.2) {
          const pct = Math.round(((hi - lo) / lo) * 1000) / 10;
          const loV = vendors.find(v => v.avg_price === lo);
          const hiV = vendors.find(v => v.avg_price === hi);
          alerts.push({
            type: 'vendor_gap',
            message: `${proc} 공정 업체간 단가 차이 ${pct}% (${loV.vendor_name} ${Math.round(lo).toLocaleString()}원 vs ${hiV.vendor_name} ${Math.round(hi).toLocaleString()}원)`,
            severity: 'info',
            action: '저가 업체 우선 발주 검토'
          });
        }
      });
    }

    // severity 정렬: critical > warning > info, 최대 10개
    const severityOrder = { critical: 0, warning: 1, info: 2 };
    alerts.sort((a, b) => (severityOrder[a.severity] || 2) - (severityOrder[b.severity] || 2));
    const alertsTop10 = alerts.slice(0, 10);

    ok(res, { monthly, byProcess, priceChanges, topProducts, momChange, processShare, vendorComparison, alerts: alertsTop10 });
    return;
  }

  // GET /api/post-process/estimate — PO 예상 후공정 비용 산출
  if (pathname === '/api/post-process/estimate' && method === 'GET') {
    const productCode = parsed.searchParams.get('product_code');
    if (!productCode) { fail(res, 400, 'product_code 필수'); return; }

    // 이 제품의 후공정 이력에서 가장 최근 단가 기반 예상
    const mapping = db.prepare(
      'SELECT * FROM product_process_map WHERE product_code=? ORDER BY process_type'
    ).all(productCode);

    let estimatedTotal = 0;
    const details = mapping.map(m => {
      estimatedTotal += m.last_amount || 0;
      return {
        process_type: m.process_type,
        vendor_name: m.vendor_name,
        default_spec: m.default_spec,
        estimated_price: m.default_price,
        last_amount: m.last_amount,
        occurrence: m.occurrence,
        last_month: m.last_month,
      };
    });

    ok(res, { product_code: productCode, estimated_total: estimatedTotal, processes: details });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  TRADE DOCUMENT API (거래명세서)
  // ════════════════════════════════════════════════════════════════════

  // POST /api/trade-document — 거래명세서 생성
  if (pathname === '/api/trade-document' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, vendor_name, vendor_type, items } = body;
    if (!po_id) { fail(res, 400, 'po_id 필수'); return; }
    const po = db.prepare('SELECT po_number FROM po_header WHERE po_id=?').get(po_id);
    if (!po) { fail(res, 404, 'PO 없음'); return; }
    const r = db.prepare(`INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')`)
      .run(po_id, po.po_number, vendor_name || '', vendor_type || 'material', JSON.stringify(items || []));
    ok(res, { id: r.lastInsertRowid });
    return;
  }

  // GET /api/trade-document — 거래명세서 목록 조회 (필터링)
  if (pathname === '/api/trade-document' && method === 'GET') {
    const params = parsed.searchParams;
    let q = 'SELECT * FROM trade_document WHERE 1=1';
    const args = [];
    if (parsed.searchParams.get('status')) { q += ' AND status=?'; args.push(parsed.searchParams.get('status')); }
    if (parsed.searchParams.get('vendor_name')) { q += ' AND vendor_name=?'; args.push(parsed.searchParams.get('vendor_name')); }
    if (parsed.searchParams.get('po_id')) { q += ' AND po_id=?'; args.push(parsed.searchParams.get('po_id')); }
    q += ' ORDER BY created_at DESC';
    ok(res, db.prepare(q).all(...args));
    return;
  }

  // PATCH /api/trade-document/:id — 거래명세서 수정 (업체 확인, 관리자 승인 등)
  const tradeDocPatch = pathname.match(/^\/api\/trade-document\/(\d+)$/);
  if (tradeDocPatch && method === 'PATCH') {
    const docId = tradeDocPatch[1];
    const doc = db.prepare('SELECT * FROM trade_document WHERE id=?').get(docId);
    if (!doc) { fail(res, 404, '문서 없음'); return; }
    const body = await readJSON(req);
    const sets = [];
    const vals = [];
    if (body.vendor_modified_json !== undefined) {
      sets.push('vendor_modified_json=?');
      vals.push(typeof body.vendor_modified_json === 'string' ? body.vendor_modified_json : JSON.stringify(body.vendor_modified_json));
    }
    if (body.vendor_memo !== undefined) { sets.push('vendor_memo=?'); vals.push(body.vendor_memo); }
    if (body.status !== undefined) {
      sets.push('status=?'); vals.push(body.status);
      if (body.status === 'vendor_confirmed') { sets.push("confirmed_at=datetime('now','localtime')"); }
      if (body.status === 'approved') { sets.push("approved_at=datetime('now','localtime')"); }
    }
    if (body.price_diff !== undefined) { sets.push('price_diff=?'); vals.push(body.price_diff ? 1 : 0); }
    if (sets.length === 0) { fail(res, 400, '수정 항목 없음'); return; }
    sets.push("updated_at=datetime('now','localtime')");
    vals.push(docId);
    db.prepare(`UPDATE trade_document SET ${sets.join(',')} WHERE id=?`).run(...vals);
    ok(res, { id: parseInt(docId) });
    return;
  }

  // GET /api/trade-document/review — 검토 대기 목록 (vendor_confirmed)
  if (pathname === '/api/trade-document/review' && method === 'GET') {
    const docs = db.prepare(`SELECT * FROM trade_document WHERE status='vendor_confirmed' ORDER BY confirmed_at DESC`).all();
    docs.forEach(d => {
      d.items = JSON.parse(d.items_json || '[]');
      d.vendor_modified = d.vendor_modified_json ? JSON.parse(d.vendor_modified_json) : null;
    });
    ok(res, docs);
    return;
  }

  // POST /api/trade-document/:id/approve — 거래명세서 승인 (재고 등록 연동)
  const approveMatch = pathname.match(/^\/api\/trade-document\/(\d+)\/approve$/);
  if (approveMatch && method === 'POST') {
    const docId = parseInt(approveMatch[1]);
    const doc = db.prepare('SELECT * FROM trade_document WHERE id=?').get(docId);
    if (!doc) { fail(res, 404, '문서 없음'); return; }

    if (doc.price_diff && !doc.vendor_memo) {
      fail(res, 400, '단가 차이가 있으나 수정 사유가 없습니다. 업체에 사유 입력을 요청하세요.');
      return;
    }

    db.prepare(`UPDATE trade_document SET status='approved', approved_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?`).run(docId);

    logPOActivity(doc.po_id, 'trade_doc_approved', {
      actor_type: 'admin',
      details: doc.price_diff ? `거래명세서 승인 (단가 수정 있음, 사유: ${doc.vendor_memo})` : '거래명세서 승인'
    });

    ok(res, { doc_id: docId, status: 'approved' });
    return;
  }

  // GET /api/trade-document/management — 거래명세서 관리 (업체별+월별 그룹, 가격변동 감지)
  if (pathname === '/api/trade-document/management' && method === 'GET') {
    const from = parsed.searchParams.get('from') || '';
    const to = parsed.searchParams.get('to') || '';
    const vendor = parsed.searchParams.get('vendor') || '';

    let q = `SELECT td.*, ph.po_date FROM trade_document td LEFT JOIN po_header ph ON td.po_id=ph.po_id WHERE td.status IN ('sent','vendor_confirmed','approved')`;
    const args = [];
    if (from) { q += ` AND ph.po_date >= ?`; args.push(from); }
    if (to) { q += ` AND ph.po_date <= ?`; args.push(to); }
    if (vendor) { q += ` AND td.vendor_name = ?`; args.push(vendor); }
    q += ` ORDER BY ph.po_date DESC, td.id DESC`;

    const docs = db.prepare(q).all(...args);

    // 가격변동 감지를 위해 업체+품목별 가격 이력 구축
    const priceHistory = {}; // { vendor_product: [price1, price2, ...] }

    const results = docs.map(doc => {
      const items = JSON.parse(doc.items_json || '[]');
      const modified = doc.vendor_modified_json ? JSON.parse(doc.vendor_modified_json) : null;
      let totalAmount = 0;
      let hasPriceChange = false;

      const enrichedItems = items.map((item, i) => {
        const mod = modified && modified[i] ? modified[i] : null;
        const currentPrice = mod ? mod.unit_price : item.unit_price;
        const qty = mod ? (mod.qty || item.qty) : item.qty;
        const amount = (currentPrice || 0) * (qty || 0);
        totalAmount += amount;

        // 가격 변동 감지 (같은 업체+품목의 직전 거래 대비)
        const key = `${doc.vendor_name}::${item.product_code}`;
        if (!priceHistory[key]) priceHistory[key] = [];
        const prev = priceHistory[key].length > 0 ? priceHistory[key][priceHistory[key].length - 1] : null;
        let priceChangeRate = 0;
        if (prev !== null && prev > 0 && currentPrice > 0 && currentPrice !== prev) {
          priceChangeRate = ((currentPrice - prev) / prev * 100);
          hasPriceChange = true;
        }
        priceHistory[key].push(currentPrice || 0);

        return {
          product_code: item.product_code,
          product_name: item.product_name || '',
          material_name: item.material_name || '',
          qty: qty || 0,
          cut: item.cut || '',
          imposition: item.imposition || '',
          unit_price: currentPrice || 0,
          last_price: item.last_price || 0,
          amount,
          price_change_rate: Math.round(priceChangeRate * 10) / 10,
          modified: !!mod && mod.unit_price !== item.unit_price
        };
      });

      return {
        id: doc.id,
        po_id: doc.po_id,
        po_number: doc.po_number,
        po_date: doc.po_date || '',
        vendor_name: doc.vendor_name,
        vendor_type: doc.vendor_type,
        status: doc.status,
        confirmed_at: doc.confirmed_at || '',
        approved_at: doc.approved_at || '',
        vendor_memo: doc.vendor_memo || '',
        price_diff: doc.price_diff || 0,
        items: enrichedItems,
        total_amount: totalAmount,
        tax: Math.round(totalAmount * 0.1),
        has_price_change: hasPriceChange
      };
    });

    // 업체 목록도 함께 반환
    const vendorList = db.prepare(`SELECT DISTINCT vendor_name FROM trade_document WHERE status IN ('sent','vendor_confirmed','approved') ORDER BY vendor_name`).all().map(r => r.vendor_name);

    ok(res, { docs: results, vendors: vendorList });
    return;
  }

  // GET /api/trade-document/export — 엑셀 다운로드
  if (pathname === '/api/trade-document/export' && method === 'GET') {
    const from = parsed.searchParams.get('from') || '';
    const to = parsed.searchParams.get('to') || '';
    const vendor = parsed.searchParams.get('vendor') || '';

    let q = `SELECT td.*, ph.po_date FROM trade_document td LEFT JOIN po_header ph ON td.po_id=ph.po_id WHERE td.status IN ('vendor_confirmed','approved')`;
    const args = [];
    if (from) { q += ` AND ph.po_date >= ?`; args.push(from); }
    if (to) { q += ` AND ph.po_date <= ?`; args.push(to); }
    if (vendor) { q += ` AND td.vendor_name = ?`; args.push(vendor); }
    q += ` ORDER BY td.vendor_name, ph.po_date DESC`;

    const docs = db.prepare(q).all(...args);
    const piMap = getProductInfo();

    // CSV 생성 (엑셀 호환)
    const BOM = '\uFEFF';
    let csv = BOM + '일자,PO번호,업체명,유형,품목코드,품명/원재료,수량,R(연),단가,금액,세액,합계,상태,가격변동\n';

    for (const doc of docs) {
      const items = JSON.parse(doc.items_json || '[]');
      const modified = doc.vendor_modified_json ? JSON.parse(doc.vendor_modified_json) : null;
      const statusLabel = doc.status === 'approved' ? '승인' : doc.status === 'vendor_confirmed' ? '확인' : '발송';
      const typeLabel = doc.vendor_type === 'process' ? '후공정' : '원재료';

      items.forEach((item, i) => {
        const mod = modified && modified[i] ? modified[i] : null;
        const price = mod ? mod.unit_price : item.unit_price;
        const qty = item.qty || 0;
        const amount = (price || 0) * qty;
        const tax = Math.round(amount * 0.1);
        const pi = piMap[item.product_code] || {};
        const cut = parseFloat(item.cut || pi['절']) || 0;
        const imp = parseFloat(item.imposition || pi['조판']) || 0;
        const ream = (cut && imp) ? (qty / 500 / cut / imp).toFixed(1) : '';
        const priceChanged = mod && mod.unit_price !== item.unit_price;
        const changePct = item.last_price && item.last_price > 0 && price !== item.last_price
          ? ((price - item.last_price) / item.last_price * 100).toFixed(1) + '%' : '';

        csv += `${doc.po_date || ''},${doc.po_number},${doc.vendor_name},${typeLabel},${item.product_code},${item.material_name || item.product_name || ''},${qty},${ream},${price || 0},${amount},${tax},${amount + tax},${statusLabel},${priceChanged ? '변동' : ''}${changePct}\n`;
      });
    }

    res.writeHead(200, {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="trade_docs_${from||'all'}_${to||'all'}.csv"`
    });
    res.end(csv);
    return;
  }

  // PATCH /api/po/:id/os — OS번호 등록 (PO 전체 또는 제품별)
  const osReg = pathname.match(/^\/api\/po\/(\d+)\/os$/);
  if (osReg && method === 'PATCH') {
    const poId = parseInt(osReg[1]);
    const body = await readJSON(req);
    const osNumber = body.os_number || '';
    const itemOS = body.item_os || []; // [{item_id, os_number}]

    if (itemOS.length > 0) {
      // 제품별 OS번호 등록
      const stmt = db.prepare('UPDATE po_items SET os_number=? WHERE item_id=? AND po_id=?');
      const tx = db.transaction(() => {
        for (const io of itemOS) {
          if (io.os_number) stmt.run(io.os_number, io.item_id, poId);
        }
      });
      tx();
      // PO 헤더에도 첫 번째 OS번호 기록 (대표값)
      const firstOS = itemOS.find(i => i.os_number)?.os_number || osNumber;
      if (firstOS) {
        db.prepare(`UPDATE po_header SET os_number = ?, status = 'os_registered', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(firstOS, poId);
      }
      logPOActivity(poId, 'os_registered', { actor_type: 'admin', to_status: 'os_registered', details: `제품별 OS번호 등록 (${itemOS.filter(i=>i.os_number).length}건)` });
      ok(res, { ok: true, po_id: poId, item_count: itemOS.filter(i=>i.os_number).length, status: 'os_registered' });
    } else if (osNumber) {
      // PO 전체 OS번호 등록
      const curPO = db.prepare('SELECT status FROM po_header WHERE po_id=?').get(poId);
      const shouldChangeStatus = curPO && curPO.status === 'os_pending';
      if (shouldChangeStatus) {
        db.prepare(`UPDATE po_header SET os_number = ?, status = 'os_registered', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(osNumber, poId);
      } else {
        db.prepare(`UPDATE po_header SET os_number = ?, updated_at = datetime('now','localtime') WHERE po_id = ?`).run(osNumber, poId);
      }
      // 모든 아이템에도 동일 OS번호 적용
      db.prepare('UPDATE po_items SET os_number=? WHERE po_id=?').run(osNumber, poId);
      const newStatus = shouldChangeStatus ? 'os_registered' : (curPO?.status || 'unknown');
      logPOActivity(poId, 'os_saved', { actor_type: 'admin', details: `OS번호: ${osNumber}` });
      ok(res, { ok: true, po_id: poId, os_number: osNumber, status: newStatus });
    } else {
      fail(res, 400, 'os_number 또는 item_os 필수');
    }
    return;
  }

  // GET /api/po/:id/activity — PO 활동 로그 조회
  const activityMatch = pathname.match(/^\/api\/po\/(\d+)\/activity$/);
  if (activityMatch && method === 'GET') {
    const poId = parseInt(activityMatch[1]);
    const logs = db.prepare('SELECT * FROM po_activity_log WHERE po_id=? ORDER BY created_at DESC').all(poId);
    ok(res, logs);
    return;
  }

  // GET /api/activity-log — 전체 활동 로그 (최근 100건)
  if (pathname === '/api/activity-log' && method === 'GET') {
    const limit = parseInt(parsed.searchParams.get('limit') || '100');
    const logs = db.prepare('SELECT * FROM po_activity_log ORDER BY created_at DESC LIMIT ?').all(limit);
    ok(res, logs);
    return;
  }

  // GET /api/po/os-pending — OS등록 대기 PO 목록 (os_pending + os_registered)
  if (pathname === '/api/po/os-pending' && method === 'GET') {
    const enToKo = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'cancelled':'취소', 'os_pending':'OS등록대기', 'os_registered':'OS검증대기' };
    const rows = db.prepare(`SELECT * FROM po_header WHERE status IN ('os_pending','os_registered') ORDER BY po_date DESC`).all();
    for (const r of rows) {
      r.status = enToKo[r.status] || r.status;
      r.items = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(r.po_id);
    }
    ok(res, rows);
    return;
  }

  // GET /api/po/os-match — XERP OS번호 자동 매칭
  if (pathname === '/api/po/os-match' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }
    try {
      // 1. 모든 PO 가져오기
      const allPOs = db.prepare(`SELECT * FROM po_header ORDER BY po_date DESC`).all();
      const itemStmt = db.prepare('SELECT * FROM po_items WHERE po_id = ?');
      for (const po of allPOs) po.items = itemStmt.all(po.po_id);

      // 2. 분류: 진행중 / 완료 / 취소
      const pending = allPOs.filter(p => p.status !== 'received' && p.status !== 'cancelled' && !p.os_number);
      const completed = allPOs.filter(p => p.status === 'received' || p.os_number).slice(0, 50);
      const cancelled = allPOs.filter(p => p.status === 'cancelled').slice(0, 30);
      const productCodes = [...new Set(pending.flatMap(po => po.items.map(i => (i.product_code || '').trim())).filter(Boolean))];

      // 3. XERP에서 제품코드로 OS 매칭 (최근 2개월)
      let xerpMatches = {};
      if (productCodes.length && xerpPool) {
        const start2m = new Date(); start2m.setMonth(start2m.getMonth() - 2);
        const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');

        // 500개씩 분할
        for (let i = 0; i < productCodes.length; i += 200) {
          const batch = productCodes.slice(i, i + 200);
          const placeholders = batch.map((_, j) => `@p${i+j}`).join(',');
          const req = xerpPool.request();
          req.input('startDate', sql.NChar(16), fmt(start2m));
          batch.forEach((c, j) => req.input(`p${i+j}`, sql.NChar(40), c));

          const result = await req.query(`
            SELECT RTRIM(i.ItemCode) AS item_code, RTRIM(h.OrderNo) AS os_number,
                   h.OrderDate AS order_date, i.OrderQty AS qty, RTRIM(h.CsCode) AS vendor_code
            FROM poOrderHeader h WITH (NOLOCK)
            JOIN poOrderItem i WITH (NOLOCK) ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
            WHERE h.SiteCode = 'BK10' AND h.OrderDate >= @startDate
              AND RTRIM(i.ItemCode) IN (${placeholders})
            ORDER BY h.OrderDate DESC
          `);

          for (const row of result.recordset) {
            const code = (row.item_code || '').trim();
            if (!xerpMatches[code]) xerpMatches[code] = [];
            xerpMatches[code].push({
              os_number: (row.os_number || '').trim(),
              order_date: (row.order_date || '').trim(),
              qty: row.qty || 0,
              vendor_code: (row.vendor_code || '').trim()
            });
          }
        }
      }

      // 4. 매칭 결과 분류
      const matched = [];
      const unmatched = [];
      const enToKo = { 'os_pending':'OS등록대기', 'os_registered':'OS검증대기', 'received':'완료' };

      for (const po of pending) {
        po.status = enToKo[po.status] || po.status;
        let poMatched = false;
        const matchedItems = [];

        for (const item of po.items) {
          const code = (item.product_code || '').trim();
          const matches = xerpMatches[code];
          if (matches && matches.length) {
            matchedItems.push({ ...item, xerp_os: matches[0] });
            poMatched = true;
          } else {
            matchedItems.push(item);
          }
        }

        po.items = matchedItems;
        if (poMatched) {
          po._matched_os = matchedItems.find(i => i.xerp_os)?.xerp_os?.os_number || '';
          matched.push(po);
        } else {
          unmatched.push(po);
        }
      }

      // 5. os_registered PO 검증 (OS번호 입력됨 → XERP 확인 후 received 또는 os_pending으로)
      const registeredPOs = db.prepare(
        "SELECT h.*, GROUP_CONCAT(i.product_code) as product_codes FROM po_header h LEFT JOIN po_items i ON h.po_id=i.po_id WHERE h.status='os_registered' GROUP BY h.po_id"
      ).all();

      const verified = [];
      const mismatched = [];

      if (registeredPOs.length) {
        // xerpByOrderNo: OrderNo → { items: [{ItemCode}] } 구조 구축 (XERP 쿼리)
        const xerpByOrderNo = {};
        const osNumbers = registeredPOs.filter(r => r.os_number).map(r => r.os_number.trim());
        if (osNumbers.length && xerpPool) {
          const start2m = new Date(); start2m.setMonth(start2m.getMonth() - 2);
          const fmt2 = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
          for (let i = 0; i < osNumbers.length; i += 100) {
            const batch = osNumbers.slice(i, i + 100);
            const placeholders = batch.map((_, j) => `@o${i+j}`).join(',');
            const xreq = xerpPool.request();
            xreq.input('startDate', sql.NChar(16), fmt2(start2m));
            batch.forEach((o, j) => xreq.input(`o${i+j}`, sql.NChar(40), o));
            try {
              const xresult = await xreq.query(`
                SELECT RTRIM(h.OrderNo) AS OrderNo, RTRIM(i.ItemCode) AS ItemCode
                FROM poOrderHeader h WITH (NOLOCK)
                JOIN poOrderItem i WITH (NOLOCK) ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
                WHERE h.SiteCode = 'BK10' AND h.OrderDate >= @startDate
                  AND RTRIM(h.OrderNo) IN (${placeholders})
              `);
              for (const row of xresult.recordset) {
                const on = (row.OrderNo || '').trim();
                if (!xerpByOrderNo[on]) xerpByOrderNo[on] = { items: [] };
                xerpByOrderNo[on].items.push({ ItemCode: (row.ItemCode || '').trim() });
              }
            } catch (xe) { console.error('os_registered XERP 조회 오류:', xe.message); }
          }
        }

        const productInfoMap = getProductInfo();

        for (const rpo of registeredPOs) {
          if (!rpo.os_number) {
            // OS번호 없음 → 다시 os_pending으로
            db.prepare("UPDATE po_header SET status='os_pending', updated_at=datetime('now','localtime') WHERE po_id=?").run(rpo.po_id);
            mismatched.push({ ...rpo, error: 'OS번호가 누락되었습니다' });
            continue;
          }

          const xerpMatch = xerpByOrderNo[rpo.os_number.trim()];
          if (!xerpMatch) {
            // XERP에서 아직 미확인 → os_registered 유지 (대기중)
            verified.push({ ...rpo, status: 'OS검증대기', xerp_status: 'XERP 미확인 (대기중)' });
            continue;
          }

          // 제품코드 → 원자재코드 변환 후 XERP ItemCode와 비교
          const poProductCodes = (rpo.product_codes || '').split(',').map(c => c.trim()).filter(Boolean);
          const xerpItemCodes = xerpMatch.items.map(i => i.ItemCode);

          const materialCodes = poProductCodes.map(pc => {
            const pInfo = productInfoMap[pc];
            return pInfo ? (pInfo.material_code || pInfo['원자재코드'] || pc) : pc;
          }).filter(Boolean);

          const hasMatch = materialCodes.some(mc => xerpItemCodes.includes(mc));

          if (hasMatch) {
            // 검증 완료 → received로 자동 완료
            db.prepare("UPDATE po_header SET status='received', updated_at=datetime('now','localtime') WHERE po_id=?").run(rpo.po_id);
            verified.push({ ...rpo, status: 'OS검증대기', xerp_status: '검증완료', auto_completed: true });
          } else {
            // 불일치 → os_pending으로 되돌리고 os_number 초기화
            db.prepare("UPDATE po_header SET status='os_pending', os_number='', updated_at=datetime('now','localtime') WHERE po_id=?").run(rpo.po_id);
            mismatched.push({
              ...rpo,
              error: `OS번호와 제품코드가 다릅니다 (OS: ${rpo.os_number}, XERP품목: ${xerpItemCodes.join(',')}, PO원자재: ${materialCodes.join(',')})`
            });
          }
        }
      }

      // 완료 PO도 한글 변환
      for (const po of completed) po.status = enToKo[po.status] || po.status;

      // 취소 PO도 한글 변환
      for (const po of cancelled) po.status = enToKo[po.status] || po.status;

      ok(res, { matched, unmatched, completed, cancelled, verified, mismatched, xerp_match_count: Object.keys(xerpMatches).length });
    } catch (e) {
      console.error('OS 매칭 오류:', e.message);
      fail(res, 500, 'OS 매칭 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: PURCHASE ORDERS
  // ════════════════════════════════════════════════════════════════════

  // GET /api/stats/vendor-summary — 거래처별 발주 통계
  if (pathname === '/api/stats/vendor-summary' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const from = qs.get('from') || new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
    const to = qs.get('to') || new Date().toISOString().slice(0, 10);
    const rows = db.prepare(`
      SELECT vendor_name,
        COUNT(*) as order_count,
        COALESCE(SUM(total_qty), 0) as total_qty,
        COALESCE(SUM(CASE WHEN status IN ('received','os_pending') THEN 1 ELSE 0 END), 0) as completed_count
      FROM po_header
      WHERE po_date >= ? AND po_date <= ? AND status != 'cancelled'
      GROUP BY vendor_name
      ORDER BY order_count DESC
    `).all(from, to);

    // 납기 준수율 및 평균 리드타임 계산
    const result = rows.map(r => {
      const ltRows = db.prepare(`
        SELECT po_date, updated_at, expected_date FROM po_header
        WHERE vendor_name = ? AND po_date >= ? AND po_date <= ?
          AND status IN ('received','os_pending') AND po_date IS NOT NULL AND updated_at IS NOT NULL
      `).all(r.vendor_name, from, to);
      let totalLT = 0, ltCount = 0, onTimeCount = 0;
      ltRows.forEach(p => {
        const d1 = new Date(p.po_date), d2 = new Date(p.updated_at);
        if (d1 && d2 && d2 > d1) {
          const days = Math.round((d2 - d1) / 86400000);
          if (days > 0 && days < 90) { totalLT += days; ltCount++; }
        }
        if (p.expected_date && p.updated_at <= p.expected_date + ' 23:59:59') onTimeCount++;
      });
      return {
        vendor_name: r.vendor_name,
        order_count: r.order_count,
        total_qty: r.total_qty,
        avg_lead_time: ltCount > 0 ? Math.round(totalLT / ltCount * 10) / 10 : 0,
        on_time_rate: ltRows.length > 0 ? Math.round(onTimeCount / ltRows.length * 100) : 0
      };
    });
    ok(res, result);
    return;
  }

  // GET /api/po/stats — 대시보드 전용 통계
  if (pathname === '/api/po/stats' && method === 'GET') {
    const enToKo = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'cancelled':'취소', 'os_pending':'OS등록대기', 'os_registered':'OS검증대기' };
    const allPO = db.prepare('SELECT * FROM po_header ORDER BY po_date DESC, po_id DESC').all();
    // 상태 정규화
    for (const r of allPO) r.status = enToKo[r.status] || r.status;

    // 파이프라인
    const pipeline = {}, pipelineQty = {};
    for (const s of ['대기','발송','확인','수령중','완료','취소']) { pipeline[s] = 0; pipelineQty[s] = 0; }
    for (const r of allPO) { pipeline[r.status] = (pipeline[r.status]||0) + 1; pipelineQty[r.status] = (pipelineQty[r.status]||0) + (r.total_qty||0); }

    // 입고율
    const itemStats = db.prepare('SELECT COALESCE(SUM(ordered_qty),0) as ordered, COALESCE(SUM(received_qty),0) as received FROM po_items').get();
    const totalOrdered = itemStats.ordered;
    const totalReceived = itemStats.received;
    const receiveRate = totalOrdered > 0 ? Math.round(totalReceived / totalOrdered * 1000) / 10 : 0;

    // 취소율
    const totalCount = allPO.length;
    const cancelCount = pipeline['취소'] || 0;
    const cancelRate = totalCount > 0 ? Math.round(cancelCount / totalCount * 1000) / 10 : 0;

    // 거래처별 TOP5
    const vendorMap = {};
    for (const r of allPO) {
      const v = r.vendor_name || '(미지정)';
      if (!vendorMap[v]) vendorMap[v] = { vendor: v, count: 0, qty: 0 };
      vendorMap[v].count++;
      vendorMap[v].qty += (r.total_qty || 0);
    }
    const vendorTop = Object.values(vendorMap).sort((a, b) => b.count - a.count).slice(0, 5);

    // 최근 5건
    const recentPOs = allPO.slice(0, 5).map(r => ({
      po_number: r.po_number, vendor_name: r.vendor_name, status: r.status,
      total_qty: r.total_qty, po_date: r.po_date, po_type: r.po_type
    }));

    // KPI: 이번달 발주, 미수령
    const thisMonth = new Date().toISOString().slice(0, 7);
    const monthPOCount = allPO.filter(r => (r.po_date || '').startsWith(thisMonth)).length;
    const pendingCount = allPO.filter(r => r.status !== '완료' && r.status !== '취소').length;

    ok(res, { pipeline, pipelineQty, totalOrdered, totalReceived, receiveRate,
      cancelRate, cancelCount, totalCount, vendorTop, recentPOs, monthPOCount, pendingCount });
    return;
  }

  if (pathname === '/api/po' && method === 'GET') {
    let sql = 'SELECT * FROM po_header WHERE 1=1';
    const params = [];
    const status = parsed.searchParams.get('status');
    const vendor = parsed.searchParams.get('vendor');
    const from = parsed.searchParams.get('from');
    const to = parsed.searchParams.get('to');
    const origin = parsed.searchParams.get('origin');
    if (status) { sql += ' AND status = ?'; params.push(status); }
    if (vendor) { sql += ' AND vendor_name LIKE ?'; params.push('%' + vendor + '%'); }
    if (from) { sql += ' AND po_date >= ?'; params.push(from); }
    if (to) { sql += ' AND po_date <= ?'; params.push(to); }
    if (origin) { sql += ' AND origin = ?'; params.push(origin); }
    sql += ' ORDER BY po_date DESC, po_id DESC';
    const rows = db.prepare(sql).all(...params);
    // 상태 영→한 정규화
    const enToKo = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'cancelled':'취소', 'os_pending':'OS등록대기', 'os_registered':'OS검증대기' };
    for (const row of rows) {
      row.status = enToKo[row.status] || row.status;
    }
    // include=items 시 품목 정보 포함
    if (parsed.searchParams.get('include') === 'items') {
      const itemStmt = db.prepare('SELECT * FROM po_items WHERE po_id = ?');
      for (const row of rows) {
        row.items = itemStmt.all(row.po_id);
      }
    }
    ok(res, rows);
    return;
  }

  // GET /api/po/:id
  const poGet = pathname.match(/^\/api\/po\/(\d+)$/);
  if (poGet && method === 'GET') {
    const id = parseInt(poGet[1]);
    const po = db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    po.items = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(id);
    ok(res, po);
    return;
  }

  // POST /api/po/bulk-import — 엑셀 일괄 발주
  if (pathname === '/api/po/bulk-import' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    if (!items.length) { fail(res, 400, '항목이 없습니다'); return; }

    // vendor_name별 그룹핑
    const vendorGroups = {};
    for (const it of items) {
      const vName = it.vendor_name || '';
      if (!vName) continue;
      if (!vendorGroups[vName]) vendorGroups[vName] = [];
      vendorGroups[vName].push(it);
    }

    const created = [];
    const errors = [];
    const today = new Date().toISOString().slice(0, 10);

    for (const [vendorName, vendorItems] of Object.entries(vendorGroups)) {
      try {
        const poNumber = generatePoNumber();
        const totalQty = vendorItems.reduce((s, it) => s + (parseInt(it.qty) || 0), 0);
        // origin: 첫 번째 품목의 products.origin 사용
        const _bulkFirstProd = db.prepare('SELECT origin FROM products WHERE product_code=?').get(vendorItems[0].product_code || '');
        const _bulkOrigin = (_bulkFirstProd && _bulkFirstProd.origin) || '';
        const tx = db.transaction(() => {
          const hdr = db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, material_status, process_status, origin, po_date)
            VALUES (?,?,?,?,?,?,?,?,?,?)`).run(
            poNumber, '원재료', vendorName, 'draft', totalQty, '엑셀 일괄 발주', 'sent', 'waiting', _bulkOrigin, today
          );
          for (const it of vendorItems) {
            db.prepare('INSERT INTO po_items (po_id, product_code, ordered_qty, notes) VALUES (?,?,?,?)').run(
              hdr.lastInsertRowid, it.product_code || '', parseInt(it.qty) || 0, '엑셀 일괄'
            );
          }
          return { po_id: Number(hdr.lastInsertRowid), po_number: poNumber };
        });
        const result = tx();
        created.push({ po_number: result.po_number, vendor: vendorName, items_count: vendorItems.length });
      } catch (e) {
        errors.push({ vendor: vendorName, error: e.message });
      }
    }

    // vendor_name이 없는 항목 에러 처리
    const noVendor = items.filter(it => !it.vendor_name);
    if (noVendor.length) {
      errors.push({ vendor: '(미지정)', error: `거래처 미지정 ${noVendor.length}건`, items: noVendor.map(it => it.product_code) });
    }

    ok(res, { created, errors });
    return;
  }

  if (pathname === '/api/po' && method === 'POST') {
    const body = await readJSON(req);
    const poNumber = generatePoNumber();
    const items = body.items || [];
    const totalQty = items.reduce((s, it) => s + (it.ordered_qty || 0), 0);

    // vendor_id로 vendor_name 자동 조회
    let vendorName = body.vendor_name || '';
    if (!vendorName && body.vendor_id) {
      const v = db.prepare('SELECT name FROM vendors WHERE vendor_id = ?').get(body.vendor_id);
      if (v) vendorName = v.name;
    }

    // origin 결정: body에서 직접 지정 또는 첫 번째 품목의 products.origin 사용
    let poOrigin = body.origin || '';
    if (!poOrigin && items.length) {
      const firstProd = db.prepare('SELECT origin FROM products WHERE product_code=?').get(items[0].product_code || '');
      if (firstProd && firstProd.origin) poOrigin = firstProd.origin;
    }

    const tx = db.transaction(() => {
      const info = db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, expected_date, total_qty, notes, process_step, parent_po_id, process_chain, origin, po_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now','localtime'))`).run(
        poNumber,
        body.po_type || 'material',
        vendorName,
        body.status || '대기',
        body.expected_date || '',
        totalQty,
        body.notes || '',
        body.process_step || 0,
        body.parent_po_id || null,
        body.process_chain || '',
        poOrigin
      );
      const poId = info.lastInsertRowid;
      const itemStmt = db.prepare(`INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?, ?, ?, ?, ?, ?, ?)`);
      for (const it of items) {
        itemStmt.run(poId, it.product_code || '', it.brand || '', it.process_type || '', it.ordered_qty || 0, it.spec || '', it.notes || '');
      }
      return poId;
    });
    const poId = tx();

    // 목형비 자동 처리: 신제품 첫 발주 시 notes에 '목형비 포함' 마킹
    for (const item of items) {
      const prod = db.prepare('SELECT is_new_product, first_order_done, die_cost FROM products WHERE product_code=?').get(item.product_code);
      if (prod && prod.is_new_product === 1 && prod.first_order_done === 0) {
        db.prepare("UPDATE po_items SET notes = CASE WHEN notes='' THEN '목형비 포함' ELSE notes || ' | 목형비 포함' END WHERE po_id=? AND product_code=?")
          .run(poId, item.product_code);
        db.prepare("UPDATE products SET first_order_done=1 WHERE product_code=?").run(item.product_code);
        logPOActivity(poId, 'die_cost_included', {
          actor_type: 'system',
          details: `신제품 최초 발주 → 목형비 포함 마킹: ${item.product_code}`
        });
        console.log(`[목형비] ${item.product_code} 첫 발주 → 목형비 포함 마킹`);
      }
    }

    ok(res, { po_id: poId, po_number: poNumber });
    return;
  }

  // PUT /api/po/:id/status
  const poStatus = pathname.match(/^\/api\/po\/(\d+)\/status$/);
  if (poStatus && method === 'PUT') {
    const id = parseInt(poStatus[1]);
    const body = await readJSON(req);
    const validStatuses = ['draft', 'sent', 'confirmed', 'partial', 'received', 'cancelled', 'os_pending', 'os_registered'];
    if (!validStatuses.includes(body.status)) { fail(res, 400, 'Invalid status. Allowed: ' + validStatuses.join(', ')); return; }
    db.prepare(`UPDATE po_header SET status = ?, updated_at = datetime('now','localtime') WHERE po_id = ?`).run(body.status, id);
    ok(res, { po_id: id, status: body.status });
    return;
  }

  // PATCH /api/po/:id — 상태 변경 (프론트엔드 호출용)
  const poPatch = pathname.match(/^\/api\/po\/(\d+)$/);
  if (poPatch && method === 'PATCH') {
    const id = parseInt(poPatch[1]);
    const body = await readJSON(req);
    const newStatus = body.status;
    const koToEn = { '대기': 'draft', '발송': 'sent', '확인': 'confirmed', '수령중': 'partial', '완료': 'received', '취소': 'cancelled', 'OS등록대기': 'os_pending', 'OS검증대기': 'os_registered' };
    const dbStatus = koToEn[newStatus] || newStatus;
    const poBeforePatch = db.prepare('SELECT status, material_status, process_status FROM po_header WHERE po_id=?').get(id);
    db.prepare(`UPDATE po_header SET status = ?, updated_at = datetime('now','localtime') WHERE po_id = ?`).run(dbStatus, id);

    // 발송 시 파이프라인 서브상태 초기화
    if (newStatus === '발송' || dbStatus === 'sent') {
      db.prepare("UPDATE po_header SET material_status='sent', process_status='waiting' WHERE po_id=?").run(id);
    }

    logPOActivity(id, 'status_change', {
      actor_type: 'admin',
      from_status: poBeforePatch ? poBeforePatch.status : '',
      to_status: dbStatus,
      details: `상태 변경: ${poBeforePatch ? poBeforePatch.status : ''} → ${dbStatus}`
    });

    // 발송(발주확인) 시 Google Sheet + 업체 이메일 발송
    let sheetResult = null;
    let emailResult = null;
    console.log(`[PATCH] po_id=${id}, newStatus='${newStatus}', dbStatus='${dbStatus}'`);
    if (newStatus === '발송' || dbStatus === 'sent') {
      const po = db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(id);
      const items = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(id);

      // Google Sheet 동기화
      if (items.length) {
        sheetResult = await appendToGoogleSheet(items.map(it => ({
          order_date: po ? po.po_date : '',
          product_code: it.product_code || '',
          product_name: it.brand || '',
          material_name: it.spec || '',
          paper_maker: po ? po.vendor_name : '',
          order_qty: it.ordered_qty || 0,
          product_spec: it.spec || ''
        })));
        console.log('Google Sheet 발송 동기화:', sheetResult);
      }

      // 업체 이메일 발송 (후공정은 수동 발송 전까지 이메일 안 보냄 — 원재료 출고 시 자동 발송)
      if (po) {
        const vendor = db.prepare('SELECT * FROM vendors WHERE name = ?').get(po.vendor_name);
        const isPost = vendor ? vendor.type === '후공정' : (po.po_type === '후공정');
        const forceEmail = body.force_email === true; // 수동 메일보내기
        if (vendor && vendor.email && (!isPost || forceEmail)) {
          emailResult = await sendPOEmail(po, items, vendor.email, vendor.name, isPost, vendor.email_cc);
          console.log(`발주확인 이메일 발송: ${po.po_number} → ${vendor.name} (${vendor.email})`, emailResult);
        } else if (isPost && !forceEmail) {
          console.log(`후공정 PO ${po.po_number}: 이메일 보류 (원재료 출고 시 자동 발송)`);
          emailResult = { ok: true, skipped: true, reason: '후공정 — 원재료 출고 시 자동 발송' };
        } else {
          console.warn(`발주확인: 업체 이메일 없음 (${po.vendor_name})`);
        }
      }

      // 발송 시 거래명세서 자동 생성
      if (po) {
        const poForDoc = db.prepare('SELECT * FROM po_header WHERE po_id=?').get(id);
        const itemsForDoc = db.prepare('SELECT * FROM po_items WHERE po_id=?').all(id);
        const piMap = getProductInfo();
        const docItems = itemsForDoc.map(item => {
          const pi = piMap[item.product_code] || {};
          const lastPrice = getLastVendorPrice(poForDoc.vendor_name, item.product_code);
          return {
            product_code: item.product_code,
            product_name: item.brand || '',
            qty: item.ordered_qty,
            unit_price: lastPrice,
            amount: lastPrice * (item.ordered_qty || 0),
            spec: item.spec || '',
            cut: pi['절'] || '',
            imposition: pi['조판'] || '',
            material_name: pi['원재료용지명'] || '',
            process_name: item.spec || '',
            last_price: lastPrice
          };
        });
        const vendorRow = db.prepare('SELECT type FROM vendors WHERE name=?').get(poForDoc.vendor_name);
        const vendorType = vendorRow ? vendorRow.type : 'material';
        db.prepare(`INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')`)
          .run(id, poForDoc.po_number, poForDoc.vendor_name, vendorType === '후공정' ? 'process' : 'material', JSON.stringify(docItems));
        logPOActivity(id, 'trade_doc_created', { actor_type: 'system', details: '거래명세서 자동 생성' });
      }
    }

    // 취소 시 Google Sheet에 취소선 + 빨간글씨 적용
    if (newStatus === '취소' || dbStatus === 'cancelled') {
      const items = db.prepare('SELECT product_code FROM po_items WHERE po_id = ?').all(id);
      const po = db.prepare('SELECT po_date FROM po_header WHERE po_id = ?').get(id);
      const codes = items.map(i => i.product_code).filter(Boolean);
      if (codes.length) {
        sheetResult = await cancelInGoogleSheet(codes, po ? po.po_date : '');
        console.log('Google Sheet 취소 포맷:', sheetResult);
      }
    }

    const emailFailed = emailResult && !emailResult.ok;
    ok(res, { updated: true, po_id: id, status: dbStatus, google_sheet: sheetResult, email: emailResult, email_failed: emailFailed });
    return;
  }

  // POST /api/po/:id/resend — 이메일 재발송
  const poResend = pathname.match(/^\/api\/po\/(\d+)\/resend$/);
  if (poResend && method === 'POST') {
    const id = parseInt(poResend[1]);
    const po = db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    const items = db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(id);
    const vendor = db.prepare('SELECT * FROM vendors WHERE name = ?').get(po.vendor_name);
    if (!vendor || !vendor.email) { fail(res, 400, '업체 이메일 미등록'); return; }
    try {
      const isPost = po.po_type === '후공정';
      const emailResult = await sendPOEmail(po, items, vendor.email, vendor.name, isPost, vendor.email_cc || '');
      // 활동 로그
      try { db.prepare('INSERT INTO po_activity_log (po_id, action, details) VALUES (?, ?, ?)').run(id, '이메일 재발송', emailResult.ok ? '성공: ' + vendor.email : '실패: ' + (emailResult.error||'')); } catch(e){}
      ok(res, { email: emailResult });
    } catch(e) {
      ok(res, { email: { ok: false, error: e.message } });
    }
    return;
  }

  // DELETE /api/po/:id
  const poDel = pathname.match(/^\/api\/po\/(\d+)$/);
  if (poDel && method === 'DELETE') {
    const id = parseInt(poDel[1]);
    const po = db.prepare('SELECT po_id, po_number, status FROM po_header WHERE po_id = ?').get(id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    try { db.prepare('DELETE FROM receipt_items WHERE receipt_id IN (SELECT receipt_id FROM receipts WHERE po_id = ?)').run(id); } catch(_){}
    try { db.prepare('DELETE FROM receipts WHERE po_id = ?').run(id); } catch(_){}
    db.prepare('DELETE FROM po_items WHERE po_id = ?').run(id);
    try { db.prepare('DELETE FROM activity_log WHERE po_id = ?').run(id); } catch(_){}
    db.prepare('DELETE FROM po_header WHERE po_id = ?').run(id);
    console.log(`PO 삭제: ${po.po_number} (ID: ${id}, 상태: ${po.status})`);
    ok(res, { deleted: id, po_number: po.po_number });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: RECEIPTS
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/receipts' && method === 'GET') {
    const rows = db.prepare(`
      SELECT r.receipt_id, r.po_id, r.receipt_date, r.received_by, r.notes, r.created_at,
             h.po_number
      FROM receipts r
      LEFT JOIN po_header h ON r.po_id = h.po_id
      ORDER BY r.created_at DESC
    `).all();
    const itemStmt = db.prepare('SELECT * FROM receipt_items WHERE receipt_id = ?');
    for (const r of rows) {
      r.items = itemStmt.all(r.receipt_id);
    }
    ok(res, rows);
    return;
  }

  if (pathname === '/api/receipts' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id) { fail(res, 400, 'po_id required'); return; }
    const items = body.items || [];

    const tx = db.transaction(() => {
      const rInfo = db.prepare(`INSERT INTO receipts (po_id, received_by, notes) VALUES (?, ?, ?)`).run(
        body.po_id, body.received_by || '', body.notes || ''
      );
      const receiptId = rInfo.lastInsertRowid;

      const riStmt = db.prepare(`INSERT INTO receipt_items (receipt_id, po_item_id, product_code, received_qty, defect_qty, notes) VALUES (?, ?, ?, ?, ?, ?)`);
      const updatePoItem = db.prepare(`UPDATE po_items SET received_qty = received_qty + ? WHERE item_id = ?`);

      for (const it of items) {
        riStmt.run(receiptId, it.po_item_id || null, it.product_code || '', it.received_qty || 0, it.defect_qty || 0, it.notes || '');
        if (it.po_item_id && it.received_qty) {
          updatePoItem.run(it.received_qty, it.po_item_id);
        }
      }

      // Check if all items fully received → update PO status
      const poItems = db.prepare('SELECT ordered_qty, received_qty FROM po_items WHERE po_id = ?').all(body.po_id);
      const allReceived = poItems.length > 0 && poItems.every(pi => pi.received_qty >= pi.ordered_qty);
      const anyReceived = poItems.some(pi => pi.received_qty > 0);

      if (allReceived) {
        db.prepare(`UPDATE po_header SET status = 'received', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(body.po_id);
      } else if (anyReceived) {
        db.prepare(`UPDATE po_header SET status = 'partial', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(body.po_id);
      }

      return receiptId;
    });
    const receiptId = tx();

    // XERP 캐시 무효화 (다음 조회 시 최신 데이터 로드)
    if (typeof xerpInventoryCacheTime !== 'undefined') xerpInventoryCacheTime = 0;

    ok(res, { receipt_id: receiptId });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: INVOICES
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/invoices' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM invoices ORDER BY created_at DESC').all();
    for (const inv of rows) {
      inv.items = db.prepare('SELECT * FROM invoice_items WHERE invoice_id = ?').all(inv.invoice_id);
    }
    ok(res, rows);
    return;
  }

  if (pathname === '/api/invoices' && method === 'POST') {
    const ct = req.headers['content-type'] || '';
    const boundaryMatch = ct.match(/boundary=(.+)/);
    if (!boundaryMatch) { fail(res, 400, 'Multipart boundary required'); return; }

    const buf = await readBody(req);
    const parts = parseMultipart(buf, boundaryMatch[1]);

    let filePath = '';
    let fileName = '';

    // Save uploaded file
    if (parts.file && parts.file.data) {
      const now = new Date();
      const ym = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
      const dir = path.join(UPLOAD_ROOT, ym);
      fs.mkdirSync(dir, { recursive: true });

      const ts = Date.now();
      const ext = path.extname(parts.file.filename) || '';
      const safeName = ts + ext;
      const fullPath = path.join(dir, safeName);
      fs.writeFileSync(fullPath, parts.file.data);
      filePath = path.relative(UPLOAD_DIR, fullPath).replace(/\\/g, '/');
      fileName = parts.file.filename;
    }

    // Parse items JSON from multipart field
    let items = [];
    try { if (parts.items) items = JSON.parse(parts.items); } catch(_) {}

    const tx = db.transaction(() => {
      const info = db.prepare(`INSERT INTO invoices (po_id, vendor_name, invoice_no, invoice_date, amount, file_path, file_name, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
        parts.po_id ? parseInt(parts.po_id) : null,
        parts.vendor_name || '',
        parts.invoice_no || '',
        parts.invoice_date || '',
        parts.amount ? parseFloat(parts.amount) : 0,
        filePath,
        fileName,
        parts.notes || ''
      );
      const invId = info.lastInsertRowid;
      if (items.length) {
        const stmt = db.prepare(`INSERT INTO invoice_items (invoice_id, product_code, product_name, qty, unit_price, amount, notes) VALUES (?, ?, ?, ?, ?, ?, ?)`);
        for (const it of items) {
          stmt.run(invId, it.product_code || '', it.product_name || '', it.qty || 0, it.unit_price || 0, it.amount || 0, it.notes || '');
        }
      }
      return invId;
    });
    const invId = tx();
    ok(res, { invoice_id: invId, file_path: filePath });
    return;
  }

  // GET /api/invoices/:id/file
  const invFile = pathname.match(/^\/api\/invoices\/(\d+)\/file$/);
  if (invFile && method === 'GET') {
    const id = parseInt(invFile[1]);
    const inv = db.prepare('SELECT file_path, file_name FROM invoices WHERE invoice_id = ?').get(id);
    if (!inv || !inv.file_path) { fail(res, 404, 'File not found'); return; }
    const fullPath = path.join(UPLOAD_DIR, inv.file_path);
    if (!fs.existsSync(fullPath)) { fail(res, 404, 'File missing from disk'); return; }
    const ext = path.extname(inv.file_name || inv.file_path);
    const ct2 = MIME[ext.toLowerCase()] || 'application/octet-stream';
    res.writeHead(200, {
      'Content-Type': ct2,
      'Content-Disposition': `inline; filename="${encodeURIComponent(inv.file_name || 'file')}"`,
      ...CORS,
    });
    fs.createReadStream(fullPath).pipe(res);
    return;
  }

  // DELETE /api/invoices/:id
  const invDel = pathname.match(/^\/api\/invoices\/(\d+)$/);
  if (invDel && method === 'DELETE') {
    const id = parseInt(invDel[1]);
    const inv = db.prepare('SELECT file_path FROM invoices WHERE invoice_id = ?').get(id);
    if (!inv) { fail(res, 404, 'Invoice not found'); return; }
    // delete file from disk
    if (inv.file_path) {
      const fullPath = path.join(UPLOAD_DIR, inv.file_path);
      try { fs.unlinkSync(fullPath); } catch (_) { /* ignore if already deleted */ }
    }
    db.prepare('DELETE FROM invoice_items WHERE invoice_id = ?').run(id);
    db.prepare('DELETE FROM invoices WHERE invoice_id = ?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: STATS
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/stats' && method === 'GET') {
    const now = new Date();
    const ym = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');

    const totalPOs = db.prepare('SELECT COUNT(*) as cnt FROM po_header').get().cnt;
    const draftPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'draft'`).get().cnt;
    const sentPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'sent'`).get().cnt;
    const confirmedPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'confirmed'`).get().cnt;
    const partialPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'partial'`).get().cnt;
    const receivedPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'received'`).get().cnt;
    const cancelledPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'cancelled'`).get().cnt;
    const pendingPOs = draftPOs + sentPOs + confirmedPOs + partialPOs;

    const thisMonthPOs = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE po_date LIKE ?`).get(ym + '%').cnt;
    const thisMonthItems = db.prepare(`SELECT COALESCE(SUM(pi.ordered_qty),0) as qty FROM po_items pi JOIN po_header ph ON ph.po_id=pi.po_id WHERE ph.po_date LIKE ?`).get(ym + '%').qty;
    const totalVendors = db.prepare('SELECT COUNT(*) as cnt FROM vendors').get().cnt;
    const totalInvoices = db.prepare('SELECT COUNT(*) as cnt FROM invoices').get().cnt;
    const thisMonthInvoiceAmt = db.prepare(`SELECT COALESCE(SUM(amount),0) as amt FROM invoices WHERE invoice_date LIKE ?`).get(ym + '%').amt;

    ok(res, {
      totalPOs, pendingPOs, draftPOs, sentPOs, confirmedPOs, partialPOs, receivedPOs, cancelledPOs,
      thisMonthPOs, thisMonthItems, totalVendors, totalInvoices, thisMonthInvoiceAmt,
    });
    return;
  }

  // GET /api/dashboard/analytics — BI 대시보드 분석 데이터
  if (pathname === '/api/dashboard/analytics' && method === 'GET') {
    const now = new Date();
    const ym = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');

    // 1. 월별 발주 추이 (최근 6개월)
    const monthlyPO = [];
    for (let i = 5; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const m = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
      const row = db.prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(total_qty),0) as qty FROM po_header WHERE po_date LIKE ? AND status != 'cancelled'`).get(m + '%');
      monthlyPO.push({ month: m, count: row.cnt, qty: row.qty });
    }

    // 2. 거래처별 발주 비중 (최근 3개월, 도넛 차트용)
    const threeMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 2, 1).toISOString().slice(0, 10);
    const vendorShare = db.prepare(`SELECT vendor_name as name, COUNT(*) as count, COALESCE(SUM(total_qty),0) as qty FROM po_header WHERE po_date >= ? AND status != 'cancelled' GROUP BY vendor_name ORDER BY count DESC LIMIT 8`).all(threeMonthsAgo);

    // 3. 발주 상태 분포 (도넛 차트용)
    const statusDist = db.prepare(`SELECT status, COUNT(*) as count FROM po_header WHERE status != 'cancelled' GROUP BY status`).all();
    const enToKo = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'os_pending':'OS등록대기' };
    statusDist.forEach(r => r.label = enToKo[r.status] || r.status);

    // 4. 리드타임 분석 (발주일~완료일)
    const ltRows = db.prepare(`SELECT po_date, updated_at, vendor_name FROM po_header WHERE status IN ('received','os_pending') AND po_date IS NOT NULL AND updated_at IS NOT NULL ORDER BY updated_at DESC LIMIT 50`).all();
    let totalLT = 0, ltCount = 0;
    const ltByVendor = {};
    ltRows.forEach(r => {
      const d1 = new Date(r.po_date), d2 = new Date(r.updated_at);
      if (d1 && d2 && d2 > d1) {
        const days = Math.round((d2 - d1) / 86400000);
        if (days > 0 && days < 90) {
          totalLT += days;
          ltCount++;
          if (!ltByVendor[r.vendor_name]) ltByVendor[r.vendor_name] = { total: 0, count: 0 };
          ltByVendor[r.vendor_name].total += days;
          ltByVendor[r.vendor_name].count++;
        }
      }
    });
    const avgLeadTime = ltCount > 0 ? Math.round(totalLT / ltCount * 10) / 10 : 0;
    const vendorLeadTime = Object.entries(ltByVendor).map(([name, v]) => ({ name, avg: Math.round(v.total / v.count * 10) / 10, count: v.count })).sort((a, b) => a.avg - b.avg);

    // 5. 불량률
    const defectTotal = db.prepare("SELECT COUNT(*) as cnt FROM defects").get().cnt;
    const defectMonth = db.prepare("SELECT COUNT(*) as cnt FROM defects WHERE created_at LIKE ?").get(ym + '%').cnt;

    // 6. 알림 (안전재고 미달 = urgent 품목 수, 납기 초과, 미승인 PO)
    const pendingPO = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status IN ('draft','sent')`).get().cnt;
    const overdueCount = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE expected_date < date('now') AND status NOT IN ('received','cancelled','os_pending')`).get().cnt;
    // 납기 임박 (D-3 이내)
    const upcomingDeadlineCount = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE expected_date >= date('now') AND expected_date <= date('now','+3 days') AND status NOT IN ('received','cancelled','os_pending')`).get().cnt;

    ok(res, {
      monthlyPO, vendorShare, statusDist, avgLeadTime, vendorLeadTime,
      defectTotal, defectMonth,
      alerts: { pendingPO, overdueCount, upcomingDeadlineCount }
    });
    return;
  }

  // GET /api/export/:type — 데이터 내보내기 (CSV)
  if (pathname.startsWith('/api/export/') && method === 'GET') {
    const type = pathname.split('/').pop();
    let rows = [], filename = '', headers = [];
    if (type === 'po') {
      rows = db.prepare('SELECT po_id, po_number, po_date, vendor_name, po_type, status, total_qty, expected_date, notes, created_at FROM po_header ORDER BY po_date DESC').all();
      filename = 'po_list.csv';
      headers = ['발주ID', '발주번호', '발주일', '거래처', '유형', '상태', '수량', '납기일', '비고', '생성일'];
    } else if (type === 'vendors') {
      rows = db.prepare('SELECT vendor_id, name, type, email, phone, contact, notes, created_at FROM vendors ORDER BY name').all();
      filename = 'vendors.csv';
      headers = ['ID', '거래처명', '유형', '이메일', '전화', '담당자', '비고', '생성일'];
    } else if (type === 'products') {
      rows = db.prepare('SELECT * FROM products ORDER BY product_code').all();
      filename = 'products.csv';
      headers = Object.keys(rows[0] || {});
    } else if (type === 'defects') {
      rows = db.prepare('SELECT * FROM defects ORDER BY created_at DESC').all();
      filename = 'defects.csv';
      headers = Object.keys(rows[0] || {});
    } else {
      fail(res, 400, 'Unknown export type');
      return;
    }
    // CSV 생성
    const bom = '\uFEFF';
    const csvLines = [headers.join(',')];
    rows.forEach(r => {
      const vals = Object.values(r).map(v => `"${String(v || '').replace(/"/g, '""')}"`);
      csvLines.push(vals.join(','));
    });
    const csv = bom + csvLines.join('\n');
    res.writeHead(200, { 'Content-Type': 'text/csv; charset=utf-8', 'Content-Disposition': `attachment; filename="${filename}"`, ...CORS });
    res.end(csv);
    return;
  }

  // GET /api/material-purchases — XERP MI(원자재입고) 제지사별 매입 현황
  if (pathname === '/api/material-purchases' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결'); return; }
    try {
      const from = parsed.searchParams.get('from') || '20250101';
      const to   = parsed.searchParams.get('to')   || '20260301';

      // product_info에서 원자재코드 목록 추출 (타겟 조회)
      let matCodes = [];
      try {
        const piPath = path.join(DATA_DIR, 'product_info.json');
        if (fs.existsSync(piPath)) {
          const pi = JSON.parse(fs.readFileSync(piPath, 'utf-8'));
          const codeSet = new Set();
          for (const info of Object.values(pi)) {
            if (info['원자재코드']) codeSet.add(info['원자재코드'].trim());
          }
          matCodes = [...codeSet];
        }
      } catch(_){}

      // 원자재코드 IN절로 타겟 조회 (인덱스 활용)
      const req = xerpPool.request();
      req.input('fromDate', sql.NChar(16), from);
      req.input('toDate',   sql.NChar(16), to);

      // 코드를 배치로 바인딩
      const codePlaceholders = matCodes.map((c, i) => { req.input(`c${i}`, sql.NChar(40), c); return `@c${i}`; }).join(',');

      const result = await req.query(`
          SELECT RTRIM(ItemCode) AS item_code,
                 MAX(RTRIM(ItemName)) AS item_name,
                 LEFT(InoutDate,6) AS ym,
                 SUM(InoutQty) AS total_qty,
                 CASE WHEN SUM(InoutQty) > 0 THEN SUM(InoutAmnt) / SUM(InoutQty) ELSE 0 END AS avg_price,
                 SUM(InoutAmnt) AS total_amount,
                 COUNT(*) AS cnt
          FROM mmInoutItem WITH (NOLOCK)
          WHERE SiteCode = 'BK10'
            AND InoutGubun = 'MI'
            AND InoutDate >= @fromDate AND InoutDate < @toDate
            ${matCodes.length ? 'AND ItemCode IN (' + codePlaceholders + ')' : ''}
          GROUP BY RTRIM(ItemCode), LEFT(InoutDate,6)
          ORDER BY RTRIM(ItemCode), LEFT(InoutDate,6)
        `);

      // product_info.json에서 원자재코드 → 제지사 매핑 로드
      let piMap = {};
      try {
        const piPath = path.join(DATA_DIR, 'product_info.json');
        if (fs.existsSync(piPath)) {
          const pi = JSON.parse(fs.readFileSync(piPath, 'utf-8'));
          for (const [code, info] of Object.entries(pi)) {
            if (info['원자재코드'] && info['제지사']) {
              piMap[info['원자재코드'].trim()] = {
                vendor: info['제지사'].trim(),
                paper_name: (info['원재료용지명'] || '').trim(),
                product_code: code
              };
            }
          }
        }
      } catch(_){}

      const rows = result.recordset.map(r => {
        const code = (r.item_code || '').trim();
        const mapping = piMap[code] || {};
        return {
          item_code: code,
          item_name: (r.item_name || '').trim(),
          paper_name: mapping.paper_name || '',
          vendor: mapping.vendor || '(미매핑)',
          product_code: mapping.product_code || '',
          ym: r.ym,
          total_qty: r.total_qty || 0,
          avg_price: Math.round(r.avg_price || 0),
          total_amount: r.total_amount || 0,
          cnt: r.cnt || 0
        };
      });

      ok(res, { rows, from, to });
    } catch(e) {
      console.error('원재료 매입 조회 오류:', e.message);
      fail(res, 500, '원재료 매입 조회 오류: ' + e.message);
    }
    return;
  }

  // GET /api/closing-verify?year=2026&month=2 — 매입금액 검증: XERP 실데이터 vs 화면 데이터 비교
  if (pathname === '/api/closing-verify' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 미연결'); return; }
    try {
      const year = parsed.searchParams.get('year') || '2026';
      const month = parsed.searchParams.get('month') || '2';
      const prevYear = (parseInt(year) - 1).toString();
      const moStr = month.padStart(2, '0');

      const req = xerpPool.request();
      req.input('fromDate', sql.NChar(16), prevYear + '0101');
      req.input('toDate', sql.NChar(16), year + moStr + '31');

      const result = await req.query(`
        SELECT RTRIM(h.CsCode) AS vendor_code,
               LEFT(h.OrderDate,4) AS yr,
               SUBSTRING(h.OrderDate,5,2) AS mo,
               SUM(i.OrderAmnt) AS amt
        FROM poOrderHeader h WITH (NOLOCK)
        JOIN poOrderItem i WITH (NOLOCK) ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
        WHERE h.SiteCode = 'BK10'
          AND h.OrderDate >= @fromDate AND h.OrderDate <= @toDate
        GROUP BY RTRIM(h.CsCode), LEFT(h.OrderDate,4), SUBSTRING(h.OrderDate,5,2)
        ORDER BY RTRIM(h.CsCode), LEFT(h.OrderDate,4), SUBSTRING(h.OrderDate,5,2)
      `);

      // 거래처별/연도별 월별 집계
      const vendors = {};
      for (const r of result.recordset) {
        const vc = (r.vendor_code || '').trim();
        const yr = (r.yr || '').trim();
        const moIdx = parseInt(r.mo) - 1;
        if (!vendors[vc]) vendors[vc] = {};
        if (!vendors[vc][yr]) vendors[vc][yr] = new Array(12).fill(0);
        vendors[vc][yr][moIdx] += Math.round(r.amt || 0);
      }

      ok(res, { year, month, prev_year: prevYear, vendors });
    } catch (e) {
      console.error('closing-verify 오류:', e.message);
      fail(res, 500, e.message);
    }
    return;
  }

  // GET /api/report-receiving?year=2025 — 수불부 원재료 거래처별 품목 매입
  // mode=columns: 테이블 컬럼 확인용
  if (pathname === '/api/report-receiving' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 미연결'); return; }
    try {
      const year = parsed.searchParams.get('year') || '2025';
      const mode = parsed.searchParams.get('mode') || '';
      const req = xerpPool.request();

      // 컬럼 탐색 모드
      if (mode === 'columns') {
        const tables = ['mmInoutHeader','mmInoutItem','lgInoutHeader','lgInoutItem','lgMoveHeader','lgMoveItem'];
        const cols = {};
        for (const t of tables) {
          try {
            const r = await xerpPool.request().query(`SELECT TOP 0 * FROM ${t} WITH (NOLOCK)`);
            cols[t] = Object.keys(r.recordset.columns);
          } catch(e) { cols[t] = 'NOT_FOUND: ' + e.message.substring(0,60); }
        }
        ok(res, cols);
        return;
      }

      // from/to 지원: ?from=202501&to=202602 형태 (기본: year 전체)
      const fromParam = parsed.searchParams.get('from') || (year + '01');
      const toParam = parsed.searchParams.get('to') || (year + '12');
      const fromDate = fromParam + '01';
      const toDate = toParam + '31';

      req.input('fromDate', sql.NChar(16), fromDate);
      req.input('toDate', sql.NChar(16), toDate);

      // 발주 기반: poOrderHeader + poOrderItem — 거래처별 원재료 품목 매입 (년-월별)
      const result = await req.query(`
        SELECT RTRIM(h.CsCode) AS vendor_code,
               RTRIM(i.ItemCode) AS item_code,
               MAX(RTRIM(i.ItemSpec)) AS item_spec,
               LEFT(h.OrderDate,6) AS ym,
               SUM(i.OrderQty) AS qty,
               SUM(i.OrderAmnt) AS amt
        FROM poOrderHeader h WITH (NOLOCK)
        JOIN poOrderItem i WITH (NOLOCK)
          ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
        WHERE h.SiteCode='BK10'
          AND h.OrderDate >= @fromDate AND h.OrderDate <= @toDate
          AND RTRIM(h.CsCode) IN ('2015259','2100005','2100013','2100006','2013391')
        GROUP BY RTRIM(h.CsCode), RTRIM(i.ItemCode), LEFT(h.OrderDate,6)
        ORDER BY RTRIM(h.CsCode), RTRIM(i.ItemCode), LEFT(h.OrderDate,6)
      `);

      // 월 컬럼 목록 생성 (from~to)
      const monthCols = [];
      let cur = fromParam;
      while (cur <= toParam) {
        monthCols.push(cur);
        let y = parseInt(cur.substring(0,4)), m = parseInt(cur.substring(4,6));
        m++; if (m > 12) { m = 1; y++; }
        cur = String(y) + String(m).padStart(2,'0');
      }

      // 품목명 보충 (product_info.json)
      const pi = getProductInfo();
      const nameMap = {};
      if (pi) {
        for (const [, info] of Object.entries(pi)) {
          const mc = (info['원자재코드'] || '').trim();
          const mn = (info['원재료용지명'] || info['원재료명'] || '').trim();
          if (mc && mn && !nameMap[mc]) nameMap[mc] = mn;
        }
      }

      // 거래처별 > 품목별 집계 (년-월 키 기반)
      const vendors = {};
      for (const r of result.recordset) {
        const vc = (r.vendor_code || '').trim();
        const ic = (r.item_code || '').trim();
        const ym = (r.ym || '').trim();
        if (!vendors[vc]) vendors[vc] = { code: vc, items: {} };
        if (!vendors[vc].items[ic]) {
          const spec = (r.item_spec || '').trim();
          const monthlyAmt = {};
          monthCols.forEach(m => { monthlyAmt[m] = 0; });
          vendors[vc].items[ic] = { code: ic, name: nameMap[ic] || spec || ic, monthly_amt: monthlyAmt };
        }
        if (vendors[vc].items[ic].monthly_amt[ym] !== undefined) {
          vendors[vc].items[ic].monthly_amt[ym] += Math.round(r.amt || 0);
        }
      }

      const out = {};
      for (const [vc, vd] of Object.entries(vendors)) {
        const itemList = Object.values(vd.items).map(it => {
          const totalAmt = Object.values(it.monthly_amt).reduce((a,b) => a+b, 0);
          return { ...it, total_amt: totalAmt };
        });
        itemList.sort((a,b) => b.total_amt - a.total_amt);
        out[vc] = { code: vc, total_amt: itemList.reduce((s,i) => s + i.total_amt, 0), items: itemList };
      }

      ok(res, { from: fromParam, to: toParam, months: monthCols, record_count: result.recordset.length, vendors: out });
    } catch (e) {
      console.error('report-receiving 오류:', e.message);
      fail(res, 500, e.message);
    }
    return;
  }

  // GET /api/report-vendor-price?vendor_code=2015259&keyword=킨니 — 거래처 품목별 단가 이력 (보고서용)
  if (pathname === '/api/report-vendor-price' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 미연결'); return; }
    try {
      const vendorCode = parsed.searchParams.get('vendor_code') || '';
      const keyword = parsed.searchParams.get('keyword') || '';

      // product_info.json에서 키워드 매칭 원자재코드 추출
      const pi = getProductInfo();
      const matchCodes = new Set();
      if (pi && keyword) {
        for (const [, info] of Object.entries(pi)) {
          const matName = (info['원재료용지명'] || info['원재료명'] || '').trim();
          const matCode = (info['원자재코드'] || '').trim();
          if (matCode && matName.includes(keyword)) matchCodes.add(matCode);
        }
      }

      const req = xerpPool.request();
      req.input('vendorCode', sql.NChar(16), vendorCode);

      // 2024~2026 전체 발주 내역 (단가 포함)
      const result = await req.query(`
        SELECT RTRIM(i.ItemCode) AS item_code,
               MAX(RTRIM(i.ItemSpec)) AS item_spec,
               LEFT(h.OrderDate,4) AS yr,
               SUBSTRING(h.OrderDate,5,2) AS mo,
               SUM(i.OrderAmnt) AS amt,
               SUM(i.OrderQty) AS qty
        FROM poOrderHeader h WITH (NOLOCK)
        JOIN poOrderItem i WITH (NOLOCK) ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
        WHERE h.SiteCode = 'BK10'
          AND h.CsCode = @vendorCode
          AND h.OrderDate >= '20240101' AND h.OrderDate <= '20261231'
        GROUP BY RTRIM(i.ItemCode), LEFT(h.OrderDate,4), SUBSTRING(h.OrderDate,5,2)
        ORDER BY RTRIM(i.ItemCode), LEFT(h.OrderDate,4), SUBSTRING(h.OrderDate,5,2)
      `);

      // 품목명 매핑
      const nameMap = {};
      if (pi) {
        for (const [, info] of Object.entries(pi)) {
          const mc = (info['원자재코드'] || '').trim();
          const mn = (info['원재료용지명'] || info['원재료명'] || '').trim();
          if (mc && mn && !nameMap[mc]) nameMap[mc] = mn;
        }
      }

      // 품목별 집계
      const items = {};
      for (const r of result.recordset) {
        const code = (r.item_code || '').trim();
        const yr = (r.yr || '').trim();
        const mo = parseInt(r.mo) - 1;
        if (!items[code]) items[code] = { code, name: nameMap[code] || '', spec: (r.item_spec || '').trim(), years: {} };
        if (!items[code].years[yr]) items[code].years[yr] = { monthly_amt: new Array(12).fill(0), monthly_qty: new Array(12).fill(0) };
        items[code].years[yr].monthly_amt[mo] += Math.round(r.amt || 0);
        items[code].years[yr].monthly_qty[mo] += Math.round(r.qty || 0);
      }

      // 키워드 필터링
      let filtered = Object.values(items);
      if (keyword && matchCodes.size > 0) {
        filtered = filtered.filter(it => matchCodes.has(it.code) || it.name.includes(keyword) || it.spec.includes(keyword));
      }

      // 단가 계산 (월별 금액/수량)
      const rows = filtered.map(it => {
        const result = { code: it.code, name: it.name, spec: it.spec, years: {} };
        for (const [yr, data] of Object.entries(it.years)) {
          const totalAmt = data.monthly_amt.reduce((a,b) => a+b, 0);
          const totalQty = data.monthly_qty.reduce((a,b) => a+b, 0);
          const avgPrice = totalQty > 0 ? Math.round(totalAmt / totalQty) : 0;
          // 월별 단가
          const monthlyPrice = data.monthly_amt.map((a, i) => {
            const q = data.monthly_qty[i];
            return q > 0 ? Math.round(a / q) : 0;
          });
          result.years[yr] = { monthly_amt: data.monthly_amt, monthly_qty: data.monthly_qty, monthly_price: monthlyPrice, total_amt: totalAmt, total_qty: totalQty, avg_price: avgPrice };
        }
        return result;
      });

      // 금액순 정렬 (25년 기준)
      rows.sort((a, b) => ((b.years['2025'] || {}).total_amt || 0) - ((a.years['2025'] || {}).total_amt || 0));

      ok(res, { vendor_code: vendorCode, keyword, match_codes: [...matchCodes], items: rows });
    } catch (e) {
      console.error('report-vendor-price 오류:', e.message);
      fail(res, 500, e.message);
    }
    return;
  }

  // ── 보고서 CRUD API ──
  // GET /api/reports — 보고서 목록
  if (pathname === '/api/reports' && method === 'GET') {
    const rows = db.prepare(`SELECT id, title, subtitle, report_type, created_at, updated_at FROM reports ORDER BY created_at DESC`).all();
    ok(res, rows);
    return;
  }

  // GET /api/reports/:id — 보고서 상세
  if (pathname.match(/^\/api\/reports\/\d+$/) && method === 'GET') {
    const id = parseInt(pathname.split('/').pop());
    const row = db.prepare('SELECT * FROM reports WHERE id=?').get(id);
    if (!row) { fail(res, 404, '보고서를 찾을 수 없습니다'); return; }
    ok(res, row);
    return;
  }

  // POST /api/reports — 보고서 저장
  if (pathname === '/api/reports' && method === 'POST') {
    const body = await readJSON(req);
    const { title, subtitle, report_type, content } = body;
    if (!title) { fail(res, 400, '제목 필수'); return; }
    const result = db.prepare(`INSERT INTO reports (title, subtitle, report_type, content) VALUES (?,?,?,?)`).run(
      title, subtitle || '', report_type || 'general', typeof content === 'string' ? content : JSON.stringify(content || {})
    );
    ok(res, { id: result.lastInsertRowid });
    return;
  }

  // PUT /api/reports/:id — 보고서 수정
  if (pathname.match(/^\/api\/reports\/\d+$/) && method === 'PUT') {
    const id = parseInt(pathname.split('/').pop());
    const body = await readJSON(req);
    const existing = db.prepare('SELECT * FROM reports WHERE id=?').get(id);
    if (!existing) { fail(res, 404, '보고서를 찾을 수 없습니다'); return; }
    const title = body.title !== undefined ? body.title : existing.title;
    const subtitle = body.subtitle !== undefined ? body.subtitle : existing.subtitle;
    const report_type = body.report_type !== undefined ? body.report_type : existing.report_type;
    const content = body.content !== undefined ? (typeof body.content === 'string' ? body.content : JSON.stringify(body.content)) : existing.content;
    db.prepare(`UPDATE reports SET title=?, subtitle=?, report_type=?, content=?, updated_at=datetime('now','localtime') WHERE id=?`).run(title, subtitle, report_type, content, id);
    ok(res, { id, updated: true });
    return;
  }

  // DELETE /api/reports/:id — 보고서 삭제
  if (pathname.match(/^\/api\/reports\/\d+$/) && method === 'DELETE') {
    const id = parseInt(pathname.split('/').pop());
    db.prepare('DELETE FROM reports WHERE id=?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // GET /api/closing-vendor-items?vendor_code=2013391&year=2026&month=2 — 거래처별 품목 상세 (한솔PNS 예시)
  if (pathname === '/api/closing-vendor-items' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 미연결'); return; }
    try {
      const vendorCode = parsed.searchParams.get('vendor_code') || '';
      const year = parsed.searchParams.get('year') || '2026';
      const month = parsed.searchParams.get('month') || '2';
      const prevYear = (parseInt(year) - 1).toString();
      const moStr = month.padStart(2, '0');

      const req = xerpPool.request();
      req.input('vendorCode', sql.NChar(16), vendorCode);
      req.input('fromDate', sql.NChar(16), prevYear + '0101');
      req.input('toDate', sql.NChar(16), year + moStr + '31');

      const result = await req.query(`
        SELECT RTRIM(i.ItemCode) AS item_code,
               MAX(RTRIM(i.ItemSpec)) AS item_spec,
               LEFT(h.OrderDate,4) AS yr,
               SUBSTRING(h.OrderDate,5,2) AS mo,
               SUM(i.OrderAmnt) AS amt,
               SUM(i.OrderQty) AS qty
        FROM poOrderHeader h WITH (NOLOCK)
        JOIN poOrderItem i WITH (NOLOCK) ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
        WHERE h.SiteCode = 'BK10'
          AND h.CsCode = @vendorCode
          AND h.OrderDate >= @fromDate AND h.OrderDate <= @toDate
        GROUP BY RTRIM(i.ItemCode), LEFT(h.OrderDate,4), SUBSTRING(h.OrderDate,5,2)
        ORDER BY RTRIM(i.ItemCode), LEFT(h.OrderDate,4), SUBSTRING(h.OrderDate,5,2)
      `);

      // 품목명 조회: product_info.json에서 원자재코드→원재료명 매핑 (가장 빠르고 정확)
      const itemCodes = [...new Set(result.recordset.map(r => (r.item_code || '').trim()).filter(Boolean))];
      const itemNameMap = {};
      const pi = getProductInfo();
      if (pi && typeof pi === 'object') {
        // product_info.json: {제품코드: {원자재코드:'BP2004', 원재료용지명:'250스노우화이트', ...}}
        // 역매핑: 원자재코드 → 원재료용지명
        for (const [, info] of Object.entries(pi)) {
          const matCode = (info['원자재코드'] || '').trim();
          const matName = (info['원재료용지명'] || info['원재료명'] || '').trim();
          if (matCode && matName && !itemNameMap[matCode]) {
            itemNameMap[matCode] = matName;
          }
        }
      }

      // 품목별 집계
      const items = {};
      for (const r of result.recordset) {
        const code = (r.item_code || '').trim();
        const yr = (r.yr || '').trim();
        const mo = parseInt(r.mo) - 1;
        if (!items[code]) items[code] = { code, name: itemNameMap[code] || '', spec: (r.item_spec || '').trim(), years: {} };
        if (!items[code].years[yr]) items[code].years[yr] = new Array(12).fill(0);
        items[code].years[yr][mo] += Math.round(r.amt || 0);
      }

      // 당월(선택연도/월) 금액 기준 정렬
      const moIdx = parseInt(month) - 1;
      const sorted = Object.values(items).sort((a, b) => {
        const aAmt = (a.years[year] || [])[moIdx] || 0;
        const bAmt = (b.years[year] || [])[moIdx] || 0;
        return bAmt - aAmt;
      });

      // 당월 총액 계산
      let monthTotal = 0;
      sorted.forEach(it => { monthTotal += (it.years[year] || [])[moIdx] || 0; });

      // 응답: 품목 목록 + 당월 총액 + 50% 이상 품목 표시
      const rows = sorted.map(it => {
        const currMonth = (it.years[year] || [])[moIdx] || 0;
        const pct = monthTotal > 0 ? (currMonth / monthTotal * 100) : 0;
        return {
          code: it.code,
          name: it.name,
          spec: it.spec,
          curr_year: it.years[year] || new Array(12).fill(0),
          prev_year: it.years[prevYear] || new Array(12).fill(0),
          curr_month_amt: currMonth,
          curr_month_pct: Math.round(pct * 10) / 10,
          is_major: pct >= 50
        };
      });

      ok(res, { vendor_code: vendorCode, year, month, prev_year: prevYear, month_total: monthTotal, items: rows });
    } catch (e) {
      console.error('closing-vendor-items 오류:', e.message);
      fail(res, 500, e.message);
    }
    return;
  }

  // GET /api/po-drafts — 발주서 목록
  if (pathname === '/api/po-drafts' && method === 'GET') {
    const rows = db.prepare(`SELECT * FROM po_drafts ORDER BY created_at DESC`).all();
    ok(res, rows);
    return;
  }

  // POST /api/po-drafts — 발주서 저장
  if (pathname === '/api/po-drafts' && method === 'POST') {
    const body = await readJSON(req);
    const { po_number, po_date, due_date, vendor_id, vendor_name, vendor_contact, vendor_phone, vendor_email,
            issuer_name, issuer_contact, issuer_phone, issuer_email, payment_terms, remark,
            items, total_supply, total_tax, total_amount } = body;
    const result = db.prepare(`INSERT INTO po_drafts
      (po_number,po_date,due_date,vendor_id,vendor_name,vendor_contact,vendor_phone,vendor_email,
       issuer_name,issuer_contact,issuer_phone,issuer_email,payment_terms,remark,
       items,total_supply,total_tax,total_amount)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      po_number||'', po_date||'', due_date||'', vendor_id||0, vendor_name||'',
      vendor_contact||'', vendor_phone||'', vendor_email||'',
      issuer_name||'바른컴퍼니', issuer_contact||'', issuer_phone||'', issuer_email||'',
      payment_terms||'', remark||'',
      typeof items === 'string' ? items : JSON.stringify(items||[]),
      total_supply||0, total_tax||0, total_amount||0
    );
    ok(res, { id: result.lastInsertRowid });
    return;
  }

  // POST /api/po-drafts/:id/email — 발주서 이메일 발송
  if (pathname.match(/^\/api\/po-drafts\/\d+\/email$/) && method === 'POST') {
    const id = parseInt(pathname.split('/')[3]);
    const draft = db.prepare('SELECT * FROM po_drafts WHERE id=?').get(id);
    if (!draft) { fail(res, 404, '발주서 없음'); return; }
    if (!smtpTransporter) { fail(res, 503, 'SMTP 미설정 — .env에 SMTP_USER, SMTP_PASS 추가 필요'); return; }
    const body = await readJSON(req);
    const to = body.to || draft.vendor_email || '';
    const cc = body.cc || '';
    const subject = body.subject || `[발주서] ${draft.po_number} - 바른컴퍼니`;
    if (!to) { fail(res, 400, '수신 이메일을 입력하세요'); return; }
    const items = (() => { try { return JSON.parse(draft.items||'[]'); } catch { return []; } })();
    const fN = n => (n||0).toLocaleString();
    const itemsHTML = items.filter(x=>x.name).map((it,i) => `
      <tr style="border-bottom:1px solid #f0f0f0">
        <td style="padding:8px 12px;text-align:center;color:#888;font-size:12px">${i+1}</td>
        <td style="padding:8px 12px;font-weight:600">${it.name||''}</td>
        <td style="padding:8px 12px;color:#555;font-size:12px">${it.spec||''}</td>
        <td style="padding:8px 12px;text-align:center;font-size:12px">${it.unit||'EA'}</td>
        <td style="padding:8px 12px;text-align:right">${fN(it.qty)}</td>
        <td style="padding:8px 12px;text-align:right">${fN(it.price)}</td>
        <td style="padding:8px 12px;text-align:right;font-weight:700;color:#0f172a">${fN(it.supply)}</td>
        <td style="padding:8px 12px;text-align:right;color:#666">${fN(it.tax)}</td>
      </tr>`).join('');
    const html = `<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;background:#f8fafc;margin:0;padding:24px">
<div style="max-width:700px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.10)">
  <div style="background:linear-gradient(135deg,#1e293b,#0f172a);padding:22px 32px;color:#fff">
    <div style="font-size:10px;color:#94a3b8;letter-spacing:2px;margin-bottom:2px">BARUN COMPANY</div>
    <div style="font-size:30px;font-weight:700;letter-spacing:8px;margin-bottom:10px">발 주 서</div>
    <div style="font-size:12px;color:#cbd5e1;display:flex;gap:20px">
      <span>발주번호 <b style="color:#fff">${draft.po_number}</b></span>
      <span>발주일 <b style="color:#fff">${draft.po_date||''}</b></span>
      ${draft.due_date?`<span>납기예정 <b style="color:#fff">${draft.due_date}</b></span>`:''}
    </div>
  </div>
  <div style="display:flex;padding:18px 32px;gap:24px;background:#f8fafc;border-bottom:1px solid #e2e8f0">
    <div style="flex:1">
      <div style="font-size:10px;font-weight:700;color:#1d4ed8;background:#eff6ff;border:1px solid #bfdbfe;display:inline-block;padding:2px 8px;border-radius:3px;margin-bottom:8px">수 신</div>
      <div style="font-size:15px;font-weight:700;margin-bottom:4px">${draft.vendor_name||''}</div>
      ${draft.vendor_contact?`<div style="font-size:13px;color:#555">담당자: ${draft.vendor_contact}</div>`:''}
      ${draft.vendor_phone?`<div style="font-size:13px;color:#555">연락처: ${draft.vendor_phone}</div>`:''}
      ${draft.payment_terms?`<div style="font-size:12px;color:#888;margin-top:4px">결제조건: ${draft.payment_terms}</div>`:''}
    </div>
    <div style="flex:1">
      <div style="font-size:10px;font-weight:700;color:#16a34a;background:#f0fdf4;border:1px solid #bbf7d0;display:inline-block;padding:2px 8px;border-radius:3px;margin-bottom:8px">발 주</div>
      <div style="font-size:15px;font-weight:700;margin-bottom:4px">${draft.issuer_name||'바른컴퍼니'}</div>
      ${draft.issuer_contact?`<div style="font-size:13px;color:#555">담당자: ${draft.issuer_contact}</div>`:''}
      ${draft.issuer_phone?`<div style="font-size:13px;color:#555">연락처: ${draft.issuer_phone}</div>`:''}
    </div>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:13px">
    <thead><tr style="background:#1e293b;color:#e2e8f0">
      <th style="padding:9px 12px;font-weight:600;width:36px">No</th>
      <th style="padding:9px 12px;font-weight:600;text-align:left">품목명</th>
      <th style="padding:9px 12px;font-weight:600;text-align:left">규격</th>
      <th style="padding:9px 12px;font-weight:600;width:48px">단위</th>
      <th style="padding:9px 12px;font-weight:600;text-align:right;width:64px">수량</th>
      <th style="padding:9px 12px;font-weight:600;text-align:right;width:80px">단가</th>
      <th style="padding:9px 12px;font-weight:600;text-align:right;width:96px">공급가액</th>
      <th style="padding:9px 12px;font-weight:600;text-align:right;width:80px">세액</th>
    </tr></thead>
    <tbody>${itemsHTML}</tbody>
    <tfoot><tr style="background:#f1f5f9;font-weight:700;border-top:2px solid #e2e8f0">
      <td colspan="4" style="padding:9px 12px;text-align:center;color:#555;font-size:12px">합 계</td>
      <td style="padding:9px 12px;text-align:right">${fN(items.reduce((s,x)=>s+(x.qty||0),0))}</td>
      <td></td>
      <td style="padding:9px 12px;text-align:right">${fN(draft.total_supply)}</td>
      <td style="padding:9px 12px;text-align:right">${fN(draft.total_tax)}</td>
    </tr></tfoot>
  </table>
  <div style="display:flex;justify-content:flex-end;padding:16px 32px;background:#f8fafc;border-top:1px solid #e2e8f0">
    <div style="width:260px">
      <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #e9ecef"><span style="font-size:12px;color:#64748b">공급가액 합계</span><span style="font-size:13px;font-weight:700">${fN(draft.total_supply)} 원</span></div>
      <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #e9ecef"><span style="font-size:12px;color:#64748b">세액 합계</span><span style="font-size:13px;font-weight:700">${fN(draft.total_tax)} 원</span></div>
      <div style="display:flex;justify-content:space-between;padding:8px 0;border-top:2px solid #1e293b;margin-top:4px"><span style="font-size:14px;font-weight:700">청 구 합 계</span><span style="font-size:18px;font-weight:800;color:#dc2626">${fN(draft.total_amount)} 원</span></div>
    </div>
  </div>
  ${draft.remark?`<div style="padding:14px 32px;border-top:1px solid #e2e8f0"><div style="font-size:11px;font-weight:700;color:#64748b;margin-bottom:6px">비 고</div><div style="font-size:13px;color:#374151;background:#f8fafc;border-radius:6px;padding:10px 12px">${draft.remark}</div></div>`:''}
  ${body.message?`<div style="padding:14px 32px;border-top:1px solid #e2e8f0"><div style="font-size:13px;color:#374151;white-space:pre-line">${body.message}</div></div>`:''}
  <div style="padding:12px 32px;background:#f1f5f9;border-top:1px solid #e2e8f0;font-size:11px;color:#94a3b8;text-align:center">본 발주서는 바른컴퍼니 ERP에서 발송되었습니다.</div>
</div></body></html>`;
    try {
      await smtpTransporter.sendMail({ from:`바른컴퍼니 <${SMTP_FROM}>`, to, cc:cc||undefined, subject, html });
      ok(res, { sent: true, to });
    } catch(e) {
      console.error('발주서 이메일 오류:', e.message);
      fail(res, 500, '이메일 발송 실패: ' + e.message);
    }
    return;
  }

  // DELETE /api/po-drafts/:id — 발주서 삭제
  if (pathname.match(/^\/api\/po-drafts\/\d+$/) && method === 'DELETE') {
    const id = parseInt(pathname.split('/').pop());
    db.prepare(`DELETE FROM po_drafts WHERE id=?`).run(id);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/purchases?year=2026 — 월별 업체별 품목별 매입 집계
  if (pathname === '/api/purchases' && method === 'GET') {
    const year = parsed.searchParams.get('year') || new Date().getFullYear().toString();
    const rows = db.prepare(`
      SELECT i.vendor_name, ii.product_code, ii.product_name,
             substr(i.invoice_date, 6, 2) as month,
             SUM(ii.qty) as total_qty, SUM(ii.amount) as total_amount
      FROM invoices i
      JOIN invoice_items ii ON i.invoice_id = ii.invoice_id
      WHERE substr(i.invoice_date, 1, 4) = ?
      GROUP BY i.vendor_name, ii.product_code, substr(i.invoice_date, 6, 2)
      ORDER BY i.vendor_name, ii.product_code, month
    `).all(year);
    ok(res, rows);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: VENDOR NOTES (미팅일지/특이사항)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/notes' && method === 'GET') {
    let sql = 'SELECT * FROM vendor_notes WHERE 1=1';
    const params = [];
    const vid = parsed.searchParams.get('vendor_id');
    const ntype = parsed.searchParams.get('note_type');
    const from = parsed.searchParams.get('from');
    const to = parsed.searchParams.get('to');
    const q = parsed.searchParams.get('q');
    if (vid) { sql += ' AND vendor_id = ?'; params.push(parseInt(vid)); }
    if (ntype) { sql += ' AND note_type = ?'; params.push(ntype); }
    if (from) { sql += ' AND note_date >= ?'; params.push(from); }
    if (to) { sql += ' AND note_date <= ?'; params.push(to); }
    if (q) { sql += ' AND (title LIKE ? OR content LIKE ?)'; params.push('%'+q+'%', '%'+q+'%'); }
    sql += ' ORDER BY note_date DESC, note_id DESC';
    const rows = db.prepare(sql).all(...params);
    ok(res, rows);
    return;
  }

  const noteGet = pathname.match(/^\/api\/notes\/(\d+)$/);
  if (noteGet && method === 'GET') {
    const id = parseInt(noteGet[1]);
    const note = db.prepare('SELECT * FROM vendor_notes WHERE note_id = ?').get(id);
    if (!note) { fail(res, 404, 'Note not found'); return; }
    ok(res, note);
    return;
  }

  if (pathname === '/api/notes' && method === 'POST') {
    const body = await readJSON(req);
    const info = db.prepare(`INSERT INTO vendor_notes (vendor_id, vendor_name, title, content, note_type, note_date) VALUES (?, ?, ?, ?, ?, ?)`).run(
      body.vendor_id || null,
      body.vendor_name || '',
      body.title || '',
      body.content || '',
      body.note_type || 'meeting',
      body.note_date || new Date().toISOString().slice(0, 10)
    );
    ok(res, { note_id: info.lastInsertRowid });
    return;
  }

  const notePut = pathname.match(/^\/api\/notes\/(\d+)$/);
  if (notePut && method === 'PUT') {
    const id = parseInt(notePut[1]);
    const body = await readJSON(req);
    const fields = [];
    const params = { id };
    for (const col of ['vendor_id', 'vendor_name', 'title', 'content', 'note_type', 'note_date', 'status']) {
      if (body[col] !== undefined) {
        fields.push(`${col} = @${col}`);
        params[col] = body[col];
      }
    }
    if (fields.length === 0) { fail(res, 400, 'No fields to update'); return; }
    fields.push(`updated_at = datetime('now','localtime')`);
    db.prepare(`UPDATE vendor_notes SET ${fields.join(', ')} WHERE note_id = @id`).run(params);
    ok(res, { note_id: id });
    return;
  }

  const noteDel = pathname.match(/^\/api\/notes\/(\d+)$/);
  if (noteDel && method === 'DELETE') {
    const id = parseInt(noteDel[1]);
    db.prepare('DELETE FROM note_comments WHERE note_id = ?').run(id);
    db.prepare('DELETE FROM vendor_notes WHERE note_id = ?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // GET /api/notes/:id/comments
  const noteComGet = pathname.match(/^\/api\/notes\/(\d+)\/comments$/);
  if (noteComGet && method === 'GET') {
    const rows = db.prepare('SELECT * FROM note_comments WHERE note_id=? ORDER BY created_at ASC').all(noteComGet[1]);
    ok(res, rows);
    return;
  }

  // POST /api/notes/:id/comments
  const noteComPost = pathname.match(/^\/api\/notes\/(\d+)\/comments$/);
  if (noteComPost && method === 'POST') {
    const b = await readJSON(req);
    if (!b.content?.trim()) { fail(res, 400, 'content required'); return; }
    const info = db.prepare('INSERT INTO note_comments (note_id, author, content) VALUES (?,?,?)').run(
      parseInt(noteComPost[1]), b.author||'', b.content.trim()
    );
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  // DELETE /api/note-comments/:id
  const noteComDel = pathname.match(/^\/api\/note-comments\/(\d+)$/);
  if (noteComDel && method === 'DELETE') {
    db.prepare('DELETE FROM note_comments WHERE id=?').run(noteComDel[1]);
    ok(res, { deleted: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  BOM API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/bom' && method === 'GET') {
    const rows = db.prepare(`SELECT b.*, (SELECT COUNT(*) FROM bom_items WHERE bom_id=b.bom_id) as item_count FROM bom_header b ORDER BY b.product_code`).all();
    ok(res, rows);
    return;
  }

  // GET /api/bom/export — BOM 전체를 플랫 CSV용 데이터로
  if (pathname === '/api/bom/export' && method === 'GET') {
    const headers = db.prepare('SELECT * FROM bom_header ORDER BY product_code').all();
    const processes = ['재단','인쇄','박/형압','톰슨','봉투가공','세아리','레이져','실크','임가공'];
    const rows = headers.map(h => {
      const items = db.prepare('SELECT * FROM bom_items WHERE bom_id=? ORDER BY sort_order').all(h.bom_id);
      const mat = items.find(i => i.item_type === 'material') || {};
      const row = {
        product_code: h.product_code, product_name: h.product_name||'', brand: h.brand||'',
        material_code: mat.material_code||'', material_name: mat.material_name||'',
        vendor_name: mat.vendor_name||'', cut_spec: mat.cut_spec||'', plate_spec: mat.plate_spec||''
      };
      processes.forEach(p => { const proc = items.find(i => i.process_type === p); row[p] = proc ? proc.vendor_name : ''; });
      return row;
    });
    ok(res, rows);
    return;
  }

  // POST /api/bom/bulk-upload — CSV 파싱 결과 일괄 등록
  if (pathname === '/api/bom/bulk-upload' && method === 'POST') {
    const body = await readJSON(req);
    const rows = body.rows || [];
    const processes = ['재단','인쇄','박/형압','톰슨','봉투가공','세아리','레이져','실크','임가공'];
    let updated = 0, created = 0;
    const txn = db.transaction(() => {
      for (const r of rows) {
        if (!r.product_code) continue;
        let header = db.prepare('SELECT bom_id FROM bom_header WHERE product_code=?').get(r.product_code);
        if (header) {
          db.prepare('UPDATE bom_header SET product_name=?, brand=?, updated_at=datetime(\'now\',\'localtime\') WHERE bom_id=?').run(r.product_name||'', r.brand||'', header.bom_id);
          db.prepare('DELETE FROM bom_items WHERE bom_id=?').run(header.bom_id);
          updated++;
        } else {
          const ins = db.prepare('INSERT INTO bom_header (product_code, product_name, brand) VALUES (?,?,?)').run(r.product_code, r.product_name||'', r.brand||'');
          header = { bom_id: ins.lastInsertRowid };
          created++;
        }
        const insItem = db.prepare('INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, sort_order) VALUES (?,?,?,?,?,?,?,?,?,?)');
        let sort = 0;
        if (r.material_code) {
          insItem.run(header.bom_id, 'material', r.material_code, r.material_name||'', r.vendor_name||'', '원재료', 1, r.cut_spec||'', r.plate_spec||'', sort++);
        }
        processes.forEach(p => {
          if (r[p]) insItem.run(header.bom_id, 'process', '', '', r[p], p, 1, '', '', sort++);
        });
      }
    });
    txn();
    ok(res, { created, updated, total: created + updated });
    return;
  }

  const bomGet = pathname.match(/^\/api\/bom\/(.+)$/);
  if (bomGet && method === 'GET' && bomGet[1] !== 'import') {
    const code = decodeURIComponent(bomGet[1]);
    const header = db.prepare('SELECT * FROM bom_header WHERE product_code = ? OR bom_id = ?').get(code, parseInt(code)||0);
    if (!header) { res.writeHead(404); res.end(JSON.stringify({error:'not found'})); return; }
    const items = db.prepare('SELECT * FROM bom_items WHERE bom_id = ? ORDER BY sort_order, bom_item_id').all(header.bom_id);
    ok(res, { ...header, items });
    return;
  }

  if (pathname === '/api/bom' && method === 'POST') {
    const b = await readJSON(req);
    const ins = db.prepare('INSERT INTO bom_header (product_code, product_name, brand, notes, default_order_qty, finished_w, finished_h) VALUES (?,?,?,?,?,?,?)');
    const insItem = db.prepare(`INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, unit, notes, sort_order,
      material_type, paper_standard, paper_type, gsm, finished_w, finished_h, bleed, grip, loss_rate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?)`);
    const txn = db.transaction(() => {
      const r = ins.run(b.product_code, b.product_name||'', b.brand||'', b.notes||'', b.default_order_qty||1000, b.finished_w||0, b.finished_h||0);
      const bomId = r.lastInsertRowid;
      (b.items||[]).forEach((it,i) => {
        insItem.run(bomId, it.item_type||'material', it.material_code||'', it.material_name||'', it.vendor_name||'', it.process_type||'', it.qty_per||1, it.cut_spec||'', it.plate_spec||'', it.unit||'EA', it.notes||'', i,
          it.material_type||'IMPOSITION', it.paper_standard||'', it.paper_type||'', it.gsm||0, it.finished_w||0, it.finished_h||0, it.bleed??3, it.grip??10, it.loss_rate??5);
      });
      return bomId;
    });
    const bomId = txn();
    ok(res, { bom_id: bomId });
    return;
  }

  const bomPut = pathname.match(/^\/api\/bom\/(\d+)$/);
  if (bomPut && method === 'PUT') {
    const bomId = parseInt(bomPut[1]);
    const b = await readJSON(req);
    const txn = db.transaction(() => {
      if (b.product_name !== undefined) db.prepare("UPDATE bom_header SET product_name=?, brand=?, notes=?, default_order_qty=?, finished_w=?, finished_h=?, updated_at=datetime('now','localtime') WHERE bom_id=?").run(b.product_name||'', b.brand||'', b.notes||'', b.default_order_qty||1000, b.finished_w||0, b.finished_h||0, bomId);
      if (b.items) {
        db.prepare('DELETE FROM bom_items WHERE bom_id=?').run(bomId);
        const insItem = db.prepare(`INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, unit, notes, sort_order,
          material_type, paper_standard, paper_type, gsm, finished_w, finished_h, bleed, grip, loss_rate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?)`);
        b.items.forEach((it,i) => {
          insItem.run(bomId, it.item_type||'material', it.material_code||'', it.material_name||'', it.vendor_name||'', it.process_type||'', it.qty_per||1, it.cut_spec||'', it.plate_spec||'', it.unit||'EA', it.notes||'', i,
            it.material_type||'IMPOSITION', it.paper_standard||'', it.paper_type||'', it.gsm||0, it.finished_w||0, it.finished_h||0, it.bleed??3, it.grip??10, it.loss_rate??5);
        });
      }
    });
    txn();
    ok(res, { updated: bomId });
    return;
  }

  const bomDel = pathname.match(/^\/api\/bom\/(\d+)$/);
  if (bomDel && method === 'DELETE') {
    db.prepare('DELETE FROM bom_header WHERE bom_id=?').run(parseInt(bomDel[1]));
    ok(res, { deleted: parseInt(bomDel[1]) });
    return;
  }

  // BOM import from product_info.json
  if (pathname === '/api/bom/import' && method === 'POST') {
    const piPath = path.join(__dir, 'product_info.json');
    let pi;
    try { pi = JSON.parse(fs.readFileSync(piPath, 'utf8')); } catch(e) { res.writeHead(500); res.end(JSON.stringify({error:'product_info.json not found'})); return; }
    const processes = ['재단','인쇄','박/형압','톰슨','봉투가공','세아리','레이져','실크','임가공'];
    const insH = db.prepare('INSERT OR IGNORE INTO bom_header (product_code, product_name, brand) VALUES (?,?,?)');
    const insI = db.prepare('INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, sort_order) VALUES (?,?,?,?,?,?,?,?,?,?)');
    let count = 0;
    const txn = db.transaction(() => {
      for (const [code, info] of Object.entries(pi)) {
        const r = insH.run(code, info['제품사양']||'', '');
        const bomId = r.lastInsertRowid || db.prepare('SELECT bom_id FROM bom_header WHERE product_code=?').get(code)?.bom_id;
        if (!bomId) continue;
        // skip if already has items
        const existing = db.prepare('SELECT COUNT(*) as c FROM bom_items WHERE bom_id=?').get(bomId);
        if (existing.c > 0) continue;
        let sort = 0;
        // raw material
        if (info['제지사'] || info['원자재코드']) {
          insI.run(bomId, 'material', info['원자재코드']||'', info['원재료용지명']||'', info['제지사']||'', '원재료', 1, info['절']||'', info['조판']||'', sort++);
        }
        // post-processes
        for (const proc of processes) {
          if (info[proc]) {
            insI.run(bomId, 'process', '', '', info[proc], proc, 1, '', '', sort++);
          }
        }
        count++;
      }
    });
    txn();
    ok(res, { imported: count });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  P2-10: product_info.json ↔ products DB 동기화
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/product-info/sync-status' && method === 'GET') {
    try {
      const piData = getProductInfo();
      const jsonCodes = new Set(Object.keys(piData));
      const dbRows = db.prepare("SELECT product_code FROM products").all();
      const dbCodes = new Set(dbRows.map(r => r.product_code));
      let inBoth = 0, onlyJson = 0, onlyDb = 0;
      for (const c of jsonCodes) { if (dbCodes.has(c)) inBoth++; else onlyJson++; }
      for (const c of dbCodes) { if (!jsonCodes.has(c)) onlyDb++; }
      ok(res, { inBoth, onlyJson, onlyDb, totalJson: jsonCodes.size, totalDb: dbCodes.size });
    } catch (e) {
      fail(res, 500, 'sync-status 오류: ' + e.message);
    }
    return;
  }

  if (pathname === '/api/product-info/sync' && method === 'POST') {
    try {
      const piData = getProductInfo();
      const dbRows = db.prepare("SELECT product_code FROM products").all();
      const dbCodes = new Set(dbRows.map(r => r.product_code));
      const upd = db.prepare(`UPDATE products SET material_code=?, material_name=?, cut_spec=?, jopan=?, paper_maker=?, updated_at=datetime('now','localtime') WHERE product_code=?`);
      let updated = 0, skipped = 0;
      const txn = db.transaction(() => {
        for (const [code, info] of Object.entries(piData)) {
          if (dbCodes.has(code)) {
            upd.run(
              info['원자재코드'] || '',
              info['원재료용지명'] || '',
              info['절'] || '',
              info['조판'] || '',
              info['제지사'] || '',
              code
            );
            updated++;
          } else {
            skipped++;
          }
        }
      });
      txn();
      // 캐시 무효화
      productInfoCache = null;
      ok(res, { updated, skipped });
    } catch (e) {
      fail(res, 500, 'sync 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  DD (디얼디어) 품목 동기화
  // ════════════════════════════════════════════════════════════════════

  // GET /api/dd/sync-status — DD 품목 동기화 상태 확인
  if (pathname === '/api/dd/sync-status' && method === 'GET') {
    const pool = await ensureDdPool();
    if (!pool) { fail(res, 503, 'DD 데이터베이스 미연결 (DD_DB_SERVER 설정 확인)'); return; }
    try {
      const [ddRows] = await pool.query("SELECT id, code, name, type, price, sale_price, is_display, printing_company FROM products WHERE deleted_at IS NULL");
      const dbRows = db.prepare("SELECT product_code FROM products WHERE origin = 'DD'").all();
      const ddCodes = new Set(ddRows.map(r => r.code));
      const dbCodes = new Set(dbRows.map(r => r.product_code));
      const inBoth = [...ddCodes].filter(c => dbCodes.has(c)).length;
      const onlyDD = [...ddCodes].filter(c => !dbCodes.has(c)).length;
      const onlyDB = [...dbCodes].filter(c => !ddCodes.has(c)).length;
      ok(res, { totalDD: ddRows.length, totalDB: dbRows.length, inBoth, onlyDD, onlyDB, ddProducts: ddRows });
    } catch(e) { fail(res, 500, 'DD 동기화 상태 조회 실패: ' + e.message); }
    return;
  }

  // POST /api/dd/sync — DD 품목 동기화 실행
  if (pathname === '/api/dd/sync' && method === 'POST') {
    const pool = await ensureDdPool();
    if (!pool) { fail(res, 503, 'DD 데이터베이스 미연결'); return; }
    try {
      const [ddRows] = await pool.query("SELECT id, code, name, type, price, sale_price, is_display, printing_company FROM products WHERE deleted_at IS NULL");
      const upsert = db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, category, status, memo, updated_at)
        VALUES (?, ?, 'DD', 'DD', ?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(product_code) DO UPDATE SET
          product_name = excluded.product_name,
          brand = 'DD',
          origin = 'DD',
          category = excluded.category,
          status = excluded.status,
          memo = excluded.memo,
          updated_at = datetime('now','localtime')`);
      let inserted = 0, updated = 0;
      const tx = db.transaction(() => {
        for (const r of ddRows) {
          const code = r.code || '';
          if (!code) continue;
          const existing = db.prepare("SELECT id FROM products WHERE product_code = ?").get(code);
          const status = r.is_display === 'Y' ? 'active' : 'inactive';
          const memo = `DD#${r.id} | ${r.printing_company || ''} | ${r.price}→${r.sale_price}원`;
          upsert.run(code, r.name || '', r.type || 'wcard', status, memo);
          if (existing) updated++; else inserted++;
        }
      });
      tx();
      productInfoCache = null;
      ok(res, { inserted, updated, total: ddRows.length });
    } catch(e) { fail(res, 500, 'DD 동기화 실패: ' + e.message); }
    return;
  }

  // GET /api/dd/sales — DD 오늘 판매현황
  if (pathname === '/api/dd/sales' && method === 'GET') {
    const pool = await ensureDdPool();
    if (!pool) { fail(res, 503, 'DD 데이터베이스 미연결'); return; }
    try {
      const days = parseInt(parsed.searchParams.get('days')) || 1;
      const startDate = new Date(); startDate.setDate(startDate.getDate() - days + 1);
      const startStr = startDate.toISOString().slice(0, 10);
      const [rows] = await pool.query(
        `SELECT product_code, product_name, COUNT(*) as order_count, SUM(qty) as total_qty
         FROM order_items WHERE created_at >= ? GROUP BY product_code, product_name ORDER BY total_qty DESC LIMIT 20`,
        [startStr]
      );
      ok(res, rows);
    } catch(e) { fail(res, 500, 'DD 판매 조회 실패: ' + e.message); }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  P2-11: XERP 출고 트렌드 분석
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/stats/usage-trend' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결'); return; }
    const code = parsed.searchParams.get('code') || '';
    const months = parseInt(parsed.searchParams.get('months')) || 6;
    if (!code) { fail(res, 400, 'code 파라미터 필요'); return; }
    try {
      const today = new Date();
      const start = new Date(today); start.setMonth(start.getMonth() - months);
      const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
      const result = await xerpPool.request()
        .input('code', sql.NVarChar(30), code)
        .input('startD', sql.NChar(16), fmt(start))
        .input('endD', sql.NChar(16), fmt(today))
        .query(`
          SELECT LEFT(RTRIM(InoutDate),6) AS ym, SUM(InoutQty) AS qty
          FROM mmInoutItem WITH (NOLOCK)
          WHERE SiteCode='BK10' AND InoutGubun='SO'
            AND RTRIM(ItemCode)=@code
            AND InoutDate>=@startD AND InoutDate<@endD
          GROUP BY LEFT(RTRIM(InoutDate),6)
          ORDER BY ym
        `);
      const monthsData = result.recordset.map(r => ({
        month: r.ym.slice(0,4) + '-' + r.ym.slice(4,6),
        qty: Math.round(r.qty || 0)
      }));
      ok(res, { product_code: code, months: monthsData });
    } catch (e) {
      fail(res, 500, '출고 트렌드 조회 오류: ' + e.message);
    }
    return;
  }

  if (pathname === '/api/stats/usage-trend-all' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결'); return; }
    const months = parseInt(parsed.searchParams.get('months')) || 6;
    try {
      const today = new Date();
      const start = new Date(today); start.setMonth(start.getMonth() - months);
      const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
      const result = await xerpPool.request()
        .input('startD', sql.NChar(16), fmt(start))
        .input('endD', sql.NChar(16), fmt(today))
        .query(`
          SELECT TOP 20 RTRIM(ItemCode) AS item_code, SUM(InoutQty) AS total_qty
          FROM mmInoutItem WITH (NOLOCK)
          WHERE SiteCode='BK10' AND InoutGubun='SO'
            AND InoutDate>=@startD AND InoutDate<@endD
          GROUP BY RTRIM(ItemCode)
          ORDER BY SUM(InoutQty) DESC
        `);
      const topCodes = result.recordset.map(r => (r.item_code||'').trim()).filter(Boolean);
      if (!topCodes.length) { ok(res, { months: [], products: [] }); return; }
      const safeList = topCodes.filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => `'${c}'`).join(',');
      const detailResult = await xerpPool.request()
        .input('startD2', sql.NChar(16), fmt(start))
        .input('endD2', sql.NChar(16), fmt(today))
        .query(`
          SELECT RTRIM(ItemCode) AS item_code, LEFT(RTRIM(InoutDate),6) AS ym, SUM(InoutQty) AS qty
          FROM mmInoutItem WITH (NOLOCK)
          WHERE SiteCode='BK10' AND InoutGubun='SO'
            AND InoutDate>=@startD2 AND InoutDate<@endD2
            AND RTRIM(ItemCode) IN (${safeList})
          GROUP BY RTRIM(ItemCode), LEFT(RTRIM(InoutDate),6)
          ORDER BY RTRIM(ItemCode), ym
        `);
      // 월 목록 생성
      const monthSet = new Set();
      detailResult.recordset.forEach(r => monthSet.add(r.ym));
      const monthList = [...monthSet].sort().map(ym => ym.slice(0,4)+'-'+ym.slice(4,6));
      // 품목별 데이터
      const prodMap = {};
      detailResult.recordset.forEach(r => {
        const c = (r.item_code||'').trim();
        if (!prodMap[c]) prodMap[c] = {};
        prodMap[c][r.ym.slice(0,4)+'-'+r.ym.slice(4,6)] = Math.round(r.qty||0);
      });
      // 품목명 조회
      const nameRows = db.prepare(`SELECT product_code, product_name FROM products WHERE product_code IN (${topCodes.map(()=>'?').join(',')})`).all(...topCodes);
      const nameMap = {};
      nameRows.forEach(r => nameMap[r.product_code] = r.product_name);
      const products = topCodes.map(code => ({
        code,
        name: nameMap[code] || code,
        data: monthList.map(m => (prodMap[code]||{})[m] || 0)
      }));
      ok(res, { months: monthList, products });
    } catch (e) {
      fail(res, 500, '전체 출고 트렌드 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  P2-12: 중국 단가 테이블 엑셀 재업로드 (전체 교체)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/china-price-tiers/upload' && method === 'POST') {
    try {
      const b = await readJSON(req);
      const products = b.products || [];
      if (!products.length) { fail(res, 400, 'products 배열 필요'); return; }
      let totalRows = 0, totalProducts = 0;
      const txn = db.transaction(() => {
        db.exec('DELETE FROM china_price_tiers');
        const ins = db.prepare('INSERT INTO china_price_tiers (product_code, product_type, qty_tier, unit_price, currency, effective_date) VALUES (?,?,?,?,?,?)');
        for (const p of products) {
          if (!p.product_code || !p.tiers || !p.tiers.length) continue;
          totalProducts++;
          for (const t of p.tiers) {
            ins.run(p.product_code, p.product_type || 'Card', t.qty || 0, t.price || 0, 'CNY', new Date().toISOString().slice(0,10));
            totalRows++;
          }
        }
      });
      txn();
      ok(res, { imported: totalRows, products: totalProducts });
    } catch (e) {
      fail(res, 500, '단가 업로드 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCTION PLAN API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/plans' && method === 'GET') {
    const month = parsed.searchParams.get('month') || '';
    let q = 'SELECT * FROM production_plan';
    const params = [];
    if (month) { q += ' WHERE plan_month = ?'; params.push(month); }
    q += ' ORDER BY product_code';
    const plans = db.prepare(q).all(...params);

    // 2024-2025 이력 데이터 첨부
    const mm = month ? month.split('-')[1] : '';
    let histProducts = {};
    try {
      const hd = JSON.parse(fs.readFileSync(path.join(__dir, 'product_monthly_sales.json'), 'utf8'));
      histProducts = hd.products || {};
    } catch(e) {}

    for (const p of plans) {
      const hist = histProducts[p.product_code];
      p.sales_2024 = (hist && hist['2024'] && hist['2024'][mm]) || 0;
      p.sales_2025 = (hist && hist['2025'] && hist['2025'][mm]) || 0;
    }

    ok(res, plans);
    return;
  }

  if (pathname === '/api/plans' && method === 'POST') {
    const b = await readJSON(req);
    const items = b.items || [b];
    const upsert = db.prepare('INSERT INTO production_plan (plan_month, product_code, product_name, brand, planned_qty, confirmed, notes) VALUES (?,?,?,?,?,?,?) ON CONFLICT(plan_month, product_code) DO UPDATE SET planned_qty=excluded.planned_qty, confirmed=excluded.confirmed, notes=excluded.notes, updated_at=datetime(\'now\',\'localtime\')');
    const txn = db.transaction(() => {
      for (const it of items) {
        upsert.run(it.plan_month, it.product_code, it.product_name||'', it.brand||'', it.planned_qty||0, it.confirmed||0, it.notes||'');
      }
    });
    txn();
    ok(res, { saved: items.length });
    return;
  }

  const planPut = pathname.match(/^\/api\/plans\/(\d+)$/);
  if (planPut && method === 'PUT') {
    const b = await readJSON(req);
    const sets = []; const vals = [];
    for (const k of ['planned_qty','confirmed','notes','product_name','brand']) {
      if (b[k] !== undefined) { sets.push(k+'=?'); vals.push(b[k]); }
    }
    if (sets.length) {
      sets.push("updated_at=datetime('now','localtime')");
      vals.push(parseInt(planPut[1]));
      db.prepare(`UPDATE production_plan SET ${sets.join(',')} WHERE plan_id=?`).run(...vals);
    }
    ok(res, { updated: parseInt(planPut[1]) });
    return;
  }

  // DELETE /api/plans?month=YYYY-MM — 월별 초기화
  if (pathname === '/api/plans' && method === 'DELETE') {
    const month = parsed.searchParams.get('month');
    if (month) {
      const r = db.prepare('DELETE FROM production_plan WHERE plan_month=?').run(month);
      ok(res, { deleted: r.changes, month });
    } else {
      const r = db.prepare('DELETE FROM production_plan').run();
      ok(res, { deleted: r.changes });
    }
    return;
  }

  const planDel = pathname.match(/^\/api\/plans\/(\d+)$/);
  if (planDel && method === 'DELETE') {
    db.prepare('DELETE FROM production_plan WHERE plan_id=?').run(parseInt(planDel[1]));
    ok(res, { deleted: parseInt(planDel[1]) });
    return;
  }

  // Auto-generate plan from sales data (2024-2025 가중평균 + 폴백)
  if (pathname === '/api/plans/from-sales' && method === 'POST') {
    const b = await readJSON(req);
    const month = b.plan_month;
    if (!month) { fail(res, 400, 'plan_month required'); return; }
    const mm = month.split('-')[1]; // '04' 등

    // 1) 품목별 월별 판매 이력 로드 (2024-2025)
    let histProducts = {};
    const histPath = path.join(__dir, 'product_monthly_sales.json');
    try {
      const hd = JSON.parse(fs.readFileSync(histPath, 'utf8'));
      histProducts = hd.products || {};
    } catch(e) { /* 파일 없으면 폴백 */ }
    const hasHist = Object.keys(histProducts).length > 0;

    // 2) ERP 스마트재고현황 로드 (브랜드/품명 + 폴백 수량)
    const erpPath = process.env.ERP_EXCEL_PATH || path.join(DATA_DIR, 'erp_smart_inventory.json');
    const jsonPath = erpPath.endsWith('.json') ? erpPath : path.join(__dir, 'erp_smart_inventory.json');
    let erpProducts = [];
    try {
      const d = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
      erpProducts = d.products || d.data || d;
    } catch(e) {}

    const w2024 = 0.4, w2025 = 0.6; // 연도별 가중치
    const upsert = db.prepare('INSERT INTO production_plan (plan_month, product_code, product_name, brand, planned_qty, notes) VALUES (?,?,?,?,?,?) ON CONFLICT(plan_month, product_code) DO UPDATE SET planned_qty=excluded.planned_qty, notes=excluded.notes, updated_at=datetime(\'now\',\'localtime\')');
    let count = 0;
    let methodUsed = hasHist ? 'weighted_history' : 'fallback_rolling';

    const txn = db.transaction(() => {
      for (const p of erpProducts) {
        const code = p['품목코드'];
        const brand = p['브랜드'] || '';
        if (brand.includes('D_') || brand.includes('(D')) continue;

        let planned = 0;
        let note = '';
        const hist = histProducts[code];

        if (hasHist && hist) {
          const s24 = (hist['2024'] && hist['2024'][mm]) || 0;
          const s25 = (hist['2025'] && hist['2025'][mm]) || 0;
          if (s24 > 0 && s25 > 0) {
            planned = Math.round(s24 * w2024 + s25 * w2025);
            note = `24:${s24} 25:${s25}`;
          } else if (s25 > 0) {
            planned = s25;
            note = `25:${s25}`;
          } else if (s24 > 0) {
            planned = s24;
            note = `24:${s24}`;
          }
        }

        // 이력 없으면 기존 폴백 (12개월매출/12)
        if (planned <= 0) {
          const sales12 = p['12개월매출'] || 0;
          planned = Math.round(sales12 / 12);
          if (planned > 0) note = 'fallback';
        }

        if (planned <= 0) continue;
        upsert.run(month, code, '', brand, planned, note);
        count++;
      }
    });
    txn();
    ok(res, { generated: count, month, method: methodUsed });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  MRP API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/mrp/run' && method === 'POST') {
    const b = await readJSON(req);
    const month = b.plan_month;
    if (!month) { res.writeHead(400); res.end(JSON.stringify({error:'plan_month required'})); return; }
    // Load ERP inventory for on_hand lookup
    const jsonPath = path.join(__dir, 'erp_smart_inventory.json');
    let erpMap = {};
    try {
      const d = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
      (d.products || d.data || d).forEach(p => { erpMap[p['품목코드']] = p; });
    } catch(e) {}
    const roundUnit = b.round_unit || 50;
    const useHistoryFilter = b.filter_by_history !== false; // 기본 활성화
    // 발주이력 필터: order_history 테이블에 있는 품목코드만 포함
    let histCodes = null;
    if (useHistoryFilter) {
      const hRows = db.prepare('SELECT DISTINCT product_code FROM order_history').all();
      if (hRows.length > 0) {
        histCodes = new Set(hRows.map(r => r.product_code));
      }
    }
    // Get plans for the month
    let plans = db.prepare('SELECT * FROM production_plan WHERE plan_month=? AND planned_qty>0').all(month);
    if (histCodes) {
      const before = plans.length;
      plans = plans.filter(p => histCodes.has(p.product_code));
      console.log(`MRP 발주이력 필터: ${before} → ${plans.length} (${before - plans.length}개 제외)`);
    }
    // Clear previous results for this month
    db.prepare('DELETE FROM mrp_result WHERE plan_month=?').run(month);
    const insR = db.prepare('INSERT INTO mrp_result (plan_month, product_code, material_code, material_name, vendor_name, process_type, gross_req, on_hand, on_order, net_req, order_qty, unit) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)');
    let resultCount = 0;
    const txn = db.transaction(() => {
      for (const plan of plans) {
        const bom = db.prepare('SELECT bi.* FROM bom_items bi JOIN bom_header bh ON bi.bom_id=bh.bom_id WHERE bh.product_code=? ORDER BY bi.sort_order').all(plan.product_code);
        if (!bom.length) continue;
        for (const item of bom) {
          const gross = plan.planned_qty * (item.qty_per || 1);
          // on_hand: for material, look up by material_code in ERP; for process, 0
          let onHand = 0;
          if (item.item_type === 'material' && item.material_code) {
            // find products using this material and sum their available stock
            const relatedBoms = db.prepare('SELECT bh.product_code FROM bom_header bh JOIN bom_items bi ON bh.bom_id=bi.bom_id WHERE bi.material_code=?').all(item.material_code);
            // Use the current product's ERP available stock as proxy
            const erpItem = erpMap[plan.product_code];
            onHand = erpItem ? Math.max(0, erpItem['가용재고'] || 0) : 0;
          }
          // on_order: sum of outstanding PO qty for this material/product
          let onOrder = 0;
          const lookupCode = item.material_code || plan.product_code;
          const poRows = db.prepare("SELECT SUM(pi.ordered_qty - pi.received_qty) as pending FROM po_items pi JOIN po_header ph ON pi.po_id=ph.po_id WHERE pi.product_code=? AND ph.status NOT IN ('완료','취소')").get(lookupCode);
          onOrder = poRows?.pending || 0;
          const net = Math.max(0, gross - onHand - onOrder);
          const orderQty = net > 0 ? Math.ceil(net / roundUnit) * roundUnit : 0;
          insR.run(month, plan.product_code, item.material_code||'', item.material_name||'', item.vendor_name||'', item.process_type||'', gross, onHand, onOrder, net, orderQty, item.unit||'EA');
          resultCount++;
        }
      }
    });
    txn();
    ok(res, { plan_month: month, results: resultCount, history_filter: histCodes ? histCodes.size : 0 });
    return;
  }

  if (pathname === '/api/mrp/results' && method === 'GET') {
    const month = parsed.searchParams.get('month') || '';
    let q = 'SELECT * FROM mrp_result';
    const params = [];
    if (month) { q += ' WHERE plan_month=?'; params.push(month); }
    q += ' ORDER BY vendor_name, product_code';
    ok(res, db.prepare(q).all(...params));
    return;
  }

  // DELETE /api/mrp/results?month=YYYY-MM — MRP 결과 초기화
  if (pathname === '/api/mrp/results' && method === 'DELETE') {
    const month = parsed.searchParams.get('month');
    if (month) {
      const r = db.prepare('DELETE FROM mrp_result WHERE plan_month=?').run(month);
      ok(res, { deleted: r.changes, month });
    } else {
      const r = db.prepare('DELETE FROM mrp_result').run();
      ok(res, { deleted: r.changes });
    }
    return;
  }

  // Create POs from MRP results
  if (pathname === '/api/mrp/create-po' && method === 'POST') {
    const b = await readJSON(req);
    const ids = b.result_ids || [];
    if (!ids.length) { res.writeHead(400); res.end(JSON.stringify({error:'result_ids required'})); return; }
    const results = db.prepare(`SELECT * FROM mrp_result WHERE result_id IN (${ids.map(()=>'?').join(',')}) AND order_qty > 0`).all(...ids);
    // Group by vendor + process_type
    const groups = {};
    for (const r of results) {
      const key = (r.vendor_name||'미지정') + '|' + (r.process_type === '원재료' ? '원재료' : '후공정');
      if (!groups[key]) groups[key] = [];
      groups[key].push(r);
    }
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const cnt = db.prepare("SELECT COUNT(*) as c FROM po_header WHERE po_number LIKE ?").get('PO-'+today+'%');
    let seq = (cnt?.c || 0) + 1;
    const created = [];
    const txn = db.transaction(() => {
      for (const [key, items] of Object.entries(groups)) {
        const [vendor, poType] = key.split('|');
        const poNum = `PO-${today}-${String(seq++).padStart(3,'0')}`;
        const totalQty = items.reduce((s,i) => s + i.order_qty, 0);
        // origin: 첫 번째 품목의 products.origin
        const _mrpFirstProd = db.prepare('SELECT origin FROM products WHERE product_code=?').get(items[0].product_code || '');
        const _mrpOrigin = (_mrpFirstProd && _mrpFirstProd.origin) || '';
        const r = db.prepare('INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, origin, po_date) VALUES (?,?,?,?,?,?,?,date(\'now\',\'localtime\'))').run(poNum, poType, vendor, '대기', totalQty, 'MRP 자동생성', _mrpOrigin);
        const poId = r.lastInsertRowid;
        for (const item of items) {
          db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec) VALUES (?,?,?,?,?,?)').run(poId, item.product_code, '', item.process_type, item.order_qty, item.material_name);
          db.prepare('UPDATE mrp_result SET status=? WHERE result_id=?').run('ordered', item.result_id);
        }
        created.push({ po_number: poNum, vendor, po_type: poType, items: items.length });
      }
    });
    txn();
    ok(res, { created });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  발주이력 (Order History) API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/order-history — 전체 목록 (요약 or 전체)
  if (pathname === '/api/order-history' && method === 'GET') {
    const mode = parsed.searchParams.get('mode') || 'full';
    if (mode === 'codes') {
      // 고유 품목코드 목록만
      const rows = db.prepare('SELECT DISTINCT product_code FROM order_history ORDER BY product_code').all();
      ok(res, rows.map(r => r.product_code));
    } else if (mode === 'today') {
      // 오늘 발주된 품목코드 + 수량
      const today = new Date().toISOString().slice(0,10).replace(/-/g,'-');
      const rows = db.prepare('SELECT product_code, SUM(order_qty) as total_qty FROM order_history WHERE order_date = ? GROUP BY product_code').all(today);
      const map = {};
      rows.forEach(r => { map[r.product_code] = r.total_qty; });
      ok(res, map);
    } else {
      const rows = db.prepare('SELECT * FROM order_history ORDER BY order_date DESC, history_id DESC LIMIT 5000').all();
      ok(res, rows);
    }
    return;
  }

  // GET /api/order-history/stats — 통계
  if (pathname === '/api/order-history/stats' && method === 'GET') {
    const total = db.prepare('SELECT COUNT(*) as cnt FROM order_history').get().cnt;
    const codes = db.prepare('SELECT COUNT(DISTINCT product_code) as cnt FROM order_history').get().cnt;
    const sheets = db.prepare("SELECT DISTINCT source_sheet FROM order_history WHERE source_sheet != ''").all().map(r => r.source_sheet);
    ok(res, { total_rows: total, unique_codes: codes, sheets });
    return;
  }

  // POST /api/order-history/import — 붙여넣기/대량 import
  if (pathname === '/api/order-history/import' && method === 'POST') {
    const b = await readJSON(req);
    const rows = b.rows || [];
    const sourceSheet = b.source_sheet || '';
    const clearExisting = b.clear_existing || false;

    if (!rows.length) { fail(res, 400, 'rows required'); return; }

    const ins = db.prepare(`INSERT INTO order_history
      (order_date, os_no, warehouse_order, product_name, product_code, actual_qty,
       material_code, material_name, paper_maker, vendor_code, qty, cut_spec,
       plate_spec, cutting, printing, foil_emboss, thomson, envelope_proc,
       seari, laser, silk, outsource, order_qty, product_spec, source_sheet)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);

    const txn = db.transaction(() => {
      if (clearExisting) {
        db.prepare('DELETE FROM order_history').run();
      }
      let count = 0;
      for (const r of rows) {
        const code = (r.product_code || r[5] || '').toString().trim();
        if (!code) continue;
        ins.run(
          r.order_date || r[0] || '',
          r.os_no || r[2] || '',
          r.warehouse_order || r[3] || '',
          r.product_name || r[4] || '',
          code,
          parseInt(r.actual_qty || r[6] || 0) || 0,
          r.material_code || r[7] || '',
          r.material_name || r[8] || '',
          r.paper_maker || r[9] || '',
          r.vendor_code || r[10] || '',
          parseFloat(r.qty || r[11] || 0) || 0,
          r.cut_spec || r[12] || '',
          r.plate_spec || r[13] || '',
          r.cutting || r[14] || '',
          r.printing || r[15] || '',
          r.foil_emboss || r[16] || '',
          r.thomson || r[17] || '',
          r.envelope_proc || r[18] || '',
          r.seari || r[19] || '',
          r.laser || r[20] || '',
          r.silk || r[21] || '',
          r.outsource || r[22] || '',
          parseInt(r.order_qty || r[23] || 0) || 0,
          r.product_spec || r[24] || '',
          sourceSheet
        );
        count++;
      }
      return count;
    });
    const imported = txn();

    // Google Sheet 동기화 (비동기, DB 저장 후 실행)
    let sheetResult = null;
    if (b.sync_to_sheet !== false) {
      const sheetRows = rows.filter(r => (r.product_code || r[5] || '').toString().trim());
      sheetResult = await appendToGoogleSheet(sheetRows.map(r => ({
        order_date: r.order_date || r[0] || '',
        os_no: r.os_no || r[2] || '',
        warehouse_order: r.warehouse_order || r[3] || '',
        product_name: r.product_name || r[4] || '',
        product_code: (r.product_code || r[5] || '').toString().trim(),
        actual_qty: parseInt(r.actual_qty || r[6] || 0) || 0,
        material_code: r.material_code || r[7] || '',
        material_name: r.material_name || r[8] || '',
        paper_maker: r.paper_maker || r[9] || '',
        vendor_code: r.vendor_code || r[10] || '',
        qty: parseFloat(r.qty || r[11] || 0) || 0,
        cut_spec: r.cut_spec || r[12] || '',
        plate_spec: r.plate_spec || r[13] || '',
        cutting: r.cutting || r[14] || '',
        printing: r.printing || r[15] || '',
        foil_emboss: r.foil_emboss || r[16] || '',
        thomson: r.thomson || r[17] || '',
        envelope_proc: r.envelope_proc || r[18] || '',
        seari: r.seari || r[19] || '',
        laser: r.laser || r[20] || '',
        silk: r.silk || r[21] || '',
        outsource: r.outsource || r[22] || '',
        order_qty: parseInt(r.order_qty || r[23] || 0) || 0,
        product_spec: r.product_spec || r[24] || ''
      })));
    }

    ok(res, { imported, source_sheet: sourceSheet, google_sheet: sheetResult });
    return;
  }

  // DELETE /api/order-history — 전체 삭제 또는 소스별 삭제
  if (pathname === '/api/order-history' && method === 'DELETE') {
    const b = await readJSON(req).catch(() => ({}));
    if (b.source_sheets && Array.isArray(b.source_sheets) && b.source_sheets.length) {
      const ph = b.source_sheets.map(() => '?').join(',');
      const r = db.prepare(`DELETE FROM order_history WHERE source_sheet IN (${ph})`).run(...b.source_sheets);
      ok(res, { deleted: r.changes, source_sheets: b.source_sheets });
    } else {
      db.prepare('DELETE FROM order_history').run();
      ok(res, { deleted: true });
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCT NOTES (품목 특이사항)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/product-notes' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM product_notes').all();
    const map = {};
    rows.forEach(r => { map[r.product_code] = { note_type: r.note_type, note_text: r.note_text }; });
    ok(res, map);
    return;
  }

  const pnMatch = pathname.match(/^\/api\/product-notes\/(.+)$/);
  if (pnMatch && method === 'PUT') {
    const code = decodeURIComponent(pnMatch[1]);
    const b = await readJSON(req);
    const noteType = b.note_type || '';
    const noteText = b.note_text || '';
    if (!noteType && !noteText) {
      db.prepare('DELETE FROM product_notes WHERE product_code=?').run(code);
    } else {
      db.prepare('INSERT INTO product_notes (product_code, note_type, note_text, updated_at) VALUES (?,?,?,datetime(\'now\',\'localtime\')) ON CONFLICT(product_code) DO UPDATE SET note_type=excluded.note_type, note_text=excluded.note_text, updated_at=excluded.updated_at').run(code, noteType, noteText);
    }
    ok(res, { saved: true, product_code: code });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  DEFECT / QUALITY MANAGEMENT API (불량 관리)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/defects/summary — 불량 현황 요약
  if (pathname === '/api/defects/summary' && method === 'GET') {
    const byStatus = db.prepare(`
      SELECT status, COUNT(*) as count FROM defects GROUP BY status
    `).all();
    const byVendor = db.prepare(`
      SELECT vendor_name, COUNT(*) as defect_count, SUM(defect_qty) as total_defect_qty
      FROM defects GROUP BY vendor_name ORDER BY defect_count DESC
    `).all();
    const byType = db.prepare(`
      SELECT defect_type, COUNT(*) as count FROM defects WHERE defect_type != '' GROUP BY defect_type ORDER BY count DESC
    `).all();
    const since30 = new Date();
    since30.setDate(since30.getDate() - 30);
    const since30str = since30.toISOString().slice(0, 10);
    const recent30 = db.prepare(`
      SELECT COUNT(*) as total, SUM(defect_qty) as total_qty,
             SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) as resolved,
             SUM(CASE WHEN status='registered' THEN 1 ELSE 0 END) as registered,
             SUM(CASE WHEN status='in_progress' THEN 1 ELSE 0 END) as in_progress
      FROM defects WHERE defect_date >= ?
    `).get(since30str);
    ok(res, { byStatus, byVendor, byType, recent30days: recent30 });
    return;
  }

  // GET /api/defects — 불량 목록 조회 (필터링)
  if (pathname === '/api/defects' && method === 'GET') {
    const sp = parsed.searchParams;
    let q = 'SELECT * FROM defects WHERE 1=1';
    const args = [];
    if (sp.get('status'))       { q += ' AND status=?';       args.push(sp.get('status')); }
    if (sp.get('vendor_name'))  { q += ' AND vendor_name=?';  args.push(sp.get('vendor_name')); }
    if (sp.get('product_code')) { q += ' AND product_code=?'; args.push(sp.get('product_code')); }
    if (sp.get('from_date'))    { q += ' AND defect_date>=?'; args.push(sp.get('from_date')); }
    if (sp.get('to_date'))      { q += ' AND defect_date<=?'; args.push(sp.get('to_date')); }
    q += ' ORDER BY defect_date DESC, created_at DESC LIMIT 200';
    ok(res, db.prepare(q).all(...args));
    return;
  }

  // POST /api/defects — 불량 접수
  if (pathname === '/api/defects' && method === 'POST') {
    const body = await readJSON(req);
    const { vendor_name, product_code, defect_date, description } = body;
    if (!vendor_name || !product_code || !defect_date || !description) {
      fail(res, 400, '필수 항목 누락: vendor_name, product_code, defect_date, description');
      return;
    }
    // Auto-generate defect_number: DF + YYMMDD + "-" + 3-digit seq
    const today = new Date();
    const ymd = String(today.getFullYear()).slice(2)
      + String(today.getMonth() + 1).padStart(2, '0')
      + String(today.getDate()).padStart(2, '0');
    const prefix = `DF${ymd}-`;
    const lastRow = db.prepare(
      `SELECT defect_number FROM defects WHERE defect_number LIKE ? ORDER BY defect_number DESC LIMIT 1`
    ).get(prefix + '%');
    let seq = 1;
    if (lastRow) {
      const lastSeq = parseInt(lastRow.defect_number.split('-')[1], 10);
      if (!isNaN(lastSeq)) seq = lastSeq + 1;
    }
    const defect_number = prefix + String(seq).padStart(3, '0');

    const insertDefect = db.prepare(`INSERT INTO defects
      (defect_number, po_id, po_number, vendor_name, product_code, product_name,
       defect_date, defect_type, defect_qty, order_qty, severity, description,
       photo_url, claim_type, claim_amount, status)
      VALUES (@defect_number, @po_id, @po_number, @vendor_name, @product_code, @product_name,
              @defect_date, @defect_type, @defect_qty, @order_qty, @severity, @description,
              @photo_url, @claim_type, @claim_amount, 'registered')`);
    const insertLog = db.prepare(`INSERT INTO defect_logs
      (defect_id, defect_number, action, from_status, to_status, actor, details)
      VALUES (@defect_id, @defect_number, @action, @from_status, @to_status, @actor, @details)`);

    const tx = db.transaction(() => {
      const info = insertDefect.run({
        defect_number,
        po_id: body.po_id || null,
        po_number: body.po_number || '',
        vendor_name,
        product_code,
        product_name: body.product_name || '',
        defect_date,
        defect_type: body.defect_type || '',
        defect_qty: body.defect_qty || 0,
        order_qty: body.order_qty || 0,
        severity: body.severity || 'minor',
        description,
        photo_url: body.photo_url || '',
        claim_type: body.claim_type || '',
        claim_amount: body.claim_amount || 0,
      });
      insertLog.run({
        defect_id: info.lastInsertRowid,
        defect_number,
        action: 'registered',
        from_status: '',
        to_status: 'registered',
        actor: body.actor || '',
        details: '불량 접수',
      });
      return info.lastInsertRowid;
    });
    const newId = tx();
    ok(res, { id: newId, defect_number });
    return;
  }

  // GET /api/defects/:id — 불량 상세 + 이력
  const defectIdMatch = pathname.match(/^\/api\/defects\/(\d+)$/);
  if (defectIdMatch && method === 'GET') {
    const defectId = parseInt(defectIdMatch[1]);
    const defect = db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    const logs = db.prepare('SELECT * FROM defect_logs WHERE defect_id=? ORDER BY created_at ASC').all(defectId);
    ok(res, { ...defect, logs });
    return;
  }

  // PUT /api/defects/:id — 불량 수정 (상태 변경 포함)
  const defectPutMatch = pathname.match(/^\/api\/defects\/(\d+)$/);
  if (defectPutMatch && method === 'PUT') {
    const defectId = parseInt(defectPutMatch[1]);
    const defect = db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    const body = await readJSON(req);

    const sets = [];
    const vals = [];
    const allowedFields = ['po_id','po_number','vendor_name','product_code','product_name',
      'defect_date','defect_type','defect_qty','order_qty','severity','description',
      'photo_url','claim_type','claim_amount','resolution','resolved_date','resolved_by','status'];
    for (const f of allowedFields) {
      if (body[f] !== undefined) { sets.push(`${f}=?`); vals.push(body[f]); }
    }
    if (sets.length === 0) { fail(res, 400, '수정 항목 없음'); return; }
    sets.push("updated_at=datetime('now','localtime')");
    vals.push(defectId);

    const statusChanged = body.status !== undefined && body.status !== defect.status;
    const insertLog = db.prepare(`INSERT INTO defect_logs
      (defect_id, defect_number, action, from_status, to_status, actor, details)
      VALUES (?,?,?,?,?,?,?)`);

    const tx = db.transaction(() => {
      // Auto-set resolved_date when resolving
      if (body.status === 'resolved' && !body.resolved_date && !defect.resolved_date) {
        const today = new Date().toISOString().slice(0, 10);
        sets.splice(sets.length - 1, 0, 'resolved_date=?');
        vals.splice(vals.length - 1, 0, today);
      }
      db.prepare(`UPDATE defects SET ${sets.join(',')} WHERE id=?`).run(...vals);
      if (statusChanged) {
        const actionLabel = body.status === 'in_progress' ? '처리 시작' :
                            body.status === 'resolved'    ? '처리 완료' : '상태 변경';
        insertLog.run(
          defectId, defect.defect_number,
          actionLabel,
          defect.status, body.status,
          body.actor || '',
          body.resolution || body.details || ''
        );
      }
    });
    tx();
    ok(res, { id: defectId, status: body.status || defect.status });
    return;
  }

  // POST /api/defects/:id/log — 불량 수동 메모/이력 추가
  const defectLogMatch = pathname.match(/^\/api\/defects\/(\d+)\/log$/);
  if (defectLogMatch && method === 'POST') {
    const defectId = parseInt(defectLogMatch[1]);
    const defect = db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    const body = await readJSON(req);
    if (!body.action) { fail(res, 400, 'action 필수'); return; }
    db.prepare(`INSERT INTO defect_logs (defect_id, defect_number, action, from_status, to_status, actor, details)
      VALUES (?,?,?,?,?,?,?)`).run(
      defectId, defect.defect_number,
      body.action,
      body.from_status || defect.status,
      body.to_status || defect.status,
      body.actor || '',
      body.details || ''
    );
    ok(res, { ok: true });
    return;
  }

  // POST /api/defects/:id/create-po — 불량 처리 발주 생성
  const defectCreatePoMatch = pathname.match(/^\/api\/defects\/(\d+)\/create-po$/);
  if (defectCreatePoMatch && method === 'POST') {
    const defectId = parseInt(defectCreatePoMatch[1]);
    const defect = db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    if (!defect.product_code) { fail(res, 400, 'product_code가 없어 발주를 생성할 수 없습니다'); return; }

    const poNumber = generatePoNumber();
    const defectQty = defect.defect_qty || 0;
    const notes = `불량처리 발주 (${defect.defect_number}) - ${defect.description || '불량 재작업'}`;

    // origin 결정
    const _defOriginProd = db.prepare('SELECT origin FROM products WHERE product_code=?').get(defect.product_code);
    const _defOrigin = (_defOriginProd && _defOriginProd.origin) || '';

    const tx = db.transaction(() => {
      // PO 헤더 생성
      const hdrInfo = db.prepare(`
        INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, defect_id, defect_number, origin, po_date)
        VALUES (?, 'post_process', ?, '대기', ?, ?, ?, ?, ?, date('now','localtime'))
      `).run(poNumber, defect.vendor_name || '', defectQty, notes, defectId, defect.defect_number || '', _defOrigin);

      const poId = hdrInfo.lastInsertRowid;

      // PO 품목 추가
      db.prepare(`
        INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes)
        VALUES (?, ?, '', ?, ?, '', ?)
      `).run(
        poId,
        defect.product_code,
        defect.claim_type || defect.defect_type || '',
        defectQty,
        defect.description || ''
      );

      // 불량 이력 로그 추가
      db.prepare(`
        INSERT INTO defect_logs (defect_id, defect_number, action, from_status, to_status, actor, details)
        VALUES (?, ?, '처리 발주 생성', ?, ?, 'system', ?)
      `).run(
        defectId,
        defect.defect_number || '',
        defect.status,
        defect.status,
        `처리 발주 생성: ${poNumber}`
      );

      return poId;
    });

    const poId = tx();
    ok(res, { po_id: poId, po_number: poNumber, defect_id: defectId, defect_number: defect.defect_number });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  INCOMING INSPECTION API (수입검사)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/inspections
  if (pathname === '/api/inspections' && method === 'GET') {
    const rows = db.prepare('SELECT * FROM incoming_inspections ORDER BY created_at DESC LIMIT 200').all();
    ok(res, rows);
    return;
  }

  // POST /api/inspections
  if (pathname === '/api/inspections' && method === 'POST') {
    const body = await readJSON(req);
    const passRate = body.total_qty > 0 ? Math.round(body.pass_qty / body.total_qty * 1000) / 10 : 0;
    const result = body.fail_qty > 0 ? (passRate < 90 ? 'rejected' : 'conditional') : 'passed';
    const info = db.prepare(`INSERT INTO incoming_inspections (po_id, po_number, vendor_name, inspection_date, inspector, result, items_json, total_qty, pass_qty, fail_qty, pass_rate, notes)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      body.po_id || null, body.po_number || '', body.vendor_name || '',
      body.inspection_date || new Date().toISOString().slice(0, 10),
      body.inspector || '', result, JSON.stringify(body.items || []),
      body.total_qty || 0, body.pass_qty || 0, body.fail_qty || 0, passRate, body.notes || ''
    );
    // 불합격 시 자동 NCR 생성
    if (result === 'rejected' || result === 'conditional') {
      const ncrNum = 'NCR' + new Date().toISOString().slice(2, 10).replace(/-/g, '') + '-' + String(info.lastInsertRowid).padStart(3, '0');
      db.prepare(`INSERT INTO ncr (ncr_number, inspection_id, po_id, vendor_name, product_code, ncr_type, description, status, severity)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(
        ncrNum, info.lastInsertRowid, body.po_id || null, body.vendor_name || '',
        body.product_code || '', 'incoming',
        `수입검사 ${result === 'rejected' ? '불합격' : '조건부합격'}: 불량 ${body.fail_qty}건 / 전체 ${body.total_qty}건 (합격률 ${passRate}%)`,
        'open', result === 'rejected' ? 'critical' : 'minor'
      );
    }
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'inspection_create', 'inspections', info.lastInsertRowid, `수입검사: ${body.po_number || ''} → ${result}`, clientIP);
    ok(res, { inspection_id: info.lastInsertRowid, result, pass_rate: passRate });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NCR API (부적합 처리)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/ncr
  if (pathname === '/api/ncr' && method === 'GET') {
    const status = parsed.searchParams.get('status');
    let sql = 'SELECT * FROM ncr';
    const params = [];
    if (status) { sql += ' WHERE status = ?'; params.push(status); }
    sql += ' ORDER BY created_at DESC LIMIT 200';
    ok(res, db.prepare(sql).all(...params));
    return;
  }

  // POST /api/ncr
  if (pathname === '/api/ncr' && method === 'POST') {
    const body = await readJSON(req);
    const ncrNum = 'NCR' + new Date().toISOString().slice(2, 10).replace(/-/g, '') + '-' + String(Date.now()).slice(-4);
    const info = db.prepare(`INSERT INTO ncr (ncr_number, defect_id, inspection_id, po_id, vendor_name, product_code, ncr_type, description, severity, responsible, due_date)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(
      ncrNum, body.defect_id || null, body.inspection_id || null, body.po_id || null,
      body.vendor_name || '', body.product_code || '', body.ncr_type || 'process',
      body.description || '', body.severity || 'minor', body.responsible || '', body.due_date || ''
    );
    db.prepare("INSERT INTO ncr_logs (ncr_id, action, to_status, actor, details) VALUES (?,'created','open',?,?)").run(info.lastInsertRowid, body.actor || '', 'NCR 생성');
    ok(res, { ncr_id: info.lastInsertRowid, ncr_number: ncrNum });
    return;
  }

  // PUT /api/ncr/:id — NCR 상태 변경 (open→analysis→action→closed)
  const ncrPut = pathname.match(/^\/api\/ncr\/(\d+)$/);
  if (ncrPut && method === 'PUT') {
    const ncrId = parseInt(ncrPut[1]);
    const ncr = db.prepare('SELECT * FROM ncr WHERE ncr_id=?').get(ncrId);
    if (!ncr) { fail(res, 404, 'NCR not found'); return; }
    const body = await readJSON(req);
    const sets = [], vals = [];
    ['status','root_cause','corrective_action','preventive_action','responsible','due_date','severity','description'].forEach(f => {
      if (body[f] !== undefined) { sets.push(`${f}=?`); vals.push(body[f]); }
    });
    if (body.status === 'closed' && !ncr.closed_at) { sets.push("closed_at=datetime('now','localtime')"); }
    sets.push("updated_at=datetime('now','localtime')");
    vals.push(ncrId);
    db.prepare(`UPDATE ncr SET ${sets.join(',')} WHERE ncr_id=?`).run(...vals);
    if (body.status && body.status !== ncr.status) {
      const labels = { analysis: '원인분석 중', action: '시정조치 중', closed: '종결' };
      db.prepare("INSERT INTO ncr_logs (ncr_id, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?)").run(
        ncrId, labels[body.status] || '상태변경', ncr.status, body.status, body.actor || '', body.details || ''
      );
    }
    ok(res, { ncr_id: ncrId, status: body.status || ncr.status });
    return;
  }

  // GET /api/ncr/:id
  const ncrGet = pathname.match(/^\/api\/ncr\/(\d+)$/);
  if (ncrGet && method === 'GET') {
    const ncr = db.prepare('SELECT * FROM ncr WHERE ncr_id=?').get(parseInt(ncrGet[1]));
    if (!ncr) { fail(res, 404, 'NCR not found'); return; }
    ncr.logs = db.prepare('SELECT * FROM ncr_logs WHERE ncr_id=? ORDER BY created_at ASC').all(ncr.ncr_id);
    ok(res, ncr);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  VENDOR SCORECARD API (협력사 평가)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/vendor-scorecard?vendor=xxx
  if (pathname === '/api/vendor-scorecard' && method === 'GET') {
    const vendor = parsed.searchParams.get('vendor');
    if (vendor) {
      ok(res, db.prepare('SELECT * FROM vendor_scorecard WHERE vendor_name=? ORDER BY eval_month DESC').all(vendor));
    } else {
      // 최신 월 기준 전체 업체 스코어카드
      const latest = db.prepare('SELECT MAX(eval_month) as m FROM vendor_scorecard').get();
      if (latest && latest.m) {
        ok(res, db.prepare('SELECT * FROM vendor_scorecard WHERE eval_month=? ORDER BY total_score DESC').all(latest.m));
      } else {
        ok(res, []);
      }
    }
    return;
  }

  // POST /api/vendor-scorecard/calculate — 협력사 평가 자동 계산
  if (pathname === '/api/vendor-scorecard/calculate' && method === 'POST') {
    const body = await readJSON(req);
    const month = body.month || new Date().toISOString().slice(0, 7);
    const monthLike = month + '%';
    const vendors = db.prepare('SELECT DISTINCT vendor_name FROM po_header WHERE po_date LIKE ? AND vendor_name IS NOT NULL').all(monthLike);
    const results = [];
    for (const { vendor_name } of vendors) {
      if (!vendor_name) continue;
      // 납기 준수율
      const totalPO = db.prepare("SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date LIKE ? AND status != 'cancelled'").get(vendor_name, monthLike).cnt;
      const ontimePO = db.prepare("SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date LIKE ? AND status IN ('received','os_pending') AND (expected_date IS NULL OR updated_at <= expected_date || ' 23:59:59')").get(vendor_name, monthLike).cnt;
      const deliveryScore = totalPO > 0 ? Math.round(ontimePO / totalPO * 100) : 100;
      // 품질 점수
      const defectCount = db.prepare("SELECT COUNT(*) as cnt FROM defects WHERE vendor_name=? AND defect_date LIKE ?").get(vendor_name, monthLike).cnt;
      const qualityScore = Math.max(0, 100 - defectCount * 10);
      // 종합
      const totalScore = Math.round(deliveryScore * 0.5 + qualityScore * 0.4 + 80 * 0.1); // 가격은 기본 80점
      db.prepare(`INSERT OR REPLACE INTO vendor_scorecard (vendor_name, eval_month, delivery_score, quality_score, price_score, total_score, total_po, ontime_po, total_defects)
        VALUES (?,?,?,?,80,?,?,?,?)`).run(vendor_name, month, deliveryScore, qualityScore, totalScore, totalPO, ontimePO, defectCount);
      results.push({ vendor_name, delivery_score: deliveryScore, quality_score: qualityScore, total_score: totalScore });
    }
    ok(res, { month, calculated: results.length, results });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCTION REQUEST API (생산요청)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/production-requests — 목록 + 요약
  if (pathname === '/api/production-requests' && method === 'GET') {
    const status = parsed.searchParams.get('status') || '';
    const type = parsed.searchParams.get('type') || '';
    let where = '1=1';
    const vals = [];
    if (status) { where += ' AND status=?'; vals.push(status); }
    if (type) { where += ' AND product_type=?'; vals.push(type); }
    const rows = db.prepare(`SELECT * FROM production_requests WHERE ${where} ORDER BY created_at DESC`).all(...vals);
    const byStatus = db.prepare(`SELECT status, COUNT(*) as count FROM production_requests GROUP BY status`).all();
    ok(res, { list: rows, byStatus });
    return;
  }

  // POST /api/production-requests — 새 요청 생성
  if (pathname === '/api/production-requests' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.product_type || !body.product_name) { fail(res, 400, 'product_type, product_name 필수'); return; }
    const num = 'PR' + new Date().toISOString().slice(2,10).replace(/-/g,'') + '-' + String(Math.floor(Math.random()*10000)).padStart(4,'0');
    const info = db.prepare(`INSERT INTO production_requests
      (request_number, product_type, product_name, brand, requested_qty, spec_json, requester, designer, printer_vendor, post_vendor, priority, due_date, notes)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      num, body.product_type, body.product_name, body.brand || '',
      body.requested_qty || 0, body.spec_json || '{}',
      body.requester || '', body.designer || '',
      body.printer_vendor || '', body.post_vendor || '',
      body.priority || 'normal', body.due_date || '', body.notes || ''
    );
    db.prepare(`INSERT INTO production_request_logs (request_id, request_number, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?,?)`)
      .run(info.lastInsertRowid, num, '생산요청 등록', '', 'requested', body.requester || 'system', `${body.product_name} ${body.requested_qty || 0}부`);
    ok(res, { id: info.lastInsertRowid, request_number: num });
    return;
  }

  // GET /api/production-requests/:id — 상세
  const prDetailMatch = pathname.match(/^\/api\/production-requests\/(\d+)$/);
  if (prDetailMatch && method === 'GET') {
    const prId = parseInt(prDetailMatch[1]);
    const pr = db.prepare('SELECT * FROM production_requests WHERE id=?').get(prId);
    if (!pr) { fail(res, 404, '요청 없음'); return; }
    const logs = db.prepare('SELECT * FROM production_request_logs WHERE request_id=? ORDER BY created_at ASC').all(prId);
    ok(res, { ...pr, logs });
    return;
  }

  // PUT /api/production-requests/:id — 수정/상태변경
  if (prDetailMatch && method === 'PUT') {
    const prId = parseInt(prDetailMatch[1]);
    const pr = db.prepare('SELECT * FROM production_requests WHERE id=?').get(prId);
    if (!pr) { fail(res, 404, '요청 없음'); return; }
    const body = await readJSON(req);

    const sets = [];
    const vals = [];
    const allowed = ['product_type','product_name','brand','requested_qty','spec_json',
      'requester','designer','printer_vendor','post_vendor','status','priority','due_date','notes'];
    for (const f of allowed) {
      if (body[f] !== undefined) { sets.push(`${f}=?`); vals.push(body[f]); }
    }

    // 상태 변경 시 타임스탬프 자동 기록
    const statusChanged = body.status !== undefined && body.status !== pr.status;
    if (statusChanged) {
      const now = new Date().toISOString().slice(0,19).replace('T',' ');
      if (body.status === 'design_confirmed') { sets.push('design_confirmed_at=?'); vals.push(now); }
      if (body.status === 'data_confirmed') { sets.push('data_confirmed_at=?'); vals.push(now); }
      if (body.status === 'in_production') { sets.push('production_started_at=?'); vals.push(now); }
      if (body.status === 'completed') { sets.push('completed_at=?'); vals.push(now); }
    }

    if (sets.length === 0) { fail(res, 400, '수정 항목 없음'); return; }
    sets.push("updated_at=datetime('now','localtime')");
    vals.push(prId);

    db.prepare(`UPDATE production_requests SET ${sets.join(',')} WHERE id=?`).run(...vals);

    if (statusChanged) {
      const statusNames = { requested:'요청등록', design_confirmed:'디자인확인', data_confirmed:'데이터확인', in_production:'생산진행', completed:'완료', cancelled:'취소' };
      db.prepare(`INSERT INTO production_request_logs (request_id, request_number, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?,?)`)
        .run(prId, pr.request_number, `상태변경: ${statusNames[body.status] || body.status}`,
          pr.status, body.status, body.actor || 'system', body.log_details || '');
    }

    ok(res, { updated: true });
    return;
  }

  // POST /api/production-requests/:id/log — 로그 추가
  const prLogMatch = pathname.match(/^\/api\/production-requests\/(\d+)\/log$/);
  if (prLogMatch && method === 'POST') {
    const prId = parseInt(prLogMatch[1]);
    const pr = db.prepare('SELECT * FROM production_requests WHERE id=?').get(prId);
    if (!pr) { fail(res, 404, '요청 없음'); return; }
    const body = await readJSON(req);
    db.prepare(`INSERT INTO production_request_logs (request_id, request_number, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?,?)`)
      .run(prId, pr.request_number, body.action || '메모', pr.status, pr.status, body.actor || '', body.details || '');
    ok(res, { added: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCT SPEC MASTER API (제품 스펙)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/specs — 목록
  if (pathname === '/api/specs' && method === 'GET') {
    const type = parsed.searchParams.get('type') || '';
    const templateOnly = parsed.searchParams.get('template') === '1';
    let where = '1=1';
    const vals = [];
    if (type) { where += ' AND product_type=?'; vals.push(type); }
    if (templateOnly) { where += ' AND is_template=1'; }
    const rows = db.prepare(`SELECT * FROM product_spec_master WHERE ${where} ORDER BY product_type, spec_name`).all(...vals);
    ok(res, rows);
    return;
  }

  // POST /api/specs — 등록
  if (pathname === '/api/specs' && method === 'POST') {
    const b = await readJSON(req);
    if (!b.spec_name) { fail(res, 400, 'spec_name 필수'); return; }
    const info = db.prepare(`INSERT INTO product_spec_master
      (product_type, spec_name, brand, paper_cover, paper_inner, print_method, print_color, binding, post_process, size, pages, weight, extras, notes, is_template)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      b.product_type||'', b.spec_name, b.brand||'',
      b.paper_cover||'', b.paper_inner||'', b.print_method||'', b.print_color||'',
      b.binding||'', b.post_process||'', b.size||'', b.pages||0, b.weight||'',
      b.extras||'', b.notes||'', b.is_template ? 1 : 0
    );
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  // PUT /api/specs/:id — 수정
  const specPutMatch = pathname.match(/^\/api\/specs\/(\d+)$/);
  if (specPutMatch && method === 'PUT') {
    const specId = parseInt(specPutMatch[1]);
    const b = await readJSON(req);
    const sets = [];
    const vals = [];
    const allowed = ['product_type','spec_name','brand','paper_cover','paper_inner','print_method','print_color','binding','post_process','size','pages','weight','extras','notes','is_template'];
    for (const f of allowed) { if (b[f] !== undefined) { sets.push(`${f}=?`); vals.push(b[f]); } }
    if (!sets.length) { fail(res, 400, '수정 항목 없음'); return; }
    sets.push("updated_at=datetime('now','localtime')");
    vals.push(specId);
    db.prepare(`UPDATE product_spec_master SET ${sets.join(',')} WHERE id=?`).run(...vals);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/specs/:id — 삭제
  if (specPutMatch && method === 'DELETE') {
    const specId = parseInt(specPutMatch[1]);
    db.prepare('DELETE FROM product_spec_master WHERE id=?').run(specId);
    ok(res, { deleted: true });
    return;
  }

  // ── 업무관리 API ───────────────────────────────────────────────────
  const taskMatch = pathname.match(/^\/api\/tasks\/(\d+)$/);
  const taskCommentMatch = pathname.match(/^\/api\/tasks\/(\d+)\/comments$/);

  // ═══════════════════════════════════════════════════════════════
  // 부속품 API
  // ═══════════════════════════════════════════════════════════════

  // GET /api/accessories — 전체 부속품 목록
  if (pathname === '/api/accessories' && method === 'GET') {
    const q = parsed.searchParams.get('q');
    const type = parsed.searchParams.get('type');
    let sql = `SELECT a.*, (SELECT COUNT(*) FROM product_accessories pa WHERE pa.acc_id=a.id) AS product_count FROM accessories a WHERE 1=1`;
    const params = [];
    if (q) { sql += ` AND (a.acc_name LIKE ? OR a.acc_code LIKE ?)`; params.push('%'+q+'%','%'+q+'%'); }
    if (type) { sql += ` AND a.acc_type=?`; params.push(type); }
    sql += ` ORDER BY a.acc_type, a.acc_name`;
    ok(res, db.prepare(sql).all(...params));
    return;
  }

  // POST /api/accessories — 부속품 추가
  if (pathname === '/api/accessories' && method === 'POST') {
    const b = await readJSON(req);
    const info = db.prepare(`INSERT INTO accessories (acc_code,acc_name,acc_type,current_stock,min_stock,unit,vendor,memo,origin) VALUES (?,?,?,?,?,?,?,?,?)`).run(
      b.acc_code||'', b.acc_name||'', b.acc_type||'기타', b.current_stock||0, b.min_stock||0, b.unit||'개', b.vendor||'', b.memo||'', b.origin||'한국'
    );
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  // PUT /api/accessories/:id — 부속품 수정
  const accPut = pathname.match(/^\/api\/accessories\/(\d+)$/);
  if (accPut && method === 'PUT') {
    const b = await readJSON(req);
    const id = accPut[1];
    const fields = [], params = { id };
    for (const col of ['acc_code','acc_name','acc_type','current_stock','min_stock','unit','vendor','memo','origin']) {
      if (b[col] !== undefined) { fields.push(`${col}=@${col}`); params[col] = b[col]; }
    }
    fields.push(`updated_at=datetime('now','localtime')`);
    db.prepare(`UPDATE accessories SET ${fields.join(',')} WHERE id=@id`).run(params);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/accessories/:id
  const accDel = pathname.match(/^\/api\/accessories\/(\d+)$/);
  if (accDel && method === 'DELETE') {
    db.prepare('DELETE FROM product_accessories WHERE acc_id=?').run(accDel[1]);
    db.prepare('DELETE FROM accessories WHERE id=?').run(accDel[1]);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/accessories/:id/products — 부속품을 사용하는 제품 목록
  const accProdGet = pathname.match(/^\/api\/accessories\/(\d+)\/products$/);
  if (accProdGet && method === 'GET') {
    const rows = db.prepare(`SELECT p.product_code, p.product_name, pa.qty_per, pa.id AS pa_id FROM product_accessories pa LEFT JOIN products p ON p.product_code=pa.product_code WHERE pa.acc_id=? ORDER BY pa.product_code`).all(accProdGet[1]);
    ok(res, rows);
    return;
  }

  // GET /api/products/:code/accessories — 제품별 부속품
  const prodAccGet = pathname.match(/^\/api\/products\/([^/]+)\/accessories$/);
  if (prodAccGet && method === 'GET') {
    const code = decodeURIComponent(prodAccGet[1]);
    const rows = db.prepare(`SELECT a.*, pa.qty_per, pa.id AS link_id FROM accessories a JOIN product_accessories pa ON a.id=pa.acc_id WHERE pa.product_code=? ORDER BY a.acc_type, a.acc_name`).all(code);
    ok(res, rows);
    return;
  }

  // POST /api/products/:code/accessories — 제품에 부속품 연결
  const prodAccPost = pathname.match(/^\/api\/products\/([^/]+)\/accessories$/);
  if (prodAccPost && method === 'POST') {
    const code = decodeURIComponent(prodAccPost[1]);
    const b = await readJSON(req);
    try {
      const info = db.prepare(`INSERT OR REPLACE INTO product_accessories (product_code, acc_id, qty_per) VALUES (?,?,?)`).run(code, b.acc_id, b.qty_per||1);
      ok(res, { id: info.lastInsertRowid });
    } catch(e) { fail(res, 400, e.message); }
    return;
  }

  // DELETE /api/product-accessories/:id — 연결 제거
  const prodAccDel = pathname.match(/^\/api\/product-accessories\/(\d+)$/);
  if (prodAccDel && method === 'DELETE') {
    db.prepare('DELETE FROM product_accessories WHERE id=?').run(prodAccDel[1]);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/tasks — 목록 (필터: status, category, assignee, priority)
  if (pathname === '/api/tasks' && method === 'GET') {
    const s = parsed.searchParams.get('status');
    const category = parsed.searchParams.get('category');
    const assignee = parsed.searchParams.get('assignee');
    const priority = parsed.searchParams.get('priority');
    const q = parsed.searchParams.get('q');
    let where = '1=1';
    const vals = [];
    if (s && s !== 'all') { where += ' AND status=?'; vals.push(s); }
    if (category && category !== 'all') { where += ' AND category=?'; vals.push(category); }
    if (assignee) { where += ' AND assignee LIKE ?'; vals.push('%' + assignee + '%'); }
    if (priority && priority !== 'all') { where += ' AND priority=?'; vals.push(priority); }
    if (q) { where += ' AND (title LIKE ? OR description LIKE ?)'; vals.push('%'+q+'%', '%'+q+'%'); }
    const rows = db.prepare(`SELECT * FROM tasks WHERE ${where} ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 WHEN 'low' THEN 4 ELSE 5 END, due_date ASC, created_at DESC`).all(...vals);
    ok(res, rows);
    return;
  }

  // GET /api/tasks/:id — 단건
  if (taskMatch && method === 'GET') {
    const row = db.prepare('SELECT * FROM tasks WHERE id=?').get(parseInt(taskMatch[1]));
    if (!row) { fail(res, 404, 'Not found'); return; }
    const comments = db.prepare('SELECT * FROM task_comments WHERE task_id=? ORDER BY created_at ASC').all(row.id);
    ok(res, { ...row, comments });
    return;
  }

  // POST /api/tasks — 생성
  if (pathname === '/api/tasks' && method === 'POST') {
    const b = await readJSON(req);
    if (!b.title) { fail(res, 400, 'title 필수'); return; }
    const today = new Date();
    const num = 'TASK-' + today.getFullYear().toString().slice(2) +
      String(today.getMonth()+1).padStart(2,'0') +
      String(today.getDate()).padStart(2,'0') + '-' +
      String(db.prepare("SELECT COUNT(*) as c FROM tasks").get().c + 1).padStart(3,'0');
    const info = db.prepare(`INSERT INTO tasks (task_number,title,description,category,status,priority,assignee,due_date,start_date,related_po,related_vendor,tags,created_by)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      num, b.title, b.description||'', b.category||'기타', b.status||'todo',
      b.priority||'normal', b.assignee||'', b.due_date||'', b.start_date||'',
      b.related_po||'', b.related_vendor||'', b.tags||'', b.created_by||''
    );
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'task_create', 'tasks', info.lastInsertRowid, `업무 생성: ${b.title}`, clientIP);
    ok(res, { id: info.lastInsertRowid, task_number: num });
    return;
  }

  // PUT /api/tasks/:id — 수정
  if (taskMatch && method === 'PUT') {
    const id = parseInt(taskMatch[1]);
    const b = await readJSON(req);
    const sets = [], vals2 = [];
    ['title','description','category','status','priority','assignee','due_date','start_date','related_po','related_vendor','tags'].forEach(f => {
      if (b[f] !== undefined) { sets.push(`${f}=?`); vals2.push(b[f]); }
    });
    if (b.status === 'done') { sets.push("completed_at=datetime('now','localtime')"); }
    else if (b.status && b.status !== 'done') { sets.push("completed_at=''"); }
    sets.push("updated_at=datetime('now','localtime')");
    vals2.push(id);
    db.prepare(`UPDATE tasks SET ${sets.join(',')} WHERE id=?`).run(...vals2);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'task_update', 'tasks', id, `업무 수정: ${b.status ? '상태→'+b.status : ''}${b.title ? ' 제목→'+b.title : ''}`, clientIP);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/tasks/:id — 삭제
  if (taskMatch && method === 'DELETE') {
    const id = parseInt(taskMatch[1]);
    db.prepare('DELETE FROM task_comments WHERE task_id=?').run(id);
    db.prepare('DELETE FROM tasks WHERE id=?').run(id);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'task_delete', 'tasks', id, `업무 삭제`, clientIP);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/tasks/:id/comments — 댓글 목록
  if (taskCommentMatch && method === 'GET') {
    const rows = db.prepare('SELECT * FROM task_comments WHERE task_id=? ORDER BY created_at ASC').all(parseInt(taskCommentMatch[1]));
    ok(res, rows);
    return;
  }

  // POST /api/tasks/:id/comments — 댓글 추가
  if (taskCommentMatch && method === 'POST') {
    const b = await readJSON(req);
    if (!b.content) { fail(res, 400, 'content 필수'); return; }
    const info = db.prepare('INSERT INTO task_comments (task_id, author, content) VALUES (?,?,?)').run(
      parseInt(taskCommentMatch[1]), b.author||'', b.content
    );
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  // GET /api/task-templates — 템플릿 목록
  if (pathname === '/api/task-templates' && method === 'GET') {
    ok(res, Object.entries(TASK_TEMPLATES).map(([id, t]) => ({ id, name: t.name, category: t.category, step_count: t.steps.length })));
    return;
  }

  // POST /api/tasks/:id/steps/init — 템플릿 기반 단계 초기화
  const stepsInitMatch = pathname.match(/^\/api\/tasks\/(\d+)\/steps\/init$/);
  if (stepsInitMatch && method === 'POST') {
    const taskId = parseInt(stepsInitMatch[1]);
    const b = await readJSON(req);
    const tpl = TASK_TEMPLATES[b.template_id];
    if (!tpl) { fail(res, 400, '템플릿 없음'); return; }
    // 기존 단계 삭제 후 재생성
    db.prepare('DELETE FROM task_steps WHERE task_id=?').run(taskId);
    const steps = b.custom_steps && b.custom_steps.length ? b.custom_steps : tpl.steps;
    const insert = db.prepare('INSERT INTO task_steps (task_id, step_order, step_name, step_type) VALUES (?,?,?,?)');
    steps.forEach((s, i) => insert.run(taskId, i, s.name, s.type || 'text'));
    db.prepare("UPDATE tasks SET template_id=? WHERE id=?").run(b.template_id, taskId);
    ok(res, { created: steps.length });
    return;
  }

  // GET /api/tasks/:id/steps — 단계 목록
  const stepsMatch = pathname.match(/^\/api\/tasks\/(\d+)\/steps$/);
  if (stepsMatch && method === 'GET') {
    const rows = db.prepare('SELECT * FROM task_steps WHERE task_id=? ORDER BY step_order').all(parseInt(stepsMatch[1]));
    ok(res, rows);
    return;
  }

  // PUT /api/task-steps/:stepId — 단계 업데이트
  const stepUpdateMatch = pathname.match(/^\/api\/task-steps\/(\d+)$/);
  if (stepUpdateMatch && method === 'PUT') {
    const stepId = parseInt(stepUpdateMatch[1]);
    const b = await readJSON(req);
    const sets = [], vals = [];
    if (b.value !== undefined) { sets.push('value=?'); vals.push(b.value); }
    if (b.note !== undefined) { sets.push('note=?'); vals.push(b.note); }
    if (b.is_done !== undefined) {
      sets.push('is_done=?'); vals.push(b.is_done ? 1 : 0);
      sets.push('done_at=?'); vals.push(b.is_done ? new Date().toLocaleString('ko-KR') : '');
    }
    if (!sets.length) { fail(res, 400, '변경 없음'); return; }
    vals.push(stepId);
    db.prepare(`UPDATE task_steps SET ${sets.join(',')} WHERE id=?`).run(...vals);
    // 모든 단계 완료 시 task 상태 자동 업데이트
    const step = db.prepare('SELECT task_id FROM task_steps WHERE id=?').get(stepId);
    if (step) {
      const total = db.prepare('SELECT COUNT(*) as c FROM task_steps WHERE task_id=?').get(step.task_id).c;
      const done = db.prepare("SELECT COUNT(*) as c FROM task_steps WHERE task_id=? AND is_done=1").get(step.task_id).c;
      if (total > 0 && done === total) {
        db.prepare("UPDATE tasks SET status='done', completed_at=datetime('now','localtime') WHERE id=?").run(step.task_id);
      } else if (done > 0) {
        db.prepare("UPDATE tasks SET status='in_progress' WHERE id=? AND status='todo'").run(step.task_id);
      }
    }
    ok(res, { updated: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  더기프트 세트 생산재고 API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/gift-sets — 세트 목록 (기초재고/생산재고/출고재고(XERP)/잔여재고)
  if (pathname === '/api/gift-sets' && method === 'GET') {
    const status = parsed.searchParams.get('status') || 'active';
    const sets = status === 'all'
      ? db.prepare('SELECT * FROM gift_sets ORDER BY set_name').all()
      : db.prepare('SELECT * FROM gift_sets WHERE status=? ORDER BY set_name').all(status);
    const bomStmt = db.prepare('SELECT * FROM gift_set_bom WHERE set_id=? ORDER BY item_type, id');
    // 생산재고 = 전체 assembly 합산
    const totalAssemblyStmt = db.prepare("SELECT COALESCE(SUM(qty),0) as total FROM gift_set_transactions WHERE set_id=? AND tx_type='assembly'");
    // 오늘 생산량
    const todayAssemblyStmt = db.prepare("SELECT COALESCE(SUM(qty),0) as total FROM gift_set_transactions WHERE set_id=? AND tx_type='assembly' AND date(created_at)=date('now','localtime')");

    // XERP 출고재고: 캐시 사용 (10분 간격 갱신)
    const now = Date.now();
    if (now - giftSetShipmentCacheTime > 600000) {
      // 백그라운드 캐시 갱신 (응답 블로킹 안 함)
      const xerpCodes = sets.map(s => (s.xerp_code || '').trim()).filter(Boolean);
      if (xerpCodes.length && xerpPool) {
        (async () => {
          try {
            const req = xerpPool.request();
            const placeholders = xerpCodes.map((c, i) => { req.input(`xc${i}`, sql.VarChar(50), c); return `@xc${i}`; }).join(',');
            const result = await req.query(`
              SELECT RTRIM(ItemCode) AS item_code, SUM(InoutQty) AS total_qty
              FROM mmInoutItem WITH (NOLOCK)
              WHERE SiteCode='BK10' AND InoutGubun='SO'
                AND InoutDate >= '20260101'
                AND ItemCode IN (${placeholders})
              GROUP BY RTRIM(ItemCode)
            `);
            const newCache = {};
            for (const r of result.recordset) {
              newCache[(r.item_code || '').trim()] = r.total_qty || 0;
            }
            giftSetShipmentCache = newCache;
            giftSetShipmentCacheTime = Date.now();
            console.log('Gift-set XERP 출고 캐시 갱신 완료:', Object.keys(newCache).length, '개');
          } catch (e) { console.warn('Gift-set XERP 출고캐시 갱신 실패:', e.message); }
        })();
      }
    }
    const xerpShipments = giftSetShipmentCache;

    for (const s of sets) {
      s.bom = bomStmt.all(s.id);
      const totalAssembly = totalAssemblyStmt.get(s.id).total;
      const todayAssembly = todayAssemblyStmt.get(s.id).total;
      const xerpCode = (s.xerp_code || '').trim();
      const totalShipped = xerpCode ? (xerpShipments[xerpCode] || 0) : 0;
      // 4가지 재고
      s.production_stock = totalAssembly;          // 생산재고 (조립 누적)
      s.shipped_stock = totalShipped;              // 출고재고 (XERP)
      s.remaining_stock = s.base_stock + totalAssembly - totalShipped; // 잔여재고
      s.today_assembled = todayAssembly;
      // current_stock도 잔여재고로 동기화
      s.current_stock = s.remaining_stock;
    }
    ok(res, sets);
    return;
  }

  // POST /api/gift-sets — 세트 등록
  if (pathname === '/api/gift-sets' && method === 'POST') {
    const body = await readJSON(req);
    const { set_code, set_name, description, base_stock, xerp_code, bom } = body;
    if (!set_code || !set_name) { fail(res, 400, '세트코드와 이름은 필수입니다'); return; }
    const existing = db.prepare('SELECT id FROM gift_sets WHERE set_code=?').get(set_code);
    if (existing) { fail(res, 409, '이미 존재하는 세트코드입니다'); return; }
    const initStock = parseInt(base_stock) || 0;
    const result = db.prepare('INSERT INTO gift_sets (set_code, set_name, description, base_stock, current_stock, xerp_code) VALUES (?,?,?,?,?,?)').run(set_code, set_name, description || '', initStock, initStock, xerp_code || '');
    const setId = result.lastInsertRowid;
    if (initStock > 0) {
      db.prepare('INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo) VALUES (?,?,?,?,?)').run(setId, 'base', initStock, body.operator || '', '기초재고 설정');
    }
    if (Array.isArray(bom) && bom.length) {
      const ins = db.prepare('INSERT OR IGNORE INTO gift_set_bom (set_id, item_type, item_code, item_name, qty_per, unit) VALUES (?,?,?,?,?,?)');
      for (const b of bom) ins.run(setId, b.item_type || 'material', b.item_code || '', b.item_name || '', b.qty_per || 1, b.unit || 'EA');
    }
    ok(res, { id: setId });
    return;
  }

  // PUT /api/gift-sets/:id — 세트 수정
  const gsUpdate = pathname.match(/^\/api\/gift-sets\/(\d+)$/);
  if (gsUpdate && method === 'PUT') {
    const id = parseInt(gsUpdate[1]);
    const body = await readJSON(req);
    const sets = [], vals = [];
    if (body.set_name !== undefined) { sets.push('set_name=?'); vals.push(body.set_name); }
    if (body.description !== undefined) { sets.push('description=?'); vals.push(body.description); }
    if (body.status !== undefined) { sets.push('status=?'); vals.push(body.status); }
    if (body.xerp_code !== undefined) { sets.push('xerp_code=?'); vals.push(body.xerp_code); }
    if (body.base_stock !== undefined) { sets.push('base_stock=?'); vals.push(parseInt(body.base_stock) || 0); }
    if (sets.length) {
      sets.push("updated_at=datetime('now','localtime')");
      vals.push(id);
      db.prepare(`UPDATE gift_sets SET ${sets.join(',')} WHERE id=?`).run(...vals);
    }
    if (Array.isArray(body.bom)) {
      db.prepare('DELETE FROM gift_set_bom WHERE set_id=?').run(id);
      const ins = db.prepare('INSERT OR IGNORE INTO gift_set_bom (set_id, item_type, item_code, item_name, qty_per, unit) VALUES (?,?,?,?,?,?)');
      for (const b of body.bom) ins.run(id, b.item_type || 'material', b.item_code || '', b.item_name || '', b.qty_per || 1, b.unit || 'EA');
    }
    ok(res, { updated: true });
    return;
  }

  // POST /api/gift-sets/:id/transaction — 입고/출고/기초재고/조정
  const gsTx = pathname.match(/^\/api\/gift-sets\/(\d+)\/transaction$/);
  if (gsTx && method === 'POST') {
    const id = parseInt(gsTx[1]);
    const body = await readJSON(req);
    const { tx_type, qty, operator, memo } = body;
    if (!['base', 'assembly', 'shipment', 'adjust'].includes(tx_type)) { fail(res, 400, '유효하지 않은 거래유형'); return; }
    const amount = parseInt(qty);
    if (!amount || amount <= 0) { fail(res, 400, '수량은 1 이상이어야 합니다'); return; }
    const gs = db.prepare('SELECT * FROM gift_sets WHERE id=?').get(id);
    if (!gs) { fail(res, 404, '세트를 찾을 수 없습니다'); return; }
    const txRun = db.transaction(() => {
      db.prepare('INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo) VALUES (?,?,?,?,?)').run(id, tx_type, amount, operator || '', memo || '');
      let newStock;
      if (tx_type === 'base') {
        newStock = amount;
        db.prepare('UPDATE gift_sets SET current_stock=?, base_stock=?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, amount, id);
      } else if (tx_type === 'assembly') {
        newStock = gs.current_stock + amount;
        db.prepare('UPDATE gift_sets SET current_stock=current_stock+?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, id);
      } else if (tx_type === 'shipment') {
        newStock = gs.current_stock - amount;
        db.prepare('UPDATE gift_sets SET current_stock=current_stock-?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, id);
      } else {
        newStock = gs.current_stock + amount;
        db.prepare('UPDATE gift_sets SET current_stock=current_stock+?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, id);
      }
      return newStock;
    });
    const newStock = txRun();
    ok(res, { new_stock: newStock });
    return;
  }

  // GET /api/gift-sets/:id/transactions — 이력 조회
  const gsTxList = pathname.match(/^\/api\/gift-sets\/(\d+)\/transactions$/);
  if (gsTxList && method === 'GET') {
    const id = parseInt(gsTxList[1]);
    const date = parsed.searchParams.get('date') || new Date().toLocaleDateString('en-CA');
    const limit = parseInt(parsed.searchParams.get('limit')) || 200;
    const rows = db.prepare("SELECT * FROM gift_set_transactions WHERE set_id=? AND date(created_at)=? ORDER BY created_at DESC LIMIT ?").all(id, date, limit);
    ok(res, rows);
    return;
  }

  // GET /api/gift-sets/production-capacity — 최대 생산가능수량
  if (pathname === '/api/gift-sets/production-capacity' && method === 'GET') {
    const sets = db.prepare("SELECT * FROM gift_sets WHERE status='active' ORDER BY set_name").all();
    const bomStmt = db.prepare('SELECT * FROM gift_set_bom WHERE set_id=?');
    const accStmt = db.prepare("SELECT current_stock FROM accessories WHERE acc_code=? OR acc_name=? LIMIT 1");
    const xerpProducts = (xerpInventoryCache && xerpInventoryCache.products) ? xerpInventoryCache.products : [];
    const result = [];
    for (const s of sets) {
      const bomItems = bomStmt.all(s.id);
      let maxProduction = Infinity;
      let bottleneck = null;
      const components = [];
      for (const b of bomItems) {
        let available = 0;
        if (b.item_type === 'material') {
          const xp = xerpProducts.find(p => (p['제품코드'] || '') === b.item_code);
          available = xp ? (xp['가용재고'] || 0) : 0;
        } else {
          const acc = accStmt.get(b.item_code, b.item_name);
          available = acc ? acc.current_stock : 0;
        }
        const canMake = b.qty_per > 0 ? Math.floor(available / b.qty_per) : Infinity;
        components.push({ item_type: b.item_type, item_code: b.item_code, item_name: b.item_name, qty_per: b.qty_per, available, can_make: canMake });
        if (canMake < maxProduction) {
          maxProduction = canMake;
          bottleneck = b.item_name || b.item_code;
        }
      }
      if (maxProduction === Infinity) maxProduction = 0;
      result.push({ id: s.id, set_code: s.set_code, set_name: s.set_name, current_stock: s.current_stock, max_production: maxProduction, bottleneck, components });
    }
    ok(res, result);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  다중 창고 관리 API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/warehouses — 창고 목록
  if (pathname === '/api/warehouses' && method === 'GET') {
    const rows = db.prepare("SELECT * FROM warehouses ORDER BY is_default DESC, id ASC").all();
    ok(res, rows);
    return;
  }

  // POST /api/warehouses — 창고 등록
  if (pathname === '/api/warehouses' && method === 'POST') {
    const body = await readJSON(req);
    const { code, name, location, description } = body;
    if (!code || !name) { fail(res, 400, '창고코드와 이름은 필수입니다'); return; }
    try {
      db.prepare("INSERT INTO warehouses (code, name, location, description) VALUES (?, ?, ?, ?)").run(code, name, location || '', description || '');
      ok(res, { message: '창고 등록 완료' });
    } catch (e) {
      if (e.message.includes('UNIQUE')) fail(res, 409, '이미 존재하는 창고코드입니다');
      else fail(res, 500, e.message);
    }
    return;
  }

  // PUT /api/warehouses/:id — 창고 수정
  const whPutMatch = pathname.match(/^\/api\/warehouses\/(\d+)$/);
  if (whPutMatch && method === 'PUT') {
    const body = await readJSON(req);
    const whId = parseInt(whPutMatch[1]);
    const { name, location, description, status } = body;
    const fields = [];
    const vals = [];
    if (name !== undefined) { fields.push('name=?'); vals.push(name); }
    if (location !== undefined) { fields.push('location=?'); vals.push(location); }
    if (description !== undefined) { fields.push('description=?'); vals.push(description); }
    if (status !== undefined) { fields.push('status=?'); vals.push(status); }
    if (fields.length === 0) { fail(res, 400, '수정할 내용이 없습니다'); return; }
    fields.push("updated_at=datetime('now','localtime')");
    vals.push(whId);
    db.prepare(`UPDATE warehouses SET ${fields.join(', ')} WHERE id=?`).run(...vals);
    ok(res, { message: '창고 수정 완료' });
    return;
  }

  // DELETE /api/warehouses/:id — 창고 삭제
  const whDelMatch = pathname.match(/^\/api\/warehouses\/(\d+)$/);
  if (whDelMatch && method === 'DELETE') {
    const whId = parseInt(whDelMatch[1]);
    const wh = db.prepare("SELECT * FROM warehouses WHERE id=?").get(whId);
    if (!wh) { fail(res, 404, '창고를 찾을 수 없습니다'); return; }
    if (wh.is_default) { fail(res, 400, '기본 창고는 삭제할 수 없습니다'); return; }
    const invCount = db.prepare("SELECT COUNT(*) as cnt FROM warehouse_inventory WHERE warehouse_id=? AND quantity>0").get(whId);
    if (invCount.cnt > 0) { fail(res, 400, '재고가 남아있는 창고는 삭제할 수 없습니다. 먼저 재고를 이동해주세요.'); return; }
    db.prepare("DELETE FROM warehouse_inventory WHERE warehouse_id=?").run(whId);
    db.prepare("DELETE FROM warehouses WHERE id=?").run(whId);
    ok(res, { message: '창고 삭제 완료' });
    return;
  }

  // GET /api/warehouses/inventory — 전체 창고 재고 (전체보기 + 창고별)
  if (pathname === '/api/warehouses/inventory' && method === 'GET') {
    const warehouseId = parsed.searchParams.get('warehouse_id');
    const search = parsed.searchParams.get('search') || '';
    let rows;
    if (warehouseId) {
      let sql = `SELECT wi.*, w.name as warehouse_name, w.code as warehouse_code
        FROM warehouse_inventory wi JOIN warehouses w ON wi.warehouse_id=w.id
        WHERE wi.warehouse_id=?`;
      const args = [warehouseId];
      if (search) { sql += " AND (wi.product_code LIKE ? OR wi.product_name LIKE ?)"; args.push(`%${search}%`, `%${search}%`); }
      sql += " ORDER BY wi.product_code";
      rows = db.prepare(sql).all(...args);
    } else {
      // 전체: 품목별 합산 + 창고별 내역
      let sql = `SELECT wi.product_code, wi.product_name,
        SUM(wi.quantity) as total_qty,
        GROUP_CONCAT(w.name || ':' || wi.quantity, ' | ') as breakdown
        FROM warehouse_inventory wi JOIN warehouses w ON wi.warehouse_id=w.id`;
      const args = [];
      if (search) { sql += " WHERE wi.product_code LIKE ? OR wi.product_name LIKE ?"; args.push(`%${search}%`, `%${search}%`); }
      sql += " GROUP BY wi.product_code, wi.product_name ORDER BY wi.product_code";
      rows = db.prepare(sql).all(...args);
    }
    ok(res, rows);
    return;
  }

  // POST /api/warehouses/inventory — 재고 입력/수정 (단건)
  if (pathname === '/api/warehouses/inventory' && method === 'POST') {
    const body = await readJSON(req);
    const { warehouse_id, product_code, product_name, quantity } = body;
    if (!warehouse_id || !product_code) { fail(res, 400, '창고ID와 제품코드는 필수입니다'); return; }
    const qty = parseInt(quantity) || 0;
    const existing = db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(warehouse_id, product_code);
    if (existing) {
      db.prepare("UPDATE warehouse_inventory SET quantity=?, product_name=?, updated_at=datetime('now','localtime') WHERE id=?").run(qty, product_name || existing.product_name, existing.id);
    } else {
      db.prepare("INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity) VALUES (?, ?, ?, ?)").run(warehouse_id, product_code, product_name || '', qty);
    }
    ok(res, { message: '재고 저장 완료' });
    return;
  }

  // POST /api/warehouses/inventory/bulk — 대량 재고 입력 (XERP 동기화 등)
  if (pathname === '/api/warehouses/inventory/bulk' && method === 'POST') {
    const body = await readJSON(req);
    const { warehouse_id, items } = body;
    if (!warehouse_id || !Array.isArray(items)) { fail(res, 400, '창고ID와 items 배열 필수'); return; }
    const upsert = db.prepare(`INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity)
      VALUES (?, ?, ?, ?) ON CONFLICT(warehouse_id, product_code) DO UPDATE SET quantity=excluded.quantity, product_name=excluded.product_name, updated_at=datetime('now','localtime')`);
    const tx = db.transaction((list) => {
      let cnt = 0;
      for (const it of list) {
        upsert.run(warehouse_id, it.product_code, it.product_name || '', parseInt(it.quantity) || 0);
        cnt++;
      }
      return cnt;
    });
    const count = tx(items);
    ok(res, { message: `${count}건 저장 완료` });
    return;
  }

  // POST /api/warehouses/transfer — 창고 간 재고 이동
  if (pathname === '/api/warehouses/transfer' && method === 'POST') {
    const body = await readJSON(req);
    const { from_warehouse, to_warehouse, product_code, product_name, quantity, operator, memo } = body;
    if (!from_warehouse || !to_warehouse || !product_code || !quantity) {
      fail(res, 400, '출발창고, 도착창고, 제품코드, 수량은 필수입니다'); return;
    }
    if (from_warehouse === to_warehouse) { fail(res, 400, '같은 창고로는 이동할 수 없습니다'); return; }
    const qty = parseInt(quantity);
    if (qty <= 0) { fail(res, 400, '이동 수량은 1 이상이어야 합니다'); return; }

    // 출발 창고 재고 확인
    const fromInv = db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(from_warehouse, product_code);
    if (!fromInv || fromInv.quantity < qty) {
      fail(res, 400, `출발 창고 재고 부족 (현재: ${fromInv ? fromInv.quantity : 0})`); return;
    }

    // 창고명 조회
    const fromWh = db.prepare("SELECT name FROM warehouses WHERE id=?").get(from_warehouse);
    const toWh = db.prepare("SELECT name FROM warehouses WHERE id=?").get(to_warehouse);
    const now = new Date().toISOString().slice(0, 10);
    const autoMemo = memo || `${now} ${qty}개 ${fromWh ? fromWh.name : ''}→${toWh ? toWh.name : ''}`;

    const tx = db.transaction(() => {
      // 출발 창고 차감 + 메모 업데이트
      const fromMemo = `${now} ${qty}개 출고→${toWh ? toWh.name : ''}`;
      db.prepare("UPDATE warehouse_inventory SET quantity=quantity-?, memo=?, updated_at=datetime('now','localtime') WHERE warehouse_id=? AND product_code=?").run(qty, fromMemo, from_warehouse, product_code);
      // 도착 창고 추가 + 메모 업데이트
      const toMemo = `${now} ${qty}개 입고←${fromWh ? fromWh.name : ''}`;
      const toInv = db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(to_warehouse, product_code);
      if (toInv) {
        db.prepare("UPDATE warehouse_inventory SET quantity=quantity+?, memo=?, updated_at=datetime('now','localtime') WHERE warehouse_id=? AND product_code=?").run(qty, toMemo, to_warehouse, product_code);
      } else {
        db.prepare("INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity, memo) VALUES (?, ?, ?, ?, ?)").run(to_warehouse, product_code, product_name || fromInv.product_name || '', qty, toMemo);
      }
      // 이력 기록
      db.prepare("INSERT INTO warehouse_transfers (from_warehouse, to_warehouse, product_code, product_name, quantity, operator, memo) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .run(from_warehouse, to_warehouse, product_code, product_name || fromInv.product_name || '', qty, operator || '', autoMemo);
    });
    tx();
    ok(res, { message: `${qty}개 이동 완료` });
    return;
  }

  // GET /api/warehouses/transfers — 이동 이력 (필터 지원)
  if (pathname === '/api/warehouses/transfers' && method === 'GET') {
    const sp = parsed.searchParams;
    const from_date = sp.get('from_date') || '';
    const to_date = sp.get('to_date') || '';
    const from_wh = sp.get('from_wh') || '';
    const to_wh = sp.get('to_wh') || '';
    const search = sp.get('search') || '';
    const operator = sp.get('operator') || '';
    const days = parseInt(sp.get('days') || '30');

    let where = [];
    let args = [];
    if (from_date) { where.push("t.created_at >= ?"); args.push(from_date + ' 00:00:00'); }
    else { where.push(`t.created_at >= datetime('now','localtime','-${days} days')`); }
    if (to_date) { where.push("t.created_at <= ?"); args.push(to_date + ' 23:59:59'); }
    if (from_wh) { where.push("t.from_warehouse = ?"); args.push(parseInt(from_wh)); }
    if (to_wh) { where.push("t.to_warehouse = ?"); args.push(parseInt(to_wh)); }
    if (search) { where.push("(t.product_code LIKE ? OR t.product_name LIKE ?)"); args.push(`%${search}%`, `%${search}%`); }
    if (operator) { where.push("t.operator LIKE ?"); args.push(`%${operator}%`); }

    const sql = `SELECT t.*, fw.name as from_name, tw.name as to_name
      FROM warehouse_transfers t
      JOIN warehouses fw ON t.from_warehouse=fw.id
      JOIN warehouses tw ON t.to_warehouse=tw.id
      WHERE ${where.join(' AND ')}
      ORDER BY t.created_at DESC`;
    const rows = db.prepare(sql).all(...args);
    ok(res, rows);
    return;
  }

  // GET /api/warehouses/transfers/stats — 이동 통계
  if (pathname === '/api/warehouses/transfers/stats' && method === 'GET') {
    const sp = parsed.searchParams;
    const from_date = sp.get('from_date') || '';
    const to_date = sp.get('to_date') || '';
    const days = parseInt(sp.get('days') || '30');

    let dateFilter;
    const args = [];
    if (from_date) {
      dateFilter = "t.created_at >= ?";
      args.push(from_date + ' 00:00:00');
      if (to_date) { dateFilter += " AND t.created_at <= ?"; args.push(to_date + ' 23:59:59'); }
    } else {
      dateFilter = `t.created_at >= datetime('now','localtime','-${days} days')`;
    }

    // 총 건수/수량
    const total = db.prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(quantity),0) as total_qty FROM warehouse_transfers t WHERE ${dateFilter}`).get(...args);
    // 오늘 건수
    const today = db.prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(quantity),0) as total_qty FROM warehouse_transfers t WHERE date(t.created_at)=date('now','localtime')`).get();
    // 최다 이동 품목
    const topItem = db.prepare(`SELECT product_code, product_name, SUM(quantity) as total_qty, COUNT(*) as cnt FROM warehouse_transfers t WHERE ${dateFilter} GROUP BY product_code ORDER BY total_qty DESC LIMIT 1`).get(...args);
    // 창고 간 흐름 TOP5
    const flows = db.prepare(`SELECT fw.name as from_name, tw.name as to_name, COUNT(*) as cnt, SUM(t.quantity) as total_qty
      FROM warehouse_transfers t
      JOIN warehouses fw ON t.from_warehouse=fw.id
      JOIN warehouses tw ON t.to_warehouse=tw.id
      WHERE ${dateFilter}
      GROUP BY t.from_warehouse, t.to_warehouse
      ORDER BY total_qty DESC LIMIT 5`).all(...args);
    // 담당자별
    const operators = db.prepare(`SELECT operator, COUNT(*) as cnt, SUM(quantity) as total_qty FROM warehouse_transfers t WHERE ${dateFilter} AND operator!='' GROUP BY operator ORDER BY cnt DESC LIMIT 5`).all(...args);

    ok(res, { total, today, topItem, flows, operators });
    return;
  }

  // POST /api/warehouses/adjust — 재고 조정
  if (pathname === '/api/warehouses/adjust' && method === 'POST') {
    const body = await readJSON(req);
    const { warehouse_id, product_code, product_name, new_quantity, reason, operator } = body;
    if (!warehouse_id || !product_code || new_quantity === undefined) {
      fail(res, 400, '창고ID, 제품코드, 조정수량은 필수입니다'); return;
    }
    const newQty = parseInt(new_quantity);
    const existing = db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(warehouse_id, product_code);
    const beforeQty = existing ? existing.quantity : 0;
    const diff = newQty - beforeQty;
    const adjType = diff > 0 ? 'increase' : diff < 0 ? 'decrease' : 'no_change';

    const tx = db.transaction(() => {
      if (existing) {
        db.prepare("UPDATE warehouse_inventory SET quantity=?, updated_at=datetime('now','localtime') WHERE id=?").run(newQty, existing.id);
      } else {
        db.prepare("INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity) VALUES (?, ?, ?, ?)").run(warehouse_id, product_code, product_name || '', newQty);
      }
      db.prepare("INSERT INTO warehouse_adjustments (warehouse_id, product_code, product_name, adj_type, before_qty, after_qty, diff_qty, reason, operator) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .run(warehouse_id, product_code, product_name || (existing ? existing.product_name : ''), adjType, beforeQty, newQty, diff, reason || '', operator || '');
    });
    tx();
    ok(res, { message: `재고 조정 완료 (${beforeQty} → ${newQty}, ${diff > 0 ? '+' : ''}${diff})` });
    return;
  }

  // GET /api/warehouses/adjustments — 조정 이력 (필터 지원)
  if (pathname === '/api/warehouses/adjustments' && method === 'GET') {
    const sp = parsed.searchParams;
    const from_date = sp.get('from_date') || '';
    const to_date = sp.get('to_date') || '';
    const wh = sp.get('warehouse_id') || '';
    const search = sp.get('search') || '';
    const days = parseInt(sp.get('days') || '30');

    let where = [];
    let args = [];
    if (from_date) { where.push("a.created_at >= ?"); args.push(from_date + ' 00:00:00'); }
    else { where.push(`a.created_at >= datetime('now','localtime','-${days} days')`); }
    if (to_date) { where.push("a.created_at <= ?"); args.push(to_date + ' 23:59:59'); }
    if (wh) { where.push("a.warehouse_id = ?"); args.push(parseInt(wh)); }
    if (search) { where.push("(a.product_code LIKE ? OR a.product_name LIKE ?)"); args.push(`%${search}%`, `%${search}%`); }

    const rows = db.prepare(`SELECT a.*, w.name as warehouse_name
      FROM warehouse_adjustments a JOIN warehouses w ON a.warehouse_id=w.id
      WHERE ${where.join(' AND ')}
      ORDER BY a.created_at DESC`).all(...args);
    ok(res, rows);
    return;
  }

  // POST /api/warehouses/sync-xerp — XERP 재고를 본사창고로 동기화
  if (pathname === '/api/warehouses/sync-xerp' && method === 'POST') {
    try {
      const pool = await ensureXerpPool();
      const result = await pool.request().query(`
        SELECT RTRIM(ItemCode) AS product_code, SUM(OhQty) AS quantity
        FROM mmInventory WITH (NOLOCK)
        WHERE SiteCode = 'BK10' AND OhQty > 0
        GROUP BY RTRIM(ItemCode)
      `);
      // 로컬 products 테이블에서 품목명 매칭
      const localProducts = {};
      try {
        const prods = db.prepare("SELECT product_code, product_name FROM products").all();
        for (const p of prods) localProducts[p.product_code] = p.product_name;
      } catch(e) {}
      const defaultWh = db.prepare("SELECT id FROM warehouses WHERE is_default=1 LIMIT 1").get();
      if (!defaultWh) { fail(res, 500, '기본 창고가 설정되지 않았습니다'); return; }

      const upsert = db.prepare(`INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity)
        VALUES (?, ?, ?, ?) ON CONFLICT(warehouse_id, product_code) DO UPDATE SET quantity=excluded.quantity, product_name=excluded.product_name, updated_at=datetime('now','localtime')`);
      const tx = db.transaction((rows) => {
        let cnt = 0;
        for (const r of rows) {
          upsert.run(defaultWh.id, r.product_code, localProducts[r.product_code] || r.product_code, parseInt(r.quantity) || 0);
          cnt++;
        }
        return cnt;
      });
      const count = tx(result.recordset);
      ok(res, { message: `XERP → 본사창고 동기화 완료 (${count}건)` });
    } catch (e) {
      fail(res, 500, 'XERP 동기화 실패: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  매출관리 API (Sales Management)
  // ════════════════════════════════════════════════════════════════════

  // 헬퍼: bar_shop1 임시 풀 (연결 누수 방지)
  async function withBarShop1Pool(callback) {
    let pool = null;
    try {
      pool = new sql.ConnectionPool({ ...xerpConfig, database: 'bar_shop1' });
      await pool.connect();
      return await callback(pool);
    } finally {
      if (pool) { try { await pool.close(); } catch (_) {} }
    }
  }

  // 헬퍼: YYYYMMDD 포맷
  function toYMD(d) {
    return d.getFullYear() + String(d.getMonth() + 1).padStart(2, '0') + String(d.getDate()).padStart(2, '0');
  }

  // 헬퍼: 월별 청크 분할
  function getMonthChunks(startYMD, endYMD) {
    const chunks = [];
    const sy = parseInt(startYMD.slice(0, 4)), sm = parseInt(startYMD.slice(4, 6)) - 1;
    const ey = parseInt(endYMD.slice(0, 4)), em = parseInt(endYMD.slice(4, 6)) - 1, ed = parseInt(endYMD.slice(6, 8));
    let cur = new Date(sy, sm, 1);
    const endDate = new Date(ey, em, ed);
    while (cur <= endDate) {
      const mStart = toYMD(cur);
      const lastDay = new Date(cur.getFullYear(), cur.getMonth() + 1, 0);
      const mEnd = toYMD(lastDay);
      chunks.push({ start: mStart < startYMD ? startYMD : mStart, end: mEnd > endYMD ? endYMD : mEnd });
      cur = new Date(cur.getFullYear(), cur.getMonth() + 1, 1);
    }
    return chunks;
  }

  // 헬퍼: XERP 매출 쿼리 (기간별)
  async function queryXerpSales(pool, startYMD, endYMD) {
    const r = await pool.request()
      .input('startDate', sql.NVarChar(16), startYMD)
      .input('endDate', sql.NVarChar(16), endYMD)
      .query(`SELECT COUNT(DISTINCT h_orderid) AS order_count,
                     ISNULL(SUM(h_sumPrice),0) AS total_sales,
                     ISNULL(SUM(h_offerPrice),0) AS total_supply,
                     ISNULL(SUM(h_superTax),0) AS total_vat,
                     ISNULL(SUM(FeeAmnt),0) AS total_fee
              FROM ERP_SalesData WITH (NOLOCK)
              WHERE h_date >= @startDate AND h_date <= @endDate`);
    return r.recordset[0] || { order_count: 0, total_sales: 0, total_supply: 0, total_vat: 0, total_fee: 0 };
  }

  // 헬퍼: DD 매출 쿼리 (기간별)
  async function queryDdSales(pool, startDate, endDate) {
    const [rows] = await pool.query(
      `SELECT COUNT(*) AS order_count, IFNULL(SUM(paid_money),0) AS total_sales
       FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C'`,
      [startDate, endDate]);
    return rows[0] || { order_count: 0, total_sales: 0 };
  }

  // 헬퍼: 더기프트 매출 쿼리 (mmInoutItem 출고 기반, 기간별)
  // 더기프트 = XERP mmInoutItem에서 SiteCode='BK10', InoutGubun='SO', 등록된 gift_sets의 xerp_code 매칭
  async function queryGiftSales(pool, startYMD, endYMD) {
    // gift_sets에서 등록된 xerp_code 목록
    const giftSets = db.prepare("SELECT xerp_code, set_name FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
    if (!giftSets.length) return { order_count: 0, total_sales: 0, total_qty: 0, items: 0 };
    const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
    if (!xerpCodes.length) return { order_count: 0, total_sales: 0, total_qty: 0, items: 0 };
    const req = pool.request();
    req.input('startDate', sql.NVarChar(16), startYMD);
    req.input('endDate', sql.NVarChar(16), endYMD);
    const placeholders = xerpCodes.map((c, i) => { req.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }).join(',');
    const r = await req.query(`
      SELECT COUNT(DISTINCT InoutNo) AS order_count,
             ISNULL(SUM(InoutAmnt),0) AS total_sales,
             ISNULL(SUM(InoutQty),0) AS total_qty,
             COUNT(DISTINCT RTRIM(ItemCode)) AS items
      FROM mmInoutItem WITH (NOLOCK)
      WHERE SiteCode='BK10' AND InoutGubun='SO'
        AND InoutDate >= @startDate AND InoutDate <= @endDate
        AND RTRIM(ItemCode) IN (${placeholders})`);
    const row = r.recordset[0] || {};
    return { order_count: row.order_count || 0, total_sales: Number(row.total_sales || 0), total_qty: Number(row.total_qty || 0), items: row.items || 0 };
  }

  // 헬퍼: 더기프트 일별 매출
  async function queryGiftDailySales(pool, startYMD, endYMD) {
    const giftSets = db.prepare("SELECT xerp_code FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
    const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
    if (!xerpCodes.length) return [];
    const req = pool.request();
    req.input('startDate', sql.NVarChar(16), startYMD);
    req.input('endDate', sql.NVarChar(16), endYMD);
    const placeholders = xerpCodes.map((c, i) => { req.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }).join(',');
    const r = await req.query(`
      SELECT RTRIM(InoutDate) AS inout_date,
             COUNT(DISTINCT InoutNo) AS order_count,
             ISNULL(SUM(InoutAmnt),0) AS total_sales,
             ISNULL(SUM(InoutQty),0) AS total_qty
      FROM mmInoutItem WITH (NOLOCK)
      WHERE SiteCode='BK10' AND InoutGubun='SO'
        AND InoutDate >= @startDate AND InoutDate <= @endDate
        AND RTRIM(ItemCode) IN (${placeholders})
      GROUP BY RTRIM(InoutDate) ORDER BY RTRIM(InoutDate)`);
    return r.recordset.map(row => ({
      date: (row.inout_date || '').trim(),
      sales: Number(row.total_sales || 0),
      orders: row.order_count || 0,
      qty: Number(row.total_qty || 0)
    }));
  }

  // 헬퍼: 더기프트 상품별 매출
  async function queryGiftProductSales(pool, startYMD, endYMD) {
    const giftSets = db.prepare("SELECT xerp_code, set_name FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
    const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
    if (!xerpCodes.length) return [];
    const codeNameMap = {};
    giftSets.forEach(g => { codeNameMap[g.xerp_code.trim()] = g.set_name; });
    const req = pool.request();
    req.input('startDate', sql.NVarChar(16), startYMD);
    req.input('endDate', sql.NVarChar(16), endYMD);
    const placeholders = xerpCodes.map((c, i) => { req.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }).join(',');
    const r = await req.query(`
      SELECT RTRIM(ItemCode) AS item_code, RTRIM(ItemName) AS item_name,
             COUNT(DISTINCT InoutNo) AS order_count,
             ISNULL(SUM(InoutAmnt),0) AS total_sales,
             ISNULL(SUM(InoutQty),0) AS total_qty
      FROM mmInoutItem WITH (NOLOCK)
      WHERE SiteCode='BK10' AND InoutGubun='SO'
        AND InoutDate >= @startDate AND InoutDate <= @endDate
        AND RTRIM(ItemCode) IN (${placeholders})
      GROUP BY RTRIM(ItemCode), RTRIM(ItemName)
      ORDER BY SUM(InoutAmnt) DESC`);
    return r.recordset.map((row, i) => ({
      rank: i + 1,
      code: (row.item_code || '').trim(),
      name: codeNameMap[(row.item_code || '').trim()] || (row.item_name || '').trim(),
      sales: Number(row.total_sales || 0),
      orders: row.order_count || 0,
      qty: Number(row.total_qty || 0)
    }));
  }

  // ── GET /api/sales/kpi ──
  if (pathname === '/api/sales/kpi' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const forceRefresh = parsed.searchParams.get('refresh') === '1';
    if (!forceRefresh && salesKpiCache && Date.now() - salesKpiCacheTime < SALES_CACHE_TTL) {
      ok(res, salesKpiCache); return;
    }
    const result = { today: {}, thisMonth: {}, lastMonth: {}, sameMonthLastYear: {}, momChange: {}, yoyChange: {}, sources: {} };
    const now = new Date();
    const todayYMD = toYMD(now);
    const monthStart = now.getFullYear() + String(now.getMonth() + 1).padStart(2, '0') + '01';
    const monthEnd = todayYMD;
    const lastMonthDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const lmStart = toYMD(lastMonthDate);
    const lmEnd = toYMD(new Date(now.getFullYear(), now.getMonth(), 0));
    const sylyDate = new Date(now.getFullYear() - 1, now.getMonth(), 1);
    const sylyStart = toYMD(sylyDate);
    const sylyEnd = toYMD(new Date(now.getFullYear() - 1, now.getMonth() + 1, 0));

    // XERP
    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      const [xToday, xThisMonth, xLastMonth, xSameLY] = await Promise.all([
        queryXerpSales(pool, todayYMD, todayYMD),
        queryXerpSales(pool, monthStart, monthEnd),
        queryXerpSales(pool, lmStart, lmEnd),
        queryXerpSales(pool, sylyStart, sylyEnd)
      ]);
      // XERP 데이터 1-2일 지연 → 오늘 데이터 없으면 어제 시도
      let xTodayFinal = xToday;
      if (xToday.order_count === 0) {
        const yesterday = new Date(now); yesterday.setDate(yesterday.getDate() - 1);
        const yYMD = toYMD(yesterday);
        xTodayFinal = await queryXerpSales(pool, yYMD, yYMD);
        xTodayFinal._dateUsed = yYMD;
      }
      result.today.xerp = { sales: Number(xTodayFinal.total_sales), orders: xTodayFinal.order_count, dateUsed: xTodayFinal._dateUsed || todayYMD };
      result.thisMonth.xerp = { sales: Number(xThisMonth.total_sales), orders: xThisMonth.order_count, supply: Number(xThisMonth.total_supply), vat: Number(xThisMonth.total_vat), fee: Number(xThisMonth.total_fee) };
      result.lastMonth.xerp = { sales: Number(xLastMonth.total_sales), orders: xLastMonth.order_count };
      result.sameMonthLastYear.xerp = { sales: Number(xSameLY.total_sales), orders: xSameLY.order_count };
      result.sources.xerp = 'connected';
    } catch (e) {
      console.error('Sales KPI XERP error:', e.message);
      logError('warn', 'Sales KPI XERP: ' + e.message, e.stack, req.url, req.method);
      result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied') || e.number === 229) ? 'access_denied' : 'error';
      result.today.xerp = { sales: 0, orders: 0 };
      result.thisMonth.xerp = { sales: 0, orders: 0, supply: 0, vat: 0, fee: 0 };
      result.lastMonth.xerp = { sales: 0, orders: 0 };
      result.sameMonthLastYear.xerp = { sales: 0, orders: 0 };
    }

    // DD
    const todayISO = now.toISOString().slice(0, 10);
    const tomorrowISO = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1).toISOString().slice(0, 10);
    const mStartISO = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-01';
    const lmStartISO = lastMonthDate.toISOString().slice(0, 10);
    const lmEndISO = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
    const sylyStartISO = sylyDate.toISOString().slice(0, 10);
    const sylyEndISO = new Date(now.getFullYear() - 1, now.getMonth() + 1, 1).toISOString().slice(0, 10);
    try {
      const pool = await ensureDdPool();
      if (!pool) throw new Error('DD pool unavailable');
      const [dToday, dThisMonth, dLastMonth, dSameLY] = await Promise.all([
        queryDdSales(pool, todayISO, tomorrowISO),
        queryDdSales(pool, mStartISO, tomorrowISO),
        queryDdSales(pool, lmStartISO, lmEndISO),
        queryDdSales(pool, sylyStartISO, sylyEndISO)
      ]);
      result.today.dd = { sales: Number(dToday.total_sales), orders: dToday.order_count };
      result.thisMonth.dd = { sales: Number(dThisMonth.total_sales), orders: dThisMonth.order_count };
      result.lastMonth.dd = { sales: Number(dLastMonth.total_sales), orders: dLastMonth.order_count };
      result.sameMonthLastYear.dd = { sales: Number(dSameLY.total_sales), orders: dSameLY.order_count };
      result.sources.dd = 'connected';
    } catch (e) {
      console.error('Sales KPI DD error:', e.message);
      logError('warn', 'Sales KPI DD: ' + e.message, e.stack, req.url, req.method);
      result.sources.dd = 'error';
      result.today.dd = { sales: 0, orders: 0 };
      result.thisMonth.dd = { sales: 0, orders: 0 };
      result.lastMonth.dd = { sales: 0, orders: 0 };
      result.sameMonthLastYear.dd = { sales: 0, orders: 0 };
    }

    // 더기프트 (XERP mmInoutItem 출고)
    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      const [gToday, gThisMonth, gLastMonth, gSameLY] = await Promise.all([
        queryGiftSales(pool, todayYMD, todayYMD),
        queryGiftSales(pool, monthStart, monthEnd),
        queryGiftSales(pool, lmStart, lmEnd),
        queryGiftSales(pool, sylyStart, sylyEnd)
      ]);
      let gTodayFinal = gToday;
      if (gToday.order_count === 0) {
        const yesterday = new Date(now); yesterday.setDate(yesterday.getDate() - 1);
        gTodayFinal = await queryGiftSales(pool, toYMD(yesterday), toYMD(yesterday));
      }
      result.today.gift = { sales: gTodayFinal.total_sales, orders: gTodayFinal.order_count, qty: gTodayFinal.total_qty };
      result.thisMonth.gift = { sales: gThisMonth.total_sales, orders: gThisMonth.order_count, qty: gThisMonth.total_qty };
      result.lastMonth.gift = { sales: gLastMonth.total_sales, orders: gLastMonth.order_count, qty: gLastMonth.total_qty };
      result.sameMonthLastYear.gift = { sales: gSameLY.total_sales, orders: gSameLY.order_count, qty: gSameLY.total_qty };
      result.sources.gift = 'connected';
    } catch (e) {
      console.error('Sales KPI Gift error:', e.message);
      logError('warn', 'Sales KPI Gift: ' + e.message, e.stack, req.url, req.method);
      result.sources.gift = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
      result.today.gift = { sales: 0, orders: 0, qty: 0 };
      result.thisMonth.gift = { sales: 0, orders: 0, qty: 0 };
      result.lastMonth.gift = { sales: 0, orders: 0, qty: 0 };
      result.sameMonthLastYear.gift = { sales: 0, orders: 0, qty: 0 };
    }

    // 합산 + MoM/YoY (XERP + DD + 더기프트)
    result.today.total = { sales: (result.today.xerp.sales || 0) + (result.today.dd.sales || 0) + (result.today.gift.sales || 0), orders: (result.today.xerp.orders || 0) + (result.today.dd.orders || 0) + (result.today.gift.orders || 0) };
    result.thisMonth.total = { sales: (result.thisMonth.xerp.sales || 0) + (result.thisMonth.dd.sales || 0) + (result.thisMonth.gift.sales || 0), orders: (result.thisMonth.xerp.orders || 0) + (result.thisMonth.dd.orders || 0) + (result.thisMonth.gift.orders || 0) };
    result.lastMonth.total = { sales: (result.lastMonth.xerp.sales || 0) + (result.lastMonth.dd.sales || 0) + (result.lastMonth.gift.sales || 0), orders: (result.lastMonth.xerp.orders || 0) + (result.lastMonth.dd.orders || 0) + (result.lastMonth.gift.orders || 0) };
    result.sameMonthLastYear.total = { sales: (result.sameMonthLastYear.xerp.sales || 0) + (result.sameMonthLastYear.dd.sales || 0) + (result.sameMonthLastYear.gift.sales || 0), orders: (result.sameMonthLastYear.xerp.orders || 0) + (result.sameMonthLastYear.dd.orders || 0) + (result.sameMonthLastYear.gift.orders || 0) };
    const tmSales = result.thisMonth.total.sales, lmSales = result.lastMonth.total.sales;
    const sylySales = result.sameMonthLastYear.total.sales;
    result.momChange = { salesPct: lmSales > 0 ? Math.round((tmSales - lmSales) / lmSales * 1000) / 10 : 0, salesDiff: tmSales - lmSales };
    result.yoyChange = { salesPct: sylySales > 0 ? Math.round((tmSales - sylySales) / sylySales * 1000) / 10 : 0, salesDiff: tmSales - sylySales };
    // 일평균
    const daysInMonth = now.getDate();
    result.dailyAvg = daysInMonth > 0 ? Math.round(tmSales / daysInMonth) : 0;
    result.cachedAt = new Date().toISOString();
    salesKpiCache = result; salesKpiCacheTime = Date.now();
    ok(res, result); return;
  }

  // ── GET /api/sales/daily ──
  if (pathname === '/api/sales/daily' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    const source = parsed.searchParams.get('source') || 'all';
    if (!startParam.match(/^\d{8}$/) || !endParam.match(/^\d{8}$/)) { fail(res, 400, 'start/end는 YYYYMMDD 형식'); return; }
    if (parseInt(endParam) - parseInt(startParam) > 3660000) { fail(res, 400, '최대 366일 범위'); return; }

    const dateMap = {};
    const result = { rows: [], summary: {}, sources: {} };

    // XERP
    if (source === 'all' || source === 'xerp') {
      try {
        const pool = await ensureXerpPool();
        if (!pool) throw new Error('XERP pool unavailable');
        const chunks = getMonthChunks(startParam, endParam);
        for (const chunk of chunks) {
          const r = await pool.request()
            .input('s', sql.NVarChar(16), chunk.start)
            .input('e', sql.NVarChar(16), chunk.end)
            .query(`SELECT h_date,
                           COUNT(DISTINCT h_orderid) AS order_count,
                           ISNULL(SUM(h_sumPrice),0) AS total_sales,
                           ISNULL(SUM(h_offerPrice),0) AS total_supply,
                           ISNULL(SUM(h_superTax),0) AS total_vat,
                           ISNULL(SUM(FeeAmnt),0) AS total_fee
                    FROM ERP_SalesData WITH (NOLOCK)
                    WHERE h_date >= @s AND h_date <= @e
                    GROUP BY h_date ORDER BY h_date`);
          for (const row of r.recordset) {
            const d = (row.h_date || '').trim();
            if (!d) continue;
            const isoDate = d.slice(0, 4) + '-' + d.slice(4, 6) + '-' + d.slice(6, 8);
            if (!dateMap[isoDate]) dateMap[isoDate] = { date: isoDate, xerp_sales: 0, xerp_orders: 0, xerp_supply: 0, xerp_vat: 0, xerp_fee: 0, dd_sales: 0, dd_orders: 0, gift_sales: 0, gift_orders: 0, gift_qty: 0 };
            dateMap[isoDate].xerp_sales = Number(row.total_sales);
            dateMap[isoDate].xerp_orders = row.order_count;
            dateMap[isoDate].xerp_supply = Number(row.total_supply);
            dateMap[isoDate].xerp_vat = Number(row.total_vat);
            dateMap[isoDate].xerp_fee = Number(row.total_fee);
          }
        }
        result.sources.xerp = 'connected';
      } catch (e) {
        console.error('Sales daily XERP error:', e.message);
        logError('warn', 'Sales daily XERP: ' + e.message, e.stack, req.url, req.method);
        result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
      }
    }

    // DD
    if (source === 'all' || source === 'dd') {
      try {
        const pool = await ensureDdPool();
        if (!pool) throw new Error('DD pool unavailable');
        const startISO = startParam.slice(0, 4) + '-' + startParam.slice(4, 6) + '-' + startParam.slice(6, 8);
        const endD = new Date(parseInt(endParam.slice(0, 4)), parseInt(endParam.slice(4, 6)) - 1, parseInt(endParam.slice(6, 8)) + 1);
        const endISO = endD.toISOString().slice(0, 10);
        const [rows] = await pool.query(
          `SELECT DATE(created_at) AS sale_date, COUNT(*) AS order_count, IFNULL(SUM(paid_money),0) AS total_sales
           FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C'
           GROUP BY DATE(created_at) ORDER BY sale_date`, [startISO, endISO]);
        for (const row of rows) {
          const d = typeof row.sale_date === 'string' ? row.sale_date : row.sale_date.toISOString().slice(0, 10);
          if (!dateMap[d]) dateMap[d] = { date: d, xerp_sales: 0, xerp_orders: 0, xerp_supply: 0, xerp_vat: 0, xerp_fee: 0, dd_sales: 0, dd_orders: 0, gift_sales: 0, gift_orders: 0, gift_qty: 0 };
          dateMap[d].dd_sales = Number(row.total_sales);
          dateMap[d].dd_orders = row.order_count;
        }
        result.sources.dd = 'connected';
      } catch (e) {
        console.error('Sales daily DD error:', e.message);
        logError('warn', 'Sales daily DD: ' + e.message, e.stack, req.url, req.method);
        result.sources.dd = 'error';
      }
    }

    // 더기프트
    if (source === 'all' || source === 'gift') {
      try {
        const pool = await ensureXerpPool();
        if (!pool) throw new Error('XERP pool unavailable');
        const gRows = await queryGiftDailySales(pool, startParam, endParam);
        for (const row of gRows) {
          const d = row.date;
          if (!d) continue;
          const isoDate = d.slice(0, 4) + '-' + d.slice(4, 6) + '-' + d.slice(6, 8);
          if (!dateMap[isoDate]) dateMap[isoDate] = { date: isoDate, xerp_sales: 0, xerp_orders: 0, xerp_supply: 0, xerp_vat: 0, xerp_fee: 0, dd_sales: 0, dd_orders: 0, gift_sales: 0, gift_orders: 0, gift_qty: 0 };
          dateMap[isoDate].gift_sales = row.sales;
          dateMap[isoDate].gift_orders = row.orders;
          dateMap[isoDate].gift_qty = row.qty;
        }
        result.sources.gift = 'connected';
      } catch (e) {
        console.error('Sales daily Gift error:', e.message);
        logError('warn', 'Sales daily Gift: ' + e.message, e.stack, req.url, req.method);
        result.sources.gift = 'error';
      }
    }

    // 합산 + 정렬
    result.rows = Object.values(dateMap).sort((a, b) => a.date.localeCompare(b.date));
    result.rows.forEach(r => { r.total_sales = (r.xerp_sales||0) + (r.dd_sales||0) + (r.gift_sales||0); r.total_orders = (r.xerp_orders||0) + (r.dd_orders||0) + (r.gift_orders||0); });
    const totalSales = result.rows.reduce((s, r) => s + r.total_sales, 0);
    const totalOrders = result.rows.reduce((s, r) => s + r.total_orders, 0);
    result.summary = { total_sales: totalSales, total_orders: totalOrders, avg_daily_sales: result.rows.length > 0 ? Math.round(totalSales / result.rows.length) : 0, days: result.rows.length };
    result.cachedAt = new Date().toISOString();
    ok(res, result); return;
  }

  // ── GET /api/sales/monthly ──
  if (pathname === '/api/sales/monthly' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const months = parseInt(parsed.searchParams.get('months') || '12');
    const source = parsed.searchParams.get('source') || 'all';
    const now = new Date();
    const startDate = new Date(now.getFullYear(), now.getMonth() - months + 1, 1);
    const startYMD = toYMD(startDate);
    const endYMD = toYMD(now);
    const monthMap = {};
    const result = { rows: [], sources: {} };

    if (source === 'all' || source === 'xerp') {
      try {
        const pool = await ensureXerpPool();
        if (!pool) throw new Error('XERP pool unavailable');
        const chunks = getMonthChunks(startYMD, endYMD);
        for (const chunk of chunks) {
          const r = await pool.request()
            .input('s', sql.NVarChar(16), chunk.start)
            .input('e', sql.NVarChar(16), chunk.end)
            .query(`SELECT LEFT(h_date,6) AS sale_month,
                           COUNT(DISTINCT h_orderid) AS order_count,
                           ISNULL(SUM(h_sumPrice),0) AS total_sales,
                           ISNULL(SUM(h_offerPrice),0) AS total_supply,
                           ISNULL(SUM(h_superTax),0) AS total_vat,
                           ISNULL(SUM(FeeAmnt),0) AS total_fee
                    FROM ERP_SalesData WITH (NOLOCK)
                    WHERE h_date >= @s AND h_date <= @e
                    GROUP BY LEFT(h_date,6) ORDER BY sale_month`);
          for (const row of r.recordset) {
            const m = (row.sale_month || '').trim();
            if (!m) continue;
            const key = m.slice(0, 4) + '-' + m.slice(4, 6);
            if (!monthMap[key]) monthMap[key] = { month: key, xerp_sales: 0, xerp_orders: 0, xerp_supply: 0, xerp_vat: 0, xerp_fee: 0, dd_sales: 0, dd_orders: 0, gift_sales: 0, gift_orders: 0, gift_qty: 0 };
            monthMap[key].xerp_sales += Number(row.total_sales);
            monthMap[key].xerp_orders += row.order_count;
            monthMap[key].xerp_supply += Number(row.total_supply);
            monthMap[key].xerp_vat += Number(row.total_vat);
            monthMap[key].xerp_fee += Number(row.total_fee);
          }
        }
        result.sources.xerp = 'connected';
      } catch (e) {
        console.error('Sales monthly XERP error:', e.message);
        result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
      }
    }

    if (source === 'all' || source === 'dd') {
      try {
        const pool = await ensureDdPool();
        if (!pool) throw new Error('DD pool unavailable');
        const startISO = startDate.toISOString().slice(0, 10);
        const endISO = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1).toISOString().slice(0, 10);
        const [rows] = await pool.query(
          `SELECT DATE_FORMAT(created_at, '%Y-%m') AS sale_month, COUNT(*) AS order_count, IFNULL(SUM(paid_money),0) AS total_sales
           FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C'
           GROUP BY DATE_FORMAT(created_at, '%Y-%m') ORDER BY sale_month`, [startISO, endISO]);
        for (const row of rows) {
          const m = row.sale_month;
          if (!monthMap[m]) monthMap[m] = { month: m, xerp_sales: 0, xerp_orders: 0, xerp_supply: 0, xerp_vat: 0, xerp_fee: 0, dd_sales: 0, dd_orders: 0, gift_sales: 0, gift_orders: 0, gift_qty: 0 };
          monthMap[m].dd_sales = Number(row.total_sales);
          monthMap[m].dd_orders = row.order_count;
        }
        result.sources.dd = 'connected';
      } catch (e) {
        console.error('Sales monthly DD error:', e.message);
        result.sources.dd = 'error';
      }
    }

    // 더기프트
    if (source === 'all' || source === 'gift') {
      try {
        const pool = await ensureXerpPool();
        if (!pool) throw new Error('XERP pool unavailable');
        const giftSets = db.prepare("SELECT xerp_code FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
        const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
        if (xerpCodes.length) {
          const req2 = pool.request();
          req2.input('s', sql.NVarChar(16), startYMD);
          req2.input('e', sql.NVarChar(16), endYMD);
          const ph = xerpCodes.map((c, i) => { req2.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }).join(',');
          const r = await req2.query(`
            SELECT LEFT(RTRIM(InoutDate),6) AS sale_month,
                   COUNT(DISTINCT InoutNo) AS order_count,
                   ISNULL(SUM(InoutAmnt),0) AS total_sales,
                   ISNULL(SUM(InoutQty),0) AS total_qty
            FROM mmInoutItem WITH (NOLOCK)
            WHERE SiteCode='BK10' AND InoutGubun='SO'
              AND InoutDate >= @s AND InoutDate <= @e
              AND RTRIM(ItemCode) IN (${ph})
            GROUP BY LEFT(RTRIM(InoutDate),6) ORDER BY sale_month`);
          for (const row of r.recordset) {
            const m0 = (row.sale_month || '').trim();
            if (!m0) continue;
            const key = m0.slice(0, 4) + '-' + m0.slice(4, 6);
            if (!monthMap[key]) monthMap[key] = { month: key, xerp_sales: 0, xerp_orders: 0, xerp_supply: 0, xerp_vat: 0, xerp_fee: 0, dd_sales: 0, dd_orders: 0, gift_sales: 0, gift_orders: 0, gift_qty: 0 };
            monthMap[key].gift_sales += Number(row.total_sales);
            monthMap[key].gift_orders += row.order_count;
            monthMap[key].gift_qty += Number(row.total_qty);
          }
        }
        result.sources.gift = 'connected';
      } catch (e) {
        console.error('Sales monthly Gift error:', e.message);
        result.sources.gift = 'error';
      }
    }

    result.rows = Object.values(monthMap).sort((a, b) => a.month.localeCompare(b.month));
    result.rows.forEach(r => { r.total_sales = (r.xerp_sales||0) + (r.dd_sales||0) + (r.gift_sales||0); r.total_orders = (r.xerp_orders||0) + (r.dd_orders||0) + (r.gift_orders||0); });
    ok(res, result); return;
  }

  // ── GET /api/sales/by-channel ──
  if (pathname === '/api/sales/by-channel' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    if (!startParam.match(/^\d{8}$/) || !endParam.match(/^\d{8}$/)) { fail(res, 400, 'start/end는 YYYYMMDD 형식'); return; }
    const result = { channels: [], total: {}, sources: {} };
    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      const r = await pool.request()
        .input('s', sql.NVarChar(16), startParam)
        .input('e', sql.NVarChar(16), endParam)
        .query(`SELECT RTRIM(DeptGubun) AS channel,
                       COUNT(DISTINCT h_orderid) AS order_count,
                       ISNULL(SUM(h_sumPrice),0) AS total_sales,
                       ISNULL(SUM(h_offerPrice),0) AS total_supply,
                       ISNULL(SUM(FeeAmnt),0) AS total_fee
                FROM ERP_SalesData WITH (NOLOCK)
                WHERE h_date >= @s AND h_date <= @e
                GROUP BY RTRIM(DeptGubun) ORDER BY SUM(h_sumPrice) DESC`);
      const grandTotal = r.recordset.reduce((s, row) => s + Number(row.total_sales), 0);
      result.channels = r.recordset.map(row => ({
        code: (row.channel || '').trim(),
        name: DEPT_GUBUN_LABELS[(row.channel || '').trim()] || (row.channel || '').trim(),
        orders: row.order_count,
        sales: Number(row.total_sales),
        supply: Number(row.total_supply),
        fee: Number(row.total_fee),
        pct: grandTotal > 0 ? Math.round(Number(row.total_sales) / grandTotal * 1000) / 10 : 0
      }));
      result.total = { orders: r.recordset.reduce((s, row) => s + row.order_count, 0), sales: grandTotal };
      result.sources.xerp = 'connected';
    } catch (e) {
      console.error('Sales by-channel error:', e.message);
      result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
    }
    result.period = { start: startParam, end: endParam };
    ok(res, result); return;
  }

  // ── GET /api/sales/by-product ──
  if (pathname === '/api/sales/by-product' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    const limit = parseInt(parsed.searchParams.get('limit') || '50');
    const source = parsed.searchParams.get('source') || 'all';
    if (!startParam.match(/^\d{8}$/) || !endParam.match(/^\d{8}$/)) { fail(res, 400, 'start/end는 YYYYMMDD 형식'); return; }
    const products = [];
    const result = { products: [], total: {}, sources: {} };

    // XERP
    if (source === 'all' || source === 'xerp') {
      try {
        const pool = await ensureXerpPool();
        if (!pool) throw new Error('XERP pool unavailable');
        const r = await pool.request()
          .input('s', sql.NVarChar(16), startParam)
          .input('e', sql.NVarChar(16), endParam)
          .input('lim', sql.Int, limit)
          .query(`SELECT TOP (@lim) RTRIM(b_goodCode) AS product_code,
                         COUNT(DISTINCT h_orderid) AS order_count,
                         ISNULL(SUM(b_OrderNum),0) AS total_qty,
                         ISNULL(SUM(b_sumPrice),0) AS total_sales
                  FROM ERP_SalesData WITH (NOLOCK)
                  WHERE h_date >= @s AND h_date <= @e
                    AND b_goodCode IS NOT NULL AND LTRIM(RTRIM(b_goodCode)) != ''
                  GROUP BY RTRIM(b_goodCode) ORDER BY SUM(b_sumPrice) DESC`);
        // bar_shop1에서 품목명 매핑
        const codes = r.recordset.map(row => (row.product_code || '').trim()).filter(Boolean);
        let nameMap = {};
        if (codes.length > 0) {
          try {
            nameMap = await withBarShop1Pool(async (bar1) => {
              const map = {};
              for (let i = 0; i < codes.length; i += 500) {
                const batch = codes.slice(i, i + 500);
                const safeCodes = batch.filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => "'" + c + "'").join(',');
                if (!safeCodes) continue;
                const nr = await bar1.request().query(`SELECT RTRIM(Card_Code) AS Card_Code, Card_Name, RTRIM(CardBrand) AS CardBrand FROM S2_Card WHERE RTRIM(Card_Code) IN (${safeCodes})`);
                nr.recordset.forEach(n => { map[(n.Card_Code || '').trim()] = { name: (n.Card_Name || '').trim(), brand: (n.CardBrand || '').trim() }; });
              }
              return map;
            });
          } catch (_) {}
        }
        for (const row of r.recordset) {
          const code = (row.product_code || '').trim();
          const info = nameMap[code] || {};
          products.push({ code, name: info.name || code, brand: BRAND_LABELS[info.brand] || info.brand || '', orders: row.order_count, qty: Number(row.total_qty), sales: Number(row.total_sales), source: 'xerp' });
        }
        result.sources.xerp = 'connected';
        result.sources.bar_shop1 = Object.keys(nameMap).length > 0 ? 'connected' : 'no_data';
      } catch (e) {
        console.error('Sales by-product XERP error:', e.message);
        result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
      }
    }

    // DD
    if (source === 'all' || source === 'dd') {
      try {
        const pool = await ensureDdPool();
        if (!pool) throw new Error('DD pool unavailable');
        const startISO = startParam.slice(0, 4) + '-' + startParam.slice(4, 6) + '-' + startParam.slice(6, 8);
        const endD = new Date(parseInt(endParam.slice(0, 4)), parseInt(endParam.slice(4, 6)) - 1, parseInt(endParam.slice(6, 8)) + 1);
        const endISO = endD.toISOString().slice(0, 10);
        const [rows] = await pool.query(
          `SELECT oi.product_code, oi.product_name, COUNT(DISTINCT oi.order_id) AS order_count,
                  SUM(oi.qty) AS total_qty, SUM(oi.total_money) AS total_sales
           FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id
           WHERE o.created_at >= ? AND o.created_at < ? AND o.order_state != 'C'
             AND oi.product_code IS NOT NULL AND oi.product_code != ''
           GROUP BY oi.product_code, oi.product_name ORDER BY total_sales DESC LIMIT ?`, [startISO, endISO, limit]);
        for (const row of rows) {
          products.push({ code: row.product_code, name: row.product_name || row.product_code, brand: 'DD', orders: row.order_count, qty: Number(row.total_qty), sales: Number(row.total_sales), source: 'dd' });
        }
        result.sources.dd = 'connected';
      } catch (e) {
        console.error('Sales by-product DD error:', e.message);
        result.sources.dd = 'error';
      }
    }

    products.sort((a, b) => b.sales - a.sales);
    result.products = products.slice(0, limit).map((p, i) => ({ ...p, rank: i + 1 }));
    result.total = { count: result.products.length, sales: result.products.reduce((s, p) => s + p.sales, 0) };
    result.period = { start: startParam, end: endParam };
    ok(res, result); return;
  }

  // ── GET /api/sales/by-brand ──
  if (pathname === '/api/sales/by-brand' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    if (!startParam.match(/^\d{8}$/) || !endParam.match(/^\d{8}$/)) { fail(res, 400, 'start/end는 YYYYMMDD 형식'); return; }
    const result = { brands: [], total: {}, sources: {} };
    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      // 모든 제품별 매출
      const r = await pool.request()
        .input('s', sql.NVarChar(16), startParam)
        .input('e', sql.NVarChar(16), endParam)
        .query(`SELECT RTRIM(b_goodCode) AS product_code,
                       COUNT(DISTINCT h_orderid) AS order_count,
                       ISNULL(SUM(b_OrderNum),0) AS total_qty,
                       ISNULL(SUM(b_sumPrice),0) AS total_sales
                FROM ERP_SalesData WITH (NOLOCK)
                WHERE h_date >= @s AND h_date <= @e
                  AND b_goodCode IS NOT NULL AND LTRIM(RTRIM(b_goodCode)) != ''
                GROUP BY RTRIM(b_goodCode)`);
      // bar_shop1에서 브랜드 매핑
      const codes = r.recordset.map(row => (row.product_code || '').trim()).filter(Boolean);
      let brandMap = {};
      if (codes.length > 0) {
        try {
          brandMap = await withBarShop1Pool(async (bar1) => {
            const map = {};
            for (let i = 0; i < codes.length; i += 500) {
              const batch = codes.slice(i, i + 500);
              const safeCodes = batch.filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => "'" + c + "'").join(',');
              if (!safeCodes) continue;
              const nr = await bar1.request().query(`SELECT RTRIM(Card_Code) AS Card_Code, RTRIM(CardBrand) AS CardBrand FROM S2_Card WHERE RTRIM(Card_Code) IN (${safeCodes})`);
              nr.recordset.forEach(n => { map[(n.Card_Code || '').trim()] = (n.CardBrand || '').trim(); });
            }
            return map;
          });
        } catch (_) {}
      }
      // 브랜드별 집계
      const brandAgg = {};
      for (const row of r.recordset) {
        const code = (row.product_code || '').trim();
        const brand = brandMap[code] || '기타';
        if (!brandAgg[brand]) brandAgg[brand] = { brand, orders: 0, qty: 0, sales: 0, products: 0 };
        brandAgg[brand].orders += row.order_count;
        brandAgg[brand].qty += Number(row.total_qty);
        brandAgg[brand].sales += Number(row.total_sales);
        brandAgg[brand].products++;
      }
      const grandTotal = Object.values(brandAgg).reduce((s, b) => s + b.sales, 0);
      result.brands = Object.values(brandAgg)
        .map(b => ({ ...b, brandName: BRAND_LABELS[b.brand] || b.brand, pct: grandTotal > 0 ? Math.round(b.sales / grandTotal * 1000) / 10 : 0 }))
        .sort((a, b) => b.sales - a.sales);
      result.total = { sales: grandTotal };
      result.sources.xerp = 'connected';
      result.sources.bar_shop1 = Object.keys(brandMap).length > 0 ? 'connected' : 'no_data';
    } catch (e) {
      console.error('Sales by-brand error:', e.message);
      result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
    }
    result.period = { start: startParam, end: endParam };
    ok(res, result); return;
  }

  // ── GET /api/sales/trend ──
  if (pathname === '/api/sales/trend' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const months = parseInt(parsed.searchParams.get('months') || '12');
    const now = new Date();
    const result = { thisYear: [], lastYear: [], yoyChanges: [], sources: {} };

    // 올해/작년 범위
    const tyStart = new Date(now.getFullYear(), now.getMonth() - months + 1, 1);
    const lyStart = new Date(now.getFullYear() - 1, now.getMonth() - months + 1, 1);
    const lyEnd = new Date(now.getFullYear() - 1, now.getMonth() + 1, 0);

    // XERP
    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      const tyChunks = getMonthChunks(toYMD(tyStart), toYMD(now));
      const lyChunks = getMonthChunks(toYMD(lyStart), toYMD(lyEnd));
      const tyMap = {}, lyMap = {};
      for (const chunk of tyChunks) {
        const r = await pool.request().input('s', sql.NVarChar(16), chunk.start).input('e', sql.NVarChar(16), chunk.end)
          .query(`SELECT LEFT(h_date,6) AS m, COUNT(DISTINCT h_orderid) AS cnt, ISNULL(SUM(h_sumPrice),0) AS sales FROM ERP_SalesData WITH (NOLOCK) WHERE h_date>=@s AND h_date<=@e GROUP BY LEFT(h_date,6)`);
        for (const row of r.recordset) { const k = row.m.slice(0,4)+'-'+row.m.slice(4,6); tyMap[k] = { sales: Number(row.sales), orders: row.cnt }; }
      }
      for (const chunk of lyChunks) {
        const r = await pool.request().input('s', sql.NVarChar(16), chunk.start).input('e', sql.NVarChar(16), chunk.end)
          .query(`SELECT LEFT(h_date,6) AS m, COUNT(DISTINCT h_orderid) AS cnt, ISNULL(SUM(h_sumPrice),0) AS sales FROM ERP_SalesData WITH (NOLOCK) WHERE h_date>=@s AND h_date<=@e GROUP BY LEFT(h_date,6)`);
        for (const row of r.recordset) { const k = row.m.slice(0,4)+'-'+row.m.slice(4,6); lyMap[k] = { sales: Number(row.sales), orders: row.cnt }; }
      }
      // 정리
      for (let i = 0; i < months; i++) {
        const d = new Date(now.getFullYear(), now.getMonth() - months + 1 + i, 1);
        const tyKey = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
        const lyD = new Date(d.getFullYear() - 1, d.getMonth(), 1);
        const lyKey = lyD.getFullYear() + '-' + String(lyD.getMonth() + 1).padStart(2, '0');
        const tyData = tyMap[tyKey] || { sales: 0, orders: 0 };
        const lyData = lyMap[lyKey] || { sales: 0, orders: 0 };
        result.thisYear.push({ month: tyKey, sales: tyData.sales, orders: tyData.orders });
        result.lastYear.push({ month: lyKey, sales: lyData.sales, orders: lyData.orders });
        result.yoyChanges.push({ monthLabel: String(d.getMonth() + 1).padStart(2, '0'), changePct: lyData.sales > 0 ? Math.round((tyData.sales - lyData.sales) / lyData.sales * 1000) / 10 : 0, changeAmt: tyData.sales - lyData.sales });
      }
      result.sources.xerp = 'connected';
    } catch (e) {
      console.error('Sales trend error:', e.message);
      result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
    }
    ok(res, result); return;
  }

  // ── GET /api/sales/order-status ──
  if (pathname === '/api/sales/order-status' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    const result = { bar_shop1: {}, dd: {}, sources: {} };

    // bar_shop1
    try {
      const data = await withBarShop1Pool(async (bar1) => {
        const [byStatus, bySite, byPay] = await Promise.all([
          bar1.request().input('s', sql.DateTime, startParam).input('e', sql.DateTime, endParam)
            .query(`SELECT status_seq, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date < @e AND status_seq >= 1 GROUP BY status_seq ORDER BY status_seq`),
          bar1.request().input('s', sql.DateTime, startParam).input('e', sql.DateTime, endParam)
            .query(`SELECT RTRIM(site_gubun) AS site_gubun, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date < @e AND status_seq >= 1 GROUP BY RTRIM(site_gubun)`),
          bar1.request().input('s', sql.DateTime, startParam).input('e', sql.DateTime, endParam)
            .query(`SELECT RTRIM(pay_Type) AS pay_type, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date < @e AND status_seq >= 1 GROUP BY RTRIM(pay_Type) ORDER BY COUNT(*) DESC`)
        ]);
        return {
          total: byStatus.recordset.reduce((s, r) => s + r.cnt, 0),
          byStatus: byStatus.recordset.map(r => ({ status_seq: r.status_seq, count: r.cnt })),
          bySite: bySite.recordset.map(r => ({ site_gubun: r.site_gubun, count: r.cnt })),
          byPayType: byPay.recordset.map(r => ({ pay_type: r.pay_type, count: r.cnt }))
        };
      });
      result.bar_shop1 = data;
      result.sources.bar_shop1 = 'connected';
    } catch (e) {
      console.error('Sales order-status bar_shop1 error:', e.message);
      result.sources.bar_shop1 = 'error';
    }

    // DD
    try {
      const pool = await ensureDdPool();
      if (!pool) throw new Error('DD pool unavailable');
      const [[byState], [byShipping]] = await Promise.all([
        pool.query(`SELECT order_state, COUNT(*) AS cnt FROM orders WHERE created_at >= ? AND created_at < ? GROUP BY order_state`, [startParam, endParam]),
        pool.query(`SELECT shipping_state, COUNT(*) AS cnt FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C' GROUP BY shipping_state`, [startParam, endParam])
      ]);
      const stateLabels = { 'B': '대기', 'P': '결제완료', 'D': '배송중', 'C': '취소', 'F': '완료' };
      result.dd = {
        total: byState.reduce((s, r) => s + r.cnt, 0),
        byState: byState.map(r => ({ state: r.order_state, label: stateLabels[r.order_state] || r.order_state, count: r.cnt })),
        byShipping: byShipping.map(r => ({ state: r.shipping_state, label: stateLabels[r.shipping_state] || r.shipping_state, count: r.cnt }))
      };
      result.sources.dd = 'connected';
    } catch (e) {
      console.error('Sales order-status DD error:', e.message);
      result.sources.dd = 'error';
    }
    ok(res, result); return;
  }

  // ── GET /api/sales/dd ──
  if (pathname === '/api/sales/dd' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    const result = { summary: {}, daily: [], topProducts: [], byPayType: [], sources: {} };
    try {
      const pool = await ensureDdPool();
      if (!pool) throw new Error('DD pool unavailable');
      const endNext = endParam ? new Date(new Date(endParam).getTime() + 86400000).toISOString().slice(0, 10) : '';
      const [[summaryRows], [dailyRows], [prodRows], [payRows]] = await Promise.all([
        pool.query(`SELECT COUNT(*) AS total_orders, IFNULL(SUM(total_money),0) AS total_sales,
                    IFNULL(SUM(paid_money),0) AS total_paid, IFNULL(SUM(delivery_price),0) AS total_delivery,
                    IFNULL(SUM(discount_money),0) AS total_discount, ROUND(AVG(paid_money)) AS avg_order
                    FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C'`, [startParam, endNext]),
        pool.query(`SELECT DATE(created_at) AS sale_date, COUNT(*) AS order_count, IFNULL(SUM(paid_money),0) AS total_sales
                    FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C'
                    GROUP BY DATE(created_at) ORDER BY sale_date`, [startParam, endNext]),
        pool.query(`SELECT oi.product_code, oi.product_name, COUNT(DISTINCT oi.order_id) AS order_count,
                    SUM(oi.qty) AS total_qty, SUM(oi.total_money) AS total_sales
                    FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id
                    WHERE o.created_at >= ? AND o.created_at < ? AND o.order_state != 'C'
                      AND oi.product_code IS NOT NULL AND oi.product_code != ''
                    GROUP BY oi.product_code, oi.product_name ORDER BY total_sales DESC LIMIT 20`, [startParam, endNext]),
        pool.query(`SELECT pay_type, pg_name, COUNT(*) AS order_count, IFNULL(SUM(paid_money),0) AS total_sales
                    FROM orders WHERE created_at >= ? AND created_at < ? AND order_state != 'C'
                    GROUP BY pay_type, pg_name ORDER BY total_sales DESC`, [startParam, endNext])
      ]);
      const s = summaryRows[0] || {};
      result.summary = { total_orders: s.total_orders || 0, total_sales: Number(s.total_sales || 0), total_paid: Number(s.total_paid || 0), total_delivery: Number(s.total_delivery || 0), total_discount: Number(s.total_discount || 0), avg_order: Number(s.avg_order || 0) };
      result.daily = dailyRows.map(r => ({ date: typeof r.sale_date === 'string' ? r.sale_date : r.sale_date.toISOString().slice(0, 10), orders: r.order_count, sales: Number(r.total_sales) }));
      result.topProducts = prodRows.map((r, i) => ({ rank: i + 1, code: r.product_code, name: r.product_name || '', orders: r.order_count, qty: Number(r.total_qty), sales: Number(r.total_sales) }));
      result.byPayType = payRows.map(r => ({ pay_type: r.pay_type || '', pg_name: r.pg_name || '', orders: r.order_count, sales: Number(r.total_sales) }));
      result.sources.dd = 'connected';
    } catch (e) {
      console.error('Sales DD error:', e.message);
      result.sources.dd = 'error';
    }
    ok(res, result); return;
  }

  // ── GET /api/sales/barun ──
  if (pathname === '/api/sales/barun' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    if (!startParam.match(/^\d{8}$/) || !endParam.match(/^\d{8}$/)) { fail(res, 400, 'start/end는 YYYYMMDD 형식'); return; }
    const result = { summary: {}, daily: [], channels: [], brands: [], orderPipeline: {}, sources: {} };

    // XERP 요약 + 일별 + 채널별
    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      const [summaryR, dailyR, channelR] = await Promise.all([
        pool.request().input('s', sql.NVarChar(16), startParam).input('e', sql.NVarChar(16), endParam)
          .query(`SELECT COUNT(DISTINCT h_orderid) AS order_count, ISNULL(SUM(h_sumPrice),0) AS total_sales,
                  ISNULL(SUM(h_offerPrice),0) AS total_supply, ISNULL(SUM(h_superTax),0) AS total_vat, ISNULL(SUM(FeeAmnt),0) AS total_fee
                  FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e`),
        pool.request().input('s', sql.NVarChar(16), startParam).input('e', sql.NVarChar(16), endParam)
          .query(`SELECT h_date, COUNT(DISTINCT h_orderid) AS order_count, ISNULL(SUM(h_sumPrice),0) AS total_sales
                  FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e GROUP BY h_date ORDER BY h_date`),
        pool.request().input('s', sql.NVarChar(16), startParam).input('e', sql.NVarChar(16), endParam)
          .query(`SELECT RTRIM(DeptGubun) AS channel, COUNT(DISTINCT h_orderid) AS order_count, ISNULL(SUM(h_sumPrice),0) AS total_sales
                  FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e GROUP BY RTRIM(DeptGubun) ORDER BY SUM(h_sumPrice) DESC`)
      ]);
      const s = summaryR.recordset[0] || {};
      result.summary = { orders: s.order_count || 0, sales: Number(s.total_sales || 0), supply: Number(s.total_supply || 0), vat: Number(s.total_vat || 0), fee: Number(s.total_fee || 0) };
      result.daily = dailyR.recordset.map(r => { const d = (r.h_date||'').trim(); return { date: d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8), orders: r.order_count, sales: Number(r.total_sales) }; });
      const grandTotal = channelR.recordset.reduce((s, r) => s + Number(r.total_sales), 0);
      result.channels = channelR.recordset.map(r => ({ code: (r.channel||'').trim(), name: DEPT_GUBUN_LABELS[(r.channel||'').trim()] || r.channel, orders: r.order_count, sales: Number(r.total_sales), pct: grandTotal > 0 ? Math.round(Number(r.total_sales) / grandTotal * 1000) / 10 : 0 }));
      result.sources.xerp = 'connected';
    } catch (e) {
      console.error('Sales barun XERP error:', e.message);
      result.sources.xerp = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
    }

    // bar_shop1 주문 파이프라인
    try {
      const startISO = startParam.slice(0,4)+'-'+startParam.slice(4,6)+'-'+startParam.slice(6,8);
      const endISO = endParam.slice(0,4)+'-'+endParam.slice(4,6)+'-'+endParam.slice(6,8);
      result.orderPipeline = await withBarShop1Pool(async (bar1) => {
        const [bySite, byPay] = await Promise.all([
          bar1.request().input('s', sql.DateTime, startISO).input('e', sql.DateTime, endISO)
            .query(`SELECT RTRIM(site_gubun) AS site_gubun, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date < DATEADD(day,1,@e) AND status_seq >= 1 GROUP BY RTRIM(site_gubun)`),
          bar1.request().input('s', sql.DateTime, startISO).input('e', sql.DateTime, endISO)
            .query(`SELECT RTRIM(pay_Type) AS pay_type, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date < DATEADD(day,1,@e) AND status_seq >= 1 GROUP BY RTRIM(pay_Type) ORDER BY COUNT(*) DESC`)
        ]);
        return { bySite: bySite.recordset.map(r => ({ site: r.site_gubun, count: r.cnt })), byPayType: byPay.recordset.map(r => ({ type: r.pay_type, count: r.cnt })) };
      });
      result.sources.bar_shop1 = 'connected';
    } catch (e) {
      console.error('Sales barun bar_shop1 error:', e.message);
      result.sources.bar_shop1 = 'error';
    }
    result.period = { start: startParam, end: endParam };
    ok(res, result); return;
  }

  // ── GET /api/sales/gift ── (더기프트 전용)
  if (pathname === '/api/sales/gift' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    if (!startParam.match(/^\d{8}$/) || !endParam.match(/^\d{8}$/)) { fail(res, 400, 'start/end는 YYYYMMDD 형식'); return; }
    const result = { summary: {}, daily: [], products: [], sources: {} };

    try {
      const pool = await ensureXerpPool();
      if (!pool) throw new Error('XERP pool unavailable');
      const [summaryData, dailyData, productData] = await Promise.all([
        queryGiftSales(pool, startParam, endParam),
        queryGiftDailySales(pool, startParam, endParam),
        queryGiftProductSales(pool, startParam, endParam)
      ]);
      result.summary = {
        total_sales: summaryData.total_sales,
        total_orders: summaryData.order_count,
        total_qty: summaryData.total_qty,
        total_items: summaryData.items,
        avg_order: summaryData.order_count > 0 ? Math.round(summaryData.total_sales / summaryData.order_count) : 0
      };
      result.daily = dailyData.map(r => ({
        date: r.date.slice(0,4)+'-'+r.date.slice(4,6)+'-'+r.date.slice(6,8),
        sales: r.sales, orders: r.orders, qty: r.qty
      }));
      result.products = productData;
      result.sources.gift = 'connected';
    } catch (e) {
      console.error('Sales gift error:', e.message);
      logError('warn', 'Sales gift: ' + e.message, e.stack, req.url, req.method);
      result.sources.gift = (e.message.includes('permission') || e.message.includes('denied')) ? 'access_denied' : 'error';
    }
    result.period = { start: startParam, end: endParam };
    ok(res, result); return;
  }

  // ── POST /api/sales/cache/refresh ──
  if (pathname === '/api/sales/cache/refresh' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    salesKpiCache = null; salesKpiCacheTime = 0;
    const d1 = db.prepare("DELETE FROM sales_daily_cache WHERE sale_date >= date('now', '-7 days')").run();
    const d2 = db.prepare("DELETE FROM sales_monthly_cache WHERE sale_month >= strftime('%Y-%m', 'now', '-2 months')").run();
    const d3 = db.prepare("DELETE FROM sales_product_cache WHERE sale_month >= strftime('%Y-%m', 'now', '-2 months')").run();
    ok(res, { message: '매출 캐시 초기화 완료', deleted: { daily: d1.changes, monthly: d2.changes, product: d3.changes } }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  경영대시보드 API (Executive Dashboard)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/exec/summary — 경영 종합 KPI
  if (pathname === '/api/exec/summary' && method === 'GET') {
    const sources = { xerp: 'unknown', bar_shop1: 'unknown', sqlite: 'ok' };
    const now = new Date();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const fmtDate = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
    const s = fmtDate(monthStart), e = fmtDate(now);

    let salesData = { total_sales: 0, total_supply: 0, total_fee: 0, order_count: 0, channels: [] };
    let barData = { total_revenue: 0, total_cost: 0, order_count: 0 };
    let prevMonthSales = 0;

    // XERP: 이번달 매출
    try {
      const xerpPool = await ensureXerpPool();
      const r = await xerpPool.request()
        .input('s', s).input('e', e)
        .query(`SELECT RTRIM(DeptGubun) AS channel, COUNT(DISTINCT h_orderid) AS order_count,
                ISNULL(SUM(h_sumPrice),0) AS total_sales, ISNULL(SUM(h_offerPrice),0) AS total_supply,
                ISNULL(SUM(FeeAmnt),0) AS total_fee
                FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e GROUP BY RTRIM(DeptGubun)`);
      salesData.channels = r.recordset || [];
      salesData.channels.forEach(c => { salesData.total_sales += c.total_sales; salesData.total_supply += c.total_supply; salesData.total_fee += c.total_fee; salesData.order_count += c.order_count; });
      // 전월 매출
      const pm = new Date(now.getFullYear(), now.getMonth()-1, 1);
      const pmEnd = new Date(now.getFullYear(), now.getMonth(), 0);
      const r2 = await xerpPool.request()
        .input('s', fmtDate(pm)).input('e', fmtDate(pmEnd))
        .query(`SELECT ISNULL(SUM(h_sumPrice),0) AS total FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e`);
      prevMonthSales = (r2.recordset[0] || {}).total || 0;
      sources.xerp = 'ok';
    } catch(e) { sources.xerp = 'error'; }

    // bar_shop1: 이번달 매출/원가
    try {
      await withBarShop1Pool(async (pool) => {
        const r = await pool.request()
          .input('s', s).input('e', e + ' 23:59:59')
          .query(`SELECT SUM(i.item_sale_price * i.item_count) AS total_revenue,
                  SUM(i.item_price * i.item_count) AS total_cost, COUNT(DISTINCT o.order_seq) AS order_count
                  FROM custom_order o WITH (NOLOCK) JOIN custom_order_item i WITH (NOLOCK) ON o.order_seq = i.order_seq
                  WHERE o.order_date >= @s AND o.order_date <= @e AND o.status_seq >= 1 AND i.item_sale_price > 0 AND i.item_price > 0`);
        const row = r.recordset[0] || {};
        barData.total_revenue = row.total_revenue || 0;
        barData.total_cost = row.total_cost || 0;
        barData.order_count = row.order_count || 0;
      });
      sources.bar_shop1 = 'ok';
    } catch(e) { sources.bar_shop1 = 'error'; }

    // SQLite: 업무/불량/후공정
    let taskStats = { total: 0, done: 0, in_progress: 0 };
    let defectCount = 0;
    let postProcessTotal = 0;
    try {
      const ts = db.prepare("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status").all();
      ts.forEach(t => { taskStats.total += t.cnt; if(t.status==='done') taskStats.done=t.cnt; if(t.status==='in_progress') taskStats.in_progress=t.cnt; });
      const dc = db.prepare("SELECT COUNT(*) as cnt FROM defects WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10));
      defectCount = dc ? dc.cnt : 0;
      try { const pp = db.prepare("SELECT COALESCE(SUM(amount),0) as total FROM post_process_history WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10)); postProcessTotal = pp ? pp.total : 0; } catch(e){}
    } catch(e) {}

    // PO 현황
    let poStats = { total: 0, pending: 0 };
    try {
      const pc = db.prepare("SELECT COUNT(*) as cnt FROM purchase_orders WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10));
      poStats.total = pc ? pc.cnt : 0;
      const pp = db.prepare("SELECT COUNT(*) as cnt FROM purchase_orders WHERE status IN ('pending','ordered','partial')").get();
      poStats.pending = pp ? pp.cnt : 0;
    } catch(e) {}

    const totalSales = salesData.total_sales + barData.total_revenue;
    const totalCost = barData.total_cost + salesData.total_fee + postProcessTotal;
    const grossProfit = totalSales - totalCost;
    const marginRate = totalSales > 0 ? (grossProfit / totalSales * 100) : 0;
    const salesGrowth = prevMonthSales > 0 ? ((salesData.total_sales - prevMonthSales) / prevMonthSales * 100) : 0;

    ok(res, {
      sources,
      period: { start: s, end: e, month: (now.getMonth()+1) + '월' },
      kpi: {
        total_sales: totalSales,
        gross_profit: grossProfit,
        margin_rate: Math.round(marginRate * 10) / 10,
        sales_growth: Math.round(salesGrowth * 10) / 10,
        order_count: salesData.order_count + barData.order_count,
        total_fee: salesData.total_fee,
        defect_count: defectCount,
        po_pending: poStats.pending
      },
      channels: salesData.channels,
      bar_shop1: barData,
      tasks: taskStats,
      po: poStats
    });
    return;
  }

  // GET /api/exec/trend — 경영 월별 추이 (최근 12개월)
  if (pathname === '/api/exec/trend' && method === 'GET') {
    const months = parseInt(parsed.searchParams.get('months') || '12');
    const sources = { xerp: 'unknown', bar_shop1: 'unknown' };
    const now = new Date();
    const result = [];

    try {
      const xerpPool = await ensureXerpPool();
      const startDate = new Date(now.getFullYear(), now.getMonth() - months + 1, 1);
      const fmtD = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');

      // 월별 청크
      for (let m = 0; m < months; m++) {
        const md = new Date(startDate.getFullYear(), startDate.getMonth() + m, 1);
        const mEnd = new Date(md.getFullYear(), md.getMonth() + 1, 0);
        const label = md.getFullYear() + '-' + String(md.getMonth()+1).padStart(2,'0');
        let sales = 0, fee = 0, supply = 0, orders = 0;

        try {
          const r = await xerpPool.request()
            .input('s', fmtD(md)).input('e', fmtD(mEnd))
            .query(`SELECT ISNULL(SUM(h_sumPrice),0) AS sales, ISNULL(SUM(FeeAmnt),0) AS fee,
                    ISNULL(SUM(h_offerPrice),0) AS supply, COUNT(DISTINCT h_orderid) AS orders
                    FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e`);
          const row = r.recordset[0] || {};
          sales = row.sales || 0; fee = row.fee || 0; supply = row.supply || 0; orders = row.orders || 0;
        } catch(e) {}

        result.push({ month: label, sales, fee, supply, orders, margin: sales - fee });
      }
      sources.xerp = 'ok';
    } catch(e) { sources.xerp = 'error'; }

    ok(res, { sources, trend: result });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  고객주문 API (Customer Orders)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/customer-orders/summary — 주문 현황 KPI
  if (pathname === '/api/customer-orders/summary' && method === 'GET') {
    const sources = { xerp: 'unknown', bar_shop1: 'unknown', dd: 'unknown', sqlite: 'ok' };
    const start = parsed.searchParams.get('start') || '';
    const end = parsed.searchParams.get('end') || '';
    const now = new Date();
    const s = start || (now.getFullYear() + String(now.getMonth()+1).padStart(2,'0') + '01');
    const e = end || (now.getFullYear() + String(now.getMonth()+1).padStart(2,'0') + String(now.getDate()).padStart(2,'0'));

    let xerpSummary = { order_count: 0, total_sales: 0, channels: [] };
    let barSummary = { order_count: 0, total_revenue: 0, status: [], sites: [] };
    let ddSummary = { order_count: 0, states: [] };

    // XERP
    try {
      const xerpPool = await ensureXerpPool();
      const r = await xerpPool.request().input('s', s).input('e', e)
        .query(`SELECT COUNT(DISTINCT h_orderid) AS order_count, ISNULL(SUM(h_sumPrice),0) AS total_sales,
                ISNULL(SUM(FeeAmnt),0) AS total_fee
                FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e`);
      const row = r.recordset[0] || {};
      xerpSummary.order_count = row.order_count || 0;
      xerpSummary.total_sales = row.total_sales || 0;
      xerpSummary.total_fee = row.total_fee || 0;
      // 채널별
      const r2 = await xerpPool.request().input('s', s).input('e', e)
        .query(`SELECT RTRIM(DeptGubun) AS channel, COUNT(DISTINCT h_orderid) AS cnt, ISNULL(SUM(h_sumPrice),0) AS sales
                FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e GROUP BY RTRIM(DeptGubun) ORDER BY sales DESC`);
      xerpSummary.channels = (r2.recordset || []).map(r => ({ channel: DEPT_GUBUN_LABELS[r.channel] || r.channel || '기타', count: r.cnt, sales: r.sales }));
      sources.xerp = 'ok';
    } catch(ex) { sources.xerp = 'error'; }

    // bar_shop1
    try {
      await withBarShop1Pool(async (pool) => {
        const r = await pool.request().input('s', s).input('e', e + ' 23:59:59')
          .query(`SELECT COUNT(*) AS order_count, ISNULL(SUM(total_price),0) AS total_revenue
                  FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date <= @e AND status_seq >= 1`);
        const row = r.recordset[0] || {};
        barSummary.order_count = row.order_count || 0;
        barSummary.total_revenue = row.total_revenue || 0;
        // 상태별
        const r2 = await pool.request().input('s', s).input('e', e + ' 23:59:59')
          .query(`SELECT status_seq, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date <= @e AND status_seq >= 1 GROUP BY status_seq`);
        barSummary.status = r2.recordset || [];
        // 사이트별
        const r3 = await pool.request().input('s', s).input('e', e + ' 23:59:59')
          .query(`SELECT RTRIM(site_gubun) AS site, COUNT(*) AS cnt FROM custom_order WITH (NOLOCK) WHERE order_date >= @s AND order_date <= @e AND status_seq >= 1 GROUP BY RTRIM(site_gubun) ORDER BY cnt DESC`);
        barSummary.sites = r3.recordset || [];
      });
      sources.bar_shop1 = 'ok';
    } catch(ex) { sources.bar_shop1 = 'error'; }

    // DD
    try {
      if (typeof ddPool !== 'undefined' && ddPool) {
        const sDate = s.substring(0,4)+'-'+s.substring(4,6)+'-'+s.substring(6,8);
        const eDate = e.substring(0,4)+'-'+e.substring(4,6)+'-'+e.substring(6,8);
        const [rows] = await ddPool.query('SELECT order_state, COUNT(*) AS cnt FROM orders WHERE created_at >= ? AND created_at < DATE_ADD(?, INTERVAL 1 DAY) GROUP BY order_state', [sDate, eDate]);
        ddSummary.states = rows || [];
        ddSummary.order_count = rows.reduce((a,b)=>a+(b.cnt||0),0);
        sources.dd = 'ok';
      } else { sources.dd = 'unavailable'; }
    } catch(ex) { sources.dd = 'error'; }

    const totalOrders = xerpSummary.order_count + barSummary.order_count + ddSummary.order_count;
    const totalSales = xerpSummary.total_sales + barSummary.total_revenue;

    ok(res, {
      sources, period: { start: s, end: e },
      kpi: { total_orders: totalOrders, total_sales: totalSales, xerp_orders: xerpSummary.order_count, bar_orders: barSummary.order_count, dd_orders: ddSummary.order_count },
      xerp: xerpSummary, bar_shop1: barSummary, dd: ddSummary
    });
    return;
  }

  // GET /api/customer-orders/list — 주문 목록 (XERP 기반)
  if (pathname === '/api/customer-orders/list' && method === 'GET') {
    const sources = { xerp: 'unknown' };
    const start = parsed.searchParams.get('start') || '';
    const end = parsed.searchParams.get('end') || '';
    const channel = parsed.searchParams.get('channel') || '';
    const search = parsed.searchParams.get('q') || '';
    const limit = Math.min(parseInt(parsed.searchParams.get('limit') || '100'), 500);
    const offset = parseInt(parsed.searchParams.get('offset') || '0');

    const now = new Date();
    const s = start || (now.getFullYear() + String(now.getMonth()+1).padStart(2,'0') + '01');
    const e = end || (now.getFullYear() + String(now.getMonth()+1).padStart(2,'0') + String(now.getDate()).padStart(2,'0'));

    let rows = [], total = 0;
    try {
      const xerpPool = await ensureXerpPool();
      let where = "h_date >= @s AND h_date <= @e";
      const request = xerpPool.request().input('s', s).input('e', e);
      if (channel) { where += " AND RTRIM(DeptGubun) = @ch"; request.input('ch', channel); }
      if (search) { where += " AND (h_orderid LIKE @q OR b_goodCode LIKE @q)"; request.input('q', '%'+search+'%'); }

      const countR = await request.query(`SELECT COUNT(*) AS cnt FROM ERP_SalesData WITH (NOLOCK) WHERE ${where}`);
      total = (countR.recordset[0] || {}).cnt || 0;

      const request2 = xerpPool.request().input('s', s).input('e', e).input('lim', limit).input('off', offset);
      if (channel) request2.input('ch', channel);
      if (search) request2.input('q', '%'+search+'%');

      const r = await request2.query(`SELECT TOP (@lim) h_orderid, h_date, RTRIM(DeptGubun) AS channel,
        RTRIM(b_goodCode) AS product_code, b_OrderNum AS qty,
        h_sumPrice AS sales, h_offerPrice AS supply, FeeAmnt AS fee, b_sumPrice AS product_sales
        FROM ERP_SalesData WITH (NOLOCK) WHERE ${where}
        ORDER BY h_date DESC, h_orderid DESC`);
      rows = (r.recordset || []).map(r => ({
        ...r, channel_name: DEPT_GUBUN_LABELS[r.channel] || r.channel || '기타'
      }));
      sources.xerp = 'ok';
    } catch(ex) { sources.xerp = 'error'; }

    ok(res, { sources, rows, total, limit, offset });
    return;
  }

  // GET /api/customer-orders/bar-list — bar_shop1 주문 목록
  if (pathname === '/api/customer-orders/bar-list' && method === 'GET') {
    const sources = { bar_shop1: 'unknown' };
    const start = parsed.searchParams.get('start') || '';
    const end = parsed.searchParams.get('end') || '';
    const status = parsed.searchParams.get('status') || '';
    const limit = Math.min(parseInt(parsed.searchParams.get('limit') || '100'), 500);

    const now = new Date();
    const s = start || (now.getFullYear() + '-' + String(now.getMonth()+1).padStart(2,'0') + '-01');
    const e = end || now.toISOString().slice(0,10);

    let rows = [];
    try {
      await withBarShop1Pool(async (pool) => {
        let where = "o.order_date >= @s AND o.order_date <= @e AND o.status_seq >= 1";
        const request = pool.request().input('s', s).input('e', e + ' 23:59:59');
        if (status) { where += " AND o.status_seq = @st"; request.input('st', parseInt(status)); }

        const r = await request.query(`SELECT TOP ${limit} o.order_seq, o.order_date, o.total_price,
          o.status_seq, RTRIM(o.site_gubun) AS site, RTRIM(o.pay_Type) AS pay_type,
          (SELECT COUNT(*) FROM custom_order_item i WITH (NOLOCK) WHERE i.order_seq = o.order_seq) AS item_count
          FROM custom_order o WITH (NOLOCK) WHERE ${where} ORDER BY o.order_date DESC`);
        rows = r.recordset || [];
      });
      sources.bar_shop1 = 'ok';
    } catch(ex) { sources.bar_shop1 = 'error'; }

    ok(res, { sources, rows });
    return;
  }

  // GET /api/customer-orders/daily — 일별 주문 추이
  if (pathname === '/api/customer-orders/daily' && method === 'GET') {
    const sources = { xerp: 'unknown' };
    const days = parseInt(parsed.searchParams.get('days') || '30');
    const now = new Date();
    const startDate = new Date(now);
    startDate.setDate(startDate.getDate() - days);
    const fmtD = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');

    let daily = [];
    try {
      const xerpPool = await ensureXerpPool();
      const r = await xerpPool.request()
        .input('s', fmtD(startDate)).input('e', fmtD(now))
        .query(`SELECT h_date AS day, COUNT(DISTINCT h_orderid) AS orders, ISNULL(SUM(h_sumPrice),0) AS sales
                FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e
                GROUP BY h_date ORDER BY h_date`);
      daily = (r.recordset || []).map(d => ({ day: d.day, orders: d.orders, sales: d.sales }));
      sources.xerp = 'ok';
    } catch(ex) { sources.xerp = 'error'; }

    ok(res, { sources, daily });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  배송추적 API (Shipping Tracking)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/shipping/summary — 배송 현황 KPI
  if (pathname === '/api/shipping/summary' && method === 'GET') {
    const sources = { bar_shop1: 'unknown', dd: 'unknown' };
    const now = new Date();
    const fmtD = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
    const s = parsed.searchParams.get('start') || fmtD(new Date(now.getFullYear(), now.getMonth(), 1));
    const e = parsed.searchParams.get('end') || fmtD(now);

    let barShipping = { total: 0, by_status: [], recent: [] };
    let ddShipping = { total: 0, by_state: [] };

    // bar_shop1: 주문 상태별 + 최근 배송
    try {
      await withBarShop1Pool(async (pool) => {
        // 상태별 집계
        const r1 = await pool.request().input('s', s).input('e', e + ' 23:59:59')
          .query(`SELECT o.status_seq, COUNT(*) AS cnt
                  FROM custom_order o WITH (NOLOCK)
                  WHERE o.order_date >= @s AND o.order_date <= @e AND o.status_seq >= 1
                  GROUP BY o.status_seq ORDER BY o.status_seq`);
        barShipping.by_status = r1.recordset || [];
        barShipping.total = barShipping.by_status.reduce((a,b) => a + b.cnt, 0);

        // 최근 배송 건
        const r2 = await pool.request().input('s', s).input('e', e + ' 23:59:59')
          .query(`SELECT TOP 50 o.order_seq, o.order_date, o.status_seq, o.total_price,
                  RTRIM(o.site_gubun) AS site,
                  d.NAME AS recipient, d.ADDR AS address
                  FROM custom_order o WITH (NOLOCK)
                  LEFT JOIN DELIVERY_INFO d WITH (NOLOCK) ON o.order_seq = d.ORDER_SEQ AND d.DELIVERY_SEQ = 1
                  WHERE o.order_date >= @s AND o.order_date <= @e AND o.status_seq >= 3
                  ORDER BY o.order_date DESC`);
        barShipping.recent = r2.recordset || [];
      });
      sources.bar_shop1 = 'ok';
    } catch(ex) { sources.bar_shop1 = 'error'; }

    // DD: 배송 상태별
    try {
      if (typeof ddPool !== 'undefined' && ddPool) {
        const sDate = s.substring(0,4)+'-'+s.substring(4,6)+'-'+s.substring(6,8);
        const eDate = e.substring(0,4)+'-'+e.substring(4,6)+'-'+e.substring(6,8);
        const [rows] = await ddPool.query(`SELECT shipping_state, COUNT(*) AS cnt FROM orders
          WHERE created_at >= ? AND created_at < DATE_ADD(?, INTERVAL 1 DAY) AND order_state != 'C'
          GROUP BY shipping_state`, [sDate, eDate]);
        ddShipping.by_state = rows || [];
        ddShipping.total = rows.reduce((a,b) => a + (b.cnt||0), 0);
        sources.dd = 'ok';
      } else { sources.dd = 'unavailable'; }
    } catch(ex) { sources.dd = 'error'; }

    // 파이프라인 계산
    const pipeline = {};
    barShipping.by_status.forEach(s => { pipeline[s.status_seq] = (pipeline[s.status_seq] || 0) + s.cnt; });

    ok(res, {
      sources, period: { start: s, end: e },
      kpi: {
        total_orders: barShipping.total + ddShipping.total,
        bar_total: barShipping.total,
        dd_total: ddShipping.total,
        shipped: (pipeline[5] || 0) + (pipeline[6] || 0),
        in_production: (pipeline[3] || 0) + (pipeline[4] || 0),
        pending: (pipeline[1] || 0) + (pipeline[2] || 0)
      },
      pipeline,
      bar_shop1: barShipping,
      dd: ddShipping
    });
    return;
  }

  // GET /api/shipping/list — 배송 목록 (bar_shop1)
  if (pathname === '/api/shipping/list' && method === 'GET') {
    const sources = { bar_shop1: 'unknown' };
    const start = parsed.searchParams.get('start') || '';
    const end = parsed.searchParams.get('end') || '';
    const status = parsed.searchParams.get('status') || '';
    const search = parsed.searchParams.get('q') || '';
    const limit = Math.min(parseInt(parsed.searchParams.get('limit') || '100'), 500);

    const now = new Date();
    const fmtD = d => d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
    const s = start || fmtD(new Date(now.getFullYear(), now.getMonth(), 1));
    const e = end || fmtD(now);

    let rows = [];
    try {
      await withBarShop1Pool(async (pool) => {
        let where = "o.order_date >= @s AND o.order_date <= @e AND o.status_seq >= 1";
        const request = pool.request().input('s', s).input('e', e + ' 23:59:59');
        if (status) { where += " AND o.status_seq = @st"; request.input('st', parseInt(status)); }
        if (search) { where += " AND (CAST(o.order_seq AS VARCHAR) LIKE @q OR d.NAME LIKE @q)"; request.input('q', '%'+search+'%'); }

        const r = await request.query(`SELECT TOP ${limit} o.order_seq, o.order_date, o.status_seq, o.total_price,
          RTRIM(o.site_gubun) AS site, RTRIM(o.pay_Type) AS pay_type,
          d.NAME AS recipient, RTRIM(d.ADDR) AS address, d.HPHONE AS phone,
          (SELECT COUNT(*) FROM custom_order_item i WITH (NOLOCK) WHERE i.order_seq = o.order_seq) AS item_count
          FROM custom_order o WITH (NOLOCK)
          LEFT JOIN DELIVERY_INFO d WITH (NOLOCK) ON o.order_seq = d.ORDER_SEQ AND d.DELIVERY_SEQ = 1
          WHERE ${where} ORDER BY o.order_date DESC`);
        rows = r.recordset || [];
      });
      sources.bar_shop1 = 'ok';
    } catch(ex) { sources.bar_shop1 = 'error'; }

    ok(res, { sources, rows });
    return;
  }

  // GET /api/shipping/dd-list — DD 배송 목록
  if (pathname === '/api/shipping/dd-list' && method === 'GET') {
    const sources = { dd: 'unknown' };
    const start = parsed.searchParams.get('start') || '';
    const end = parsed.searchParams.get('end') || '';
    const limit = Math.min(parseInt(parsed.searchParams.get('limit') || '100'), 500);
    let rows = [];
    try {
      if (typeof ddPool !== 'undefined' && ddPool) {
        const now = new Date();
        const s = start || (now.getFullYear()+'-'+String(now.getMonth()+1).padStart(2,'0')+'-01');
        const e = end || now.toISOString().slice(0,10);
        const [r] = await ddPool.query(`SELECT id, order_number, order_state, shipping_state,
          total_money, created_at, cj_invoice_numbers
          FROM orders WHERE created_at >= ? AND created_at < DATE_ADD(?, INTERVAL 1 DAY) AND order_state != 'C'
          ORDER BY created_at DESC LIMIT ?`, [s, e, limit]);
        rows = r || [];
        sources.dd = 'ok';
      } else { sources.dd = 'unavailable'; }
    } catch(ex) { sources.dd = 'error'; }

    ok(res, { sources, rows });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  원가관리 API (Cost Management / Margin Analysis)
  // ════════════════════════════════════════════════════════════════════

  // ── GET /api/cost/summary ── KPI: 평균마진율, 매출총이익, 최고/최저 채널마진, 원재료비 비중
  if (pathname === '/api/cost/summary' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const now = Date.now();
    const refresh = parsed.searchParams.get('refresh') === '1';
    if (!refresh && costSummaryCache && (now - costSummaryCacheTime < COST_CACHE_TTL)) {
      ok(res, costSummaryCache); return;
    }
    const result = { sources: {}, channels: [], bar_shop1: {}, cost_basis: {}, kpi: {} };
    const today = new Date();
    const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    const sYMD = startOfMonth.toISOString().slice(0,10).replace(/-/g,'');
    const eYMD = today.toISOString().slice(0,10).replace(/-/g,'');

    // ── XERP: 채널별 매출/공급가/수수료 ──
    try {
      const pool = await ensureXerpPool();
      const r = await pool.request()
        .input('s', sql.NVarChar(8), sYMD)
        .input('e', sql.NVarChar(8), eYMD)
        .query(`SELECT RTRIM(DeptGubun) AS channel,
                  COUNT(DISTINCT h_orderid) AS order_count,
                  ISNULL(SUM(h_sumPrice),0) AS total_sales,
                  ISNULL(SUM(h_offerPrice),0) AS total_supply,
                  ISNULL(SUM(FeeAmnt),0) AS total_fee
                FROM ERP_SalesData WITH (NOLOCK)
                WHERE h_date >= @s AND h_date <= @e
                GROUP BY RTRIM(DeptGubun)`);
      result.channels = r.recordset.map(row => ({
        channel: row.channel,
        channel_name: DEPT_GUBUN_LABELS[row.channel] || row.channel,
        order_count: row.order_count,
        total_sales: Number(row.total_sales),
        total_supply: Number(row.total_supply),
        total_fee: Number(row.total_fee),
        margin: Number(row.total_sales) - Number(row.total_supply) - Number(row.total_fee),
        margin_rate: Number(row.total_sales) > 0
          ? ((Number(row.total_sales) - Number(row.total_supply) - Number(row.total_fee)) / Number(row.total_sales) * 100).toFixed(1)
          : '0.0'
      }));
      result.sources.xerp = 'connected';
    } catch (e) {
      result.sources.xerp = 'error';
      console.error('[cost/summary] XERP error:', e.message);
    }

    // ── bar_shop1: 주문별 매출 vs 원가 ──
    try {
      await withBarShop1Pool(async (bPool) => {
        const r = await bPool.request()
          .input('s', sql.NVarChar(10), startOfMonth.toISOString().slice(0,10))
          .input('e', sql.NVarChar(10), today.toISOString().slice(0,10))
          .query(`SELECT SUM(i.item_sale_price * i.item_count) AS total_revenue,
                    SUM(i.item_price * i.item_count) AS total_cost,
                    COUNT(DISTINCT o.order_seq) AS order_count
                  FROM custom_order o WITH (NOLOCK)
                  JOIN custom_order_item i WITH (NOLOCK) ON o.order_seq = i.order_seq
                  WHERE o.order_date >= @s AND o.order_date < DATEADD(day,1,@e)
                    AND o.status_seq >= 1 AND i.item_sale_price > 0 AND i.item_price > 0`);
        const row = r.recordset[0] || {};
        result.bar_shop1 = {
          total_revenue: Number(row.total_revenue || 0),
          total_cost: Number(row.total_cost || 0),
          order_count: row.order_count || 0,
          margin: Number(row.total_revenue || 0) - Number(row.total_cost || 0),
          margin_rate: Number(row.total_revenue || 0) > 0
            ? ((Number(row.total_revenue || 0) - Number(row.total_cost || 0)) / Number(row.total_revenue || 0) * 100).toFixed(1) : '0.0'
        };
        result.sources.bar_shop1 = 'connected';
      });
    } catch (e) {
      result.sources.bar_shop1 = 'error';
      console.error('[cost/summary] bar_shop1 error:', e.message);
    }

    // ── SQLite: 후공정비 합계 ──
    try {
      const ppRow = db.prepare(`SELECT COALESCE(SUM(amount),0) AS total FROM post_process_history
        WHERE date >= ? AND date <= ?`).get(startOfMonth.toISOString().slice(0,10), today.toISOString().slice(0,10));
      result.post_process_total = ppRow ? Number(ppRow.total || 0) : 0;
      result.sources.sqlite = 'connected';
    } catch (e) {
      // post_process_history 테이블이 없을 수 있음
      result.post_process_total = 0;
      result.sources.sqlite = 'connected';
    }

    // ── 원가 기준 정보 (Cost Basis Definition) ──
    result.cost_basis = {
      description: '원가 산출 기준',
      components: [
        { name: '상품원가(Cost_Price)', source: 'bar_shop1.S2_Card.Cost_Price', desc: '품목 마스터에 등록된 단위 원가 (제조원가 기준)' },
        { name: '공장도가(CardFactory_Price)', source: 'bar_shop1.S2_Card.CardFactory_Price', desc: '공장 출고가 (인쇄/제조 원가)' },
        { name: '판매가(Card_Price)', source: 'bar_shop1.S2_Card.Card_Price', desc: '정가 (소비자가)' },
        { name: '실매출', source: 'XERP.ERP_SalesData.h_sumPrice', desc: '실제 거래 매출액 (할인/쿠폰 적용 후)' },
        { name: '공급가', source: 'XERP.ERP_SalesData.h_offerPrice', desc: '채널에 공급하는 가격' },
        { name: '수수료', source: 'XERP.ERP_SalesData.FeeAmnt', desc: '채널 수수료 (플랫폼 수수료 등)' },
        { name: '후공정비', source: 'SQLite.post_process_history', desc: '후가공 비용 (형압, 금박, UV 등)' },
        { name: '주문원가(item_price)', source: 'bar_shop1.custom_order_item.item_price', desc: '주문 시점의 개별 원가 (실거래 원가)' },
        { name: '주문매출(item_sale_price)', source: 'bar_shop1.custom_order_item.item_sale_price', desc: '주문 시점의 판매가' }
      ],
      margin_formula: '마진 = 실매출(h_sumPrice) - 상품원가(Cost_Price × 수량) - 수수료(FeeAmnt)',
      margin_rate_formula: '마진율 = (마진 / 실매출) × 100%',
      notes: [
        '원가 미등록 상품(Cost_Price=0 또는 NULL)은 "원가 미등록"으로 별도 표시',
        '음수 마진은 적자 상품으로 빨간색 강조 표시',
        'bar_shop1 마진은 주문 시점 원가(item_price) 기준, XERP 마진은 품목원가(Cost_Price) 기준',
        '수수료는 채널별로 상이 (자사몰 0%, 외부몰 10~30%)'
      ]
    };

    // ── KPI 산출 ──
    const xerpTotalSales = result.channels.reduce((s, c) => s + c.total_sales, 0);
    const xerpTotalFee = result.channels.reduce((s, c) => s + c.total_fee, 0);
    const xerpTotalSupply = result.channels.reduce((s, c) => s + c.total_supply, 0);
    const xerpMargin = xerpTotalSales - xerpTotalSupply - xerpTotalFee;
    const avgMarginRate = xerpTotalSales > 0 ? (xerpMargin / xerpTotalSales * 100) : 0;
    const bestChannel = result.channels.length > 0
      ? result.channels.reduce((a, b) => parseFloat(a.margin_rate) > parseFloat(b.margin_rate) ? a : b) : null;
    const worstChannel = result.channels.length > 0
      ? result.channels.reduce((a, b) => parseFloat(a.margin_rate) < parseFloat(b.margin_rate) ? a : b) : null;
    const costRatio = xerpTotalSales > 0 ? (xerpTotalSupply / xerpTotalSales * 100) : 0;

    result.kpi = {
      avg_margin_rate: avgMarginRate.toFixed(1),
      gross_profit: xerpMargin,
      total_sales: xerpTotalSales,
      total_supply: xerpTotalSupply,
      total_fee: xerpTotalFee,
      best_channel: bestChannel ? { name: bestChannel.channel_name, rate: bestChannel.margin_rate } : null,
      worst_channel: worstChannel ? { name: worstChannel.channel_name, rate: worstChannel.margin_rate } : null,
      cost_ratio: costRatio.toFixed(1),
      bar_shop1_margin_rate: result.bar_shop1.margin_rate || '0.0',
      post_process_total: result.post_process_total,
      period: { start: sYMD, end: eYMD }
    };

    costSummaryCache = result; costSummaryCacheTime = now;
    ok(res, result); return;
  }

  // ── GET /api/cost/products ── 상품별 원가/마진 분석
  if (pathname === '/api/cost/products' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    const limit = parseInt(parsed.searchParams.get('limit') || '50', 10);
    const sort = parsed.searchParams.get('sort') || 'sales'; // sales | margin_rate | qty
    if (!startParam || !endParam) { fail(res, 400, 'start, end 필수'); return; }
    const result = { products: [], sources: {}, cost_basis: '상품원가=S2_Card.Cost_Price, 마진=실매출-(원가×수량)-수수료' };

    // ── XERP: 상품별 매출/수량/수수료 ──
    let xerpProducts = [];
    try {
      const pool = await ensureXerpPool();
      const r = await pool.request()
        .input('s', sql.NVarChar(8), startParam)
        .input('e', sql.NVarChar(8), endParam)
        .query(`SELECT TOP 200 RTRIM(b_goodCode) AS product_code,
                  COUNT(DISTINCT h_orderid) AS order_count,
                  ISNULL(SUM(h_sumPrice),0) AS total_sales,
                  ISNULL(SUM(b_quantity),0) AS total_qty,
                  ISNULL(SUM(FeeAmnt),0) AS total_fee
                FROM ERP_SalesData WITH (NOLOCK)
                WHERE h_date >= @s AND h_date <= @e
                  AND b_goodCode IS NOT NULL AND RTRIM(b_goodCode) != ''
                GROUP BY RTRIM(b_goodCode)
                ORDER BY SUM(h_sumPrice) DESC`);
      xerpProducts = r.recordset;
      result.sources.xerp = 'connected';
    } catch (e) {
      result.sources.xerp = 'error';
      console.error('[cost/products] XERP error:', e.message);
    }

    // ── bar_shop1: S2_Card 배치 조회 (원가/공장도가/판매가) ──
    let cardMap = {};
    if (xerpProducts.length > 0) {
      try {
        await withBarShop1Pool(async (bPool) => {
          const codes = xerpProducts.map(p => p.product_code.trim()).filter(Boolean);
          const uniqueCodes = [...new Set(codes)].slice(0, 200);
          const req = bPool.request();
          const placeholders = uniqueCodes.map((c, i) => { req.input(`c${i}`, sql.VarChar(30), c); return `@c${i}`; }).join(',');
          const r = await req.query(`SELECT RTRIM(Card_Code) AS card_code, RTRIM(Card_Name) AS card_name,
                    Card_Price, Cost_Price, CardFactory_Price, RTRIM(Brand) AS brand
                  FROM S2_Card WITH (NOLOCK)
                  WHERE RTRIM(Card_Code) IN (${placeholders})`);
          r.recordset.forEach(row => {
            cardMap[row.card_code] = {
              name: row.card_name,
              price: Number(row.Card_Price || 0),
              cost: Number(row.Cost_Price || 0),
              factory_price: Number(row.CardFactory_Price || 0),
              brand: BRAND_LABELS[row.brand] || row.brand || ''
            };
          });
          result.sources.bar_shop1 = 'connected';
        });
      } catch (e) {
        result.sources.bar_shop1 = 'error';
        console.error('[cost/products] bar_shop1 error:', e.message);
      }
    }

    // ── 결합: 마진 계산 ──
    result.products = xerpProducts.map(p => {
      const code = p.product_code.trim();
      const card = cardMap[code] || null;
      const totalSales = Number(p.total_sales);
      const totalQty = Number(p.total_qty);
      const totalFee = Number(p.total_fee);
      const unitCost = card ? card.cost : 0;
      const totalCost = unitCost * totalQty;
      const margin = unitCost > 0 ? (totalSales - totalCost - totalFee) : null;
      const marginRate = (margin !== null && totalSales > 0) ? (margin / totalSales * 100) : null;
      return {
        code,
        name: card ? card.name : code,
        brand: card ? card.brand : '',
        unit_price: card ? card.price : 0,
        unit_cost: unitCost,
        factory_price: card ? card.factory_price : 0,
        cost_registered: unitCost > 0,
        total_sales: totalSales,
        total_qty: totalQty,
        total_fee: totalFee,
        total_cost: totalCost,
        margin: margin,
        margin_rate: marginRate !== null ? marginRate.toFixed(1) : null,
        order_count: p.order_count
      };
    });

    // 정렬
    if (sort === 'margin_rate') {
      result.products.sort((a, b) => (parseFloat(b.margin_rate) || -999) - (parseFloat(a.margin_rate) || -999));
    } else if (sort === 'qty') {
      result.products.sort((a, b) => b.total_qty - a.total_qty);
    }
    // sales는 이미 정렬됨

    result.products = result.products.slice(0, limit);
    result.products.forEach((p, i) => { p.rank = i + 1; });
    ok(res, result); return;
  }

  // ── GET /api/cost/by-channel ── 채널별 마진 비교
  if (pathname === '/api/cost/by-channel' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    if (!startParam || !endParam) { fail(res, 400, 'start, end 필수'); return; }
    const result = { channels: [], sources: {}, cost_basis: '채널마진=매출-공급가-수수료, 수수료율=수수료/매출×100' };

    // ── XERP: DeptGubun별 ──
    try {
      const pool = await ensureXerpPool();
      const r = await pool.request()
        .input('s', sql.NVarChar(8), startParam)
        .input('e', sql.NVarChar(8), endParam)
        .query(`SELECT RTRIM(DeptGubun) AS channel,
                  COUNT(DISTINCT h_orderid) AS order_count,
                  ISNULL(SUM(h_sumPrice),0) AS total_sales,
                  ISNULL(SUM(h_offerPrice),0) AS total_supply,
                  ISNULL(SUM(FeeAmnt),0) AS total_fee,
                  ISNULL(SUM(b_quantity),0) AS total_qty
                FROM ERP_SalesData WITH (NOLOCK)
                WHERE h_date >= @s AND h_date <= @e
                GROUP BY RTRIM(DeptGubun)
                ORDER BY SUM(h_sumPrice) DESC`);
      result.channels = r.recordset.map(row => {
        const sales = Number(row.total_sales);
        const supply = Number(row.total_supply);
        const fee = Number(row.total_fee);
        const margin = sales - supply - fee;
        return {
          channel: row.channel,
          channel_name: DEPT_GUBUN_LABELS[row.channel] || row.channel,
          order_count: row.order_count,
          total_sales: sales,
          total_supply: supply,
          total_fee: fee,
          total_qty: Number(row.total_qty),
          margin: margin,
          margin_rate: sales > 0 ? (margin / sales * 100).toFixed(1) : '0.0',
          fee_rate: sales > 0 ? (fee / sales * 100).toFixed(1) : '0.0'
        };
      });
      result.sources.xerp = 'connected';
    } catch (e) {
      result.sources.xerp = 'error';
      console.error('[cost/by-channel] XERP error:', e.message);
    }

    // ── bar_shop1: site_gubun별 ──
    try {
      await withBarShop1Pool(async (bPool) => {
        const startDate = startParam.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
        const endDate = endParam.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
        const r = await bPool.request()
          .input('s', sql.NVarChar(10), startDate)
          .input('e', sql.NVarChar(10), endDate)
          .query(`SELECT ISNULL(RTRIM(o.site_gubun),'기타') AS site,
                    SUM(i.item_sale_price * i.item_count) AS revenue,
                    SUM(i.item_price * i.item_count) AS cost,
                    COUNT(DISTINCT o.order_seq) AS orders
                  FROM custom_order o WITH (NOLOCK)
                  JOIN custom_order_item i WITH (NOLOCK) ON o.order_seq = i.order_seq
                  WHERE o.order_date >= @s AND o.order_date < DATEADD(day,1,@e)
                    AND o.status_seq >= 1 AND i.item_sale_price > 0 AND i.item_price > 0
                  GROUP BY RTRIM(o.site_gubun)`);
        result.bar_shop1_channels = r.recordset.map(row => {
          const rev = Number(row.revenue || 0);
          const cost = Number(row.cost || 0);
          return {
            site: row.site,
            revenue: rev, cost: cost,
            margin: rev - cost,
            margin_rate: rev > 0 ? ((rev - cost) / rev * 100).toFixed(1) : '0.0',
            orders: row.orders
          };
        });
        result.sources.bar_shop1 = 'connected';
      });
    } catch (e) {
      result.sources.bar_shop1 = 'error';
      result.bar_shop1_channels = [];
      console.error('[cost/by-channel] bar_shop1 error:', e.message);
    }

    ok(res, result); return;
  }

  // ── GET /api/cost/trend ── 월별 매출/마진율 추이
  if (pathname === '/api/cost/trend' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const months = parseInt(parsed.searchParams.get('months') || '12', 10);
    const result = { months: [], sources: {}, cost_basis: '월별마진=매출-공급가-수수료' };

    // 시작일 계산
    const today = new Date();
    const startDate = new Date(today.getFullYear(), today.getMonth() - months + 1, 1);
    const sYMD = startDate.toISOString().slice(0,10).replace(/-/g,'');
    const eYMD = today.toISOString().slice(0,10).replace(/-/g,'');

    // ── XERP: 월별 매출/수수료 ──
    let monthMap = {};
    try {
      const pool = await ensureXerpPool();
      const r = await pool.request()
        .input('s', sql.NVarChar(8), sYMD)
        .input('e', sql.NVarChar(8), eYMD)
        .query(`SELECT LEFT(h_date,6) AS ym,
                  ISNULL(SUM(h_sumPrice),0) AS total_sales,
                  ISNULL(SUM(h_offerPrice),0) AS total_supply,
                  ISNULL(SUM(FeeAmnt),0) AS total_fee,
                  COUNT(DISTINCT h_orderid) AS order_count
                FROM ERP_SalesData WITH (NOLOCK)
                WHERE h_date >= @s AND h_date <= @e
                GROUP BY LEFT(h_date,6)
                ORDER BY LEFT(h_date,6)`);
      r.recordset.forEach(row => {
        monthMap[row.ym] = {
          month: row.ym,
          total_sales: Number(row.total_sales),
          total_supply: Number(row.total_supply),
          total_fee: Number(row.total_fee),
          order_count: row.order_count
        };
      });
      result.sources.xerp = 'connected';
    } catch (e) {
      result.sources.xerp = 'error';
      console.error('[cost/trend] XERP error:', e.message);
    }

    // ── bar_shop1: 월별 원가/매출 ──
    try {
      await withBarShop1Pool(async (bPool) => {
        const startDateStr = startDate.toISOString().slice(0,10);
        const endDateStr = today.toISOString().slice(0,10);
        const r = await bPool.request()
          .input('s', sql.NVarChar(10), startDateStr)
          .input('e', sql.NVarChar(10), endDateStr)
          .query(`SELECT FORMAT(o.order_date, 'yyyyMM') AS ym,
                    SUM(i.item_sale_price * i.item_count) AS revenue,
                    SUM(i.item_price * i.item_count) AS cost,
                    COUNT(DISTINCT o.order_seq) AS orders
                  FROM custom_order o WITH (NOLOCK)
                  JOIN custom_order_item i WITH (NOLOCK) ON o.order_seq = i.order_seq
                  WHERE o.order_date >= @s AND o.order_date < DATEADD(day,1,@e)
                    AND o.status_seq >= 1 AND i.item_sale_price > 0 AND i.item_price > 0
                  GROUP BY FORMAT(o.order_date, 'yyyyMM')
                  ORDER BY FORMAT(o.order_date, 'yyyyMM')`);
        r.recordset.forEach(row => {
          if (!monthMap[row.ym]) monthMap[row.ym] = { month: row.ym, total_sales: 0, total_supply: 0, total_fee: 0, order_count: 0 };
          monthMap[row.ym].bs_revenue = Number(row.revenue || 0);
          monthMap[row.ym].bs_cost = Number(row.cost || 0);
          monthMap[row.ym].bs_orders = row.orders || 0;
        });
        result.sources.bar_shop1 = 'connected';
      });
    } catch (e) {
      result.sources.bar_shop1 = 'error';
      console.error('[cost/trend] bar_shop1 error:', e.message);
    }

    // 월별 배열 생성 + 마진 계산
    result.months = Object.values(monthMap).sort((a, b) => a.month.localeCompare(b.month)).map(m => {
      const margin = m.total_sales - m.total_supply - m.total_fee;
      return {
        ...m,
        margin: margin,
        margin_rate: m.total_sales > 0 ? (margin / m.total_sales * 100).toFixed(1) : '0.0',
        bs_margin: (m.bs_revenue || 0) - (m.bs_cost || 0),
        bs_margin_rate: (m.bs_revenue || 0) > 0 ? (((m.bs_revenue || 0) - (m.bs_cost || 0)) / (m.bs_revenue || 0) * 100).toFixed(1) : '0.0'
      };
    });

    ok(res, result); return;
  }

  // ── GET /api/cost/breakdown ── 원가 구성 분해 (파이차트용)
  if (pathname === '/api/cost/breakdown' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const startParam = parsed.searchParams.get('start') || '';
    const endParam = parsed.searchParams.get('end') || '';
    const productCode = parsed.searchParams.get('product_code') || '';
    if (!startParam || !endParam) { fail(res, 400, 'start, end 필수'); return; }
    const result = { breakdown: [], total: 0, sources: {},
      cost_basis: '원가구성: 원재료비(Cost_Price), 수수료(FeeAmnt), 후공정비(post_process), 기타' };

    // ── XERP: 매출/수수료 ──
    let totalSales = 0, totalFee = 0, totalSupply = 0, totalQty = 0;
    try {
      const pool = await ensureXerpPool();
      const req = pool.request()
        .input('s', sql.NVarChar(8), startParam)
        .input('e', sql.NVarChar(8), endParam);
      let where = `h_date >= @s AND h_date <= @e`;
      if (productCode) {
        req.input('pc', sql.VarChar(30), productCode);
        where += ` AND RTRIM(b_goodCode) = @pc`;
      }
      const r = await req.query(`SELECT ISNULL(SUM(h_sumPrice),0) AS sales,
                ISNULL(SUM(h_offerPrice),0) AS supply,
                ISNULL(SUM(FeeAmnt),0) AS fee,
                ISNULL(SUM(b_quantity),0) AS qty
              FROM ERP_SalesData WITH (NOLOCK)
              WHERE ${where}`);
      const row = r.recordset[0] || {};
      totalSales = Number(row.sales || 0);
      totalSupply = Number(row.supply || 0);
      totalFee = Number(row.fee || 0);
      totalQty = Number(row.qty || 0);
      result.sources.xerp = 'connected';
    } catch (e) {
      result.sources.xerp = 'error';
      console.error('[cost/breakdown] XERP error:', e.message);
    }

    // ── bar_shop1: 원재료비 (Cost_Price 기반) ──
    let materialCost = 0;
    try {
      if (productCode) {
        await withBarShop1Pool(async (bPool) => {
          const r = await bPool.request()
            .input('c', sql.VarChar(30), productCode)
            .query(`SELECT Cost_Price, CardFactory_Price FROM S2_Card WITH (NOLOCK) WHERE RTRIM(Card_Code)=@c`);
          const card = r.recordset[0];
          if (card && card.Cost_Price > 0) {
            materialCost = Number(card.Cost_Price) * totalQty;
          } else if (card && card.CardFactory_Price > 0) {
            materialCost = Number(card.CardFactory_Price) * totalQty;
          }
          result.sources.bar_shop1 = 'connected';
        });
      } else {
        // 전체 원가: 공급가를 원재료비 추정치로 사용
        materialCost = totalSupply;
        result.sources.bar_shop1 = 'estimated';
      }
    } catch (e) {
      materialCost = totalSupply; // fallback
      result.sources.bar_shop1 = 'error';
    }

    // ── SQLite: 후공정비 ──
    let postProcessCost = 0;
    try {
      const startDate = startParam.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
      const endDate = endParam.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
      let ppSql = `SELECT COALESCE(SUM(amount),0) AS total FROM post_process_history WHERE date >= ? AND date <= ?`;
      const ppParams = [startDate, endDate];
      if (productCode) { ppSql += ` AND product_code = ?`; ppParams.push(productCode); }
      const ppRow = db.prepare(ppSql).get(...ppParams);
      postProcessCost = ppRow ? Number(ppRow.total || 0) : 0;
    } catch (e) {
      postProcessCost = 0;
    }

    // ── 구성 분해 ──
    const otherCost = Math.max(0, totalSales - materialCost - totalFee - postProcessCost);
    const totalCost = materialCost + totalFee + postProcessCost;
    result.breakdown = [
      { name: '원재료비', amount: materialCost, pct: totalCost > 0 ? (materialCost / totalCost * 100).toFixed(1) : '0.0', color: '#3b82f6' },
      { name: '수수료', amount: totalFee, pct: totalCost > 0 ? (totalFee / totalCost * 100).toFixed(1) : '0.0', color: '#f59e0b' },
      { name: '후공정비', amount: postProcessCost, pct: totalCost > 0 ? (postProcessCost / totalCost * 100).toFixed(1) : '0.0', color: '#10b981' }
    ];
    result.total = totalCost;
    result.total_sales = totalSales;
    result.gross_profit = totalSales - totalCost;
    result.margin_rate = totalSales > 0 ? ((totalSales - totalCost) / totalSales * 100).toFixed(1) : '0.0';
    result.sources.sqlite = 'connected';

    ok(res, result); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  공지/게시판 API (Notice Board)
  // ════════════════════════════════════════════════════════════════════

  // ── GET /api/notices ── 공지 목록
  if (pathname === '/api/notices' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const category = parsed.searchParams.get('category') || '';
    const page = parseInt(parsed.searchParams.get('page') || '1', 10);
    const limit = parseInt(parsed.searchParams.get('limit') || '20', 10);
    const offset = (page - 1) * limit;
    let where = "status = 'active'";
    const params = [];
    if (category) { where += " AND category = ?"; params.push(category); }
    const total = db.prepare(`SELECT COUNT(*) as cnt FROM notices WHERE ${where}`).get(...params).cnt;
    const rows = db.prepare(`SELECT * FROM notices WHERE ${where} ORDER BY is_pinned DESC, created_at DESC LIMIT ? OFFSET ?`)
      .all(...params, limit, offset);
    // 읽음 여부 추가
    const reads = db.prepare(`SELECT notice_id FROM notice_reads WHERE user_id = ?`).all(decoded.userId);
    const readSet = new Set(reads.map(r => r.notice_id));
    rows.forEach(r => { r.is_read = readSet.has(r.id) ? 1 : 0; });
    ok(res, { notices: rows, total, page, limit, totalPages: Math.ceil(total / limit) }); return;
  }

  // ── GET /api/notices/popup ── 활성 팝업 공지 (로그인 시 표시)
  if (pathname === '/api/notices/popup' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const popups = db.prepare(`SELECT n.* FROM notices n
      WHERE n.status = 'active' AND n.is_popup = 1
        AND (n.popup_start IS NULL OR n.popup_start <= ?)
        AND (n.popup_end IS NULL OR n.popup_end >= ?)
      ORDER BY n.created_at DESC`).all(now, now);
    // 사용자가 이미 닫은 팝업 제외
    const dismissed = db.prepare(`SELECT notice_id FROM notice_reads WHERE user_id = ? AND popup_dismissed = 1`).all(decoded.userId);
    const dismissedSet = new Set(dismissed.map(r => r.notice_id));
    const active = popups.filter(p => !dismissedSet.has(p.id));
    ok(res, { popups: active }); return;
  }

  // ── POST /api/notices/popup/:id/dismiss ── 팝업 닫기 (오늘 하루 안보기)
  if (pathname.match(/^\/api\/notices\/popup\/(\d+)\/dismiss$/) && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const noticeId = parseInt(pathname.match(/\/(\d+)\/dismiss/)[1], 10);
    db.prepare(`INSERT INTO notice_reads (notice_id, user_id, popup_dismissed) VALUES (?, ?, 1)
      ON CONFLICT(notice_id, user_id) DO UPDATE SET popup_dismissed = 1, read_at = datetime('now','localtime')`)
      .run(noticeId, decoded.userId);
    ok(res, { message: '팝업 닫기 완료' }); return;
  }

  // ── GET /api/notices/:id ── 공지 상세 + 조회수 증가
  if (pathname.match(/^\/api\/notices\/(\d+)$/) && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const notice = db.prepare("SELECT * FROM notices WHERE id = ?").get(id);
    if (!notice) { fail(res, 404, '공지를 찾을 수 없습니다'); return; }
    // 조회수 증가
    db.prepare("UPDATE notices SET view_count = view_count + 1 WHERE id = ?").run(id);
    notice.view_count += 1;
    // 읽음 처리
    db.prepare(`INSERT INTO notice_reads (notice_id, user_id) VALUES (?, ?)
      ON CONFLICT(notice_id, user_id) DO UPDATE SET read_at = datetime('now','localtime')`)
      .run(id, decoded.userId);
    ok(res, notice); return;
  }

  // ── POST /api/notices ── 공지 작성 (admin만)
  if (pathname === '/api/notices' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한 필요'); return; }
    const body = await readJSON(req);
    const { title, content, category, is_popup, popup_start, popup_end, is_pinned } = body;
    if (!title) { fail(res, 400, '제목 필수'); return; }
    const r = db.prepare(`INSERT INTO notices (title, content, category, is_popup, popup_start, popup_end, is_pinned, author_id, author_name)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      title, content || '', category || 'notice',
      is_popup ? 1 : 0, popup_start || null, popup_end || null,
      is_pinned ? 1 : 0, decoded.userId, decoded.username
    );
    auditLog(decoded.userId, decoded.username, 'notice_create', 'notices', r.lastInsertRowid, `공지 작성: ${title}${is_popup ? ' (팝업)' : ''}`, clientIP);
    ok(res, { id: r.lastInsertRowid, message: '공지 등록 완료' }); return;
  }

  // ── PUT /api/notices/:id ── 공지 수정 (admin만)
  if (pathname.match(/^\/api\/notices\/(\d+)$/) && method === 'PUT') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const body = await readJSON(req);
    const { title, content, category, is_popup, popup_start, popup_end, is_pinned, status } = body;
    db.prepare(`UPDATE notices SET title=COALESCE(?,title), content=COALESCE(?,content),
      category=COALESCE(?,category), is_popup=?, popup_start=?, popup_end=?,
      is_pinned=?, status=COALESCE(?,status), updated_at=datetime('now','localtime') WHERE id=?`).run(
      title || null, content !== undefined ? content : null, category || null,
      is_popup ? 1 : 0, popup_start || null, popup_end || null,
      is_pinned ? 1 : 0, status || null, id
    );
    auditLog(decoded.userId, decoded.username, 'notice_update', 'notices', id, `공지 수정: ${title || '(제목 유지)'}`, clientIP);
    ok(res, { message: '공지 수정 완료' }); return;
  }

  // ── DELETE /api/notices/:id ── 공지 삭제 (soft delete, admin만)
  if (pathname.match(/^\/api\/notices\/(\d+)$/) && method === 'DELETE') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    db.prepare("UPDATE notices SET status = 'deleted', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
    auditLog(decoded.userId, decoded.username, 'notice_delete', 'notices', id, '공지 삭제', clientIP);
    ok(res, { message: '공지 삭제 완료' }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  세금계산서 API (Tax Invoice)
  // ════════════════════════════════════════════════════════════════════

  // ── GET /api/tax-invoice/list ── 세금계산서 목록
  if (pathname === '/api/tax-invoice/list' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || (() => { const d = new Date(); d.setMonth(d.getMonth()-1); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const arAp = qs.get('type') || ''; // AR, AP, or '' for all
    const search = qs.get('search') || '';
    const offset = parseInt(qs.get('offset') || '0', 10);
    const limit = Math.min(parseInt(qs.get('limit') || '100', 10), 500);
    const sources = { xerp: 'unknown' };
    let invoices = [], totalCount = 0;
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      let whereExtra = '';
      const cReq = pool.request().input('from', from).input('to', to);
      const dReq = pool.request().input('from', from).input('to', to).input('offset', offset).input('limit', limit);
      if (arAp) { whereExtra += ' AND h.ArApGubun = @arAp'; cReq.input('arAp', arAp); dReq.input('arAp', arAp); }
      if (search) { whereExtra += " AND (h.InvoiceNo LIKE @search OR h.CsCode LIKE @search)"; cReq.input('search', '%'+search+'%'); dReq.input('search', '%'+search+'%'); }
      const countR = await cReq.query(`SELECT COUNT(*) AS cnt FROM rpInvoiceHeader h WITH(NOLOCK) WHERE h.SiteCode='BK10' AND h.InvoiceDate >= @from AND h.InvoiceDate <= @to ${whereExtra}`);
      totalCount = countR.recordset[0].cnt;
      const dataR = await dReq.query(`
        SELECT RTRIM(h.InvoiceNo) AS invoice_no, h.InvoiceDate, h.ArApGubun,
               RTRIM(h.CsCode) AS cs_code, RTRIM(h.CsRegNo) AS cs_reg_no,
               ISNULL(h.SupplyAmnt,0) AS supply_amt, ISNULL(h.VatAmnt,0) AS vat_amt,
               h.TaxCode, RTRIM(h.DocNo) AS doc_no, h.EseroUp, h.RelCheck, h.BillCheck,
               RTRIM(h.CsEmail) AS cs_email
        FROM rpInvoiceHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10' AND h.InvoiceDate >= @from AND h.InvoiceDate <= @to ${whereExtra}
        ORDER BY h.InvoiceDate DESC, h.InvoiceNo DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
      `);
      invoices = dataR.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    const totals = {
      count: totalCount,
      supply: invoices.reduce((s,r) => s + (r.supply_amt||0), 0),
      vat: invoices.reduce((s,r) => s + (r.vat_amt||0), 0),
      electronic: invoices.filter(r => (r.EseroUp||'').trim() === 'Y').length,
    };
    ok(res, { invoices, totalCount, offset, limit, totals, sources }); return;
  }

  // ── GET /api/tax-invoice/detail/:invoiceNo ── 세금계산서 상세
  if (pathname.match(/^\/api\/tax-invoice\/detail\/(.+)$/) && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const invoiceNo = decodeURIComponent(pathname.match(/^\/api\/tax-invoice\/detail\/(.+)$/)[1]);
    const sources = { xerp: 'unknown' };
    let header = null, items = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const hR = await pool.request().input('no', invoiceNo).query(`
        SELECT RTRIM(h.InvoiceNo) AS invoice_no, h.InvoiceDate, h.ArApGubun,
               RTRIM(h.CsCode) AS cs_code, RTRIM(h.CsRegNo) AS cs_reg_no,
               RTRIM(h.OurRegNo) AS our_reg_no,
               ISNULL(h.SupplyAmnt,0) AS supply_amt, ISNULL(h.VatAmnt,0) AS vat_amt,
               h.TaxCode, RTRIM(h.DocNo) AS doc_no, h.EseroUp, h.RelCheck,
               RTRIM(h.CsEmail) AS cs_email, RTRIM(h.CsMobile) AS cs_mobile
        FROM rpInvoiceHeader h WITH(NOLOCK) WHERE h.SiteCode='BK10' AND RTRIM(h.InvoiceNo)=@no
      `);
      if (hR.recordset.length > 0) header = hR.recordset[0];
      const iR = await pool.request().input('no', invoiceNo).query(`
        SELECT i.InvoiceSerNo, i.ItemDate, RTRIM(i.ItemName) AS item_name,
               ISNULL(i.ItemQty,0) AS qty, ISNULL(i.ItemPrice,0) AS price,
               ISNULL(i.ItemAmnt,0) AS amt, ISNULL(i.ItemVatAmnt,0) AS vat
        FROM rpInvoiceItem i WITH(NOLOCK)
        WHERE i.SiteCode='BK10' AND RTRIM(i.InvoiceNo)=@no
        ORDER BY i.InvoiceSerNo
      `);
      items = iR.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    ok(res, { header, items, sources }); return;
  }

  // ── GET /api/tax-invoice/summary ── 월별 세금계산서 집계
  if (pathname === '/api/tax-invoice/summary' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const sources = { xerp: 'unknown' };
    let monthly = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const r = await pool.request().input('yearStart', year+'0101').input('yearEnd', year+'1231').query(`
        SELECT LEFT(h.InvoiceDate,6) AS ym, h.ArApGubun,
               COUNT(*) AS cnt,
               ISNULL(SUM(h.SupplyAmnt),0) AS supply,
               ISNULL(SUM(h.VatAmnt),0) AS vat,
               SUM(CASE WHEN RTRIM(h.EseroUp)='Y' THEN 1 ELSE 0 END) AS electronic
        FROM rpInvoiceHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10' AND h.InvoiceDate >= @yearStart AND h.InvoiceDate <= @yearEnd
        GROUP BY LEFT(h.InvoiceDate,6), h.ArApGubun
        ORDER BY LEFT(h.InvoiceDate,6), h.ArApGubun
      `);
      monthly = r.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    ok(res, { year, monthly, sources }); return;
  }

  // ── POST /api/tax-invoice/upload ── 홈택스 엑셀 업로드
  if (pathname === '/api/tax-invoice/upload' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    try {
      const body = await readJSON(req);
      const rows = body.rows; // [{invoice_no, invoice_date, ar_ap, cs_name, cs_reg_no, supply_amt, vat_amt, total_amt, item_name, remark, electronic}]
      if (!rows || !Array.isArray(rows) || rows.length === 0) { fail(res, 400, '업로드 데이터 없음'); return; }
      const stmt = db.prepare(`INSERT OR IGNORE INTO hometax_invoices
        (invoice_no, invoice_date, ar_ap, cs_name, cs_reg_no, supply_amt, vat_amt, total_amt, item_name, remark, electronic, uploaded_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
      let inserted = 0, skipped = 0;
      const txn = db.transaction((items) => {
        for (const r of items) {
          const info = stmt.run(
            (r.invoice_no||'').trim(), (r.invoice_date||'').replace(/-/g,'').trim(),
            (r.ar_ap||'AP').trim(), (r.cs_name||'').trim(), (r.cs_reg_no||'').replace(/-/g,'').trim(),
            parseFloat(r.supply_amt)||0, parseFloat(r.vat_amt)||0, parseFloat(r.total_amt)||0,
            (r.item_name||'').trim(), (r.remark||'').trim(),
            (r.electronic||'Y').trim(), decoded.name || decoded.email || ''
          );
          if (info.changes > 0) inserted++; else skipped++;
        }
      });
      txn(rows);
      ok(res, { inserted, skipped, total: rows.length }); return;
    } catch (e) { fail(res, 500, e.message); return; }
  }

  // ── GET /api/tax-invoice/hometax ── 홈택스 업로드 세금계산서 목록
  if (pathname === '/api/tax-invoice/hometax' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || (() => { const d = new Date(); d.setMonth(d.getMonth()-1); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const arAp = qs.get('type') || '';
    const search = qs.get('search') || '';
    const offset = parseInt(qs.get('offset') || '0', 10);
    const limit = Math.min(parseInt(qs.get('limit') || '100', 10), 500);
    let where = 'invoice_date >= ? AND invoice_date <= ?';
    const params = [from, to];
    if (arAp) { where += ' AND ar_ap = ?'; params.push(arAp); }
    if (search) { where += ' AND (invoice_no LIKE ? OR cs_name LIKE ? OR cs_reg_no LIKE ?)'; params.push('%'+search+'%','%'+search+'%','%'+search+'%'); }
    const totalCount = db.prepare('SELECT COUNT(*) AS cnt FROM hometax_invoices WHERE ' + where).get(...params).cnt;
    const invoices = db.prepare('SELECT * FROM hometax_invoices WHERE ' + where + ' ORDER BY invoice_date DESC, id DESC LIMIT ? OFFSET ?').all(...params, limit, offset);
    const totals = {
      count: totalCount,
      supply: invoices.reduce((s,r) => s + (r.supply_amt||0), 0),
      vat: invoices.reduce((s,r) => s + (r.vat_amt||0), 0),
    };
    ok(res, { invoices, totalCount, offset, limit, totals, sources: { hometax: 'ok' } }); return;
  }

  // ── DELETE /api/tax-invoice/hometax ── 홈택스 데이터 삭제 (기간)
  if (pathname === '/api/tax-invoice/hometax' && method === 'DELETE') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from'), to = qs.get('to');
    if (!from || !to) { fail(res, 400, 'from/to 필수'); return; }
    const info = db.prepare('DELETE FROM hometax_invoices WHERE invoice_date >= ? AND invoice_date <= ?').run(from, to);
    ok(res, { deleted: info.changes }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  원재료 단가 API (Material Price)
  // ════════════════════════════════════════════════════════════════════

  // ── POST /api/material-price/upload ── 원재료 단가 엑셀 업로드
  if (pathname === '/api/material-price/upload' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    try {
      const body = await readJSON(req);
      const rows = body.rows;
      if (!rows || !Array.isArray(rows) || rows.length === 0) { fail(res, 400, '데이터 없음'); return; }
      const stmt = db.prepare(`INSERT OR REPLACE INTO material_prices
        (product_code, product_name, spec, unit, vendor_name, list_price, apply_price, discount_rate, apply_month, uploaded_by, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))`);
      let upserted = 0;
      const txn = db.transaction((items) => {
        for (const r of items) {
          stmt.run(
            (r.product_code||'').trim(), (r.product_name||'').trim(),
            (r.spec||'').trim(), (r.unit||'R').trim(),
            (r.vendor_name||'').trim(),
            parseFloat(r.list_price)||0, parseFloat(r.apply_price)||0,
            parseFloat(r.discount_rate)||0,
            (r.apply_month||'').trim(),
            decoded.name || decoded.email || ''
          );
          upserted++;
        }
      });
      txn(rows);
      ok(res, { upserted, total: rows.length }); return;
    } catch (e) { fail(res, 500, e.message); return; }
  }

  // ── GET /api/material-price/list ── 원재료 단가 목록
  if (pathname === '/api/material-price/list' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const vendor = qs.get('vendor') || '';
    const search = qs.get('search') || '';
    const month = qs.get('month') || '';
    let where = '1=1';
    const params = [];
    if (vendor) { where += ' AND vendor_name = ?'; params.push(vendor); }
    if (month) { where += ' AND apply_month = ?'; params.push(month); }
    if (search) { where += ' AND (product_code LIKE ? OR product_name LIKE ?)'; params.push('%'+search+'%','%'+search+'%'); }
    const items = db.prepare('SELECT * FROM material_prices WHERE ' + where + ' ORDER BY vendor_name, product_code, apply_month DESC').all(...params);
    // 제지사 목록
    const vendors = db.prepare('SELECT DISTINCT vendor_name FROM material_prices ORDER BY vendor_name').all().map(r => r.vendor_name);
    // 적용월 목록
    const months = db.prepare('SELECT DISTINCT apply_month FROM material_prices ORDER BY apply_month DESC').all().map(r => r.apply_month);
    ok(res, { items, vendors, months, count: items.length }); return;
  }

  // ── GET /api/material-price/latest ── 품목별 최신 단가 (중복 제거)
  if (pathname === '/api/material-price/latest' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const vendor = qs.get('vendor') || '';
    const search = qs.get('search') || '';
    let where = '1=1';
    const params = [];
    if (vendor) { where += ' AND vendor_name = ?'; params.push(vendor); }
    if (search) { where += ' AND (product_code LIKE ? OR product_name LIKE ?)'; params.push('%'+search+'%','%'+search+'%'); }
    const items = db.prepare(`SELECT m.* FROM material_prices m
      INNER JOIN (SELECT product_code, vendor_name, MAX(apply_month) AS max_month FROM material_prices GROUP BY product_code, vendor_name) g
      ON m.product_code=g.product_code AND m.vendor_name=g.vendor_name AND m.apply_month=g.max_month
      WHERE ${where} ORDER BY m.vendor_name, m.product_code`).all(...params);
    const vendors = db.prepare('SELECT DISTINCT vendor_name FROM material_prices ORDER BY vendor_name').all().map(r => r.vendor_name);
    ok(res, { items, vendors, count: items.length }); return;
  }

  // ── GET /api/material-price/compare ── XERP 실매입가 vs 단가표 비교
  if (pathname === '/api/material-price/compare' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || (() => { const d = new Date(); d.setMonth(d.getMonth()-3); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const search = qs.get('search') || '';
    const sources = { xerp: 'unknown', sqlite: 'ok' };

    // SQLite 최신 단가표
    const priceMap = {};
    const allPrices = db.prepare(`SELECT m.* FROM material_prices m
      INNER JOIN (SELECT product_code, vendor_name, MAX(apply_month) AS max_month FROM material_prices GROUP BY product_code, vendor_name) g
      ON m.product_code=g.product_code AND m.vendor_name=g.vendor_name AND m.apply_month=g.max_month`).all();
    for (const p of allPrices) priceMap[p.product_code] = p;

    // XERP 실매입가 조회
    let xerpItems = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      let searchWhere = '';
      const req2 = pool.request().input('from', from).input('to', to);
      if (search) { searchWhere = " AND (RTRIM(d.ItemCode) LIKE @search OR RTRIM(d.ItemName) LIKE @search)"; req2.input('search', '%'+search+'%'); }
      const r = await req2.query(`
        SELECT RTRIM(d.ItemCode) AS item_code, RTRIM(d.ItemName) AS item_name,
               RTRIM(d.ItemStnd) AS item_spec, RTRIM(d.ItemUnit) AS item_unit,
               COUNT(*) AS txn_count,
               SUM(d.Qty) AS total_qty,
               SUM(d.CurAmt) AS total_amt,
               CASE WHEN SUM(d.Qty)>0 THEN SUM(d.CurAmt)/SUM(d.Qty) ELSE 0 END AS avg_price,
               MIN(d.CurPrice) AS min_price, MAX(d.CurPrice) AS max_price
        FROM mmInoutItem d WITH(NOLOCK)
        INNER JOIN mmInoutHeader h WITH(NOLOCK) ON d.SiteCode=h.SiteCode AND d.InoutNo=h.InoutNo
        WHERE d.SiteCode='BK10' AND h.InoutDate>=@from AND h.InoutDate<=@to
          AND h.InoutType IN ('10','11') ${searchWhere}
        GROUP BY RTRIM(d.ItemCode), RTRIM(d.ItemName), RTRIM(d.ItemStnd), RTRIM(d.ItemUnit)
        ORDER BY RTRIM(d.ItemCode)
      `);
      xerpItems = r.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }

    // 비교 결과 생성
    const comparison = xerpItems.map(x => {
      const mp = priceMap[x.item_code] || null;
      const diff = mp ? (x.avg_price - mp.apply_price) : null;
      const diffPct = (mp && mp.apply_price > 0) ? ((x.avg_price - mp.apply_price) / mp.apply_price * 100) : null;
      return {
        item_code: x.item_code, item_name: x.item_name, item_spec: x.item_spec,
        xerp_avg_price: Math.round(x.avg_price), xerp_min: x.min_price, xerp_max: x.max_price,
        xerp_qty: x.total_qty, xerp_amt: x.total_amt, xerp_txn_count: x.txn_count,
        our_price: mp ? mp.apply_price : null, our_list_price: mp ? mp.list_price : null,
        our_discount: mp ? mp.discount_rate : null, our_vendor: mp ? mp.vendor_name : null,
        our_month: mp ? mp.apply_month : null,
        diff: diff !== null ? Math.round(diff) : null,
        diff_pct: diffPct !== null ? Math.round(diffPct * 10) / 10 : null,
        matched: !!mp
      };
    });

    ok(res, { comparison, sources, from, to, totalXerp: xerpItems.length, totalMatched: comparison.filter(c=>c.matched).length }); return;
  }

  // ── DELETE /api/material-price ── 원재료 단가 삭제
  if (pathname === '/api/material-price' && method === 'DELETE') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const month = qs.get('month');
    const vendor = qs.get('vendor');
    let where = '1=1'; const params = [];
    if (month) { where += ' AND apply_month = ?'; params.push(month); }
    if (vendor) { where += ' AND vendor_name = ?'; params.push(vendor); }
    const info = db.prepare('DELETE FROM material_prices WHERE ' + where).run(...params);
    ok(res, { deleted: info.changes }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  작업지시 API (Work Order)
  // ════════════════════════════════════════════════════════════════════

  // ── GET /api/work-orders ── 작업지시 목록
  if (pathname === '/api/work-orders' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const status = qs.get('status') || '';
    const search = qs.get('search') || '';
    let where = '1=1';
    if (status) where += " AND status = '" + status.replace(/'/g, '') + "'";
    if (search) where += " AND (wo_number LIKE '%" + search.replace(/'/g, '') + "%' OR product_name LIKE '%" + search.replace(/'/g, '') + "%')";
    const orders = db.prepare(`SELECT * FROM work_orders WHERE ${where} ORDER BY created_at DESC LIMIT 200`).all();
    const summary = db.prepare(`SELECT status, COUNT(*) AS cnt FROM work_orders GROUP BY status`).all();
    const statusMap = {};
    summary.forEach(s => { statusMap[s.status] = s.cnt; });
    ok(res, { orders, summary: statusMap, total: orders.length }); return;
  }

  // ── POST /api/work-orders ── 작업지시 생성
  if (pathname === '/api/work-orders' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const body = await readJSON(req);
    const now = new Date();
    const woNum = 'WO' + now.getFullYear().toString().slice(2) + String(now.getMonth()+1).padStart(2,'0') + String(now.getDate()).padStart(2,'0') + '-' + String(Math.floor(Math.random()*9999)).padStart(4,'0');
    const stmt = db.prepare(`INSERT INTO work_orders (wo_number, request_id, product_code, product_name, brand, ordered_qty, status, priority, start_date, due_date, printer_vendor, post_vendor, paper_type, notes, created_by)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    const result = stmt.run(woNum, body.request_id||null, body.product_code||'', body.product_name||'', body.brand||'', body.ordered_qty||0, 'planned', body.priority||'normal', body.start_date||now.toISOString().slice(0,10), body.due_date||'', body.printer_vendor||'', body.post_vendor||'', body.paper_type||'', body.notes||'', decoded.username||'');
    db.prepare(`INSERT INTO work_order_logs (wo_id, wo_number, action, to_status, actor, details) VALUES (?,?,?,?,?,?)`).run(result.lastInsertRowid, woNum, 'created', 'planned', decoded.username||'', '작업지시 생성');
    ok(res, { wo_id: result.lastInsertRowid, wo_number: woNum }); return;
  }

  // ── GET /api/work-orders/:id ── 작업지시 상세
  if (pathname.match(/^\/api\/work-orders\/(\d+)$/) && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const wo = db.prepare('SELECT * FROM work_orders WHERE wo_id = ?').get(id);
    const logs = db.prepare('SELECT * FROM work_order_logs WHERE wo_id = ? ORDER BY created_at DESC').all(id);
    if (!wo) { fail(res, 404, '작업지시 없음'); return; }
    ok(res, { order: wo, logs }); return;
  }

  // ── PUT /api/work-orders/:id ── 작업지시 수정 (상태변경, 실적등록)
  if (pathname.match(/^\/api\/work-orders\/(\d+)$/) && method === 'PUT') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const body = await readJSON(req);
    const wo = db.prepare('SELECT * FROM work_orders WHERE wo_id = ?').get(id);
    if (!wo) { fail(res, 404, '작업지시 없음'); return; }
    const oldStatus = wo.status;
    const updates = [];
    const params = [];
    if (body.status) { updates.push('status=?'); params.push(body.status); }
    if (body.produced_qty !== undefined) { updates.push('produced_qty=?'); params.push(body.produced_qty); }
    if (body.defect_qty !== undefined) { updates.push('defect_qty=?'); params.push(body.defect_qty); }
    if (body.cost_material !== undefined) { updates.push('cost_material=?'); params.push(body.cost_material); }
    if (body.cost_labor !== undefined) { updates.push('cost_labor=?'); params.push(body.cost_labor); }
    if (body.cost_overhead !== undefined) { updates.push('cost_overhead=?'); params.push(body.cost_overhead); }
    if (body.notes !== undefined) { updates.push('notes=?'); params.push(body.notes); }
    if (body.status === 'in_progress' && !wo.start_date) { updates.push("start_date=date('now','localtime')"); }
    if (body.status === 'completed') { updates.push("completed_date=datetime('now','localtime')"); }
    // 원가 합계
    const cm = body.cost_material !== undefined ? body.cost_material : wo.cost_material;
    const cl = body.cost_labor !== undefined ? body.cost_labor : wo.cost_labor;
    const co = body.cost_overhead !== undefined ? body.cost_overhead : wo.cost_overhead;
    updates.push('cost_total=?'); params.push((cm||0) + (cl||0) + (co||0));
    updates.push("updated_at=datetime('now','localtime')");
    params.push(id);
    if (updates.length > 1) {
      db.prepare(`UPDATE work_orders SET ${updates.join(',')} WHERE wo_id=?`).run(...params);
    }
    // 로그
    let action = 'updated';
    let details = '';
    if (body.status && body.status !== oldStatus) { action = 'status_change'; details = oldStatus + ' → ' + body.status; }
    else if (body.produced_qty !== undefined) { action = 'production_report'; details = '생산수량: ' + body.produced_qty + ', 불량: ' + (body.defect_qty||0); }
    else if (body.cost_material !== undefined || body.cost_labor !== undefined) { action = 'cost_update'; details = '원가 업데이트'; }
    db.prepare(`INSERT INTO work_order_logs (wo_id, wo_number, action, from_status, to_status, qty_change, actor, details) VALUES (?,?,?,?,?,?,?,?)`).run(id, wo.wo_number, action, oldStatus, body.status||oldStatus, body.produced_qty||0, decoded.username||'', details);
    ok(res, { message: '업데이트 완료' }); return;
  }

  // ── GET /api/work-orders/stats ── 작업지시 통계
  if (pathname === '/api/work-orders/stats' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const statusSummary = db.prepare(`SELECT status, COUNT(*) AS cnt, SUM(ordered_qty) AS total_ordered, SUM(produced_qty) AS total_produced, SUM(defect_qty) AS total_defect, SUM(cost_total) AS total_cost FROM work_orders GROUP BY status`).all();
    const monthlyOrders = db.prepare(`SELECT strftime('%Y-%m', created_at) AS ym, COUNT(*) AS cnt, SUM(ordered_qty) AS total_qty FROM work_orders GROUP BY strftime('%Y-%m', created_at) ORDER BY ym DESC LIMIT 12`).all();
    const recentCompleted = db.prepare(`SELECT * FROM work_orders WHERE status='completed' ORDER BY completed_date DESC LIMIT 10`).all();
    ok(res, { statusSummary, monthlyOrders, recentCompleted }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  알림센터 API (Notifications)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/notifications' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const rows = db.prepare("SELECT * FROM notifications WHERE user_id IS NULL OR user_id = ? ORDER BY created_at DESC LIMIT 100").all(uid);
    ok(res, rows); return;
  }
  if (pathname === '/api/notifications/unread-count' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const r = db.prepare("SELECT COUNT(*) AS cnt FROM notifications WHERE (user_id IS NULL OR user_id = ?) AND is_read = 0").get(uid);
    ok(res, { count: r.cnt }); return;
  }
  if (pathname.match(/^\/api\/notifications\/read\/(\d+)$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/notifications\/read\/(\d+)$/)[1];
    db.prepare("UPDATE notifications SET is_read = 1 WHERE id = ?").run(id);
    ok(res, { updated: true }); return;
  }
  if (pathname === '/api/notifications/read-all' && method === 'POST') {
    const uid = currentUser ? currentUser.userId : 0;
    db.prepare("UPDATE notifications SET is_read = 1 WHERE (user_id IS NULL OR user_id = ?) AND is_read = 0").run(uid);
    ok(res, { updated: true }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  전자결재 API (Approvals)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/approvals/pending-count' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const r = db.prepare("SELECT COUNT(*) AS cnt FROM approval_lines al JOIN approvals a ON a.id=al.approval_id WHERE al.approver_id=? AND al.status='pending' AND a.status='pending'").get(uid);
    ok(res, { count: r.cnt }); return;
  }
  if (pathname === '/api/approvals' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const tab = qs.get('tab') || 'my'; // my/pending/done
    let rows = [];
    if (tab === 'my') rows = db.prepare("SELECT * FROM approvals WHERE requester_id=? ORDER BY created_at DESC LIMIT 200").all(uid);
    else if (tab === 'pending') rows = db.prepare("SELECT a.* FROM approvals a JOIN approval_lines al ON a.id=al.approval_id WHERE al.approver_id=? AND al.status='pending' AND a.status='pending' ORDER BY a.created_at DESC").all(uid);
    else rows = db.prepare("SELECT a.* FROM approvals a JOIN approval_lines al ON a.id=al.approval_id WHERE al.approver_id=? AND al.status IN ('approved','rejected') ORDER BY al.acted_at DESC LIMIT 200").all(uid);
    ok(res, rows); return;
  }
  if (pathname === '/api/approvals' && method === 'POST') {
    const body = await readJSON(req);
    const uid = currentUser ? currentUser.userId : 0;
    const uname = currentUser ? currentUser.username : 'system';
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const seq = db.prepare("SELECT COUNT(*) AS cnt FROM approvals WHERE approval_no LIKE ?").get('AP-'+today+'%').cnt + 1;
    const no = 'AP-'+today+'-'+String(seq).padStart(3,'0');
    const lines = body.lines || []; // [{approver_id, approver_name}]
    const info = db.prepare("INSERT INTO approvals (approval_no,doc_type,doc_ref,title,content,amount,status,requester_id,requester_name,current_step,total_steps) VALUES (?,?,?,?,?,?,?,?,?,1,?)").run(
      no, body.doc_type||'general', body.doc_ref||'', body.title||'', body.content||'', body.amount||0, 'pending', uid, uname, Math.max(lines.length,1));
    const aid = info.lastInsertRowid;
    lines.forEach(function(ln, i) {
      db.prepare("INSERT INTO approval_lines (approval_id,step_order,approver_id,approver_name,role) VALUES (?,?,?,?,?)").run(aid, i+1, ln.approver_id, ln.approver_name||'', ln.role||'approver');
    });
    // 첫 번째 결재자에게 알림
    if (lines.length > 0) createNotification(lines[0].approver_id, 'approval', '결재 요청: '+body.title, uname+'님이 결재를 요청했습니다.', 'approval');
    ok(res, { id: aid, approval_no: no }); return;
  }
  if (pathname.match(/^\/api\/approvals\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/approvals\/(\d+)$/)[1];
    const row = db.prepare("SELECT * FROM approvals WHERE id=?").get(id);
    if (!row) { fail(res, 404, '결재 문서를 찾을 수 없습니다'); return; }
    const lines = db.prepare("SELECT * FROM approval_lines WHERE approval_id=? ORDER BY step_order").all(id);
    ok(res, { ...row, lines }); return;
  }
  if (pathname.match(/^\/api\/approvals\/(\d+)\/approve$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/approvals\/(\d+)\/approve$/)[1];
    const body = await readJSON(req);
    const uid = currentUser ? currentUser.userId : 0;
    const ap = db.prepare("SELECT * FROM approvals WHERE id=?").get(id);
    if (!ap || ap.status !== 'pending') { fail(res, 400, '결재 불가 상태'); return; }
    const line = db.prepare("SELECT * FROM approval_lines WHERE approval_id=? AND step_order=? AND status='pending'").get(id, ap.current_step);
    if (!line) { fail(res, 403, '결재 권한이 없습니다'); return; }
    db.prepare("UPDATE approval_lines SET status='approved', comment=?, acted_at=datetime('now','localtime') WHERE id=?").run(body.comment||'', line.id);
    // 다음 단계 또는 최종 승인
    const nextLine = db.prepare("SELECT * FROM approval_lines WHERE approval_id=? AND step_order>? AND status='pending' ORDER BY step_order LIMIT 1").get(id, line.step_order);
    if (nextLine) {
      db.prepare("UPDATE approvals SET current_step=?, updated_at=datetime('now','localtime') WHERE id=?").run(nextLine.step_order, id);
      createNotification(nextLine.approver_id, 'approval', '결재 요청: '+ap.title, '다음 단계 결재를 요청합니다.', 'approval');
    } else {
      db.prepare("UPDATE approvals SET status='approved', updated_at=datetime('now','localtime') WHERE id=?").run(id);
      createNotification(ap.requester_id, 'approval', '결재 승인: '+ap.title, '요청하신 결재가 최종 승인되었습니다.', 'approval');
    }
    ok(res, { approved: true }); return;
  }
  if (pathname.match(/^\/api\/approvals\/(\d+)\/reject$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/approvals\/(\d+)\/reject$/)[1];
    const body = await readJSON(req);
    const uid = currentUser ? currentUser.userId : 0;
    const ap = db.prepare("SELECT * FROM approvals WHERE id=?").get(id);
    if (!ap || ap.status !== 'pending') { fail(res, 400, '결재 불가 상태'); return; }
    const line = db.prepare("SELECT * FROM approval_lines WHERE approval_id=? AND step_order=? AND status='pending'").get(id, ap.current_step);
    if (!line) { fail(res, 403, '결재 권한이 없습니다'); return; }
    db.prepare("UPDATE approval_lines SET status='rejected', comment=?, acted_at=datetime('now','localtime') WHERE id=?").run(body.comment||'', line.id);
    db.prepare("UPDATE approvals SET status='rejected', updated_at=datetime('now','localtime') WHERE id=?").run(id);
    createNotification(ap.requester_id, 'approval', '결재 반려: '+ap.title, (body.comment||'사유 없음'), 'approval');
    ok(res, { rejected: true }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  수주관리 API (Sales Orders)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/sales-orders/summary' && method === 'GET') {
    const quote_count = db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE order_type='quote' AND status NOT IN ('cancelled')").get().cnt;
    const order_amount = db.prepare("SELECT COALESCE(SUM(total_amount),0) AS amt FROM sales_orders WHERE order_type='sales' AND status NOT IN ('cancelled','delivered')").get().amt;
    const shipped_count = db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE status='shipped'").get().cnt;
    const unshipped_count = db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE order_type='sales' AND status IN ('draft','confirmed','in_production')").get().cnt;
    const bySource = db.prepare("SELECT source, COUNT(*) AS cnt, COALESCE(SUM(total_amount),0) AS amt FROM sales_orders GROUP BY source").all();
    const syncDD = db.prepare("SELECT last_sync, status FROM sync_meta WHERE key='sales-dd'").get();
    const syncXerp = db.prepare("SELECT last_sync, status FROM sync_meta WHERE key='sales-xerp'").get();
    ok(res, { quote_count, order_amount, shipped_count, unshipped_count, by_source: bySource, sync: { dd: syncDD, xerp: syncXerp } }); return;
  }
  if (pathname === '/api/sales-orders' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const status = qs.get('status') || '';
    const type = qs.get('type') || '';
    const search = qs.get('search') || '';
    let where = '1=1';
    if (status) where += " AND status='" + status.replace(/'/g,'') + "'";
    if (type) where += " AND order_type='" + type.replace(/'/g,'') + "'";
    if (search) where += " AND (order_no LIKE '%" + search.replace(/'/g,'') + "%' OR customer_name LIKE '%" + search.replace(/'/g,'') + "%')";
    const rows = db.prepare('SELECT * FROM sales_orders WHERE '+where+' ORDER BY created_at DESC LIMIT 200').all();
    ok(res, rows); return;
  }
  if (pathname === '/api/sales-orders' && method === 'POST') {
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const prefix = (body.order_type === 'quote') ? 'QT' : 'SO';
    const seq = db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE order_no LIKE ?").get(prefix+'-'+today+'%').cnt + 1;
    const no = prefix+'-'+today+'-'+String(seq).padStart(3,'0');
    const items = body.items || [];
    const totalQty = items.reduce(function(s,i){return s+(i.qty||0);},0);
    const totalAmt = items.reduce(function(s,i){return s+(i.amount||(i.qty||0)*(i.unit_price||0));},0);
    const info = db.prepare("INSERT INTO sales_orders (order_no,order_type,status,customer_name,customer_contact,customer_tel,order_date,delivery_date,total_qty,total_amount,tax_amount,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      no, body.order_type||'quote', 'draft', body.customer_name||'', body.customer_contact||'', body.customer_tel||'', body.order_date||new Date().toISOString().slice(0,10), body.delivery_date||'', totalQty, totalAmt, body.tax_amount||0, body.notes||'', uname);
    const oid = info.lastInsertRowid;
    items.forEach(function(it) {
      db.prepare("INSERT INTO sales_order_items (order_id,product_code,product_name,spec,unit_price,qty,amount,notes) VALUES (?,?,?,?,?,?,?,?)").run(oid, it.product_code||'', it.product_name||'', it.spec||'', it.unit_price||0, it.qty||0, it.amount||(it.qty||0)*(it.unit_price||0), it.notes||'');
    });
    ok(res, { id: oid, order_no: no }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)$/)[1];
    const row = db.prepare("SELECT * FROM sales_orders WHERE id=?").get(id);
    if (!row) { fail(res, 404, '수주를 찾을 수 없습니다'); return; }
    const items = db.prepare("SELECT * FROM sales_order_items WHERE order_id=?").all(id);
    ok(res, { ...row, items }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)$/) && method === 'PUT') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)$/)[1];
    const body = await readJSON(req);
    db.prepare("UPDATE sales_orders SET customer_name=COALESCE(?,customer_name), customer_contact=COALESCE(?,customer_contact), customer_tel=COALESCE(?,customer_tel), delivery_date=COALESCE(?,delivery_date), notes=COALESCE(?,notes), updated_at=datetime('now','localtime') WHERE id=?").run(body.customer_name, body.customer_contact, body.customer_tel, body.delivery_date, body.notes, id);
    if (body.items) {
      db.prepare("DELETE FROM sales_order_items WHERE order_id=?").run(id);
      body.items.forEach(function(it) { db.prepare("INSERT INTO sales_order_items (order_id,product_code,product_name,spec,unit_price,qty,amount,notes) VALUES (?,?,?,?,?,?,?,?)").run(id, it.product_code||'', it.product_name||'', it.spec||'', it.unit_price||0, it.qty||0, it.amount||0, it.notes||''); });
      const totalQty = body.items.reduce(function(s,i){return s+(i.qty||0);},0);
      const totalAmt = body.items.reduce(function(s,i){return s+(i.amount||0);},0);
      db.prepare("UPDATE sales_orders SET total_qty=?, total_amount=?, updated_at=datetime('now','localtime') WHERE id=?").run(totalQty, totalAmt, id);
    }
    ok(res, { updated: true }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)\/confirm$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)\/confirm$/)[1];
    const row = db.prepare("SELECT * FROM sales_orders WHERE id=?").get(id);
    if (!row) { fail(res, 404, '문서 없음'); return; }
    const newType = row.order_type === 'quote' ? 'sales' : row.order_type;
    db.prepare("UPDATE sales_orders SET order_type=?, status='confirmed', updated_at=datetime('now','localtime') WHERE id=?").run(newType, id);
    ok(res, { confirmed: true }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)\/ship$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)\/ship$/)[1];
    db.prepare("UPDATE sales_orders SET status='shipped', shipped_date=date('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?").run(id);
    ok(res, { shipped: true }); return;
  }

  // ── 수주관리: DD 주문 자동 동기화 ──
  if (pathname === '/api/sales-orders/sync-dd' && method === 'POST') {
    const pool = await ensureDdPool();
    if (!pool) { fail(res, 503, 'DD 데이터베이스 미연결'); return; }
    try {
      const [rows] = await pool.query(`SELECT o.id, o.order_number, o.order_state, o.shipping_state,
        o.total_money, o.paid_money, o.delivery_price, o.created_at, o.cj_invoice_numbers,
        GROUP_CONCAT(DISTINCT oi.product_name SEPARATOR ', ') AS product_names,
        SUM(oi.qty) AS total_qty
        FROM orders o LEFT JOIN order_items oi ON oi.order_id = o.id
        WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY) AND o.order_state != 'C'
        GROUP BY o.id ORDER BY o.created_at DESC LIMIT 500`);
      const stateMap = {B:'draft', P:'confirmed', D:'shipped', F:'delivered', C:'cancelled'};
      const upsert = db.prepare(`INSERT INTO sales_orders (order_no,order_type,status,customer_name,total_qty,total_amount,order_date,shipped_date,notes,source,external_id,created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(order_no) DO UPDATE SET status=excluded.status, total_qty=excluded.total_qty, total_amount=excluded.total_amount, shipped_date=excluded.shipped_date, updated_at=datetime('now','localtime')`);
      const tx = db.transaction(function() {
        rows.forEach(function(r) {
          const oNo = 'DD-' + (r.order_number || r.id);
          const st = stateMap[r.order_state] || 'draft';
          const shipDate = (r.shipping_state === 'Y' || r.order_state === 'D') ? (r.created_at ? r.created_at.toISOString().slice(0,10) : '') : '';
          upsert.run(oNo, 'sales', st, 'DD고객', r.total_qty||0, r.total_money||0,
            r.created_at ? r.created_at.toISOString().slice(0,10) : '', shipDate,
            (r.product_names||'').substring(0,200), 'dd', String(r.id));
        });
      });
      tx();
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('sales-dd',datetime('now','localtime'),?,'ok')").run(rows.length);
      ok(res, { synced: rows.length, source: 'DD' }); return;
    } catch(e) {
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,status,message) VALUES ('sales-dd',datetime('now','localtime'),'error',?)").run(e.message);
      fail(res, 500, 'DD 동기화 실패: ' + e.message); return;
    }
  }
  if (pathname === '/api/sales-orders/sync-xerp' && method === 'POST') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결'); return; }
    try {
      const end = new Date(); const start = new Date(); start.setDate(start.getDate() - 90);
      const fmt = d => d.toISOString().slice(0,10).replace(/-/g,'');
      const r = await xerpPool.request()
        .input('s', sql.NVarChar(16), fmt(start))
        .input('e', sql.NVarChar(16), fmt(end))
        .query(`SELECT h_date, h_orderid, DeptGubun, b_goodCode, b_OrderNum, h_sumPrice, h_offerPrice
          FROM ERP_SalesData WITH (NOLOCK) WHERE h_date >= @s AND h_date <= @e ORDER BY h_date DESC`);
      const rows = r.recordset || [];
      // 일별 집계로 수주 데이터 생성
      const byDate = {};
      rows.forEach(function(row) {
        const d = (row.h_date||'').toString().trim();
        if (!byDate[d]) byDate[d] = { count: 0, amount: 0, qty: 0, dept: new Set() };
        byDate[d].count++;
        byDate[d].amount += (row.h_sumPrice || 0);
        byDate[d].qty += (row.b_OrderNum || 0);
        if (row.DeptGubun) byDate[d].dept.add(row.DeptGubun);
      });
      const upsert = db.prepare(`INSERT INTO sales_orders (order_no,order_type,status,customer_name,total_qty,total_amount,order_date,notes,source,external_id,created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(order_no) DO UPDATE SET total_qty=excluded.total_qty, total_amount=excluded.total_amount, notes=excluded.notes, updated_at=datetime('now','localtime')`);
      const tx = db.transaction(function() {
        Object.keys(byDate).forEach(function(d) {
          const v = byDate[d];
          const dateStr = d.length===8 ? d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8) : d;
          upsert.run('XERP-'+d, 'sales', 'delivered', 'XERP매출', v.qty, v.amount, dateStr,
            '채널: '+Array.from(v.dept).join(',')+'  건수: '+v.count, 'xerp', d, 'sync');
        });
      });
      tx();
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('sales-xerp',datetime('now','localtime'),?,'ok')").run(Object.keys(byDate).length);
      ok(res, { synced: Object.keys(byDate).length, raw_records: rows.length, source: 'XERP' }); return;
    } catch(e) {
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,status,message) VALUES ('sales-xerp',datetime('now','localtime'),'error',?)").run(e.message);
      fail(res, 500, 'XERP 동기화 실패: ' + e.message); return;
    }
  }
  if (pathname === '/api/sync-meta' && method === 'GET') {
    const rows = db.prepare("SELECT * FROM sync_meta ORDER BY key").all();
    ok(res, rows); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Lot/시리얼 추적 API (Lot Tracking)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/lots/summary' && method === 'GET') {
    const total = db.prepare("SELECT COUNT(*) AS cnt FROM batch_master").get().cnt;
    const active = db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE current_qty > 0").get().cnt;
    const held = db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE quality_status='HOLD'").get().cnt;
    const totalQty = db.prepare("SELECT COALESCE(SUM(current_qty),0) AS qty FROM batch_master").get().qty;
    const expiring = db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE exp_date IS NOT NULL AND exp_date != '' AND exp_date <= date('now','+30 days','localtime') AND current_qty > 0").get().cnt;
    ok(res, { total, active, held, totalQty, expiring }); return;
  }
  if (pathname === '/api/lots' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const product = qs.get('product') || '';
    const warehouse = qs.get('warehouse') || '';
    const status = qs.get('status') || '';
    const search = qs.get('search') || '';
    let where = '1=1';
    if (product) where += " AND product_code='" + product.replace(/'/g,'') + "'";
    if (warehouse) where += " AND warehouse='" + warehouse.replace(/'/g,'') + "'";
    if (status) where += " AND quality_status='" + status.replace(/'/g,'') + "'";
    if (search) where += " AND (batch_number LIKE '%" + search.replace(/'/g,'') + "%' OR product_name LIKE '%" + search.replace(/'/g,'') + "%')";
    const rows = db.prepare('SELECT * FROM batch_master WHERE '+where+' ORDER BY created_at DESC LIMIT 300').all();
    ok(res, rows); return;
  }
  if (pathname === '/api/lots' && method === 'POST') {
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const seq = db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE batch_number LIKE ?").get('LOT-'+today+'%').cnt + 1;
    const batchNo = 'LOT-'+today+'-'+String(seq).padStart(3,'0');
    const info = db.prepare("INSERT INTO batch_master (batch_number,product_code,product_name,vendor_name,vendor_lot,received_date,po_number,received_qty,current_qty,quality_status,warehouse,mfg_date,exp_date,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      batchNo, body.product_code||'', body.product_name||'', body.vendor_name||'', body.vendor_lot||'', body.received_date||new Date().toISOString().slice(0,10), body.po_number||'', body.received_qty||0, body.received_qty||0, body.quality_status||'GOOD', body.warehouse||'본사', body.mfg_date||'', body.exp_date||'', body.notes||'', uname);
    const bid = info.lastInsertRowid;
    db.prepare("INSERT INTO batch_transactions (batch_id,batch_number,txn_type,product_code,qty,qty_before,qty_after,to_warehouse,reference_no,actor,notes) VALUES (?,?,'receipt',?,?,0,?,?,?,?,?)").run(bid, batchNo, body.product_code||'', body.received_qty||0, body.received_qty||0, body.warehouse||'본사', body.po_number||'', uname, '입고 등록');
    ok(res, { id: bid, batch_number: batchNo }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/lots\/(\d+)$/)[1];
    const row = db.prepare("SELECT * FROM batch_master WHERE batch_id=?").get(id);
    if (!row) { fail(res, 404, 'Lot를 찾을 수 없습니다'); return; }
    const txns = db.prepare("SELECT * FROM batch_transactions WHERE batch_id=? ORDER BY created_at DESC").all(id);
    const inspections = db.prepare("SELECT * FROM batch_inspections WHERE batch_id=? ORDER BY insp_date DESC").all(id);
    ok(res, { ...row, transactions: txns, inspections }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)$/) && method === 'PUT') {
    const id = pathname.match(/^\/api\/lots\/(\d+)$/)[1];
    const body = await readJSON(req);
    db.prepare("UPDATE batch_master SET quality_status=COALESCE(?,quality_status), warehouse=COALESCE(?,warehouse), notes=COALESCE(?,notes), updated_at=datetime('now','localtime') WHERE batch_id=?").run(body.quality_status, body.warehouse, body.notes, id);
    ok(res, { updated: true }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)\/transaction$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/lots\/(\d+)\/transaction$/)[1];
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    const lot = db.prepare("SELECT * FROM batch_master WHERE batch_id=?").get(id);
    if (!lot) { fail(res, 404, 'Lot 없음'); return; }
    const qtyBefore = lot.current_qty;
    let qtyAfter = qtyBefore;
    const txnType = body.txn_type || 'usage';
    if (txnType === 'usage' || txnType === 'transfer') qtyAfter = qtyBefore - (body.qty || 0);
    else if (txnType === 'receipt' || txnType === 'return') qtyAfter = qtyBefore + (body.qty || 0);
    else if (txnType === 'quality_hold') qtyAfter = qtyBefore;
    db.prepare("INSERT INTO batch_transactions (batch_id,batch_number,txn_type,txn_date,from_warehouse,to_warehouse,product_code,qty,qty_before,qty_after,reference_no,actor,notes) VALUES (?,?,?,datetime('now','localtime'),?,?,?,?,?,?,?,?,?)").run(
      id, lot.batch_number, txnType, body.from_warehouse||lot.warehouse, body.to_warehouse||'', lot.product_code, body.qty||0, qtyBefore, qtyAfter, body.reference_no||'', uname, body.notes||'');
    db.prepare("UPDATE batch_master SET current_qty=?, warehouse=COALESCE(?,warehouse), updated_at=datetime('now','localtime') WHERE batch_id=?").run(qtyAfter, body.to_warehouse||null, id);
    if (txnType === 'quality_hold') db.prepare("UPDATE batch_master SET quality_status='HOLD' WHERE batch_id=?").run(id);
    ok(res, { txn_type: txnType, qty_before: qtyBefore, qty_after: qtyAfter }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)\/inspect$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/lots\/(\d+)\/inspect$/)[1];
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    db.prepare("INSERT INTO batch_inspections (batch_id,batch_number,insp_date,inspector,insp_type,sample_size,defects_found,defect_desc,result,next_action,notes,created_by) VALUES (?,?,datetime('now','localtime'),?,?,?,?,?,?,?,?,?)").run(
      id, body.batch_number||'', body.inspector||uname, body.insp_type||'RECEIVING', body.sample_size||0, body.defects_found||0, body.defect_desc||'', body.result||'PASS', body.next_action||'OK', body.notes||'', uname);
    if (body.result === 'FAIL') db.prepare("UPDATE batch_master SET quality_status='HOLD', updated_at=datetime('now','localtime') WHERE batch_id=?").run(id);
    else if (body.result === 'PASS') db.prepare("UPDATE batch_master SET quality_status='GOOD', updated_at=datetime('now','localtime') WHERE batch_id=?").run(id);
    ok(res, { inspected: true }); return;
  }
  if (pathname.match(/^\/api\/lots\/trace\/(.+)$/) && method === 'GET') {
    const bn = decodeURIComponent(pathname.match(/^\/api\/lots\/trace\/(.+)$/)[1]);
    const lot = db.prepare("SELECT * FROM batch_master WHERE batch_number=?").get(bn);
    if (!lot) { fail(res, 404, 'Lot 없음'); return; }
    const txns = db.prepare("SELECT * FROM batch_transactions WHERE batch_number=? ORDER BY created_at").all(bn);
    const insps = db.prepare("SELECT * FROM batch_inspections WHERE batch_number=? ORDER BY insp_date").all(bn);
    ok(res, { lot, transactions: txns, inspections: insps }); return;
  }

  // ── Lot 추적: XERP 입출고 자동 동기화 ──
  if (pathname === '/api/lots/sync-xerp' && method === 'POST') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결'); return; }
    try {
      // mmInventory에서 현재 재고 → Lot 생성/갱신
      const invR = await xerpPool.request().query(`
        SELECT RTRIM(ItemCode) AS item_code, RTRIM(ItemName) AS item_name,
          RTRIM(ItemStnd) AS item_spec, SUM(OhQty) AS oh_qty, RTRIM(WhCode) AS wh_code
        FROM mmInventory WITH (NOLOCK) WHERE SiteCode='BK10' AND OhQty > 0
        GROUP BY RTRIM(ItemCode), RTRIM(ItemName), RTRIM(ItemStnd), RTRIM(WhCode)`);
      const invRows = invR.recordset || [];
      const upsertLot = db.prepare(`INSERT INTO batch_master (batch_number,product_code,product_name,warehouse,received_qty,current_qty,quality_status,notes,created_by)
        VALUES (?,?,?,?,?,?,'GOOD','XERP 자동동기화','sync') ON CONFLICT(batch_number,product_code) DO UPDATE SET current_qty=excluded.current_qty, warehouse=excluded.warehouse, updated_at=datetime('now','localtime')`);
      const tx1 = db.transaction(function() {
        invRows.forEach(function(r) {
          const bn = 'XERP-' + (r.item_code||'').trim();
          upsertLot.run(bn, (r.item_code||'').trim(), (r.item_name||'').trim(), (r.wh_code||'BK10').trim(), r.oh_qty||0, r.oh_qty||0);
        });
      });
      tx1();
      // mmInoutItem에서 최근 30일 입출고 → 거래이력
      const end = new Date(); const start = new Date(); start.setDate(start.getDate() - 30);
      const fmt = d => d.toISOString().slice(0,10).replace(/-/g,'');
      const txnR = await xerpPool.request()
        .input('s', sql.NVarChar(16), fmt(start))
        .input('e', sql.NVarChar(16), fmt(end))
        .query(`SELECT RTRIM(ItemCode) AS item_code, RTRIM(ItemName) AS item_name,
          InoutDate, InoutGubun, SUM(InoutQty) AS qty, SUM(InoutAmnt) AS amt
          FROM mmInoutItem WITH (NOLOCK) WHERE SiteCode='BK10' AND InoutDate >= @s AND InoutDate <= @e
          GROUP BY RTRIM(ItemCode), RTRIM(ItemName), InoutDate, InoutGubun`);
      const txnRows = txnR.recordset || [];
      const typeMap = {MI:'receipt', MO:'usage', SO:'usage', SI:'return'};
      const insTxn = db.prepare(`INSERT OR IGNORE INTO batch_transactions (batch_number,txn_type,txn_date,product_code,qty,reference_no,actor,notes)
        VALUES (?,?,?,?,?,?,?,?)`);
      const tx2 = db.transaction(function() {
        txnRows.forEach(function(r) {
          const bn = 'XERP-' + (r.item_code||'').trim();
          const d = (r.InoutDate||'').toString().trim();
          const dateStr = d.length===8 ? d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8) : d;
          insTxn.run(bn, typeMap[r.InoutGubun]||'usage', dateStr, (r.item_code||'').trim(), r.qty||0, 'XERP-'+r.InoutGubun+'-'+d, 'sync', (r.item_name||'').trim());
        });
      });
      tx2();
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('lot-xerp',datetime('now','localtime'),?,'ok')").run(invRows.length);
      ok(res, { lots_synced: invRows.length, transactions_synced: txnRows.length, source: 'XERP' }); return;
    } catch(e) {
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,status,message) VALUES ('lot-xerp',datetime('now','localtime'),'error',?)").run(e.message);
      fail(res, 500, 'Lot XERP 동기화 실패: ' + e.message); return;
    }
  }

  // ── 예산관리: XERP GL 실적 자동 집계 ──
  if (pathname === '/api/budget/sync-actual' && method === 'POST') {
    try {
      const qs = new URL(req.url, 'http://localhost').searchParams;
      const year = qs.get('year') || new Date().getFullYear().toString();
      // gl_balance_cache 갱신 (이미 있으면 그걸 사용)
      const budgets = db.prepare("SELECT DISTINCT acc_code, month FROM budgets WHERE year=? AND acc_code IS NOT NULL AND acc_code != ''").all(year);
      let updated = 0;
      budgets.forEach(function(b) {
        const ym = year + '-' + b.month;
        const cache = db.prepare("SELECT period_dr, period_cr FROM gl_balance_cache WHERE acc_code=? AND year_month=?").get(b.acc_code, ym);
        if (cache) {
          const budget = db.prepare("SELECT budget_type FROM budgets WHERE year=? AND month=? AND acc_code=?").get(year, b.month, b.acc_code);
          const actual = budget && budget.budget_type === 'revenue' ? cache.period_cr : cache.period_dr;
          db.prepare("UPDATE budgets SET actual_amount=?, updated_at=datetime('now','localtime') WHERE year=? AND month=? AND acc_code=?").run(actual, year, b.month, b.acc_code);
          updated++;
        }
      });
      db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('budget-actual',datetime('now','localtime'),?,'ok')").run(updated);
      ok(res, { updated, source: 'GL Cache' }); return;
    } catch(e) {
      fail(res, 500, '예산 실적 동기화 실패: ' + e.message); return;
    }
  }

  // ════════════════════════════════════════════════════════════════════
  //  예산관리 API (Budget)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/budget/list' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const rows = db.prepare("SELECT * FROM budgets WHERE year=? ORDER BY month, acc_code").all(year);
    ok(res, rows); return;
  }
  if (pathname === '/api/budget/save' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    const upsert = db.prepare("INSERT INTO budgets (year,month,acc_code,acc_name,budget_type,budget_amount,notes) VALUES (?,?,?,?,?,?,?) ON CONFLICT(year,month,acc_code) DO UPDATE SET budget_amount=excluded.budget_amount, acc_name=excluded.acc_name, budget_type=excluded.budget_type, notes=excluded.notes, updated_at=datetime('now','localtime')");
    const tx = db.transaction(function() { items.forEach(function(it) { upsert.run(it.year, it.month, it.acc_code||'', it.acc_name||'', it.budget_type||'expense', it.budget_amount||0, it.notes||''); }); });
    tx();
    ok(res, { saved: items.length }); return;
  }
  if (pathname === '/api/budget/vs-actual' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const month = qs.get('month') || String(new Date().getMonth()+1).padStart(2,'0');
    const budgets = db.prepare("SELECT * FROM budgets WHERE year=? AND month=? ORDER BY acc_code").all(year, month);
    // 실적은 gl_balance_cache에서 가져오기 시도
    const ym = year + '-' + month;
    budgets.forEach(function(b) {
      const cache = db.prepare("SELECT period_dr, period_cr FROM gl_balance_cache WHERE acc_code=? AND year_month=?").get(b.acc_code, ym);
      if (cache) b.actual_amount = (b.budget_type === 'expense') ? cache.period_dr : cache.period_cr;
    });
    ok(res, budgets); return;
  }
  if (pathname === '/api/budget/summary' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const rows = db.prepare("SELECT month, budget_type, SUM(budget_amount) AS total_budget, SUM(actual_amount) AS total_actual FROM budgets WHERE year=? GROUP BY month, budget_type ORDER BY month").all(year);
    ok(res, rows); return;
  }
  if (pathname === '/api/cash/daily' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || new Date(Date.now()-30*86400000).toISOString().slice(0,10);
    const to = qs.get('to') || new Date().toISOString().slice(0,10);
    const rows = db.prepare("SELECT * FROM daily_cash WHERE cash_date BETWEEN ? AND ? ORDER BY cash_date, acc_code").all(from, to);
    ok(res, rows); return;
  }
  if (pathname === '/api/cash/daily' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [body];
    const upsert = db.prepare("INSERT INTO daily_cash (cash_date,acc_code,acc_name,inflow,outflow,balance,notes) VALUES (?,?,?,?,?,?,?) ON CONFLICT(cash_date,acc_code) DO UPDATE SET inflow=excluded.inflow, outflow=excluded.outflow, balance=excluded.balance, notes=excluded.notes");
    items.forEach(function(it) { upsert.run(it.cash_date, it.acc_code||'', it.acc_name||'', it.inflow||0, it.outflow||0, it.balance||0, it.notes||''); });
    ok(res, { saved: items.length }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  생산실적 API (Work Order Results)
  // ════════════════════════════════════════════════════════════════════

  if (pathname.match(/^\/api\/work-orders\/(\d+)\/result$/) && method === 'POST') {
    const woid = pathname.match(/^\/api\/work-orders\/(\d+)\/result$/)[1];
    const body = await readJSON(req);
    db.prepare("INSERT INTO work_order_results (work_order_id,result_date,good_qty,defect_qty,worker_name,work_hours,notes) VALUES (?,?,?,?,?,?,?)").run(
      woid, body.result_date||new Date().toISOString().slice(0,10), body.good_qty||0, body.defect_qty||0, body.worker_name||'', body.work_hours||0, body.notes||'');
    // 작업지시에 누적 반영
    const totals = db.prepare("SELECT COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect FROM work_order_results WHERE work_order_id=?").get(woid);
    db.prepare("UPDATE work_orders SET completed_qty=?, updated_at=datetime('now','localtime') WHERE id=?").run(totals.good, woid);
    ok(res, { saved: true, total_good: totals.good, total_defect: totals.defect }); return;
  }
  if (pathname.match(/^\/api\/work-orders\/(\d+)\/results$/) && method === 'GET') {
    const woid = pathname.match(/^\/api\/work-orders\/(\d+)\/results$/)[1];
    const rows = db.prepare("SELECT * FROM work_order_results WHERE work_order_id=? ORDER BY result_date DESC").all(woid);
    ok(res, rows); return;
  }
  if (pathname === '/api/work-orders/daily-report' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const date = qs.get('date') || new Date().toISOString().slice(0,10);
    const rows = db.prepare("SELECT r.*, w.wo_number, w.product_name FROM work_order_results r JOIN work_orders w ON w.wo_id=r.work_order_id WHERE r.result_date=? ORDER BY r.created_at DESC").all(date);
    const summary = db.prepare("SELECT COUNT(*) AS cnt, COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect, COALESCE(SUM(work_hours),0) AS hours FROM work_order_results WHERE result_date=?").get(date);
    ok(res, { total_good: summary.good, total_defect: summary.defect, total_hours: summary.hours, order_count: summary.cnt, results: rows }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  복식부기 회계 API (Double-Entry Bookkeeping)
  // ════════════════════════════════════════════════════════════════════

  // ── GET /api/acct/seed-accounts ── XERP에서 계정코드 추출 → SQLite 시드
  if (pathname === '/api/acct/seed-accounts' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const sources = { xerp: 'unknown', sqlite: 'ok' };
    let seeded = 0;
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const result = await pool.request().query(`
        SELECT DISTINCT RTRIM(AccCode) AS acc_code
        FROM glDocItem WITH (NOLOCK)
        WHERE SiteCode = 'BK10' AND AccCode IS NOT NULL AND RTRIM(AccCode) != ''
      `);
      const upsert = db.prepare(`INSERT INTO gl_account_map (acc_code, acc_name, acc_type, acc_group, parent_code, depth, sort_order, updated_at)
        VALUES (?,?,?,?,?,?,?,datetime('now','localtime'))
        ON CONFLICT(acc_code) DO UPDATE SET
          acc_name=CASE WHEN excluded.acc_name!='' THEN excluded.acc_name ELSE gl_account_map.acc_name END,
          acc_type=excluded.acc_type, acc_group=excluded.acc_group,
          parent_code=excluded.parent_code, depth=excluded.depth, sort_order=excluded.sort_order,
          updated_at=datetime('now','localtime')`);
      const tx = db.transaction(() => {
        for (const row of result.recordset) {
          const code = row.acc_code.trim();
          if (!code) continue;
          const cls = classifyAccount(code);
          upsert.run(code, cls.acc_name, cls.acc_type, cls.acc_group, cls.parent_code, cls.depth, cls.sort_order);
          seeded++;
        }
      });
      tx();
    } catch (e) {
      sources.xerp = 'error: ' + e.message;
    }
    const total = db.prepare('SELECT COUNT(*) AS cnt FROM gl_account_map').get().cnt;
    ok(res, { seeded, total, sources }); return;
  }

  // ── GET /api/acct/accounts ── 계정과목 트리 (SQLite)
  if (pathname === '/api/acct/accounts' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const accounts = db.prepare(`SELECT acc_code, acc_name, acc_type, acc_group, parent_code, depth, sort_order, is_active
      FROM gl_account_map ORDER BY sort_order, acc_code`).all();
    ok(res, { accounts, total: accounts.length }); return;
  }

  // ── PUT /api/acct/accounts/:code ── 계정명 수정
  if (pathname.match(/^\/api\/acct\/accounts\/(.+)$/) && method === 'PUT') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const code = decodeURIComponent(pathname.match(/^\/api\/acct\/accounts\/(.+)$/)[1]);
    const body = await readJSON(req);
    if (body.acc_name !== undefined) {
      db.prepare(`UPDATE gl_account_map SET acc_name=?, updated_at=datetime('now','localtime') WHERE acc_code=?`).run(body.acc_name, code);
    }
    ok(res, { message: '계정 수정 완료' }); return;
  }

  // ── GET /api/acct/account-stats ── XERP 계정별 거래 통계
  if (pathname === '/api/acct/account-stats' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const cacheKey = 'acctStats_' + year;
    if (acctStatsCache && acctStatsCache._key === cacheKey && Date.now() - acctStatsCacheTime < ACCT_CACHE_TTL && !qs.get('refresh')) {
      ok(res, acctStatsCache); return;
    }
    const sources = { xerp: 'unknown', sqlite: 'ok' };
    let stats = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const fromDate = year + '0101';
      const toDate = year + '1231';
      const result = await pool.request()
        .input('fromDate', fromDate).input('toDate', toDate)
        .query(`
          SELECT RTRIM(i.AccCode) AS acc_code,
                 COUNT(*) AS txn_count,
                 SUM(CASE WHEN i.DrCr = 'D' THEN i.DocAmnt ELSE 0 END) AS total_dr,
                 SUM(CASE WHEN i.DrCr = 'C' THEN i.DocAmnt ELSE 0 END) AS total_cr
          FROM glDocHeader h WITH (NOLOCK)
          JOIN glDocItem i WITH (NOLOCK) ON h.SiteCode = i.SiteCode AND h.DocNo = i.DocNo
          WHERE h.SiteCode = 'BK10' AND h.RelCheck = 'Y'
            AND h.RelDate >= @fromDate AND h.RelDate <= @toDate
          GROUP BY RTRIM(i.AccCode)
          ORDER BY SUM(i.DocAmnt) DESC
        `);
      stats = result.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    // 계정명 조인
    const accMap = {};
    db.prepare('SELECT acc_code, acc_name, acc_type, acc_group FROM gl_account_map').all().forEach(a => { accMap[a.acc_code] = a; });
    stats = stats.map(s => ({
      ...s,
      acc_name: (accMap[s.acc_code] || {}).acc_name || '',
      acc_type: (accMap[s.acc_code] || {}).acc_type || '',
      acc_group: (accMap[s.acc_code] || {}).acc_group || ''
    }));
    const resp = { year, stats, sources, _key: cacheKey };
    acctStatsCache = resp; acctStatsCacheTime = Date.now();
    ok(res, resp); return;
  }

  // ── GET /api/acct/vouchers ── 분개장 전표 목록
  if (pathname === '/api/acct/vouchers' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || (() => { const d = new Date(); d.setDate(d.getDate()-30); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const status = qs.get('status') || 'Y';
    const offset = parseInt(qs.get('offset') || '0', 10);
    const limit = Math.min(parseInt(qs.get('limit') || '100', 10), 500);
    const search = qs.get('search') || '';
    const sources = { xerp: 'unknown' };
    let vouchers = [], totalCount = 0;
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      let whereExtra = '';
      const req2 = pool.request().input('fromDate', from).input('toDate', to).input('offset', offset).input('limit', limit);
      if (status) { whereExtra += ' AND h.RelCheck = @status'; req2.input('status', status); }
      if (search) { whereExtra += " AND (h.DocNo LIKE @search OR h.DocDescr LIKE @search)"; req2.input('search', '%' + search + '%'); }
      const countResult = await req2.query(`
        SELECT COUNT(*) AS cnt FROM glDocHeader h WITH (NOLOCK)
        WHERE h.SiteCode = 'BK10' AND h.RelDate >= @fromDate AND h.RelDate <= @toDate ${whereExtra}
      `);
      totalCount = countResult.recordset[0].cnt;
      const req3 = pool.request().input('fromDate', from).input('toDate', to).input('offset', offset).input('limit', limit);
      if (status) req3.input('status', status);
      if (search) req3.input('search', '%' + search + '%');
      const result = await req3.query(`
        SELECT h.DocNo, h.DocDate, h.DocGubun, h.DocDescr, h.RelCheck, h.RelDate, h.OriginNo,
          (SELECT SUM(CASE WHEN i2.DrCr='D' THEN i2.DocAmnt ELSE 0 END)
           FROM glDocItem i2 WITH(NOLOCK) WHERE i2.SiteCode=h.SiteCode AND i2.DocNo=h.DocNo) AS total_debit,
          (SELECT SUM(CASE WHEN i2.DrCr='C' THEN i2.DocAmnt ELSE 0 END)
           FROM glDocItem i2 WITH(NOLOCK) WHERE i2.SiteCode=h.SiteCode AND i2.DocNo=h.DocNo) AS total_credit,
          (SELECT COUNT(*) FROM glDocItem i3 WITH(NOLOCK) WHERE i3.SiteCode=h.SiteCode AND i3.DocNo=h.DocNo) AS line_count
        FROM glDocHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10' AND h.RelDate >= @fromDate AND h.RelDate <= @toDate ${whereExtra}
        ORDER BY h.RelDate DESC, h.DocNo DESC
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
      `);
      vouchers = result.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    ok(res, { vouchers, totalCount, offset, limit, sources }); return;
  }

  // ── GET /api/acct/voucher/:docNo ── 전표 상세 (차변/대변)
  if (pathname.match(/^\/api\/acct\/voucher\/(.+)$/) && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const docNo = decodeURIComponent(pathname.match(/^\/api\/acct\/voucher\/(.+)$/)[1]);
    const sources = { xerp: 'unknown' };
    let header = null, items = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const hResult = await pool.request().input('docNo', docNo).query(`
        SELECT h.DocNo, h.DocDate, h.DocGubun, h.DocType, h.DocDescr,
               h.RelCheck, h.RelDate, h.EmpCode, h.OriginNo, h.DeptCode
        FROM glDocHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10' AND h.DocNo=@docNo
      `);
      if (hResult.recordset.length > 0) header = hResult.recordset[0];
      const iResult = await pool.request().input('docNo', docNo).query(`
        SELECT i.DocSerNo, RTRIM(i.AccCode) AS acc_code, i.DrCr, i.DocAmnt,
               i.DocDescr, RTRIM(i.CsCode) AS cs_code, i.VatBillNo, i.TeCode
        FROM glDocItem i WITH(NOLOCK)
        WHERE i.SiteCode='BK10' AND i.DocNo=@docNo
        ORDER BY i.DocSerNo
      `);
      items = iResult.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    // 계정명 + 거래처명 보강
    const accMap = {};
    db.prepare('SELECT acc_code, acc_name FROM gl_account_map').all().forEach(a => { accMap[a.acc_code] = a.acc_name; });
    const csMap = {};
    db.prepare('SELECT cs_code, cs_name FROM cs_code_cache').all().forEach(c => { csMap[c.cs_code] = c.cs_name; });
    items = items.map(it => ({
      ...it,
      acc_name: accMap[it.acc_code] || '',
      cs_name: csMap[it.cs_code] || ''
    }));
    const totalDr = items.filter(i => i.DrCr === 'D').reduce((s, i) => s + (i.DocAmnt || 0), 0);
    const totalCr = items.filter(i => i.DrCr === 'C').reduce((s, i) => s + (i.DocAmnt || 0), 0);
    const balanced = Math.abs(totalDr - totalCr) < 1;
    ok(res, { header, items, totalDr, totalCr, balanced, sources }); return;
  }

  // ── GET /api/acct/gl ── 총계정원장 (계정별 거래내역 + 잔액 누계)
  if (pathname === '/api/acct/gl' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const acc = qs.get('acc');
    if (!acc) { fail(res, 400, 'acc 파라미터 필요'); return; }
    const from = qs.get('from') || (() => { const d = new Date(); return d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + '01'; })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const csFilter = qs.get('cs') || '';
    const sources = { xerp: 'unknown' };
    let openingDr = 0, openingCr = 0, transactions = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      // 기초잔액: from 이전 모든 거래 합산
      const openReq = pool.request().input('acc', acc).input('fromDate', from);
      const openResult = await openReq.query(`
        SELECT SUM(CASE WHEN i.DrCr='D' THEN i.DocAmnt ELSE 0 END) AS total_dr,
               SUM(CASE WHEN i.DrCr='C' THEN i.DocAmnt ELSE 0 END) AS total_cr
        FROM glDocHeader h WITH(NOLOCK)
        JOIN glDocItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.DocNo=i.DocNo
        WHERE h.SiteCode='BK10' AND h.RelCheck='Y'
          AND RTRIM(i.AccCode)=@acc AND h.RelDate < @fromDate
      `);
      if (openResult.recordset[0]) {
        openingDr = openResult.recordset[0].total_dr || 0;
        openingCr = openResult.recordset[0].total_cr || 0;
      }
      // 당기 거래
      const txReq = pool.request().input('acc', acc).input('fromDate', from).input('toDate', to);
      let csWhere = '';
      if (csFilter) { csWhere = " AND RTRIM(i.CsCode) LIKE @csFilter"; txReq.input('csFilter', '%' + csFilter + '%'); }
      const txResult = await txReq.query(`
        SELECT h.DocNo, h.DocDate, h.DocDescr AS header_descr,
               i.DocSerNo, i.DrCr, i.DocAmnt, i.DocDescr AS item_descr,
               RTRIM(i.CsCode) AS cs_code, i.VatBillNo
        FROM glDocHeader h WITH(NOLOCK)
        JOIN glDocItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.DocNo=i.DocNo
        WHERE h.SiteCode='BK10' AND h.RelCheck='Y'
          AND RTRIM(i.AccCode)=@acc
          AND h.RelDate >= @fromDate AND h.RelDate <= @toDate ${csWhere}
        ORDER BY h.RelDate, h.DocNo, i.DocSerNo
      `);
      transactions = txResult.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    // 계정 유형 확인 (자산/비용은 Dr+, 부채/자본/수익은 Cr+)
    const accInfo = db.prepare('SELECT acc_type, acc_name FROM gl_account_map WHERE acc_code=?').get(acc) || {};
    const isDebitNature = ['asset', 'expense'].includes(accInfo.acc_type);
    // 잔액 누계 계산
    let runBal = isDebitNature ? (openingDr - openingCr) : (openingCr - openingDr);
    const openingBalance = runBal;
    transactions = transactions.map(t => {
      if (t.DrCr === 'D') runBal += t.DocAmnt;
      else runBal -= (isDebitNature ? t.DocAmnt : -t.DocAmnt);
      if (!isDebitNature) {
        if (t.DrCr === 'D') runBal = openingBalance + transactions.filter(x => x === t || transactions.indexOf(x) < transactions.indexOf(t)).reduce((s, x) => s + (x.DrCr === 'C' ? x.DocAmnt : -x.DocAmnt), 0);
      }
      return { ...t, balance: runBal };
    });
    // 재계산: 정확한 러닝밸런스
    let rb = openingBalance;
    transactions = transactions.map(t => {
      if (isDebitNature) rb += (t.DrCr === 'D' ? t.DocAmnt : -t.DocAmnt);
      else rb += (t.DrCr === 'C' ? t.DocAmnt : -t.DocAmnt);
      return { ...t, balance: rb };
    });
    const closingBalance = rb;
    const periodDr = transactions.filter(t => t.DrCr === 'D').reduce((s, t) => s + (t.DocAmnt || 0), 0);
    const periodCr = transactions.filter(t => t.DrCr === 'C').reduce((s, t) => s + (t.DocAmnt || 0), 0);
    ok(res, { acc, acc_name: accInfo.acc_name || '', acc_type: accInfo.acc_type || '',
      openingBalance, closingBalance, periodDr, periodCr, isDebitNature,
      transactions, count: transactions.length, sources }); return;
  }

  // ── GET /api/acct/trial-balance ── 시산표
  if (pathname === '/api/acct/trial-balance' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const month = qs.get('month') || String(new Date().getMonth() + 1);
    const fromDate = year + String(month).padStart(2, '0') + '01';
    const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate();
    const toDate = year + String(month).padStart(2, '0') + String(lastDay);
    const fiscalStart = year + '0101';
    const cacheKey = 'tb_' + fromDate;
    if (trialBalanceCache && trialBalanceCache._key === cacheKey && Date.now() - trialBalanceCacheTime < ACCT_CACHE_TTL && !qs.get('refresh')) {
      ok(res, trialBalanceCache); return;
    }
    const sources = { xerp: 'unknown', sqlite: 'ok' };
    let periodData = [], priorData = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      // 당기 발생액
      const pResult = await pool.request().input('fromDate', fromDate).input('toDate', toDate).query(`
        SELECT RTRIM(i.AccCode) AS acc_code,
               SUM(CASE WHEN i.DrCr='D' THEN i.DocAmnt ELSE 0 END) AS period_dr,
               SUM(CASE WHEN i.DrCr='C' THEN i.DocAmnt ELSE 0 END) AS period_cr
        FROM glDocHeader h WITH(NOLOCK)
        JOIN glDocItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.DocNo=i.DocNo
        WHERE h.SiteCode='BK10' AND h.RelCheck='Y'
          AND h.RelDate >= @fromDate AND h.RelDate <= @toDate
        GROUP BY RTRIM(i.AccCode)
      `);
      periodData = pResult.recordset;
      // 기초잔액 (회계연도 시작~당월 직전)
      if (fromDate !== fiscalStart) {
        const beforeDate = fromDate; // fromDate 미만
        const oResult = await pool.request().input('fiscalStart', fiscalStart).input('beforeDate', beforeDate).query(`
          SELECT RTRIM(i.AccCode) AS acc_code,
                 SUM(CASE WHEN i.DrCr='D' THEN i.DocAmnt ELSE 0 END) AS prior_dr,
                 SUM(CASE WHEN i.DrCr='C' THEN i.DocAmnt ELSE 0 END) AS prior_cr
          FROM glDocHeader h WITH(NOLOCK)
          JOIN glDocItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.DocNo=i.DocNo
          WHERE h.SiteCode='BK10' AND h.RelCheck='Y'
            AND h.RelDate >= @fiscalStart AND h.RelDate < @beforeDate
          GROUP BY RTRIM(i.AccCode)
        `);
        priorData = oResult.recordset;
      }
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    // 계정명 매핑
    const accMap = {};
    db.prepare('SELECT acc_code, acc_name, acc_type, acc_group, sort_order FROM gl_account_map').all()
      .forEach(a => { accMap[a.acc_code] = a; });
    // 통합
    const allCodes = new Set([...periodData.map(p => p.acc_code), ...priorData.map(p => p.acc_code)]);
    const priorMap = {}; priorData.forEach(p => { priorMap[p.acc_code] = p; });
    const periodMap = {}; periodData.forEach(p => { periodMap[p.acc_code] = p; });
    const rows = [];
    for (const code of allCodes) {
      const info = accMap[code] || classifyAccount(code);
      const prior = priorMap[code] || { prior_dr: 0, prior_cr: 0 };
      const period = periodMap[code] || { period_dr: 0, period_cr: 0 };
      const isDebitNature = ['asset', 'expense'].includes(info.acc_type);
      const openBal = isDebitNature ? (prior.prior_dr - prior.prior_cr) : (prior.prior_cr - prior.prior_dr);
      const periodNet = isDebitNature ? (period.period_dr - period.period_cr) : (period.period_cr - period.period_dr);
      const closeBal = openBal + periodNet;
      rows.push({
        acc_code: code,
        acc_name: info.acc_name || '',
        acc_type: info.acc_type || '',
        acc_group: info.acc_group || '',
        sort_order: info.sort_order || 9999,
        opening_dr: prior.prior_dr || 0, opening_cr: prior.prior_cr || 0,
        period_dr: period.period_dr || 0, period_cr: period.period_cr || 0,
        closing_dr: (prior.prior_dr || 0) + (period.period_dr || 0),
        closing_cr: (prior.prior_cr || 0) + (period.period_cr || 0),
        opening_balance: openBal, closing_balance: closeBal
      });
    }
    rows.sort((a, b) => (a.sort_order - b.sort_order) || a.acc_code.localeCompare(b.acc_code));
    const totals = {
      opening_dr: rows.reduce((s, r) => s + r.opening_dr, 0),
      opening_cr: rows.reduce((s, r) => s + r.opening_cr, 0),
      period_dr: rows.reduce((s, r) => s + r.period_dr, 0),
      period_cr: rows.reduce((s, r) => s + r.period_cr, 0),
      closing_dr: rows.reduce((s, r) => s + r.closing_dr, 0),
      closing_cr: rows.reduce((s, r) => s + r.closing_cr, 0),
    };
    totals.balanced = Math.abs(totals.period_dr - totals.period_cr) < 1;
    const resp = { year, month, rows, totals, count: rows.length, sources, _key: cacheKey };
    trialBalanceCache = resp; trialBalanceCacheTime = Date.now();
    // 캐시 저장
    const upsertBal = db.prepare(`INSERT INTO gl_balance_cache (acc_code,year_month,opening_dr,opening_cr,period_dr,period_cr,closing_dr,closing_cr)
      VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(acc_code,year_month) DO UPDATE SET
      opening_dr=excluded.opening_dr,opening_cr=excluded.opening_cr,
      period_dr=excluded.period_dr,period_cr=excluded.period_cr,
      closing_dr=excluded.closing_dr,closing_cr=excluded.closing_cr,
      cached_at=datetime('now','localtime')`);
    const txBal = db.transaction(() => {
      const ym = year + String(month).padStart(2, '0');
      for (const r of rows) { upsertBal.run(r.acc_code, ym, r.opening_dr, r.opening_cr, r.period_dr, r.period_cr, r.closing_dr, r.closing_cr); }
    });
    try { txBal(); } catch (e) { /* 캐시 저장 실패는 무시 */ }
    ok(res, resp); return;
  }

  // ── GET /api/acct/financial-statements ── 재무제표 (BS + IS)
  if (pathname === '/api/acct/financial-statements' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const month = qs.get('month') || String(new Date().getMonth() + 1);
    const compare = qs.get('compare') || ''; // mom, yoy, ''
    const sources = { xerp: 'unknown', sqlite: 'ok' };

    // 내부 함수: 특정 월의 시산표 데이터 가져오기
    async function getTrialData(y, m) {
      const fromDate = y + String(m).padStart(2, '0') + '01';
      const lastDay = new Date(parseInt(y), parseInt(m), 0).getDate();
      const toDate = y + String(m).padStart(2, '0') + String(lastDay);
      const fiscalStart = y + '0101';
      try {
        const pool = await ensureXerpPool();
        sources.xerp = 'ok';
        const [pRes, oRes] = await Promise.all([
          pool.request().input('f', fromDate).input('t', toDate).query(`
            SELECT RTRIM(i.AccCode) AS acc_code,
                   SUM(CASE WHEN i.DrCr='D' THEN i.DocAmnt ELSE 0 END) AS pd,
                   SUM(CASE WHEN i.DrCr='C' THEN i.DocAmnt ELSE 0 END) AS pc
            FROM glDocHeader h WITH(NOLOCK)
            JOIN glDocItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.DocNo=i.DocNo
            WHERE h.SiteCode='BK10' AND h.RelCheck='Y' AND h.RelDate>=@f AND h.RelDate<=@t
            GROUP BY RTRIM(i.AccCode)`),
          fromDate !== fiscalStart ?
            pool.request().input('fs', fiscalStart).input('bf', fromDate).query(`
              SELECT RTRIM(i.AccCode) AS acc_code,
                     SUM(CASE WHEN i.DrCr='D' THEN i.DocAmnt ELSE 0 END) AS od,
                     SUM(CASE WHEN i.DrCr='C' THEN i.DocAmnt ELSE 0 END) AS oc
              FROM glDocHeader h WITH(NOLOCK)
              JOIN glDocItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.DocNo=i.DocNo
              WHERE h.SiteCode='BK10' AND h.RelCheck='Y' AND h.RelDate>=@fs AND h.RelDate<@bf
              GROUP BY RTRIM(i.AccCode)`)
            : Promise.resolve({ recordset: [] })
        ]);
        return { period: pRes.recordset, prior: oRes.recordset };
      } catch (e) { sources.xerp = 'error: ' + e.message; return { period: [], prior: [] }; }
    }

    const accMap = {};
    db.prepare('SELECT acc_code, acc_name, acc_type, acc_group, sort_order FROM gl_account_map').all()
      .forEach(a => { accMap[a.acc_code] = a; });

    function buildStatement(periodArr, priorArr) {
      const priorMap = {}; priorArr.forEach(p => { priorMap[p.acc_code] = p; });
      const bs = { assets: [], liabilities: [], equity: [], totalAssets: 0, totalLiabilities: 0, totalEquity: 0 };
      const is = { revenue: [], expense: [], totalRevenue: 0, totalExpense: 0, netIncome: 0 };
      const allCodes = new Set([...periodArr.map(p => p.acc_code), ...priorArr.map(p => p.acc_code)]);
      for (const code of allCodes) {
        const info = accMap[code] || classifyAccount(code);
        const prior = priorMap[code] || { od: 0, oc: 0 };
        const period = periodArr.find(p => p.acc_code === code) || { pd: 0, pc: 0 };
        const isDebitNature = ['asset', 'expense'].includes(info.acc_type);
        // BS 계정: 누적잔액 = 전기이월 + 당기
        // IS 계정: 당기 발생만
        const totalDr = (prior.od || 0) + (period.pd || 0);
        const totalCr = (prior.oc || 0) + (period.pc || 0);
        const balance = isDebitNature ? (totalDr - totalCr) : (totalCr - totalDr);
        const periodOnly = isDebitNature ? ((period.pd||0) - (period.pc||0)) : ((period.pc||0) - (period.pd||0));
        const row = { acc_code: code, acc_name: info.acc_name || code, acc_group: info.acc_group || '', balance, periodOnly, sort_order: info.sort_order || 9999 };
        if (info.acc_type === 'asset') { bs.assets.push(row); bs.totalAssets += balance; }
        else if (info.acc_type === 'liability') { bs.liabilities.push(row); bs.totalLiabilities += balance; }
        else if (info.acc_type === 'equity') { bs.equity.push(row); bs.totalEquity += balance; }
        else if (info.acc_type === 'revenue') { is.revenue.push(row); is.totalRevenue += periodOnly; }
        else if (info.acc_type === 'expense') { is.expense.push(row); is.totalExpense += periodOnly; }
      }
      is.netIncome = is.totalRevenue - is.totalExpense;
      bs.totalEquity += is.netIncome; // 당기순이익 반영
      bs.assets.sort((a, b) => a.sort_order - b.sort_order);
      bs.liabilities.sort((a, b) => a.sort_order - b.sort_order);
      bs.equity.sort((a, b) => a.sort_order - b.sort_order);
      is.revenue.sort((a, b) => a.sort_order - b.sort_order);
      is.expense.sort((a, b) => a.sort_order - b.sort_order);
      bs.balanced = Math.abs(bs.totalAssets - (bs.totalLiabilities + bs.totalEquity)) < 100;
      return { bs, is };
    }

    const current = await getTrialData(year, month);
    const stmt = buildStatement(current.period, current.prior);
    let compareStmt = null, compareLabel = '';
    if (compare === 'mom') {
      let cm = parseInt(month) - 1, cy = parseInt(year);
      if (cm < 1) { cm = 12; cy--; }
      const prev = await getTrialData(String(cy), String(cm));
      compareStmt = buildStatement(prev.period, prev.prior);
      compareLabel = cy + '년 ' + cm + '월';
    } else if (compare === 'yoy') {
      const prev = await getTrialData(String(parseInt(year) - 1), month);
      compareStmt = buildStatement(prev.period, prev.prior);
      compareLabel = (parseInt(year) - 1) + '년 ' + month + '월';
    }
    ok(res, { year, month, current: stmt, compare: compareStmt, compareLabel, sources }); return;
  }

  // ── GET /api/acct/ar-summary ── 채권/채무 요약
  if (pathname === '/api/acct/ar-summary' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || (() => { const d = new Date(); return d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + '01'; })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const arAp = qs.get('type') || 'AR'; // AR or AP
    const sources = { xerp: 'unknown' };
    let summary = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const result = await pool.request().input('from', from).input('to', to).input('arAp', arAp).query(`
        SELECT RTRIM(h.CsCode) AS cs_code,
               COUNT(*) AS bill_count,
               ISNULL(SUM(h.BillAmnt),0) AS total_billed,
               ISNULL(SUM(h.VatAmnt),0) AS total_vat,
               ISNULL(SUM(h.MoneySumAmnt),0) AS total_collected
        FROM rpBillHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10' AND h.ArApGubun=@arAp
          AND h.BillDate >= @from AND h.BillDate <= @to
        GROUP BY RTRIM(h.CsCode)
        ORDER BY SUM(h.BillAmnt) DESC
      `);
      summary = result.recordset.map(r => ({
        ...r,
        outstanding: (r.total_billed + r.total_vat) - r.total_collected
      }));
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    const totals = {
      total_billed: summary.reduce((s, r) => s + r.total_billed, 0),
      total_vat: summary.reduce((s, r) => s + r.total_vat, 0),
      total_collected: summary.reduce((s, r) => s + r.total_collected, 0),
      outstanding: summary.reduce((s, r) => s + r.outstanding, 0),
    };
    ok(res, { type: arAp, summary, totals, sources }); return;
  }

  // ── GET /api/acct/aging ── 채권 에이징 분석
  if (pathname === '/api/acct/aging' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const sources = { xerp: 'unknown' };
    const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const d30 = (() => { const d = new Date(); d.setDate(d.getDate() - 30); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const d60 = (() => { const d = new Date(); d.setDate(d.getDate() - 60); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const d90 = (() => { const d = new Date(); d.setDate(d.getDate() - 90); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    let aging = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const result = await pool.request()
        .input('today', today).input('d30', d30).input('d60', d60).input('d90', d90)
        .query(`
          SELECT RTRIM(me.CsCode) AS cs_code,
            SUM(CASE WHEN me.ExpectDate >= @today THEN me.ExpectRemainAmnt ELSE 0 END) AS current_amt,
            SUM(CASE WHEN me.ExpectDate < @today AND me.ExpectDate >= @d30 THEN me.ExpectRemainAmnt ELSE 0 END) AS days_30,
            SUM(CASE WHEN me.ExpectDate < @d30 AND me.ExpectDate >= @d60 THEN me.ExpectRemainAmnt ELSE 0 END) AS days_60,
            SUM(CASE WHEN me.ExpectDate < @d60 AND me.ExpectDate >= @d90 THEN me.ExpectRemainAmnt ELSE 0 END) AS days_90,
            SUM(CASE WHEN me.ExpectDate < @d90 THEN me.ExpectRemainAmnt ELSE 0 END) AS over_90,
            SUM(me.ExpectRemainAmnt) AS total_outstanding
          FROM rpMoneyExpect me WITH(NOLOCK)
          WHERE me.SiteCode='BK10' AND me.ExpectRemainAmnt > 0
          GROUP BY RTRIM(me.CsCode)
          HAVING SUM(me.ExpectRemainAmnt) > 0
          ORDER BY SUM(me.ExpectRemainAmnt) DESC
        `);
      aging = result.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    const totals = {
      current_amt: aging.reduce((s, r) => s + (r.current_amt || 0), 0),
      days_30: aging.reduce((s, r) => s + (r.days_30 || 0), 0),
      days_60: aging.reduce((s, r) => s + (r.days_60 || 0), 0),
      days_90: aging.reduce((s, r) => s + (r.days_90 || 0), 0),
      over_90: aging.reduce((s, r) => s + (r.over_90 || 0), 0),
      total: aging.reduce((s, r) => s + (r.total_outstanding || 0), 0),
    };
    ok(res, { aging, totals, sources }); return;
  }

  // ── GET /api/acct/ar-detail ── 거래처별 채권/채무 상세
  if (pathname === '/api/acct/ar-detail' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const cs = qs.get('cs');
    if (!cs) { fail(res, 400, 'cs 파라미터 필요'); return; }
    const from = qs.get('from') || (() => { const d = new Date(); d.setMonth(d.getMonth()-3); return d.toISOString().slice(0,10).replace(/-/g,''); })();
    const to = qs.get('to') || new Date().toISOString().slice(0,10).replace(/-/g,'');
    const sources = { xerp: 'unknown' };
    let bills = [], payments = [];
    try {
      const pool = await ensureXerpPool();
      sources.xerp = 'ok';
      const bResult = await pool.request().input('cs', cs).input('from', from).input('to', to).query(`
        SELECT h.BillNo, h.BillDate, h.ArApGubun,
               ISNULL(h.BillAmnt,0) AS bill_amt, ISNULL(h.VatAmnt,0) AS vat_amt,
               ISNULL(h.MoneySumAmnt,0) AS collected, h.BillDescr
        FROM rpBillHeader h WITH(NOLOCK)
        WHERE h.SiteCode='BK10' AND RTRIM(h.CsCode)=@cs
          AND h.BillDate >= @from AND h.BillDate <= @to
        ORDER BY h.BillDate DESC
      `);
      bills = bResult.recordset;
      const pResult = await pool.request().input('cs', cs).query(`
        SELECT ma.OriginNo, ma.AllocDate, ISNULL(ma.AllocAmnt,0) AS alloc_amt, ma.PayCode, me.ArApGubun
        FROM rpExpectMoneyAlloc ma WITH(NOLOCK)
        JOIN rpMoneyExpect me WITH(NOLOCK) ON ma.SiteCode=me.SiteCode AND ma.OriginNo=me.OriginNo AND ma.OriginSerNo=me.OriginSerNo
        WHERE me.SiteCode='BK10' AND RTRIM(me.CsCode)=@cs
        ORDER BY ma.AllocDate DESC
        OFFSET 0 ROWS FETCH NEXT 200 ROWS ONLY
      `);
      payments = pResult.recordset;
    } catch (e) { sources.xerp = 'error: ' + e.message; }
    ok(res, { cs_code: cs, bills, payments, sources }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  STATIC FILE ROUTES (existing, preserved)
  // ════════════════════════════════════════════════════════════════════

  let filePath;
  if (pathname === '/' || pathname === '/index.html') {
    filePath = path.join(__dir, 'app.html');
  } else if (pathname === '/legacy') {
    filePath = path.join(__dir, 'dashboard.html');
  } else if (pathname === '/inventory') {
    filePath = path.join(__dir, 'smart_inv_erp.html');
  } else if (pathname === '/order') {
    filePath = path.join(__dir, 'order_page.html');
  } else if (pathname === '/data.json') {
    filePath = path.join(__dir, 'erp_smart_inventory.json');
  } else if (pathname.startsWith('/receiver')) {
    filePath = path.join(__dir, 'receiver.html');
  } else if (pathname === '/product_info.json') {
    filePath = path.join(__dir, 'product_info.json');
  } else if (pathname === '/monthly_sales.json') {
    filePath = path.join(__dir, 'monthly_sales.json');
  } else if (pathname === '/email_preview.html') {
    filePath = path.join(__dir, 'email_preview.html');
  } else if (pathname === '/gift-set') {
    filePath = path.join(__dir, 'gift_set.html');
  } else {
    fail(res, 404, 'Not Found');
    return;
  }

  if (!fs.existsSync(filePath)) {
    fail(res, 404, 'File not found: ' + path.basename(filePath));
    return;
  }

  const ext = path.extname(filePath);
  const headers = {
    'Content-Type': MIME[ext] || 'text/plain',
    ...CORS,
  };
  // HTML 파일은 캐시 비활성화 (항상 최신 버전 로드)
  if (ext === '.html') {
    headers['Cache-Control'] = 'no-cache, no-store, must-revalidate';
    headers['Pragma'] = 'no-cache';
    headers['Expires'] = '0';
  }
  res.writeHead(200, headers);
  fs.createReadStream(filePath).pipe(res);
}

// ── 자동발주 스케줄러 ──────────────────────────────────
async function runAutoOrderScheduler() {
  console.log(`[자동발주 스케줄러] ${new Date().toLocaleString('ko-KR')} 실행 시작`);

  const items = db.prepare('SELECT * FROM auto_order_items WHERE enabled=1').all();
  if (!items.length) {
    console.log('[자동발주 스케줄러] 자동발주 설정 품목 없음');
    return;
  }

  // 재고 데이터 로드
  let inv = [];
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(__dir, 'erp_smart_inventory.json'), 'utf8'));
    inv = raw.products || raw.data || (Array.isArray(raw) ? raw : []);
  } catch(e) {
    console.log('[자동발주 스케줄러] 재고 데이터 로드 실패:', e.message);
    return;
  }
  const invMap = {};
  for (const p of inv) invMap[p['품목코드']] = p;

  // 예상 소진일 기준 정렬 (빠른 순 = 긴급한 것 먼저)
  items.sort((a, b) => {
    const pa = invMap[a.product_code] || invMap[(a.product_code||'').toUpperCase()];
    const pb = invMap[b.product_code] || invMap[(b.product_code||'').toUpperCase()];
    const dailyA = pa ? (pa['_xerpDaily'] || 0) : 0;
    const dailyB = pb ? (pb['_xerpDaily'] || 0) : 0;
    const availA = pa ? (typeof pa['가용재고'] === 'number' ? pa['가용재고'] : 0) : 0;
    const availB = pb ? (typeof pb['가용재고'] === 'number' ? pb['가용재고'] : 0) : 0;
    const remainA = dailyA > 0 ? availA / dailyA : 9999;
    const remainB = dailyB > 0 ? availB / dailyB : 9999;
    return remainA - remainB;
  });

  const today = new Date().toISOString().slice(0, 10);
  let createdCount = 0;

  // 이번 주 월요일
  const nowSch = new Date();
  const dowSch = nowSch.getDay() || 7;
  const mondaySch = new Date(nowSch); mondaySch.setDate(nowSch.getDate() - dowSch + 1); mondaySch.setHours(0,0,0,0);
  const mondayStrSch = mondaySch.toISOString().slice(0, 10);
  const weeklyVendorCountSch = {};

  for (const item of items) {
    const p = invMap[item.product_code] || invMap[(item.product_code||'').toUpperCase()];
    const avail = p ? (typeof p['가용재고'] === 'number' ? p['가용재고'] : 0) : null;
    if (avail === null) continue;

    const daily = p ? (p['_xerpDaily'] || 0) : 0;
    const monthly = p['_xerpMonthly'] || (p._xerpTotal3m ? Math.round(p._xerpTotal3m / 3) : 0);
    if (monthly <= 0) continue;

    const remainDays = daily > 0 ? avail / daily : 9999;
    const isUrgent = remainDays <= 14;
    const isDanger = remainDays <= 21;

    // 안전 품목은 발주 안 함
    if (!isDanger) continue;

    // 거래처별 주간 6건 제한 (긴급은 한도 무시)
    const vendor = item.vendor_name || '';
    if (vendor && !isUrgent) {
      if (!(vendor in weeklyVendorCountSch)) {
        weeklyVendorCountSch[vendor] = db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date>=? AND status!='cancelled' AND status!='취소'`).get(vendor, mondayStrSch).cnt;
      }
      if (weeklyVendorCountSch[vendor] >= 6) {
        console.log(`[자동발주] 스킵: ${item.product_code} — ${vendor} 주간 한도 초과 (${weeklyVendorCountSch[vendor]}/6건)`);
        continue;
      }
    }

    // 미완료 PO 스킵 (중복발주 방지)
    const pendingPO = db.prepare(`
      SELECT h.po_number, h.status FROM po_header h
      JOIN po_items i ON i.po_id = h.po_id
      WHERE i.product_code = ? AND h.status IN ('draft','발송','확인','수령중','OS등록대기','sent')
      LIMIT 1
    `).get(item.product_code);
    if (pendingPO) {
      console.log(`[자동발주] 스킵: ${item.product_code} — 미완료 PO (${pendingPO.po_number})`);
      continue;
    }

    // 발주수량 = 월출고량 - 가용재고 (천단위 올림)
    const shortage = Math.max(monthly - avail, 0);
    const orderQty = shortage > 0 ? Math.ceil(shortage / 1000) * 1000 : 0;
    if (orderQty <= 0) continue;

    // PO 생성 (status='sent'로 바로 발송 상태)
    const poNumber = generatePoNumber();
    // origin 결정
    const _schedOriginProd = db.prepare('SELECT origin FROM products WHERE product_code=?').get(item.product_code);
    const _schedOrigin = (_schedOriginProd && _schedOriginProd.origin) || '한국';
    const tx = db.transaction(() => {
      const hdr = db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, material_status, process_status, origin, po_date)
        VALUES (?,?,?,?,?,?,?,?,?,date('now','localtime'))`).run(
        poNumber, '자동발주', item.vendor_name || '', 'sent', orderQty, '자동발주 스케줄러', 'sent', 'waiting', _schedOrigin
      );
      db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)').run(
        hdr.lastInsertRowid, item.product_code, p['브랜드'] || '', '', orderQty, '', '자동발주'
      );
      db.prepare('UPDATE auto_order_items SET last_ordered_at=? WHERE id=?').run(new Date().toISOString(), item.id);
      return { po_id: Number(hdr.lastInsertRowid), po_number: poNumber };
    });
    const result = tx();

    // 활동 로그
    logPOActivity(result.po_id, 'auto_order', {
      actor_type: 'scheduler',
      to_status: 'sent',
      details: `자동발주: ${item.product_code} ${orderQty}매 → ${item.vendor_name || '미지정'} (가용재고: ${avail}, 월출고: ${monthly})`
    });

    // 거래명세서 자동 생성
    const po = db.prepare('SELECT * FROM po_header WHERE po_id=?').get(result.po_id);
    const poItems = db.prepare('SELECT * FROM po_items WHERE po_id=?').all(result.po_id);
    const docItems = poItems.map(it => ({
      product_code: it.product_code, product_name: it.brand || '',
      qty: it.ordered_qty, unit_price: 0, amount: 0, spec: it.spec || ''
    }));
    const vendorRow = db.prepare('SELECT type FROM vendors WHERE name=?').get(po.vendor_name);
    const vendorType = vendorRow ? vendorRow.type : 'material';
    try {
      db.prepare("INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')")
        .run(result.po_id, poNumber, po.vendor_name, vendorType, JSON.stringify(docItems));
    } catch(e) { console.log('[자동발주] 거래명세서 생성 오류:', e.message); }

    // 이메일 발송
    const vendorInfo = db.prepare('SELECT * FROM vendors WHERE name=?').get(item.vendor_name);
    if (vendorInfo && vendorInfo.email) {
      try {
        const isPost = vendorInfo.type === '후공정';
        const emailResult = await sendPOEmail(po, poItems, vendorInfo.email, vendorInfo.name, isPost, vendorInfo.email_cc);
        console.log(`[자동발주] 이메일 발송: ${poNumber} → ${vendorInfo.name} (${vendorInfo.email})`, emailResult);
      } catch(e) { console.log(`[자동발주] 이메일 발송 실패: ${e.message}`); }
    }

    // Google Sheet 동기화
    try {
      await appendToGoogleSheet(poItems.map(it => ({
        order_date: po.po_date || '', product_code: it.product_code || '',
        product_name: it.brand || '', material_name: it.spec || '',
        paper_maker: po.vendor_name || '', order_qty: it.ordered_qty || 0,
        product_spec: it.spec || ''
      })));
    } catch(e) { console.log(`[자동발주] Google Sheet 동기화 실패: ${e.message}`); }

    createdCount++;
    if (vendor) weeklyVendorCountSch[vendor]++;
    console.log(`[자동발주] PO 생성+발송: ${poNumber} (${item.product_code} → ${item.vendor_name})`);
  }

  console.log(`[자동발주 스케줄러] 완료: ${createdCount}건 발주, 총 ${items.length}건 체크`);
}

// ── 출고일 도래 시 후공정 자동 이메일 ──────────────────────────────────
async function runShipmentEmailCheck() {
  const today = new Date().toISOString().slice(0, 10);
  console.log(`[출고일 체크] ${today} 실행`);

  // 오늘 출고 예정 + 아직 이메일 안 보낸 건
  const schedules = db.prepare(
    "SELECT * FROM vendor_shipment_schedule WHERE ship_date=? AND auto_email_sent=0 AND status='scheduled'"
  ).all(today);

  if (!schedules.length) {
    console.log('[출고일 체크] 오늘 출고 예정 없음');
    return;
  }

  for (const sch of schedules) {
    if (!sch.post_vendor_email) {
      console.log(`[출고일 체크] ${sch.po_number}: 후공정 이메일 없음, 스킵`);
      continue;
    }

    // PO 정보 조회
    const po = db.prepare('SELECT * FROM po_header WHERE po_id=?').get(sch.po_id);
    const items = db.prepare('SELECT * FROM po_items WHERE po_id=?').all(sch.po_id);
    if (!po) continue;

    // 이메일 발송 (후공정 업체에게)
    try {
      const postVendorForCc = db.prepare('SELECT email_cc FROM vendors WHERE name=?').get(sch.post_vendor_name);
      const emailResult = await sendPOEmail(po, items, sch.post_vendor_email, sch.post_vendor_name, true, postVendorForCc ? postVendorForCc.email_cc : '');
      console.log(`[출고일 체크] 후공정 이메일 발송: ${sch.po_number} → ${sch.post_vendor_name} (${sch.post_vendor_email})`, emailResult);

      // auto_email_sent = 1 업데이트
      db.prepare("UPDATE vendor_shipment_schedule SET auto_email_sent=1, updated_at=datetime('now','localtime') WHERE id=?").run(sch.id);

      // 후공정 상태 업데이트: process_status → 'sent'
      db.prepare("UPDATE po_header SET process_status='sent' WHERE po_id=? AND process_status='waiting'").run(sch.po_id);

      // 활동 로그
      logPOActivity(sch.po_id, 'post_vendor_notified', {
        actor_type: 'scheduler',
        details: `출고일 도래 → 후공정 자동 발송: ${sch.post_vendor_name} (${sch.post_vendor_email})`
      });
    } catch(e) {
      console.log(`[출고일 체크] 이메일 실패: ${sch.po_number} → ${e.message}`);
    }
  }

  console.log(`[출고일 체크] 완료: ${schedules.length}건 처리`);
}

// ── 납기 D-3 알림 체크 ──────────────────────────────────
async function runDeadlineAlertCheck() {
  const today = new Date().toISOString().slice(0, 10);
  console.log(`[납기알림] ${today} 실행`);

  // expected_date가 오늘~3일 후인 미완료 PO 조회
  const upcomingPOs = db.prepare(`
    SELECT h.po_id, h.po_number, h.vendor_name, h.expected_date, h.total_qty, h.po_date
    FROM po_header h
    WHERE h.expected_date >= date('now') AND h.expected_date <= date('now','+3 days')
      AND h.status NOT IN ('received','cancelled','os_pending')
  `).all();

  if (!upcomingPOs.length) {
    console.log('[납기알림] 임박 건 없음');
    return;
  }

  console.log(`[납기알림] 임박 ${upcomingPOs.length}건 발견`);

  for (const po of upcomingPOs) {
    const vendor = db.prepare('SELECT email, name FROM vendors WHERE name = ?').get(po.vendor_name);
    if (!vendor || !vendor.email) {
      console.log(`[납기알림] ${po.po_number}: 거래처 이메일 없음 (${po.vendor_name})`);
      continue;
    }

    const dDay = Math.round((new Date(po.expected_date) - new Date(today)) / 86400000);
    const subject = `[바른손] 납기일 D-${dDay} 리마인더 — ${po.po_number}`;
    const text = `안녕하세요, ${vendor.name} 담당자님.\n\n아래 발주건의 납기일이 ${dDay}일 남았습니다.\n\n- 발주번호: ${po.po_number}\n- 발주일: ${po.po_date}\n- 납기예정일: ${po.expected_date}\n- 수량: ${po.total_qty}\n\n납기일 준수 부탁드립니다.\n\n바른손 자재관리팀`;

    try {
      if (smtpTransporter) {
        const targetEmail = TEST_EMAIL || vendor.email;
        await smtpTransporter.sendMail({
          from: SMTP_FROM,
          to: targetEmail,
          subject,
          text
        });
        console.log(`[납기알림] 이메일 발송: ${po.po_number} → ${targetEmail} (D-${dDay})`);
      } else {
        console.log(`[납기알림] SMTP 미설정 — ${po.po_number} D-${dDay} (${vendor.email})`);
      }
    } catch (e) {
      console.log(`[납기알림] 이메일 실패: ${po.po_number} → ${e.message}`);
    }
  }
}

// 자동발주 스케줄러: 매일 9시 실행
function scheduleAutoOrder() {
  const now = new Date();
  const next9am = new Date(now);
  next9am.setHours(9, 0, 0, 0);
  if (now >= next9am) next9am.setDate(next9am.getDate() + 1);

  const msUntilNext = next9am - now;
  console.log(`[자동발주 스케줄러] 다음 실행: ${next9am.toLocaleString('ko-KR')} (${Math.round(msUntilNext / 60000)}분 후)`);

  setTimeout(() => {
    runAutoOrderScheduler();
    runShipmentEmailCheck();
    runDeadlineAlertCheck();
    // 이후 24시간마다 반복
    setInterval(() => {
      runAutoOrderScheduler();
      runShipmentEmailCheck();
      runDeadlineAlertCheck();
    }, 24 * 60 * 60 * 1000);
  }, msUntilNext);
}

// XERP 데이터 자동 동기화: 매일 9:30 실행
function scheduleXerpSync() {
  const now = new Date();
  const next930 = new Date(now);
  next930.setHours(9, 30, 0, 0);
  if (now >= next930) next930.setDate(next930.getDate() + 1);

  const msUntil = next930 - now;
  console.log(`[XERP 동기화] 다음 실행: ${next930.toLocaleString('ko-KR')} (${Math.round(msUntil / 60000)}분 후)`);

  setTimeout(async () => {
    await refreshXerpCache();
    setInterval(async () => {
      await refreshXerpCache();
    }, 24 * 60 * 60 * 1000);
  }, msUntil);
}

async function refreshXerpCache() {
  console.log(`[XERP 동기화] ${new Date().toLocaleString('ko-KR')} 자동 동기화 시작`);
  try {
    xerpInventoryCacheTime = 0; // 캐시 무효화
    const http = require('http');
    const req = http.get(`http://localhost:${PORT}/api/xerp-inventory?refresh=1`, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => console.log('[XERP 동기화] 완료'));
    });
    req.on('error', e => console.warn('[XERP 동기화] 실패:', e.message));
  } catch(e) { console.warn('[XERP 동기화] 오류:', e.message); }
}
