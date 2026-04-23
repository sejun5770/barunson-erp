const _startTime = Date.now();
// ERP 애플리케이션 버전 (MANUAL.md / CHANGELOG.md 와 동기화)
const APP_VERSION = '1.1.1';
const APP_VERSION_DATE = '2026-04-14';
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { URL } = require('url');
const zlib = require('zlib');
const pgAdapter = require('./pg-adapter');
const nodemailer = require('nodemailer');
const sql = require('mssql');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

// ── 파일 로거 (일별 로테이션, 30일 보관) ────────────────────────────
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function _logDate() { return new Date().toISOString().slice(0,10); }
function _logTime() { return new Date().toISOString().replace('T',' ').slice(0,19); }
let _logFileDate = _logDate();
let _accessStream = fs.createWriteStream(path.join(LOG_DIR, `access-${_logFileDate}.log`), { flags: 'a' });
let _errorStream  = fs.createWriteStream(path.join(LOG_DIR, `error-${_logFileDate}.log`),  { flags: 'a' });
let _appStream    = fs.createWriteStream(path.join(LOG_DIR, `app-${_logFileDate}.log`),    { flags: 'a' });

function _rotateIfNeeded() {
  const today = _logDate();
  if (today === _logFileDate) return;
  _logFileDate = today;
  _accessStream.end(); _errorStream.end(); _appStream.end();
  _accessStream = fs.createWriteStream(path.join(LOG_DIR, `access-${today}.log`), { flags: 'a' });
  _errorStream  = fs.createWriteStream(path.join(LOG_DIR, `error-${today}.log`),  { flags: 'a' });
  _appStream    = fs.createWriteStream(path.join(LOG_DIR, `app-${today}.log`),    { flags: 'a' });
  // 30일 이전 로그 삭제
  _cleanOldLogs();
}

function _cleanOldLogs() {
  const cutoff = Date.now() - 30 * 86400000;
  try {
    for (const f of fs.readdirSync(LOG_DIR)) {
      const m = f.match(/^(?:access|error|app)-(\d{4}-\d{2}-\d{2})\.log$/);
      if (m && new Date(m[1]).getTime() < cutoff) {
        fs.unlinkSync(path.join(LOG_DIR, f));
      }
    }
  } catch (_) {}
}

const logger = {
  access(method, url, status, ms, ip) {
    _rotateIfNeeded();
    const line = `${_logTime()} ${ip} ${method} ${url} ${status} ${ms}ms\n`;
    _accessStream.write(line);
  },
  info(msg, ...args) {
    _rotateIfNeeded();
    const line = `${_logTime()} [INFO] ${msg} ${args.length ? JSON.stringify(args) : ''}\n`;
    _appStream.write(line);
    console.log(msg, ...args);
  },
  warn(msg, ...args) {
    _rotateIfNeeded();
    const line = `${_logTime()} [WARN] ${msg} ${args.length ? JSON.stringify(args) : ''}\n`;
    _appStream.write(line);
    console.warn(msg, ...args);
  },
  error(msg, ...args) {
    _rotateIfNeeded();
    const line = `${_logTime()} [ERROR] ${msg} ${args.length ? JSON.stringify(args) : ''}\n`;
    _errorStream.write(line);
    _appStream.write(line);
    console.error(msg, ...args);
    // Slack 알림 (fire-and-forget, 실패해도 서버 영향 없음)
    try { slackError(msg, args); } catch(_) {}
  }
};
_cleanOldLogs(); // 서버 시작 시 1회 정리

// ── Slack Incoming Webhook 알림 ─────────────────────────────────
// SLACK_WEBHOOK_URL 환경변수가 설정되어 있을 때만 동작. 미설정 시 전부 no-op.
// 에러 알림은 중복 억제(dedupe) 및 rate limit 적용.
const _slackDedupe = new Map(); // key → lastSentTime
const SLACK_DEDUPE_MS = 5 * 60 * 1000; // 같은 에러는 5분에 1번만
let _slackWebhookUrl = ''; // 서버 시작 후 envVars 로드되면 채워짐

function _slackCleanupDedupe() {
  const cutoff = Date.now() - SLACK_DEDUPE_MS * 2;
  for (const [k, t] of _slackDedupe.entries()) {
    if (t < cutoff) _slackDedupe.delete(k);
  }
}

async function sendSlack(text, opts = {}) {
  if (!_slackWebhookUrl) return false;
  if (!text || typeof text !== 'string') return false;
  const payload = { text: text.slice(0, 3000) };
  if (opts.blocks) payload.blocks = opts.blocks;
  try {
    const https = require('https');
    const url = new URL(_slackWebhookUrl);
    const body = JSON.stringify(payload);
    return await new Promise((resolve) => {
      const req = https.request({
        method: 'POST',
        hostname: url.hostname,
        path: url.pathname + url.search,
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
        timeout: 5000
      }, (res) => {
        res.on('data', () => {});
        res.on('end', () => resolve(res.statusCode >= 200 && res.statusCode < 300));
      });
      req.on('error', () => resolve(false));
      req.on('timeout', () => { req.destroy(); resolve(false); });
      req.write(body);
      req.end();
    });
  } catch (_) { return false; }
}

function slackError(msg, args) {
  if (!_slackWebhookUrl) return;
  // 에러 메시지 필터: 너무 흔한 것들(요청 취소, ECONNRESET) 제외
  const full = String(msg) + ' ' + (args && args.length ? JSON.stringify(args).slice(0, 300) : '');
  if (/ECONNRESET|EPIPE|aborted|socket hang up/i.test(full)) return;
  // dedupe key: 앞 150자
  const key = full.slice(0, 150);
  const now = Date.now();
  if (_slackDedupe.has(key) && (now - _slackDedupe.get(key)) < SLACK_DEDUPE_MS) return;
  _slackDedupe.set(key, now);
  _slackCleanupDedupe();
  // fire-and-forget
  sendSlack(`🚨 *ERP 에러*\n\`\`\`${full.slice(0, 1500)}\`\`\``).catch(() => {});
}

// ── XERP MSSQL 연결 ─────────────────────────────────────────────────
let xerpPool = null;
let xerpUsageCache = null;
let xerpUsageCacheTime = 0;
let xerpInventoryCache = null;
let xerpInventoryCacheTime = 0;
let xerpInventoryCaches = {};     // { barunson: {data, time}, dd: {data, time} }
// In-memory 동기화 상태 (sync_log 테이블 없을 때 대체)
let inMemorySyncState = { running: false, done_at: null, count: 0, error: null };
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
    if (m) {
      let val = m[2].trim();
      // 따옴표 제거 (DB_PASSWORD="xxx#" 형태 지원)
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      envVars[m[1].trim()] = val;
    }
  });
  console.log('환경변수 로드:', dotenvPath, '(DB_SERVER:', envVars.DB_SERVER ? 'OK' : 'missing', ')');
} catch (e) { console.warn('.env 로드 실패:', e.message); }

// Slack Webhook URL 활성화 (envVars 로드 완료 후)
_slackWebhookUrl = envVars.SLACK_WEBHOOK_URL || process.env.SLACK_WEBHOOK_URL || '';
if (_slackWebhookUrl) {
  console.log('[Slack] Webhook 활성화됨');
} else {
  console.log('[Slack] SLACK_WEBHOOK_URL 미설정 → 알림 비활성');
}

// 서버 크래시 감지 (uncaughtException / unhandledRejection)
process.on('uncaughtException', (err) => {
  console.error('[CRASH] uncaughtException:', err.message, err.stack);
  try { sendSlack(`💥 *ERP 서버 크래시 (uncaughtException)*\n\`\`\`${err.message}\n${(err.stack||'').slice(0, 1500)}\`\`\``); } catch(_){}
  // 바로 종료하지 않고 1초 대기 (Slack 전송 시간 확보)
  setTimeout(() => process.exit(1), 1000);
});
process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  const stack = reason instanceof Error ? reason.stack : '';
  console.error('[CRASH] unhandledRejection:', msg, stack);
  try { sendSlack(`⚠️ *ERP unhandledRejection*\n\`\`\`${msg}\n${(stack||'').slice(0, 1500)}\`\`\``); } catch(_){}
});

const xerpConfig = {
  // XERP_DB_* 우선 (전용 readonly_erp 계정), 없으면 DB_* 로 fallback (레거시 운영환경 호환)
  server: envVars.XERP_DB_SERVER || process.env.XERP_DB_SERVER || envVars.DB_SERVER || process.env.DB_SERVER || '',
  port: parseInt(envVars.XERP_DB_PORT || process.env.XERP_DB_PORT || envVars.DB_PORT || process.env.DB_PORT || '1433'),
  user: envVars.XERP_DB_USER || process.env.XERP_DB_USER || envVars.DB_USER || process.env.DB_USER || '',
  password: envVars.XERP_DB_PASSWORD || process.env.XERP_DB_PASSWORD || envVars.DB_PASSWORD || process.env.DB_PASSWORD || '',
  database: 'XERP',
  options: { encrypt: true, trustServerCertificate: false, requestTimeout: 120000 },
  pool: { max: 5, min: 1, idleTimeoutMillis: 300000 }
};

// 재고 조회 시 특정 창고만 필터링 — 미설정이면 전체 창고 합산 (legacy 동작).
// 사용자가 "파주물류센터(제품)" 한 창고만 보고 싶다고 해서 도입. WhCode 정확값은
//   GET /api/debug/xerp-warehouses 로 조회 후 .env 에 XERP_INV_WAREHOUSE=<code> 설정.
//   여러 창고 합산이 필요하면 콤마로 구분 (예: 'MF24,MF14').
const XERP_INV_WAREHOUSE = (envVars.XERP_INV_WAREHOUSE || process.env.XERP_INV_WAREHOUSE || '').trim();
const XERP_INV_WH_LIST = XERP_INV_WAREHOUSE ? XERP_INV_WAREHOUSE.split(',').map(s => s.trim()).filter(Boolean) : [];
if (XERP_INV_WH_LIST.length > 0) console.log(`[xerp-inv] 창고 필터 활성: WhCode IN (${XERP_INV_WH_LIST.join(', ')})`);
else console.log('[xerp-inv] 창고 필터 미설정 — 전체 창고 합산');

// bar_shop1 전용 config — DB_* (readonly_user) 사용. server 는 XERP 와 동일 인스턴스지만
// SQL 로그인이 달라서 (readonly_user vs readonly_erp) 별도 관리.
// 기존엔 `{...xerpConfig, database: 'bar_shop1'}` 로 덮어써서 readonly_erp 가 bar_shop1 접근 권한 없으면 실패.
const barShopConfig = {
  server: envVars.DB_SERVER || process.env.DB_SERVER || xerpConfig.server,
  port: parseInt(envVars.DB_PORT || process.env.DB_PORT) || xerpConfig.port,
  user: envVars.DB_USER || process.env.DB_USER || xerpConfig.user,
  password: envVars.DB_PASSWORD || process.env.DB_PASSWORD || xerpConfig.password,
  database: 'bar_shop1',
  options: { encrypt: true, trustServerCertificate: false, requestTimeout: 120000 },
  pool: { max: 3, min: 1, idleTimeoutMillis: 300000 }
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

    // XERP 품목명 캐시 로딩 (비동기, 백그라운드)
    loadXerpItemNames().catch(e => console.warn('XERP 품목명 캐시 실패:', e.message));

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

// XERP 품목코드→품목명 캐시 (수불원장용, 서버 시작 시 1회 로딩)
let xerpItemNameCache = {};
async function loadXerpItemNames() {
  if (!xerpPool) return;
  const cache = {};
  try {
    // 1) poOrderItem에서 품목명 (717개+, 가장 신뢰성 높음)
    const poResult = await xerpPool.request().query(`
      SELECT RTRIM(ItemCode) AS ic, MAX(RTRIM(ItemName)) AS nm
      FROM poOrderItem WITH (NOLOCK)
      WHERE SiteCode = 'BK10'
        AND ItemName IS NOT NULL AND LEN(RTRIM(ItemName)) > 0
      GROUP BY RTRIM(ItemCode)
    `);
    for (const r of poResult.recordset) {
      const code = (r.ic||'').trim();
      const name = (r.nm||'').trim();
      if (code && name) cache[code] = name;
    }
    console.log(`XERP 품목명 캐시(poOrderItem): ${Object.keys(cache).length}개`);
  } catch(e) { console.warn('poOrderItem 품목명 로딩 실패:', e.message); }
  // mmInoutItem은 40M건으로 GROUP BY 쿼리가 타임아웃됨 — poOrderItem만 사용
  // 향후 품목 마스터 테이블 별도 구축 필요
  xerpItemNameCache = cache;
  console.log(`XERP 품목명 캐시 총: ${Object.keys(cache).length}개 품목`);
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

// 초기 연결 (서버 기동 후 백그라운드에서 실행 — Docker 헬스체크 타임아웃 방지)
function initXERP() {
  setTimeout(async () => {
    const ok = await connectXERP();
    if (!ok) scheduleXerpReconnect();
  }, 1000);
}

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

// DD 초기 연결 (서버 기동 후 백그라운드에서 실행)
function initDD() {
  setTimeout(async () => {
    if (ddConfig.host) await ensureDdPool();
    else console.log('ℹ DD_DB_SERVER 미설정 → DD 동기화 비활성');
  }, 2000);
}

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

function generateVendorToken(email, vendorName) {
  try {
    return jwt.sign({ email, name: vendorName || '', type: 'vendor' }, VENDOR_JWT_SECRET, { expiresIn: '90d' });
  } catch (e) {
    console.warn('JWT 생성 실패, 레거시 토큰 사용:', e.message);
    return generateVendorTokenLegacy(email);
  }
}

function decodeVendorToken(token) {
  try {
    const decoded = jwt.verify(token, VENDOR_JWT_SECRET);
    if (decoded && decoded.type === 'vendor') return decoded;
  } catch (_) {}
  return null;
}

function verifyVendorToken(email, token) {
  // 1. JWT 검증 시도 (access 토큰: email 파라미터 없이 토큰만으로 검증)
  const decoded = decodeVendorToken(token);
  if (decoded) {
    if (!email) return true;
    if (decoded.email === email) return true;
  }
  // 2. 레거시 해시 토큰 폴백 (하위 호환)
  if (email) return generateVendorTokenLegacy(email) === token;
  return false;
}

// vendor-portal 공통 인증 헬퍼
// 2026-04: 외부 접근 차단 후 — 진입점(미들웨어)에서 관리자 JWT 필수로 검증하므로
//          여기서는 단순히 params 에서 email/vendor_name 만 뽑아 전달.
function extractVendorAuth(params) {
  const email = params.email || '';
  const vendorName = params.vendor_name || '';
  if (!email) return null;
  return { email, vendorName, token: '' };
}

// product_info 데이터 (원자재코드, 원재료명, 절, 후공정업체 조회용)
// 1차 소스: products + product_post_vendor DB 테이블
// 2차 폴백: product_info.json 파일 (레거시)
let productInfoCache = null;
let _productInfoReloadScheduled = false;

const _POST_COL_TO_KR = {
  thomson: '톰슨',
  envelope: '봉투가공',
  seari: '세아리',
  laser: '레이져',
  cutting: '재단',
  silk: '실크',
};

async function reloadProductInfoFromDB() {
  try {
    const products = await db.prepare("SELECT product_code, material_code, material_name, cut_spec, jopan, paper_maker, product_spec, thomson, envelope, seari, laser, cutting, silk FROM products").all();
    let ppv = [];
    try { ppv = await db.prepare('SELECT product_code, process_type, vendor_name, step_order FROM product_post_vendor ORDER BY product_code, step_order').all(); } catch(_) {}
    const ppvByCode = {};
    const ppvStepsByCode = {};
    for (const r of ppv) {
      if (!r.product_code) continue;
      if (!ppvByCode[r.product_code]) ppvByCode[r.product_code] = {};
      if (!ppvStepsByCode[r.product_code]) ppvStepsByCode[r.product_code] = [];
      if (r.process_type && r.vendor_name) {
        ppvByCode[r.product_code][r.process_type] = r.vendor_name;
        ppvStepsByCode[r.product_code].push({ step: r.step_order || 1, process: r.process_type, vendor: r.vendor_name });
      }
    }
    // 후공정 타입 목록 (이 키들은 product_post_vendor에서만 관리)
    let postProcessTypes = ['재단','인쇄','박/형압','톰슨','봉투가공','단면접착','우찌누끼','접지','코팅'];
    try { const pt = await getPostProcessTypes(); if (pt.length) postProcessTypes = pt; } catch(_) {}
    const postTypeSet = new Set(postProcessTypes);

    // 레거시 JSON은 기본정보(원자재코드 등)의 폴백으로만 사용 — 후공정 필드는 무시
    let legacy = {};
    try { legacy = JSON.parse(fs.readFileSync(path.join(__dir, 'product_info.json'), 'utf8')); } catch(_) {}
    const out = {};
    // 레거시에서 기본정보만 복사 (후공정 키는 제외)
    for (const code in legacy) {
      const row = {};
      for (const [k, v] of Object.entries(legacy[code] || {})) {
        if (!postTypeSet.has(k)) row[k] = v;
      }
      out[code] = row;
    }
    // DB products 테이블로 override (단일 소스)
    for (const p of products) {
      const code = p.product_code;
      if (!code) continue;
      const row = out[code] || {};
      if (p.material_code) row['원자재코드'] = p.material_code;
      if (p.material_name) row['원재료용지명'] = p.material_name;
      if (p.paper_maker) row['제지사'] = p.paper_maker;
      if (p.cut_spec !== null && p.cut_spec !== undefined && p.cut_spec !== '') row['절'] = String(p.cut_spec);
      if (p.jopan !== null && p.jopan !== undefined && p.jopan !== '') row['조판'] = String(p.jopan);
      if (p.product_spec) row['제품사양'] = p.product_spec;
      // 레거시 후공정 키가 남아있으면 제거
      for (const pt of postProcessTypes) { delete row[pt]; }
      out[code] = row;
    }
    // product_post_vendor가 유일한 후공정 소스 (사용자가 품목관리에서 관리)
    for (const [code, ptMap] of Object.entries(ppvByCode)) {
      if (!out[code]) out[code] = {};
      for (const [pt, vn] of Object.entries(ptMap)) out[code][pt] = vn;
      // step_order 기반 정렬된 후공정 체인 (_steps)
      if (ppvStepsByCode[code]?.length) {
        out[code]._steps = ppvStepsByCode[code].sort((a, b) => a.step - b.step);
      }
    }
    productInfoCache = out;
    console.log(`[product_info] DB 재로드 완료: ${Object.keys(out).length}개 품목`);
  } catch (e) {
    console.warn('[product_info] DB 재로드 실패:', e.message);
    // DB 실패 시 레거시 JSON으로라도 초기화
    if (!productInfoCache) {
      try { productInfoCache = JSON.parse(fs.readFileSync(path.join(__dir, 'product_info.json'), 'utf8')); }
      catch (_) { productInfoCache = {}; }
    }
  }
}

let _lastProductInfoReloadAt = 0;
function scheduleProductInfoReload() {
  if (_productInfoReloadScheduled) return;
  // bulk-import 처럼 연속 write 가 들어오면 300ms 디바운스만으로는 과호출.
  // 1.5s 로 늘려 burst 를 한 번에 흡수. 사용자 체감 딜레이는 여전히 < 2s.
  _productInfoReloadScheduled = true;
  setTimeout(async () => {
    _productInfoReloadScheduled = false;
    // 최근 5초 안에 이미 재로드했으면 스킵 — bulk 흐름에서 중복 스캔 방지
    if (Date.now() - _lastProductInfoReloadAt < 5000) return;
    await reloadProductInfoFromDB();
    _lastProductInfoReloadAt = Date.now();
  }, 1500);
}

function getProductInfo() {
  if (productInfoCache) return productInfoCache;
  // 첫 호출 — 레거시 파일에서 기본정보만 로드 (후공정 키는 즉시 제거)
  const _defaultPostTypes = ['재단','인쇄','박/형압','톰슨','봉투가공','단면접착','우찌누끼','접지','코팅'];
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(__dir, 'product_info.json'), 'utf8'));
    const cleaned = {};
    for (const code in raw) {
      const row = {};
      for (const [k, v] of Object.entries(raw[code] || {})) {
        if (!_defaultPostTypes.includes(k)) row[k] = v;
      }
      cleaned[code] = row;
    }
    productInfoCache = cleaned;
  } catch (e) { productInfoCache = {}; }
  scheduleProductInfoReload();
  return productInfoCache;
}

// 업체+제품코드 직전 단가 조회 (최신 승인/확인 거래명세서에서)
async function getLastVendorPrice(vendorName, productCode) {
  try {
    const docs = await db.prepare(`SELECT items_json, vendor_modified_json FROM trade_document
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
    const pp = await db.prepare(`SELECT unit_price FROM post_process_price WHERE vendor_name=? AND unit_price>0 ORDER BY id DESC LIMIT 1`).get(vendorName);
    if (pp) return pp.unit_price;
  } catch(e) {}
  return 0;
}

// PO 활동 로그 기록
async function logPOActivity(poId, action, opts = {}) {
  const po = await db.prepare('SELECT po_number, status, material_status, process_status FROM po_header WHERE po_id=?').get(poId);
  if (!po) return;
  await db.prepare(`INSERT INTO po_activity_log (po_id, po_number, action, actor, actor_type, from_status, to_status, from_material_status, to_material_status, from_process_status, to_process_status, details) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
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
// 2026-04: 거래처 포털 링크 제거. PDF 첨부 + 이메일 회신/전화 안내로 변경.
// 2026-04: DD 법인 발주는 '바른디자인' 명의로 발송 (바른컴퍼니 아님)
async function sendPOEmail(po, items, vendorEmail, vendorName, isPostProcess, emailCc) {

  const pInfo = getProductInfo();

  // 법인별 발송 명의 분기
  const isDD = (po.legal_entity === 'dd');
  const SENDER_COMPANY = isDD ? '바른디자인' : '바른컴퍼니';

  const typeLabel = isPostProcess ? '후공정' : '원재료';
  const subject = `[${SENDER_COMPANY}] ${typeLabel} 발주서 - ${po.po_number} (${vendorName})`;

  // 품목별 product_info 매핑 + 연(R) 계산: 발주수량 / 500 / 절 / 조판
  // parseJeolServer: 'T3K'→3, '3TK'→3 등 문자열에서 절 숫자 추출
  const parseJeolServer = (val) => {
    if (!val) return 0;
    const n = parseFloat(val);
    if (!isNaN(n) && n > 0) return n;
    const m = String(val).match(/(\d+)/);
    return m ? parseFloat(m[1]) : 0;
  };
  const enrichedItems = items.map(it => {
    const pi = pInfo[it.product_code] || {};
    const cut = parseJeolServer(pi['절']) || 1;
    const jopan = parseJeolServer(pi['조판']) || 1;
    const qty = it.ordered_qty || 0;
    const reams = qty / 500 / cut / jopan;
    const reamsStr = reams % 1 === 0 ? String(reams) : reams.toFixed(1);
    return {
      ...it,
      material_code: pi['원자재코드'] || '',
      // 원재료명: 규격 컬럼이 별도로 존재하므로 it.spec fallback 제거 (중복 표시 방지)
      material_name: pi['원재료용지명'] || '',
      cut_spec: pi['절'] || '',
      ream_qty: reamsStr,
    };
  });

  // 원재료/후공정 구분
  const isRawMaterial = !isPostProcess;

  // 원재료: 다음 입고처(후공정 업체) 조회
  let nextDestinations = [];
  if (isRawMaterial && po.po_date) {
    const postPOs = await db.prepare(`SELECT DISTINCT vendor_name FROM po_header WHERE po_date = ? AND po_type = '후공정' AND status != 'cancelled'`).all(po.po_date);
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
      <th style="${thStyle}">규격</th>
      <th style="${thStyle};text-align:right">발주수량(R)</th>
      <th style="${thStyle};text-align:center">절</th>
    </tr>`;
    tableRows = enrichedItems.map(it => `<tr>
      <td style="${tdStyle};font-weight:600">${it.product_code || ''}</td>
      <td style="${tdStyle}">${it.material_code || ''}</td>
      <td style="${tdStyle}">${it.material_name || ''}</td>
      <td style="${tdStyle}">${it.spec || ''}</td>
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
        <h2 style="margin:0">${SENDER_COMPANY} ${typeLabel} 발주서</h2>
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
        <div style="margin-top:24px;padding:16px 20px;background:#fff7ed;border:1px solid #fdba74;border-radius:8px;color:#7c2d12">
          <div style="font-weight:700;font-size:14px;margin-bottom:8px">📋 발주 확인 및 진행 안내</div>
          <div style="font-size:13px;line-height:1.7">
            ① 첨부된 <b>PDF 발주서</b>를 확인해주세요.<br>
            ② <b>발주 확인 / 출고 예정일</b>을 이 메일에 회신해주시거나 담당자에게 전화 부탁드립니다.<br>
            ③ <b>출고 완료 시점</b>에도 회신 부탁드립니다. 접수 후 후공정 업체로 전달됩니다.
          </div>
        </div>
        <p style="margin-top:14px;color:#888;font-size:11px;text-align:center">
          ※ 본 메일은 ${SENDER_COMPANY} ERP에서 자동 발송되었습니다.<br>
          문의: seungchan.back@barunn.net
        </p>
      </div>
    </div>`;

  // 합계 계산
  const totalQty = enrichedItems.reduce((s, it) => s + (it.ordered_qty || 0), 0);
  const totalReams = enrichedItems.reduce((s, it) => s + (parseFloat(it.ream_qty) || 0), 0);

  // 중국 거래처 판별 (origin 기반)
  let isChinaVendor = false;
  for (const it of items) {
    const prod = await db.prepare('SELECT origin FROM products WHERE product_code=?').get(it.product_code);
    if (prod && prod.origin === '중국') { isChinaVendor = true; break; }
  }

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
          <th>규격${isChinaVendor ? '<br><span style="font-weight:400;color:#999">规格</span>' : ''}</th>
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
          <td>${it.spec || ''}</td>
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
          <td colspan="${isRawMaterial ? 5 : 3}" style="text-align:right;border:1px solid #ccc">합계 ${isChinaVendor ? '/ 合计' : ''}</td>
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
        <div class="name">${SENDER_COMPANY}</div>
      </div>
      <div class="sign-box">
        <div class="label">공급처 ${isChinaVendor ? '/ 供应方' : ''}</div>
        <div class="name">${vendorName}</div>
      </div>
    </div>

    <div class="footer">${SENDER_COMPANY} 발주시스템 | Generated ${new Date().toISOString().slice(0, 10)} | ${po.po_number}</div>
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
        from: `"${SENDER_COMPANY} 발주시스템" <${SMTP_FROM}>`,
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

// ── PostgreSQL ──────────────────────────────────────────────────────────
const db = pgAdapter;

async function startServer() {
  // PostgreSQL 연결
  await db.connect({
    host: envVars.PG_HOST || process.env.PG_HOST || 'onely-postgres',
    port: envVars.PG_PORT || process.env.PG_PORT || '5432',
    user: envVars.PG_USER || process.env.PG_USER || 'onely',
    password: envVars.PG_PASSWORD || process.env.PG_PASSWORD || 'onely',
    database: envVars.PG_DATABASE || process.env.PG_DATABASE || 'sc_erp',
  });
  console.log('✅ PostgreSQL 연결 완료');

  // ── [prelude] onely superuser 로 DDL 권한 선제 해결 ────────────────────
  // prod 에선 PG_USER=sc_erp 로 접속하는데, sc_erp 가 public 스키마 CREATE 권한도 없고
  // 기존 테이블의 owner 도 아니어서 후속 CREATE TABLE / ALTER TABLE 전부 silent-fail.
  // 결과: sync_log 테이블이 없어서 /api/sync/xerp-inventory 가 snapshot 비활성으로 떨어지고
  //       "live 리프레시 중" 루프 + "⚠️ 동기화 필요" 배지 영구 표시 (ISSUE-2026-04-22-DB-PERMISSION.md).
  // 해결: 매 부팅마다 onely superuser 로 (a) schema GRANT, (b) 모든 public 테이블 owner 를 sc_erp 로 이전,
  //       (c) sync_log/inventory_snapshot 을 superuser 권한으로 직접 CREATE — 이후 sc_erp CREATE/ALTER 가 정상화됨.
  {
    const _pguser = envVars.PG_USER || process.env.PG_USER || 'onely';
    const _pgadminUser = envVars.PG_ADMIN_USER || process.env.PG_ADMIN_USER || 'onely';
    const _pgadminPass = envVars.PG_ADMIN_PASSWORD || process.env.PG_ADMIN_PASSWORD || 'onely';
    // 이미 superuser 로 접속 중이면 prelude 자체를 skip (중복 연결 불필요)
    if (_pguser === _pgadminUser) {
      console.log('[prelude] 접속 계정이 이미 admin(' + _pgadminUser + ') — 권한 prelude 생략');
    } else {
      const { Pool: _PgPool } = require('pg');
      const _adminHost = envVars.PG_HOST || process.env.PG_HOST || 'onely-postgres';
      const _adminPort = parseInt(envVars.PG_PORT || process.env.PG_PORT || '5432');
      const _adminDb = envVars.PG_DATABASE || process.env.PG_DATABASE || 'sc_erp';
      let _adminPool = null;
      try {
        _adminPool = new _PgPool({ host: _adminHost, port: _adminPort, user: _pgadminUser, password: _pgadminPass, database: _adminDb, max: 2, connectionTimeoutMillis: 5000 });
        // 연결 테스트
        await _adminPool.query('SELECT 1');
      } catch (e) {
        console.warn('[prelude] admin 계정(' + _pgadminUser + ') 연결 실패 — 권한 prelude 생략:', e.message);
        _adminPool = null;
      }
      if (_adminPool) {
        // (a) schema public 에 CREATE/USAGE 권한 부여 → sc_erp 가 CREATE TABLE 할 수 있게
        try { await _adminPool.query(`GRANT ALL PRIVILEGES ON SCHEMA public TO ${_pguser}`); console.log('[prelude] GRANT schema public → ' + _pguser); } catch(e) { console.warn('[prelude] GRANT schema public 실패:', e.message); }
        try { await _adminPool.query(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ${_pguser}`); } catch(_) {}
        try { await _adminPool.query(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO ${_pguser}`); } catch(_) {}

        // (b) 기존 모든 public 테이블의 owner 를 sc_erp 로 이전 → 이후 ALTER TABLE 가능
        try {
          const r = await _adminPool.query(`SELECT tablename FROM pg_tables WHERE schemaname='public'`);
          let _owned = 0;
          for (const row of r.rows) {
            try {
              await _adminPool.query(`ALTER TABLE public."${row.tablename}" OWNER TO ${_pguser}`);
              _owned++;
            } catch(_) {}
          }
          if (_owned) console.log('[prelude] ' + _owned + '개 테이블 owner → ' + _pguser);
        } catch(e) { console.warn('[prelude] owner 이전 실패:', e.message); }

        // (c) sync_log / inventory_snapshot 은 superuser 로 직접 CREATE (존재 보장)
        //     이후 sc_erp 의 CREATE TABLE IF NOT EXISTS 와 ALTER 는 owner 도 sc_erp 라 정상 동작.
        try {
          await _adminPool.query(`CREATE TABLE IF NOT EXISTS sync_log (
            id            SERIAL PRIMARY KEY,
            sync_type     TEXT NOT NULL,
            started_at    TEXT DEFAULT TO_CHAR(NOW() AT TIME ZONE 'Asia/Seoul','YYYY-MM-DD HH24:MI:SS'),
            finished_at   TEXT DEFAULT '',
            success_count INTEGER DEFAULT 0,
            fail_count    INTEGER DEFAULT 0,
            status        TEXT DEFAULT 'running',
            error_msg     TEXT DEFAULT '',
            triggered_by  TEXT DEFAULT 'manual'
          )`);
          await _adminPool.query(`ALTER TABLE sync_log OWNER TO ${_pguser}`);
          console.log('[prelude] sync_log 테이블 OK (owner=' + _pguser + ')');
        } catch(e) { console.warn('[prelude] sync_log CREATE 실패:', e.message); }
        try {
          await _adminPool.query(`CREATE TABLE IF NOT EXISTS inventory_snapshot (
            product_code   TEXT PRIMARY KEY,
            legal_entity   TEXT DEFAULT 'barunson',
            site_code      TEXT DEFAULT 'BK10',
            current_stock  INTEGER DEFAULT 0,
            monthly_out    INTEGER DEFAULT 0,
            daily_out      INTEGER DEFAULT 0,
            total_3m       INTEGER DEFAULT 0,
            item_name      TEXT DEFAULT '',
            synced_at      TEXT DEFAULT ''
          )`);
          await _adminPool.query(`ALTER TABLE inventory_snapshot OWNER TO ${_pguser}`);
          console.log('[prelude] inventory_snapshot 테이블 OK (owner=' + _pguser + ')');
        } catch(e) { console.warn('[prelude] inventory_snapshot CREATE 실패:', e.message); }

        // (d) 기존 객체에 대한 마무리 GRANT
        try { await _adminPool.query(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${_pguser}`); } catch(_) {}
        try { await _adminPool.query(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${_pguser}`); } catch(_) {}

        try { await _adminPool.end(); } catch(_) {}
      }
    }
  }

// ── 핵심 테이블 DDL (ALTER TABLE 보다 선행되어야 함) ───────────────────
// 이전엔 serve_inv2.js 에 이 테이블들의 CREATE 가 없어 init_db.js(SQLite) 에만 의존.
// PG 환경에선 fresh DB 부팅 시 ALTER TABLE 이 "relation does not exist" 로 전부 실패 → 누락 컬럼 런타임 쿼리 실패 악순환.
// 여기서 먼저 생성해 후속 ALTER/CREATE INDEX 가 안전히 진행되도록 보장. pg-adapter 가 SQLite 문법 → PG 로 자동 변환.
await db.exec(`
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
  CREATE TABLE IF NOT EXISTS po_header (
    po_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    po_number   TEXT UNIQUE NOT NULL,
    po_type     TEXT NOT NULL DEFAULT 'material',
    vendor_name TEXT DEFAULT '',
    po_date     TEXT DEFAULT (date('now','localtime')),
    status      TEXT DEFAULT 'draft',
    expected_date TEXT DEFAULT '',
    due_date    TEXT DEFAULT '',
    total_qty   INTEGER DEFAULT 0,
    notes       TEXT DEFAULT '',
    created_at  TEXT DEFAULT (datetime('now','localtime')),
    updated_at  TEXT DEFAULT (datetime('now','localtime'))
  );
  CREATE INDEX IF NOT EXISTS idx_po_status ON po_header(status);
  CREATE INDEX IF NOT EXISTS idx_po_date ON po_header(po_date);
  CREATE TABLE IF NOT EXISTS po_items (
    item_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id        INTEGER NOT NULL,
    product_code TEXT NOT NULL,
    brand        TEXT DEFAULT '',
    process_type TEXT DEFAULT '',
    ordered_qty  INTEGER DEFAULT 0,
    received_qty INTEGER DEFAULT 0,
    spec         TEXT DEFAULT '',
    notes        TEXT DEFAULT ''
  );
  CREATE INDEX IF NOT EXISTS idx_po_items_po ON po_items(po_id);
  CREATE INDEX IF NOT EXISTS idx_po_items_code ON po_items(product_code);
  CREATE TABLE IF NOT EXISTS receipts (
    receipt_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id        INTEGER NOT NULL,
    receipt_date TEXT DEFAULT (date('now','localtime')),
    received_by  TEXT DEFAULT '',
    notes        TEXT DEFAULT '',
    created_at   TEXT DEFAULT (datetime('now','localtime'))
  );
  CREATE INDEX IF NOT EXISTS idx_receipts_po ON receipts(po_id);
  CREATE TABLE IF NOT EXISTS receipt_items (
    receipt_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id      INTEGER NOT NULL,
    po_item_id      INTEGER,
    product_code    TEXT NOT NULL,
    received_qty    INTEGER DEFAULT 0,
    defect_qty      INTEGER DEFAULT 0,
    notes           TEXT DEFAULT ''
  );
  CREATE INDEX IF NOT EXISTS idx_receipt_items_receipt ON receipt_items(receipt_id);
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
  CREATE TABLE IF NOT EXISTS bom_items (
    bom_item_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    bom_id        INTEGER NOT NULL,
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
  CREATE INDEX IF NOT EXISTS idx_bom_items_bom ON bom_items(bom_id);
  CREATE TABLE IF NOT EXISTS product_post_vendor (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    product_code TEXT NOT NULL,
    process_type TEXT NOT NULL DEFAULT '',
    vendor_name  TEXT NOT NULL DEFAULT '',
    step_order   INTEGER DEFAULT 1,
    created_at   TEXT DEFAULT (datetime('now','localtime')),
    updated_at   TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(product_code, process_type, step_order)
  );
  CREATE INDEX IF NOT EXISTS idx_ppv_code ON product_post_vendor(product_code);
`);
console.log('[init] 핵심 테이블 7종 OK (vendors/po_header/po_items/receipts/receipt_items/bom_header/bom_items/product_post_vendor)');

// ── Ensure order_history table ──────────────────────────────────────
await db.exec(`
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
await db.exec(`
  CREATE TABLE IF NOT EXISTS product_notes (
    product_code TEXT PRIMARY KEY,
    note_type    TEXT DEFAULT '',
    note_text    TEXT DEFAULT '',
    updated_at   TEXT DEFAULT (datetime('now','localtime'))
  );
`);
try { await db.exec("ALTER TABLE product_notes ADD COLUMN op_category TEXT DEFAULT ''"); } catch(_) {}

// ── 원장 매핑 테이블 (거래처 품목코드 ↔ XERP 품목코드) ──
await db.exec(`
  CREATE TABLE IF NOT EXISTS ledger_code_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_code TEXT NOT NULL,
    vendor_item_code TEXT NOT NULL,
    vendor_item_name TEXT DEFAULT '',
    xerp_item_code TEXT NOT NULL,
    xerp_item_name TEXT DEFAULT '',
    created_at TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(vendor_code, vendor_item_code)
  );
`);

// ── 필수 자동발주 테이블 ─────────────────────────────────────────────
await db.exec(`
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
await db.exec(`
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

// ── XERP 재고/출고 스냅샷 테이블 (2026-04 재도입) ──
// 매번 XERP 호출하지 않고 하루 1회 또는 수동 '동기화' 시에만 갱신.
// 프로덕션 PG onely 유저에 CREATE 권한 없는 경우를 대비해 별도 try/catch.
try {
  await db.exec(`CREATE TABLE IF NOT EXISTS inventory_snapshot (
    product_code   TEXT PRIMARY KEY,
    legal_entity   TEXT DEFAULT 'barunson',
    site_code      TEXT DEFAULT 'BK10',
    current_stock  INTEGER DEFAULT 0,
    monthly_out    INTEGER DEFAULT 0,
    daily_out      INTEGER DEFAULT 0,
    total_3m       INTEGER DEFAULT 0,
    item_name      TEXT DEFAULT '',
    synced_at      TEXT DEFAULT (datetime('now','localtime'))
  )`);
  await db.exec("CREATE INDEX IF NOT EXISTS idx_invsnap_entity ON inventory_snapshot(legal_entity)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_invsnap_synced ON inventory_snapshot(synced_at)");
  // 구 스키마 호환: 구 버전 코드로 테이블이 만들어져 있으면 legal_entity/site_code/item_name 등 누락 가능.
  const _invsnapCols = [
    ['legal_entity',  "TEXT DEFAULT 'barunson'"],
    ['site_code',     "TEXT DEFAULT 'BK10'"],
    ['monthly_out',   "INTEGER DEFAULT 0"],
    ['daily_out',     "INTEGER DEFAULT 0"],
    ['total_3m',      "INTEGER DEFAULT 0"],
    ['item_name',     "TEXT DEFAULT ''"],
    ['synced_at',     "TEXT DEFAULT ''"]
  ];
  for (const [col, type] of _invsnapCols) {
    try { await db.exec(`ALTER TABLE inventory_snapshot ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(_) {}
  }
  console.log('[init] inventory_snapshot 테이블 OK');
} catch(e) {
  console.error('[init] ★ inventory_snapshot 테이블 생성 실패:', e.message);
  console.error('[init] ★ 관리자가 아래 SQL을 PG 에 실행해야 함:');
  console.error('[init] ★  CREATE TABLE inventory_snapshot (...);');
  console.error('[init] ★  GRANT SELECT,INSERT,UPDATE,DELETE ON inventory_snapshot TO onely;');
}

try {
  await db.exec(`CREATE TABLE IF NOT EXISTS sync_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_type     TEXT NOT NULL,
    started_at    TEXT DEFAULT (datetime('now','localtime')),
    finished_at   TEXT DEFAULT '',
    success_count INTEGER DEFAULT 0,
    fail_count    INTEGER DEFAULT 0,
    status        TEXT DEFAULT 'running',
    error_msg     TEXT DEFAULT '',
    triggered_by  TEXT DEFAULT 'manual'
  )`);
  await db.exec("CREATE INDEX IF NOT EXISTS idx_synclog_type_time ON sync_log(sync_type, started_at)");
  // 상태별 조회 최적화 (stale 감지 / /api/sync/status 모두 status='running' 으로 필터)
  await db.exec("CREATE INDEX IF NOT EXISTS idx_synclog_status ON sync_log(status) WHERE status='running'");
  // 구 스키마 호환: 구 버전 코드로 sync_log 가 이미 만들어져 있고 triggered_by/error_msg/fail_count 등이 빠져 있을 수 있음.
  // CREATE TABLE IF NOT EXISTS 는 기존 테이블을 건드리지 않으므로 ALTER 로 명시적 보강.
  // 누락 시 INSERT "column does not exist" → "does not exist" 정규식 매칭 → snapshot_disabled 로 오폴백.
  const _synclogCols = [
    ['triggered_by', "TEXT DEFAULT 'manual'"],
    ['error_msg',    "TEXT DEFAULT ''"],
    ['fail_count',   "INTEGER DEFAULT 0"],
    ['success_count',"INTEGER DEFAULT 0"],
    ['finished_at',  "TEXT DEFAULT ''"],
    ['status',       "TEXT DEFAULT 'running'"]
  ];
  for (const [col, type] of _synclogCols) {
    try { await db.exec(`ALTER TABLE sync_log ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(_) {}
  }
  // 서버 크래시/재시작 직후 남은 running row 자동 해제.
  // 크래시되면 해당 node 프로세스는 이미 죽었으므로 락을 정리하지 않으면 409가 영구화됨.
  try {
    const cleared = await db.prepare("UPDATE sync_log SET status='failed', error_msg='서버 재시작 감지 — 자동 해제', finished_at=datetime('now','localtime') WHERE status='running'").run();
    if (cleared && cleared.changes > 0) console.log(`[init] sync_log stale 자동 해제: ${cleared.changes}건`);
  } catch(_) {}
  console.log('[init] sync_log 테이블 OK');
} catch(e) {
  console.error('[init] ★ sync_log 테이블 생성 실패:', e.message);
}

// ── BOM 조판 계산 확장 컬럼 ──
try { await db.exec("ALTER TABLE bom_items ADD COLUMN material_type TEXT DEFAULT 'IMPOSITION'"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN paper_standard TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN paper_type TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN gsm INTEGER DEFAULT 0"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN finished_w REAL DEFAULT 0"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN finished_h REAL DEFAULT 0"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN bleed REAL DEFAULT 3"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN grip REAL DEFAULT 10"); } catch {}
try { await db.exec("ALTER TABLE bom_items ADD COLUMN loss_rate REAL DEFAULT 5"); } catch {}
try { await db.exec("ALTER TABLE bom_header ADD COLUMN default_order_qty INTEGER DEFAULT 1000"); } catch {}
try { await db.exec("ALTER TABLE bom_header ADD COLUMN finished_w REAL DEFAULT 0"); } catch {}
try { await db.exec("ALTER TABLE bom_header ADD COLUMN finished_h REAL DEFAULT 0"); } catch {}

// ── os_number 컬럼 추가 (발주 프로세스 자동화) ──
try { await db.exec("ALTER TABLE po_header ADD COLUMN os_number TEXT DEFAULT ''"); } catch(_) {}

// ── 신제품 관리 컬럼 추가 ──
try { await db.exec("ALTER TABLE products ADD COLUMN is_new_product INTEGER DEFAULT 0"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN first_order_done INTEGER DEFAULT 0"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN die_cost INTEGER DEFAULT 0"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN lead_time_days INTEGER DEFAULT 0"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN post_vendor TEXT DEFAULT ''"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN unit TEXT DEFAULT 'EA'"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN op_category TEXT DEFAULT ''"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN temp_code TEXT DEFAULT ''"); } catch(e) {}
// 규격 — XERP mmInoutItem.ItemSpec 에서 1회성 동기화됨. 한번 채워지면 수동 수정 전까지 유지.
try { await db.exec("ALTER TABLE products ADD COLUMN spec TEXT DEFAULT ''"); } catch(e) {}
// inventory_snapshot 에 창고별 재고 JSON 컬럼 — { "MF01": 100, "MT01": 50, ... }
// 프론트가 창고 드롭다운으로 즉시 필터링 가능. current_stock 은 전체 합산을 그대로 유지(legacy 호환).
try { await db.exec("ALTER TABLE inventory_snapshot ADD COLUMN warehouses_json TEXT DEFAULT ''"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN moq TEXT DEFAULT ''"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN payment_terms TEXT DEFAULT ''"); } catch(e) {}
try { await db.exec("ALTER TABLE products ADD COLUMN supplier_id INTEGER DEFAULT 0"); } catch(e) {}

// ── 생산지별 기본 리드타임 (일) ──
const ORIGIN_LEAD_TIME = { '중국': 50, '한국': 7, '더기프트': 14 };

// ── 공유 상수: PO 상태 매핑 (중복 제거) ──
const PO_STATUS_EN_TO_KO = { 'draft':'대기', 'sent':'발송', 'confirmed':'확인', 'partial':'수령중', 'received':'완료', 'cancelled':'취소', 'os_pending':'OS등록대기', 'os_registered':'OS검증대기' };
const PO_STATUS_KO_TO_EN = { '대기':'draft', '발송':'sent', '확인':'confirmed', '수령중':'partial', '완료':'received', '취소':'cancelled', 'OS등록대기':'os_pending', 'OS검증대기':'os_registered' };
const MATERIAL_STATUS_KO = { 'sent':'발주완료', 'confirmed':'확인', 'scheduled':'출고예정', 'shipped':'출고완료' };
const PROCESS_STATUS_KO = { 'waiting':'대기', 'sent':'발주완료', 'confirmed':'확인', 'working':'작업중', 'completed':'완료' };

// ── 후공정 타입: DB에서 로드 (getPostProcessTypes() 사용) ──
let _cachedPostCols = null;
let _processTypesInMemory = null; // DB 테이블 없을 때 인메모리 저장소
const _defaultPostTypes = [
  {id:1,name:'재단',category:'post',group_name:'',icon:'⚙️',sort_order:1,is_active:1,default_vendor:''},
  {id:2,name:'인쇄',category:'post',group_name:'',icon:'⚙️',sort_order:2,is_active:1,default_vendor:''},
  {id:3,name:'박/형압',category:'post',group_name:'',icon:'⚙️',sort_order:3,is_active:1,default_vendor:''},
  {id:4,name:'톰슨',category:'post',group_name:'',icon:'⚙️',sort_order:4,is_active:1,default_vendor:''},
  {id:5,name:'봉투가공',category:'post',group_name:'',icon:'⚙️',sort_order:5,is_active:1,default_vendor:''},
  {id:22,name:'단면접착',category:'post',group_name:'',icon:'⚙️',sort_order:6,is_active:1,default_vendor:''},
  {id:10,name:'우찌누끼',category:'post',group_name:'',icon:'⚙️',sort_order:7,is_active:1,default_vendor:'예지가'},
  {id:21,name:'접지',category:'post',group_name:'',icon:'⚙️',sort_order:8,is_active:1,default_vendor:''},
  {id:23,name:'코팅',category:'post',group_name:'',icon:'⚙️',sort_order:9,is_active:1,default_vendor:''},
  // 아래 4종은 미사용 — 과거 po_items 참조 호환을 위해 row 유지, is_active=0
  {id:6,name:'세아리',category:'post',group_name:'',icon:'⚙️',sort_order:90,is_active:0,default_vendor:''},
  {id:7,name:'레이져',category:'post',group_name:'',icon:'⚙️',sort_order:91,is_active:0,default_vendor:''},
  {id:8,name:'실크',category:'post',group_name:'',icon:'⚙️',sort_order:92,is_active:0,default_vendor:''},
  {id:9,name:'임가공',category:'post',group_name:'',icon:'⚙️',sort_order:93,is_active:0,default_vendor:''}
];
const _defaultBomTypes = [
  {id:11,name:'오프셋인쇄',category:'bom',group_name:'인쇄',icon:'🖨️',sort_order:1,is_active:1,default_vendor:''},
  {id:12,name:'디지털인쇄',category:'bom',group_name:'인쇄',icon:'💻',sort_order:2,is_active:1,default_vendor:''},
  {id:13,name:'박가공',category:'bom',group_name:'후가공',icon:'✨',sort_order:3,is_active:1,default_vendor:''},
  {id:14,name:'형압',category:'bom',group_name:'후가공',icon:'🔲',sort_order:4,is_active:1,default_vendor:''},
  {id:15,name:'에폭시',category:'bom',group_name:'후가공',icon:'💎',sort_order:5,is_active:1,default_vendor:''},
  {id:16,name:'톰슨',category:'bom',group_name:'후가공',icon:'✂️',sort_order:6,is_active:1,default_vendor:''},
  {id:17,name:'코팅/라미',category:'bom',group_name:'후가공',icon:'🛡️',sort_order:7,is_active:1,default_vendor:''},
  {id:18,name:'접지',category:'bom',group_name:'제본',icon:'📐',sort_order:8,is_active:1,default_vendor:''},
  {id:19,name:'제본',category:'bom',group_name:'제본',icon:'📚',sort_order:9,is_active:1,default_vendor:''},
  {id:20,name:'포장',category:'bom',group_name:'포장',icon:'📦',sort_order:10,is_active:1,default_vendor:''},
  {id:23,name:'단면접착',category:'bom',group_name:'후가공',icon:'🩹',sort_order:11,is_active:1,default_vendor:''}
];
let _processTypesTableExists = null;

async function checkProcessTypesTable() {
  if (_processTypesTableExists !== null) return _processTypesTableExists;
  try {
    await db.prepare("SELECT COUNT(*) AS cnt FROM process_types").get();
    _processTypesTableExists = true;
  } catch(e) {
    _processTypesTableExists = false;
    // 인메모리 초기화
    _processTypesInMemory = [..._defaultPostTypes, ..._defaultBomTypes];
    // JSON 파일로 영속화 시도
    try {
      const ptFile = path.join(__dirname, 'process_types.json');
      if (fs.existsSync(ptFile)) _processTypesInMemory = JSON.parse(fs.readFileSync(ptFile, 'utf8'));
    } catch(_) {}
  }
  return _processTypesTableExists;
}

function saveProcessTypesToFile() {
  if (!_processTypesInMemory) return;
  try { fs.writeFileSync(path.join(__dirname, 'process_types.json'), JSON.stringify(_processTypesInMemory, null, 2)); } catch(_) {}
}

async function getPostProcessTypes() {
  if (_cachedPostCols) return _cachedPostCols;
  const hasTable = await checkProcessTypesTable();
  if (hasTable) {
    try {
      const rows = await db.prepare("SELECT name FROM process_types WHERE category='post' AND is_active=1 ORDER BY sort_order, id").all();
      if (rows.length > 0) { _cachedPostCols = rows.map(r => r.name); return _cachedPostCols; }
    } catch(e) { /* fallback */ }
  } else if (_processTypesInMemory) {
    _cachedPostCols = _processTypesInMemory.filter(p => p.category === 'post' && p.is_active).sort((a,b) => a.sort_order - b.sort_order).map(p => p.name);
    return _cachedPostCols;
  }
  return ['재단','인쇄','박/형압','톰슨','봉투가공','단면접착','우찌누끼','접지','코팅'];
}
function invalidatePostColsCache() { _cachedPostCols = null; }

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
try { await db.exec("ALTER TABLE po_header ADD COLUMN material_status TEXT DEFAULT 'sent'"); } catch(e) {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN process_status TEXT DEFAULT 'waiting'"); } catch(e) {}

// ── 발송처리 시점 기록 ──
try { await db.exec("ALTER TABLE po_header ADD COLUMN shipped_at TEXT DEFAULT ''"); } catch(e) {}

// ── 불량 처리 발주 연결 컬럼 추가 ──
try { await db.exec("ALTER TABLE po_header ADD COLUMN defect_id INTEGER DEFAULT NULL"); } catch(e) {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN defect_number TEXT DEFAULT ''"); } catch(e) {}

// ── 생산지(origin) 컬럼 추가 (한국/중국/더기프트 분리) ──
try { await db.exec("ALTER TABLE po_header ADD COLUMN origin TEXT DEFAULT ''"); } catch(_) {}

// ============================================================
// 법인(legal_entity) 컬럼 일괄 추가 — 바른컴퍼니(barunson) / 디디(dd) 분리
// ============================================================
const _entityTables = [
  'products', 'po_header', 'po_items', 'trade_document',
  'receipts', 'defects', 'batch_master', 'work_orders'
];
// 1st pass: 이 시점에 이미 존재하는 테이블만 대상.
// trade_document/defects/batch_master/work_orders 는 뒤에서 CREATE 되므로 skip — 뒤의 2nd pass 에서 처리.
// 존재 체크 없이 ALTER 하면 pg-adapter 가 "relation does not exist" 를 ERROR 레벨로 로그해 노이즈.
for (const tbl of _entityTables) {
  try {
    const exists = await db.prepare("SELECT 1 AS x FROM information_schema.tables WHERE table_name=?").get(tbl);
    if (!exists) continue;
    await db.exec(`ALTER TABLE ${tbl} ADD COLUMN legal_entity TEXT DEFAULT 'barunson'`);
  } catch(_) {}
}
try { await db.exec("CREATE INDEX IF NOT EXISTS idx_products_entity ON products(legal_entity)"); } catch(_) {}
try { await db.exec("CREATE INDEX IF NOT EXISTS idx_po_header_entity ON po_header(legal_entity)"); } catch(_) {}

// ── DD 자동 동기화로 들어왔던 origin='DD' 품목 정리 (1회성, 거래 미연결 확인 완료) ──
try {
  const _ddCleanup = await db.prepare("SELECT COUNT(*) AS c FROM products WHERE origin='DD' OR brand='DD'").get();
  if (_ddCleanup && Number(_ddCleanup.c) > 0) {
    const _used = await db.prepare(`SELECT COUNT(*) AS c FROM po_items i
      JOIN products p ON p.product_code=i.product_code
      WHERE p.origin='DD' OR p.brand='DD'`).get();
    // v1.0.8: origin='DD'를 '한국'으로 강제 UPDATE 하던 로직 제거 —
    // 원래 생산지(한국/중국/더기프트)가 소실되어 발주서 작성 탭에서 DD 품목이 바른손과 섞이는 버그 유발.
    // 이제는 legal_entity만 마킹하고 origin은 건드리지 않는다.
    if (_used && Number(_used.c) === 0) {
      await db.prepare("DELETE FROM products WHERE origin='DD' OR brand='DD'").run();
      console.log(`[entity-migration] DD 더미 품목 ${_ddCleanup.c}건 삭제 완료`);
    } else {
      await db.prepare("UPDATE products SET legal_entity='dd' WHERE (origin='DD' OR brand='DD')").run();
      console.log(`[entity-migration] DD 품목 ${_ddCleanup.c}건 dd 법인으로 마킹 (origin 보존, 거래 ${_used.c}건 연결)`);
    }
  }
} catch(e) { console.warn('[entity-migration] DD 정리 오류:', e.message); }

// ── 기존 거래 데이터 backfill (legal_entity NULL/'' → 'barunson') ──
for (const tbl of _entityTables) {
  try {
    await db.prepare(`UPDATE ${tbl} SET legal_entity='barunson' WHERE legal_entity IS NULL OR legal_entity=''`).run();
  } catch(e) { /* 컬럼 없으면 무시 */ }
}
// DD 제품 마킹: product_code가 DD로 시작하는 품목 → legal_entity='dd'
try {
  const ddUpdated = await db.prepare("UPDATE products SET legal_entity='dd' WHERE product_code LIKE 'DD%' AND (legal_entity IS NULL OR legal_entity='barunson')").run();
  if (ddUpdated.changes > 0) console.log(`[entity-backfill] DD 제품 ${ddUpdated.changes}건 → legal_entity='dd' 마킹`);
  // PO도 DD 품목이 포함된 것은 dd로 마킹
  const ddPoUpdated = await db.prepare(`UPDATE po_header SET legal_entity='dd' WHERE po_id IN (SELECT DISTINCT po_id FROM po_items WHERE product_code LIKE 'DD%') AND (legal_entity IS NULL OR legal_entity='barunson')`).run();
  if (ddPoUpdated.changes > 0) console.log(`[entity-backfill] DD 관련 PO ${ddPoUpdated.changes}건 → legal_entity='dd' 마킹`);
} catch(e) { console.warn('[entity-backfill] DD 마킹 오류:', e.message); }

// ── DDL 권한 부족 대응: onely 계정으로 legal_entity 컬럼 추가 시도 ──
// sc_erp 계정은 ALTER TABLE 불가 → onely(superuser)로 별도 연결하여 DDL 실행
{
  const { Pool: _PgPool } = require('pg');
  const _ddlHost = envVars.PG_HOST || process.env.PG_HOST || 'onely-postgres';
  const _ddlPort = parseInt(envVars.PG_PORT || process.env.PG_PORT || '5432');
  const _ddlDb = envVars.PG_DATABASE || process.env.PG_DATABASE || 'sc_erp';
  const _ddlPool = new _PgPool({ host: _ddlHost, port: _ddlPort, user: 'onely', password: 'onely', database: _ddlDb, max: 2 });
  let _ddlOk = 0;
  for (const tbl of _entityTables) {
    try {
      const chk = await _ddlPool.query("SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name='legal_entity'", [tbl]);
      if (!chk.rows.length) {
        await _ddlPool.query(`ALTER TABLE ${tbl} ADD COLUMN legal_entity TEXT DEFAULT 'barunson'`);
        _ddlOk++;
        console.log(`[DDL/onely] ${tbl}.legal_entity 컬럼 추가 완료`);
      }
    } catch(e) { /* 테이블 없거나 이미 존재 */ }
  }
  if (_ddlOk) console.log(`[DDL/onely] ${_ddlOk}개 테이블에 legal_entity 컬럼 추가`);

  // ── sync_log / inventory_snapshot 누락 컬럼 보강 (onely 권한으로) ──
  // 운영 PG 의 이 테이블들이 구 스키마로 만들어져 있으면 INSERT 실패 → snapshot_disabled → "live 리프레시 중" 루프.
  // sc_erp 계정이 ALTER 권한 없어서 이전 보강 코드(db.exec)가 silent-fail 하던 문제 해결.
  const _criticalCols = [
    ['sync_log', 'triggered_by',  "TEXT DEFAULT 'manual'"],
    ['sync_log', 'error_msg',     "TEXT DEFAULT ''"],
    ['sync_log', 'fail_count',    "INTEGER DEFAULT 0"],
    ['sync_log', 'success_count', "INTEGER DEFAULT 0"],
    ['sync_log', 'finished_at',   "TEXT DEFAULT ''"],
    ['sync_log', 'status',        "TEXT DEFAULT 'running'"],
    ['inventory_snapshot', 'legal_entity', "TEXT DEFAULT 'barunson'"],
    ['inventory_snapshot', 'site_code',    "TEXT DEFAULT 'BK10'"],
    ['inventory_snapshot', 'monthly_out',  "INTEGER DEFAULT 0"],
    ['inventory_snapshot', 'daily_out',    "INTEGER DEFAULT 0"],
    ['inventory_snapshot', 'total_3m',     "INTEGER DEFAULT 0"],
    ['inventory_snapshot', 'item_name',    "TEXT DEFAULT ''"],
    ['inventory_snapshot', 'synced_at',    "TEXT DEFAULT ''"]
  ];
  let _criticalOk = 0;
  for (const [tbl, col, type] of _criticalCols) {
    try {
      const chk = await _ddlPool.query("SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2", [tbl, col]);
      if (!chk.rows.length) {
        await _ddlPool.query(`ALTER TABLE ${tbl} ADD COLUMN ${col} ${type}`);
        _criticalOk++;
        console.log(`[DDL/onely] ${tbl}.${col} 컬럼 추가 완료`);
      }
    } catch(e) { /* 테이블 자체가 없거나 이미 존재 — 초기 CREATE 단계가 처리 */ }
  }
  if (_criticalOk) console.log(`[DDL/onely] sync_log/inventory_snapshot 누락 컬럼 ${_criticalOk}개 보강 완료`);

  // GRANT 권한도 부여 (sc_erp 계정이 읽기/쓰기 가능하도록)
  try { await _ddlPool.query("GRANT ALL ON ALL TABLES IN SCHEMA public TO sc_erp"); } catch(e) {}
  // 시퀀스도 GRANT (AUTOINCREMENT/SERIAL 쓰는 테이블 INSERT 시 nextval 권한 필요)
  try { await _ddlPool.query("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO sc_erp"); } catch(e) {}
  await _ddlPool.end();
}

// ── 컬럼 존재 여부 점검 (PG 권한 부족으로 ALTER 실패한 테이블 대응) ──
// 운영 PG의 일부 테이블은 owner가 아니라서 ALTER TABLE이 silent fail함.
// 컬럼이 실제로 있는 테이블만 entity 필터/저장을 적용해서 페이지 깨짐을 방지.
const _hasEntity = {};
for (const tbl of _entityTables) {
  try {
    const r = await db.prepare(
      "SELECT 1 AS x FROM information_schema.columns WHERE table_name=? AND column_name='legal_entity'"
    ).get(tbl);
    _hasEntity[tbl] = !!r;
  } catch(e) { _hasEntity[tbl] = false; }
}
console.log('[entity] legal_entity 컬럼 존재 여부:', _hasEntity);

// ── 납품 스케줄 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS vendor_shipment_schedule (
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
await db.exec(`CREATE TABLE IF NOT EXISTS process_lead_time (
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
await db.exec(`CREATE TABLE IF NOT EXISTS lead_time_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_name TEXT NOT NULL,
  process_type TEXT NOT NULL,
  old_days INTEGER,
  new_days INTEGER,
  changed_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_lt_hist_vendor ON lead_time_history(vendor_name)`);

// ── 거래명세서 테이블 (v2 - 기존 invoice 플로우 대체) ──
await db.exec(`CREATE TABLE IF NOT EXISTS trade_document (
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
await db.exec(`CREATE INDEX IF NOT EXISTS idx_shipment_po ON vendor_shipment_schedule(po_id)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_shipment_date ON vendor_shipment_schedule(ship_date)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_shipment_status ON vendor_shipment_schedule(status)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_doc_po ON trade_document(po_id)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_doc_status ON trade_document(status)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_trade_doc_vendor ON trade_document(vendor_name)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_lead_time_vendor ON process_lead_time(vendor_name)`);

// ── 후공정 단가 마스터 ──
await db.exec(`CREATE TABLE IF NOT EXISTS post_process_price (
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
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_price_vendor ON post_process_price(vendor_name)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_price_process ON post_process_price(process_type)`);

// ── 후공정 거래 이력 ──
await db.exec(`CREATE TABLE IF NOT EXISTS post_process_history (
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
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_vendor ON post_process_history(vendor_name)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_product ON post_process_history(product_code)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_month ON post_process_history(month)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pp_hist_process ON post_process_history(process_type)`);

// ── 제품별 후공정 매핑 ──
await db.exec(`CREATE TABLE IF NOT EXISTS product_process_map (
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
await db.exec(`CREATE INDEX IF NOT EXISTS idx_ppm_product ON product_process_map(product_code)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_ppm_vendor ON product_process_map(vendor_name)`);

// ── 공정 타입 마스터 (동적 관리) ──
await db.exec(`CREATE TABLE IF NOT EXISTS process_types (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  category TEXT NOT NULL DEFAULT 'post',
  group_name TEXT DEFAULT '',
  icon TEXT DEFAULT '⚙️',
  sort_order INTEGER DEFAULT 0,
  is_active INTEGER DEFAULT 1,
  default_vendor TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(name, category)
)`);
// 시드 데이터: 사용 중인 후공정 타입 9종 (2026-04-22 재확장).
// 과거 시드된 4종(세아리/레이져/실크/임가공)은 아래 마이그레이션에서 is_active=0 으로 비활성화.
// 우찌누끼/접지/코팅은 이전 트리밍에서 비활성화됐다가 이번에 재활성화.
try {
  const seedPost = [
    {name:'재단',sort:1},{name:'인쇄',sort:2},{name:'박/형압',sort:3},{name:'톰슨',sort:4},
    {name:'봉투가공',sort:5},{name:'단면접착',sort:6},
    {name:'우찌누끼',sort:7,vendor:'예지가'},{name:'접지',sort:8},{name:'코팅',sort:9}
  ];
  const seedBom = [
    {name:'오프셋인쇄',group:'인쇄',icon:'🖨️',sort:1},{name:'디지털인쇄',group:'인쇄',icon:'💻',sort:2},
    {name:'박가공',group:'후가공',icon:'✨',sort:3},{name:'형압',group:'후가공',icon:'🔲',sort:4},
    {name:'에폭시',group:'후가공',icon:'💎',sort:5},{name:'톰슨',group:'후가공',icon:'✂️',sort:6},
    {name:'코팅/라미',group:'후가공',icon:'🛡️',sort:7},{name:'접지',group:'제본',icon:'📐',sort:8},
    {name:'제본',group:'제본',icon:'📚',sort:9},{name:'포장',group:'포장',icon:'📦',sort:10},
    {name:'단면접착',group:'후가공',icon:'🩹',sort:11}
  ];
  const ins = db.prepare("INSERT OR IGNORE INTO process_types (name,category,group_name,icon,sort_order,default_vendor) VALUES (?,?,?,?,?,?)");
  for (const s of seedPost) await ins.run(s.name,'post','',s.icon||'⚙️',s.sort,s.vendor||'');
  for (const s of seedBom) await ins.run(s.name,'bom',s.group||'',s.icon||'⚙️',s.sort,'');
  // 마이그레이션 1: 더 이상 사용하지 않는 4종 비활성화 (기존 시드된 row 가 있을 때).
  // 데이터 삭제는 안 함 — 과거 po_items 의 process_type 으로 참조될 수 있어 hidden 처리만.
  try {
    await db.prepare("UPDATE process_types SET is_active=0 WHERE category='post' AND name IN ('세아리','레이져','실크','임가공')").run();
  } catch(_) {}
  // 마이그레이션 2: 이전 트리밍에서 비활성화됐던 3종(우찌누끼/접지/코팅) 재활성화 + sort_order 정리.
  // INSERT OR IGNORE 는 이미 존재하는 row 의 sort_order/is_active 를 덮어쓰지 않으므로 명시적으로 UPDATE.
  try {
    await db.prepare("UPDATE process_types SET is_active=1, sort_order=7 WHERE category='post' AND name='우찌누끼'").run();
    await db.prepare("UPDATE process_types SET is_active=1, sort_order=8 WHERE category='post' AND name='접지'").run();
    await db.prepare("UPDATE process_types SET is_active=1, sort_order=9 WHERE category='post' AND name='코팅'").run();
  } catch(_) {}
} catch(e) { console.warn('process_types 시드 데이터 삽입 실패 (무시):', e.message); }

// ── 품목 필드 변경 이력 ──
await db.exec(`CREATE TABLE IF NOT EXISTS product_field_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  field_name TEXT NOT NULL,
  old_value TEXT DEFAULT '',
  new_value TEXT DEFAULT '',
  changed_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pfh_code ON product_field_history(product_code)`);
// reason, changed_by 컬럼 추가 (기존 테이블 호환)
try { await db.exec("ALTER TABLE product_field_history ADD COLUMN reason TEXT DEFAULT ''"); } catch(_) {}
try { await db.exec("ALTER TABLE product_field_history ADD COLUMN changed_by TEXT DEFAULT ''"); } catch(_) {}

// ── 중국 선적 이력 ──
await db.exec(`CREATE TABLE IF NOT EXISTS china_shipment_log (
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
try { await db.exec("ALTER TABLE china_shipment_log ADD COLUMN bl_number TEXT DEFAULT ''"); } catch(_) {}
try { await db.exec("ALTER TABLE china_shipment_log ADD COLUMN ship_date TEXT DEFAULT ''"); } catch(_) {}
try { await db.exec("ALTER TABLE china_shipment_log ADD COLUMN eta_date TEXT DEFAULT ''"); } catch(_) {}

// ── 선적 ↔ PO 아이템 연결 (파셜 입고 추적) ──
await db.exec(`CREATE TABLE IF NOT EXISTS shipment_po_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shipment_id INTEGER NOT NULL,
  po_id INTEGER NOT NULL,
  po_item_id INTEGER,
  product_code TEXT DEFAULT '',
  product_name TEXT DEFAULT '',
  shipped_qty INTEGER DEFAULT 0,
  received_qty INTEGER DEFAULT 0,
  notes TEXT DEFAULT '',
  UNIQUE(shipment_id, po_item_id)
)`);

// ── 더기프트 포장작업 ──
await db.exec(`CREATE TABLE IF NOT EXISTS gift_assembly (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  assembly_no TEXT UNIQUE,
  product_code TEXT NOT NULL,
  product_name TEXT DEFAULT '',
  target_qty INTEGER DEFAULT 0,
  completed_qty INTEGER DEFAULT 0,
  status TEXT DEFAULT 'planned',
  assembly_date TEXT,
  completed_date TEXT,
  worker_name TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

await db.exec(`CREATE TABLE IF NOT EXISTS gift_assembly_materials (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  assembly_id INTEGER NOT NULL,
  item_code TEXT NOT NULL,
  item_name TEXT DEFAULT '',
  required_qty INTEGER DEFAULT 0,
  issued_qty INTEGER DEFAULT 0,
  lot_id INTEGER,
  UNIQUE(assembly_id, item_code)
)`);

// ── 거래명세서 파일 관리 ──
await db.exec(`CREATE TABLE IF NOT EXISTS trade_doc_files (
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
await db.exec(`CREATE TABLE IF NOT EXISTS po_activity_log (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_activity_po ON po_activity_log(po_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_activity_date ON po_activity_log(created_at)");

// ── po_items에 ship_date 컬럼 추가 (품목별 출고일) ──
try { await db.exec(`ALTER TABLE po_items ADD COLUMN ship_date TEXT DEFAULT ''`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN os_number TEXT DEFAULT ''`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN produced_qty INTEGER DEFAULT 0`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN defect_qty INTEGER DEFAULT 0`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN unit_price REAL DEFAULT 0`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN amount REAL DEFAULT 0`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN tax_amount REAL DEFAULT 0`); } catch(e) {}
try { await db.exec(`ALTER TABLE po_items ADD COLUMN received_date TEXT DEFAULT ''`); } catch(e) {}

// ── 구매마감 업체 첨부파일 (세금계산서/거래명세서) ──
await db.exec(`CREATE TABLE IF NOT EXISTS purchase_closing_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  closing_year INTEGER NOT NULL,
  closing_month INTEGER NOT NULL,
  legal_entity TEXT NOT NULL DEFAULT 'barunson',
  vendor_name TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  file_type TEXT DEFAULT '',
  file_size INTEGER DEFAULT 0,
  parsed_total REAL DEFAULT 0,
  parsed_tax REAL DEFAULT 0,
  parsed_data TEXT DEFAULT '[]',
  match_status TEXT DEFAULT 'pending',
  notes TEXT DEFAULT '',
  uploaded_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── 거래처 불량 보고 ──
await db.exec(`CREATE TABLE IF NOT EXISTS vendor_defect_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER NOT NULL,
  item_id INTEGER,
  product_code TEXT,
  vendor_name TEXT,
  defect_type TEXT NOT NULL,
  defect_qty INTEGER DEFAULT 0,
  description TEXT DEFAULT '',
  action_taken TEXT DEFAULT '',
  status TEXT DEFAULT 'reported',
  reported_at TEXT DEFAULT (datetime('now','localtime')),
  resolved_at TEXT,
  resolved_by TEXT
)`);

// ── 불량/품질 관리 ──
await db.exec(`CREATE TABLE IF NOT EXISTS defects (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_defect_vendor ON defects(vendor_name)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_defect_product ON defects(product_code)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_defect_status ON defects(status)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_defect_date ON defects(defect_date)");

await db.exec(`CREATE TABLE IF NOT EXISTS defect_logs (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_defect_log_defect ON defect_logs(defect_id)");

// ── 생산요청 관리 ──
await db.exec(`CREATE TABLE IF NOT EXISTS production_requests (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_pr_status ON production_requests(status)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_pr_type ON production_requests(product_type)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_pr_date ON production_requests(created_at)");

await db.exec(`CREATE TABLE IF NOT EXISTS production_request_logs (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_pr_log_req ON production_request_logs(request_id)");

// ── 제품 스펙 마스터 ──
await db.exec(`CREATE TABLE IF NOT EXISTS product_spec_master (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_spec_type ON product_spec_master(product_type)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_spec_template ON product_spec_master(is_template)");

// ── 수입검사 (Incoming Inspection) ──
await db.exec(`CREATE TABLE IF NOT EXISTS incoming_inspections (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_insp_po ON incoming_inspections(po_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_insp_date ON incoming_inspections(inspection_date)");

// ── 부적합 처리 (Non-Conformance Report) ──
await db.exec(`CREATE TABLE IF NOT EXISTS ncr (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_ncr_status ON ncr(status)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_ncr_vendor ON ncr(vendor_name)");

await db.exec(`CREATE TABLE IF NOT EXISTS ncr_logs (
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
await db.exec(`CREATE TABLE IF NOT EXISTS vendor_scorecard (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_vs_vendor ON vendor_scorecard(vendor_name)");

// ── 중국 상품별 단가 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS china_price_tiers (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_cpt_product ON china_price_tiers(product_code)");

// ── 중국 재고 DB (매주 엑셀 업로드) ──────────────────────────────
await db.exec(`CREATE TABLE IF NOT EXISTS china_inventory (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  product_name TEXT DEFAULT '',
  cn_stock INTEGER DEFAULT 0,
  incoming_qty INTEGER DEFAULT 0,
  incoming_date TEXT DEFAULT '',
  upload_date TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(product_code)
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_china_inv_code ON china_inventory(product_code)");
// china_inventory 컬럼 확장 (PO 양식)
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN po_no TEXT DEFAULT ''"); } catch(_) {}
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN order_qty INTEGER DEFAULT 0"); } catch(_) {}
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN order_date TEXT DEFAULT ''"); } catch(_) {}
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN due_date TEXT DEFAULT ''"); } catch(_) {}
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN received_qty INTEGER DEFAULT 0"); } catch(_) {}
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN unproduced_qty INTEGER DEFAULT 0"); } catch(_) {}
try { await db.exec("ALTER TABLE china_inventory ADD COLUMN is_complete TEXT DEFAULT 'N'"); } catch(_) {}

// ── 인증/권한 테이블 (S1) ─────────────────────────────────────────
await db.exec(`CREATE TABLE IF NOT EXISTS users (
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
try { await db.exec("ALTER TABLE users ADD COLUMN google_id TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE users ADD COLUMN profile_picture TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE users ADD COLUMN permissions TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE users ADD COLUMN favorites TEXT DEFAULT '[]'"); } catch {}
try { await db.exec("ALTER TABLE vendors ADD COLUMN email_cc TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE product_post_vendor ADD COLUMN step_order INTEGER DEFAULT 1"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN process_step INTEGER DEFAULT 0"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN parent_po_id INTEGER DEFAULT NULL"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN process_chain TEXT DEFAULT ''"); } catch {}
// ±5% tolerance + force-approve columns
try { await db.exec("ALTER TABLE po_header ADD COLUMN tolerance_pct REAL DEFAULT 5.0"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN force_completed INTEGER DEFAULT 0"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN force_completed_by TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN force_completed_at TEXT DEFAULT ''"); } catch {}
// 제지사→후공정 2-stage flow columns
try { await db.exec("ALTER TABLE po_header ADD COLUMN material_vendor_name TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN process_vendor_name TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN material_send_date TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN material_confirmed_at TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE po_header ADD COLUMN process_email_sent INTEGER DEFAULT 0"); } catch {}
// 거래처 입고일 확정
try { await db.exec("ALTER TABLE po_header ADD COLUMN vendor_confirmed_date TEXT DEFAULT ''"); } catch {} // 거래처가 확정한 입고일
try { await db.exec("ALTER TABLE po_header ADD COLUMN vendor_confirmed_at TEXT DEFAULT ''"); } catch {} // 확정 시각
// 중국 신제품/리오더 구분
try { await db.exec("ALTER TABLE po_header ADD COLUMN order_type TEXT DEFAULT ''"); } catch {} // new/reorder
// 더기프트 출고 트래킹
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN delivery_status TEXT DEFAULT ''"); } catch {} // packed/shipped/delivered
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN tracking_number TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN carrier TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN shipped_date TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN delivered_date TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN delivery_address TEXT DEFAULT ''"); } catch {}
try { await db.exec("ALTER TABLE gift_assembly ADD COLUMN recipient_name TEXT DEFAULT ''"); } catch {}

// ── po_header.origin 빈 값 backfill (품목 → products.origin 매핑) ──
try {
  const emptyOriginPOs = await db.prepare("SELECT po_id FROM po_header WHERE origin='' OR origin IS NULL").all();
  let backfilled = 0;
  for (const po of emptyOriginPOs) {
    const item = await db.prepare("SELECT pi.product_code FROM po_items pi WHERE pi.po_id=? LIMIT 1").get(po.po_id);
    if (item) {
      const prod = await db.prepare("SELECT origin FROM products WHERE product_code=?").get(item.product_code);
      if (prod && prod.origin) {
        await db.prepare("UPDATE po_header SET origin=? WHERE po_id=?").run(prod.origin, po.po_id);
        backfilled++;
      }
    }
  }
  if (backfilled > 0) console.log(`[origin backfill] ${backfilled}/${emptyOriginPOs.length} PO에 origin 설정 완료`);
} catch (e) { console.warn('origin backfill 실패:', e.message); }

// ── 업무관리 ──────────────────────────────────────────────────────
await db.exec(`CREATE TABLE IF NOT EXISTS tasks (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_task_due ON tasks(due_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_task_assignee ON tasks(assignee)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_task_category ON tasks(category)");

await db.exec(`CREATE TABLE IF NOT EXISTS task_comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id INTEGER NOT NULL,
  author TEXT DEFAULT '',
  content TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  FOREIGN KEY (task_id) REFERENCES tasks(id)
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_tc_task ON task_comments(task_id)");

// ── 업무 단계 (Workflow Steps) ──
await db.exec(`CREATE TABLE IF NOT EXISTS task_steps (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_ts_task ON task_steps(task_id)");

// ── tasks 테이블에 template_id 컬럼 추가 ──
try { await db.exec("ALTER TABLE tasks ADD COLUMN template_id TEXT DEFAULT ''"); } catch(_) {}

// ── 부속품 마스터 ──
await db.exec(`CREATE TABLE IF NOT EXISTS accessories (
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

try { await db.exec("ALTER TABLE accessories ADD COLUMN origin TEXT DEFAULT '한국'"); } catch(_) {}

// ── 제품↔부속품 연결 ──
await db.exec(`CREATE TABLE IF NOT EXISTS product_accessories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  acc_id INTEGER NOT NULL,
  qty_per INTEGER DEFAULT 1,
  UNIQUE(product_code, acc_id)
)`);

await db.exec(`CREATE TABLE IF NOT EXISTS po_drafts (
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

// v1.0.9: vendor_notes 테이블 (미팅일지) — CREATE 누락 수정
await db.exec(`CREATE TABLE IF NOT EXISTS vendor_notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER,
  vendor_name TEXT DEFAULT '',
  title TEXT NOT NULL DEFAULT '',
  content TEXT DEFAULT '',
  note_type TEXT DEFAULT 'meeting',
  note_date TEXT DEFAULT (date('now','localtime')),
  status TEXT DEFAULT 'open',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
try { await db.exec("ALTER TABLE vendor_notes ADD COLUMN status TEXT DEFAULT 'open'"); } catch(_) {}
try { await db.exec("ALTER TABLE vendor_notes ADD COLUMN updated_at TEXT DEFAULT (datetime('now','localtime'))"); } catch(_) {}

// ── 자료함 (reference docs) ──
await db.exec(`CREATE TABLE IF NOT EXISTS reference_docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL DEFAULT 'link',
  title TEXT NOT NULL DEFAULT '',
  url TEXT DEFAULT '',
  file_path TEXT DEFAULT '',
  file_name TEXT DEFAULT '',
  file_size INTEGER DEFAULT 0,
  category TEXT DEFAULT '',
  memo TEXT DEFAULT '',
  uploader TEXT DEFAULT '',
  uploader_id INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
// v1.0.8: po_drafts 법인 분리
try { await db.exec("ALTER TABLE po_drafts ADD COLUMN legal_entity TEXT DEFAULT 'barunson'"); } catch(_) {}
// v1.0.9: po_drafts 완료/연결PO
try { await db.exec("ALTER TABLE po_drafts ADD COLUMN linked_po_id INTEGER DEFAULT 0"); } catch(_) {}
try { await db.exec("ALTER TABLE po_drafts ADD COLUMN completed_at TEXT"); } catch(_) {}

// ── 불량 클레임 정산 (defect settlements) ──
await db.exec(`CREATE TABLE IF NOT EXISTS defect_settlements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  defect_id INTEGER NOT NULL,
  defect_number TEXT DEFAULT '',
  vendor_name TEXT NOT NULL,
  claim_amount REAL DEFAULT 0,
  settled_amount REAL DEFAULT 0,
  balance REAL DEFAULT 0,
  status TEXT DEFAULT 'open',
  applied_po_id INTEGER,
  applied_po_number TEXT DEFAULT '',
  applied_at TEXT DEFAULT '',
  applied_by TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_settle_vendor ON defect_settlements(vendor_name)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_settle_status ON defect_settlements(status)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_settle_defect ON defect_settlements(defect_id)");

await db.exec(`CREATE TABLE IF NOT EXISTS note_comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  note_id INTEGER NOT NULL,
  author TEXT DEFAULT '',
  content TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_nc_note ON note_comments(note_id)");

await db.exec(`CREATE TABLE IF NOT EXISTS reports (
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
try { await db.exec("UPDATE users SET password_hash = '' WHERE password_hash IS NULL"); } catch {}

await db.exec(`CREATE TABLE IF NOT EXISTS audit_log (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)");

await db.exec(`CREATE TABLE IF NOT EXISTS error_logs (
  error_id INTEGER PRIMARY KEY AUTOINCREMENT,
  level TEXT DEFAULT 'error',
  message TEXT NOT NULL,
  stack TEXT DEFAULT '',
  url TEXT DEFAULT '',
  method TEXT DEFAULT '',
  user_id INTEGER,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_error_created ON error_logs(created_at)");

// ── 더기프트 세트 생산재고 ─────────────────────────────────────────
await db.exec(`CREATE TABLE IF NOT EXISTS gift_sets (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_gs_code ON gift_sets(set_code)");

// xerp_code 컬럼 추가 (출고재고 XERP 연동용)
try { await db.exec("ALTER TABLE gift_sets ADD COLUMN xerp_code TEXT DEFAULT ''"); } catch(e) { /* 이미 존재 */ }

await db.exec(`CREATE TABLE IF NOT EXISTS gift_set_bom (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  set_id        INTEGER NOT NULL,
  item_type     TEXT NOT NULL,
  item_code     TEXT NOT NULL,
  item_name     TEXT DEFAULT '',
  qty_per       REAL DEFAULT 1,
  unit          TEXT DEFAULT 'EA',
  UNIQUE(set_id, item_type, item_code)
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_gsb_set ON gift_set_bom(set_id)");

await db.exec(`CREATE TABLE IF NOT EXISTS gift_set_transactions (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  set_id     INTEGER NOT NULL,
  tx_type    TEXT NOT NULL,
  qty        INTEGER NOT NULL,
  operator   TEXT DEFAULT '',
  memo       TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_gst_set ON gift_set_transactions(set_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_gst_created ON gift_set_transactions(created_at)");

// ── 더기프트 출고 스케줄 (출고예정 관리) ──
await db.exec(`CREATE TABLE IF NOT EXISTS gift_shipment_schedule (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  set_id        INTEGER NOT NULL,
  set_code      TEXT DEFAULT '',
  set_name      TEXT DEFAULT '',
  ship_date     TEXT NOT NULL,
  planned_qty   INTEGER DEFAULT 0,
  shipped_qty   INTEGER DEFAULT 0,
  status        TEXT DEFAULT 'planned',
  order_ref     TEXT DEFAULT '',
  recipient     TEXT DEFAULT '',
  address       TEXT DEFAULT '',
  notes         TEXT DEFAULT '',
  created_at    TEXT DEFAULT (datetime('now','localtime')),
  updated_at    TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_gss_set ON gift_shipment_schedule(set_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_gss_date ON gift_shipment_schedule(ship_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_gss_status ON gift_shipment_schedule(status)");

// 소비기한 관리 컬럼 추가
try { await db.exec("ALTER TABLE gift_sets ADD COLUMN expiry_date TEXT DEFAULT ''"); } catch(e) {}
try { await db.exec("ALTER TABLE gift_set_transactions ADD COLUMN expiry_date TEXT DEFAULT ''"); } catch(e) {}

// ── 더기프트 품목 자동 등록 (gift_sets + BOM → products) ──
try {
  const giftSets = await db.prepare("SELECT set_code, set_name FROM gift_sets").all();
  const bomItems = await db.prepare("SELECT DISTINCT item_code, item_name FROM gift_set_bom").all();
  const allGiftCodes = new Map();
  giftSets.forEach(s => allGiftCodes.set(s.set_code, s.set_name));
  bomItems.forEach(b => { if (!allGiftCodes.has(b.item_code)) allGiftCodes.set(b.item_code, b.item_name); });
  if (allGiftCodes.size > 0) {
    const upsert = db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, status)
      VALUES (?, ?, '바른손카드', '더기프트', 'active')
      ON CONFLICT(product_code) DO UPDATE SET origin='더기프트', status='active', updated_at=NOW()
      WHERE products.origin != '더기프트'`);
    const tx = db.transaction(async () => { for (const [code, name] of allGiftCodes) await upsert.run(code, name || ''); });
    await tx();
    console.log(`더기프트 품목 자동 등록/업데이트: ${allGiftCodes.size}건`);
  }
} catch(e) { console.warn('더기프트 품목 자동등록 실패:', e.message); }

// ── 매출관리 캐시 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS sales_daily_cache (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_sdc_date ON sales_daily_cache(sale_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_sdc_source ON sales_daily_cache(source)");

await db.exec(`CREATE TABLE IF NOT EXISTS sales_monthly_cache (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_smc_month ON sales_monthly_cache(sale_month)");

await db.exec(`CREATE TABLE IF NOT EXISTS sales_product_cache (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_spc_month ON sales_product_cache(sale_month)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_spc_product ON sales_product_cache(product_code)");

await db.exec(`CREATE TABLE IF NOT EXISTS sales_settings (
  key             TEXT PRIMARY KEY,
  value           TEXT NOT NULL,
  updated_at      TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("INSERT OR IGNORE INTO sales_settings (key, value) VALUES ('default_range_days', '30')");
await db.exec("INSERT OR IGNORE INTO sales_settings (key, value) VALUES ('cache_ttl_minutes', '30')");

// ── 다중 창고 재고 관리 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS warehouses (
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

await db.exec(`CREATE TABLE IF NOT EXISTS warehouse_inventory (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  warehouse_id  INTEGER NOT NULL,
  product_code  TEXT NOT NULL,
  product_name  TEXT DEFAULT '',
  quantity      INTEGER DEFAULT 0,
  memo          TEXT DEFAULT '',
  updated_at    TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(warehouse_id, product_code)
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_wi_wh ON warehouse_inventory(warehouse_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_wi_pc ON warehouse_inventory(product_code)");

await db.exec(`CREATE TABLE IF NOT EXISTS warehouse_transfers (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_wt_from ON warehouse_transfers(from_warehouse)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_wt_to ON warehouse_transfers(to_warehouse)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_wt_created ON warehouse_transfers(created_at)");

await db.exec(`CREATE TABLE IF NOT EXISTS warehouse_adjustments (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_wa_wh ON warehouse_adjustments(warehouse_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_wa_created ON warehouse_adjustments(created_at)");

// 기본 창고 초기화 (처음 실행 시)
const whCount = await db.prepare("SELECT COUNT(*) as cnt FROM warehouses").get();
if (whCount.cnt === 0) {
  const insertWh = db.prepare("INSERT INTO warehouses (code, name, location, description, is_default) VALUES (?, ?, ?, ?, ?)");
  await insertWh.run('WH-HQ', '본사창고', '본사', 'XERP 연동 기본 창고', 1);
  await insertWh.run('WH-02', '제2창고', '', '', 0);
  await insertWh.run('WH-03', '제3창고', '', '', 0);
  await insertWh.run('WH-04', '제4창고', '', '', 0);
  console.log('[DB] 기본 창고 4개 초기화 완료');
}

// ── 공지/게시판 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS notices (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_notices_status ON notices(status, created_at)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_notices_popup ON notices(is_popup, popup_start, popup_end)");

await db.exec(`CREATE TABLE IF NOT EXISTS notice_reads (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  notice_id  INTEGER NOT NULL,
  user_id    INTEGER NOT NULL,
  read_at    TEXT DEFAULT (datetime('now','localtime')),
  popup_dismissed INTEGER DEFAULT 0
)`);
await db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_nr_unique ON notice_reads(notice_id, user_id)");

// ── 회계 모듈 SQLite 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS gl_account_map (
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
await db.exec(`CREATE TABLE IF NOT EXISTS gl_balance_cache (
  acc_code   TEXT NOT NULL,
  year_month TEXT NOT NULL,
  opening_dr REAL DEFAULT 0, opening_cr REAL DEFAULT 0,
  period_dr  REAL DEFAULT 0, period_cr  REAL DEFAULT 0,
  closing_dr REAL DEFAULT 0, closing_cr REAL DEFAULT 0,
  cached_at  TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(acc_code, year_month)
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS accounting_settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL DEFAULT '',
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS cs_code_cache (
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
await db.exec(`CREATE TABLE IF NOT EXISTS work_orders (
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
await db.exec(`CREATE TABLE IF NOT EXISTS work_order_logs (
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
await db.exec(`CREATE TABLE IF NOT EXISTS hometax_invoices (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_hometax_date ON hometax_invoices(invoice_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_hometax_arap ON hometax_invoices(ar_ap, invoice_date)");

// ── 로트/배치 추적 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS batch_master (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_batch_product ON batch_master(product_code)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_master(quality_status)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_batch_date ON batch_master(received_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_batch_warehouse ON batch_master(warehouse)");

await db.exec(`CREATE TABLE IF NOT EXISTS batch_transactions (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_btxn_batch ON batch_transactions(batch_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_btxn_date ON batch_transactions(txn_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_btxn_type ON batch_transactions(txn_type)");

await db.exec(`CREATE TABLE IF NOT EXISTS batch_inspections (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_binsp_batch ON batch_inspections(batch_id)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_binsp_date ON batch_inspections(insp_date)");

// ── 원재료 단가 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS material_prices (
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
await db.exec("CREATE INDEX IF NOT EXISTS idx_matprice_code ON material_prices(product_code)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_matprice_vendor ON material_prices(vendor_name)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_matprice_month ON material_prices(apply_month)");

// ── Phase 5: 알림센터 ──
await db.exec(`CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY, user_id INTEGER, type TEXT NOT NULL DEFAULT 'system',
  title TEXT NOT NULL DEFAULT '', message TEXT DEFAULT '', link TEXT DEFAULT '',
  is_read INTEGER DEFAULT 0, is_email_sent INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_noti_user ON notifications(user_id, is_read)");

// ── Phase 1: 전자결재 ──
await db.exec(`CREATE TABLE IF NOT EXISTS approvals (
  id SERIAL PRIMARY KEY, approval_no TEXT UNIQUE, doc_type TEXT NOT NULL DEFAULT 'general',
  doc_ref TEXT DEFAULT '', title TEXT NOT NULL DEFAULT '', content TEXT DEFAULT '',
  amount DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'draft',
  requester_id INTEGER, requester_name TEXT DEFAULT '',
  current_step INTEGER DEFAULT 1, total_steps INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (NOW()),
  updated_at TEXT DEFAULT (NOW())
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS approval_lines (
  id SERIAL PRIMARY KEY, approval_id INTEGER NOT NULL, step_order INTEGER NOT NULL,
  approver_id INTEGER, approver_name TEXT DEFAULT '',
  role TEXT DEFAULT 'approver', status TEXT DEFAULT 'pending',
  comment TEXT DEFAULT '', acted_at TEXT,
  UNIQUE(approval_id, step_order)
)`);

// ── 동기화 메타 ──
await db.exec(`CREATE TABLE IF NOT EXISTS sync_meta (
  key TEXT PRIMARY KEY, last_sync TEXT, record_count INTEGER DEFAULT 0, status TEXT DEFAULT 'ok', message TEXT DEFAULT ''
)`);

// ── Phase 2: 수주관리 ──
await db.exec(`CREATE TABLE IF NOT EXISTS sales_orders (
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
try { await db.exec("ALTER TABLE sales_orders ADD COLUMN source TEXT DEFAULT 'manual'"); } catch(_){}
try { await db.exec("ALTER TABLE sales_orders ADD COLUMN external_id TEXT DEFAULT ''"); } catch(_){}
await db.exec(`CREATE TABLE IF NOT EXISTS sales_order_items (
  id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL,
  product_code TEXT DEFAULT '', product_name TEXT DEFAULT '', spec TEXT DEFAULT '',
  unit_price REAL DEFAULT 0, qty INTEGER DEFAULT 0, amount REAL DEFAULT 0, notes TEXT DEFAULT ''
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_sales_order_items_order_id ON sales_order_items(order_id)");

// ── Phase 4: 예산관리 ──
await db.exec(`CREATE TABLE IF NOT EXISTS budgets (
  id INTEGER PRIMARY KEY, year TEXT NOT NULL, month TEXT NOT NULL,
  acc_code TEXT DEFAULT '', acc_name TEXT DEFAULT '',
  budget_type TEXT DEFAULT 'expense', budget_amount REAL DEFAULT 0,
  actual_amount REAL DEFAULT 0, notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(year, month, acc_code)
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS daily_cash (
  id INTEGER PRIMARY KEY, cash_date TEXT NOT NULL,
  acc_code TEXT DEFAULT '', acc_name TEXT DEFAULT '',
  inflow REAL DEFAULT 0, outflow REAL DEFAULT 0, balance REAL DEFAULT 0,
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(cash_date, acc_code)
)`);

// ── 수동 분개 (Manual Journal Entries) ──
await db.exec(`CREATE TABLE IF NOT EXISTS journal_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_no TEXT UNIQUE,
  entry_date TEXT NOT NULL,
  description TEXT DEFAULT '',
  total_amount REAL DEFAULT 0,
  status TEXT DEFAULT 'posted',
  created_by TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS journal_entry_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL,
  line_no INTEGER DEFAULT 1,
  acc_code TEXT NOT NULL,
  acc_name TEXT DEFAULT '',
  debit REAL DEFAULT 0,
  credit REAL DEFAULT 0,
  description TEXT DEFAULT ''
)`);
await db.exec("CREATE INDEX IF NOT EXISTS idx_je_date ON journal_entries(entry_date)");
await db.exec("CREATE INDEX IF NOT EXISTS idx_jel_entry ON journal_entry_lines(entry_id)");

// ── Phase 6: 생산실적 ──
await db.exec(`CREATE TABLE IF NOT EXISTS work_order_results (
  id INTEGER PRIMARY KEY, work_order_id INTEGER NOT NULL,
  result_date TEXT NOT NULL, good_qty INTEGER DEFAULT 0, defect_qty INTEGER DEFAULT 0,
  worker_name TEXT DEFAULT '', work_hours REAL DEFAULT 0, notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// ── Phase 1-6 확장 테이블 ──

// 안전재고 규칙
await db.exec(`CREATE TABLE IF NOT EXISTS safety_stock_rules (
  id INTEGER PRIMARY KEY, product_code TEXT NOT NULL UNIQUE,
  product_name TEXT DEFAULT '', min_qty INTEGER DEFAULT 0,
  reorder_qty INTEGER DEFAULT 0, reorder_point INTEGER DEFAULT 0,
  lead_time_days INTEGER DEFAULT 7, warehouse TEXT DEFAULT '',
  auto_po INTEGER DEFAULT 0, vendor_name TEXT DEFAULT '',
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// 재고실사
await db.exec(`CREATE TABLE IF NOT EXISTS cycle_count_plans (
  id INTEGER PRIMARY KEY, plan_no TEXT UNIQUE,
  plan_date TEXT NOT NULL, warehouse TEXT DEFAULT '',
  status TEXT DEFAULT 'planned', note TEXT DEFAULT '',
  created_by TEXT DEFAULT '', completed_at TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS cycle_count_items (
  id INTEGER PRIMARY KEY, plan_id INTEGER NOT NULL,
  product_code TEXT NOT NULL, product_name TEXT DEFAULT '',
  system_qty INTEGER DEFAULT 0, counted_qty INTEGER,
  variance INTEGER DEFAULT 0, adjusted INTEGER DEFAULT 0,
  note TEXT DEFAULT '',
  UNIQUE(plan_id, product_code)
)`);

// 제조원가
await db.exec(`CREATE TABLE IF NOT EXISTS mfg_cost_cards (
  id INTEGER PRIMARY KEY, product_code TEXT NOT NULL,
  product_name TEXT DEFAULT '', calc_date TEXT NOT NULL,
  material_cost REAL DEFAULT 0, labor_cost REAL DEFAULT 0,
  overhead_cost REAL DEFAULT 0, outsource_cost REAL DEFAULT 0,
  total_cost REAL DEFAULT 0, unit_cost REAL DEFAULT 0,
  qty INTEGER DEFAULT 1, source TEXT DEFAULT 'auto',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(product_code, calc_date)
)`);
await db.exec(`CREATE TABLE IF NOT EXISTS cost_rates (
  id INTEGER PRIMARY KEY, rate_type TEXT NOT NULL,
  rate_key TEXT NOT NULL, rate_value REAL DEFAULT 0,
  unit TEXT DEFAULT '', effective_from TEXT,
  notes TEXT DEFAULT '',
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(rate_type, rate_key)
)`);
// 기본 노무비/경비 시드
try {
  await db.prepare("INSERT OR IGNORE INTO cost_rates (rate_type,rate_key,rate_value,unit,notes) VALUES (?,?,?,?,?)").run('labor','default',25000,'원/시간','기본 노무비 단가');
  await db.prepare("INSERT OR IGNORE INTO cost_rates (rate_type,rate_key,rate_value,unit,notes) VALUES (?,?,?,?,?)").run('overhead','rate',15,'%','제조경비 비율 (재료비+노무비 대비)');
} catch(_){}

// RBAC 권한
await db.exec(`CREATE TABLE IF NOT EXISTS role_permissions (
  id INTEGER PRIMARY KEY, role TEXT NOT NULL,
  permission TEXT NOT NULL, resource TEXT DEFAULT '*',
  granted INTEGER DEFAULT 1,
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(role, permission, resource)
)`);
// 기본 권한 시드
try {
  const seedPerms = db.transaction(async () => {
    const ins = db.prepare("INSERT OR IGNORE INTO role_permissions (role,permission,resource) VALUES (?,?,?)");
    await ins.run('admin', '*', '*');
    for (const p of ['read','write']) { for (const res of ['po','vendor','receipt','inventory','auto-order']) { await ins.run('purchase',p,res); } } await ins.run('purchase','read','dashboard');
    for (const p of ['read','write']) { for (const res of ['sales-order','customer-orders','shipping']) { await ins.run('sales',p,res); } } await ins.run('sales','read','dashboard'); await ins.run('sales','read','inventory');
    for (const p of ['read','write']) { for (const res of ['work-order','production-req','bom','equipment','process-routing']) { await ins.run('production',p,res); } } await ins.run('production','read','dashboard'); await ins.run('production','read','inventory');
    for (const p of ['read','write']) { for (const res of ['journal','budget','tax-invoice','ar-ap','mfg-cost']) { await ins.run('accounting',p,res); } } await ins.run('accounting','read','dashboard');
    await ins.run('executive','read','*');
    await ins.run('viewer','read','*');
  });
  await seedPerms();
} catch(_){}

// 공정 라우팅
await db.exec(`CREATE TABLE IF NOT EXISTS process_routing (
  id INTEGER PRIMARY KEY, product_code TEXT NOT NULL,
  step_no INTEGER NOT NULL, process_name TEXT NOT NULL,
  process_type TEXT DEFAULT 'internal',
  equipment_id INTEGER, vendor_name TEXT DEFAULT '',
  std_time_min REAL DEFAULT 0, setup_time_min REAL DEFAULT 0,
  notes TEXT DEFAULT '',
  UNIQUE(product_code, step_no)
)`);

// 공정별 실적
await db.exec(`CREATE TABLE IF NOT EXISTS process_results (
  id INTEGER PRIMARY KEY, wo_id INTEGER NOT NULL,
  routing_id INTEGER, step_no INTEGER NOT NULL,
  process_name TEXT NOT NULL, equipment_id INTEGER,
  start_time TEXT, end_time TEXT,
  good_qty INTEGER DEFAULT 0, defect_qty INTEGER DEFAULT 0,
  worker_name TEXT DEFAULT '', status TEXT DEFAULT 'pending',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// 설비 마스터
await db.exec(`CREATE TABLE IF NOT EXISTS equipment (
  id INTEGER PRIMARY KEY, eq_code TEXT UNIQUE NOT NULL,
  eq_name TEXT NOT NULL, eq_type TEXT DEFAULT '',
  location TEXT DEFAULT '', status TEXT DEFAULT 'active',
  purchase_date TEXT, manufacturer TEXT DEFAULT '',
  model TEXT DEFAULT '', capacity_per_hour REAL DEFAULT 0,
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// 설비 로그 (가동/비가동/정비)
await db.exec(`CREATE TABLE IF NOT EXISTS equipment_logs (
  id INTEGER PRIMARY KEY, equipment_id INTEGER NOT NULL,
  log_date TEXT NOT NULL, log_type TEXT NOT NULL,
  start_time TEXT, end_time TEXT,
  duration_min REAL DEFAULT 0, reason TEXT DEFAULT '',
  worker_name TEXT DEFAULT '', notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime'))
)`);

// 알림 생성 헬퍼
async function createNotification(userId, type, title, message, link) {
  try {
    await db.prepare("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)").run(userId, type, title, message || '', link || '');
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
const masterUser = await db.prepare("SELECT user_id FROM users WHERE email = ?").get(masterEmail);
if (!masterUser) {
  const hash = bcrypt.hashSync('1234', 10);
  const oldAdmin = await db.prepare("SELECT user_id FROM users WHERE username = 'admin'").get();
  if (oldAdmin) {
    await db.prepare("UPDATE users SET username = ?, email = ?, password_hash = ?, display_name = ?, role = 'admin' WHERE user_id = ?")
      .run('seungchan.back', masterEmail, hash, '백승찬', oldAdmin.user_id);
  } else {
    await db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?, ?, ?, ?, ?)")
      .run('seungchan.back', hash, '백승찬', 'admin', masterEmail);
  }
  console.log('✅ 마스터 계정: seungchan.back@barunn.net / 1234');
} else {
  // 이미 존재하면 admin 역할 보장 + username 통일
  await db.prepare("UPDATE users SET role = 'admin', username = 'seungchan.back' WHERE user_id = ?").run(masterUser.user_id);
}

// 공용 관리자 계정 (admin / 1234) — 다수 사용자 공용 접속용
const adminUsername = 'admin';
const adminEmail2 = 'admin@barunn.net';
const adminExisting = await db.prepare("SELECT user_id, password_hash FROM users WHERE username = ? OR email = ?").get(adminUsername, adminEmail2);
if (!adminExisting) {
  const hash2 = bcrypt.hashSync('1234', 10);
  await db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?, ?, ?, ?, ?)")
    .run(adminUsername, hash2, '관리자(공용)', 'admin', adminEmail2);
  console.log('✅ 공용 관리자 계정 생성: admin / 1234');
} else {
  // 이미 존재하면 admin 역할 + 비밀번호 기본값 보장
  const hash2 = bcrypt.hashSync('1234', 10);
  try {
    const pwOk = adminExisting.password_hash && bcrypt.compareSync('1234', adminExisting.password_hash);
    if (!pwOk) {
      await db.prepare("UPDATE users SET password_hash = ?, role = 'admin' WHERE user_id = ?").run(hash2, adminExisting.user_id);
      console.log('✅ 공용 관리자 계정 비밀번호 초기화: admin / 1234');
    } else {
      await db.prepare("UPDATE users SET role = 'admin' WHERE user_id = ?").run(adminExisting.user_id);
    }
  } catch(e) {
    await db.prepare("UPDATE users SET password_hash = ?, role = 'admin' WHERE user_id = ?").run(hash2, adminExisting.user_id);
  }
}
// 마스터 계정 비밀번호 보장 (seed.db에서 복원 시 비밀번호가 다를 수 있으므로)
const masterCheck = await db.prepare("SELECT user_id, password_hash FROM users WHERE email = ?").get(masterEmail);
if (masterCheck && masterCheck.password_hash) {
  const defaultPw = '1234';
  try {
    if (!bcrypt.compareSync(defaultPw, masterCheck.password_hash)) {
      // 비밀번호가 기본값이 아니면 유지 (사용자가 변경했을 수 있음)
    }
  } catch(e) { console.warn('bcrypt 비교 실패 (무시):', e.message); }
  // username이 admin이면 seungchan.back으로 통일
  await db.prepare("UPDATE users SET username = 'seungchan.back' WHERE user_id = ? AND username = 'admin'").run(masterCheck.user_id);
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
async function auditLog(userId, username, action, resource, resourceId, details, ip) {
  try {
    await db.prepare("INSERT INTO audit_log (user_id, username, action, resource, resource_id, details, ip_address) VALUES (?,?,?,?,?,?,?)")
      .run(userId || 0, username || '', action, resource || '', String(resourceId || ''), details || '', ip || '');
  } catch (e) { console.error('감사 로그 기록 실패:', e.message); }
}

// 에러 로그 기록
async function logError(level, message, stack, url, method, userId) {
  try {
    await db.prepare("INSERT INTO error_logs (level, message, stack, url, method, user_id) VALUES (?,?,?,?,?,?)")
      .run(level, message, stack || '', url || '', method || '', userId || null);
  } catch (e) { console.error('에러 로그 기록 실패:', e.message); }
}

// 시스템 공지 작성 (릴리스 노트 등 서버 내부에서 자동 게시)
async function postSystemNotice(title, content, options = {}) {
  try {
    const { category = 'update', is_popup = 0, is_pinned = 0 } = options;
    const admin = await db.prepare("SELECT user_id, username FROM users WHERE role = 'admin' LIMIT 1").get();
    const authorId = admin ? admin.user_id : 0;
    const authorName = admin ? admin.username : 'system';
    const r = await db.prepare(`INSERT INTO notices (title, content, category, is_popup, popup_start, popup_end, is_pinned, author_id, author_name)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      title, content, category, is_popup ? 1 : 0, null, null, is_pinned ? 1 : 0, authorId, authorName
    );
    logger.info(`[시스템 공지] ${title} (id: ${r.lastInsertRowid})`);
    return r.lastInsertRowid;
  } catch (e) { logger.error('시스템 공지 작성 실패:', e.message); return null; }
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
  { id: 'purchase-closing', name: '구매마감', group: '구매' },
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
  // 통합발주
  { id: 'procurement', name: '통합발주관리', group: '구매' },
  // 재고 확장
  { id: 'inventory-ledger', name: '수불원장', group: '재고' },
  { id: 'safety-stock', name: '안전재고관리', group: '재고' },
  { id: 'cycle-count', name: '재고실사', group: '재고' },
  // 생산 확장
  { id: 'process-routing', name: '공정라우팅', group: '생산' },
  { id: 'equipment', name: '설비관리', group: '생산' },
  // 회계 확장
  { id: 'mfg-cost', name: '제조원가', group: '회계' },
  { id: 'vat-report', name: '부가세 신고', group: '회계' },
  { id: 'journal-auto', name: '자동 분개', group: '회계' },
  // 재고 확장
  { id: 'barcode', name: '바코드/QR', group: '재고' },
  // 경영분석 확장
  { id: 'report-builder', name: '보고서 빌더', group: '경영분석' },
  // 시스템
  { id: 'settings', name: '설정', group: '시스템' },
  { id: 'user-mgmt', name: '사용자 관리', group: '시스템' },
  { id: 'audit-log', name: '감사로그', group: '시스템' },
  { id: 'notification', name: '알림센터', group: '시스템' },
  { id: 'rbac', name: '권한관리', group: '시스템' },
];

// 역할 기본 권한 맵 (개별 permissions가 없을 때 fallback)
const ROLE_PERMISSIONS = {
  admin: ['*'],  // 모든 권한
  purchase: ['dashboard', 'inventory', 'warehouse', 'shipments', 'inventory-ledger', 'auto-order', 'create-po', 'po-list', 'os-register',
    'delivery-schedule', 'receipts', 'invoices', 'notes', 'product-mgmt', 'bom', 'mrp', 'post-process', 'defects',
    'closing', 'report', 'po-mgmt', 'china-shipment', 'mat-purchase', 'tasks', 'meeting-log', 'sales', 'sales-barun', 'sales-dd', 'sales-gift', 'cost-mgmt', 'board', 'audit-log', 'exec-dashboard', 'customer-orders', 'shipping',
    'chart-of-accounts', 'journal', 'general-ledger', 'trial-balance', 'financial-statements', 'ar-ap', 'tax-invoice', 'work-order', 'lot-tracking',
    'approval', 'sales-order', 'budget', 'notification', 'safety-stock', 'cycle-count', 'mfg-cost', 'procurement', 'vendor-performance'],
  production: ['dashboard', 'inventory', 'warehouse', 'shipments', 'inventory-ledger', 'production-req', 'mrp', 'bom', 'post-process', 'defects', 'product-mgmt', 'notes', 'production-stock', 'tasks', 'approval', 'lot-tracking',
    'process-routing', 'equipment', 'mfg-cost', 'safety-stock', 'work-order'],
  logistics: ['dashboard', 'inventory', 'warehouse', 'shipments', 'inventory-ledger', 'receipts', 'delivery-schedule', 'shipping', 'lot-tracking',
    'safety-stock', 'cycle-count', 'barcode', 'tasks', 'notes', 'board', 'notification', 'customer-orders'],
  sales_team: ['dashboard', 'sales', 'sales-barun', 'sales-dd', 'sales-gift', 'sales-order', 'customer-orders', 'shipping',
    'inventory', 'warehouse', 'shipments', 'ar-ap', 'invoices', 'tasks', 'notes', 'board', 'notification', 'exec-dashboard', 'analytics', 'report'],
  accounting: ['dashboard', 'invoices', 'mat-purchase', 'cost-mgmt', 'closing', 'chart-of-accounts', 'journal', 'general-ledger',
    'trial-balance', 'financial-statements', 'ar-ap', 'tax-invoice', 'budget', 'mfg-cost', 'vat-report', 'journal-auto',
    'sales', 'sales-barun', 'sales-dd', 'sales-gift', 'tasks', 'notes', 'board', 'notification', 'approval', 'exec-dashboard'],
  packaging: ['dashboard', 'inventory', 'warehouse', 'shipments', 'production-req', 'production-stock', 'work-order', 'bom',
    'post-process', 'lot-tracking', 'receipts', 'tasks', 'notes', 'board', 'notification'],
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

// ── 구매마감 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS purchase_closing (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  legal_entity TEXT NOT NULL DEFAULT 'barunson',
  vendor_name TEXT NOT NULL,
  closing_year INTEGER NOT NULL,
  closing_month INTEGER NOT NULL,
  po_count INTEGER DEFAULT 0,
  total_ordered_qty INTEGER DEFAULT 0,
  total_received_qty INTEGER DEFAULT 0,
  total_defect_qty INTEGER DEFAULT 0,
  total_amount REAL DEFAULT 0,
  adjustment_amount REAL DEFAULT 0,
  final_amount REAL DEFAULT 0,
  status TEXT DEFAULT 'open',
  confirmed_by TEXT DEFAULT '',
  confirmed_at TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT DEFAULT (datetime('now','localtime')),
  UNIQUE(legal_entity, vendor_name, closing_year, closing_month)
)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pc_ym ON purchase_closing(closing_year, closing_month)`);
await db.exec(`CREATE INDEX IF NOT EXISTS idx_pc_status ON purchase_closing(status)`);

// ── 메뉴 활성화/비활성화 설정 테이블 ──
await db.exec(`CREATE TABLE IF NOT EXISTS menu_settings (
  page_id    TEXT PRIMARY KEY,
  is_enabled INTEGER DEFAULT 1,
  sort_order INTEGER DEFAULT 0,
  updated_at TEXT DEFAULT (datetime('now','localtime'))
)`);
// 기본값: 모든 페이지 활성화 (dashboard, settings는 항상 활성)
try {
  const existingRows = await db.prepare('SELECT page_id FROM menu_settings').all();
  const existing = existingRows.map(r => r.page_id);
  const insertStmt = db.prepare('INSERT OR IGNORE INTO menu_settings (page_id, is_enabled, sort_order) VALUES (?, 1, ?)');
  for (let idx = 0; idx < ALL_PAGES.length; idx++) {
    const p = ALL_PAGES[idx];
    if (!existing.includes(p.id)) await insertStmt.run(p.id, idx);
  }
} catch (_) {}

// ── DB 자동 백업 (PostgreSQL은 pg_dump 등 외부 도구 사용) ──
// SQLite 백업 코드 제거됨 — PostgreSQL은 별도 백업 정책 적용

// 초기 필수발주 품목 4건
const aoInit = db.prepare('INSERT OR IGNORE INTO auto_order_items (product_code, min_stock, order_qty) VALUES (?, ?, ?)');
await aoInit.run('BE004', 0, 0);
await aoInit.run('BE005', 0, 0);
await aoInit.run('2010wh_n', 0, 0);
await aoInit.run('BE042', 0, 0);


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
  const body = JSON.stringify(payload);
  const _req = res._req;
  const acceptEncoding = (_req && _req.headers && _req.headers['accept-encoding']) || '';
  if (acceptEncoding.includes('gzip')) {
    const hdrs = { 'Content-Type': 'application/json; charset=utf-8', 'Content-Encoding': 'gzip', ...CORS };
    res.writeHead(statusCode, hdrs);
    zlib.gzip(Buffer.from(body), (err, compressed) => {
      if (err) { res.end(body); } else { res.end(compressed); }
    });
  } else {
    res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8', ...CORS });
    res.end(body);
  }
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
async function generatePoNumber() {
  const today = new Date();
  const ymd = today.getFullYear().toString()
    + String(today.getMonth() + 1).padStart(2, '0')
    + String(today.getDate()).padStart(2, '0');
  const prefix = `PO-${ymd}-`;
  const row = await db.prepare(`SELECT po_number FROM po_header WHERE po_number LIKE ? ORDER BY po_number DESC LIMIT 1`).get(prefix + '%');
  let seq = 1;
  if (row) {
    const last = parseInt(row.po_number.split('-')[2], 10);
    if (!isNaN(last)) seq = last + 1;
  }
  return prefix + String(seq).padStart(3, '0');
}

// ── Route Modules (모듈화) ──────────────────────────────────────────
const routeCtx = require('./routes/_ctx');
// ctx 초기화 — 모든 공유 상태를 route 모듈에 전달
Object.assign(routeCtx, {
  db, ok, fail, jsonRes, readBody, readJSON, parseMultipart,
  signToken, verifyToken, extractToken,
  auditLog, logError, logPOActivity: typeof logPOActivity !== 'undefined' ? logPOActivity : null,
  createNotification, generatePoNumber, hasPermission,
  ALL_PAGES, ROLE_PERMISSIONS, KNOWN_ACCOUNTS: typeof KNOWN_ACCOUNTS !== 'undefined' ? KNOWN_ACCOUNTS : {},
  DEPT_GUBUN_LABELS: typeof DEPT_GUBUN_LABELS !== 'undefined' ? DEPT_GUBUN_LABELS : {},
  BRAND_LABELS: typeof BRAND_LABELS !== 'undefined' ? BRAND_LABELS : {},
  bcrypt, jwt, sql, nodemailer, fs, path,
  _smtpTransporter: typeof smtpTransporter !== 'undefined' ? smtpTransporter : null,
  SMTP_FROM: typeof SMTP_FROM !== 'undefined' ? SMTP_FROM : '',
  xerpInventoryCache: typeof xerpInventoryCache !== 'undefined' ? xerpInventoryCache : null,
  getXerpPool: () => typeof xerpPool !== 'undefined' ? xerpPool : null,
  getDdPool: () => typeof ddPool !== 'undefined' ? ddPool : null,
  ensureXerpPool: typeof ensureXerpPool !== 'undefined' ? ensureXerpPool : null,
});

// ── legal_entity 2차 ALTER (뒤늦게 CREATE 된 trade_document/defects/batch_master/work_orders 대응) ──
// 1차 ALTER 루프(line ~1560)는 이 테이블들이 아직 CREATE 전이라 silent-fail 했음.
// 이제 모든 CREATE TABLE 이 끝났으니 누락된 것들만 재시도.
try {
  const _entityTablesLate = ['trade_document', 'defects', 'batch_master', 'work_orders'];
  let _lateOk = 0;
  for (const tbl of _entityTablesLate) {
    try {
      const chk = await db.prepare("SELECT 1 AS x FROM information_schema.columns WHERE table_name=? AND column_name='legal_entity'").get(tbl);
      if (!chk) {
        await db.exec(`ALTER TABLE ${tbl} ADD COLUMN legal_entity TEXT DEFAULT 'barunson'`);
        await db.prepare(`UPDATE ${tbl} SET legal_entity='barunson' WHERE legal_entity IS NULL OR legal_entity=''`).run().catch(()=>{});
        _lateOk++;
      }
    } catch(e) { /* 테이블 자체가 없으면 skip */ }
  }
  if (_lateOk) console.log(`[entity 2nd pass] ${_lateOk}개 테이블에 legal_entity 추가 완료`);
} catch(_) {}

// ── products 누락 컬럼 보강 (product_spec 등 — reloadProductInfoFromDB 의 SELECT 에서 참조) ──
// products 테이블은 외부 마이그레이션/수동 DDL 로 생성된 케이스가 많아 스키마 편차 발생.
// 코드가 참조하는 컬럼이 없으면 런타임 쿼리 전체 실패 → reloadProductInfoFromDB 재로드 실패 연쇄.
try {
  const productsCols = [
    ['product_spec', "TEXT DEFAULT ''"],
    ['thomson',  "TEXT DEFAULT ''"],
    ['envelope', "TEXT DEFAULT ''"],
    ['seari',    "TEXT DEFAULT ''"],
    ['laser',    "TEXT DEFAULT ''"],
    ['cutting',  "TEXT DEFAULT ''"],
    ['silk',     "TEXT DEFAULT ''"]
  ];
  for (const [col, type] of productsCols) {
    try { await db.exec(`ALTER TABLE products ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(_) {}
  }
} catch(_) {}

// 라우트 모듈 로드 (새 기능은 모듈로 추가)
const moduleRouters = [];
try {
  const journalAuto = require('./routes/journal-auto');
  if (journalAuto.initTables) journalAuto.initTables();
  if (journalAuto.router) moduleRouters.push(journalAuto.router);
  // 입고 시 자동 전표 훅을 전역에 등록
  global._hookReceiveJournal = journalAuto.hookReceiveJournal || null;
  global._hookShipmentJournal = journalAuto.hookShipmentJournal || null;
  console.log('✅ 모듈 로드: journal-auto (' + (journalAuto.router?.count || 0) + ' routes)');
} catch(e) { console.log('⚠️ journal-auto 모듈 미로드:', e.message); }

try {
  const vatReport = require('./routes/vat-report');
  if (vatReport.initTables) vatReport.initTables();
  if (vatReport.router) moduleRouters.push(vatReport.router);
  console.log('✅ 모듈 로드: vat-report (' + (vatReport.router?.count || 0) + ' routes)');
} catch(e) { console.log('⚠️ vat-report 모듈 미로드:', e.message); }

try {
  const barcode = require('./routes/barcode');
  if (barcode.initTables) await barcode.initTables();
  if (barcode.router) moduleRouters.push(barcode.router);
  console.log('✅ 모듈 로드: barcode (' + (barcode.router?.count || 0) + ' routes)');
} catch(e) { console.log('⚠️ barcode 모듈 미로드:', e.message); }

try {
  const reportEngine = require('./routes/report-engine');
  if (reportEngine.initTables) await reportEngine.initTables();
  if (reportEngine.router) moduleRouters.push(reportEngine.router);
  console.log('✅ 모듈 로드: report-engine (' + (reportEngine.router?.count || 0) + ' routes)');
} catch(e) { console.log('⚠️ report-engine 모듈 미로드:', e.message); }

console.log(`📦 총 ${moduleRouters.reduce((s,r) => s + r.count, 0)}개 모듈 라우트 등록`);

// ── Server ──────────────────────────────────────────────────────────
http.createServer(async (req, res) => {
  const _reqStart = Date.now();
  const _clientIp = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  // 헬스체크는 최우선 처리 (Docker/Starlog 배포 안정성)
  const u = req.url;
  if ((u === '/health' || u === '/api/health') && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json', ...CORS });
    res.end(JSON.stringify({ ok: true, data: { status: 'ok', timestamp: new Date().toISOString() } }));
    return;
  }
  // 응답 완료 시 액세스 로그
  res.on('finish', () => {
    const ms = Date.now() - _reqStart;
    if (u !== '/health' && u !== '/api/health') {
      logger.access(req.method, u, res.statusCode, ms, _clientIp);
    }
  });
  try {
    await handleRequest(req, res);
  } catch (e) {
    logger.error('Server error:', e.message, e.stack);
    logError('error', e.message, e.stack, req.url, req.method);
    fail(res, 500, e.message || 'Internal Server Error');
  }
}).listen(PORT, '0.0.0.0', () => {
  logger.info(`스마트재고현황: http://localhost:${PORT}  (startup: ${((Date.now() - _startTime)/1000).toFixed(1)}s)`);
  logger.info(`헬스체크: http://localhost:${PORT}/api/health`);
  logger.info(`로그 디렉토리: ${LOG_DIR}`);
  // product_info 캐시를 DB에서 즉시 로드 (첫 요청 전에 완료)
  reloadProductInfoFromDB().catch(e => console.warn('[startup] product_info 초기 로드 실패:', e.message));
  // 외부 DB 연결은 서버 기동 후 백그라운드에서 (Docker 헬스체크 타임아웃 방지)
  initXERP();
  initDD();
  scheduleAutoOrder();
  scheduleXerpSync();
});

// 미처리 예외/프라미스 거부 핸들러
process.on('uncaughtException', (e) => {
  console.error('Uncaught Exception:', e);
  try { logError('fatal', e.message, e.stack); } catch(_) {}
});
process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  const stack = reason instanceof Error ? reason.stack : '';
  console.error('Unhandled Rejection:', msg);
  try { logError('error', msg, stack); } catch(_) {}
});

async function handleRequest(req, res) {
  res._req = req; // gzip 판단용 req 참조 보관
  const parsed = new URL(req.url, `http://localhost:${PORT}`);
  const pathname = parsed.pathname;
  const method = req.method;

  // ── CORS preflight ──
  if (method === 'OPTIONS') {
    res.writeHead(204, CORS);
    res.end();
    return;
  }

  // ── 모듈 라우터 우선 처리 ──
  for (const mr of moduleRouters) {
    if (await mr.handle(req, res, pathname, method, parsed)) return;
  }

  // GET /api/version — 앱 버전 정보 (공개)
  if (pathname === '/api/version' && method === 'GET') {
    ok(res, { version: APP_VERSION, version_date: APP_VERSION_DATE, started_at: new Date(_startTime).toISOString() });
    return;
  }

  // GET /api/manual — 사용 설명서 (MANUAL.md 원문)
  if (pathname === '/api/manual' && method === 'GET') {
    try {
      const content = fs.readFileSync(path.join(__dirname, 'MANUAL.md'), 'utf8');
      ok(res, { version: APP_VERSION, content });
    } catch (e) {
      fail(res, 500, 'MANUAL.md 로드 실패: ' + e.message);
    }
    return;
  }
  // GET /api/vendor-guide — 거래처 사용안내서
  if (pathname === '/api/vendor-guide' && method === 'GET') {
    try {
      const content = fs.readFileSync(path.join(__dirname, 'VENDOR_GUIDE.md'), 'utf8');
      ok(res, { content });
    } catch (e) {
      fail(res, 500, 'VENDOR_GUIDE.md 로드 실패: ' + e.message);
    }
    return;
  }

  // GET /api/changelog — 변경 이력 (CHANGELOG.md 원문)
  if (pathname === '/api/changelog' && method === 'GET') {
    try {
      const content = fs.readFileSync(path.join(__dirname, 'CHANGELOG.md'), 'utf8');
      ok(res, { version: APP_VERSION, content });
    } catch (e) {
      fail(res, 500, 'CHANGELOG.md 로드 실패: ' + e.message);
    }
    return;
  }

  // GET /api/health — 시스템 헬스체크 (최상위 배치 — Docker 배포 안정성)
  if ((pathname === '/api/health' || pathname === '/health') && method === 'GET') {
    const health = { status: 'ok', timestamp: new Date().toISOString(), checks: {} };
    try { await db.prepare('SELECT 1').get(); health.checks.postgresql = 'ok'; }
    catch (e) { health.checks.postgresql = 'error: ' + e.message; health.status = 'degraded'; }
    try {
      if (xerpPool && xerpPool.connected) { health.checks.xerp = 'ok'; }
      else if (xerpReconnectAttempts > 0) { health.checks.xerp = `reconnecting (attempt #${xerpReconnectAttempts})`; }
      else health.checks.xerp = 'not configured';
    } catch (e) { health.checks.xerp = 'error'; health.status = 'degraded'; }
    health.checks.smtp = smtpTransporter ? 'configured' : 'not configured';
    health.checks.google_sheet = gAccessToken ? 'ok' : (gRefreshToken ? 'token expired' : 'not configured');
    health.checks.backup = 'PostgreSQL (외부 백업 정책 적용)';
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
    const user = await db.prepare("SELECT user_id, username, display_name, role, email, permissions, favorites FROM users WHERE role = 'admin' AND is_active = 1 LIMIT 1").get();
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
    const user = await db.prepare("SELECT * FROM users WHERE (username = ? OR email = ?) AND is_active = 1").get(username, username);
    if (!user || !bcrypt.compareSync(password, user.password_hash)) {
      auditLog(null, username, 'login_failed', 'auth', '', '로그인 실패', clientIP);
      fail(res, 401, '아이디 또는 비밀번호가 일치하지 않습니다');
      return;
    }
    const token = signToken(user);
    await db.prepare("UPDATE users SET last_login = datetime('now','localtime') WHERE user_id = ?").run(user.user_id);
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
      let user = await db.prepare("SELECT * FROM users WHERE google_id = ? OR email = ?").get(googleId, email);
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
        while (await db.prepare("SELECT user_id FROM users WHERE username = ?").get(finalUsername)) {
          finalUsername = username + suffix++;
        }
        const result = await db.prepare("INSERT INTO users (username, password_hash, display_name, role, email, google_id, profile_picture) VALUES (?,?,?,?,?,?,?)")
          .run(finalUsername, '', name || email.split('@')[0], 'viewer', email, googleId, picture || '');
        user = await db.prepare("SELECT * FROM users WHERE user_id = ?").get(result.lastInsertRowid);
        auditLog(user.user_id, finalUsername, 'google_register', 'auth', user.user_id, `Google 자동 등록: ${email}`, clientIP);
        console.log(`✅ Google 신규 사용자 등록: ${email} (${finalUsername})`);
      } else {
        // 기존 사용자 → google_id, profile_picture 업데이트
        await db.prepare("UPDATE users SET google_id = ?, profile_picture = ?, display_name = CASE WHEN display_name = '' OR display_name = username THEN ? ELSE display_name END WHERE user_id = ?")
          .run(googleId, picture || '', name || user.display_name, user.user_id);
      }
      // JWT 발급
      const token = signToken(user);
      await db.prepare("UPDATE users SET last_login = datetime('now','localtime') WHERE user_id = ?").run(user.user_id);
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
    const exists = await db.prepare("SELECT user_id FROM users WHERE email = ?").get(email);
    if (exists) { fail(res, 409, '이미 등록된 이메일입니다. 로그인해주세요.'); return; }
    const username = email.split('@')[0];
    let finalUsername = username;
    let suffix = 1;
    while (await db.prepare("SELECT user_id FROM users WHERE username = ?").get(finalUsername)) {
      finalUsername = username + suffix++;
    }
    const hash = bcrypt.hashSync(password, 10);
    const result = await db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?,?,?,?,?)")
      .run(finalUsername, hash, display_name || username, 'viewer', email);
    const user = await db.prepare("SELECT * FROM users WHERE user_id = ?").get(result.lastInsertRowid);
    const token = signToken(user);
    await db.prepare("UPDATE users SET last_login = datetime('now','localtime') WHERE user_id = ?").run(user.user_id);
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
    const user = await db.prepare("SELECT user_id, username, display_name, role, email, permissions, favorites, last_login FROM users WHERE user_id = ?").get(decoded.userId);
    if (!user) { fail(res, 401, '사용자를 찾을 수 없습니다'); return; }
    const userPerms = user.permissions ? JSON.parse(user.permissions) : [];
    const effectivePerms = user.role === 'admin' ? ['*'] : (userPerms.length > 0 ? userPerms : (ROLE_PERMISSIONS[user.role] || []));
    let favs = []; try { favs = JSON.parse(user.favorites || '[]'); } catch {}
    // 메뉴 활성화 상태 포함
    let menuEnabled = {};
    try {
      const ms = await db.prepare('SELECT page_id, is_enabled FROM menu_settings').all();
      ms.forEach(r => { menuEnabled[r.page_id] = r.is_enabled; });
    } catch (_) {}
    ok(res, { user: { ...user, permissions: undefined, favorites: undefined }, permissions: effectivePerms, favorites: favs, menuEnabled });
    return;
  }

  // GET /api/auth/pages — 전체 페이지 목록 (관리자 권한 UI용)
  if (pathname === '/api/auth/pages' && method === 'GET') {
    ok(res, ALL_PAGES);
    return;
  }

  // ── 메뉴 활성화/비활성화 API ──
  if (pathname === '/api/menu-settings' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const rows = await db.prepare('SELECT page_id, is_enabled, sort_order FROM menu_settings ORDER BY sort_order').all();
    const map = {};
    rows.forEach(r => { map[r.page_id] = { is_enabled: r.is_enabled, sort_order: r.sort_order }; });
    ok(res, map);
    return;
  }
  if (pathname === '/api/menu-settings' && method === 'POST') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const user = await db.prepare("SELECT role FROM users WHERE user_id = ?").get(decoded.userId);
    if (!user || user.role !== 'admin') { fail(res, 403, '관리자만 메뉴 설정을 변경할 수 있습니다'); return; }
    const body = await readJSON(req);
    // body: { settings: { page_id: { is_enabled: 0|1, sort_order?: N }, ... } }
    const upsert = db.prepare(`INSERT INTO menu_settings (page_id, is_enabled, sort_order, updated_at) VALUES (?, ?, ?, datetime('now','localtime'))
      ON CONFLICT(page_id) DO UPDATE SET is_enabled=excluded.is_enabled, sort_order=excluded.sort_order, updated_at=excluded.updated_at`);
    const PROTECTED = ['dashboard', 'settings']; // 항상 활성화
    const tx = db.transaction(async () => {
      for (const [pageId, cfg] of Object.entries(body.settings || {})) {
        const enabled = PROTECTED.includes(pageId) ? 1 : (cfg.is_enabled ? 1 : 0);
        await upsert.run(pageId, enabled, cfg.sort_order || 0);
      }
    });
    await tx();
    ok(res, { message: '메뉴 설정이 저장되었습니다' });
    return;
  }

  // POST /api/auth/change-password
  if (pathname === '/api/auth/change-password' && method === 'POST') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const body = await readJSON(req);
    const user = await db.prepare("SELECT * FROM users WHERE user_id = ?").get(decoded.userId);
    if (!user) { fail(res, 404, '사용자 없음'); return; }
    if (!bcrypt.compareSync(body.current_password, user.password_hash)) { fail(res, 400, '현재 비밀번호가 일치하지 않습니다'); return; }
    if (!body.new_password || body.new_password.length < 4) { fail(res, 400, '새 비밀번호는 4자 이상이어야 합니다'); return; }
    const hash = bcrypt.hashSync(body.new_password, 10);
    await db.prepare("UPDATE users SET password_hash = ?, updated_at = datetime('now','localtime') WHERE user_id = ?").run(hash, decoded.userId);
    auditLog(decoded.userId, decoded.username, 'password_change', 'auth', decoded.userId, '비밀번호 변경', clientIP);
    ok(res, { message: '비밀번호가 변경되었습니다' });
    return;
  }

  // GET /api/auth/favorites — 즐겨찾기 조회
  if (pathname === '/api/auth/favorites' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증이 필요합니다'); return; }
    const row = await db.prepare("SELECT favorites FROM users WHERE user_id = ?").get(decoded.userId);
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
    await db.prepare("UPDATE users SET favorites = ? WHERE user_id = ?").run(JSON.stringify(favs), decoded.userId);
    ok(res, { message: '즐겨찾기 저장 완료', favorites: favs });
    return;
  }

  // GET /api/auth/user-list — 담당자 드롭다운용 (로그인 사용자)
  if (pathname === '/api/auth/user-list' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '로그인 필요'); return; }
    const users = await db.prepare("SELECT user_id, username, display_name, role FROM users WHERE is_active=1 ORDER BY display_name").all();
    ok(res, users);
    return;
  }

  // GET /api/auth/users — 사용자 목록 (admin만)
  if (pathname === '/api/auth/users' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const users = await db.prepare("SELECT user_id, username, display_name, role, email, permissions, is_active, last_login, created_at FROM users ORDER BY user_id").all();
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
    const exists = await db.prepare("SELECT user_id FROM users WHERE username = ?").get(body.username);
    if (exists) { fail(res, 409, '이미 존재하는 아이디입니다'); return; }
    const hash = bcrypt.hashSync(body.password, 10);
    const result = await db.prepare("INSERT INTO users (username, password_hash, display_name, role, email) VALUES (?,?,?,?,?)")
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
    await db.prepare(`UPDATE users SET ${sets.join(',')} WHERE user_id=?`).run(...params);
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
    const total = (await db.prepare("SELECT COUNT(*) as cnt FROM audit_log" + whereClause).get(...params)).cnt;
    const rows = await db.prepare("SELECT * FROM audit_log" + whereClause + " ORDER BY created_at DESC LIMIT ? OFFSET ?").all(...params, limit, offset);

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

    const totalLogs = (await db.prepare("SELECT COUNT(*) as cnt FROM audit_log WHERE created_at::text >= ?").get(since)).cnt;
    const byAction = await db.prepare("SELECT action, COUNT(*) as cnt FROM audit_log WHERE created_at::text >= ? GROUP BY action ORDER BY cnt DESC").all(since);
    const byUser = await db.prepare("SELECT username, COUNT(*) as cnt FROM audit_log WHERE created_at::text >= ? GROUP BY username ORDER BY cnt DESC LIMIT 20").all(since);
    const byResource = await db.prepare("SELECT resource, COUNT(*) as cnt FROM audit_log WHERE created_at::text >= ? GROUP BY resource ORDER BY cnt DESC").all(since);
    const byDay = await db.prepare("SELECT created_at::date as day, COUNT(*) as cnt FROM audit_log WHERE created_at::text >= ? GROUP BY created_at::date ORDER BY day DESC LIMIT ?").all(since, days);
    const loginFailed = (await db.prepare("SELECT COUNT(*) as cnt FROM audit_log WHERE action='login_failed' AND created_at::text >= ?").get(since)).cnt;
    const uniqueUsers = (await db.prepare("SELECT COUNT(DISTINCT username) as cnt FROM audit_log WHERE created_at::text >= ? AND action IN ('login','google_login')").get(since)).cnt;
    const recentActions = await db.prepare("SELECT action, COUNT(*) as cnt FROM audit_log WHERE created_at::text >= (NOW() - INTERVAL '1 hour')::text GROUP BY action ORDER BY cnt DESC").all();

    ok(res, { total: totalLogs, login_failed: loginFailed, unique_users: uniqueUsers, by_action: byAction, by_user: byUser, by_resource: byResource, by_day: byDay, recent_hour: recentActions, days });
    return;
  }

  // GET /api/audit-log/actions — 감사 로그 액션 유형 목록
  if (pathname === '/api/audit-log/actions' && method === 'GET') {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한이 필요합니다'); return; }
    const actions = await db.prepare("SELECT DISTINCT action FROM audit_log ORDER BY action").all();
    const resources = await db.prepare("SELECT DISTINCT resource FROM audit_log ORDER BY resource").all();
    const users = await db.prepare("SELECT DISTINCT username FROM audit_log WHERE username IS NOT NULL ORDER BY username").all();
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
    const total = (await db.prepare("SELECT COUNT(*) as cnt FROM error_logs" + whereClause).get(...params)).cnt;
    const rows = await db.prepare("SELECT * FROM error_logs" + whereClause + " ORDER BY created_at DESC LIMIT ? OFFSET ?").all(...params, limit, offset);
    ok(res, { rows, total, limit, offset });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  인증 미들웨어 — 여기서부터 모든 API는 토큰 필요
  //  2026-04: 거래처 포털 외부 접근 전면 차단 (해킹 리스크 대응)
  //          /api/vendor-portal/* 는 이제 관리자 JWT 있는 경우에만 동작 (미리보기 용도)
  // ════════════════════════════════════════════════════════════════════
  let currentUser = null;
  if (pathname.startsWith('/api/')) {
    const token = extractToken(req);
    const decoded = token ? verifyToken(token) : null;
    if (decoded) currentUser = decoded;

    if (pathname.startsWith('/api/vendor-portal')) {
      // 관리자 JWT 없으면 외부 접근으로 간주 → 차단
      if (!currentUser) {
        fail(res, 403, '거래처 포털은 더 이상 외부 접근을 지원하지 않습니다. 발주서는 이메일(PDF)로 발송됩니다.');
        return;
      }
    } else {
      // 인증 강제 모드: AUTH_REQUIRED=true 일 때만 차단 (기본: 선택적)
      const authRequired = (envVars.AUTH_REQUIRED || process.env.AUTH_REQUIRED || 'false') === 'true';
      if (authRequired && !decoded) {
        fail(res, 401, '인증이 필요합니다. 로그인하세요.');
        return;
      }
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

  // 거래처 입력값 검증: 빈 이름/스크립트/SQL 인젝션/명백한 테스트 패턴 차단
  // (실제 거래처는 모두 통과시키고, 테스트 찌꺼기 유입만 막는 보호막)
  function validateVendorInput(body) {
    const name = (body && body.name || '').trim();
    if (!name) return '거래처명은 필수입니다 (빈 이름 불가)';
    if (name.length < 2) return '거래처명은 2자 이상이어야 합니다';
    if (name.length > 100) return '거래처명은 100자를 초과할 수 없습니다';
    // XSS 방지
    if (/<\s*script/i.test(name) || /<\s*\/\s*script/i.test(name)) return '거래처명에 스크립트 태그를 사용할 수 없습니다';
    if (/on\w+\s*=/i.test(name)) return '거래처명에 이벤트 핸들러를 포함할 수 없습니다';
    // SQL 인젝션 명시 패턴 (prepared statement라 실제로 뚫리진 않지만 쓰레기 저장 방지)
    if (/drop\s+table/i.test(name) || /;\s*delete\s+from/i.test(name) || /union\s+select/i.test(name)) {
      return '거래처명에 SQL 예약어 패턴을 사용할 수 없습니다';
    }
    // 명백한 테스트 패턴 차단
    const testPatterns = [
      /^test$/i,
      /^testvendor/i,
      /^FT[-_]/i,
      /^FT-?(Customer|Supplier|Cust|Sup)/i,
      /^NoAuth/i,
      /^테스트업체[-_]/,
      /^dummy/i,
      /^sample[-_]/i,
      /^placeholder/i
    ];
    if (testPatterns.some(re => re.test(name))) {
      return '테스트용 이름은 사용할 수 없습니다';
    }
    // 제어문자 (깨진 인코딩 / null byte 등)
    if (/[\x00-\x1F\x7F]/.test(name)) return '거래처명에 제어문자를 사용할 수 없습니다';
    // 대체 문자(�) 포함 시 거부 (인코딩 손상)
    if (name.includes('\uFFFD')) return '거래처명 인코딩이 손상되었습니다';
    return null;
  }

  if (pathname === '/api/vendors' && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM vendors ORDER BY name').all();
    ok(res, rows);
    return;
  }

  if (pathname === '/api/vendors' && method === 'POST') {
    const body = await readJSON(req);
    // ── Validation: 빈 이름/스크립트/SQL 인젝션/테스트 패턴 차단 ──
    const vErr = validateVendorInput(body);
    if (vErr) { fail(res, 400, vErr); return; }
    const info = await db.prepare(`INSERT INTO vendors (vendor_code, name, type, contact, phone, email, email_cc, kakao, memo) VALUES (?,?,?,?,?,?,?,?,?)`).run(
      body.vendor_code || '', body.name || '', body.type || '', body.contact || '',
      body.phone || '', body.email || '', body.email_cc || '', body.kakao || '', body.memo || ''
    );
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'vendor_create', 'vendors', info.lastInsertRowid, `거래처 등록: ${body.name}`, clientIP);
    ok(res, { vendor_id: info.lastInsertRowid });
    return;
  }

  if (pathname === '/api/vendors/migrate' && method === 'POST') {
    const vendors = await readJSON(req);
    if (!Array.isArray(vendors)) { fail(res, 400, 'Expected array'); return; }
    const tx = db.transaction(async (list) => {
      let count = 0;
      for (const v of list) {
        const info = await db.prepare(`INSERT INTO vendors (name, type, contact, phone, email, kakao, memo) VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`).run(
          v.name || '', v.type || '', v.contact || '', v.phone || '', v.email || '', v.kakao || '', v.memo || ''
        );
        if (info.changes > 0) count++;
      }
      return count;
    });
    const count = await tx(vendors);
    ok(res, { migrated: count, total: vendors.length });
    return;
  }

  // PUT /api/vendors/:id
  const vendorPut = pathname.match(/^\/api\/vendors\/(\d+)$/);
  if (vendorPut && method === 'PUT') {
    const id = parseInt(vendorPut[1]);
    const body = await readJSON(req);
    // name 변경 시 validation 적용
    if (body.name !== undefined) {
      const vErr = validateVendorInput(body);
      if (vErr) { fail(res, 400, vErr); return; }
    }
    const fields = [];
    const values = [];
    for (const col of ['vendor_code', 'name', 'type', 'contact', 'phone', 'email', 'email_cc', 'kakao', 'memo']) {
      if (body[col] !== undefined) {
        fields.push(`${col} = ?`);
        values.push(body[col]);
      }
    }
    if (fields.length === 0) { fail(res, 400, 'No fields to update'); return; }
    fields.push(`updated_at = datetime('now','localtime')`);
    values.push(id);
    await db.prepare(`UPDATE vendors SET ${fields.join(', ')} WHERE vendor_id = ?`).run(...values);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'vendor_update', 'vendors', id, `거래처 수정: ${body.name || id}`, clientIP);
    ok(res, { vendor_id: id });
    return;
  }

  // DELETE /api/vendors/:id
  const vendorDel = pathname.match(/^\/api\/vendors\/(\d+)$/);
  if (vendorDel && method === 'DELETE') {
    const id = parseInt(vendorDel[1]);
    await db.prepare('DELETE FROM vendors WHERE vendor_id = ?').run(id);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'vendor_delete', 'vendors', id, `거래처 삭제`, clientIP);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  REFERENCE DOCS (자료함)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/reference-docs' && method === 'GET') {
    const cat = parsed.searchParams.get('category') || '';
    const q = parsed.searchParams.get('q') || '';
    let sql = 'SELECT * FROM reference_docs WHERE 1=1';
    const params = [];
    if (cat) { sql += ' AND category = ?'; params.push(cat); }
    if (q) { sql += ' AND (title LIKE ? OR memo LIKE ?)'; params.push('%'+q+'%', '%'+q+'%'); }
    sql += ' ORDER BY created_at DESC';
    const rows = await db.prepare(sql).all(...params);
    ok(res, rows);
    return;
  }

  if (pathname === '/api/reference-docs/categories' && method === 'GET') {
    const rows = await db.prepare("SELECT DISTINCT category FROM reference_docs WHERE category != '' ORDER BY category").all();
    ok(res, rows.map(r => r.category));
    return;
  }

  if (pathname === '/api/reference-docs' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.title) { fail(res, 400, 'title 필수'); return; }
    if (!body.url) { fail(res, 400, 'url 필수'); return; }
    const uploader = currentUser ? (currentUser.username || '') : '';
    const uploaderId = currentUser ? (currentUser.userId || 0) : 0;
    const info = await db.prepare(
      `INSERT INTO reference_docs (type, title, url, category, memo, uploader, uploader_id) VALUES ('link', ?, ?, ?, ?, ?, ?)`
    ).run(body.title, body.url, body.category || '', body.memo || '', uploader, uploaderId);
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  if (pathname === '/api/reference-docs/upload' && method === 'POST') {
    try {
      const contentType = req.headers['content-type'] || '';
      if (!contentType.includes('multipart/form-data')) { fail(res, 400, 'multipart/form-data 필요'); return; }
      const boundary = contentType.split('boundary=')[1];
      if (!boundary) { fail(res, 400, 'boundary 없음'); return; }
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const buf = Buffer.concat(chunks);
      const sep = Buffer.from('--' + boundary);
      const fields = {};
      let fileData = null, fileName = '';
      const parts = [];
      let start = 0;
      while (true) {
        const idx = buf.indexOf(sep, start);
        if (idx === -1) break;
        if (start > 0) parts.push(buf.slice(start, idx - 2));
        start = idx + sep.length + 2;
      }
      for (const part of parts) {
        const headerEnd = part.indexOf('\r\n\r\n');
        if (headerEnd === -1) continue;
        const header = part.slice(0, headerEnd).toString('utf8');
        const pbody = part.slice(headerEnd + 4);
        const nameMatch = header.match(/name="([^"]+)"/);
        const fileMatch = header.match(/filename="([^"]+)"/);
        if (nameMatch) {
          if (fileMatch) { fileData = pbody; fileName = fileMatch[1]; }
          else { fields[nameMatch[1]] = pbody.toString('utf8').trim(); }
        }
      }
      if (!fileData || !fileName) { fail(res, 400, '파일 없음'); return; }
      const title = fields.title || fileName;
      const category = fields.category || '';
      const memo = fields.memo || '';
      const uploadDir = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
      const refDir = path.join(uploadDir, 'reference');
      const fsMod = require('fs');
      if (!fsMod.existsSync(refDir)) fsMod.mkdirSync(refDir, { recursive: true });
      const safeName = `${Date.now()}_${fileName.replace(/[^a-zA-Z0-9가-힣._-]/g, '_')}`;
      const filePath = path.join(refDir, safeName);
      fsMod.writeFileSync(filePath, fileData);
      const uploader = currentUser ? (currentUser.username || '') : '';
      const uploaderId = currentUser ? (currentUser.userId || 0) : 0;
      const info = await db.prepare(
        `INSERT INTO reference_docs (type, title, file_path, file_name, file_size, category, memo, uploader, uploader_id) VALUES ('file', ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(title, filePath, fileName, fileData.length, category, memo, uploader, uploaderId);
      ok(res, { id: info.lastInsertRowid, file_name: fileName });
    } catch(e) {
      console.error('자료함 업로드 오류:', e.message);
      fail(res, 500, '업로드 실패: ' + e.message);
    }
    return;
  }

  const refFile = pathname.match(/^\/api\/reference-docs\/(\d+)\/file$/);
  if (refFile && method === 'GET') {
    const id = parseInt(refFile[1]);
    const row = await db.prepare('SELECT file_path, file_name FROM reference_docs WHERE id = ?').get(id);
    if (!row || !row.file_path) { fail(res, 404, '파일 없음'); return; }
    const fsMod = require('fs');
    if (!fsMod.existsSync(row.file_path)) { fail(res, 404, '파일이 디스크에 없음'); return; }
    const ext = path.extname(row.file_name || '').toLowerCase();
    const ct = (typeof MIME !== 'undefined' && MIME[ext]) || 'application/octet-stream';
    res.writeHead(200, {
      'Content-Type': ct,
      'Content-Disposition': `inline; filename="${encodeURIComponent(row.file_name || 'file')}"`,
      ...CORS,
    });
    fsMod.createReadStream(row.file_path).pipe(res);
    return;
  }

  const refUpd = pathname.match(/^\/api\/reference-docs\/(\d+)$/);
  if (refUpd && method === 'PATCH') {
    const id = parseInt(refUpd[1]);
    const body = await readJSON(req);
    const fields = [];
    const params = [];
    for (const k of ['title','url','category','memo']) {
      if (body[k] !== undefined) { fields.push(`${k}=?`); params.push(body[k]); }
    }
    if (!fields.length) { fail(res, 400, '수정할 필드 없음'); return; }
    fields.push("updated_at=datetime('now','localtime')");
    params.push(id);
    await db.prepare(`UPDATE reference_docs SET ${fields.join(',')} WHERE id = ?`).run(...params);
    ok(res, { updated: id });
    return;
  }

  if (refUpd && method === 'DELETE') {
    const id = parseInt(refUpd[1]);
    const row = await db.prepare('SELECT file_path FROM reference_docs WHERE id = ?').get(id);
    if (row && row.file_path) {
      try { require('fs').unlinkSync(row.file_path); } catch(_) {}
    }
    await db.prepare('DELETE FROM reference_docs WHERE id = ?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCTS (품목관리)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/products' && method === 'GET') {
    const entity = parsed.searchParams.get('entity') || '';
    let sql = 'SELECT * FROM products';
    const params = [];
    if (entity && entity !== 'all' && _hasEntity.products) {
      sql += ' WHERE legal_entity=?';
      params.push(entity);
    }
    sql += ' ORDER BY origin, product_code';
    let rows = await db.prepare(sql).all(...params);
    // legal_entity 컬럼이 없는 환경(운영 PG 권한 부족)에서는
    // 품목코드 prefix로 가상 매핑 (DD*/DDC*/DDE*/DDT*/DD_seal_* → dd, 나머지 → barunson)
    const _deriveEntity = (code) => {
      const c = String(code||'').toUpperCase();
      if (c.startsWith('DD')) return 'dd';
      return 'barunson';
    };
    rows = rows.map(r => {
      if (!r.legal_entity) r.legal_entity = _deriveEntity(r.product_code);
      return r;
    });
    // product_code 중복 제거 (첫 등장 유지) — DB UNIQUE 제약 누락 대비
    {
      const _seen = new Set();
      rows = rows.filter(r => {
        const c = (r.product_code || '').trim();
        if (!c || _seen.has(c)) return false;
        _seen.add(c);
        return true;
      });
    }
    // 컬럼이 없어서 DB 필터를 못 건 경우, 메모리에서 한번 더 필터
    if (entity && entity !== 'all' && !_hasEntity.products) {
      rows = rows.filter(r => r.legal_entity === entity);
    }
    ok(res, rows);
    return;
  }

  if (pathname === '/api/products' && method === 'POST') {
    const b = await readJSON(req);
    if (!b.product_code) { fail(res, 400, 'product_code required'); return; }
    const entity = (b.legal_entity === 'dd') ? 'dd' : 'barunson';
    // 생산지 정규화 (DD의 "예지가/코리아/마커엘엔피" 변형값 → "한국")
    const _normOrigin = (v) => {
      const s = String(v||'').trim();
      if (!s) return '한국';
      if (s === '한국' || s === '중국' || s === '더기프트') return s;
      if (s === '코리아' || s === '예지가' || s === '마커엘엔피' ||
          s.indexOf('코리아') === 0 || s.indexOf('예지가') === 0 || s.indexOf('마커') === 0) return '한국';
      return s;
    };
    b.origin = _normOrigin(b.origin);
    // temp_code 컬럼 존재 여부 런타임 체크
    let _hasTempCode = false;
    try { await db.prepare('SELECT temp_code FROM products LIMIT 1').get(); _hasTempCode = true; } catch(_){}
    try {
      let info;
      // op_category, is_new_product 는 PUT 핸들러에만 있었음. 신규등록 시에도 저장되도록 baseCols 포함.
      const baseCols = 'product_code, product_name, brand, origin, category, status, material_code, material_name, unit, cut_spec, jopan, paper_maker, memo, op_category, is_new_product, spec';
      const baseVals = [b.product_code, b.product_name||'', b.brand||'', b.origin||'한국', b.category||'', b.status||'active',
        b.material_code||'', b.material_name||'', b.unit||'EA', b.cut_spec||'', b.jopan||'', b.paper_maker||'', b.memo||'', b.op_category||'', b.is_new_product ? 1 : 0, b.spec||''];
      let cols = baseCols, vals = [...baseVals];
      if (_hasEntity.products) { cols += ', legal_entity'; vals.push(entity); }
      if (_hasTempCode) { cols += ', temp_code'; vals.push(b.temp_code||''); }
      const ph = vals.map((_,i)=>'?').join(',');
      info = await db.prepare(`INSERT INTO products (${cols}) VALUES (${ph})`).run(...vals);
      // op_category → product_notes 동기화 (PUT 핸들러와 동일 — 재고현황 필터/그룹핑이 product_notes 를 참조)
      if (b.op_category) {
        try {
          await db.prepare(`INSERT INTO product_notes (product_code, op_category, updated_at) VALUES (?,?,datetime('now','localtime'))
            ON CONFLICT(product_code) DO UPDATE SET op_category=excluded.op_category, updated_at=excluded.updated_at`).run(b.product_code, b.op_category);
        } catch(_){}
      }
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
    const entity = (b.legal_entity === 'dd') ? 'dd' : 'barunson';
    // temp_code 컬럼 존재 여부 런타임 체크
    let _hasTempCodeU = false;
    try { await db.prepare('SELECT temp_code FROM products LIMIT 1').get(); _hasTempCodeU = true; } catch(_){}
    let setCols = 'product_name=?, brand=?, origin=?, category=?, status=?, material_code=?, material_name=?, unit=?, cut_spec=?, jopan=?, paper_maker=?, memo=?, op_category=?, is_new_product=?, spec=?';
    let setVals = [b.product_name||'', b.brand||'', b.origin||'한국', b.category||'', b.status||'active',
      b.material_code||'', b.material_name||'', b.unit||'EA', b.cut_spec||'', b.jopan||'', b.paper_maker||'', b.memo||'', b.op_category||'', b.is_new_product ? 1 : 0, b.spec||''];
    if (_hasEntity.products) { setCols += ', legal_entity=?'; setVals.push(entity); }
    if (_hasTempCodeU) { setCols += ', temp_code=?'; setVals.push(b.temp_code||''); }
    // 매입관리 필드: lead_time_days, moq, payment_terms, supplier_id
    const _procureCols = ['lead_time_days','moq','payment_terms','supplier_id'];
    for (const pc of _procureCols) {
      if (b[pc] !== undefined) {
        let _hasPc = false;
        try { await db.prepare(`SELECT ${pc} FROM products LIMIT 1`).get(); _hasPc = true; } catch(_){}
        if (_hasPc) { setCols += `, ${pc}=?`; setVals.push(b[pc]); }
      }
    }
    setCols += ", updated_at=datetime('now','localtime')";
    setVals.push(id);
    await db.prepare(`UPDATE products SET ${setCols} WHERE id=?`).run(...setVals);
    scheduleProductInfoReload();
    // op_category → product_notes 동기화
    if (b.op_category) {
      const prod = await db.prepare('SELECT product_code FROM products WHERE id=?').get(id);
      if (prod) await db.prepare(`INSERT INTO product_notes (product_code, op_category, updated_at) VALUES (?,?,datetime('now','localtime'))
        ON CONFLICT(product_code) DO UPDATE SET op_category=excluded.op_category, updated_at=excluded.updated_at`).run(prod.product_code, b.op_category);
    }
    ok(res, { id });
    return;
  }

  const prodDel = pathname.match(/^\/api\/products\/(\d+)$/);
  if (prodDel && method === 'DELETE') {
    const id = parseInt(prodDel[1]);
    // 삭제 전 origin 확인 — DD/XERP 동기화 데이터는 삭제 차단
    const prod = await db.prepare('SELECT id, origin, product_name FROM products WHERE id = ?').get(id);
    if (!prod) { fail(res, 404, '품목을 찾을 수 없습니다'); return; }
    if (prod.origin && prod.origin !== 'manual') {
      fail(res, 403, `외부 동기화 데이터(${prod.origin})는 삭제할 수 없습니다. 원본 시스템에서 관리하세요.`);
      return;
    }
    await db.prepare('DELETE FROM products WHERE id = ? AND (origin IS NULL OR origin = "manual")').run(id);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCTS BULK UPLOAD (품목관리 엑셀 일괄 업로드)
  // ════════════════════════════════════════════════════════════════════

  // PREVIEW: 저장 없이 신규/기존/오류 건수만 계산
  if (pathname === '/api/products/bulk/preview' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    if (!items.length) { fail(res, 400, 'items required'); return; }
    const newList = [];      // 신규 품목
    const updateList = [];   // 기존 품목 (덮어쓸 대상)
    const errorList = [];    // 오류 행 (product_code 누락 등)
    for (const it of items) {
      if (!it || !it.product_code) {
        errorList.push({ product_code: it && it.product_code || '', reason: 'product_code 누락' });
        continue;
      }
      const existing = await db.prepare('SELECT product_code, product_name, brand, origin, cut_spec, jopan FROM products WHERE product_code=?').get(it.product_code);
      if (existing) {
        updateList.push({
          product_code: it.product_code,
          new_name: it.product_name || '',
          old_name: existing.product_name || '',
          new_origin: it.origin || '',
          old_origin: existing.origin || ''
        });
      } else {
        newList.push({
          product_code: it.product_code,
          product_name: it.product_name || '',
          origin: it.origin || ''
        });
      }
    }
    ok(res, {
      total: items.length,
      new_count: newList.length,
      update_count: updateList.length,
      error_count: errorList.length,
      new_list: newList,
      update_list: updateList,
      error_list: errorList
    });
    return;
  }

  if (pathname === '/api/products/bulk' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    // mode: 'upsert'(기본, 전부반영) | 'insert_only'(신규만 등록, 기존은 skip)
    const mode = body.mode === 'insert_only' ? 'insert_only' : 'upsert';
    if (!items.length) { fail(res, 400, 'items required'); return; }
    // 서버측 방어: 생산지 변형값 정규화
    const _normOriginBulk = (v) => {
      const s = String(v||'').trim();
      if (!s) return '한국';
      if (s === '한국' || s === '중국' || s === '더기프트') return s;
      if (s === '코리아' || s === '예지가' || s === '마커엘엔피' ||
          s.indexOf('코리아') === 0 || s.indexOf('예지가') === 0 || s.indexOf('마커') === 0) return '한국';
      return s;
    };
    for (const it of items) { if (it) it.origin = _normOriginBulk(it.origin); }

    // is_new_product 추가 — 기존에는 엑셀에 '신제품' 컬럼이 있어도 DB 저장 안 됐음.
    //   엑셀에 컬럼이 없으면 프론트가 0 으로 기본값 세팅하므로, ON CONFLICT 에서 excluded.is_new_product=0 이면
    //   기존값 보존 (업데이트로 기존 신제품 플래그가 일괄 해제되는 사고 방지).
    const upsert = _hasEntity.products
      ? db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, memo, op_category, legal_entity, is_new_product)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(product_code) DO UPDATE SET
        product_name=CASE WHEN excluded.product_name='' THEN products.product_name ELSE excluded.product_name END,
        brand=CASE WHEN excluded.brand='' THEN products.brand ELSE excluded.brand END,
        origin=CASE WHEN excluded.origin='' THEN products.origin ELSE excluded.origin END,
        material_code=CASE WHEN excluded.material_code='' THEN products.material_code ELSE excluded.material_code END,
        material_name=CASE WHEN excluded.material_name='' THEN products.material_name ELSE excluded.material_name END,
        cut_spec=CASE WHEN excluded.cut_spec='' THEN products.cut_spec ELSE excluded.cut_spec END,
        jopan=CASE WHEN excluded.jopan='' THEN products.jopan ELSE excluded.jopan END,
        paper_maker=CASE WHEN excluded.paper_maker='' THEN products.paper_maker ELSE excluded.paper_maker END,
        memo=CASE WHEN excluded.memo='' THEN products.memo ELSE excluded.memo END,
        op_category=CASE WHEN excluded.op_category='' THEN products.op_category ELSE excluded.op_category END,
        legal_entity=CASE WHEN excluded.legal_entity='' THEN products.legal_entity ELSE excluded.legal_entity END,
        is_new_product=CASE WHEN excluded.is_new_product=0 THEN products.is_new_product ELSE excluded.is_new_product END,
        updated_at=datetime('now','localtime')`)
      : db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, memo, op_category, is_new_product)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(product_code) DO UPDATE SET
        product_name=CASE WHEN excluded.product_name='' THEN products.product_name ELSE excluded.product_name END,
        brand=CASE WHEN excluded.brand='' THEN products.brand ELSE excluded.brand END,
        origin=CASE WHEN excluded.origin='' THEN products.origin ELSE excluded.origin END,
        material_code=CASE WHEN excluded.material_code='' THEN products.material_code ELSE excluded.material_code END,
        material_name=CASE WHEN excluded.material_name='' THEN products.material_name ELSE excluded.material_name END,
        cut_spec=CASE WHEN excluded.cut_spec='' THEN products.cut_spec ELSE excluded.cut_spec END,
        jopan=CASE WHEN excluded.jopan='' THEN products.jopan ELSE excluded.jopan END,
        paper_maker=CASE WHEN excluded.paper_maker='' THEN products.paper_maker ELSE excluded.paper_maker END,
        memo=CASE WHEN excluded.memo='' THEN products.memo ELSE excluded.memo END,
        op_category=CASE WHEN excluded.op_category='' THEN products.op_category ELSE excluded.op_category END,
        is_new_product=CASE WHEN excluded.is_new_product=0 THEN products.is_new_product ELSE excluded.is_new_product END,
        updated_at=datetime('now','localtime')`);

    // op_category → product_notes 동기화용
    const upsertNote = db.prepare(`INSERT INTO product_notes (product_code, op_category, updated_at) VALUES (?,?,datetime('now','localtime'))
      ON CONFLICT(product_code) DO UPDATE SET op_category=excluded.op_category, updated_at=excluded.updated_at`);

    let inserted = 0, updated = 0, skipped = 0;
    const tx = db.transaction(async () => {
      for (const it of items) {
        if (!it.product_code) continue;
        const existing = await db.prepare('SELECT id FROM products WHERE product_code=?').get(it.product_code);
        // mode='insert_only': 기존 품목은 건너뜀
        if (existing && mode === 'insert_only') { skipped++; continue; }
        const _bArgs = [
          it.product_code, it.product_name||'', it.brand||'', it.origin||'한국',
          it.material_code||'', it.material_name||'', it.cut_spec||'', it.jopan||'',
          it.paper_maker||'', it.memo||'', it.op_category||''
        ];
        if (_hasEntity.products) _bArgs.push((it.legal_entity === 'dd') ? 'dd' : 'barunson');
        // is_new_product 는 INSERT 컬럼 맨 끝에 위치 (_hasEntity 여부와 무관). 프론트에서 Boolean/0/1 모두 올 수 있어 강제 변환.
        _bArgs.push(it.is_new_product ? 1 : 0);
        await upsert.run(..._bArgs);
        // op_category가 있으면 product_notes에도 동기화
        if (it.op_category) await upsertNote.run(it.product_code, it.op_category);
        if (existing) updated++; else inserted++;
      }
    });
    await tx();
    ok(res, { inserted, updated, skipped, total: inserted + updated + skipped, mode });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  POST /api/products/bulk-import-korea
  //  바른컴퍼니 한국 생산품목 TSV 일괄 업데이트 + 카테고리별 기본 후공정 체인 매칭
  //
  //  요청: { tsv: "품목코드\t카드구분\t브랜드\t계정\t상태\n1\tBC1602\t청첩장\t...", dry_run?: true }
  //  동작:
  //    1) TSV 파싱 (헤더/번호 무시, 컬럼 5개)
  //    2) product_code 기준 중복 제거 (첫 행 유지)
  //    3) products 테이블에 UPSERT: category, brand, origin='한국', status='active', legal_entity='barunson'
  //       - 브랜드의 (D_내)/(D_외) 접미사 제거
  //    4) product_post_vendor 에 카테고리별 기본 체인 insert (기존 체인 있으면 skip)
  //    5) 요약 반환: 파싱/중복제거/신규/업데이트/카테고리별 집계
  // ════════════════════════════════════════════════════════════════════

  // 카드구분 → 기본 후공정 체인 (vendor 는 비어있음. 사용자가 품목별/발주별로 나중에 지정)
  const KOREA_CATEGORY_POST_CHAIN = {
    '청첩장':  [{step:1,process:'인쇄'}, {step:2,process:'재단'}, {step:3,process:'톰슨'}, {step:4,process:'접지'}, {step:5,process:'조립'}],
    '봉투':    [{step:1,process:'인쇄'}, {step:2,process:'재단'}, {step:3,process:'봉투가공'}],
    '내지':    [{step:1,process:'인쇄'}, {step:2,process:'재단'}],
    '감사장':  [{step:1,process:'인쇄'}, {step:2,process:'재단'}, {step:3,process:'톰슨'}],
    '부속':    [{step:1,process:'재단'}],
    '리본':    [{step:1,process:'재단'}],
    '답례품':  [],
    '용지':    [],
    '기타':    [{step:1,process:'인쇄'}, {step:2,process:'재단'}]
  };

  if (pathname === '/api/products/bulk-import-korea' && method === 'POST') {
    const body = await readJSON(req);
    const tsv = body?.tsv || '';
    const dryRun = !!body?.dry_run;
    if (!tsv) { fail(res, 400, 'tsv required'); return; }

    // 1) 파싱
    const lines = tsv.split(/\r?\n/).filter(l => l.trim());
    const rows = [];
    for (const line of lines) {
      const cols = line.split('\t');
      // 헤더 스킵
      if (cols[0] === 'No' || cols[1] === '품목코드') continue;
      // 최소 2개 컬럼 (번호 + 품목코드) 요구
      if (cols.length < 2) continue;
      // 컬럼 매핑: [번호, 품목코드, 카드구분, 브랜드, 계정, 상태]
      let idx = 0;
      // 첫 컬럼이 숫자면 번호로 취급하고 skip, 아니면 품목코드로 취급
      if (/^\d+$/.test(cols[0])) idx = 1;
      const code = (cols[idx] || '').replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();
      const category = (cols[idx+1] || '').trim();
      const brandRaw = (cols[idx+2] || '').trim();
      if (!code) continue;
      // 브랜드에서 (D_내)/(D_외) 접미사 제거 → 원본 브랜드와 변형 플래그 추출
      const dInner = /\(D_내\)/.test(brandRaw);
      const dOuter = /\(D_외\)/.test(brandRaw);
      const brand = brandRaw.replace(/\s*\(D_[내외]\)\s*/g, '').trim();
      const memo = (dInner ? 'D_내' : dOuter ? 'D_외' : '').trim();
      rows.push({ code, category, brand, memo });
    }

    // 2) product_code 기준 중복 제거
    const seen = new Map();
    for (const r of rows) if (!seen.has(r.code)) seen.set(r.code, r);
    const unique = Array.from(seen.values());

    // 카테고리별 집계
    const categoryCount = {};
    for (const r of unique) categoryCount[r.category || '(빈값)'] = (categoryCount[r.category || '(빈값)'] || 0) + 1;

    if (dryRun) {
      ok(res, {
        dry_run: true,
        parsed: rows.length,
        unique: unique.length,
        duplicates_removed: rows.length - unique.length,
        category_breakdown: categoryCount,
        sample: unique.slice(0, 10)
      });
      return;
    }

    // 3) products UPSERT + product_post_vendor 기본 체인 INSERT
    let inserted = 0, updated = 0, chainInserted = 0, chainSkipped = 0, errors = 0;

    // product_post_vendor 테이블 존재 여부 확인
    let ppvOK = true;
    try { await db.prepare('SELECT 1 FROM product_post_vendor LIMIT 1').get(); } catch(_) { ppvOK = false; }

    for (const r of unique) {
      try {
        const existing = await db.prepare('SELECT id FROM products WHERE product_code=?').get(r.code);
        if (existing) {
          await db.prepare(`UPDATE products SET
            category=CASE WHEN ?='' THEN category ELSE ? END,
            brand=CASE WHEN ?='' THEN brand ELSE ? END,
            origin='한국',
            status='active',
            memo=CASE WHEN ?='' THEN memo ELSE ? END,
            updated_at=datetime('now','localtime')
            WHERE product_code=?`).run(r.category, r.category, r.brand, r.brand, r.memo, r.memo, r.code);
          updated++;
        } else {
          await db.prepare(`INSERT INTO products (product_code, product_name, brand, origin, category, status, memo)
            VALUES (?, '', ?, '한국', ?, 'active', ?)`).run(r.code, r.brand, r.category, r.memo);
          inserted++;
        }

        // 기본 후공정 체인 INSERT (기존 체인 있으면 skip)
        if (ppvOK) {
          const chain = KOREA_CATEGORY_POST_CHAIN[r.category] || [];
          if (chain.length) {
            const hasExisting = await db.prepare('SELECT 1 FROM product_post_vendor WHERE product_code=? LIMIT 1').get(r.code);
            if (hasExisting) {
              chainSkipped++;
            } else {
              for (const step of chain) {
                try {
                  await db.prepare('INSERT INTO product_post_vendor (product_code, process_type, vendor_name, step_order) VALUES (?, ?, \'\', ?)').run(r.code, step.process, step.step);
                } catch(_) {}
              }
              chainInserted++;
            }
          }
        }
      } catch (e) {
        errors++;
        if (errors < 5) console.error('[bulk-import-korea] row 오류:', r.code, e.message);
      }
    }

    scheduleProductInfoReload();
    console.log(`[bulk-import-korea] 완료: 신규 ${inserted} / 업데이트 ${updated} / 체인신설 ${chainInserted} / 체인유지 ${chainSkipped} / 오류 ${errors}`);
    ok(res, {
      parsed: rows.length,
      unique: unique.length,
      duplicates_removed: rows.length - unique.length,
      inserted, updated,
      chain_inserted: chainInserted,
      chain_skipped_existing: chainSkipped,
      ppv_table_available: ppvOK,
      errors,
      category_breakdown: categoryCount
    });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  POST /api/admin/products/dedupe
  //  products 테이블 product_code 중복 row 정리 (최신 id 유지, 나머지 삭제)
  //
  //  Body: { dry_run?: true }  — dry_run=true 면 미리보기만, 삭제 안 함
  //
  //  동작:
  //    1) product_code 별 중복 그룹 조회
  //    2) 각 그룹에서 MAX(id) 제외한 나머지 id 들 수집
  //    3) dry_run=false 면 DELETE. dry_run=true 면 목록만 반환
  // ════════════════════════════════════════════════════════════════════
  if (pathname === '/api/admin/products/dedupe' && method === 'POST') {
    const body = await readJSON(req).catch(() => ({}));
    const dryRun = !!body?.dry_run;

    try {
      // 중복 그룹 탐지
      const dupGroups = await db.prepare(`
        SELECT product_code, COUNT(*) AS cnt, STRING_AGG(CAST(id AS TEXT), ',' ORDER BY id) AS ids
        FROM products
        GROUP BY product_code
        HAVING COUNT(*) > 1
      `).all();

      const summary = dupGroups.map(g => {
        const ids = (g.ids || '').split(',').map(x => parseInt(x, 10)).filter(Boolean);
        const keepId = ids[ids.length - 1]; // 최대 id = 가장 최근 row
        const deleteIds = ids.slice(0, -1);
        return { product_code: g.product_code, cnt: Number(g.cnt), keep_id: keepId, delete_ids: deleteIds };
      });

      const allDeleteIds = summary.flatMap(s => s.delete_ids);

      if (dryRun || allDeleteIds.length === 0) {
        ok(res, {
          dry_run: true,
          total_dup_groups: summary.length,
          total_rows_to_delete: allDeleteIds.length,
          sample: summary.slice(0, 20)
        });
        return;
      }

      // 실제 삭제 (배치)
      let deleted = 0;
      for (const id of allDeleteIds) {
        try {
          await db.prepare('DELETE FROM products WHERE id=?').run(id);
          deleted++;
        } catch (_) {}
      }

      console.log(`[dedupe] products 중복 정리 완료: ${deleted}/${allDeleteIds.length}`);
      ok(res, {
        dry_run: false,
        total_dup_groups: summary.length,
        requested_delete: allDeleteIds.length,
        actually_deleted: deleted
      });
    } catch (e) {
      console.error('[dedupe] 실패:', e.message);
      fail(res, 500, '중복 정리 실패: ' + e.message);
    }
    return;
  }

  // PUT /api/products/:code/field — 개별 필드 업데이트
  const prodFieldMatch = pathname.match(/^\/api\/products\/(.+)\/field$/);
  if (prodFieldMatch && method === 'PUT') {
    const code = decodeURIComponent(prodFieldMatch[1]);
    const body = await readJSON(req);
    const allowed = ['cut_spec','jopan','paper_maker','material_name','material_code','post_vendor','lead_time_days','moq','payment_terms','supplier_id','memo'];
    if (!allowed.includes(body.field)) { fail(res, 400, '허용되지 않는 필드'); return; }
    // 이전 값 조회 후 이력 저장
    let prev;
    try { prev = await db.prepare(`SELECT ${body.field} as val FROM products WHERE product_code=?`).get(code); } catch(_) { prev = undefined; }
    const oldVal = prev ? (prev.val || '') : '';
    if (String(oldVal) !== String(body.value)) {
      try {
        // reason, changed_by 컬럼 존재 여부 런타임 체크
        let _hasPfhReason = false;
        try { await db.prepare('SELECT reason FROM product_field_history LIMIT 1').get(); _hasPfhReason = true; } catch(_){}
        const reason = body.reason || '';
        const changer = body.changed_by || (currentUser ? currentUser.username : '');
        if (_hasPfhReason) {
          await db.prepare('INSERT INTO product_field_history (product_code, field_name, old_value, new_value, reason, changed_by) VALUES (?,?,?,?,?,?)').run(code, body.field, String(oldVal), String(body.value), reason, changer);
        } else {
          await db.prepare('INSERT INTO product_field_history (product_code, field_name, old_value, new_value) VALUES (?,?,?,?)').run(code, body.field, String(oldVal), String(body.value));
        }
      } catch(histErr) { console.warn('[product_field_history] insert skip:', histErr.message); }
    }
    await db.prepare(`UPDATE products SET ${body.field}=?, updated_at=datetime('now','localtime') WHERE product_code=?`).run(body.value, code);
    scheduleProductInfoReload();
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'product_update', 'products', code, `품목수정: ${code} ${body.field} "${oldVal}"→"${body.value}"${body.reason ? ' 사유: '+body.reason : ''}`, clientIP);
    ok(res, { updated: code, field: body.field });
    return;
  }

  // PATCH /api/products/:code/post-vendor — 후공정 업체 설정
  const postVendorMatch = pathname.match(/^\/api\/products\/(.+)\/post-vendor$/);
  if (postVendorMatch && method === 'PATCH') {
    const code = decodeURIComponent(postVendorMatch[1]);
    const body = await readJSON(req);
    await db.prepare("UPDATE products SET post_vendor=?, updated_at=datetime('now','localtime') WHERE product_code=?").run(body.post_vendor || '', code);
    // 캐시 무효화
    xerpInventoryCacheTime = 0;
    scheduleProductInfoReload();
    ok(res, { ok: true, code, post_vendor: body.post_vendor });
    return;
  }

  // GET /api/products/:code/history — 필드 변경 이력 조회
  const prodHistMatch = pathname.match(/^\/api\/products\/(.+)\/history$/);
  if (prodHistMatch && method === 'GET') {
    const code = decodeURIComponent(prodHistMatch[1]);
    const rows = await db.prepare('SELECT * FROM product_field_history WHERE product_code=? ORDER BY changed_at DESC LIMIT 50').all(code);
    ok(res, rows);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  품목별 후공정 업체 매핑 API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/product-post-vendor' && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM product_post_vendor ORDER BY product_code, step_order, process_type').all();
    ok(res, rows);
    return;
  }

  if (pathname === '/api/product-post-vendor' && method === 'POST') {
    const body = await readJSON(req);
    const { mappings } = body; // [{product_code, process_type, vendor_name, step_order}, ...]
    if (!mappings || !mappings.length) { fail(res, 400, 'mappings 필요'); return; }
    // replace-all-for-code: 유효한 매핑이 1건 이상인 product_code만 기존 행 삭제 후 INSERT
    // 유효한 매핑 없는 코드는 건드리지 않음 (다른 품목 저장 시 기존 등록 날아가는 것 방지)
    const validMappings = mappings.filter(m => m.product_code && m.process_type && m.vendor_name);
    const codesWithValid = [...new Set(validMappings.map(m => m.product_code))];
    const insert = db.prepare(`INSERT INTO product_post_vendor (product_code, process_type, vendor_name, step_order, updated_at)
      VALUES (?, ?, ?, ?, datetime('now','localtime'))`);
    const delByCode = db.prepare('DELETE FROM product_post_vendor WHERE product_code=?');
    const tx = db.transaction(async () => {
      for (const code of codesWithValid) {
        await delByCode.run(code);
      }
      for (const m of validMappings) {
        await insert.run(m.product_code, m.process_type, m.vendor_name, m.step_order || 1);
      }
    });
    await tx();
    scheduleProductInfoReload();
    ok(res, { ok: true, saved: validMappings.length, replaced_codes: codesWithValid.length });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  XERP 실시간 재고 API (mmInventory + 월출고 통합)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/xerp-inventory' && method === 'GET') {
    // 법인 파라미터 (all/barunson/dd)
    const company = parsed.searchParams.get('company') || 'all';
    // barunson → XERP DB, SiteCode=BK10 (바른손 창고)
    // dd       → BHC  DB, SiteCode=BHC2 (디얼디어 창고)
    // all      → 둘 다 조회 후 병합

    const forceRefresh = parsed.searchParams.get('refresh') === '1';

    // ★ 1단계: Snapshot 우선 조회 (2026-04 재도입)
    // 매번 XERP 호출하지 않음. POST /api/sync/xerp-inventory 로 갱신된 데이터만 반환.
    // refresh=1 이 아닐 때만 snapshot 사용.
    // snapshot 테이블 자체가 없으면(권한 문제) live 로 폴백. 테이블은 있는데 비어있으면
    // 빈 배열 + _needsSync=true 반환 (live 폴백 금지 — nginx 60s 한도 초과 방지)
    let snapTableExists = true;
    if (!forceRefresh) {
      try {
        const snapCount = await db.prepare("SELECT COUNT(*) AS cnt FROM inventory_snapshot").get();
        if (snapCount && snapCount.cnt > 0) {
          // entity 필터
          let entityFilter = '';
          if (company === 'barunson') entityFilter = " AND legal_entity = 'barunson'";
          else if (company === 'dd') entityFilter = " AND legal_entity = 'dd'";

          const snapRows = await db.prepare(`SELECT * FROM inventory_snapshot WHERE 1=1${entityFilter}`).all();
          const snapMap = {};
          for (const s of snapRows) snapMap[s.product_code] = s;

          // Local products 로 메타 보강
          const productFilterParts = ["status IN ('active','inactive')"];
          if (company === 'barunson') productFilterParts.push("(product_code NOT LIKE 'DD%' AND origin != 'DD')");
          else if (company === 'dd') productFilterParts.push("(product_code LIKE 'DD%' OR origin = 'DD')");
          const productFilter = productFilterParts.join(' AND ');
          const products = await db.prepare(
            `SELECT product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, post_vendor, spec FROM products WHERE ${productFilter}`
          ).all();

          const out = [];
          let latestSync = '';
          for (const p of products) {
            const code = (p.product_code || '').replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();
            const s = snapMap[code] || {};
            if (s.synced_at && (!latestSync || s.synced_at > latestSync)) latestSync = s.synced_at;
            const isDD = (code.startsWith('DD') || p.origin === 'DD');
            out.push({
              '제품코드': code,
              '품목명': p.product_name || s.item_name || '',
              '브랜드': p.brand || '',
              '생산지': p.origin || '',
              '현재고': s.current_stock || 0,
              '가용재고': s.current_stock || 0,
              '요청량': 0,
              '_xerpMonthly': s.monthly_out || 0,
              '_xerpDaily': s.daily_out || 0,
              '_xerpTotal3m': s.total_3m || 0,
              '_원자재코드': p.material_code || '',
              '_원재료용지명': p.material_name || '',
              '_절': p.cut_spec || '',
              '_조판': p.jopan || '',
              '_원지사': p.paper_maker || '',
              '_후공정업체': p.post_vendor || '',
              '_규격': p.spec || '',
              '_warehouses': (() => { try { return s.warehouses_json ? JSON.parse(s.warehouses_json) || {} : {}; } catch(_) { return {}; } })(),
              'legal_entity': isDD ? 'dd' : 'barunson',
              '_invSource': s.synced_at ? 'snapshot' : 'no-sync',
              '_siteCode': s.site_code || (isDD ? 'BHC2' : 'BK10'),
              '_snapshotAt': s.synced_at || ''
            });
          }
          console.log(`[xerp-inventory] snapshot 조회: ${out.length}개 반환 (snap 보유 ${snapRows.length}, 최종 sync: ${latestSync || '-'})`);
          ok(res, { products: out, updated: latestSync || new Date().toISOString(), count: out.length, source: 'snapshot' });
          return;
        }
        // 테이블은 있는데 비어있음 → live 폴백 금지 (nginx 60s 넘음).
        // 이전엔 products:[] 빈 응답이라 제품관리에 등록한 품목 목록조차 못 봄.
        // 이제는 products 테이블에서 목록만 가져와 재고 0 으로 표시 (필터/정렬 작동).
        // 동기화 완료되면 snapshot 경로가 재고 수치를 채워서 재렌더.
        console.log('[xerp-inventory] snapshot 테이블 비어있음 — products-only fallback 로 목록 표시');
        try {
          const _emptyFilterParts = ["status IN ('active','inactive')"];
          if (company === 'barunson') _emptyFilterParts.push("(product_code NOT LIKE 'DD%' AND origin != 'DD')");
          else if (company === 'dd') _emptyFilterParts.push("(product_code LIKE 'DD%' OR origin = 'DD')");
          const _emptyRows = await db.prepare(
            `SELECT product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, post_vendor, spec FROM products WHERE ${_emptyFilterParts.join(' AND ')}`
          ).all();
          const _emptyOut = _emptyRows.map(p => {
            const code = (p.product_code || '').replace(/[\s ​‌‍﻿]/g, '').trim();
            const isDD = code.startsWith('DD') || p.origin === 'DD';
            return {
              '제품코드': code, '품목명': p.product_name || '', '브랜드': p.brand || '',
              '생산지': p.origin || '', '현재고': 0, '가용재고': 0, '요청량': 0,
              '_xerpMonthly': 0, '_xerpDaily': 0, '_xerpTotal3m': 0,
              '_원자재코드': p.material_code || '', '_원재료용지명': p.material_name || '',
              '_절': p.cut_spec || '', '_조판': p.jopan || '', '_원지사': p.paper_maker || '',
              '_후공정업체': p.post_vendor || '', '_규격': p.spec || '', '_warehouses': {},
              'legal_entity': isDD ? 'dd' : 'barunson',
              '_invSource': 'empty-snapshot', '_siteCode': isDD ? 'BHC2' : 'BK10'
            };
          });
          ok(res, { products: _emptyOut, updated: new Date().toISOString(), count: _emptyOut.length, source: 'empty-snapshot', message: '아직 동기화된 데이터가 없습니다. 상단 [DB 동기화] 버튼을 눌러주세요. (현재 재고 0 으로 표시)' });
          return;
        } catch (_fbErr) {
          console.error('[xerp-inventory] empty-snapshot fallback 실패:', _fbErr.message);
          ok(res, { products: [], updated: new Date().toISOString(), count: 0, source: 'empty-snapshot', message: '아직 동기화된 데이터가 없습니다. 상단 [DB 동기화] 버튼을 눌러주세요.' });
          return;
        }
      } catch (e) {
        // 테이블 자체가 없는 경우 — live 폴백도 nginx 60s 넘어 HTML 504 나므로 회피
        // refresh=1 (관리자 셀프콜) 일 때만 live 허용, 일반 페이지 로드는 캐시/products만
        if (/does not exist|relation.*not exist/i.test(e.message)) {
          snapTableExists = false;
          console.warn('[xerp-inventory] snapshot 테이블 미존재:', e.message);
        } else {
          fail(res, 500, 'snapshot 조회 오류: ' + e.message);
          return;
        }
      }
    }

    // ★ 2단계: Live XERP 쿼리 경로
    // - refresh=1 (DB 동기화 버튼의 백그라운드 self-call): 끝까지 live 수행
    // - 일반 페이지 로드: 캐시 있으면 캐시, 없으면 products-only 응답 (nginx 504 방지)
    const now = Date.now();
    if (!xerpInventoryCaches) xerpInventoryCaches = {};
    const cacheEntry = xerpInventoryCaches[company];
    if (!forceRefresh && cacheEntry && now - cacheEntry.time < 600000) {
      ok(res, cacheEntry.data);
      return;
    }

    // snapshot 도 없고 캐시도 없고 refresh=1 도 아니면 → products-only 응답으로 빠르게 종료
    // (nginx 타임아웃 방지). 사용자는 'DB 동기화' 버튼으로 명시적 live 트리거해야 함.
    if (!forceRefresh && !snapTableExists) {
      try {
        const productFilterParts = ["status IN ('active','inactive')"];
        if (company === 'barunson') productFilterParts.push("(product_code NOT LIKE 'DD%' AND origin != 'DD')");
        else if (company === 'dd') productFilterParts.push("(product_code LIKE 'DD%' OR origin = 'DD')");
        const productFilter = productFilterParts.join(' AND ');
        const products = await db.prepare(
          `SELECT product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, post_vendor FROM products WHERE ${productFilter}`
        ).all();
        const out = products.map(p => {
          const code = (p.product_code || '').replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();
          const isDD = code.startsWith('DD') || p.origin === 'DD';
          return {
            '제품코드': code,
            '품목명': p.product_name || '',
            '브랜드': p.brand || '',
            '생산지': p.origin || '',
            '현재고': 0, '가용재고': 0, '요청량': 0,
            '_xerpMonthly': 0, '_xerpDaily': 0, '_xerpTotal3m': 0,
            '_원자재코드': p.material_code || '',
            '_원재료용지명': p.material_name || '',
            '_절': p.cut_spec || '', '_조판': p.jopan || '',
            '_원지사': p.paper_maker || '',
            '_후공정업체': p.post_vendor || '',
            'legal_entity': isDD ? 'dd' : 'barunson',
            '_invSource': 'products-only',
            '_siteCode': isDD ? 'BHC2' : 'BK10'
          };
        });
        console.log(`[xerp-inventory] snapshot 없음 + 캐시 없음 → products-only 응답: ${out.length}개`);
        ok(res, { products: out, updated: new Date().toISOString(), count: out.length, source: 'empty-snapshot', message: 'XERP 재고/출고 데이터 없음. 상단 [DB 동기화] 버튼을 눌러주세요.' });
        return;
      } catch (e) {
        console.error('[xerp-inventory] products-only 조회 실패:', e.message);
      }
    }

    // 여기까지 왔다면 refresh=1 (DB 동기화의 백그라운드 self-call). live XERP 쿼리 진행.
    await ensureXerpPool().catch(()=>null);
    // live 경로 진입 조건: forceRefresh 또는 snapTableExists (empty 경우는 위에서 이미 return 됨)
    {
      /* 공백 블록 — 아래 fetchCompanyInventory 흐름으로 진입 */
    }

    // ── 내부 헬퍼: 특정 법인의 재고/출고를 해당 DB+SiteCode에서 조회 ──
    // legalEntity: 'barunson' | 'dd'
    async function fetchCompanyInventory(legalEntity) {
      const isDd = legalEntity === 'dd';
      // 품목 리스트 필터
      const originFilter = isDd
        ? "(product_code LIKE 'DD%' OR origin = 'DD')"
        : "(product_code NOT LIKE 'DD%' AND origin != 'DD')";
      const statusFilter = isDd
        ? "(status = 'active' OR status = 'inactive')"  // DD: inactive 포함
        : "status = 'active'";
      const rawRegistered = await db.prepare(
        `SELECT product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, post_vendor FROM products WHERE ${statusFilter} AND ${originFilter}`
      ).all();
      if (!rawRegistered.length) return [];

      // ★ 핵심 매칭 보정: product_code 공백 + 보이지 않는 유니코드 문자 제거
      // XERP ItemCode 는 nchar(40) 패딩이라 SQL RTRIM 으로 받아오지만,
      // 로컬 products.product_code 에 ZWSP/NBSP 등이 섞여 있으면 IN절 필터에서 탈락해서
      // 조용히 매칭 실패 (재고 0). 양쪽 다 cleanProductCode 처리.
      for (const p of rawRegistered) {
        p.product_code = (p.product_code || '').replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();
      }

      // ★ products 테이블 중복 제거 (product_code 기준, 첫 등장 유지)
      // 증상: BAG_ACC06/BAG_ACC07/BAG_ACC08 등이 화면에 2행씩 뜸 — UNIQUE 제약이 과거에 제대로 적용 안
      //      됐거나 import 시 duplicate 가 쌓인 케이스. SELECT 단계에서 JS 로 dedupe.
      const _seenCodes = new Set();
      const registeredProducts = [];
      let _dupSkipped = 0;
      for (const p of rawRegistered) {
        const c = p.product_code;
        if (!c) continue;
        if (_seenCodes.has(c)) { _dupSkipped++; continue; }
        _seenCodes.add(c);
        registeredProducts.push(p);
      }
      if (_dupSkipped > 0) console.warn(`[xerp-inv ${legalEntity}] products 테이블 중복 ${_dupSkipped}건 skip (첫 row 유지)`);

      const productCodes = registeredProducts.map(p => p.product_code);
      // IN절 안전 코드 (SQL Injection 방지: 영숫자/_/- 만 허용)
      const validCodeCount = productCodes.filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).length;
      if (!validCodeCount) return [];

      // DB 풀 + SiteCode 분기
      const dbName = isDd ? 'BHC' : 'XERP';
      const siteCode = isDd ? 'BHC2' : 'BK10';
      let workPool = null;
      let createdLocal = false;
      if (isDd) {
        // BHC는 on-demand pool (사용 후 닫음)
        workPool = new sql.ConnectionPool({ ...xerpConfig, database: 'BHC' });
        await workPool.connect();
        createdLocal = true;
      } else {
        workPool = xerpPool;
      }

      try {
        if (!workPool) throw new Error('XERP pool not connected');

        // ★ 2026-04 리팩토링: chunk + IN 절 방식 폐기 → SiteCode 전체 단일 쿼리.
        // 이전엔 chunk(20개) × 57회 쿼리 중 일부가 조용히 타임아웃되면 그 chunk 품목
        // 전원이 ohQty=0 으로 fallback 돼 재고가 0 으로 저장되는 버그.
        // 수정: SiteCode=BK10/BHC2 전체를 한 쿼리로 SUM GROUP BY ItemCode 해서 받고
        // 로컬 products 와 매칭. 쿼리 1회 → 실패 감지 명확, 타임아웃 1개만 관리.
        const validCodeSet = new Set(productCodes.filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => c.toUpperCase()));

        // 1. 현재고 — SiteCode 전체 단일 쿼리, ItemCode + WhCode 별로 GROUP BY.
        // ★ 창고별 분해 도입: 프론트가 드롭다운으로 특정 창고만 보고 싶을 수 있어서, 합산값(invMap) 외에
        //   창고별 맵(invByWh: {item_code: {wh_code: qty}}) 도 별도 보관해 snapshot 에 JSON 으로 저장.
        const invMap = {};                      // 전체 합산 — 기존 사용처 호환
        const invByWh = {};                     // {item_code: {wh_code: qty}}
        try {
          const req = workPool.request();
          req.timeout = 120000; // 2분
          const r = await req.query(`
            SELECT RTRIM(ItemCode) AS item_code,
                   RTRIM(WhCode)   AS wh_code,
                   SUM(CASE WHEN OhQty > 0 THEN OhQty ELSE 0 END) AS oh_qty
            FROM mmInventory WITH (NOLOCK)
            WHERE SiteCode = '${siteCode}'
            GROUP BY RTRIM(ItemCode), RTRIM(WhCode)
          `);
          for (const row of r.recordset) {
            const code = (row.item_code || '').trim().toUpperCase();
            const wh   = (row.wh_code   || '').trim();
            const qty  = Math.round(row.oh_qty || 0);
            if (!code || !validCodeSet.has(code)) continue;
            invMap[code] = (invMap[code] || 0) + qty;
            if (wh && qty > 0) {
              if (!invByWh[code]) invByWh[code] = {};
              invByWh[code][wh] = (invByWh[code][wh] || 0) + qty;
            }
          }
          console.log(`[xerp-inv ${legalEntity}] 현재고 단일쿼리 성공: ${Object.keys(invMap).length}개 품목 매칭, 창고-품목 조합 ${r.recordset.length}건`);
        } catch (invErr) {
          console.error(`[xerp-inv ${legalEntity}] 현재고 단일쿼리 실패 — 전체 sync 중단:`, invErr.message);
          throw invErr; // 전체 sync 실패 처리 (기존 snapshot 유지)
        }

        // 2. 최근 3개월 출고 — SiteCode 전체 단일 쿼리
        const today = new Date();
        const start3m = new Date(today); start3m.setMonth(start3m.getMonth() - 3);
        const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
        const shipMap = {};
        try {
          const req = workPool.request()
            .input('start3m', sql.NChar(16), fmt(start3m))
            .input('today', sql.NChar(16), fmt(today));
          req.timeout = 120000;
          const r = await req.query(`
            SELECT RTRIM(ItemCode) AS item_code, SUM(InoutQty) AS total_qty
            FROM mmInoutItem WITH (NOLOCK)
            WHERE SiteCode = '${siteCode}' AND InoutGubun = 'SO'
              AND InoutDate >= @start3m AND InoutDate < @today
            GROUP BY RTRIM(ItemCode)
          `);
          for (const row of r.recordset) {
            const code = (row.item_code || '').trim().toUpperCase();
            if (code && validCodeSet.has(code)) {
              const total = Math.round(row.total_qty || 0);
              shipMap[code] = { total, monthly: Math.round(total / 3), daily: Math.round(total / 90) };
            }
          }
          console.log(`[xerp-inv ${legalEntity}] 출고 단일쿼리 성공: ${Object.keys(shipMap).length}개 매칭 (3개월치)`);
        } catch (shipErr) {
          console.warn(`[xerp-inv ${legalEntity}] 출고 단일쿼리 실패 — 출고 0 으로 진행:`, shipErr.message);
          // 출고는 실패해도 재고만이라도 보이도록 계속 진행
        }

        // 3. 품목명 (S2_Card) — 바른손 모드에서만, 전체 Card 테이블 한 번에 가져와서 매칭
        let itemNames = {};
        if (!isDd) {
          try {
            const bar1Pool = new sql.ConnectionPool(barShopConfig);
            await bar1Pool.connect();
            try {
              const req = bar1Pool.request();
              req.timeout = 60000;
              const r = await req.query(`SELECT RTRIM(Card_Code) AS code, Card_Name FROM S2_Card`);
              r.recordset.forEach(row => {
                const code = (row.code || '').trim().toUpperCase();
                if (code && validCodeSet.has(code)) {
                  itemNames[code] = (row.Card_Name || '').trim();
                }
              });
              console.log(`[xerp-inv ${legalEntity}] 품목명 단일쿼리 성공: ${Object.keys(itemNames).length}개 매칭`);
            } catch (nameErr) { console.warn(`[xerp-inv ${legalEntity}] 품목명 쿼리 실패:`, nameErr.message); }
            await bar1Pool.close();
          } catch (e) { console.warn('품목명 풀 생성 실패:', e.message); }
        }

        // 4. 품목 병합
        const out = [];
        for (const p of registeredProducts) {
          const code = p.product_code;
          const codeUpper = code.toUpperCase();
          const ohQty = invMap[codeUpper] || 0;
          const ship = shipMap[codeUpper] || { total: 0, monthly: 0, daily: 0 };
          const whMap = invByWh[codeUpper] || {};
          out.push({
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
            '_후공정업체': p.post_vendor || '',
            '_warehouses': whMap,                         // {wh_code: qty} — 프론트 드롭다운 필터용
            'legal_entity': isDd ? 'dd' : 'barunson',
            '_invSource': dbName,
            '_siteCode': siteCode
          });
        }
        console.log(`${dbName}/${siteCode} 재고 로드 (${legalEntity}): ${out.length}개 품목`);
        return out;
      } catch (xerpErr) {
        console.warn(`[xerp-inventory] ${dbName}/${siteCode} XERP 실패 — products 테이블 폴백: ${xerpErr.message}`);
        return registeredProducts.map(p => ({
          '제품코드': p.product_code,
          '품목명': p.product_name || '',
          '브랜드': p.brand || '',
          '생산지': p.origin || '',
          '현재고': 0,
          '가용재고': 0,
          '요청량': 0,
          '_xerpMonthly': 0,
          '_xerpDaily': 0,
          '_xerpTotal3m': 0,
          '_원자재코드': p.material_code || '',
          '_원재료용지명': p.material_name || '',
          '_절': p.cut_spec || '',
          '_조판': p.jopan || '',
          '_원지사': p.paper_maker || '',
          '_후공정업체': p.post_vendor || '',
          'legal_entity': isDd ? 'dd' : 'barunson',
          '_invSource': 'fallback:products',
          '_siteCode': siteCode,
          '_xerpError': xerpErr.message
        }));
      } finally {
        if (createdLocal && workPool) {
          try { await workPool.close(); } catch(_){}
        }
      }
    }

    try {
      // ★ Fix #5: 전체 live 쿼리에 하드 타임아웃 45s 부여.
      // chunk 별 7s + 재시도 15s 가 있지만 chunk 수가 많으면 누적 합계가 nginx 60s 를 넘길 수 있음.
      // 45s 초과 시 TIMEOUT 플래그와 함께 snapshot/products-only fallback 으로 응답 — 504 캐스케이드 방지.
      const HARD_BUDGET_MS = 45_000;
      const TIMEOUT_SENTINEL = Symbol('xerp_live_timeout');
      const withBudget = (p) => Promise.race([
        p,
        new Promise(resolve => setTimeout(() => resolve(TIMEOUT_SENTINEL), HARD_BUDGET_MS))
      ]);

      let products = [];
      let liveTimedOut = false;
      if (company === 'barunson') {
        const r = await withBudget(fetchCompanyInventory('barunson'));
        if (r === TIMEOUT_SENTINEL) liveTimedOut = true; else products = r;
      } else if (company === 'dd') {
        const r = await withBudget(fetchCompanyInventory('dd'));
        if (r === TIMEOUT_SENTINEL) liveTimedOut = true; else products = r;
      } else {
        // all: 둘 다 조회 후 병합 — 총 시간도 45s 버짓 내에서 끝나야 함
        const combined = withBudget(Promise.all([
          fetchCompanyInventory('barunson').catch(e => { console.error('barunson 조회 실패:', e.message); return []; }),
          fetchCompanyInventory('dd').catch(e => { console.error('dd 조회 실패:', e.message); return []; })
        ]));
        const r = await combined;
        if (r === TIMEOUT_SENTINEL) liveTimedOut = true;
        else { const [bs, dd] = r; products = [...bs, ...dd]; }
      }

      if (liveTimedOut) {
        // products-only fallback — 로컬 products 테이블만 조회해서 재고 0 으로 응답.
        // 사용자는 재고 수치만 안 보일 뿐 페이지 자체는 뜸 → 504 대비 훨씬 나음.
        console.warn(`[xerp-inventory] live 45s 타임아웃 — products-only fallback (company=${company})`);
        try {
          const productFilterParts = ["status IN ('active','inactive')"];
          if (company === 'barunson') productFilterParts.push("(product_code NOT LIKE 'DD%' AND origin != 'DD')");
          else if (company === 'dd') productFilterParts.push("(product_code LIKE 'DD%' OR origin = 'DD')");
          const fbRows = await db.prepare(
            `SELECT product_code, product_name, brand, origin, material_code, material_name, cut_spec, jopan, paper_maker, post_vendor FROM products WHERE ${productFilterParts.join(' AND ')}`
          ).all();
          const fbOut = fbRows.map(p => {
            const code = (p.product_code || '').replace(/[\s ​‌‍﻿]/g, '').trim();
            const isDD = code.startsWith('DD') || p.origin === 'DD';
            return {
              '제품코드': code, '품목명': p.product_name || '', '브랜드': p.brand || '',
              '생산지': p.origin || '', '현재고': 0, '가용재고': 0, '요청량': 0,
              '_xerpMonthly': 0, '_xerpDaily': 0, '_xerpTotal3m': 0,
              '_원자재코드': p.material_code || '', '_원재료용지명': p.material_name || '',
              '_절': p.cut_spec || '', '_조판': p.jopan || '', '_원지사': p.paper_maker || '',
              '_후공정업체': p.post_vendor || '',
              'legal_entity': isDD ? 'dd' : 'barunson',
              '_invSource': 'live-timeout', '_siteCode': isDD ? 'BHC2' : 'BK10'
            };
          });
          ok(res, { products: fbOut, updated: new Date().toISOString(), count: fbOut.length, source: 'live-timeout', message: 'XERP 응답 지연 (45s 초과) — 재고 수치 없이 목록만 표시. 다시 시도해주세요.' });
          return;
        } catch (fbErr) {
          fail(res, 504, 'XERP 응답 지연 (45s) + products-only fallback 실패: ' + fbErr.message);
          return;
        }
      }

      if (!products.length) {
        ok(res, { products: [], updated: new Date().toISOString(), count: 0, message: '품목관리에 등록된 제품이 없습니다. 먼저 품목을 등록해주세요.' });
        return;
      }

      const cacheData = { products, updated: new Date().toISOString(), count: products.length };
      xerpInventoryCaches[company] = { data: cacheData, time: now };
      // 기존 캐시 호환 (다른 모듈에서 참조)
      if (company === 'barunson') { xerpInventoryCache = cacheData; xerpInventoryCacheTime = now; }
      console.log(`재고 API 완료 (company=${company}): ${products.length}개 품목`);
      ok(res, cacheData);
    } catch (e) {
      console.error('재고 조회 오류:', e.message, '(company=' + company + ')');
      // v1.0.8: 타임아웃/연결 끊김 자동 재연결은 XERP(바른손) 경로에서만 수행.
      //         BHC(dd) 경로의 에러가 XERP 메인 풀을 내리지 않도록 company 분기.
      //         all 모드는 Promise.all 내부에서 catch로 흡수되므로 이쪽까지 안 옴.
      const isXerpRelated = (company === 'barunson' || company === 'all');
      if (isXerpRelated && (e.message.includes('imeout') || e.message.includes('closed') || e.message.includes('ECONN'))) {
        try { await xerpPool.close(); } catch(_){}
        try { xerpPool = await sql.connect(xerpConfig); console.log('XERP 재연결 완료'); } catch(re) { console.error('XERP 재연결 실패:', re.message); }
      }
      fail(res, 500, '재고 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  품목별 발주 트래킹 API (중복발주 방지 — 입고중 + 입고완료 미동기화)
  // ════════════════════════════════════════════════════════════════════
  if (pathname === '/api/inventory/pending-orders' && method === 'GET') {
    try {
      // 1) 진행중 PO (미입고 잔량) — draft~partial
      const pendingRows = await db.prepare(`
        SELECT i.product_code,
               SUM(i.ordered_qty - COALESCE(i.received_qty,0)) as pending_qty,
               SUM(i.ordered_qty) as ordered_qty,
               SUM(COALESCE(i.received_qty,0)) as partial_received_qty,
               STRING_AGG(DISTINCT h.os_number, ',') as os_numbers,
               STRING_AGG(DISTINCT h.po_number, ',') as po_numbers,
               STRING_AGG(DISTINCT h.po_type, ',') as po_types,
               MIN(h.due_date) as earliest_due,
               MAX(h.status) as latest_status,
               MAX(h.po_date) as latest_po_date
        FROM po_items i
        JOIN po_header h ON h.po_id = i.po_id
        WHERE h.status NOT IN ('cancelled','completed','received')
          AND (i.ordered_qty - COALESCE(i.received_qty,0)) > 0
        GROUP BY i.product_code
      `).all();

      // 2) 입고완료 but XERP 미동기화 (os_number 미등록 or os_registered 아닌 received PO)
      const receivedRows = await db.prepare(`
        SELECT i.product_code,
               SUM(COALESCE(i.received_qty,0)) as received_qty,
               STRING_AGG(DISTINCT h.po_number, ',') as po_numbers,
               MAX(h.updated_at) as completed_at
        FROM po_items i
        JOIN po_header h ON h.po_id = i.po_id
        WHERE h.status = 'received'
          AND COALESCE(i.received_qty,0) > 0
          AND (h.os_number IS NULL OR h.os_number = '')
        GROUP BY i.product_code
      `).all();

      const map = {};
      pendingRows.forEach(r => {
        map[r.product_code] = {
          pending_qty: r.pending_qty || 0,
          ordered_qty: r.ordered_qty || 0,
          partial_received_qty: r.partial_received_qty || 0,
          received_not_synced: 0,
          os_numbers: (r.os_numbers || '').split(',').filter(Boolean),
          po_numbers: (r.po_numbers || '').split(',').filter(Boolean),
          po_types: (r.po_types || '').split(',').filter(Boolean),
          earliest_due: r.earliest_due || '',
          status: r.latest_status || '',
          last_order_date: r.latest_po_date || ''
        };
      });
      // 입고완료(미동기화) 수량 합산
      receivedRows.forEach(r => {
        if (!map[r.product_code]) {
          map[r.product_code] = {
            pending_qty: 0, ordered_qty: 0, partial_received_qty: 0,
            received_not_synced: 0,
            os_numbers: [], po_numbers: [], po_types: [],
            earliest_due: '', status: 'received', last_order_date: ''
          };
        }
        map[r.product_code].received_not_synced = r.received_qty || 0;
        const extraPOs = (r.po_numbers || '').split(',').filter(Boolean);
        extraPOs.forEach(p => {
          if (!map[r.product_code].po_numbers.includes(p)) map[r.product_code].po_numbers.push(p);
        });
      });
      ok(res, map);
    } catch(e) { console.error('pending-orders error:', e.message); ok(res, {}); }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  [진단] XERP ↔ 로컬 제품코드 매칭 확인
  //  GET /api/debug/xerp-match?code=1107RE
  //  - 로컬 products 에서 해당 코드 조회
  //  - XERP mmInventory / mmInoutItem 원시 쿼리 (단일 코드)
  //  - LIKE 매칭으로 유사 코드도 함께 검색 (prefix/pad 차이 찾기)
  // ════════════════════════════════════════════════════════════════════
  if (pathname === '/api/debug/env-status' && method === 'GET') {
    // 환경변수 로드 상태 진단 — 비번은 길이만 노출해 SSL 로그/스샷 유출 방지.
    const mask = (v) => v ? `[${String(v).length}자]` : '(없음)';
    const src = (k) => envVars[k] ? '.env 파일' : (process.env[k] ? 'docker env' : '미설정');
    const out = {
      dotenv_path: dotenvPath,
      dotenv_exists: require('fs').existsSync(dotenvPath),
      xerp_config: {
        server: xerpConfig.server || '(없음)',
        port: xerpConfig.port,
        user: xerpConfig.user || '(없음)',
        password_len: mask(xerpConfig.password),
        database: xerpConfig.database
      },
      bar_shop_config: {
        server: barShopConfig.server || '(없음)',
        port: barShopConfig.port,
        user: barShopConfig.user || '(없음)',
        password_len: mask(barShopConfig.password),
        database: barShopConfig.database
      },
      dd_config: {
        host: ddConfig.host || '(없음)',
        port: ddConfig.port,
        user: ddConfig.user || '(없음)',
        password_len: mask(ddConfig.password)
      },
      sources: {
        DB_SERVER: src('DB_SERVER'),
        DB_USER: src('DB_USER'),
        DB_PASSWORD: src('DB_PASSWORD'),
        XERP_DB_SERVER: src('XERP_DB_SERVER'),
        XERP_DB_USER: src('XERP_DB_USER'),
        XERP_DB_PASSWORD: src('XERP_DB_PASSWORD'),
        DD_DB_SERVER: src('DD_DB_SERVER'),
        DD_DB_USER: src('DD_DB_USER'),
        DD_DB_PASSWORD: src('DD_DB_PASSWORD')
      },
      xerp_pool_connected: !!xerpPool
    };
    ok(res, out);
    return;
  }

  // ── 창고 목록 조회 — XERP_INV_WAREHOUSE 설정 시 어떤 코드를 써야 하는지 확인용 ──
  if (pathname === '/api/debug/xerp-warehouses' && method === 'GET') {
    try {
      await ensureXerpPool();
      if (!xerpPool) { fail(res, 503, 'XERP 풀 미연결'); return; }
      // mmInventory 와 mmInoutItem 양쪽에서 등장하는 WhCode 별 통계 (SiteCode=BK10).
      // 추가: mmWarehouse 가 있으면 WhName 도 함께 (LEFT JOIN, 없어도 동작).
      const r = await xerpPool.request().query(`
        SELECT RTRIM(inv.WhCode) AS wh_code,
               COUNT(DISTINCT RTRIM(inv.ItemCode)) AS item_count,
               SUM(CASE WHEN inv.OhQty > 0 THEN inv.OhQty ELSE 0 END) AS total_qty
        FROM mmInventory inv WITH (NOLOCK)
        WHERE inv.SiteCode = 'BK10' AND inv.WhCode IS NOT NULL AND RTRIM(inv.WhCode) <> ''
        GROUP BY RTRIM(inv.WhCode)
        ORDER BY item_count DESC
      `);
      ok(res, {
        configured: XERP_INV_WH_LIST,
        configured_active: XERP_INV_WH_LIST.length > 0,
        warehouses: r.recordset,
        hint: '환경변수 XERP_INV_WAREHOUSE=<wh_code> 로 설정하면 해당 창고만 조회. 여러 개는 콤마 구분.'
      });
    } catch (e) {
      fail(res, 500, '창고 조회 실패: ' + e.message);
    }
    return;
  }

  if (pathname === '/api/debug/xerp-match' && method === 'GET') {
    const code = (parsed.searchParams.get('code') || '').trim();
    if (!code) { fail(res, 400, 'code 파라미터 필요'); return; }
    const cleanCode = code.replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();

    const out = { input: code, cleaned: cleanCode };
    try {
      // 1) 로컬 products
      const localRows = await db.prepare("SELECT product_code, product_name, origin, status FROM products WHERE product_code = ? OR product_code LIKE ?").all(cleanCode, '%' + cleanCode + '%');
      out.local_products = localRows.map(r => ({
        product_code: r.product_code,
        code_chars: Array.from(r.product_code || '').map(ch => ch.charCodeAt(0)).join(','),
        product_name: r.product_name,
        origin: r.origin,
        status: r.status
      }));
    } catch(e) { out.local_error = e.message; }

    try {
      await ensureXerpPool();
      if (!xerpPool) { out.xerp_error = 'XERP 풀 미연결'; }
      else {
        // 2) XERP mmInventory - 정확 매칭 (모든 row + 모든 column)
        const exact = await xerpPool.request().query(`
          SELECT * FROM mmInventory WITH (NOLOCK)
          WHERE RTRIM(ItemCode) = '${cleanCode.replace(/'/g, "''")}'
        `);
        out.xerp_inventory_exact = exact.recordset;
        // 집계 방식별 비교
        const rows = exact.recordset || [];
        const sumAll = rows.reduce((s,r) => s + Number(r.OhQty || 0), 0);
        const sumPositiveOnly = rows.filter(r => Number(r.OhQty || 0) > 0).reduce((s,r) => s + Number(r.OhQty || 0), 0);
        const sumByBK10 = rows.filter(r => (r.SiteCode || '').trim() === 'BK10').reduce((s,r) => s + Number(r.OhQty || 0), 0);
        const sumBK10PositiveOnly = rows.filter(r => (r.SiteCode || '').trim() === 'BK10' && Number(r.OhQty || 0) > 0).reduce((s,r) => s + Number(r.OhQty || 0), 0);
        out.xerp_inventory_summary = {
          row_count: rows.length,
          sites: [...new Set(rows.map(r => (r.SiteCode || '').trim()))],
          sum_all: sumAll,                              // 전체 rows SUM (저희 현재 방식)
          sum_positive_only: sumPositiveOnly,           // 양수 rows만 SUM
          sum_bk10_all: sumBK10PositiveOnly,            // BK10만 + 양수만
          sum_bk10_including_negative: sumByBK10        // BK10만 (음수 포함)
        };

        // 3) XERP mmInventory - LIKE 매칭 (prefix/suffix 차이 확인)
        const like = await xerpPool.request().query(`
          SELECT TOP 10 RTRIM(ItemCode) AS item_code, SiteCode, OhQty,
                 LEN(ItemCode) AS len_ic
          FROM mmInventory WITH (NOLOCK)
          WHERE ItemCode LIKE '%${cleanCode.replace(/'/g, "''")}%'
            AND RTRIM(ItemCode) <> '${cleanCode.replace(/'/g, "''")}'
        `);
        out.xerp_inventory_like = like.recordset;

        // 4) XERP mmInoutItem - 최근 3개월 출고 (정확 매칭)
        const today = new Date();
        const start3m = new Date(today); start3m.setMonth(start3m.getMonth() - 3);
        const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
        const ship = await xerpPool.request()
          .input('s', sql.NChar(16), fmt(start3m))
          .input('t', sql.NChar(16), fmt(today))
          .query(`
            SELECT TOP 5 RTRIM(ItemCode) AS item_code, InoutDate, InoutQty, InoutGubun
            FROM mmInoutItem WITH (NOLOCK)
            WHERE RTRIM(ItemCode) = '${cleanCode.replace(/'/g, "''")}'
              AND InoutDate >= @s AND InoutDate < @t
              AND InoutGubun = 'SO'
            ORDER BY InoutDate DESC
          `);
        out.xerp_shipment_exact = ship.recordset;
      }
    } catch(e) { out.xerp_error = e.message; }

    ok(res, out);
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Snapshot 동기화 API (2026-04 재도입)
  //  POST /api/sync/xerp-inventory : 현재 /api/xerp-inventory?refresh=1 과 동일한 live 쿼리로
  //                                  전체 재고/출고 받아와서 inventory_snapshot 테이블에 UPSERT
  //                                  즉시 202 반환, 백그라운드에서 실행
  //  GET  /api/sync/status          : 마지막 동기화 시각 / 진행 여부 조회
  // ════════════════════════════════════════════════════════════════════
  if (pathname === '/api/sync/xerp-inventory' && method === 'POST') {
    // 중복 실행 방지 (7분 이상은 자동 stale 처리)
    try {
      // started_at 은 'YYYY-MM-DD HH24:MI:SS' KST 텍스트. naive ::timestamp 로 캐스트하면 서버 TZ와 섞여
      // UTC 컨테이너에서 +9h 미래로 오판돼 stale 해제가 영구 0건이던 문제.
      // 수정: started_at 을 명시적으로 KST timestamptz 로 변환 → NOW() 와 시점 비교. 서버 TZ 무관하게 정확.
      await db.prepare("UPDATE sync_log SET status='failed', error_msg='stale 자동 해제', finished_at=datetime('now','localtime') WHERE sync_type='xerp_inventory' AND status='running' AND (started_at::timestamp AT TIME ZONE 'Asia/Seoul') < NOW() - INTERVAL '7 minutes'").run();
    } catch (_) {}
    try {
      const running = await db.prepare("SELECT id, started_at FROM sync_log WHERE sync_type='xerp_inventory' AND status='running' LIMIT 1").get();
      if (running) { fail(res, 409, '이미 동기화 진행 중 (시작: ' + running.started_at + ')'); return; }
    } catch (_) {}

    // sync_log 시작 기록. 테이블 없으면 (권한 이슈) snapshot 기능 비활성 + live 리프레시 모드로 폴백.
    let syncLogId = null;
    let triggeredBy = 'manual';
    let snapshotDisabled = false;
    const body = await readJSON(req).catch(() => ({}));
    triggeredBy = body?.triggered_by || 'manual';
    try {
      const info = await db.prepare("INSERT INTO sync_log (sync_type, status, triggered_by) VALUES (?,?,?)").run('xerp_inventory', 'running', triggeredBy);
      syncLogId = info.lastInsertRowid;
      console.log('[sync] 시작 sync_log_id=' + syncLogId + ' (trigger=' + triggeredBy + ')');
    } catch (e) {
      // ★ 자가복구: sync_log INSERT 실패 시 원인 불문하고 현장에서 테이블/컬럼 재생성 + INSERT 재시도.
      //   이전에는 startup CREATE TABLE 이 권한 문제로 실패했거나 구 스키마였을 때 영구적으로 snapshot_disabled 로 떨어져
      //   "live 리프레시 중" 루프가 됐음. 이제는 요청 경로 안에서 스스로 고치므로 한 사이클 안에 정상화.
      console.warn('[sync] sync_log INSERT 1차 실패 원문:', e.message);
      console.warn('[sync] 자가복구 시도 — 테이블 재생성 + 누락 컬럼 보강');
      try {
        // 1) 테이블 자체가 없으면 생성
        await db.exec(`CREATE TABLE IF NOT EXISTS sync_log (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          sync_type     TEXT NOT NULL,
          started_at    TEXT DEFAULT (datetime('now','localtime')),
          finished_at   TEXT DEFAULT '',
          success_count INTEGER DEFAULT 0,
          fail_count    INTEGER DEFAULT 0,
          status        TEXT DEFAULT 'running',
          error_msg     TEXT DEFAULT '',
          triggered_by  TEXT DEFAULT 'manual'
        )`);
        // 2) 구 스키마일 경우 누락 컬럼 보강 (IF NOT EXISTS — 각각 개별 try)
        const _fix = [
          ['triggered_by',  "TEXT DEFAULT 'manual'"],
          ['error_msg',     "TEXT DEFAULT ''"],
          ['fail_count',    "INTEGER DEFAULT 0"],
          ['success_count', "INTEGER DEFAULT 0"],
          ['finished_at',   "TEXT DEFAULT ''"],
          ['status',        "TEXT DEFAULT 'running'"]
        ];
        for (const [col, type] of _fix) {
          try { await db.exec(`ALTER TABLE sync_log ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(_) {}
        }
        // 3) 재시도
        const info2 = await db.prepare("INSERT INTO sync_log (sync_type, status, triggered_by) VALUES (?,?,?)").run('xerp_inventory', 'running', triggeredBy);
        syncLogId = info2.lastInsertRowid;
        console.log('[sync] 자가복구 성공 — sync_log_id=' + syncLogId);
      } catch (e2) {
        // 자가복구도 실패 → 진짜 권한 문제. 500 반환해 관리자 개입 필요 명시.
        console.error('[sync] sync_log 자가복구 실패:', e2.message);
        fail(res, 500, 'sync_log 기록 실패 (자가복구도 실패): ' + e2.message + ' — PG 관리자에게 CREATE TABLE 권한 요청 필요');
        return;
      }
    }

    // 즉시 202 반환
    try {
      res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ok: true,
        data: {
          sync_log_id: syncLogId,
          status: snapshotDisabled ? 'started-live-only' : 'started',
          triggered_by: triggeredBy,
          snapshot_disabled: snapshotDisabled,
          hint: snapshotDisabled ? 'snapshot 테이블 미존재 — live 리프레시만 수행. 관리자에게 CREATE TABLE + GRANT 요청 필요' : undefined
        }
      }));
    } catch (_) {}

    // 백그라운드 실행 (fetchCompanyInventory 는 이미 chunk+timeout 적용돼 있음)
    (async () => {
      const t0 = Date.now();
      if (snapshotDisabled) { inMemorySyncState = { running: true, done_at: null, count: 0, error: null }; }
      try {
        await ensureXerpPool().catch(()=>null);

        // 내부 fetchCompanyInventory 가 /api/xerp-inventory 핸들러 스코프에 있어서
        // 여기서는 직접 호출 불가. 대신 live 경로를 self-call.
        // ★ 명시적 5분 timeout — 없으면 XERP 지연 시 프로세스가 영구 매달려 sync_log 가 running 에 고정됨.
        const ctrl = new AbortController();
        const timeoutHandle = setTimeout(() => ctrl.abort(new Error('sync bg timeout 5m')), 5 * 60 * 1000);
        const selfBase = 'http://127.0.0.1:' + PORT;
        const r = await fetch(selfBase + '/api/xerp-inventory?refresh=1&company=all', { signal: ctrl.signal }).catch(e=>({__err:e}));
        clearTimeout(timeoutHandle);
        if (r.__err) throw r.__err;
        const j = await r.json();
        if (!j.ok) throw new Error('xerp-inventory live 호출 실패: ' + (j.error || ''));
        // 응답 포맷: { products: [...], updated, count, source }
        const rows = (j.data && Array.isArray(j.data.products)) ? j.data.products : (Array.isArray(j.data) ? j.data : []);

        // 전부 fallback:products 면 실패 처리 (기존 snapshot 보존)
        const allFallback = rows.length > 0 && rows.every(p => p['_invSource'] === 'fallback:products');
        if (allFallback || rows.length === 0) {
          console.warn('[sync bg] 모든 데이터 fallback/empty — snapshot 갱신 스킵');
          if (!snapshotDisabled) {
            try { await db.prepare("UPDATE sync_log SET status='failed', error_msg=?, finished_at=datetime('now','localtime') WHERE id=?").run('XERP 전체 실패 — 기존 snapshot 유지', syncLogId); } catch(_) {}
          }
          return;
        }

        // Snapshot 비활성 모드(테이블 없음) — UPSERT 스킵, live 캐시 리프레시만 수행한 셈
        if (snapshotDisabled) {
          inMemorySyncState = { running: false, done_at: new Date().toISOString(), count: rows.length, error: null };
          console.log(`[sync bg] snapshot 비활성 모드 — live 리프레시만 수행 (${rows.length}개 받음, 테이블 없어 UPSERT 스킵)`);
          return;
        }

        // ★ 자가복구: inventory_snapshot 테이블/컬럼 선제 보장 — UPSERT 전에 한 번 확인.
        try {
          await db.exec(`CREATE TABLE IF NOT EXISTS inventory_snapshot (
            product_code   TEXT PRIMARY KEY,
            legal_entity   TEXT DEFAULT 'barunson',
            site_code      TEXT DEFAULT 'BK10',
            current_stock  INTEGER DEFAULT 0,
            monthly_out    INTEGER DEFAULT 0,
            daily_out      INTEGER DEFAULT 0,
            total_3m       INTEGER DEFAULT 0,
            item_name      TEXT DEFAULT '',
            synced_at      TEXT DEFAULT (datetime('now','localtime'))
          )`);
          for (const [col, type] of [['legal_entity',"TEXT DEFAULT 'barunson'"],['site_code',"TEXT DEFAULT 'BK10'"],['monthly_out',"INTEGER DEFAULT 0"],['daily_out',"INTEGER DEFAULT 0"],['total_3m',"INTEGER DEFAULT 0"],['item_name',"TEXT DEFAULT ''"],['synced_at',"TEXT DEFAULT ''"]]) {
            try { await db.exec(`ALTER TABLE inventory_snapshot ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(_) {}
          }
        } catch(_) {}

        // UPSERT — 전체를 하나의 트랜잭션으로 감싸서 fsync 1회로 줄임 (이전: 900건 × 개별 autocommit).
        // ★ 추가로 multi-row VALUES 로 배치화 (BATCH_SIZE=200). 이전엔 .run() 을 900번 호출 → PG 어댑터 사용 시
        //   매 row 마다 네트워크 왕복 발생. 이제 ~5회 왕복으로 축소 → 10~50배 가속 기대.
        //   배치 실패 시 해당 배치만 row-by-row 폴백하여 문제 품목 코드 식별 (기존 fail 로그 포맷 유지).
        let successCount = 0;
        let failCount = 0;
        const failedCodes = [];
        const BATCH_SIZE = 200;
        // ★ warehouses_json 추가 (placeholder 1개 늘어 9개 → 10개 + datetime).
        const rowPh = "(?,?,?,?,?,?,?,?,?,datetime('now','localtime'))";
        const onConflict = `ON CONFLICT(product_code) DO UPDATE SET
              legal_entity=excluded.legal_entity,
              site_code=excluded.site_code,
              current_stock=excluded.current_stock,
              monthly_out=excluded.monthly_out,
              daily_out=excluded.daily_out,
              total_3m=excluded.total_3m,
              item_name=CASE WHEN excluded.item_name='' THEN inventory_snapshot.item_name ELSE excluded.item_name END,
              warehouses_json=excluded.warehouses_json,
              synced_at=excluded.synced_at`;
        const rowParams = p => [
          p['제품코드'], p['legal_entity'], p['_siteCode'],
          p['현재고'] || 0, p['_xerpMonthly'] || 0, p['_xerpDaily'] || 0, p['_xerpTotal3m'] || 0,
          p['품목명'] || '',
          p['_warehouses'] ? JSON.stringify(p['_warehouses']) : ''
        ];
        const runUpsert = db.transaction(async () => {
          // 폴백용 단건 UPSERT (배치 실패 시 어느 row 가 원인인지 식별)
          const singleUpsert = db.prepare(`INSERT INTO inventory_snapshot
            (product_code, legal_entity, site_code, current_stock, monthly_out, daily_out, total_3m, item_name, warehouses_json, synced_at)
            VALUES ${rowPh}
            ${onConflict}`);

          for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const placeholders = batch.map(() => rowPh).join(',');
            const batchSql = `INSERT INTO inventory_snapshot
              (product_code, legal_entity, site_code, current_stock, monthly_out, daily_out, total_3m, item_name, warehouses_json, synced_at)
              VALUES ${placeholders}
              ${onConflict}`;
            const flatParams = batch.flatMap(rowParams);
            try {
              await db.prepare(batchSql).run(...flatParams);
              successCount += batch.length;
            } catch (batchErr) {
              // 배치 전체 실패 → row-by-row 폴백 (잘못된 품목만 fail 로 집계)
              console.warn(`[sync bg] batch ${i}..${i+batch.length-1} 실패, row-by-row 재시도:`, batchErr.message.split('\n')[0]);
              for (const p of batch) {
                try {
                  await singleUpsert.run(...rowParams(p));
                  successCount++;
                } catch (e) {
                  failCount++;
                  if (failedCodes.length < 10) failedCodes.push(p['제품코드'] + ':' + e.message.split('\n')[0]);
                }
              }
            }
          }
        });
        await runUpsert();

        // 캐시 무효화 — snapshot 이 갱신됐으니 10분 TTL 캐시도 폐기해야 새 데이터 보임.
        // 이전엔 동기화 눌러도 최대 10분간 구 데이터 유지되는 버그 원인이었음.
        try { if (typeof xerpInventoryCaches === 'object' && xerpInventoryCaches) for (const k in xerpInventoryCaches) delete xerpInventoryCaches[k]; } catch(_) {}

        // ── 규격 1회성 동기화 ──
        // products.spec 이 비어있는 품목만 XERP mmInoutItem.ItemSpec 최신값으로 채움.
        // 이미 있으면 skip (수동 편집 보존). 첫 동기화 때만 무거움, 이후 자동으로 no-op 에 수렴.
        let specSyncedCount = 0;
        try {
          const needSpec = await db.prepare("SELECT product_code FROM products WHERE (spec IS NULL OR spec = '') AND status IN ('active','inactive')").all();
          const validSpecCodes = (needSpec || []).map(r => (r.product_code || '').replace(/[\s ​‌‍﻿]/g, '').trim()).filter(c => /^[A-Za-z0-9_\-]+$/.test(c));
          if (validSpecCodes.length > 0 && xerpPool) {
            const SPEC_CHUNK = 50;
            const SPEC_CONCURRENT = 3;
            const specMap = {};
            const chunks = [];
            for (let i = 0; i < validSpecCodes.length; i += SPEC_CHUNK) chunks.push(validSpecCodes.slice(i, i + SPEC_CHUNK));
            const fetchSpecChunk = async (chunk, idx) => {
              const inClause = chunk.map(c => `'${c}'`).join(',');
              try {
                const reqQ = xerpPool.request();
                reqQ.timeout = 10000;
                const r = await reqQ.query(`
                  SELECT item_code, item_spec FROM (
                    SELECT RTRIM(ItemCode) AS item_code,
                           RTRIM(ItemSpec) AS item_spec,
                           ROW_NUMBER() OVER (PARTITION BY RTRIM(ItemCode) ORDER BY InoutDate DESC, InoutSerNo DESC) AS rn
                    FROM mmInoutItem WITH (NOLOCK)
                    WHERE SiteCode = 'BK10' AND RTRIM(ItemCode) IN (${inClause})
                  ) t
                  WHERE t.rn = 1 AND t.item_spec <> ''
                `);
                for (const row of (r.recordset || [])) {
                  const c = (row.item_code || '').trim();
                  const sp = (row.item_spec || '').trim();
                  if (c && sp) specMap[c] = sp;
                }
              } catch (e) {
                console.warn(`[sync bg] 규격 chunk ${idx+1}/${chunks.length} 실패:`, e.message);
              }
            };
            for (let i = 0; i < chunks.length; i += SPEC_CONCURRENT) {
              const wave = chunks.slice(i, i + SPEC_CONCURRENT);
              await Promise.all(wave.map((ch, j) => fetchSpecChunk(ch, i + j)));
            }
            // UPDATE only where spec is still empty (경합 방지 — 동기화 중 사용자가 수동 입력했을 수도)
            if (Object.keys(specMap).length > 0) {
              const updSpec = db.prepare("UPDATE products SET spec = ? WHERE product_code = ? AND (spec IS NULL OR spec = '')");
              const applyTx = db.transaction(async () => {
                for (const [code, spec] of Object.entries(specMap)) {
                  try {
                    const info = await updSpec.run(spec, code);
                    if (info && info.changes > 0) specSyncedCount++;
                  } catch (_) {}
                }
              });
              await applyTx();
            }
            console.log(`[sync bg] 규격 동기화: ${specSyncedCount}개 품목 채움 (후보 ${validSpecCodes.length}개 중 XERP 발견 ${Object.keys(specMap).length}개)`);
          }
        } catch (specErr) {
          console.warn('[sync bg] 규격 동기화 예외:', specErr.message);
        }

        try {
          await db.prepare("UPDATE sync_log SET status=?, success_count=?, fail_count=?, error_msg=?, finished_at=datetime('now','localtime') WHERE id=?")
            .run(failCount > 0 && successCount === 0 ? 'failed' : 'success', successCount, failCount, failCount > 0 ? ('UPSERT 실패 ' + failCount + '건: ' + failedCodes.join(' | ')).slice(0, 500) : '', syncLogId);
        } catch (_) {}
        console.log(`[sync bg] snapshot 갱신 완료: ${successCount}/${rows.length}개 저장 (실패 ${failCount}, 규격 ${specSyncedCount}개 추가, 총 ${((Date.now()-t0)/1000).toFixed(1)}s)`);
      } catch (e) {
        console.error('[sync bg] 실패:', e.message);
        if (snapshotDisabled) {
          inMemorySyncState = { running: false, done_at: new Date().toISOString(), count: 0, error: e.message };
        } else {
          try {
            await db.prepare("UPDATE sync_log SET status='failed', error_msg=?, finished_at=datetime('now','localtime') WHERE id=?").run(e.message.slice(0, 500), syncLogId);
          } catch (_) {}
        }
      }
    })().catch(e => console.error('[sync bg] IIFE reject:', e?.message || e));
    return;
  }

  if (pathname === '/api/sync/status' && method === 'GET') {
    let lastSuccess = null, running = null, lastFailed = null, snapCount = 0;
    try { lastSuccess = await db.prepare("SELECT started_at, finished_at, success_count, triggered_by FROM sync_log WHERE sync_type='xerp_inventory' AND status='success' ORDER BY id DESC LIMIT 1").get(); } catch(_) {}
    try { running = await db.prepare("SELECT started_at, triggered_by FROM sync_log WHERE sync_type='xerp_inventory' AND status='running' ORDER BY id DESC LIMIT 1").get(); } catch(_) {}
    try { lastFailed = await db.prepare("SELECT started_at, finished_at, error_msg, triggered_by FROM sync_log WHERE sync_type='xerp_inventory' AND status='failed' ORDER BY id DESC LIMIT 1").get(); } catch(_) {}
    try { const c = await db.prepare("SELECT COUNT(*) AS cnt FROM inventory_snapshot").get(); snapCount = c?.cnt || 0; } catch(_) {}
    ok(res, {
      is_running: !!running,
      running_since: running?.started_at || null,
      last_success_at: lastSuccess?.finished_at || null,
      last_success_count: lastSuccess?.success_count || 0,
      last_success_by: lastSuccess?.triggered_by || null,
      last_failed_at: lastFailed?.finished_at || null,
      last_failed_error: lastFailed?.error_msg || null,
      snapshot_count: snapCount,
      // in-memory 상태 (snapshot 테이블 없을 때 대체 경로에서 사용)
      in_memory_running: !!inMemorySyncState.running,
      in_memory_done: !!inMemorySyncState.done_at,
      in_memory_done_at: inMemorySyncState.done_at,
      in_memory_count: inMemorySyncState.count || 0,
      in_memory_error: inMemorySyncState.error || null
    });
    return;
  }

  if (pathname === '/api/sync/xerp-inventory/reset' && method === 'POST') {
    try {
      const info = await db.prepare("UPDATE sync_log SET status='failed', error_msg='수동 리셋', finished_at=datetime('now','localtime') WHERE sync_type='xerp_inventory' AND status='running'").run();
      ok(res, { reset_count: info?.changes || 0 });
    } catch (e) { fail(res, 500, '리셋 실패: ' + e.message); }
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
      const registeredProducts = await db.prepare("SELECT product_code FROM products WHERE status = 'active'").all();
      // ★ product_code 공백/보이지않는문자 정제 (XERP 매칭 성공률 유지)
      for (const p of registeredProducts) {
        p.product_code = (p.product_code || '').replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();
      }
      const registeredCodes = new Set(registeredProducts.map(r => r.product_code));
      const validCodes = registeredProducts.map(p => p.product_code).filter(c => /^[A-Za-z0-9_\-]+$/.test(c));
      if (!validCodes.length) { ok(res, {}); return; }

      // ★ readonly 10초 타임아웃 대응: 50개씩 chunk + req.timeout=7000
      // ★ 병렬화: 이전엔 순차 await 로 chunk 수만큼 누적 대기 (N*평균지연). 이제 MAX_CONCURRENT 웨이브로 묶어 처리.
      //   XERP pool max=5 이므로 동시 3으로 제한 → 다른 요청용 커넥션 여유 2개 확보. 평균 3~5배 가속.
      const CHUNK_SIZE = 50;
      const MAX_CONCURRENT = 3;
      const codeChunks = [];
      for (let i = 0; i < validCodes.length; i += CHUNK_SIZE) codeChunks.push(validCodes.slice(i, i + CHUNK_SIZE));

      const usage = {};
      let okCount = 0, failCount = 0;

      const processChunk = async (chunk, idx) => {
        const inClause = chunk.map(c => `'${c}'`).join(',');
        try {
          const reqQ = xerpPool.request()
            .input('start3m', sql.NChar(16), fmt(start3m))
            .input('today', sql.NChar(16), fmt(today));
          reqQ.timeout = 7000;
          const r = await reqQ.query(`
            SELECT RTRIM(ItemCode) AS item_code, SUM(InoutQty) AS total_qty, COUNT(DISTINCT RTRIM(InoutDate)) AS ship_days
            FROM mmInoutItem WITH (NOLOCK)
            WHERE SiteCode = 'BK10' AND InoutGubun = 'SO'
              AND InoutDate >= @start3m AND InoutDate < @today
              AND RTRIM(ItemCode) IN (${inClause})
            GROUP BY RTRIM(ItemCode)
          `);
          for (const row of r.recordset) {
            const code = (row.item_code || '').trim();
            if (!code || !registeredCodes.has(code)) continue;
            const total = Math.round(row.total_qty || 0);
            usage[code] = { total, monthly: Math.round(total / 3), daily: Math.round(total / 90) };
          }
          okCount++;
        } catch (e) {
          failCount++;
          console.warn(`[xerp-monthly] chunk ${idx+1}/${codeChunks.length} 실패:`, e.message);
        }
      };

      // 웨이브 단위 병렬 처리 — Promise.all 로 MAX_CONCURRENT 개씩 동시에 대기
      for (let i = 0; i < codeChunks.length; i += MAX_CONCURRENT) {
        const wave = codeChunks.slice(i, i + MAX_CONCURRENT);
        await Promise.all(wave.map((chunk, j) => processChunk(chunk, i + j)));
      }
      console.log(`XERP 월출고: ${okCount}/${codeChunks.length} chunk 성공 (실패 ${failCount}), ${Object.keys(usage).length}개 품목`);

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
        const bar1Pool = new sql.ConnectionPool(barShopConfig);
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
  //  수불원장 (Inventory Ledger)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/xerp-inventory-ledger — 수불원장 (품목별 입출고 + 재고)
  if (pathname === '/api/xerp-inventory-ledger' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }

    const WH_NAMES = {
      MF01:'본사공장', MF02:'공장2', MF03:'공장부속', MF15:'공장보조',
      MF21:'공장21', MF23:'후가공', MF24:'완제품', MT01:'자재창고',
      MT04:'자재보조', MT09:'자재09', M006:'외주06', M011:'외주11', W062:'외부창고'
    };

    try {
      const qFrom = parsed.searchParams.get('from');
      const qTo = parsed.searchParams.get('to');
      const qWarehouse = parsed.searchParams.get('warehouse');
      const qItemCode = parsed.searchParams.get('item_code');

      const today = new Date();
      const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
      const def = new Date(today); def.setMonth(def.getMonth() - 3); def.setDate(1);
      let startStr = qFrom || fmt(def);
      let endStr = qTo || fmt(today);
      const endNext = new Date(parseInt(endStr.slice(0,4)), parseInt(endStr.slice(4,6))-1, parseInt(endStr.slice(6,8))+1);
      const endNextStr = fmt(endNext);

      // 1) 기간 입출고 + 품목명 (단일 쿼리로 최적화)
      // 품목명: 기간 데이터에서 MAX(ItemName)으로 가져오되, 빈 것은 나중에 보충
      // 2) 현재고 (mmInventory — 76K건, 빠름)
      // ★ 양수 OhQty 만 SUM — XERP 스마트재고현황과 일치 (음수 예약/조정 lot 제외)
      const invResult = await xerpPool.request().query(`
        SELECT RTRIM(ItemCode) AS ic, RTRIM(WhCode) AS wh,
               SUM(CASE WHEN ISNULL(OhQty,0) > 0 THEN OhQty ELSE 0 END) AS stock_qty
        FROM mmInventory WITH (NOLOCK)
        WHERE SiteCode = 'BK10'
        GROUP BY RTRIM(ItemCode), RTRIM(WhCode)
      `);
      const stockMap = {};
      // 품목명도 mmInventory에서 안 나오므로, 나중에 결과에서 빈 이름만 보충 쿼리
      for (const r of invResult.recordset) {
        const key = `${(r.ic||'').trim()}|${(r.wh||'').trim()}`;
        stockMap[key] = r.stock_qty || 0;
      }

      // 3) 창고 목록
      const whResult = await xerpPool.request().query(`
        SELECT DISTINCT RTRIM(WhCode) AS wh_code
        FROM mmInoutItem WITH (NOLOCK)
        WHERE SiteCode = 'BK10' AND WhCode IS NOT NULL AND RTRIM(WhCode) <> ''
        ORDER BY RTRIM(WhCode)
      `);
      const warehouses = whResult.recordset.map(r => ({
        code: r.wh_code, name: WH_NAMES[r.wh_code] || r.wh_code
      }));

      // 4) 기간 입출고 — 품목×창고별 집계
      const req = xerpPool.request()
        .input('startDate', sql.NChar(16), startStr)
        .input('endDate', sql.NChar(16), endNextStr);
      let whereExtra = '';
      if (qWarehouse) { req.input('whFilter', sql.VarChar(20), qWarehouse); whereExtra += ' AND RTRIM(WhCode) = @whFilter'; }
      if (qItemCode) { req.input('itemFilter', sql.VarChar(60), '%' + qItemCode + '%'); whereExtra += ' AND (RTRIM(ItemCode) LIKE @itemFilter OR RTRIM(ItemName) LIKE @itemFilter)'; }

      const result = await req.query(`
        SELECT RTRIM(ItemCode) AS item_code,
               MAX(RTRIM(ItemName)) AS item_name,
               RTRIM(WhCode) AS warehouse,
               SUM(CASE WHEN InoutGubun IN ('SI','MI') THEN InoutQty ELSE 0 END) AS in_qty,
               SUM(CASE WHEN InoutGubun IN ('SO','MO') THEN InoutQty ELSE 0 END) AS out_qty,
               SUM(CASE WHEN InoutGubun IN ('SI','MI') THEN InoutAmnt ELSE 0 END) AS in_amnt,
               SUM(CASE WHEN InoutGubun IN ('SO','MO') THEN InoutAmnt ELSE 0 END) AS out_amnt
        FROM mmInoutItem WITH (NOLOCK)
        WHERE SiteCode = 'BK10'
          AND InoutDate >= @startDate AND InoutDate < @endDate
          ${whereExtra}
        GROUP BY RTRIM(ItemCode), RTRIM(WhCode)
        ORDER BY RTRIM(ItemCode), RTRIM(WhCode)
      `);

      // 5) 품목명: XERP 캐시 + 로컬 products 테이블에서 보충
      const nameMap = { ...xerpItemNameCache };
      try {
        const localProds = await db.prepare('SELECT product_code, product_name, material_name, brand FROM products').all();
        for (const p of localProds) {
          if (!p.product_code || nameMap[p.product_code]) continue;
          const nm = (p.product_name||'').trim() || (p.material_name||'').trim() || (p.brand||'').trim();
          if (nm) nameMap[p.product_code] = nm;
        }
      } catch(e) { /* 로컬 DB 없으면 무시 */ }

      // 6) 행 구성: 품목×창고 + 품목명 + 현재고 + 기초재고 계산
      const rows = result.recordset.map(r => {
        const code = (r.item_code||'').trim();
        const wh = (r.warehouse||'').trim();
        const inQty = r.in_qty || 0;
        const outQty = r.out_qty || 0;
        const curStock = stockMap[`${code}|${wh}`] || 0;
        const beginStock = curStock - inQty + outQty;
        const name = (r.item_name||'').trim() || nameMap[code] || '';
        return {
          item_code: code,
          item_name: name,
          warehouse: wh,
          wh_name: WH_NAMES[wh] || wh,
          begin_stock: beginStock,
          in_qty: inQty,
          out_qty: outQty,
          end_stock: curStock,
          in_amnt: r.in_amnt || 0,
          out_amnt: r.out_amnt || 0
        };
      });

      const totals = rows.reduce((acc, r) => {
        acc.in_qty += r.in_qty; acc.out_qty += r.out_qty;
        acc.in_amnt += r.in_amnt; acc.out_amnt += r.out_amnt;
        return acc;
      }, { in_qty:0, out_qty:0, in_amnt:0, out_amnt:0 });

      ok(res, { rows, warehouses, totals, range: { start: startStr, end: endStr } });
    } catch (e) {
      console.error('수불원장 조회 오류:', e.message);
      fail(res, 500, '수불원장 조회 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  AUTO-ORDER (필수 자동발주)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/china-price-tiers — 전체 단가 조회
  if (pathname === '/api/china-price-tiers' && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM china_price_tiers ORDER BY product_code, qty_tier').all();
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
    const rows = await db.prepare('SELECT * FROM china_price_tiers WHERE product_code=? ORDER BY qty_tier').all(code);
    if (!rows.length) {
      // 대소문자 무시 재시도
      const rows2 = await db.prepare('SELECT * FROM china_price_tiers WHERE UPPER(product_code)=UPPER(?) ORDER BY qty_tier').all(code);
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
    const tx = db.transaction(async () => {
      let cnt = 0;
      for (const item of items) {
        await insert.run(item.product_code, item.product_type || 'Card', item.qty_tier, item.unit_price, item.currency || 'KRW', item.effective_date || '2025-05-01');
        cnt++;
      }
      return cnt;
    });
    const count = await tx();
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

    const tiers = await db.prepare('SELECT qty_tier, unit_price FROM china_price_tiers WHERE UPPER(product_code)=UPPER(?) ORDER BY qty_tier').all(code);
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
    const rows = await db.prepare(`SELECT a.*, COALESCE(p.origin,'') as origin FROM auto_order_items a LEFT JOIN products p ON a.product_code=p.product_code ORDER BY a.id`).all();
    ok(res, rows);
    return;
  }

  // GET /api/auto-order/search?q=... — 품목 검색 (XERP 재고+products DB)
  if (pathname === '/api/auto-order/search' && method === 'GET') {
    const q = (parsed.searchParams.get('q') || '').trim();
    if (!q) { ok(res, []); return; }
    // products DB에서 검색
    const dbRows = await db.prepare(`SELECT product_code, product_name, brand, origin FROM products WHERE product_code LIKE ? OR product_name LIKE ? LIMIT 20`).all(`%${q}%`, `%${q}%`);
    // 이미 등록된 품목 체크
    const existingCodes = new Set((await db.prepare('SELECT product_code FROM auto_order_items').all()).map(r => r.product_code));
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
      const prod = await db.prepare('SELECT origin FROM products WHERE product_code=?').get(b.product_code);
      if (prod) origin = prod.origin || '';
    }
    if (!origin && xerpInventoryCache && xerpInventoryCache.products) {
      const xp = xerpInventoryCache.products.find(p => (p['제품코드']||'') === b.product_code);
      if (xp) origin = xp['생산지'] || '';
    }
    if (!origin) origin = '한국'; // 기본값
    // products 테이블에 origin 없으면 업데이트
    const existProd = await db.prepare('SELECT id, origin FROM products WHERE product_code=?').get(b.product_code);
    if (existProd && !existProd.origin) {
      await db.prepare('UPDATE products SET origin=? WHERE id=?').run(origin, existProd.id);
    }
    try {
      const info = await db.prepare('INSERT INTO auto_order_items (product_code, min_stock, order_qty, vendor_name) VALUES (?,?,?,?)').run(
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
    const existing = await db.prepare('SELECT * FROM auto_order_items WHERE id=?').get(id);
    if (!existing) { fail(res, 404, 'not found'); return; }
    await db.prepare('UPDATE auto_order_items SET min_stock=?, order_qty=?, vendor_name=?, enabled=? WHERE id=?').run(
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
    await db.prepare('DELETE FROM auto_order_items WHERE id=?').run(id);
    ok(res, { deleted: true });
    return;
  }

  // POST /api/auto-order/bulk-add — 재고현황에서 특정 생산지 품목 일괄 추가
  if (pathname === '/api/auto-order/bulk-add' && method === 'POST') {
    const b = await readJSON(req);
    const origin = b.origin || '중국';
    // products DB에서 해당 origin 품목 가져오기
    const prodsByOrigin = await db.prepare('SELECT product_code FROM products WHERE origin=?').all(origin);
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

    const existing = new Set((await db.prepare('SELECT product_code FROM auto_order_items').all()).map(r => r.product_code));
    const insert = db.prepare('INSERT OR IGNORE INTO auto_order_items (product_code, min_stock, order_qty, vendor_name, enabled) VALUES (?,?,?,?,1)');
    let added = 0, skipped = 0;
    const tx = db.transaction(async () => {
      for (const code of targetCodes) {
        if (!code) continue;
        if (existing.has(code)) { skipped++; continue; }
        await insert.run(code, 0, 0, '');
        added++;
      }
    });
    await tx();
    ok(res, { added, skipped, origin, total: targetCodes.size });
    return;
  }

  // ── 전략발주 최적수량 계산 공통 함수 ──
  async function calculateOptimalOrder(productCode, invData, origin) {
    const avail = typeof invData['가용재고'] === 'number' ? invData['가용재고'] : 0;
    const daily = invData['_xerpDaily'] || 0;
    const monthly = invData['_xerpMonthly'] || (invData._xerpTotal3m ? Math.round(invData._xerpTotal3m / 3) : 0);
    if (monthly <= 0) return { skip: true, reason: '월출고량 없음' };

    // 1. 리드타임 결정: 품목별 > 생산지별 기본값 (중국 50일)
    const prod = await db.prepare('SELECT lead_time_days FROM products WHERE product_code=?').get(productCode);
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
    const tiers = await db.prepare('SELECT qty_tier, unit_price FROM china_price_tiers WHERE UPPER(product_code)=UPPER(?) ORDER BY qty_tier').all(productCode);
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
    const items = await db.prepare('SELECT * FROM auto_order_items WHERE enabled=1').all();
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
      if (!isDanger) { skipped.push({ product_code: item.product_code, reason: `안전 (잔여 ${Math.round(remainDays)}일, 목표재고 ${(calc.targetStock||0).toLocaleString()})` }); continue; }

      // 거래처별 주간 6건 제한 (긴급은 한도 무시)
      const vendor = item.vendor_name || '';
      if (vendor && !isUrgent) {
        if (!(vendor in weeklyVendorCount)) {
          weeklyVendorCount[vendor] = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date>=? AND status!='cancelled' AND status!='취소'`).get(vendor, mondayStr)).cnt;
        }
        if (weeklyVendorCount[vendor] >= 6) {
          skipped.push({ product_code: item.product_code, reason: `${vendor} 주간 한도 초과 (${weeklyVendorCount[vendor]}/6건)` });
          continue;
        }
      }

      // 미완료 PO가 있는 품목 스킵 (중복발주 방지)
      const pendingPO = await db.prepare(`
        SELECT h.po_number, h.status FROM po_header h
        JOIN po_items i ON i.po_id = h.po_id
        WHERE i.product_code = ? AND h.status IN ('draft','sent','confirmed','partial','os_pending',
          'draft','발송','확인','수령중','OS등록대기')
        LIMIT 1
      `).get(item.product_code);
      if (pendingPO) { skipped.push({ product_code: item.product_code, reason: `미완료 PO (${pendingPO.po_number})` }); continue; }
      // 입고완료 but XERP 미동기화 (OS번호 미등록) → 스킵
      const receivedNotSynced = await db.prepare(`
        SELECT h.po_number, SUM(COALESCE(i.received_qty,0)) as recv_qty FROM po_header h
        JOIN po_items i ON i.po_id = h.po_id
        WHERE i.product_code = ? AND h.status = 'received'
          AND (h.os_number IS NULL OR h.os_number = '')
        GROUP BY h.po_number LIMIT 1
      `).get(item.product_code);
      if (receivedNotSynced) { skipped.push({ product_code: item.product_code, reason: `입고완료 XERP미동기화 (${receivedNotSynced.po_number}, ${receivedNotSynced.recv_qty}개)` }); continue; }
      // 거래처 결정: auto_order_items.vendor_name > products.paper_maker 매핑
      let resolvedVendor = vendor;
      if (!resolvedVendor) {
        const prodInfo = await db.prepare('SELECT paper_maker FROM products WHERE product_code=?').get(item.product_code);
        if (prodInfo && prodInfo.paper_maker) {
          resolvedVendor = resolveVendor(prodInfo.paper_maker) || '';
        }
      }

      // PO 생성
      const poNumber = await generatePoNumber();
      // origin 결정
      const _aoOriginProd = await db.prepare(`SELECT ${_hasEntity.products ? 'origin, legal_entity' : 'origin'} FROM products WHERE product_code=?`).get(item.product_code);
      const _aoOrigin = (_aoOriginProd && _aoOriginProd.origin) || '한국';
      const _aoEntity = (_aoOriginProd && _aoOriginProd.legal_entity === 'dd') ? 'dd' : 'barunson';
      const tx = db.transaction(async () => {
        const hdr = _hasEntity.po_header
          ? await db.prepare('INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, origin, legal_entity, po_date) VALUES (?,?,?,?,?,?,?,?,date(\'now\',\'localtime\'))').run(
              poNumber, '자동발주', resolvedVendor, 'draft', orderQty, '필수 자동발주', _aoOrigin, _aoEntity)
          : await db.prepare('INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, origin, po_date) VALUES (?,?,?,?,?,?,?,date(\'now\',\'localtime\'))').run(
              poNumber, '자동발주', resolvedVendor, 'draft', orderQty, '필수 자동발주', _aoOrigin);
        await db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)').run(
          hdr.lastInsertRowid, item.product_code, p['브랜드'] || '', '', orderQty, '', '자동발주'
        );
        await db.prepare('UPDATE auto_order_items SET last_ordered_at=? WHERE id=?').run(new Date().toISOString(), item.id);
        // auto_order_items에 vendor_name도 업데이트 (다음번부터 사용)
        if (resolvedVendor && !vendor) {
          await db.prepare('UPDATE auto_order_items SET vendor_name=? WHERE id=?').run(resolvedVendor, item.id);
        }
        return { po_id: hdr.lastInsertRowid, po_number: poNumber };
      });
      const result = await tx();
      if (resolvedVendor) weeklyVendorCount[resolvedVendor] = (weeklyVendorCount[resolvedVendor] || 0) + 1;

      // 거래처 이메일이 있으면 자동 발송
      let emailSent = false;
      if (resolvedVendor) {
        const vendorInfo = await db.prepare('SELECT * FROM vendors WHERE name=?').get(resolvedVendor);
        if (vendorInfo && vendorInfo.email) {
          try {
            const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(result.po_id);
            const poItems = await db.prepare('SELECT * FROM po_items WHERE po_id=?').all(result.po_id);
            await sendPOEmail(po, poItems, vendorInfo.email, vendorInfo.name, false, vendorInfo.email_cc || '');
            await db.prepare("UPDATE po_header SET status='sent' WHERE po_id=?").run(result.po_id);
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
    const rows = await db.prepare('SELECT * FROM china_shipment_log ORDER BY created_at DESC LIMIT 50').all();
    ok(res, rows);
    return;
  }

  // GET /api/china-shipment/logs/:id — 특정 선적 상세
  const csDetailMatch = pathname.match(/^\/api\/china-shipment\/logs\/(\d+)$/);
  if (csDetailMatch && method === 'GET') {
    const row = await db.prepare('SELECT * FROM china_shipment_log WHERE id=?').get(csDetailMatch[1]);
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
    const result = await db.prepare(`INSERT INTO china_shipment_log (shipment_date, file_name, total_boxes, total_items, target_boxes, items_json, notes, status, bl_number, ship_date, eta_date) VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(
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
    await db.prepare('UPDATE china_shipment_log SET status=? WHERE id=?').run(body.status, csStatusMatch[1]);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/china-shipment/logs/:id — 선적 이력 삭제
  const csDelMatch = pathname.match(/^\/api\/china-shipment\/logs\/(\d+)$/);
  if (csDelMatch && method === 'DELETE') {
    await db.prepare('DELETE FROM china_shipment_log WHERE id=?').run(csDelMatch[1]);
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
      await db.prepare(`UPDATE china_shipment_log SET ${updates.join(',')} WHERE id=?`).run(...params);
    }
    ok(res, { updated: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  통합발주관리 API (한국/중국/더기프트 origin별 워크플로우)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/procurement/dashboard — origin별 파이프라인 요약 (entity 필터 지원)
  if (pathname === '/api/procurement/dashboard' && method === 'GET') {
    const entity = parsed.searchParams.get('entity') || 'all';
    const _useEnt = _hasEntity.po_header && entity && entity !== 'all';
    const entityClause = _useEnt ? ' AND legal_entity=?' : '';
    const entityParams = _useEnt ? [entity] : [];
    // 디디는 더기프트가 없음
    const origins = (entity === 'dd') ? ['한국', '중국'] : ['한국', '중국', '더기프트'];
    const result = {};
    for (const org of origins) {
      const total = await db.prepare(`SELECT COUNT(*) AS c FROM po_header WHERE origin=?${entityClause}`).get(org, ...entityParams);
      const byStatus = await db.prepare(`SELECT status, COUNT(*) AS c FROM po_header WHERE origin=?${entityClause} GROUP BY status`).all(org, ...entityParams);
      const partial = await db.prepare(`SELECT COUNT(*) AS c FROM po_header WHERE origin=? AND status='partial'${entityClause}`).get(org, ...entityParams);
      const overdue = await db.prepare(`SELECT COUNT(*) AS c FROM po_header WHERE origin=? AND status NOT IN ('received','cancelled','completed') AND due_date != '' AND due_date::date < CURRENT_DATE${entityClause}`).get(org, ...entityParams);
      const _recentCols = _hasEntity.po_header
        ? 'po_id, po_number, vendor_name, status, due_date as expected_date, po_date, total_qty, legal_entity'
        : 'po_id, po_number, vendor_name, status, due_date as expected_date, po_date, total_qty';
      const recentPo = await db.prepare(`SELECT ${_recentCols} FROM po_header WHERE origin=?${entityClause} ORDER BY created_at DESC LIMIT 5`).all(org, ...entityParams);
      const rcvRate = await db.prepare(`
        SELECT COALESCE(SUM(i.received_qty),0) AS received, COALESCE(SUM(i.ordered_qty),0) AS ordered
        FROM po_items i JOIN po_header h ON h.po_id=i.po_id WHERE h.origin=? AND h.status NOT IN ('cancelled','draft')${entityClause.replace('legal_entity','h.legal_entity')}
      `).get(org, ...entityParams);
      result[org] = {
        total: Number(total.c),
        by_status: Object.fromEntries(byStatus.map(r => [r.status, Number(r.c)])),
        partial: Number(partial.c),
        overdue: Number(overdue.c),
        receive_rate: rcvRate.ordered > 0 ? Math.round(rcvRate.received / rcvRate.ordered * 100) : 0,
        recent: recentPo
      };
    }
    // 법인별 합계 (컬럼 있을 때만)
    const entityTotals = { barunson: 0, dd: 0 };
    if (_hasEntity.po_header) {
      for (const ent of ['barunson', 'dd']) {
        const r = await db.prepare("SELECT COUNT(*) AS c FROM po_header WHERE legal_entity=?").get(ent);
        entityTotals[ent] = Number(r.c);
      }
    }
    // 중국 선적/더기프트 포장 (바른컴퍼니 전용)
    const shipments = (entity === 'dd') ? [] : await db.prepare("SELECT * FROM china_shipment_log WHERE status NOT IN ('completed','cancelled') ORDER BY eta_date ASC LIMIT 10").all();
    const assemblies = (entity === 'dd') ? [] : await db.prepare("SELECT * FROM gift_assembly WHERE status NOT IN ('completed','cancelled') ORDER BY created_at DESC LIMIT 10").all();
    ok(res, { origins: result, shipments, assemblies, entity_totals: entityTotals, entity }); return;
  }

  // GET /api/procurement/po-receive-status — PO별 아이템 입고현황 (분할입고 추적)
  if (pathname === '/api/procurement/po-receive-status' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const origin = qs.get('origin') || '';
    const status = qs.get('status') || '';
    const entity = qs.get('entity') || '';
    let where = "WHERE 1=1";
    const params = [];
    if (origin) { where += " AND h.origin=?"; params.push(origin); }
    if (entity && entity !== 'all' && _hasEntity.po_header) { where += " AND h.legal_entity=?"; params.push(entity); }
    if (status) { where += " AND h.status=?"; params.push(status); }
    else { where += " AND h.status NOT IN ('cancelled','draft')"; }
    const rows = await db.prepare(`
      SELECT h.po_id, h.po_number, h.origin, h.vendor_name, h.status, h.po_date, h.due_date as expected_date,
             h.po_type, h.material_status, h.process_status, h.process_step, h.notes,
             i.item_id, i.product_code, i.brand, i.process_type, i.ordered_qty, i.received_qty, i.spec
      FROM po_header h JOIN po_items i ON h.po_id = i.po_id ${where}
      ORDER BY h.created_at DESC
    `).all(...params);
    // Group by PO
    const poMap = new Map();
    for (const r of rows) {
      if (!poMap.has(r.po_id)) {
        poMap.set(r.po_id, {
          po_id: r.po_id, po_number: r.po_number, origin: r.origin,
          vendor_name: r.vendor_name, status: r.status, po_date: r.po_date,
          expected_date: r.expected_date, po_type: r.po_type,
          material_status: r.material_status, process_status: r.process_status,
          process_step: r.process_step, notes: r.notes,
          items: [], total_ordered: 0, total_received: 0
        });
      }
      const po = poMap.get(r.po_id);
      po.items.push({ item_id: r.item_id, product_code: r.product_code, brand: r.brand, process_type: r.process_type, ordered_qty: r.ordered_qty, received_qty: r.received_qty, spec: r.spec, progress: r.ordered_qty > 0 ? Math.round(r.received_qty / r.ordered_qty * 100) : 0 });
      po.total_ordered += r.ordered_qty;
      po.total_received += r.received_qty;
    }
    const data = Array.from(poMap.values()).map(po => ({ ...po, progress: po.total_ordered > 0 ? Math.round(po.total_received / po.total_ordered * 100) : 0 }));
    ok(res, data); return;
  }

  // POST /api/procurement/receive — 분할입고 (아이템 단위)
  if (pathname === '/api/procurement/receive' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id) { fail(res, 400, 'po_id required'); return; }
    const items = body.items || []; // [{po_item_id, product_code, received_qty, defect_qty, notes}]
    if (!items.length) { fail(res, 400, 'items required'); return; }
    const tx = db.transaction(async () => {
      const rInfo = await db.prepare("INSERT INTO receipts (po_id, receipt_date, received_by, notes) VALUES (?, ?, ?, ?)").run(
        body.po_id, body.receipt_date || new Date().toISOString().slice(0,10), body.received_by || '', body.notes || '');
      const receiptId = rInfo.lastInsertRowid;
      const riStmt = db.prepare("INSERT INTO receipt_items (receipt_id, po_item_id, product_code, received_qty, defect_qty, notes) VALUES (?,?,?,?,?,?)");
      const updItem = db.prepare("UPDATE po_items SET received_qty = received_qty + ? WHERE item_id = ?");
      for (const it of items) {
        await riStmt.run(receiptId, it.po_item_id || null, it.product_code || '', it.received_qty || 0, it.defect_qty || 0, it.notes || '');
        if (it.po_item_id && it.received_qty) await updItem.run(it.received_qty, it.po_item_id);
      }
      // PO 상태 자동 갱신 (±tolerance% 허용 로직)
      const poHeader = await db.prepare('SELECT tolerance_pct, origin FROM po_header WHERE po_id=?').get(body.po_id);
      const tolerancePct = poHeader?.tolerance_pct || 5.0;
      const poItems = await db.prepare('SELECT ordered_qty, received_qty FROM po_items WHERE po_id=?').all(body.po_id);
      const totalOrdered = poItems.reduce((s, pi) => s + pi.ordered_qty, 0);
      const totalReceived = poItems.reduce((s, pi) => s + pi.received_qty, 0);
      const lowerBound = totalOrdered * (1 - tolerancePct / 100); // e.g. 9,500 for 10,000 @ 5%
      const upperBound = totalOrdered * (1 + tolerancePct / 100); // e.g. 10,500
      const allExact = poItems.length > 0 && poItems.every(pi => pi.received_qty >= pi.ordered_qty);
      const withinTolerance = totalReceived >= lowerBound && totalReceived <= upperBound;
      const anyDone = poItems.some(pi => pi.received_qty > 0);
      let autoCompleted = false;
      if (allExact || (withinTolerance && totalReceived >= lowerBound)) {
        await db.prepare("UPDATE po_header SET status='received', process_status='completed', material_status='received', updated_at=datetime('now','localtime') WHERE po_id=?").run(body.po_id);
        autoCompleted = true;
        if (withinTolerance && !allExact) {
          await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(
            body.po_id, 'auto_complete_tolerance', 'system',
            `±${tolerancePct}% 허용범위 자동완료 (발주:${totalOrdered}, 입고:${totalReceived}, 범위:${Math.round(lowerBound)}~${Math.round(upperBound)})`);
        }
      } else if (anyDone) {
        await db.prepare("UPDATE po_header SET status='partial', process_status='working', updated_at=datetime('now','localtime') WHERE po_id=?").run(body.po_id);
      }
      // 입고 활동 로그
      await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(
        body.po_id, 'receive', body.received_by || 'system',
        JSON.stringify(items.map(i => ({ code: i.product_code, qty: i.received_qty })))
      );
      // ── 입고 → 창고재고(warehouse_inventory) 자동 연동 ──
      const defaultWh = await db.prepare("SELECT id FROM warehouses WHERE is_default=1 LIMIT 1").get();
      if (defaultWh) {
        for (const it of items) {
          if (!it.product_code || !it.received_qty) continue;
          const pName = (await db.prepare('SELECT product_name FROM products WHERE product_code=?').get(it.product_code))?.product_name || '';
          const existing = await db.prepare('SELECT id, quantity FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?').get(defaultWh.id, it.product_code);
          if (existing) {
            await db.prepare('UPDATE warehouse_inventory SET quantity=quantity+?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(it.received_qty, existing.id);
          } else {
            await db.prepare('INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity) VALUES (?,?,?,?)').run(defaultWh.id, it.product_code, pName, it.received_qty);
          }
        }
      }
      // 선적 연결 (중국인 경우)
      if (body.shipment_id) {
        const spStmt = db.prepare("INSERT OR REPLACE INTO shipment_po_items (shipment_id, po_id, po_item_id, product_code, shipped_qty, received_qty) VALUES (?,?,?,?,?,?)");
        for (const it of items) {
          await spStmt.run(body.shipment_id, body.po_id, it.po_item_id || null, it.product_code || '', it.shipped_qty || it.received_qty, it.received_qty || 0);
        }
      }
      return receiptId;
    });
    const receiptId = await tx();
    // 알림
    const po = await db.prepare("SELECT po_number, origin, vendor_name FROM po_header WHERE po_id=?").get(body.po_id);
    if (po) createNotification(null, 'po', `입고완료: ${po.po_number}`, `${po.vendor_name} - ${items.length}건 입고`, 'procurement');
    // 자동 전표 생성 훅 (입고→매입전표)
    if (global._hookReceiveJournal) {
      try { global._hookReceiveJournal(body.po_id, items, body.received_by || ''); }
      catch(e) { console.error('전표 자동생성 오류:', e.message); }
    }
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'receipt_create', 'receipts', receiptId, `입고: PO ${body.po_id}, ${items.length}건`, clientIP);
    ok(res, { receipt_id: receiptId }); return;
  }

  // GET /api/procurement/receive-history/:poId — PO의 입고이력
  const rcvHistMatch = pathname.match(/^\/api\/procurement\/receive-history\/(\d+)$/);
  if (rcvHistMatch && method === 'GET') {
    const poId = rcvHistMatch[1];
    const receipts = await db.prepare(`
      SELECT r.id, r.receipt_date, r.received_by, r.notes AS receipt_notes,
             ri.product_code, ri.received_qty, ri.defect_qty, ri.notes AS item_notes
      FROM receipts r JOIN receipt_items ri ON r.id = ri.receipt_id
      WHERE r.po_id = ? ORDER BY r.receipt_date DESC
    `).all(poId);
    ok(res, receipts); return;
  }

  // ── 중국 선적 ↔ PO 연결 ──

  // POST /api/procurement/shipment-link — 선적에 PO 아이템 연결
  if (pathname === '/api/procurement/shipment-link' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.shipment_id || !body.items?.length) { fail(res, 400, 'shipment_id + items required'); return; }
    const stmt = db.prepare("INSERT OR REPLACE INTO shipment_po_items (shipment_id, po_id, po_item_id, product_code, product_name, shipped_qty, notes) VALUES (?,?,?,?,?,?,?)");
    const tx = db.transaction(async () => {
      for (const it of body.items) {
        await stmt.run(body.shipment_id, it.po_id, it.po_item_id || null, it.product_code || '', it.product_name || '', it.shipped_qty || 0, it.notes || '');
      }
    });
    await tx();
    ok(res, { linked: body.items.length }); return;
  }

  // GET /api/procurement/shipment-items/:shipmentId — 선적에 포함된 PO 아이템
  const shipItemsMatch = pathname.match(/^\/api\/procurement\/shipment-items\/(\d+)$/);
  if (shipItemsMatch && method === 'GET') {
    const rows = await db.prepare(`
      SELECT s.*, h.po_number, h.vendor_name, h.origin
      FROM shipment_po_items s
      JOIN po_header h ON h.po_id = s.po_id
      WHERE s.shipment_id = ?
    `).all(shipItemsMatch[1]);
    ok(res, rows); return;
  }

  // ── 더기프트 포장작업 ──

  // GET /api/procurement/assembly — 포장작업 목록
  if (pathname === '/api/procurement/assembly' && method === 'GET') {
    const rows = await db.prepare("SELECT * FROM gift_assembly ORDER BY created_at DESC").all();
    for (const r of rows) {
      r.materials = await db.prepare("SELECT * FROM gift_assembly_materials WHERE assembly_id=?").all(r.id);
    }
    ok(res, rows); return;
  }

  // POST /api/procurement/assembly — 포장작업 생성
  if (pathname === '/api/procurement/assembly' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.product_code || !body.target_qty) { fail(res, 400, 'product_code + target_qty required'); return; }
    const no = 'GA-' + new Date().toISOString().slice(0,10).replace(/-/g,'') + '-' + String(Math.random()).slice(2,5);
    const info = await db.prepare("INSERT INTO gift_assembly (assembly_no, product_code, product_name, target_qty, assembly_date, worker_name, notes) VALUES (?,?,?,?,?,?,?)").run(
      no, body.product_code, body.product_name || '', body.target_qty, body.assembly_date || new Date().toISOString().slice(0,10), body.worker_name || '', body.notes || '');
    const asmId = info.lastInsertRowid;
    // 자재 등록
    if (body.materials?.length) {
      const stmt = db.prepare("INSERT INTO gift_assembly_materials (assembly_id, item_code, item_name, required_qty) VALUES (?,?,?,?)");
      for (const m of body.materials) await stmt.run(asmId, m.item_code, m.item_name || '', m.required_qty || 0);
    }
    ok(res, { id: asmId, assembly_no: no }); return;
  }

  // POST /api/procurement/assembly/:id/complete — 포장완료
  const asmCompleteMatch = pathname.match(/^\/api\/procurement\/assembly\/(\d+)\/complete$/);
  if (asmCompleteMatch && method === 'POST') {
    const body = await readJSON(req);
    const id = asmCompleteMatch[1];
    await db.prepare("UPDATE gift_assembly SET status='completed', completed_qty=?, completed_date=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?").run(body.completed_qty || 0, id);
    const asm = await db.prepare("SELECT * FROM gift_assembly WHERE id=?").get(id);
    if (asm) {
      createNotification(null, 'po', `포장완료: ${asm.assembly_no}`, `${asm.product_name} ${body.completed_qty}개 포장 완료`, 'procurement');
      // 생산재고 연동: gift_sets에 매칭되는 세트가 있으면 assembly 트랜잭션 기록
      const matchedSet = await db.prepare("SELECT id, set_name FROM gift_sets WHERE set_code=? OR set_name=?").get(asm.product_code, asm.product_name);
      if (matchedSet) {
        await db.prepare("INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo) VALUES (?,?,?,?,?)").run(
          matchedSet.id, 'assembly', body.completed_qty || 0, asm.worker_name || '', `포장작업 연동: ${asm.assembly_no}`);
      }
    }
    ok(res, { completed: true }); return;
  }

  // POST /api/procurement/force-complete — 허용범위 미달 시 관리자 강제완료
  if (pathname === '/api/procurement/force-complete' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id) { fail(res, 400, 'po_id required'); return; }
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(body.po_id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    const poItems = await db.prepare('SELECT ordered_qty, received_qty FROM po_items WHERE po_id=?').all(body.po_id);
    const totalOrdered = poItems.reduce((s, pi) => s + pi.ordered_qty, 0);
    const totalReceived = poItems.reduce((s, pi) => s + pi.received_qty, 0);
    await db.prepare("UPDATE po_header SET status='received', process_status='completed', material_status='received', force_completed=1, force_completed_by=?, force_completed_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE po_id=?")
      .run(body.completed_by || 'admin', body.po_id);
    await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(
      body.po_id, 'force_complete', body.completed_by || 'admin',
      `강제완료 (발주:${totalOrdered}, 입고:${totalReceived}, 사유:${body.reason || '관리자 승인'})`);
    createNotification(null, 'po', `강제완료: ${po.po_number}`, `${po.vendor_name} - 관리자 강제완료 (${totalReceived}/${totalOrdered})`, 'procurement');
    ok(res, { force_completed: true, ordered: totalOrdered, received: totalReceived }); return;
  }

  // POST /api/procurement/confirm-material-date — 제지사가 후공정 업체에 자재 보내는 날짜 확정
  if (pathname === '/api/procurement/confirm-material-date' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id || !body.material_send_date) { fail(res, 400, 'po_id + material_send_date required'); return; }
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(body.po_id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    await db.prepare("UPDATE po_header SET material_send_date=?, material_confirmed_at=datetime('now','localtime'), material_status='confirmed', updated_at=datetime('now','localtime') WHERE po_id=?")
      .run(body.material_send_date, body.po_id);
    await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(
      body.po_id, 'material_date_confirmed', body.actor || 'vendor',
      `자재 출고일 확정: ${body.material_send_date}`);
    // 후공정 업체에 이메일 자동 발송 (자재 출고일 확정 시)
    // 법인별 발송 명의: DD → 바른디자인, 바른손 → 바른컴퍼니
    const _senderCompany = (po.legal_entity === 'dd') ? '바른디자인' : '바른컴퍼니';
    let emailResult = null;
    const processVendor = po.process_vendor_name || '';
    if (processVendor) {
      const vendor = await db.prepare('SELECT * FROM vendors WHERE name=?').get(processVendor);
      if (vendor && vendor.email && smtpTransporter) {
        const items = await db.prepare('SELECT * FROM po_items WHERE po_id=?').all(body.po_id);
        const itemsList = items.map(i => `<tr><td>${i.product_code}</td><td>${i.brand||''}</td><td>${i.ordered_qty}</td><td>${i.spec||''}</td></tr>`).join('');
        const html = `<h3>${_senderCompany} - 자재 출고 안내</h3>
          <p>발주번호: <b>${po.po_number}</b></p>
          <p>원재료 업체(${po.material_vendor_name||po.vendor_name})에서 <b>${body.material_send_date}</b>에 자재를 출고합니다.</p>
          <table border="1" cellpadding="6" style="border-collapse:collapse"><thead><tr><th>품목코드</th><th>브랜드</th><th>수량</th><th>규격</th></tr></thead><tbody>${itemsList}</tbody></table>
          <p>작업 일정 확인 부탁드립니다.</p>`;
        try {
          await smtpTransporter.sendMail({ from: `${_senderCompany} <${SMTP_FROM}>`, to: vendor.email, cc: vendor.email_cc || undefined, subject: `[${_senderCompany}] 자재 출고 안내 - ${po.po_number}`, html });
          await db.prepare("UPDATE po_header SET process_email_sent=1 WHERE po_id=?").run(body.po_id);
          emailResult = { sent: true, to: vendor.email };
        } catch(e) { emailResult = { sent: false, error: e.message }; }
      }
    }
    createNotification(null, 'po', `자재출고 확정: ${po.po_number}`, `${body.material_send_date} 후공정(${processVendor})으로 출고`, 'procurement');
    ok(res, { confirmed: true, material_send_date: body.material_send_date, email: emailResult }); return;
  }

  // POST /api/procurement/confirm-delivery-date — 거래처가 입고일 확정 (후공정 업체가 입고일 클릭)
  if (pathname === '/api/procurement/confirm-delivery-date' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id || !body.confirmed_date) { fail(res, 400, 'po_id + confirmed_date required'); return; }
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(body.po_id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    await db.prepare("UPDATE po_header SET vendor_confirmed_date=?, vendor_confirmed_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE po_id=?")
      .run(body.confirmed_date, body.po_id);
    await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(
      body.po_id, 'delivery_date_confirmed', body.actor || 'vendor',
      `입고일 확정: ${body.confirmed_date}`);
    createNotification(null, 'po', `입고일 확정: ${po.po_number}`, `${po.vendor_name} → ${body.confirmed_date} 입고 확정`, 'procurement');
    ok(res, { confirmed: true, vendor_confirmed_date: body.confirmed_date }); return;
  }

  // POST /api/procurement/create-korea-po — 한국 후공정 PO 생성 (제지사+후공정 2단계)
  if (pathname === '/api/procurement/create-korea-po' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.material_vendor || !body.process_vendor || !body.items?.length) {
      fail(res, 400, 'material_vendor, process_vendor, items required'); return;
    }
    const poNum = 'PO-KR-' + new Date().toISOString().slice(0,10).replace(/-/g,'') + '-' + String(Math.random()).slice(2,5);
    const totalQty = body.items.reduce((s, i) => s + (i.ordered_qty || 0), 0);
    const info = await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, material_vendor_name, process_vendor_name, status, due_date, total_qty, notes, origin, po_date, tolerance_pct) VALUES (?,?,?,?,?,?,?,?,?,?,date('now','localtime'),?)`)
      .run(poNum, '후공정', body.material_vendor, body.material_vendor, body.process_vendor, 'sent', body.expected_date || '', totalQty, body.notes || '', '한국', body.tolerance_pct || 5.0);
    const poId = info.lastInsertRowid;
    const stmt = db.prepare("INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec) VALUES (?,?,?,?,?,?)");
    for (const it of body.items) await stmt.run(poId, it.product_code, it.brand || '', it.process_type || '', it.ordered_qty || 0, it.spec || '');
    // 거래명세서 자동 생성
    await db.prepare("INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')")
      .run(poId, poNum, body.material_vendor, 'material', JSON.stringify(body.items));
    await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(
      poId, 'created', body.actor || 'system', `한국 후공정 PO 생성 (원재료:${body.material_vendor}, 후공정:${body.process_vendor})`);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'po_create', 'po_header', poId, `발주생성: ${poNum}, 원재료:${body.material_vendor}, 후공정:${body.process_vendor}`, clientIP);
    ok(res, { po_id: poId, po_number: poNum }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  한국 발주 마법사 관련 API (3단계: 품목 → 원재료업체 → 후가공업체)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/po/latest-by-product?codes=A,B,C — 품목별 최근 PO 설정 (마법사 기본값)
  if (pathname === '/api/po/latest-by-product' && method === 'GET') {
    try {
      const codesParam = parsed.searchParams.get('product_codes') || parsed.searchParams.get('codes') || '';
      const codes = codesParam.split(',').map(s => s.trim()).filter(Boolean);
      if (!codes.length) { ok(res, {}); return; }
      // 품목별 가장 최근 PO (한국 origin, 취소/draft 제외)
      // PG/SQLite 호환: 파라미터 바인딩으로 전체 조회 후 JS에서 first-only
      const placeholders = codes.map(() => '?').join(',');
      const rows = await db.prepare(`
        SELECT poi.product_code, poh.po_id, poh.po_date,
               poh.material_vendor_name, poh.process_vendor_name,
               poh.process_chain, poh.po_type
        FROM po_items poi
        JOIN po_header poh ON poh.po_id = poi.po_id
        WHERE poi.product_code IN (${placeholders})
          AND poh.origin = '한국'
          AND poh.status NOT IN ('cancelled','draft')
        ORDER BY poi.product_code, poh.po_date DESC, poh.po_id DESC
      `).all(...codes);
      // 품목당 첫 행만 채택
      const result = {};
      for (const r of rows) {
        if (result[r.product_code]) continue;
        let chain = [];
        try { if (r.process_chain) chain = JSON.parse(r.process_chain); } catch(_) {}
        result[r.product_code] = {
          material_vendor: r.material_vendor_name || '',
          process_vendor: r.process_vendor_name || '',
          process_chain: Array.isArray(chain) ? chain : [],
          po_date: r.po_date || ''
        };
      }
      ok(res, result);
    } catch (e) {
      console.error('[latest-by-product] 실패:', e.message);
      fail(res, 500, '최근 PO 조회 실패: ' + e.message);
    }
    return;
  }

  // POST /api/po/korea-wizard — 마법사 확정 (원재료 PO N장 + 후가공 PO M장 생성)
  if (pathname === '/api/po/korea-wizard' && method === 'POST') {
    const body = await readJSON(req);
    const items = Array.isArray(body.items) ? body.items : [];
    if (!items.length) { fail(res, 400, 'items required'); return; }
    // 각 품목은 {product_code, ordered_qty, material_vendor, material_code, material_name, process_chain:[{step,process,vendor}], spec}
    // 검증
    for (const it of items) {
      if (!it.product_code) { fail(res, 400, `product_code 누락: ${JSON.stringify(it)}`); return; }
      if (!it.material_vendor) { fail(res, 400, `${it.product_code}: material_vendor 미지정`); return; }
      if (!Array.isArray(it.process_chain) || !it.process_chain.length) {
        fail(res, 400, `${it.product_code}: process_chain 미지정 (최소 1개 공정 필요)`); return;
      }
      for (const s of it.process_chain) {
        if (!s.vendor || !s.process) {
          fail(res, 400, `${it.product_code}: process_chain에 process와 vendor 모두 필요`); return;
        }
      }
    }

    const expectedDate = body.expected_date || '';
    const notes = body.notes || '';
    const tolerancePct = body.tolerance_pct != null ? Number(body.tolerance_pct) : 5.0;
    const actorName = body.actor || (currentUser ? currentUser.username : 'system');

    // product_code 정제
    for (const it of items) {
      it.product_code = (it.product_code || '').replace(/[\s\u00A0\u200B\u200C\u200D\uFEFF]/g, '').trim();
    }

    // ─ 1) 원재료 업체별 그룹 ─
    const materialGroups = {}; // { 업체명: [item, ...] }
    for (const it of items) {
      const key = it.material_vendor;
      if (!materialGroups[key]) materialGroups[key] = [];
      materialGroups[key].push(it);
    }

    // ─ 2) 후가공: 1단계 업체별로 그룹 ─
    //    동일 1단계 업체라도 기본은 품목 묶음이 원재료 PO와 동일하게 흘러가도록
    //    원재료 PO 당 1개의 1단계 후가공 PO를 생성 (parent_po_id 연결)
    //    2+ step이 있는 품목은 process_chain JSON에만 저장, 실제 체인 트리거는 기존 vendor-portal 로직이 처리
    const materialPos = [];
    const processPos = [];

    try {
      for (const [matVendor, matItems] of Object.entries(materialGroups)) {
        const matPoNumber = await generatePoNumber();
        const matTotalQty = matItems.reduce((s, i) => s + (Number(i.ordered_qty) || 0), 0);
        const matNotes = notes || `원재료 발주 (${matVendor})`;
        // 전체 공정 체인을 JSON으로 저장 (첫 품목 기준 대표값 — UI 참고용; 실제 품목별 체인은 po_items.notes에도 기록)
        const repChain = matItems[0].process_chain || [];
        const matInfo = await db.prepare(`INSERT INTO po_header
          (po_number, po_type, vendor_name, material_vendor_name, process_vendor_name, status,
           due_date, total_qty, notes, origin, po_date, tolerance_pct, process_chain, process_step)
          VALUES (?,?,?,?,?,?,?,?,?,?,date('now','localtime'),?,?,?)`)
          .run(matPoNumber, '원재료', matVendor, matVendor, '', 'sent',
               expectedDate, matTotalQty, matNotes, '한국', tolerancePct,
               JSON.stringify(repChain), 0);
        const matPoId = matInfo.lastInsertRowid;

        const insItem = db.prepare(`INSERT INTO po_items
          (po_id, product_code, brand, process_type, ordered_qty, spec, notes)
          VALUES (?,?,?,?,?,?,?)`);
        for (const it of matItems) {
          await insItem.run(matPoId, it.product_code, it.brand || '', '원재료',
            Number(it.ordered_qty) || 0, it.spec || '',
            `용지:${it.material_name || ''} / 공정체인:${JSON.stringify(it.process_chain)}`);
        }

        await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)")
          .run(matPoId, 'created', actorName, `마법사 생성: 원재료 PO (${matVendor}, ${matItems.length}품목)`);

        materialPos.push({ po_id: matPoId, po_number: matPoNumber, vendor: matVendor, items: matItems.length, total_qty: matTotalQty });

        // ─ 동일 원재료 그룹의 1단계 후가공 업체 수집 → 업체별로 후가공 PO 생성 ─
        const postStep1ByVendor = {};
        for (const it of matItems) {
          const step1 = it.process_chain[0];
          if (!step1) continue;
          const key = step1.vendor;
          if (!postStep1ByVendor[key]) postStep1ByVendor[key] = { process: step1.process, items: [] };
          postStep1ByVendor[key].items.push(it);
        }
        for (const [postVendor, info] of Object.entries(postStep1ByVendor)) {
          const postPoNumber = await generatePoNumber();
          const postTotalQty = info.items.reduce((s, i) => s + (Number(i.ordered_qty) || 0), 0);
          const postInfo = await db.prepare(`INSERT INTO po_header
            (po_number, po_type, vendor_name, material_vendor_name, process_vendor_name, status,
             due_date, total_qty, notes, origin, po_date, tolerance_pct, process_chain, process_step, parent_po_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,date('now','localtime'),?,?,?,?)`)
            .run(postPoNumber, '후공정', postVendor, matVendor, postVendor, 'draft',
                 expectedDate, postTotalQty, `후가공(${info.process}) 대기 - 원재료 입고 후 발송`, '한국', tolerancePct,
                 JSON.stringify(info.items[0].process_chain || []), 1, matPoId);
          const postPoId = postInfo.lastInsertRowid;
          for (const it of info.items) {
            await insItem.run(postPoId, it.product_code, it.brand || '', info.process,
              Number(it.ordered_qty) || 0, it.spec || '',
              `원재료:${it.material_name || ''} / 전체체인:${JSON.stringify(it.process_chain)}`);
          }
          await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)")
            .run(postPoId, 'created', actorName, `마법사 생성: 후가공 PO (${postVendor}, ${info.process}, 원재료 PO: ${matPoNumber})`);
          processPos.push({ po_id: postPoId, po_number: postPoNumber, vendor: postVendor, process: info.process, items: info.items.length, total_qty: postTotalQty, parent_po_id: matPoId });
        }
      }

      // ─ 이메일 발송 (비동기, 실패해도 응답은 성공) ─
      (async () => {
        for (const m of materialPos) {
          try {
            const vendorRow = await db.prepare("SELECT email, email_cc FROM vendors WHERE name=?").get(m.vendor);
            if (vendorRow?.email) {
              const matPo = await db.prepare("SELECT * FROM po_header WHERE po_id=?").get(m.po_id);
              const matItems = await db.prepare("SELECT * FROM po_items WHERE po_id=?").all(m.po_id);
              await sendPOEmail(matPo, matItems, vendorRow.email, m.vendor, false, vendorRow.email_cc);
            }
          } catch (e) { console.warn(`[korea-wizard] 원재료 이메일 실패 (${m.vendor}):`, e.message); }
        }
        // 후가공은 원재료 입고 후 자동 발송되므로 여기서 즉시 발송하지 않음 (draft 상태 유지)
        // 사용자가 원재료 입고 확정 시 기존 vendor-portal PATCH 로직이 자동으로 후가공 PO를 sent로 전환하며 이메일 발송
      })().catch(e => console.error('[korea-wizard] 비동기 이메일 오류:', e.message));

      if (currentUser) auditLog(currentUser.userId, currentUser.username, 'po_create_wizard', 'po_header', null,
        `한국 마법사 발주: 원재료 ${materialPos.length}장 + 후가공 ${processPos.length}장`, clientIP);

      ok(res, { material_pos: materialPos, process_pos: processPos });
    } catch (e) {
      console.error('[korea-wizard] 실패:', e.message, e.stack);
      fail(res, 500, '마법사 발주 생성 실패: ' + e.message);
    }
    return;
  }

  // POST /api/procurement/assembly/:id/ship — 더기프트 출고 등록
  const asmShipMatch = pathname.match(/^\/api\/procurement\/assembly\/(\d+)\/ship$/);
  if (asmShipMatch && method === 'POST') {
    const body = await readJSON(req);
    const id = asmShipMatch[1];
    await db.prepare(`UPDATE gift_assembly SET delivery_status=?, tracking_number=?, carrier=?, shipped_date=?, delivery_address=?, recipient_name=?, updated_at=datetime('now','localtime') WHERE id=?`)
      .run(body.delivery_status || 'shipped', body.tracking_number || '', body.carrier || '', body.shipped_date || new Date().toISOString().slice(0,10), body.delivery_address || '', body.recipient_name || '', id);
    const asm = await db.prepare("SELECT * FROM gift_assembly WHERE id=?").get(id);
    if (asm) createNotification(null, 'po', `출고: ${asm.assembly_no}`, `${asm.product_name} 출고 (${body.carrier||''} ${body.tracking_number||''})`, 'procurement');
    ok(res, { shipped: true }); return;
  }

  // POST /api/procurement/assembly/:id/deliver — 더기프트 배송완료
  const asmDeliverMatch = pathname.match(/^\/api\/procurement\/assembly\/(\d+)\/deliver$/);
  if (asmDeliverMatch && method === 'POST') {
    const id = asmDeliverMatch[1];
    await db.prepare("UPDATE gift_assembly SET delivery_status='delivered', delivered_date=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?").run(id);
    ok(res, { delivered: true }); return;
  }

  // GET /api/procurement/korea-detail/:poId — 한국 PO 상세 (2단계 flow 포함)
  const koreaDetailMatch = pathname.match(/^\/api\/procurement\/korea-detail\/(\d+)$/);
  if (koreaDetailMatch && method === 'GET') {
    const poId = koreaDetailMatch[1];
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(poId);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    const items = await db.prepare('SELECT * FROM po_items WHERE po_id=?').all(poId);
    const totalOrdered = items.reduce((s, i) => s + i.ordered_qty, 0);
    const totalReceived = items.reduce((s, i) => s + i.received_qty, 0);
    const tolerancePct = po.tolerance_pct || 5;
    const lowerBound = Math.round(totalOrdered * (1 - tolerancePct / 100));
    const upperBound = Math.round(totalOrdered * (1 + tolerancePct / 100));
    const needsForceApprove = totalReceived > 0 && totalReceived < lowerBound && po.status !== 'received';
    const tradeDocs = await db.prepare('SELECT * FROM trade_document WHERE po_id=? ORDER BY created_at DESC').all(poId);
    const logs = await db.prepare('SELECT * FROM po_activity_log WHERE po_id=? ORDER BY id DESC LIMIT 20').all(poId);
    ok(res, { po, items, totalOrdered, totalReceived, tolerancePct, lowerBound, upperBound, needsForceApprove, tradeDocs, logs }); return;
  }

  // GET /api/procurement/china-shipments — 중국 합선적 현황 (여러 PO 합선적)
  if (pathname === '/api/procurement/china-shipments' && method === 'GET') {
    const shipments = await db.prepare(`SELECT * FROM china_shipment_log ORDER BY created_at DESC LIMIT 50`).all();
    for (const s of shipments) {
      s.po_items = await db.prepare(`SELECT sp.*, h.po_number, h.vendor_name, h.order_type FROM shipment_po_items sp JOIN po_header h ON h.po_id=sp.po_id WHERE sp.shipment_id=?`).all(s.id);
    }
    ok(res, shipments); return;
  }

  // POST /api/procurement/china-shipment-link — 중국 합선적에 여러 PO 연결
  if (pathname === '/api/procurement/china-shipment-link' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.shipment_id || !body.po_items?.length) { fail(res, 400, 'shipment_id + po_items required'); return; }
    const stmt = db.prepare("INSERT OR REPLACE INTO shipment_po_items (shipment_id, po_id, po_item_id, product_code, product_name, shipped_qty, notes) VALUES (?,?,?,?,?,?,?)");
    const tx = db.transaction(async () => {
      for (const it of body.po_items) {
        await stmt.run(body.shipment_id, it.po_id, it.po_item_id || null, it.product_code || '', it.product_name || '', it.shipped_qty || 0, it.notes || '');
      }
    });
    await tx();
    // 연결된 PO들 선적 상태 업데이트
    const poIds = [...new Set(body.po_items.map(i => i.po_id))];
    for (const pid of poIds) {
      await db.prepare("UPDATE po_header SET status='shipped', process_status='shipped', updated_at=datetime('now','localtime') WHERE po_id=? AND status NOT IN ('received','cancelled')").run(pid);
    }
    ok(res, { linked: body.po_items.length, shipment_id: body.shipment_id }); return;
  }

  // GET /api/procurement/pipeline — origin별 워크플로우 단계 현황
  if (pathname === '/api/procurement/pipeline' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const origin = qs.get('origin') || '';
    const pipeEntity = qs.get('entity') || 'all';
    const _pipeUseEnt = _hasEntity.po_header && pipeEntity && pipeEntity !== 'all';
    const pipeEntityClause = _pipeUseEnt ? ' AND legal_entity=?' : '';
    const pipeEntityParams = _pipeUseEnt ? [pipeEntity] : [];
    // 한국: draft → sent → 자재지급(material_sent) → 가공중(processing) → partial → received
    // 중국: draft → sent → 제작중(processing) → 선적(shipped) → 통관(customs) → partial → received
    // 더기프트: draft → sent → partial → received → 포장(assembly) → 출고(shipped)
    const stages = {
      '한국': ['draft','sent','자재지급','가공중','partial','received'],
      '중국': ['draft','sent','제작중','선적','통관','partial','received'],
      '더기프트': ['draft','sent','partial','received','포장','출고']
    };
    if (origin && stages[origin]) {
      const pos = await db.prepare(`
        SELECT po_id, po_number, vendor_name, status, material_status, process_status,
               due_date as expected_date, po_date, total_qty, notes, process_step,
               material_vendor_name, process_vendor_name, material_send_date,
               material_confirmed_at, process_email_sent, force_completed,
               tolerance_pct, order_type, vendor_confirmed_date
        FROM po_header WHERE origin=? AND status != 'cancelled'${pipeEntityClause}
        ORDER BY created_at DESC LIMIT 100
      `).all(origin, ...pipeEntityParams);
      // Map PO to pipeline stage
      for (const po of pos) {
        if (origin === '한국') {
          if (po.status === 'received') po.stage = 'received';
          else if (po.status === 'partial') po.stage = 'partial';
          else if (po.process_status === 'processing' || po.process_status === 'in_progress') po.stage = '가공중';
          else if (po.material_status === 'sent') po.stage = '자재지급';
          else if (po.status === 'sent') po.stage = 'sent';
          else po.stage = 'draft';
        } else if (origin === '중국') {
          if (po.status === 'received') po.stage = 'received';
          else if (po.status === 'partial') po.stage = 'partial';
          else if (po.process_status === 'customs') po.stage = '통관';
          else if (po.status === 'shipped' || po.shipped_at) po.stage = '선적';
          else if (po.process_status === 'processing' || po.process_status === 'in_progress') po.stage = '제작중';
          else if (po.status === 'sent') po.stage = 'sent';
          else po.stage = 'draft';
        } else {
          if (po.process_status === 'shipped') po.stage = '출고';
          else if (po.process_status === 'assembly') po.stage = '포장';
          else if (po.status === 'received') po.stage = 'received';
          else if (po.status === 'partial') po.stage = 'partial';
          else if (po.status === 'sent') po.stage = 'sent';
          else po.stage = 'draft';
        }
        // 입고율 + 강제완료 필요 여부
        const items = await db.prepare("SELECT COALESCE(SUM(ordered_qty),0) AS ord, COALESCE(SUM(received_qty),0) AS rcv FROM po_items WHERE po_id=?").get(po.po_id);
        po.progress = items.ord > 0 ? Math.round(items.rcv / items.ord * 100) : 0;
        po.total_ordered = items.ord;
        po.total_received = items.rcv;
        const tol = po.tolerance_pct || 5;
        const lb = items.ord * (1 - tol / 100);
        po.needs_force_approve = items.rcv > 0 && items.rcv < lb && po.status !== 'received';
      }
      ok(res, { origin, stages: stages[origin], orders: pos }); return;
    }
    ok(res, { stages }); return;
  }

  // POST /api/procurement/update-stage — PO 워크플로우 단계 전환
  if (pathname === '/api/procurement/update-stage' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id || !body.stage) { fail(res, 400, 'po_id + stage required'); return; }
    const po = await db.prepare("SELECT * FROM po_header WHERE po_id=?").get(body.po_id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    const updates = [];
    const params = [];
    // Map stage back to status fields
    const stageMap = {
      'draft': { status: 'draft' },
      'sent': { status: 'sent' },
      '자재지급': { material_status: 'sent', status: 'sent' },
      '가공중': { process_status: 'processing', status: 'sent' },
      '제작중': { process_status: 'processing', status: 'sent' },
      '선적': { status: 'shipped' },
      '통관': { process_status: 'customs' },
      'partial': { status: 'partial' },
      'received': { status: 'received' },
      '포장': { process_status: 'assembly' },
      '출고': { process_status: 'shipped' }
    };
    const mapping = stageMap[body.stage];
    if (mapping) {
      for (const [k, v] of Object.entries(mapping)) {
        updates.push(`${k}=?`); params.push(v);
      }
    }
    updates.push("updated_at=datetime('now','localtime')");
    params.push(body.po_id);
    await db.prepare(`UPDATE po_header SET ${updates.join(',')} WHERE po_id=?`).run(...params);
    // Activity log
    await db.prepare("INSERT INTO po_activity_log (po_id, po_number, action, actor, from_status, to_status, details) VALUES (?,?,?,?,?,?,?)").run(
      body.po_id, po.po_number, 'stage_change', body.actor || 'system', po.status, body.stage, body.notes || '');
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'po_stage_change', 'po_header', body.po_id, `발주단계변경: ${po.po_number} ${po.status}→${body.stage}`, clientIP);
    ok(res, { updated: true, stage: body.stage }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  구매마감 API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/purchase-closing — 월별 업체별 마감 데이터 조회
  if (pathname === '/api/purchase-closing' && method === 'GET') {
    try {
      const year = parseInt(parsed.searchParams.get('year')) || new Date().getFullYear();
      const month = parseInt(parsed.searchParams.get('month')) || new Date().getMonth() + 1;
      const entity = parsed.searchParams.get('entity') || 'all'; // barunson, dd, all
      const monthStr = `${year}-${String(month).padStart(2,'0')}`;
      const nextMonth = month === 12 ? `${year+1}-01` : `${year}-${String(month+1).padStart(2,'0')}`;

      let entityWhere = '';
      if (entity === 'barunson') entityWhere = "AND h.legal_entity = 'barunson'";
      else if (entity === 'dd') entityWhere = "AND h.legal_entity = 'dd'";

      // po_items에 amount 컬럼이 있는지 확인 (PG 권한 부족 시 ALTER TABLE 실패)
      let hasAmountCol = false;
      try {
        await db.prepare('SELECT amount FROM po_items LIMIT 1').get();
        hasAmountCol = true;
      } catch(e) {}

      const amtSelect = hasAmountCol
        ? ', SUM(COALESCE(i.amount,0)) AS sys_amount, SUM(COALESCE(i.tax_amount,0)) AS sys_tax'
        : ', 0 AS sys_amount, 0 AS sys_tax';

      const rows = await db.prepare(`
        SELECT h.vendor_name,
               h.legal_entity,
               h.po_type,
               COUNT(DISTINCT h.po_id) AS po_count,
               SUM(i.ordered_qty) AS total_ordered,
               SUM(COALESCE(i.received_qty,0)) AS total_received,
               SUM(COALESCE(i.produced_qty,0)) AS total_produced,
               SUM(COALESCE(i.defect_qty,0)) AS total_defect
               ${amtSelect}
        FROM po_header h
        JOIN po_items i ON h.po_id = i.po_id
        WHERE h.status NOT IN ('cancelled','draft')
          AND h.po_date >= ? AND h.po_date < ?
          ${entityWhere}
        GROUP BY h.vendor_name, h.legal_entity, h.po_type
        ORDER BY h.legal_entity, h.vendor_name
      `).all(monthStr + '-01', nextMonth + '-01');

      // 거래명세서에서 금액 가져오기
      // trade_document는 doc_date 없음 → po_header의 po_date로 조인, legal_entity도 po_header에서 가져옴
      const amountRaw = await db.prepare(`
        SELECT td.vendor_name, h.legal_entity, td.items_json
        FROM trade_document td
        JOIN po_header h ON td.po_id = h.po_id
        WHERE h.po_date >= ? AND h.po_date < ?
          AND td.status NOT IN ('cancelled','rejected')
          ${entityWhere}
      `).all(monthStr + '-01', nextMonth + '-01');
      const amountMap = {};
      for (const r of amountRaw) {
        const key = `${r.vendor_name}|${r.legal_entity}`;
        let amt = 0;
        try {
          if (r.items_json) {
            const items = JSON.parse(r.items_json);
            if (Array.isArray(items)) items.forEach(it => { amt += Number(it.amount) || 0; });
          }
        } catch(e) {}
        amountMap[key] = (amountMap[key] || 0) + amt;
      }

      // 기존 마감 상태 (테이블 없으면 빈 맵)
      const closingMap = {};
      try {
        const closings = await db.prepare(`
          SELECT * FROM purchase_closing
          WHERE closing_year = ? AND closing_month = ?
          ${entity !== 'all' ? "AND legal_entity = '" + entity + "'" : ''}
        `).all(year, month);
        for (const c of closings) {
          closingMap[`${c.vendor_name}|${c.legal_entity}`] = c;
        }
      } catch(e) { /* purchase_closing 테이블 미존재 시 무시 */ }

      // 첨부파일 금액 (업체 청구금액)
      const uploadAmtMap = {};
      try {
        const uploadRows = await db.prepare(`SELECT vendor_name, legal_entity, SUM(parsed_total) AS vendor_total, SUM(parsed_tax) AS vendor_tax FROM purchase_closing_files WHERE closing_year=? AND closing_month=? GROUP BY vendor_name, legal_entity`).all(year, month);
        for (const u of uploadRows) { uploadAmtMap[`${u.vendor_name}|${u.legal_entity}`] = { total: u.vendor_total||0, tax: u.vendor_tax||0 }; }
      } catch(e) {}

      // 결과 조합
      const vendors = rows.map(r => {
        const key = `${r.vendor_name}|${r.legal_entity}`;
        const closing = closingMap[key] || null;
        const upload = uploadAmtMap[key] || null;
        const sysAmt = parseFloat(r.sys_amount) || 0;
        const sysTax = parseFloat(r.sys_tax) || 0;
        const vendorAmt = upload ? parseFloat(upload.total) : 0;
        const vendorTax = upload ? parseFloat(upload.tax) : 0;
        return {
          vendor_name: r.vendor_name,
          legal_entity: r.legal_entity,
          po_type: r.po_type,
          po_count: r.po_count,
          total_ordered: r.total_ordered || 0,
          total_received: r.total_received || 0,
          total_produced: r.total_produced || 0,
          total_defect: r.total_defect || 0,
          sys_amount: sysAmt,
          sys_tax: sysTax,
          vendor_amount: vendorAmt,
          vendor_tax: vendorTax,
          diff_amount: sysAmt > 0 && vendorAmt > 0 ? sysAmt - vendorAmt : 0,
          has_upload: !!upload,
          total_amount: amountMap[key] || 0,
          closing_status: closing ? closing.status : 'open',
          closing_id: closing ? closing.id : null,
          confirmed_at: closing ? closing.confirmed_at : '',
          final_amount: closing ? closing.final_amount : sysAmt,
          adjustment_amount: closing ? closing.adjustment_amount : 0,
          notes: closing ? closing.notes : ''
        };
      });

      ok(res, { vendors, year, month, entity });
    } catch(e) {
      console.error('구매마감 조회 오류:', e.message);
      fail(res, 500, '구매마감 조회 오류: ' + e.message);
    }
    return;
  }

  // GET /api/purchase-closing/detail — 업체별 상세 PO 품목 목록 (단가/금액 포함)
  if (pathname === '/api/purchase-closing/detail' && method === 'GET') {
    try {
      const year = parseInt(parsed.searchParams.get('year')) || new Date().getFullYear();
      const month = parseInt(parsed.searchParams.get('month')) || new Date().getMonth() + 1;
      const vendorName = parsed.searchParams.get('vendor');
      const entity = parsed.searchParams.get('entity') || 'barunson';
      if (!vendorName) { fail(res, 400, '거래처명 필수'); return; }

      const monthStr = `${year}-${String(month).padStart(2,'0')}`;
      const nextMonth = month === 12 ? `${year+1}-01` : `${year}-${String(month+1).padStart(2,'0')}`;

      // po_items에 amount 컬럼 존재 여부 확인
      let _hasAmt = false;
      try { await db.prepare('SELECT amount FROM po_items LIMIT 1').get(); _hasAmt = true; } catch(e) {}

      const priceSelect = _hasAmt
        ? ', COALESCE(i.unit_price,0) AS unit_price, COALESCE(i.amount,0) AS amount, COALESCE(i.tax_amount,0) AS tax_amount, COALESCE(i.received_date,\'\') AS received_date'
        : ', 0 AS unit_price, 0 AS amount, 0 AS tax_amount, \'\' AS received_date';

      const pos = await db.prepare(`
        SELECT h.po_id, h.po_number, h.po_type, h.status, h.po_date, h.total_qty,
               i.item_id, i.product_code, i.brand, i.process_type, i.spec,
               i.ordered_qty, COALESCE(i.received_qty,0) AS received_qty,
               COALESCE(i.produced_qty,0) AS produced_qty,
               COALESCE(i.defect_qty,0) AS defect_qty,
               i.ship_date
               ${priceSelect}
        FROM po_header h
        JOIN po_items i ON h.po_id = i.po_id
        WHERE h.vendor_name = ? AND h.legal_entity = ?
          AND h.status NOT IN ('cancelled','draft')
          AND h.po_date >= ? AND h.po_date < ?
        ORDER BY h.po_date DESC, h.po_id DESC, i.item_id
      `).all(vendorName, entity, monthStr + '-01', nextMonth + '-01');

      // 품목명 보충 (xerpItemNameCache + products)
      for (const p of pos) {
        const code = (p.product_code||'').trim();
        p.product_name = (xerpItemNameCache && xerpItemNameCache[code]) || '';
        if (!p.product_name) {
          try {
            const lp = await db.prepare('SELECT product_name, material_name FROM products WHERE product_code=?').get(code);
            if (lp) p.product_name = (lp.product_name||'').trim() || (lp.material_name||'').trim();
          } catch(e) {}
        }
      }

      // 첨부파일 목록
      let files = [];
      try {
        files = await db.prepare(`SELECT * FROM purchase_closing_files WHERE closing_year=? AND closing_month=? AND vendor_name=? AND legal_entity=? ORDER BY uploaded_at DESC`).all(year, month, vendorName, entity);
      } catch(e) {}

      // 시스템 합계
      let sysTotal = 0, sysTax = 0;
      for (const p of pos) {
        sysTotal += parseFloat(p.amount) || 0;
        sysTax += parseFloat(p.tax_amount) || 0;
      }

      ok(res, { items: pos, vendor_name: vendorName, entity, year, month, files, sys_total: sysTotal, sys_tax: sysTax });
    } catch(e) {
      fail(res, 500, '구매마감 상세 오류: ' + e.message);
    }
    return;
  }

  // POST /api/purchase-closing/update-price — 품목별 단가/금액 수정
  if (pathname === '/api/purchase-closing/update-price' && method === 'POST') {
    try {
      const body = await readJSON(req);
      const { item_id, unit_price, amount, tax_amount, received_date } = body;
      if (!item_id) { fail(res, 400, 'item_id 필수'); return; }
      const up = parseFloat(unit_price) || 0;
      const amt = parseFloat(amount) || 0;
      const tax = parseFloat(tax_amount) || 0;
      const rd = received_date || '';
      await db.prepare(`UPDATE po_items SET unit_price=?, amount=?, tax_amount=?, received_date=? WHERE item_id=?`).run(up, amt, tax, rd, item_id);
      ok(res, { message: '단가 수정 완료', item_id, unit_price: up, amount: amt, tax_amount: tax });
    } catch(e) {
      fail(res, 500, '단가 수정 오류: ' + e.message);
    }
    return;
  }

  // POST /api/purchase-closing/upload — 세금계산서/거래명세서 파일 업로드
  if (pathname === '/api/purchase-closing/upload' && method === 'POST') {
    try {
      const contentType = req.headers['content-type'] || '';
      if (!contentType.includes('multipart/form-data')) {
        fail(res, 400, 'multipart/form-data 필요'); return;
      }
      const boundary = contentType.split('boundary=')[1];
      if (!boundary) { fail(res, 400, 'boundary 없음'); return; }

      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const buf = Buffer.concat(chunks);
      const sep = Buffer.from('--' + boundary);

      // 파트 파싱
      const fields = {};
      let fileData = null, fileName = '', fileType = '';
      const parts = [];
      let start = 0;
      while (true) {
        const idx = buf.indexOf(sep, start);
        if (idx === -1) break;
        if (start > 0) parts.push(buf.slice(start, idx - 2));
        start = idx + sep.length + 2;
      }
      for (const part of parts) {
        const headerEnd = part.indexOf('\r\n\r\n');
        if (headerEnd === -1) continue;
        const header = part.slice(0, headerEnd).toString('utf8');
        const body = part.slice(headerEnd + 4);
        const nameMatch = header.match(/name="([^"]+)"/);
        const fileMatch = header.match(/filename="([^"]+)"/);
        if (nameMatch) {
          if (fileMatch) {
            fileData = body;
            fileName = fileMatch[1];
            const ctMatch = header.match(/Content-Type:\s*(.+)/i);
            fileType = ctMatch ? ctMatch[1].trim() : '';
          } else {
            fields[nameMatch[1]] = body.toString('utf8').trim();
          }
        }
      }

      if (!fileData || !fileName) { fail(res, 400, '파일이 없습니다'); return; }
      const vendor = fields.vendor_name || '';
      const entity = fields.legal_entity || 'barunson';
      const yr = parseInt(fields.year) || new Date().getFullYear();
      const mo = parseInt(fields.month) || new Date().getMonth() + 1;
      if (!vendor) { fail(res, 400, 'vendor_name 필수'); return; }

      // 파일 저장
      const uploadDir = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
      const closingDir = path.join(uploadDir, 'closing', `${yr}-${String(mo).padStart(2,'0')}`);
      const fs = require('fs');
      if (!fs.existsSync(closingDir)) fs.mkdirSync(closingDir, { recursive: true });
      const safeName = `${entity}_${vendor}_${Date.now()}_${fileName.replace(/[^a-zA-Z0-9가-힣._-]/g, '_')}`;
      const filePath = path.join(closingDir, safeName);
      fs.writeFileSync(filePath, fileData);

      // 엑셀 파싱 시도
      let parsedTotal = 0, parsedTax = 0, parsedData = [];
      const ext = path.extname(fileName).toLowerCase();
      if (ext === '.xlsx' || ext === '.xls') {
        try {
          const XLSX = require('xlsx');
          const wb = XLSX.read(fileData, { type: 'buffer' });
          const ws = wb.Sheets[wb.SheetNames[0]];
          const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
          // 금액/세액 컬럼 자동 탐지
          const amtKeys = ['금액','공급가액','공급가','amount','supply_amount','합계금액'];
          const taxKeys = ['세액','부가세','vat','tax','tax_amount'];
          for (const row of rows) {
            let amt = 0, tax = 0;
            for (const [k,v] of Object.entries(row)) {
              const kl = k.toLowerCase().replace(/\s/g,'');
              if (amtKeys.some(a => kl.includes(a))) amt = parseFloat(String(v).replace(/,/g,'')) || 0;
              if (taxKeys.some(t => kl.includes(t))) tax = parseFloat(String(v).replace(/,/g,'')) || 0;
            }
            if (amt > 0 || tax > 0) {
              parsedTotal += amt;
              parsedTax += tax;
              parsedData.push(row);
            }
          }
        } catch(e) { console.warn('엑셀 파싱 실패:', e.message); }
      }

      // DB 저장
      await db.prepare(`INSERT INTO purchase_closing_files (closing_year, closing_month, legal_entity, vendor_name, file_name, file_path, file_type, file_size, parsed_total, parsed_tax, parsed_data) VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(
        yr, mo, entity, vendor, fileName, filePath, fileType, fileData.length,
        parsedTotal, parsedTax, JSON.stringify(parsedData)
      );

      ok(res, { message: '파일 업로드 완료', file_name: fileName, parsed_total: parsedTotal, parsed_tax: parsedTax, parsed_rows: parsedData.length });
    } catch(e) {
      console.error('파일 업로드 오류:', e.message);
      fail(res, 500, '파일 업로드 오류: ' + e.message);
    }
    return;
  }

  // DELETE /api/purchase-closing/file/:id — 첨부파일 삭제
  if (pathname.startsWith('/api/purchase-closing/file/') && method === 'DELETE') {
    try {
      const fileId = parseInt(pathname.split('/').pop());
      if (!fileId) { fail(res, 400, 'file id 필수'); return; }
      const f = await db.prepare('SELECT file_path FROM purchase_closing_files WHERE id=?').get(fileId);
      if (f && f.file_path) {
        const fs = require('fs');
        try { fs.unlinkSync(f.file_path); } catch(e) {}
      }
      await db.prepare('DELETE FROM purchase_closing_files WHERE id=?').run(fileId);
      ok(res, { message: '파일 삭제 완료' });
    } catch(e) {
      fail(res, 500, '파일 삭제 오류: ' + e.message);
    }
    return;
  }

  // POST /api/purchase-closing/confirm — 마감 확정
  if (pathname === '/api/purchase-closing/confirm' && method === 'POST') {
    try {
      const body = await readJSON(req);
      const { vendor_name, legal_entity, year, month, notes } = body;
      if (!vendor_name || !year || !month) { fail(res, 400, '필수 값 누락'); return; }
      const ent = legal_entity || 'barunson';

      const monthStr = `${year}-${String(month).padStart(2,'0')}`;
      const nextMonth = month === 12 ? `${year+1}-01` : `${year}-${String(month+1).padStart(2,'0')}`;

      // 집계
      const agg = await db.prepare(`
        SELECT COUNT(DISTINCT h.po_id) AS po_count,
               SUM(i.ordered_qty) AS total_ordered,
               SUM(COALESCE(i.received_qty,0)) AS total_received,
               SUM(COALESCE(i.defect_qty,0)) AS total_defect
        FROM po_header h JOIN po_items i ON h.po_id = i.po_id
        WHERE h.vendor_name = ? AND h.legal_entity = ?
          AND h.status NOT IN ('cancelled','draft')
          AND h.po_date >= ? AND h.po_date < ?
      `).get(vendor_name, ent, monthStr + '-01', nextMonth + '-01');

      // 금액
      const amtRaw = await db.prepare(`
        SELECT td.items_json FROM trade_document td
        JOIN po_header h ON td.po_id = h.po_id
        WHERE td.vendor_name = ? AND h.legal_entity = ?
          AND h.po_date >= ? AND h.po_date < ?
          AND td.status NOT IN ('cancelled','rejected')
      `).all(vendor_name, ent, monthStr + '-01', nextMonth + '-01');
      let totalAmount = 0;
      for (const ar of amtRaw) {
        try {
          if (ar.items_json) {
            const items = JSON.parse(ar.items_json);
            if (Array.isArray(items)) items.forEach(it => { totalAmount += Number(it.amount) || 0; });
          }
        } catch(e) {}
      }

      await db.prepare(`
        INSERT INTO purchase_closing (legal_entity, vendor_name, closing_year, closing_month,
          po_count, total_ordered_qty, total_received_qty, total_defect_qty,
          total_amount, final_amount, status, confirmed_at, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,'confirmed',datetime('now','localtime'),?)
        ON CONFLICT(legal_entity, vendor_name, closing_year, closing_month)
        DO UPDATE SET po_count=excluded.po_count, total_ordered_qty=excluded.total_ordered_qty,
          total_received_qty=excluded.total_received_qty, total_defect_qty=excluded.total_defect_qty,
          total_amount=excluded.total_amount, final_amount=excluded.final_amount,
          status='confirmed', confirmed_at=datetime('now','localtime'),
          notes=excluded.notes, updated_at=datetime('now','localtime')
      `).run(ent, vendor_name, year, month,
        agg.po_count||0, agg.total_ordered||0, agg.total_received||0, agg.total_defect||0,
        totalAmount, totalAmount, notes||'');

      ok(res, { message: '마감 확정 완료' });
    } catch(e) {
      fail(res, 500, '마감 확정 오류: ' + e.message);
    }
    return;
  }

  // POST /api/purchase-closing/reopen — 마감 해제
  if (pathname === '/api/purchase-closing/reopen' && method === 'POST') {
    try {
      const body = await readJSON(req);
      const { closing_id } = body;
      if (!closing_id) { fail(res, 400, 'closing_id 필수'); return; }
      await db.prepare(`UPDATE purchase_closing SET status='open', updated_at=datetime('now','localtime') WHERE id=?`).run(closing_id);
      ok(res, { message: '마감 해제 완료' });
    } catch(e) {
      fail(res, 500, '마감 해제 오류: ' + e.message);
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  VENDOR PORTAL API (업체 포털)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/vendor-portal/generate-token — 관리자가 거래처 포탈 접속 토큰 생성
  if (pathname === '/api/vendor-portal/generate-token' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const email = qs.get('email') || '';
    const name = qs.get('name') || '';
    if (!email) { fail(res, 400, '이메일 필요'); return; }
    const token = generateVendorToken(email, name);
    ok(res, { access: token });
    return;
  }

  // GET /api/vendor-portal — 업체 전용 PO 목록
  if (pathname === '/api/vendor-portal' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    // access 토큰 방식 (신규) 또는 email+token 방식 (레거시)
    const accessToken = qs.get('access') || qs.get('token') || '';
    const decoded = decodeVendorToken(accessToken);
    const email = decoded ? decoded.email : (qs.get('email') || '');
    const vendorNameParam = decoded ? (decoded.name || '') : (qs.get('vendor_name') || '');
    if (!email || !verifyVendorToken(email, accessToken)) {
      fail(res, 403, '인증 실패'); return;
    }
    // vendor_name 파라미터가 있으면 이름으로 정확 매칭, 없으면 이메일로 조회
    let vendor;
    if (vendorNameParam) {
      vendor = await db.prepare('SELECT * FROM vendors WHERE name = ? AND email = ?').get(vendorNameParam, email);
    }
    if (!vendor) {
      vendor = await db.prepare('SELECT * FROM vendors WHERE email = ?').get(email);
    }
    if (!vendor) { fail(res, 404, '등록된 업체가 아닙니다'); return; }

    const rows = await db.prepare('SELECT * FROM po_header WHERE vendor_name = ? ORDER BY po_date DESC, po_id DESC').all(vendor.name);
    // ── N+1 제거: po_items 일괄 조회 후 그룹핑 ──
    const poIds = rows.map(r => r.po_id);
    let allItems = [];
    if (poIds.length) {
      const placeholders = poIds.map(() => '?').join(',');
      allItems = await db.prepare(`SELECT * FROM po_items WHERE po_id IN (${placeholders})`).all(...poIds);
    }
    const itemsByPoId = {};
    for (const it of allItems) {
      if (!itemsByPoId[it.po_id]) itemsByPoId[it.po_id] = [];
      itemsByPoId[it.po_id].push(it);
    }
    const _postCols = await getPostProcessTypes();
    for (const r of rows) {
      r.status = PO_STATUS_EN_TO_KO[r.status] || r.status;
      r.material_status_label = MATERIAL_STATUS_KO[r.material_status] || r.material_status;
      r.process_status_label = PROCESS_STATUS_KO[r.process_status] || r.process_status;
      r.items = itemsByPoId[r.po_id] || [];
      // product_info 데이터 보강 (원자재코드, 원재료용지명, 절, 조판, 후공정체인)
      const pInfo = getProductInfo();
      const postCols = _postCols;
      const steps = [];
      for (const it of r.items) {
        const info = pInfo[it.product_code] || {};
        it.material_code = info['원자재코드'] || info.material_code || '';
        it.material_name = info['원재료용지명'] || info.material_name || '';
        it.cut = info['절'] || '';
        it.imposition = info['조판'] || '';
        it.product_spec = info['제품사양'] || it.spec || '';
        // 품목별 후공정 체인 — _steps(step_order) 우선, 없으면 postCols 순서 fallback
        let itemSteps = [];
        if (info._steps && info._steps.length) {
          itemSteps = info._steps.map(s => ({ p: s.process, v: s.vendor }));
        } else {
          postCols.forEach(c => {
            if (info[c] && info[c] !== '0' && !/^[\d,.]+$/.test(String(info[c]).trim())) itemSteps.push({ p: c, v: info[c] });
          });
        }
        it.first_process = itemSteps.length ? itemSteps[0].p : '';
        it.first_process_vendor = itemSteps.length ? itemSteps[0].v : '';
        it.process_chain_full = itemSteps.map(s => s.v + '(' + s.p + ')').join(' → ');
        // 품목별 입고처 + 공정명 보정: 현재 업체의 공정 찾기
        if (vendor.type === '후공정' && itemSteps.length > 0) {
          const vName = vendor.name || '';
          let foundIdx = -1;
          // 1) 정확 매칭
          for (let si = 0; si < itemSteps.length; si++) {
            if (itemSteps[si].v === vName) { foundIdx = si; break; }
          }
          // 2) 부분 매칭 (코리아↔코리아패키지 등)
          if (foundIdx < 0) {
            for (let si = 0; si < itemSteps.length; si++) {
              const sv = itemSteps[si].v;
              if (vName.includes(sv) || sv.includes(vName)) { foundIdx = si; break; }
            }
          }
          // 공정명: product_info 기반으로 보정 (DB에 깨진 한글 방지)
          if (foundIdx >= 0) {
            it.resolved_process = itemSteps[foundIdx].p;
          }
          if (foundIdx >= 0 && foundIdx < itemSteps.length - 1) {
            const nxt = itemSteps[foundIdx + 1];
            it.item_next_dest = nxt.v + '(' + nxt.p + ')';
          } else {
            it.item_next_dest = '파주(본사)';
          }
        }
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
    // 재고 정보 보강 (스마트재고현황 캐시 활용 — 모든 업체에 긴급도 제공)
    try {
      // XERP 재고 캐시에서 가져오기 (10분 갱신)
      const invMap = {};
      const cacheKeys = ['all', 'barunson', 'dd'];
      for (const ck of cacheKeys) {
        const ce = (typeof xerpInventoryCaches !== 'undefined' && xerpInventoryCaches) ? xerpInventoryCaches[ck] : null;
        if (ce && ce.data && ce.data.products) {
          for (const p of ce.data.products) {
            const code = p['제품코드'] || p.product_code;
            if (code && !invMap[code]) {
              invMap[code] = {
                stock: p['현재고'] || p['가용재고'] || 0,
                monthly: p['_xerpMonthly'] || 0,
                status: p._status || '',           // urgent/danger/safe
                remainDays: p._remainDays || null,  // 잔여일수
                safetyStock: p._safetyStock || 0
              };
            }
          }
        }
      }
      // 캐시가 비어있으면 XERP 직접 조회 (원재료 업체만 — 후공정은 캐시만 사용)
      if (Object.keys(invMap).length === 0 && vendor.type === '원재료' && typeof ensureXerpPool === 'function') {
        try {
          if (await ensureXerpPool()) {
            const codes = new Set();
            for (const po of rows) { for (const it of (po.items||[])) { if (it.product_code) codes.add(it.product_code); } }
            if (codes.size > 0) {
              const safeList = [...codes].filter(c => /^[A-Za-z0-9_\-]+$/.test(c)).map(c => `'${c}'`).join(',');
              if (safeList) {
                // ★ 양수 OhQty 만 SUM — XERP 스마트재고현황과 일치 (음수 예약/조정 lot 제외)
                const invR = await xerpPool.request().query(`SELECT RTRIM(ItemCode) AS code, SUM(CASE WHEN OhQty > 0 THEN OhQty ELSE 0 END) AS qty FROM mmInventory WITH(NOLOCK) WHERE SiteCode='BK10' AND RTRIM(ItemCode) IN (${safeList}) GROUP BY RTRIM(ItemCode)`);
                for (const r of (invR.recordset||[])) { invMap[r.code.trim()] = { stock: Math.round(r.qty||0), monthly: 0 }; }
                // 월출고
                const today = new Date(); const s3m = new Date(today); s3m.setMonth(s3m.getMonth()-3);
                const fmt = d => d.getFullYear()+String(d.getMonth()+1).padStart(2,'0')+String(d.getDate()).padStart(2,'0');
                const shipR = await xerpPool.request().input('s3',sql.NChar(16),fmt(s3m)).input('t',sql.NChar(16),fmt(today)).query(`SELECT RTRIM(ItemCode) AS code, SUM(InoutQty) AS qty FROM mmInoutItem WITH(NOLOCK) WHERE SiteCode='BK10' AND InoutGubun='SO' AND InoutDate>=@s3 AND InoutDate<@t AND RTRIM(ItemCode) IN (${safeList}) GROUP BY RTRIM(ItemCode)`);
                for (const r of (shipR.recordset||[])) { const c=r.code.trim(); if(invMap[c]) invMap[c].monthly = Math.round((r.qty||0)/3); }
              }
            }
          }
        } catch(xe) { console.warn('vendor-portal XERP 직접 조회 실패:', xe.message); }
      }
      for (const po of rows) {
        for (const it of (po.items||[])) {
          const inv = invMap[it.product_code] || {};
          it.current_stock = inv.stock || 0;
          it.monthly_usage = inv.monthly || 0;
          it.stock_months = inv.monthly > 0 ? Math.round((inv.stock / inv.monthly) * 10) / 10 : null;
          it.is_urgent = inv.status === 'urgent' || (inv.monthly > 0 && inv.stock <= inv.monthly); // 스마트재고 긴급 또는 1개월 이하
          it.is_danger = inv.status === 'danger';
          it.remain_days = inv.remainDays;
        }
      }
    } catch(e) { console.warn('vendor-portal 재고 보강 실패:', e.message); }

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
    const auth = extractVendorAuth(body);
    if (!auth) { fail(res, 403, '인증 실패'); return; }
    const email = auth.email;
    // 이메일로 등록된 업체 확인 (같은 이메일로 여러 업체 가능)
    const vendorsWithEmail = await db.prepare('SELECT * FROM vendors WHERE email = ?').all(email);
    if (!vendorsWithEmail.length) { fail(res, 404, '등록된 업체가 아닙니다'); return; }

    const po = await db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(poId);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    // PO의 vendor_name과 매칭되는 vendor 찾기
    const vendor = vendorsWithEmail.find(v => v.name === po.vendor_name || v.name.startsWith(po.vendor_name) || po.vendor_name.startsWith(v.name.slice(0,2)));
    if (!vendor) { fail(res, 403, '본인 발주서가 아닙니다'); return; }

    const action = body.action; // 'confirm' or 'ship'
    const currentStatus = PO_STATUS_EN_TO_KO[po.status] || po.status;

    let emailResult = null;

    if (action === 'confirm' && currentStatus === '발송') {
      // 업체가 발주 확인
      const beforeConfirm = { status: po.status, mat: po.material_status, proc: po.process_status };
      await db.prepare(`UPDATE po_header SET status = 'confirmed', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
      // 파이프라인 서브상태 업데이트 (vendor.type 기준)
      if (vendor.type === '후공정') {
        await db.prepare('UPDATE po_header SET process_status=? WHERE po_id=?').run('confirmed', poId);
      } else {
        // 원재료 또는 타입 미설정
        await db.prepare('UPDATE po_header SET material_status=? WHERE po_id=?').run('confirmed', poId);
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
          await db.prepare(`UPDATE po_header SET status = '확인', process_status='step_done', shipped_at=datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
          logPOActivity(poId, 'vendor_ship', {
            actor: vendor.name, actor_type: vendor.type,
            from_status: po.status, to_status: '확인',
            from_mat: po.material_status, to_mat: po.material_status,
            from_proc: po.process_status, to_proc: 'step_done',
            details: `${vendor.name} 공정 Step ${currentStep} 완료 → Step ${currentStep+1} (${nextStepInfo.process}@${nextStepInfo.vendor}) 자동 트리거`
          });

          // 다음 step PO가 이미 대기 중인지 확인
          const parentId = po.parent_po_id || poId;
          let nextPO = await db.prepare(`SELECT * FROM po_header WHERE parent_po_id = ? AND process_step = ? AND po_type = '후공정'`).get(parentId, currentStep + 1);

          // 없으면 자동 생성
          if (!nextPO) {
            const nextPoNumber = await generatePoNumber();
            // 거래처명에서 vendors 테이블 매칭 (부분 매칭)
            let nextVendorRow = await db.prepare('SELECT * FROM vendors WHERE name = ?').get(nextStepInfo.vendor);
            if (!nextVendorRow) {
              nextVendorRow = await db.prepare("SELECT * FROM vendors WHERE name LIKE ?").get(nextStepInfo.vendor.slice(0,2) + '%');
            }
            const nextVendorName = nextVendorRow ? nextVendorRow.name : nextStepInfo.vendor;
            // 현재 PO의 품목을 복사
            const currentItems = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(poId);
            const nextHdr = await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, due_date, total_qty, notes, process_step, parent_po_id, process_chain, origin, po_date, material_status, process_status)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,date('now','localtime'),'sent','waiting')`).run(
              nextPoNumber, '후공정', nextVendorName, 'sent',
              po.due_date || '', po.total_qty || 0,
              `공정체인 자동생성: ${nextStepInfo.process}@${nextVendorName} (원PO: ${po.po_number})`,
              currentStep + 1, parentId, po.process_chain || '', po.origin || '한국'
            );
            const nextPoId = nextHdr.lastInsertRowid;
            for (const ci of currentItems) {
              await db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)').run(
                nextPoId, ci.product_code, ci.brand || '', nextStepInfo.process, ci.ordered_qty || 0, ci.spec || '', ''
              );
            }
            nextPO = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(nextPoId);
            console.log(`[공정체인] 자동 PO 생성: ${nextPoNumber} → ${nextVendorName}(${nextStepInfo.process}), parent=${parentId}`);
            logPOActivity(nextPoId, 'auto_chain_create', {
              actor_type: 'system',
              to_status: 'sent',
              details: `공정체인 자동생성: Step ${currentStep}(${vendor.name}) → Step ${currentStep+1}(${nextVendorName}, ${nextStepInfo.process})`
            });
          }

          if (nextPO) {
            const nextVendor = await db.prepare('SELECT * FROM vendors WHERE name = ?').get(nextPO.vendor_name);
            if (nextVendor && nextVendor.email) {
              const nextItems = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(nextPO.po_id);
              await db.prepare(`UPDATE po_header SET status = 'sent', material_status='sent', process_status='waiting', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(nextPO.po_id);
              emailResult = await sendPOEmail(nextPO, nextItems, nextVendor.email, nextVendor.name, true, nextVendor.email_cc);
              console.log(`공정체인 Step ${currentStep}→${currentStep+1}: ${nextPO.po_number} → ${nextVendor.name}`);
            }
          }
          ok(res, { po_id: poId, status: '확인', next_step: currentStep + 1, next_vendor: nextStepInfo.vendor, next_po: nextPO ? nextPO.po_number : null });
          return;
        }

        // 마지막 공정 → OS등록대기 상태로, process_status = completed
        await db.prepare(`UPDATE po_header SET status = 'os_pending', process_status='completed', shipped_at=datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
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
        await db.prepare(`UPDATE po_header SET material_status='shipped', shipped_at=datetime('now','localtime'), updated_at = datetime('now','localtime') WHERE po_id = ?`).run(poId);
        logPOActivity(poId, 'vendor_ship', {
          actor: vendor.name, actor_type: vendor.type,
          from_status: po.status, to_status: po.status,
          from_mat: po.material_status, to_mat: 'shipped',
          from_proc: po.process_status, to_proc: po.process_status,
          details: `${vendor.name} 원재료 출고`
        });
        // 후공정 PO 찾기 (같은 날짜, 대기 상태, 후공정 타입)
        const postPOs = await db.prepare(`SELECT * FROM po_header WHERE po_date = ? AND status IN ('draft','sent') AND po_type = '후공정'`).all(po.po_date);
        const _chainOk = [];
        const _chainNoEmail = [];
        const _chainEmailFail = [];
        for (const pp of postPOs) {
          const postVendor = await db.prepare('SELECT * FROM vendors WHERE name = ?').get(pp.vendor_name);
          if (postVendor && postVendor.email) {
            const ppItems = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(pp.po_id);
            // 후공정 PO를 발송 상태로
            await db.prepare(`UPDATE po_header SET status = 'sent', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(pp.po_id);
            emailResult = await sendPOEmail(pp, ppItems, postVendor.email, postVendor.name, true, postVendor.email_cc);
            console.log(`원재료→후공정 체인: ${pp.po_number} → ${postVendor.name} (${postVendor.email})`);
            if (emailResult && emailResult.ok !== false) {
              _chainOk.push(`${pp.po_number} → ${postVendor.name}`);
            } else {
              _chainEmailFail.push(`${pp.po_number} → ${postVendor.name}`);
            }
          } else {
            _chainNoEmail.push(`${pp.po_number} → ${pp.vendor_name}`);
          }
        }
        // Slack 알림: 체인 결과
        try {
          const lines = [];
          lines.push(`🔗 *원재료 출고 → 후공정 체인*`);
          lines.push(`원재료: ${po.po_number} (${po.vendor_name})`);
          if (_chainOk.length) lines.push(`✅ 이메일 발송 (${_chainOk.length}): \n• ${_chainOk.join('\n• ')}`);
          if (_chainNoEmail.length) lines.push(`⚠️ 이메일 없음 (${_chainNoEmail.length}): \n• ${_chainNoEmail.join('\n• ')}`);
          if (_chainEmailFail.length) lines.push(`🔴 발송 실패 (${_chainEmailFail.length}): \n• ${_chainEmailFail.join('\n• ')}`);
          if (!postPOs.length) lines.push(`❌ 매칭되는 후공정 PO 없음 (po_date=${po.po_date})`);
          sendSlack(lines.join('\n')).catch(()=>{});
        } catch (_) {}
        ok(res, { po_id: poId, status: '확인', chain_triggered: postPOs.length, chain_ok: _chainOk.length, chain_no_email: _chainNoEmail.length, email: emailResult });
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
    const auth = extractVendorAuth(body);
    if (!auth) { fail(res, 403, '인증 실패'); return; }
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(poId);
    if (!po) { fail(res, 404, 'PO 없음'); return; }
    // shipped_at 초기화 (발송 처리 전 상태로)
    await db.prepare(`UPDATE po_header SET shipped_at='', updated_at=datetime('now','localtime') WHERE po_id=?`).run(poId);
    logPOActivity(poId, 'reset_ship', { actor_type: 'vendor', details: '발송처리 수정 요청' });
    ok(res, { po_id: poId });
    return;
  }

  // POST /api/po/:id/reorder — 취소된 PO 재발주
  const reorderMatch = pathname.match(/^\/api\/po\/(\d+)\/reorder$/);
  if (reorderMatch && method === 'POST') {
    const oldPoId = parseInt(reorderMatch[1]);
    const oldPO = await db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(oldPoId);
    if (!oldPO) { fail(res, 404, 'PO not found'); return; }
    if (oldPO.status !== 'cancelled') { fail(res, 400, '취소된 발주만 재발주 가능합니다'); return; }

    const oldItems = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(oldPoId);
    const today = new Date();
    const y = today.getFullYear(), m = String(today.getMonth()+1).padStart(2,'0'), d = String(today.getDate()).padStart(2,'0');
    const dateTag = `${y}${m}${d}`;
    const todayCount = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE po_number LIKE ?`).get(`PO-${dateTag}-%`)).cnt;
    const poNumber = `PO-${dateTag}-${String(todayCount+1).padStart(3,'0')}`;

    // 새 PO 생성
    const totalQty = oldItems.reduce((s, it) => s + (it.ordered_qty || 0), 0);
    const info = _hasEntity.po_header
      ? await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, due_date, total_qty, notes, origin, legal_entity, po_date)
          VALUES (?, ?, ?, 'sent', ?, ?, ?, ?, ?, date('now','localtime'))`).run(
          poNumber, oldPO.po_type, oldPO.vendor_name, oldPO.due_date || oldPO.expected_date || '', totalQty, `재발주 (원본: ${oldPO.po_number})`, oldPO.origin || '', oldPO.legal_entity || 'barunson')
      : await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, due_date, total_qty, notes, origin, po_date)
          VALUES (?, ?, ?, 'sent', ?, ?, ?, ?, date('now','localtime'))`).run(
          poNumber, oldPO.po_type, oldPO.vendor_name, oldPO.due_date || oldPO.expected_date || '', totalQty, `재발주 (원본: ${oldPO.po_number})`, oldPO.origin || '');
    const newPoId = info.lastInsertRowid;

    // 품목 복사
    const insItem = db.prepare('INSERT INTO po_items (po_id, product_code, brand, ordered_qty, received_qty, process_type, spec) VALUES (?,?,?,?,0,?,?)');
    for (const it of oldItems) {
      await insItem.run(newPoId, it.product_code, it.brand || '', it.ordered_qty || 0, it.process_type || '', it.spec || '');
    }

    // 이메일 발송
    let emailSent = false;
    const newPO = await db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(newPoId);
    const newItems = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(newPoId);
    const vendor = await db.prepare('SELECT * FROM vendors WHERE name = ?').get(newPO.vendor_name);
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
    await db.prepare("UPDATE po_header SET material_status='shipped' WHERE po_id=?").run(po_id);
    // 납품 스케줄 상태도 업데이트
    await db.prepare("UPDATE vendor_shipment_schedule SET status='shipped' WHERE po_id=?").run(po_id);
    logPOActivity(po_id, 'material_shipped', { actor_type: 'material', to_mat: 'shipped', details: '원재료 출고 완료' });
    ok(res, { po_id, material_status: 'shipped' });
    return;
  }

  // POST /api/vendor-portal/set-shipment — 업체가 출하 일정 등록/수정
  if (pathname === '/api/vendor-portal/set-shipment' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, ship_date, ship_time, post_vendor_name } = body;
    if (!po_id || !ship_date) { fail(res, 400, '필수 항목 누락'); return; }
    const po = await db.prepare('SELECT po_number, vendor_name FROM po_header WHERE po_id=?').get(po_id);
    if (!po) { fail(res, 404, 'PO 없음'); return; }
    const postVendor = await db.prepare('SELECT email FROM vendors WHERE name=?').get(post_vendor_name || '');
    const postEmail = postVendor ? postVendor.email : '';
    const existing = await db.prepare('SELECT id FROM vendor_shipment_schedule WHERE po_id=?').get(po_id);
    if (existing) {
      await db.prepare(`UPDATE vendor_shipment_schedule SET ship_date=?, ship_time=?, post_vendor_name=?, post_vendor_email=?, updated_at=datetime('now','localtime') WHERE po_id=?`)
        .run(ship_date, ship_time || 'AM', post_vendor_name || '', postEmail, po_id);
    } else {
      await db.prepare(`INSERT INTO vendor_shipment_schedule (po_id, po_number, vendor_name, ship_date, ship_time, post_vendor_name, post_vendor_email) VALUES (?,?,?,?,?,?,?)`)
        .run(po_id, po.po_number, po.vendor_name, ship_date, ship_time || 'AM', post_vendor_name || '', postEmail);
    }
    // 출하 일정 등록 시 원재료 파이프라인 상태를 '출고예정'으로 업데이트
    await db.prepare("UPDATE po_header SET material_status='scheduled' WHERE po_id=?").run(po_id);
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
    const { po_id, item_id, ship_date } = body;
    const authItem = extractVendorAuth(body);
    if (body.access && !authItem) { fail(res, 403, '인증 실패'); return; }
    if (!po_id || !ship_date) { fail(res, 400, '필수 항목 누락'); return; }
    if (item_id !== undefined && item_id !== null) {
      await db.prepare('UPDATE po_items SET ship_date=? WHERE po_id=? AND item_id=?').run(ship_date, po_id, item_id);
    } else {
      await db.prepare('UPDATE po_items SET ship_date=? WHERE po_id=?').run(ship_date, po_id);
    }
    ok(res, { saved: true });
    return;
  }

  // POST /api/vendor-portal/item-produced-qty — 품목별 생산완료수량 저장
  if (pathname === '/api/vendor-portal/item-produced-qty' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, item_id, produced_qty } = body;
    const authPQ = extractVendorAuth(body);
    if (body.access && !authPQ) { fail(res, 403, '인증 실패'); return; }
    if (!po_id || produced_qty === undefined) { fail(res, 400, '필수 항목 누락'); return; }
    const qty = parseInt(produced_qty) || 0;
    if (item_id !== undefined && item_id !== null) {
      await db.prepare('UPDATE po_items SET produced_qty=? WHERE po_id=? AND item_id=?').run(qty, po_id, item_id);
    } else {
      await db.prepare('UPDATE po_items SET produced_qty=? WHERE po_id=?').run(qty, po_id);
    }
    ok(res, { saved: true });
    return;
  }

  // POST /api/vendor-portal/defect-report — 불량 보고
  if (pathname === '/api/vendor-portal/defect-report' && method === 'POST') {
    const body = await readJSON(req);
    const auth = extractVendorAuth(body);
    if (body.access && !auth) { fail(res, 403, '인증 실패'); return; }
    const { po_id, item_id, product_code, defect_type, defect_qty, description } = body;
    if (!po_id || !defect_type || !defect_qty) { fail(res, 400, '필수 항목 누락 (po_id, defect_type, defect_qty)'); return; }
    const vendorName = auth ? auth.vendorName : '';
    const qty = parseInt(defect_qty) || 0;
    await db.prepare(`INSERT INTO vendor_defect_reports (po_id, item_id, product_code, vendor_name, defect_type, defect_qty, description) VALUES (?,?,?,?,?,?,?)`).run(
      po_id, item_id || null, product_code || '', vendorName, defect_type, qty, description || ''
    );
    // po_items에 불량수량 누적
    if (item_id) {
      await db.prepare('UPDATE po_items SET defect_qty = defect_qty + ? WHERE po_id=? AND item_id=?').run(qty, po_id, item_id);
    }
    ok(res, { saved: true });
    return;
  }

  // GET /api/vendor-portal/defect-reports — 불량 보고 목록
  if (pathname === '/api/vendor-portal/defect-reports' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const accessToken = qs.get('access') || '';
    const decoded = decodeVendorToken(accessToken);
    if (!decoded) { fail(res, 403, '인증 실패'); return; }
    const email = decoded.email;
    const vendor = await db.prepare('SELECT * FROM vendors WHERE email = ?').get(email);
    if (!vendor) { fail(res, 404, '업체 없음'); return; }
    const reports = await db.prepare(`SELECT r.*, h.po_number FROM vendor_defect_reports r LEFT JOIN po_header h ON h.po_id=r.po_id WHERE r.vendor_name=? ORDER BY r.reported_at DESC LIMIT 100`).all(vendor.name);
    ok(res, reports);
    return;
  }

  // POST /api/vendor-portal/items-ship-dates — 품목별 출고일 일괄 저장
  if (pathname === '/api/vendor-portal/items-ship-dates' && method === 'POST') {
    const body = await readJSON(req);
    const { po_id, dates } = body;
    const authDates = extractVendorAuth(body);
    if (body.access && !authDates) { fail(res, 403, '인증 실패'); return; }
    if (!po_id || !Array.isArray(dates)) { fail(res, 400, '필수 항목 누락'); return; }
    const stmt = db.prepare('UPDATE po_items SET ship_date=? WHERE po_id=? AND item_id=?');
    const tx = db.transaction(async () => { for (const d of dates) await stmt.run(d.ship_date, po_id, d.item_id); });
    await tx();
    ok(res, { saved: dates.length });
    return;
  }

  // GET /api/vendor-portal/shipment-schedule — 출하 일정 조회
  if (pathname === '/api/vendor-portal/shipment-schedule' && method === 'GET') {
    const poId = parsed.searchParams.get('po_id');
    if (poId) {
      const schedule = await db.prepare('SELECT * FROM vendor_shipment_schedule WHERE po_id=?').get(poId);
      ok(res, schedule || null);
    } else {
      const all = await db.prepare('SELECT * FROM vendor_shipment_schedule ORDER BY ship_date').all();
      ok(res, all);
    }
    return;
  }

  // GET /api/vendor-portal/lead-time — 벤더 포털 공정 리드타임 조회
  if (pathname === '/api/vendor-portal/lead-time' && method === 'GET') {
    const qsLt = Object.fromEntries(parsed.searchParams);
    const authLt = extractVendorAuth(qsLt);
    if (!authLt) { fail(res, 403, '인증 실패'); return; }
    const vendorName = authLt.vendorName || parsed.searchParams.get('vendor_name') || '';
    if (!vendorName) { fail(res, 400, 'vendor_name 필수'); return; }

    const DEFAULT_LEAD_TIMES = [
      { process_type: '재단', default_days: 1 },
      { process_type: '인쇄', default_days: 3 },
      { process_type: '박/형압', default_days: 2 },
      { process_type: '톰슨', default_days: 2 },
      { process_type: '봉투가공', default_days: 3 },
      { process_type: '단면접착', default_days: 2 },
      { process_type: '우찌누끼', default_days: 2 },
      { process_type: '접지', default_days: 2 },
      { process_type: '코팅', default_days: 2 },
    ];

    const saved = await db.prepare('SELECT * FROM process_lead_time WHERE vendor_name=?').all(vendorName);
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
    const history = await db.prepare('SELECT process_type, old_days, new_days, changed_at FROM lead_time_history WHERE vendor_name=? ORDER BY changed_at DESC LIMIT 20').all(vendorName);
    ok(res, { rows: result, history });
    return;
  }

  // POST /api/vendor-portal/lead-time — 벤더 포털 공정 리드타임 저장
  if (pathname === '/api/vendor-portal/lead-time' && method === 'POST') {
    const body = await readJSON(req);
    const authLtPost = extractVendorAuth(body);
    if (!authLtPost) { fail(res, 403, '인증 실패'); return; }
    const vendor_name = authLtPost.vendorName || body.vendor_name || '';
    const lead_times = body.lead_times;
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
    const prevRows = await db.prepare('SELECT process_type, adjusted_days, default_days FROM process_lead_time WHERE vendor_name=?').all(vendor_name);
    for (const r of prevRows) prevMap[r.process_type] = r.adjusted_days ?? r.default_days;

    const logStmt = db.prepare('INSERT INTO lead_time_history (vendor_name, process_type, old_days, new_days) VALUES (?,?,?,?)');

    const upsertAll = db.transaction(async (items) => {
      for (const lt of items) {
        const oldVal = prevMap[lt.process_type] ?? lt.default_days;
        const newVal = lt.adjusted_days ?? lt.default_days;
        if (oldVal !== newVal) {
          await logStmt.run(vendor_name, lt.process_type, oldVal, newVal);
        }
        await upsert.run(vendor_name, lt.process_type, lt.default_days ?? 1, lt.adjusted_days ?? null, lt.adjusted_reason || '');
      }
    });
    await upsertAll(lead_times);

    console.log(`[vendor-portal/lead-time] ${vendor_name} (${email}) — ${lead_times.length}개 공정 리드타임 저장`);
    ok(res, { ok: true, vendor_name, saved: lead_times.length });
    return;
  }

  // GET /api/vendor-portal/trade-doc — 업체 포털 거래명세서 조회
  if (pathname === '/api/vendor-portal/trade-doc' && method === 'GET') {
    const poId = parsed.searchParams.get('po_id');
    const qsTd = Object.fromEntries(parsed.searchParams);
    const authTd = extractVendorAuth(qsTd);
    if (!authTd) { fail(res, 403, '인증 실패'); return; }
    const doc = await db.prepare('SELECT * FROM trade_document WHERE po_id=? ORDER BY id DESC LIMIT 1').get(poId);
    if (!doc) { ok(res, null); return; }
    doc.items = JSON.parse(doc.items_json || '[]');
    doc.vendor_modified = doc.vendor_modified_json ? JSON.parse(doc.vendor_modified_json) : null;
    ok(res, doc);
    return;
  }

  // POST /api/vendor-portal/update-trade-doc — 업체 포털 거래명세서 단가 수정
  if (pathname === '/api/vendor-portal/update-trade-doc' && method === 'POST') {
    const body = await readJSON(req);
    const { doc_id, modified_items, memo } = body;
    const authUtd = extractVendorAuth(body);
    if (!authUtd) { fail(res, 403, '인증 실패'); return; }
    if (!doc_id) { fail(res, 400, 'doc_id 필수'); return; }
    const doc = await db.prepare('SELECT * FROM trade_document WHERE id=?').get(doc_id);
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

    await db.prepare(`UPDATE trade_document SET vendor_modified_json=?, vendor_memo=?, price_diff=?, status='vendor_confirmed', confirmed_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?`)
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
      const rows = await db.prepare('SELECT * FROM process_lead_time WHERE vendor_name=?').all(vn);
      ok(res, rows);
    } else {
      const rows = await db.prepare('SELECT * FROM process_lead_time ORDER BY vendor_name, process_type').all();
      ok(res, rows);
    }
    return;
  }

  // POST /api/process-lead-time — 공정 리드타임 등록/수정
  if (pathname === '/api/process-lead-time' && method === 'POST') {
    const body = await readJSON(req);
    const { vendor_name, process_type, default_days, adjusted_days, adjusted_reason } = body;
    if (!vendor_name || !process_type) { fail(res, 400, '필수 항목 누락'); return; }
    await db.prepare(`INSERT INTO process_lead_time (vendor_name, process_type, default_days, adjusted_days, adjusted_reason)
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
    ok(res, await db.prepare(sql).all(...params));
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
    ok(res, await db.prepare(sql).all(...params));
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
    ok(res, await db.prepare(sql).all(...params));
    return;
  }

  // GET /api/post-process/summary — 후공정 단가 요약 (대시보드용)
  if (pathname === '/api/post-process/summary' && method === 'GET') {
    const vendor = parsed.searchParams.get('vendor_name') || '코리아패키지';
    const isAll = !parsed.searchParams.get('vendor_name');
    const whereVendor = isAll ? '1=1' : 'vendor_name=?';
    const vendorParam = isAll ? [] : [vendor];

    // 월별 총액
    const monthly = await db.prepare(`SELECT month, SUM(amount) as total, COUNT(*) as cnt FROM post_process_history WHERE ${whereVendor} GROUP BY month ORDER BY month`).all(...vendorParam);

    // 공정별 총액
    const byProcess = await db.prepare(`SELECT process_type, SUM(amount) as total, COUNT(*) as cnt, AVG(unit_price) as avg_price FROM post_process_history WHERE ${whereVendor} AND unit_price>0 GROUP BY process_type ORDER BY total DESC`).all(...vendorParam);

    // 단가 변동 감지 (같은 제품+공정인데 단가가 다른 경우)
    const priceChanges = await db.prepare(`
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
    const topProducts = await db.prepare(`
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
      const vendorStats = await db.prepare(`
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
      const vendorGapRows = await db.prepare(`
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
    const mapping = await db.prepare(
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
    const po = await db.prepare('SELECT po_number FROM po_header WHERE po_id=?').get(po_id);
    if (!po) { fail(res, 404, 'PO 없음'); return; }
    const r = await db.prepare(`INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')`)
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
    const _tdEnt = parsed.searchParams.get('entity');
    if (_tdEnt && _tdEnt !== 'all' && _hasEntity.trade_document) { q += ' AND legal_entity=?'; args.push(_tdEnt); }
    q += ' ORDER BY created_at DESC';
    ok(res, await db.prepare(q).all(...args));
    return;
  }

  // PATCH /api/trade-document/:id — 거래명세서 수정 (업체 확인, 관리자 승인 등)
  const tradeDocPatch = pathname.match(/^\/api\/trade-document\/(\d+)$/);
  if (tradeDocPatch && method === 'PATCH') {
    const docId = tradeDocPatch[1];
    const doc = await db.prepare('SELECT * FROM trade_document WHERE id=?').get(docId);
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
    await db.prepare(`UPDATE trade_document SET ${sets.join(',')} WHERE id=?`).run(...vals);
    ok(res, { id: parseInt(docId) });
    return;
  }

  // GET /api/trade-document/review — 검토 대기 목록 (vendor_confirmed)
  if (pathname === '/api/trade-document/review' && method === 'GET') {
    const docs = await db.prepare(`SELECT * FROM trade_document WHERE status='vendor_confirmed' ORDER BY confirmed_at DESC`).all();
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
    const doc = await db.prepare('SELECT * FROM trade_document WHERE id=?').get(docId);
    if (!doc) { fail(res, 404, '문서 없음'); return; }

    if (doc.price_diff && !doc.vendor_memo) {
      fail(res, 400, '단가 차이가 있으나 수정 사유가 없습니다. 업체에 사유 입력을 요청하세요.');
      return;
    }

    await db.prepare(`UPDATE trade_document SET status='approved', approved_at=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?`).run(docId);

    logPOActivity(doc.po_id, 'trade_doc_approved', {
      actor_type: 'admin',
      details: doc.price_diff ? `거래명세서 승인 (단가 수정 있음, 사유: ${doc.vendor_memo})` : '거래명세서 승인'
    });

    ok(res, { doc_id: docId, status: 'approved' });
    return;
  }

  // POST /api/trade-document/:id/create-po — 거래명세서 → 신규 발주서 역변환
  // (단가 수정된 거래명세서 또는 외부 업로드 명세서로부터 새 PO 생성)
  const tdCreatePoMatch = pathname.match(/^\/api\/trade-document\/(\d+)\/create-po$/);
  if (tdCreatePoMatch && method === 'POST') {
    const docId = parseInt(tdCreatePoMatch[1]);
    const body = await readJSON(req).catch(() => ({}));
    const doc = await db.prepare('SELECT * FROM trade_document WHERE id=?').get(docId);
    if (!doc) { fail(res, 404, '거래명세서 없음'); return; }
    // 우선순위: 사용자 전달 items > vendor_modified > items_json
    let items = [];
    try {
      if (body.items && Array.isArray(body.items)) items = body.items;
      else if (doc.vendor_modified_json) items = JSON.parse(doc.vendor_modified_json) || [];
      else items = JSON.parse(doc.items_json || '[]');
    } catch (e) { fail(res, 400, '명세서 items 파싱 실패: ' + e.message); return; }
    if (!items.length) { fail(res, 400, 'items가 비어있습니다'); return; }
    const vendor = body.vendor_name || doc.vendor_name || '';
    if (!vendor) { fail(res, 400, 'vendor_name 없음'); return; }
    const poType = body.po_type || (doc.vendor_type === 'process' ? '후공정' : '원재료');
    const totalQty = items.reduce((s, i) => s + (Number(i.ordered_qty || i.qty || 0)), 0);
    const totalAmt = items.reduce((s, i) => s + (Number(i.ordered_qty || i.qty || 0) * Number(i.unit_price || 0)), 0);
    const poNum = 'PO-RV-' + new Date().toISOString().slice(0,10).replace(/-/g,'') + '-' + String(Math.floor(Math.random()*900)+100);
    const notes = (body.notes || `거래명세서 #${docId} 역변환`) + (totalAmt ? ` (예상금액 ${Math.round(totalAmt).toLocaleString()}원)` : '');

    const tx = db.transaction(async () => {
      const info = await db.prepare(`INSERT INTO po_header
        (po_number, po_type, vendor_name, status, due_date, total_qty, notes, parent_po_id, origin, po_date)
        VALUES (?,?,?,?,?,?,?,?,?,date('now','localtime'))`).run(
        poNum, poType, vendor, 'draft',
        body.due_date || '', totalQty, notes,
        doc.po_id || null, '거래명세서역변환'
      );
      const newPoId = info.lastInsertRowid;
      const stmt = db.prepare(`INSERT INTO po_items
        (po_id, product_code, brand, process_type, ordered_qty, spec, notes)
        VALUES (?,?,?,?,?,?,?)`);
      for (const it of items) {
        const unitPrice = Number(it.unit_price || 0);
        const noteParts = [];
        if (it.product_name) noteParts.push(it.product_name);
        if (unitPrice) noteParts.push(`@${unitPrice.toLocaleString()}`);
        await stmt.run(
          newPoId,
          it.product_code || '',
          it.brand || '',
          it.process_type || '',
          Number(it.ordered_qty || it.qty || 0),
          it.spec || '',
          noteParts.join(' ')
        );
      }
      await db.prepare(`INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)`).run(
        newPoId, 'created_from_trade_doc', body.actor || 'system',
        `거래명세서 #${docId} (${doc.po_number || ''}) 에서 역변환 생성`
      );
      // 원본 PO에도 로그 (있다면)
      if (doc.po_id) {
        await db.prepare(`INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)`).run(
          doc.po_id, 'reverse_po_created', body.actor || 'system',
          `거래명세서 #${docId} → 신규 PO ${poNum}`
        );
      }
      return newPoId;
    });
    const newPoId = await tx();
    ok(res, { po_id: newPoId, po_number: poNum, total_qty: totalQty, total_amount: totalAmt, items_count: items.length });
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

    const docs = await db.prepare(q).all(...args);

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
        const amount = Math.round((currentPrice || 0) * (qty || 0));
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
    const vendorList = (await db.prepare(`SELECT DISTINCT vendor_name FROM trade_document WHERE status IN ('sent','vendor_confirmed','approved') ORDER BY vendor_name`).all()).map(r => r.vendor_name);

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

    const docs = await db.prepare(q).all(...args);
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
        const amount = Math.round((price || 0) * qty);
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
      const tx = db.transaction(async () => {
        for (const io of itemOS) {
          if (io.os_number) await stmt.run(io.os_number, io.item_id, poId);
        }
      });
      await tx();
      // PO 헤더에도 첫 번째 OS번호 기록 (대표값)
      const firstOS = itemOS.find(i => i.os_number)?.os_number || osNumber;
      if (firstOS) {
        await db.prepare(`UPDATE po_header SET os_number = ?, status = 'os_registered', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(firstOS, poId);
      }
      logPOActivity(poId, 'os_registered', { actor_type: 'admin', to_status: 'os_registered', details: `제품별 OS번호 등록 (${itemOS.filter(i=>i.os_number).length}건)` });
      ok(res, { ok: true, po_id: poId, item_count: itemOS.filter(i=>i.os_number).length, status: 'os_registered' });
    } else if (osNumber) {
      // PO 전체 OS번호 등록
      const curPO = await db.prepare('SELECT status FROM po_header WHERE po_id=?').get(poId);
      const shouldChangeStatus = curPO && curPO.status === 'os_pending';
      if (shouldChangeStatus) {
        await db.prepare(`UPDATE po_header SET os_number = ?, status = 'os_registered', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(osNumber, poId);
      } else {
        await db.prepare(`UPDATE po_header SET os_number = ?, updated_at = datetime('now','localtime') WHERE po_id = ?`).run(osNumber, poId);
      }
      // 모든 아이템에도 동일 OS번호 적용
      await db.prepare('UPDATE po_items SET os_number=? WHERE po_id=?').run(osNumber, poId);
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
    const logs = await db.prepare('SELECT * FROM po_activity_log WHERE po_id=? ORDER BY created_at DESC').all(poId);
    ok(res, logs);
    return;
  }

  // GET /api/activity-log — 전체 활동 로그 (최근 100건)
  if (pathname === '/api/activity-log' && method === 'GET') {
    const limit = parseInt(parsed.searchParams.get('limit') || '100');
    const logs = await db.prepare('SELECT * FROM po_activity_log ORDER BY created_at DESC LIMIT ?').all(limit);
    ok(res, logs);
    return;
  }

  // GET /api/po/os-pending — OS등록 대기 PO 목록 (os_pending + os_registered)
  if (pathname === '/api/po/os-pending' && method === 'GET') {
    const rows = await db.prepare(`SELECT * FROM po_header WHERE status IN ('os_pending','os_registered') ORDER BY po_date DESC`).all();
    for (const r of rows) {
      r.status = PO_STATUS_EN_TO_KO[r.status] || r.status;
      r.items = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(r.po_id);
    }
    ok(res, rows);
    return;
  }

  // GET /api/po/os-match — XERP OS번호 자동 매칭
  if (pathname === '/api/po/os-match' && method === 'GET') {
    if (!await ensureXerpPool()) { fail(res, 503, 'XERP 데이터베이스 미연결 (재연결 시도 중)'); return; }
    try {
      // 1. 모든 PO 가져오기
      const allPOs = await db.prepare(`SELECT * FROM po_header ORDER BY po_date DESC`).all();
      const itemStmt = db.prepare('SELECT * FROM po_items WHERE po_id = ?');
      for (const po of allPOs) po.items = await itemStmt.all(po.po_id);

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
      for (const po of pending) {
        po.status = PO_STATUS_EN_TO_KO[po.status] || po.status;
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
      const registeredPOs = await db.prepare(
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
            await db.prepare("UPDATE po_header SET status='os_pending', updated_at=datetime('now','localtime') WHERE po_id=?").run(rpo.po_id);
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
            await db.prepare("UPDATE po_header SET status='received', process_status='completed', material_status='received', updated_at=datetime('now','localtime') WHERE po_id=?").run(rpo.po_id);
            verified.push({ ...rpo, status: 'OS검증대기', xerp_status: '검증완료', auto_completed: true });
          } else {
            // 불일치 → os_pending으로 되돌리고 os_number 초기화
            await db.prepare("UPDATE po_header SET status='os_pending', os_number='', updated_at=datetime('now','localtime') WHERE po_id=?").run(rpo.po_id);
            mismatched.push({
              ...rpo,
              error: `OS번호와 제품코드가 다릅니다 (OS: ${rpo.os_number}, XERP품목: ${xerpItemCodes.join(',')}, PO원자재: ${materialCodes.join(',')})`
            });
          }
        }
      }

      // 완료 PO도 한글 변환
      for (const po of completed) po.status = PO_STATUS_EN_TO_KO[po.status] || po.status;

      // 취소 PO도 한글 변환
      for (const po of cancelled) po.status = PO_STATUS_EN_TO_KO[po.status] || po.status;

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
    const rows = await db.prepare(`
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
    const result = await Promise.all(rows.map(async r => {
      const ltRows = await db.prepare(`
        SELECT po_date, updated_at, due_date as expected_date FROM po_header
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
    }));
    ok(res, result);
    return;
  }

  // GET /api/vendor-performance — 업체 종합 성과 (납기준수율 + 불량률 + 종합점수)
  if (pathname === '/api/vendor-performance' && method === 'GET') {
    const qs = new URL(req.url, `http://${req.headers.host}`).searchParams;
    const today = new Date();
    const defaultFrom = new Date(today.getFullYear(), today.getMonth() - 5, 1).toISOString().slice(0, 10); // 최근 6개월
    const from = qs.get('from') || defaultFrom;
    const to = qs.get('to') || today.toISOString().slice(0, 10);
    const minOrders = parseInt(qs.get('min_orders') || '1', 10);

    // 1) 발주 통계 (업체별)
    const poStats = await db.prepare(`
      SELECT vendor_name,
        COUNT(*) as order_count,
        COALESCE(SUM(total_qty), 0) as total_qty,
        COALESCE(SUM(CASE WHEN status IN ('received','os_pending') THEN 1 ELSE 0 END), 0) as completed_count,
        COALESCE(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END), 0) as cancel_count
      FROM po_header
      WHERE po_date >= ? AND po_date <= ? AND vendor_name IS NOT NULL AND vendor_name != ''
      GROUP BY vendor_name
    `).all(from, to);

    // 2) 납기 준수율 + 평균 리드타임 (업체별)
    const ltMap = {};
    for (const r of poStats) {
      const ltRows = await db.prepare(`
        SELECT po_date, updated_at, due_date as expected_date FROM po_header
        WHERE vendor_name = ? AND po_date >= ? AND po_date <= ?
          AND status IN ('received','os_pending') AND po_date IS NOT NULL AND updated_at IS NOT NULL
      `).all(r.vendor_name, from, to);
      let totalLT = 0, ltCount = 0, onTimeCount = 0, withExpected = 0;
      ltRows.forEach(p => {
        const d1 = new Date(p.po_date), d2 = new Date(p.updated_at);
        if (d1 && d2 && d2 > d1) {
          const days = Math.round((d2 - d1) / 86400000);
          if (days > 0 && days < 90) { totalLT += days; ltCount++; }
        }
        if (p.expected_date) {
          withExpected++;
          if (p.updated_at <= p.expected_date + ' 23:59:59') onTimeCount++;
        }
      });
      ltMap[r.vendor_name] = {
        avg_lead_time: ltCount > 0 ? Math.round(totalLT / ltCount * 10) / 10 : 0,
        on_time_rate: withExpected > 0 ? Math.round(onTimeCount / withExpected * 100) : null
      };
    }

    // 3) 불량 통계 (업체별)
    const defectStats = await db.prepare(`
      SELECT vendor_name,
        COUNT(*) as defect_count,
        COALESCE(SUM(defect_qty), 0) as total_defect_qty,
        COALESCE(SUM(claim_amount), 0) as total_claim_amount,
        COALESCE(SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END), 0) as resolved_count
      FROM defects
      WHERE defect_date >= ? AND defect_date <= ?
      GROUP BY vendor_name
    `).all(from, to);
    const defectMap = {};
    defectStats.forEach(d => { defectMap[d.vendor_name] = d; });

    // 4) 결합 + 종합 점수 (PostgreSQL은 bigint를 string으로 반환 → Number() 변환 필수)
    const result = poStats
      .map(r => ({
        vendor_name: r.vendor_name,
        order_count: Number(r.order_count) || 0,
        total_qty: Number(r.total_qty) || 0,
        completed_count: Number(r.completed_count) || 0,
        cancel_count: Number(r.cancel_count) || 0
      }))
      .filter(r => r.order_count >= minOrders)
      .map(r => {
        const lt = ltMap[r.vendor_name] || { avg_lead_time: 0, on_time_rate: null };
        const dfRaw = defectMap[r.vendor_name] || { defect_count: 0, total_defect_qty: 0, total_claim_amount: 0, resolved_count: 0 };
        const df = {
          defect_count: Number(dfRaw.defect_count) || 0,
          total_defect_qty: Number(dfRaw.total_defect_qty) || 0,
          total_claim_amount: Number(dfRaw.total_claim_amount) || 0,
          resolved_count: Number(dfRaw.resolved_count) || 0
        };
        const defectRate = r.total_qty > 0 ? Math.round(df.total_defect_qty / r.total_qty * 1000) / 10 : 0;
        // 종합 점수 (0~100):
        //   납기 준수율(50%) + 품질(50%, 100 - defect_rate*5, clamp 0~100)
        const onTime = lt.on_time_rate != null ? lt.on_time_rate : 80; // 기대일 없으면 평균값 80
        const quality = Math.max(0, Math.min(100, 100 - defectRate * 5));
        const score = Math.round(onTime * 0.5 + quality * 0.5);
        let grade = 'D';
        if (score >= 90) grade = 'A';
        else if (score >= 75) grade = 'B';
        else if (score >= 60) grade = 'C';
        return {
          vendor_name: r.vendor_name,
          order_count: r.order_count,
          total_qty: r.total_qty,
          completed_count: r.completed_count,
          cancel_count: r.cancel_count,
          completion_rate: r.order_count > 0 ? Math.round(r.completed_count / r.order_count * 100) : 0,
          avg_lead_time: lt.avg_lead_time,
          on_time_rate: lt.on_time_rate,
          defect_count: df.defect_count,
          total_defect_qty: df.total_defect_qty,
          total_claim_amount: df.total_claim_amount,
          defect_resolved: df.resolved_count,
          defect_rate: defectRate,
          score,
          grade
        };
      })
      .sort((a, b) => b.score - a.score);

    // 요약 통계
    const summary = {
      total_vendors: result.length,
      grade_a: result.filter(r => r.grade === 'A').length,
      grade_b: result.filter(r => r.grade === 'B').length,
      grade_c: result.filter(r => r.grade === 'C').length,
      grade_d: result.filter(r => r.grade === 'D').length,
      avg_score: result.length > 0 ? Math.round(result.reduce((s, r) => s + r.score, 0) / result.length) : 0,
      avg_on_time: (() => {
        const v = result.filter(r => r.on_time_rate != null);
        return v.length > 0 ? Math.round(v.reduce((s, r) => s + r.on_time_rate, 0) / v.length) : 0;
      })(),
      total_defects: result.reduce((s, r) => s + r.defect_count, 0),
      total_claim: result.reduce((s, r) => s + r.total_claim_amount, 0),
      from, to
    };

    ok(res, { summary, vendors: result });
    return;
  }

  // GET /api/po/stats — 대시보드 전용 통계
  if (pathname === '/api/po/stats' && method === 'GET') {
    const allPO = await db.prepare('SELECT * FROM po_header ORDER BY po_date DESC, po_id DESC').all();
    // 상태 정규화
    for (const r of allPO) r.status = PO_STATUS_EN_TO_KO[r.status] || r.status;

    // 파이프라인
    const pipeline = {}, pipelineQty = {};
    for (const s of ['대기','발송','확인','수령중','완료','취소']) { pipeline[s] = 0; pipelineQty[s] = 0; }
    for (const r of allPO) { pipeline[r.status] = (pipeline[r.status]||0) + 1; pipelineQty[r.status] = (pipelineQty[r.status]||0) + (r.total_qty||0); }

    // 입고율
    const itemStats = await db.prepare('SELECT COALESCE(SUM(ordered_qty),0) as ordered, COALESCE(SUM(received_qty),0) as received FROM po_items').get();
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
    const entity = parsed.searchParams.get('entity');
    if (status) { sql += ' AND status = ?'; params.push(status); }
    if (vendor) { sql += ' AND vendor_name LIKE ?'; params.push('%' + vendor + '%'); }
    if (from) { sql += ' AND po_date >= ?'; params.push(from); }
    if (to) { sql += ' AND po_date <= ?'; params.push(to); }
    if (origin) { sql += ' AND origin = ?'; params.push(origin); }
    if (entity && entity !== 'all' && _hasEntity.po_header) { sql += ' AND legal_entity = ?'; params.push(entity); }
    sql += ' ORDER BY po_date DESC, po_id DESC';
    const rows = await db.prepare(sql).all(...params);
    // 상태 영→한 정규화
    for (const row of rows) {
      row.status = PO_STATUS_EN_TO_KO[row.status] || row.status;
    }
    // include=items / include=progress 지원 (comma-separated 허용)
    const includeParam = parsed.searchParams.get('include') || '';
    if (includeParam === 'items' || includeParam.includes('items')) {
      const itemStmt = db.prepare('SELECT * FROM po_items WHERE po_id = ?');
      for (const row of rows) {
        row.items = await itemStmt.all(row.po_id);
      }
    }
    // include=progress 시 후공정 체인(자식 PO) 요약 포함 — 발주현황 목록 미니바용 (N+1 방지)
    if (includeParam === 'progress' || includeParam.includes('progress')) {
      const rootIds = rows.filter(r => !r.parent_po_id).map(r => r.po_id);
      const childrenByParent = {};
      if (rootIds.length) {
        const placeholders = rootIds.map(() => '?').join(',');
        const children = await db.prepare(
          `SELECT po_id, parent_po_id, process_step, status, po_type, vendor_name, process_vendor_name, shipped_at FROM po_header WHERE parent_po_id IN (${placeholders}) ORDER BY process_step ASC, po_id ASC`
        ).all(...rootIds);
        for (const c of children) {
          if (!childrenByParent[c.parent_po_id]) childrenByParent[c.parent_po_id] = [];
          childrenByParent[c.parent_po_id].push({
            po_id: c.po_id,
            step: c.process_step || 0,
            status: PO_STATUS_EN_TO_KO[c.status] || c.status,
            raw_status: c.status,
            vendor: c.process_vendor_name || c.vendor_name || '',
            shipped_at: c.shipped_at || ''
          });
        }
      }
      for (const row of rows) {
        row._children = childrenByParent[row.po_id] || [];
      }
    }
    ok(res, rows);
    return;
  }

  // GET /api/po/:id
  const poGet = pathname.match(/^\/api\/po\/(\d+)$/);
  if (poGet && method === 'GET') {
    const id = parseInt(poGet[1]);
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    po.items = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(id);
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
        const poNumber = await generatePoNumber();
        const totalQty = vendorItems.reduce((s, it) => s + (parseInt(it.qty) || 0), 0);
        // origin/legal_entity: 첫 번째 품목 기준
        const _bulkFirstProd = await db.prepare(`SELECT ${_hasEntity.products ? 'origin, legal_entity' : 'origin'} FROM products WHERE product_code=?`).get(vendorItems[0].product_code || '');
        const _bulkOrigin = (_bulkFirstProd && _bulkFirstProd.origin) || '';
        const _bulkEntity = (_bulkFirstProd && _bulkFirstProd.legal_entity === 'dd') ? 'dd' : 'barunson';
        const tx = db.transaction(async () => {
          const hdr = _hasEntity.po_header
            ? await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, material_status, process_status, origin, legal_entity, po_date)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(
                poNumber, '원재료', vendorName, 'draft', totalQty, '엑셀 일괄 발주', 'sent', 'waiting', _bulkOrigin, _bulkEntity, today)
            : await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, material_status, process_status, origin, po_date)
                VALUES (?,?,?,?,?,?,?,?,?,?)`).run(
                poNumber, '원재료', vendorName, 'draft', totalQty, '엑셀 일괄 발주', 'sent', 'waiting', _bulkOrigin, today);
          for (const it of vendorItems) {
            await db.prepare('INSERT INTO po_items (po_id, product_code, ordered_qty, notes) VALUES (?,?,?,?)').run(
              hdr.lastInsertRowid, it.product_code || '', parseInt(it.qty) || 0, '엑셀 일괄'
            );
          }
          return { po_id: Number(hdr.lastInsertRowid), po_number: poNumber };
        });
        const result = await tx();
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

  // POST /api/slack/manual-po-notify — 수동 일괄 발주 완료 알림
  if (pathname === '/api/slack/manual-po-notify' && method === 'POST') {
    try {
      const body = await readJSON(req);
      const savedVendors = Array.isArray(body.saved_vendors) ? body.saved_vendors : [];
      const emailOk = Array.isArray(body.email_ok) ? body.email_ok : [];
      const emailFail = Array.isArray(body.email_fail) ? body.email_fail : [];
      const origin = body.origin || '';
      const totalQty = Number(body.total_qty || 0);
      if (_slackWebhookUrl && savedVendors.length) {
        const lines = [];
        lines.push(`📮 *수동 발주 완료* (${origin || '국가미지정'})`);
        lines.push(`저장: ${savedVendors.length}건 / 수량합: ${totalQty.toLocaleString()}`);
        lines.push(`• ${savedVendors.join(', ')}`);
        if (emailOk.length) lines.push(`✅ 이메일 발송 (${emailOk.length}): ${emailOk.join(', ')}`);
        if (emailFail.length) lines.push(`⚠️ 이메일 미발송 (${emailFail.length}): ${emailFail.join(', ')}`);
        sendSlack(lines.join('\n')).catch(()=>{});
      }
      ok(res, { notified: !!_slackWebhookUrl });
    } catch (e) {
      fail(res, 500, e.message);
    }
    return;
  }

  if (pathname === '/api/po' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    const totalQty = items.reduce((s, it) => s + (it.ordered_qty || 0), 0);

    // vendor_id로 vendor_name 자동 조회
    let vendorName = body.vendor_name || '';
    if (!vendorName && body.vendor_id) {
      const v = await db.prepare('SELECT name FROM vendors WHERE vendor_id = ?').get(body.vendor_id);
      if (v) vendorName = v.name;
    }

    // origin 결정: body에서 직접 지정 또는 첫 번째 품목의 products.origin 사용
    let poOrigin = body.origin || '';
    let poEntity = body.legal_entity || '';
    if ((!poOrigin || !poEntity) && items.length) {
      const _selCols = _hasEntity.products ? 'origin, legal_entity' : 'origin';
      const firstProd = await db.prepare(`SELECT ${_selCols} FROM products WHERE product_code=?`).get(items[0].product_code || '');
      if (firstProd) {
        if (!poOrigin && firstProd.origin) poOrigin = firstProd.origin;
        if (!poEntity && firstProd.legal_entity) poEntity = firstProd.legal_entity;
      }
    }
    if (poEntity !== 'dd') poEntity = 'barunson';

    // 중복 발주 방지: 같은 날짜+같은 거래처+같은 품목 조합이 이미 있으면 차단
    if (vendorName && items.length) {
      const today = new Date().toISOString().slice(0, 10);
      const productCodes = items.map(it => it.product_code).filter(Boolean).sort().join(',');
      const dupCheck = await db.prepare(`SELECT po_id, po_number FROM po_header WHERE vendor_name = ? AND po_date::text >= ? AND status != 'cancelled' ORDER BY po_id DESC LIMIT 1`).get(vendorName, today);
      if (dupCheck) {
        const dupItems = await db.prepare('SELECT product_code FROM po_items WHERE po_id = ? ORDER BY product_code').all(dupCheck.po_id);
        const existingCodes = dupItems.map(r => r.product_code).filter(Boolean).sort().join(',');
        if (existingCodes === productCodes) {
          fail(res, 409, `중복 발주: 오늘 동일 거래처(${vendorName})에 같은 품목으로 이미 발주(${dupCheck.po_number})가 생성되었습니다.`);
          return;
        }
      }
    }

    const poNumber = await generatePoNumber();

    const tx = db.transaction(async () => {
      const _poSql = _hasEntity.po_header
        ? `INSERT INTO po_header (po_number, po_type, vendor_name, status, due_date, total_qty, notes, process_step, parent_po_id, process_chain, origin, legal_entity, po_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now','localtime'))`
        : `INSERT INTO po_header (po_number, po_type, vendor_name, status, due_date, total_qty, notes, process_step, parent_po_id, process_chain, origin, po_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now','localtime'))`;
      const _poArgs = [
        poNumber,
        body.po_type || 'material',
        vendorName,
        body.status || '대기',
        body.expected_date || body.due_date || '',
        totalQty,
        body.notes || '',
        body.process_step || 0,
        body.parent_po_id || null,
        body.process_chain || '',
        poOrigin
      ];
      if (_hasEntity.po_header) _poArgs.push(poEntity);
      const info = await db.prepare(_poSql).run(..._poArgs);
      const poId = info.lastInsertRowid;
      const itemStmt = db.prepare(`INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?, ?, ?, ?, ?, ?, ?)`);
      for (const it of items) {
        await itemStmt.run(poId, it.product_code || '', it.brand || '', it.process_type || '', it.ordered_qty || 0, it.spec || '', it.notes || '');
      }
      // 규격 자동 보정 — 품목 단위로 spec 이 비어 있으면 products.spec 에서 복사. 발주서 HTML 렌더러가 i.spec 을 사용하므로
      // 프론트가 spec 을 안 보내도 이 단계에서 자동 채워짐. 이미 채워진 건 덮어쓰지 않음.
      try { await db.prepare(`UPDATE po_items SET spec = COALESCE((SELECT spec FROM products WHERE product_code = po_items.product_code), '') WHERE po_id = ? AND (spec IS NULL OR spec = '')`).run(poId); } catch(_){}
      return poId;
    });
    const poId = await tx();

    // 후공정 체인 자동 설정: product_info에서 후공정 순서를 읽어 process_chain 저장
    if (!body.process_chain && items.length) {
      try {
        const pInfo = getProductInfo();
        const postCols = await getPostProcessTypes();
        // 첫 번째 품목 기준으로 후공정 체인 구성
        const info = pInfo[items[0].product_code] || {};
        let chainSteps = [];
        // _steps가 있으면 step_order 기반 (product_post_vendor 직접 사용)
        if (info._steps && info._steps.length) {
          chainSteps = info._steps.map((s, i) => ({ step: i + 1, process: s.process, vendor: s.vendor }));
        } else {
          // fallback: postCols 순서로 빌드
          let stepNum = 1;
          postCols.forEach(col => {
            if (info[col] && info[col] !== '0') {
              chainSteps.push({ step: stepNum, process: col, vendor: info[col] });
              stepNum++;
            }
          });
        }
        if (chainSteps.length > 0) {
          // 현재 PO의 vendor가 체인의 몇 번째 step인지 확인
          const myStep = chainSteps.findIndex(s => {
            const vn = vendorName.replace('패키지','').replace('봉투','');
            return s.vendor.startsWith(vn.slice(0,2)) || vendorName.startsWith(s.vendor.slice(0,2));
          });
          const processStep = myStep >= 0 ? chainSteps[myStep].step : 1;
          await db.prepare("UPDATE po_header SET process_chain=?, process_step=? WHERE po_id=?")
            .run(JSON.stringify(chainSteps), processStep, poId);
          console.log(`[공정체인] PO ${poNumber}: ${chainSteps.map(s=>s.vendor+'('+s.process+')').join(' → ')}, 현재 step=${processStep}`);
        }
      } catch(e) { console.warn('[공정체인 자동설정 실패]', e.message); }
    }

    // 목형비 자동 처리: 신제품 첫 발주 시 notes에 '목형비 포함' 마킹
    for (const item of items) {
      const prod = await db.prepare('SELECT is_new_product, first_order_done, die_cost FROM products WHERE product_code=?').get(item.product_code);
      if (prod && prod.is_new_product === 1 && prod.first_order_done === 0) {
        await db.prepare("UPDATE po_items SET notes = CASE WHEN notes='' THEN '목형비 포함' ELSE notes || ' | 목형비 포함' END WHERE po_id=? AND product_code=?")
          .run(poId, item.product_code);
        await db.prepare("UPDATE products SET first_order_done=1 WHERE product_code=?").run(item.product_code);
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

  // PUT /api/po/:id/items — 발주서 품목 추가/수정 (발송 후에도 admin 가능)
  const poItemsMatch = pathname.match(/^\/api\/po\/(\d+)\/items$/);
  if (poItemsMatch && method === 'PUT') {
    const poId = parseInt(poItemsMatch[1]);
    const body = await readJSON(req);
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(poId);
    if (!po) { fail(res, 404, '발주서 없음'); return; }

    const isAdmin = currentUser && currentUser.role === 'admin';
    // 수정 가능 조건: admin이면 항상, 아니면 원재료 발송 전까지만
    const lockedStatuses = ['received', 'cancelled'];
    const materialShipped = po.material_status === 'shipped' || po.material_status === 'received';
    if (!isAdmin && (lockedStatuses.includes(po.status) || materialShipped)) {
      fail(res, 403, '원재료가 이미 발송된 발주서는 수정할 수 없습니다. 관리자에게 요청하세요.'); return;
    }
    if (lockedStatuses.includes(po.status) && !isAdmin) {
      fail(res, 403, '완료/취소된 발주서는 수정 불가'); return;
    }

    const addItems = body.add || []; // [{product_code, brand, ordered_qty, spec, notes, process_type}]
    const removeItemIds = body.remove || []; // [item_id, ...]
    const updateItems = body.update || []; // [{item_id, ordered_qty, spec, notes}]
    let added = 0, removed = 0, updated = 0;

    const tx = db.transaction(async () => {
      // 품목 추가
      for (const it of addItems) {
        if (!it.product_code || !it.ordered_qty) continue;
        await db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)')
          .run(poId, it.product_code, it.brand||'', it.process_type||'', it.ordered_qty||0, it.spec||'', it.notes||'');
        added++;
      }
      // 품목 삭제
      for (const itemId of removeItemIds) {
        await db.prepare('DELETE FROM po_items WHERE item_id=? AND po_id=?').run(itemId, poId);
        removed++;
      }
      // 품목 수정
      for (const it of updateItems) {
        if (!it.item_id) continue;
        const sets = [], vals = [];
        if (it.ordered_qty !== undefined) { sets.push('ordered_qty=?'); vals.push(it.ordered_qty); }
        if (it.spec !== undefined) { sets.push('spec=?'); vals.push(it.spec); }
        if (it.notes !== undefined) { sets.push('notes=?'); vals.push(it.notes); }
        if (it.process_type !== undefined) { sets.push('process_type=?'); vals.push(it.process_type); }
        if (sets.length) { vals.push(it.item_id, poId); await db.prepare(`UPDATE po_items SET ${sets.join(',')} WHERE item_id=? AND po_id=?`).run(...vals); updated++; }
      }
      // po_header 업데이트 (업체명 변경 등)
      if (body.vendor_name) {
        if (po.po_type === '원재료') {
          // 원재료 PO: vendor_name + material_vendor_name 동기화 + 자식 후공정 PO 의 material_vendor_name 도 전파
          await db.prepare("UPDATE po_header SET vendor_name=?, material_vendor_name=?, updated_at=datetime('now','localtime') WHERE po_id=?").run(body.vendor_name, body.vendor_name, poId);
          try {
            await db.prepare("UPDATE po_header SET material_vendor_name=?, updated_at=datetime('now','localtime') WHERE parent_po_id=?").run(body.vendor_name, poId);
          } catch(_) {}
        } else if (po.po_type === '후공정') {
          await db.prepare("UPDATE po_header SET vendor_name=?, process_vendor_name=?, updated_at=datetime('now','localtime') WHERE po_id=?").run(body.vendor_name, body.vendor_name, poId);
        } else {
          await db.prepare("UPDATE po_header SET vendor_name=?, updated_at=datetime('now','localtime') WHERE po_id=?").run(body.vendor_name, poId);
        }
      }
      // total_qty 갱신
      const total = await db.prepare('SELECT COALESCE(SUM(ordered_qty),0) AS t FROM po_items WHERE po_id=?').get(poId);
      await db.prepare("UPDATE po_header SET total_qty=?, updated_at=datetime('now','localtime') WHERE po_id=?").run(total.t, poId);
    });
    await tx();

    // 이력 기록
    const details = [];
    if (added) details.push(`${added}건 추가`);
    if (removed) details.push(`${removed}건 삭제`);
    if (updated) details.push(`${updated}건 수정`);
    logPOActivity(poId, 'items_modified', {
      actor: currentUser?.username || 'unknown',
      actor_type: isAdmin ? 'admin' : 'user',
      details: `품목변경: ${details.join(', ')}${body.reason ? ' | 사유: '+body.reason : ''}`
    });
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'po_items_modify', 'po_items', poId, `PO ${po.po_number} 품목변경: ${details.join(', ')}${body.reason?' 사유:'+body.reason:''}`, clientIP);

    ok(res, { po_id: poId, added, removed, updated }); return;
  }

  // POST /api/po/:matPoId/add-postprocess — 원재료 PO 에서 자식 후공정 PO 에 항목 추가
  // 동일 (parent_po_id, vendor_name) 조합의 후공정 PO 가 있으면 거기에 추가, 없으면 새로 생성.
  // 마법사 단계가 아니라 사후 추가도 가능하도록 별도 엔드포인트로 분리.
  const addPostMatch = pathname.match(/^\/api\/po\/(\d+)\/add-postprocess$/);
  if (addPostMatch && method === 'POST') {
    const matPoId = parseInt(addPostMatch[1]);
    const body = await readJSON(req);
    const matPo = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(matPoId);
    if (!matPo) { fail(res, 404, '원재료 PO 없음'); return; }
    if (matPo.po_type !== '원재료') { fail(res, 400, '원재료 PO 가 아님 (po_type=' + matPo.po_type + ')'); return; }

    const productCodes = Array.isArray(body.product_codes) ? body.product_codes : [];
    const processType = (body.process_type || '').trim();
    const postVendor = (body.post_vendor || '').trim();
    if (!productCodes.length) { fail(res, 400, '품목 미선택'); return; }
    if (!processType) { fail(res, 400, '공정명 미입력'); return; }
    if (!postVendor) { fail(res, 400, '후공정 업체 미입력'); return; }

    // 원재료 PO 의 해당 품목 수량 가져오기
    const matItems = await db.prepare('SELECT * FROM po_items WHERE po_id=? AND product_code IN (' + productCodes.map(() => '?').join(',') + ')').all(matPoId, ...productCodes);
    const qtyByCode = {};
    for (const it of matItems) qtyByCode[it.product_code] = (qtyByCode[it.product_code] || 0) + (it.ordered_qty || 0);

    // 동일 vendor_name 의 자식 후공정 PO 찾기 (없으면 새로 생성)
    let postPo = await db.prepare("SELECT * FROM po_header WHERE parent_po_id=? AND po_type='후공정' AND vendor_name=? AND status NOT IN ('완료','취소') ORDER BY po_id DESC LIMIT 1").get(matPoId, postVendor);
    let postPoId, postPoNumber;
    let createdNew = false;
    if (postPo) {
      postPoId = postPo.po_id;
      postPoNumber = postPo.po_number;
    } else {
      postPoNumber = await generatePoNumber();
      const totalQty = productCodes.reduce((s, c) => s + (qtyByCode[c] || 0), 0);
      const info = await db.prepare(`INSERT INTO po_header
        (po_number, po_type, vendor_name, material_vendor_name, process_vendor_name, status,
         due_date, total_qty, notes, origin, po_date, process_step, parent_po_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,date('now','localtime'),?,?)`)
        .run(postPoNumber, '후공정', postVendor, matPo.vendor_name || '', postVendor, 'draft',
             matPo.due_date || '', totalQty, '사후추가: ' + processType, matPo.origin || '한국', 1, matPoId);
      postPoId = info.lastInsertRowid;
      createdNew = true;
      try { await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(postPoId, 'created', currentUser?.username || 'system', `사후 추가: 후공정 PO (${postVendor}, ${processType}, 원재료 PO: ${matPo.po_number})`); } catch(_) {}
    }

    // 후공정 PO 에 항목 추가 (동일 product_code+process_type 중복 시 스킵)
    let added = 0, skipped = 0;
    const insItem = db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)');
    for (const code of productCodes) {
      const exists = await db.prepare('SELECT item_id FROM po_items WHERE po_id=? AND product_code=? AND process_type=? LIMIT 1').get(postPoId, code, processType);
      if (exists) { skipped++; continue; }
      const qty = qtyByCode[code] || 0;
      try {
        await insItem.run(postPoId, code, '', processType, qty, processType, '사후 추가 (원재료 PO ' + matPo.po_number + ')');
        added++;
      } catch (_) {}
    }
    // total_qty 재계산
    try {
      const t = await db.prepare('SELECT COALESCE(SUM(ordered_qty),0) AS t FROM po_items WHERE po_id=?').get(postPoId);
      await db.prepare("UPDATE po_header SET total_qty=?, updated_at=datetime('now','localtime') WHERE po_id=?").run(t.t, postPoId);
    } catch(_) {}
    try { await db.prepare("INSERT INTO po_activity_log (po_id, action, actor, details) VALUES (?,?,?,?)").run(postPoId, 'items_modified', currentUser?.username || 'system', `사후 후공정 ${added}건 추가 (${processType}): ${productCodes.join(',')}`); } catch(_) {}

    ok(res, { post_po_id: postPoId, post_po_number: postPoNumber, added, skipped, created_new: createdNew });
    return;
  }

  // PUT /api/po/:id/status
  const poStatus = pathname.match(/^\/api\/po\/(\d+)\/status$/);
  if (poStatus && method === 'PUT') {
    const id = parseInt(poStatus[1]);
    const body = await readJSON(req);
    const validStatuses = ['draft', 'sent', 'confirmed', 'partial', 'received', 'cancelled', 'os_pending', 'os_registered'];
    if (!validStatuses.includes(body.status)) { fail(res, 400, 'Invalid status. Allowed: ' + validStatuses.join(', ')); return; }
    await db.prepare(`UPDATE po_header SET status = ?, updated_at = datetime('now','localtime') WHERE po_id = ?`).run(body.status, id);
    ok(res, { po_id: id, status: body.status });
    return;
  }

  // PATCH /api/po/:id — 상태 변경 (프론트엔드 호출용)
  const poPatch = pathname.match(/^\/api\/po\/(\d+)$/);
  if (poPatch && method === 'PATCH') {
    const id = parseInt(poPatch[1]);
    const body = await readJSON(req);
    const newStatus = body.status;
    const dbStatus = PO_STATUS_KO_TO_EN[newStatus] || newStatus;
    const poBeforePatch = await db.prepare('SELECT status, material_status, process_status FROM po_header WHERE po_id=?').get(id);
    await db.prepare(`UPDATE po_header SET status = ?, updated_at = datetime('now','localtime') WHERE po_id = ?`).run(dbStatus, id);

    // 발송 시 파이프라인 서브상태 초기화
    if (newStatus === '발송' || dbStatus === 'sent') {
      await db.prepare("UPDATE po_header SET material_status='sent', process_status='waiting' WHERE po_id=?").run(id);
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
      const po = await db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(id);
      const items = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(id);

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
        const vendor = await db.prepare('SELECT * FROM vendors WHERE name = ?').get(po.vendor_name);
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
        const poForDoc = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(id);
        const itemsForDoc = await db.prepare('SELECT * FROM po_items WHERE po_id=?').all(id);
        const piMap = getProductInfo();
        const docItems = [];
        for (const item of itemsForDoc) {
          const pi = piMap[item.product_code] || {};
          const lastPrice = await getLastVendorPrice(poForDoc.vendor_name, item.product_code);
          docItems.push({
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
          });
        }
        const vendorRow = await db.prepare('SELECT type FROM vendors WHERE name=?').get(poForDoc.vendor_name);
        const vendorType = vendorRow ? vendorRow.type : 'material';
        await db.prepare(`INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')`)
          .run(id, poForDoc.po_number, poForDoc.vendor_name, vendorType === '후공정' ? 'process' : 'material', JSON.stringify(docItems));
        logPOActivity(id, 'trade_doc_created', { actor_type: 'system', details: '거래명세서 자동 생성' });
      }
    }

    // 취소 시 Google Sheet에 취소선 + 빨간글씨 적용
    if (newStatus === '취소' || dbStatus === 'cancelled') {
      const items = await db.prepare('SELECT product_code FROM po_items WHERE po_id = ?').all(id);
      const po = await db.prepare('SELECT po_date FROM po_header WHERE po_id = ?').get(id);
      const codes = items.map(i => i.product_code).filter(Boolean);
      if (codes.length) {
        sheetResult = await cancelInGoogleSheet(codes, po ? po.po_date : '');
        console.log('Google Sheet 취소 포맷:', sheetResult);
      }
    }

    const emailFailed = emailResult && !emailResult.ok;
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'po_update', 'po_header', id, `발주수정: PO#${id} 상태→${dbStatus}`, clientIP);
    ok(res, { updated: true, po_id: id, status: dbStatus, google_sheet: sheetResult, email: emailResult, email_failed: emailFailed });
    return;
  }

  // POST /api/po/:id/resend — 이메일 재발송
  const poResend = pathname.match(/^\/api\/po\/(\d+)\/resend$/);
  if (poResend && method === 'POST') {
    const id = parseInt(poResend[1]);
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id = ?').get(id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    const items = await db.prepare('SELECT * FROM po_items WHERE po_id = ?').all(id);
    const vendor = await db.prepare('SELECT * FROM vendors WHERE name = ?').get(po.vendor_name);
    if (!vendor || !vendor.email) { fail(res, 400, '업체 이메일 미등록'); return; }
    try {
      const isPost = po.po_type === '후공정';
      const emailResult = await sendPOEmail(po, items, vendor.email, vendor.name, isPost, vendor.email_cc || '');
      // 활동 로그
      try { await db.prepare('INSERT INTO po_activity_log (po_id, action, details) VALUES (?, ?, ?)').run(id, '이메일 재발송', emailResult.ok ? '성공: ' + vendor.email : '실패: ' + (emailResult.error||'')); } catch(e){}
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
    const po = await db.prepare('SELECT po_id, po_number, status FROM po_header WHERE po_id = ?').get(id);
    if (!po) { fail(res, 404, 'PO not found'); return; }
    try { await db.prepare('DELETE FROM receipt_items WHERE receipt_id IN (SELECT receipt_id FROM receipts WHERE po_id = ?)').run(id); } catch(_){}
    try { await db.prepare('DELETE FROM receipts WHERE po_id = ?').run(id); } catch(_){}
    await db.prepare('DELETE FROM po_items WHERE po_id = ?').run(id);
    try { await db.prepare('DELETE FROM activity_log WHERE po_id = ?').run(id); } catch(_){}
    await db.prepare('DELETE FROM po_header WHERE po_id = ?').run(id);
    console.log(`PO 삭제: ${po.po_number} (ID: ${id}, 상태: ${po.status})`);
    ok(res, { deleted: id, po_number: po.po_number });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: RECEIPTS
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/receipts' && method === 'GET') {
    const qs = Object.fromEntries(new URL(req.url, 'http://localhost').searchParams);
    const _rcvEntCol = _hasEntity.po_header ? ', h.legal_entity' : '';
    let sql = `
      SELECT r.id as receipt_id, r.po_id, r.receipt_date, r.received_by, r.notes, r.created_at,
             r.batch_no, h.po_number, h.vendor_name, h.origin${_rcvEntCol}
      FROM receipts r
      LEFT JOIN po_header h ON r.po_id = h.po_id
    `;
    const conditions = [];
    const params = [];
    if (qs.po_id) { conditions.push('r.po_id = $' + (params.length+1)); params.push(parseInt(qs.po_id)); }
    if (qs.origin) { conditions.push('h.origin = $' + (params.length+1)); params.push(qs.origin); }
    if (qs.entity && qs.entity !== 'all' && _hasEntity.po_header) { conditions.push('h.legal_entity = $' + (params.length+1)); params.push(qs.entity); }
    if (conditions.length) sql += ' WHERE ' + conditions.join(' AND ');
    sql += ' ORDER BY r.created_at DESC';
    const rows = params.length ? await db.prepare(sql).all(...params) : await db.prepare(sql).all();
    const itemStmt = db.prepare('SELECT * FROM receipt_items WHERE receipt_id = $1');
    for (const r of rows) {
      r.items = await itemStmt.all(r.receipt_id);
    }
    ok(res, rows);
    return;
  }

  if (pathname === '/api/receipts' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.po_id) { fail(res, 400, 'po_id required'); return; }
    const items = body.items || [];

    const tx = db.transaction(async () => {
      const rInfo = await db.prepare(`INSERT INTO receipts (po_id, received_by, notes, batch_no) VALUES (?, ?, ?, ?)`).run(
        body.po_id, body.received_by || '', body.notes || '', body.batch_no || 1
      );
      const receiptId = rInfo.lastInsertRowid;

      const riStmt = db.prepare(`INSERT INTO receipt_items (receipt_id, po_item_id, product_code, received_qty, defect_qty, notes) VALUES (?, ?, ?, ?, ?, ?)`);
      const updatePoItem = db.prepare(`UPDATE po_items SET received_qty = received_qty + ? WHERE item_id = ?`);

      for (const it of items) {
        let poItemId = it.po_item_id || null;
        // po_item_id가 없으면 product_code + po_id로 자동 매칭
        if (!poItemId && it.product_code && body.po_id) {
          const match = await db.prepare('SELECT item_id FROM po_items WHERE po_id=? AND product_code=? LIMIT 1').get(body.po_id, it.product_code);
          if (match) poItemId = match.item_id;
        }
        await riStmt.run(receiptId, poItemId, it.product_code || '', it.received_qty || 0, it.defect_qty || 0, it.notes || '');
        if (poItemId && it.received_qty) {
          await updatePoItem.run(it.received_qty, poItemId);
        }
      }

      // Check if all items fully received → update PO status
      const poItems = await db.prepare('SELECT ordered_qty, received_qty FROM po_items WHERE po_id = ?').all(body.po_id);
      const allReceived = poItems.length > 0 && poItems.every(pi => pi.received_qty >= pi.ordered_qty);
      const anyReceived = poItems.some(pi => pi.received_qty > 0);

      if (allReceived) {
        await db.prepare(`UPDATE po_header SET status = 'received', process_status = 'completed', material_status = 'received', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(body.po_id);
      } else if (anyReceived) {
        await db.prepare(`UPDATE po_header SET status = 'partial', process_status = 'working', updated_at = datetime('now','localtime') WHERE po_id = ?`).run(body.po_id);
      }

      return receiptId;
    });
    const receiptId = await tx();

    // XERP 캐시 무효화 (다음 조회 시 최신 데이터 로드)
    if (typeof xerpInventoryCacheTime !== 'undefined') xerpInventoryCacheTime = 0;

    ok(res, { receipt_id: receiptId });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: INVOICES
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/invoices' && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM invoices ORDER BY created_at DESC').all();
    for (const inv of rows) {
      inv.items = await db.prepare('SELECT * FROM invoice_items WHERE invoice_id = ?').all(inv.invoice_id);
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

    const tx = db.transaction(async () => {
      const info = await db.prepare(`INSERT INTO invoices (po_id, vendor_name, invoice_no, invoice_date, amount, file_path, file_name, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(
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
          await stmt.run(invId, it.product_code || '', it.product_name || '', it.qty || 0, it.unit_price || 0, it.amount || 0, it.notes || '');
        }
      }
      return invId;
    });
    const invId = await tx();
    ok(res, { invoice_id: invId, file_path: filePath });
    return;
  }

  // GET /api/invoices/:id/file
  const invFile = pathname.match(/^\/api\/invoices\/(\d+)\/file$/);
  if (invFile && method === 'GET') {
    const id = parseInt(invFile[1]);
    const inv = await db.prepare('SELECT file_path, file_name FROM invoices WHERE invoice_id = ?').get(id);
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
    const inv = await db.prepare('SELECT file_path FROM invoices WHERE invoice_id = ?').get(id);
    if (!inv) { fail(res, 404, 'Invoice not found'); return; }
    // delete file from disk
    if (inv.file_path) {
      const fullPath = path.join(UPLOAD_DIR, inv.file_path);
      try { fs.unlinkSync(fullPath); } catch (_) { /* ignore if already deleted */ }
    }
    await db.prepare('DELETE FROM invoice_items WHERE invoice_id = ?').run(id);
    await db.prepare('DELETE FROM invoices WHERE invoice_id = ?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  NEW API: STATS
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/stats' && method === 'GET') {
    const now = new Date();
    const ym = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');

    const totalPOs = (await db.prepare('SELECT COUNT(*) as cnt FROM po_header').get()).cnt;
    const draftPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'draft'`).get()).cnt;
    const sentPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'sent'`).get()).cnt;
    const confirmedPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'confirmed'`).get()).cnt;
    const partialPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'partial'`).get()).cnt;
    const receivedPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'received'`).get()).cnt;
    const cancelledPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status = 'cancelled'`).get()).cnt;
    const pendingPOs = draftPOs + sentPOs + confirmedPOs + partialPOs;

    const thisMonthPOs = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE po_date LIKE ?`).get(ym + '%')).cnt;
    const thisMonthItems = (await db.prepare(`SELECT COALESCE(SUM(pi.ordered_qty),0) as qty FROM po_items pi JOIN po_header ph ON ph.po_id=pi.po_id WHERE ph.po_date LIKE ?`).get(ym + '%')).qty;
    const totalVendors = (await db.prepare('SELECT COUNT(*) as cnt FROM vendors').get()).cnt;
    const totalInvoices = (await db.prepare('SELECT COUNT(*) as cnt FROM invoices').get()).cnt;
    const thisMonthInvoiceAmt = (await db.prepare(`SELECT COALESCE(SUM(amount),0) as amt FROM invoices WHERE invoice_date LIKE ?`).get(ym + '%')).amt;

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
      const row = await db.prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(total_qty),0) as qty FROM po_header WHERE po_date LIKE ? AND status != 'cancelled'`).get(m + '%');
      monthlyPO.push({ month: m, count: row.cnt, qty: row.qty });
    }

    // 2. 거래처별 발주 비중 (최근 3개월, 도넛 차트용)
    const threeMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 2, 1).toISOString().slice(0, 10);
    const vendorShare = await db.prepare(`SELECT vendor_name as name, COUNT(*) as count, COALESCE(SUM(total_qty),0) as qty FROM po_header WHERE po_date >= ? AND status != 'cancelled' GROUP BY vendor_name ORDER BY count DESC LIMIT 8`).all(threeMonthsAgo);

    // 3. 발주 상태 분포 (도넛 차트용)
    const statusDist = await db.prepare(`SELECT status, COUNT(*) as count FROM po_header WHERE status != 'cancelled' GROUP BY status`).all();
    statusDist.forEach(r => r.label = PO_STATUS_EN_TO_KO[r.status] || r.status);

    // 4. 리드타임 분석 (발주일~완료일)
    const ltRows = await db.prepare(`SELECT po_date, updated_at, vendor_name FROM po_header WHERE status IN ('received','os_pending') AND po_date IS NOT NULL AND updated_at IS NOT NULL ORDER BY updated_at DESC LIMIT 50`).all();
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
    const defectTotal = (await db.prepare("SELECT COUNT(*) as cnt FROM defects").get()).cnt;
    const defectMonth = (await db.prepare("SELECT COUNT(*) as cnt FROM defects WHERE created_at LIKE ?").get(ym + '%')).cnt;

    // 6. 알림 (안전재고 미달 = urgent 품목 수, 납기 초과, 미승인 PO)
    const pendingPO = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE status IN ('draft','sent')`).get()).cnt;
    const overdueCount = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE due_date != '' AND due_date::date < CURRENT_DATE AND status NOT IN ('received','cancelled','os_pending')`).get()).cnt;
    // 납기 임박 (D-3 이내)
    const upcomingDeadlineCount = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE due_date != '' AND due_date::date >= CURRENT_DATE AND due_date::date <= CURRENT_DATE + INTERVAL '3 days' AND status NOT IN ('received','cancelled','os_pending')`).get()).cnt;

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
      rows = await db.prepare('SELECT po_id, po_number, po_date, vendor_name, po_type, status, total_qty, due_date as expected_date, notes, created_at FROM po_header ORDER BY po_date DESC').all();
      filename = 'po_list.csv';
      headers = ['발주ID', '발주번호', '발주일', '거래처', '유형', '상태', '수량', '납기일', '비고', '생성일'];
    } else if (type === 'vendors') {
      rows = await db.prepare('SELECT vendor_id, name, type, email, phone, contact, notes, created_at FROM vendors ORDER BY name').all();
      filename = 'vendors.csv';
      headers = ['ID', '거래처명', '유형', '이메일', '전화', '담당자', '비고', '생성일'];
    } else if (type === 'products') {
      rows = await db.prepare('SELECT * FROM products ORDER BY product_code').all();
      filename = 'products.csv';
      headers = Object.keys(rows[0] || {});
    } else if (type === 'defects') {
      rows = await db.prepare('SELECT * FROM defects ORDER BY created_at DESC').all();
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
    const rows = await db.prepare(`SELECT id, title, subtitle, report_type, created_at, updated_at FROM reports ORDER BY created_at DESC`).all();
    ok(res, rows);
    return;
  }

  // GET /api/reports/:id — 보고서 상세
  if (pathname.match(/^\/api\/reports\/\d+$/) && method === 'GET') {
    const id = parseInt(pathname.split('/').pop());
    const row = await db.prepare('SELECT * FROM reports WHERE id=?').get(id);
    if (!row) { fail(res, 404, '보고서를 찾을 수 없습니다'); return; }
    ok(res, row);
    return;
  }

  // POST /api/reports — 보고서 저장
  if (pathname === '/api/reports' && method === 'POST') {
    const body = await readJSON(req);
    const { title, subtitle, report_type, content } = body;
    if (!title) { fail(res, 400, '제목 필수'); return; }
    const result = await db.prepare(`INSERT INTO reports (title, subtitle, report_type, content) VALUES (?,?,?,?)`).run(
      title, subtitle || '', report_type || 'general', typeof content === 'string' ? content : JSON.stringify(content || {})
    );
    ok(res, { id: result.lastInsertRowid });
    return;
  }

  // PUT /api/reports/:id — 보고서 수정
  if (pathname.match(/^\/api\/reports\/\d+$/) && method === 'PUT') {
    const id = parseInt(pathname.split('/').pop());
    const body = await readJSON(req);
    const existing = await db.prepare('SELECT * FROM reports WHERE id=?').get(id);
    if (!existing) { fail(res, 404, '보고서를 찾을 수 없습니다'); return; }
    const title = body.title !== undefined ? body.title : existing.title;
    const subtitle = body.subtitle !== undefined ? body.subtitle : existing.subtitle;
    const report_type = body.report_type !== undefined ? body.report_type : existing.report_type;
    const content = body.content !== undefined ? (typeof body.content === 'string' ? body.content : JSON.stringify(body.content)) : existing.content;
    await db.prepare(`UPDATE reports SET title=?, subtitle=?, report_type=?, content=?, updated_at=datetime('now','localtime') WHERE id=?`).run(title, subtitle, report_type, content, id);
    ok(res, { id, updated: true });
    return;
  }

  // DELETE /api/reports/:id — 보고서 삭제
  if (pathname.match(/^\/api\/reports\/\d+$/) && method === 'DELETE') {
    const id = parseInt(pathname.split('/').pop());
    await db.prepare('DELETE FROM reports WHERE id=?').run(id);
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
    const rows = await db.prepare(`SELECT * FROM po_drafts ORDER BY created_at DESC`).all();
    ok(res, rows);
    return;
  }

  // POST /api/po-drafts — 발주서 저장
  if (pathname === '/api/po-drafts' && method === 'POST') {
    const body = await readJSON(req);
    const { po_number, po_date, due_date, vendor_id, vendor_name, vendor_contact, vendor_phone, vendor_email,
            issuer_name, issuer_contact, issuer_phone, issuer_email, payment_terms, remark,
            items, total_supply, total_tax, total_amount } = body;
    const legal_entity = (body.legal_entity === 'dd') ? 'dd' : 'barunson';
    const result = await db.prepare(`INSERT INTO po_drafts
      (po_number,po_date,due_date,vendor_id,vendor_name,vendor_contact,vendor_phone,vendor_email,
       issuer_name,issuer_contact,issuer_phone,issuer_email,payment_terms,remark,
       items,total_supply,total_tax,total_amount,legal_entity)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      po_number||'', po_date||'', due_date||'', vendor_id||0, vendor_name||'',
      vendor_contact||'', vendor_phone||'', vendor_email||'',
      issuer_name||'바른컴퍼니', issuer_contact||'', issuer_phone||'', issuer_email||'',
      payment_terms||'', remark||'',
      typeof items === 'string' ? items : JSON.stringify(items||[]),
      total_supply||0, total_tax||0, total_amount||0, legal_entity
    );
    ok(res, { id: result.lastInsertRowid });
    return;
  }

  // POST /api/po-drafts/:id/email — 발주서 이메일 발송
  if (pathname.match(/^\/api\/po-drafts\/\d+\/email$/) && method === 'POST') {
    const id = parseInt(pathname.split('/')[3]);
    const draft = await db.prepare('SELECT * FROM po_drafts WHERE id=?').get(id);
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

  // PATCH /api/po-drafts/:id/status — 발주서 완료/복원
  if (pathname.match(/^\/api\/po-drafts\/\d+\/status$/) && method === 'PATCH') {
    const id = parseInt(pathname.split('/')[3]);
    const body = await readJSON(req);
    if (body.action === 'complete') {
      await db.prepare("UPDATE po_drafts SET status='completed', completed_at=datetime('now','localtime') WHERE id=?").run(id);
    } else if (body.action === 'restore') {
      await db.prepare("UPDATE po_drafts SET status='sent', completed_at=NULL WHERE id=?").run(id);
    }
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/po-drafts/:id — 발주서 삭제
  if (pathname.match(/^\/api\/po-drafts\/\d+$/) && method === 'DELETE') {
    const id = parseInt(pathname.split('/').pop());
    await db.prepare(`DELETE FROM po_drafts WHERE id=?`).run(id);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/purchases?year=2026 — 월별 업체별 품목별 매입 집계
  if (pathname === '/api/purchases' && method === 'GET') {
    const year = parsed.searchParams.get('year') || new Date().getFullYear().toString();
    const rows = await db.prepare(`
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
    sql += ' ORDER BY note_date DESC, id DESC';
    const rows = await db.prepare(sql).all(...params);
    // note_id 호환: id를 note_id로도 매핑
    rows.forEach(r => { if(!r.note_id) r.note_id = r.id; });
    ok(res, rows);
    return;
  }

  const noteGet = pathname.match(/^\/api\/notes\/(\d+)$/);
  if (noteGet && method === 'GET') {
    const id = parseInt(noteGet[1]);
    const note = await db.prepare('SELECT * FROM vendor_notes WHERE id = ?').get(id);
    if (!note) { fail(res, 404, 'Note not found'); return; }
    ok(res, note);
    return;
  }

  if (pathname === '/api/notes' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.vendor_id) { fail(res, 400, 'vendor_id는 필수입니다'); return; }
    const info = await db.prepare(`INSERT INTO vendor_notes (vendor_id, vendor_name, title, content, note_type, note_date) VALUES (?, ?, ?, ?, ?, ?)`).run(
      body.vendor_id,
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
    const values = [];
    for (const col of ['vendor_id', 'vendor_name', 'title', 'content', 'note_type', 'note_date', 'status']) {
      if (body[col] !== undefined) {
        fields.push(`${col} = ?`);
        values.push(body[col]);
      }
    }
    if (fields.length === 0) { fail(res, 400, 'No fields to update'); return; }
    fields.push(`updated_at = datetime('now','localtime')`);
    values.push(id);
    await db.prepare(`UPDATE vendor_notes SET ${fields.join(', ')} WHERE id = ?`).run(...values);
    ok(res, { note_id: id });
    return;
  }

  const noteDel = pathname.match(/^\/api\/notes\/(\d+)$/);
  if (noteDel && method === 'DELETE') {
    const id = parseInt(noteDel[1]);
    try { await db.prepare('DELETE FROM note_comments WHERE note_id = ?').run(id); } catch(_) {}
    await db.prepare('DELETE FROM vendor_notes WHERE id = ?').run(id);
    ok(res, { deleted: id });
    return;
  }

  // GET /api/notes/:id/comments
  const noteComGet = pathname.match(/^\/api\/notes\/(\d+)\/comments$/);
  if (noteComGet && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM note_comments WHERE note_id=? ORDER BY created_at ASC').all(noteComGet[1]);
    ok(res, rows);
    return;
  }

  // POST /api/notes/:id/comments
  const noteComPost = pathname.match(/^\/api\/notes\/(\d+)\/comments$/);
  if (noteComPost && method === 'POST') {
    const b = await readJSON(req);
    if (!b.content?.trim()) { fail(res, 400, 'content required'); return; }
    const info = await db.prepare('INSERT INTO note_comments (note_id, author, content) VALUES (?,?,?)').run(
      parseInt(noteComPost[1]), b.author||'', b.content.trim()
    );
    ok(res, { id: info.lastInsertRowid });
    return;
  }

  // DELETE /api/note-comments/:id
  const noteComDel = pathname.match(/^\/api\/note-comments\/(\d+)$/);
  if (noteComDel && method === 'DELETE') {
    await db.prepare('DELETE FROM note_comments WHERE id=?').run(noteComDel[1]);
    ok(res, { deleted: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  BOM API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/bom' && method === 'GET') {
    const rows = await db.prepare(`SELECT b.*, (SELECT COUNT(*) FROM bom_items WHERE bom_id=b.bom_id) as item_count FROM bom_header b ORDER BY b.product_code`).all();
    ok(res, rows);
    return;
  }

  // GET /api/bom/export — BOM 전체를 플랫 CSV용 데이터로
  if (pathname === '/api/bom/export' && method === 'GET') {
    const headers = await db.prepare('SELECT * FROM bom_header ORDER BY product_code').all();
    const processes = await getPostProcessTypes();
    const rows = await Promise.all(headers.map(async h => {
      const items = await db.prepare('SELECT * FROM bom_items WHERE bom_id=? ORDER BY sort_order').all(h.bom_id);
      const mat = items.find(i => i.item_type === 'material') || {};
      const row = {
        product_code: h.product_code, product_name: h.product_name||'', brand: h.brand||'',
        material_code: mat.material_code||'', material_name: mat.material_name||'',
        vendor_name: mat.vendor_name||'', cut_spec: mat.cut_spec||'', plate_spec: mat.plate_spec||''
      };
      processes.forEach(p => { const proc = items.find(i => i.process_type === p); row[p] = proc ? proc.vendor_name : ''; });
      return row;
    }));
    ok(res, rows);
    return;
  }

  // POST /api/bom/bulk-upload — CSV 파싱 결과 일괄 등록
  if (pathname === '/api/bom/bulk-upload' && method === 'POST') {
    const body = await readJSON(req);
    const rows = body.rows || [];
    const processes = await getPostProcessTypes();
    let updated = 0, created = 0;
    const txn = db.transaction(async () => {
      for (const r of rows) {
        if (!r.product_code) continue;
        let header = await db.prepare('SELECT bom_id FROM bom_header WHERE product_code=?').get(r.product_code);
        if (header) {
          await db.prepare('UPDATE bom_header SET product_name=?, brand=?, updated_at=datetime(\'now\',\'localtime\') WHERE bom_id=?').run(r.product_name||'', r.brand||'', header.bom_id);
          await db.prepare('DELETE FROM bom_items WHERE bom_id=?').run(header.bom_id);
          updated++;
        } else {
          const ins = await db.prepare('INSERT INTO bom_header (product_code, product_name, brand) VALUES (?,?,?)').run(r.product_code, r.product_name||'', r.brand||'');
          header = { bom_id: ins.lastInsertRowid };
          created++;
        }
        const insItem = db.prepare('INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, sort_order) VALUES (?,?,?,?,?,?,?,?,?,?)');
        let sort = 0;
        if (r.material_code) {
          await insItem.run(header.bom_id, 'material', r.material_code, r.material_name||'', r.vendor_name||'', '원재료', 1, r.cut_spec||'', r.plate_spec||'', sort++);
        }
        processes.forEach(async p => {
          if (r[p]) await insItem.run(header.bom_id, 'process', '', '', r[p], p, 1, '', '', sort++);
        });
      }
    });
    await txn();
    ok(res, { created, updated, total: created + updated });
    return;
  }

  const bomGet = pathname.match(/^\/api\/bom\/(.+)$/);
  if (bomGet && method === 'GET' && bomGet[1] !== 'import') {
    const code = decodeURIComponent(bomGet[1]);
    const header = await db.prepare('SELECT * FROM bom_header WHERE product_code = ? OR bom_id = ?').get(code, parseInt(code)||0);
    if (!header) { res.writeHead(404); res.end(JSON.stringify({error:'not found'})); return; }
    const items = await db.prepare('SELECT * FROM bom_items WHERE bom_id = ? ORDER BY sort_order, bom_item_id').all(header.bom_id);
    ok(res, { ...header, items });
    return;
  }

  if (pathname === '/api/bom' && method === 'POST') {
    const b = await readJSON(req);
    const ins = db.prepare('INSERT INTO bom_header (product_code, product_name, brand, notes, default_order_qty, finished_w, finished_h) VALUES (?,?,?,?,?,?,?)');
    const insItem = db.prepare(`INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, unit, notes, sort_order,
      material_type, paper_standard, paper_type, gsm, finished_w, finished_h, bleed, grip, loss_rate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?)`);
    const txn = db.transaction(async () => {
      const r = await ins.run(b.product_code, b.product_name||'', b.brand||'', b.notes||'', b.default_order_qty||1000, b.finished_w||0, b.finished_h||0);
      const bomId = r.lastInsertRowid;
      for (let i = 0; i < (b.items||[]).length; i++) { const it = (b.items||[])[i];
        await insItem.run(bomId, it.item_type||'material', it.material_code||'', it.material_name||'', it.vendor_name||'', it.process_type||'', it.qty_per||1, it.cut_spec||'', it.plate_spec||'', it.unit||'EA', it.notes||'', i,
          it.material_type||'IMPOSITION', it.paper_standard||'', it.paper_type||'', it.gsm||0, it.finished_w||0, it.finished_h||0, it.bleed??3, it.grip??10, it.loss_rate??5);
      }
      return bomId;
    });
    const bomId = await txn();
    ok(res, { bom_id: bomId });
    return;
  }

  const bomPut = pathname.match(/^\/api\/bom\/(\d+)$/);
  if (bomPut && method === 'PUT') {
    const bomId = parseInt(bomPut[1]);
    const b = await readJSON(req);
    const txn = db.transaction(async () => {
      if (b.product_name !== undefined) await db.prepare("UPDATE bom_header SET product_name=?, brand=?, notes=?, default_order_qty=?, finished_w=?, finished_h=?, updated_at=datetime('now','localtime') WHERE bom_id=?").run(b.product_name||'', b.brand||'', b.notes||'', b.default_order_qty||1000, b.finished_w||0, b.finished_h||0, bomId);
      if (b.items) {
        await db.prepare('DELETE FROM bom_items WHERE bom_id=?').run(bomId);
        const insItem = db.prepare(`INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, unit, notes, sort_order,
          material_type, paper_standard, paper_type, gsm, finished_w, finished_h, bleed, grip, loss_rate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?)`);
        for (let i = 0; i < b.items.length; i++) { const it = b.items[i];
          await insItem.run(bomId, it.item_type||'material', it.material_code||'', it.material_name||'', it.vendor_name||'', it.process_type||'', it.qty_per||1, it.cut_spec||'', it.plate_spec||'', it.unit||'EA', it.notes||'', i,
            it.material_type||'IMPOSITION', it.paper_standard||'', it.paper_type||'', it.gsm||0, it.finished_w||0, it.finished_h||0, it.bleed??3, it.grip??10, it.loss_rate??5);
        }
      }
    });
    await txn();
    ok(res, { updated: bomId });
    return;
  }

  const bomDel = pathname.match(/^\/api\/bom\/(\d+)$/);
  if (bomDel && method === 'DELETE') {
    await db.prepare('DELETE FROM bom_header WHERE bom_id=?').run(parseInt(bomDel[1]));
    ok(res, { deleted: parseInt(bomDel[1]) });
    return;
  }

  // BOM import from product_info.json
  if (pathname === '/api/bom/import' && method === 'POST') {
    const piPath = path.join(__dir, 'product_info.json');
    let pi;
    try { pi = JSON.parse(fs.readFileSync(piPath, 'utf8')); } catch(e) { res.writeHead(500); res.end(JSON.stringify({error:'product_info.json not found'})); return; }
    const processes = await getPostProcessTypes();
    const insH = db.prepare('INSERT INTO bom_header (product_code, product_name, brand) VALUES (?,?,?) ON CONFLICT (product_code) DO NOTHING');
    const insI = db.prepare('INSERT INTO bom_items (bom_id, item_type, material_code, material_name, vendor_name, process_type, qty_per, cut_spec, plate_spec, sort_order) VALUES (?,?,?,?,?,?,?,?,?,?)');
    let count = 0;
    const txn = db.transaction(async () => {
      for (const [code, info] of Object.entries(pi)) {
        const r = await insH.run(code, info['제품사양']||'', '');
        const bomId = r.lastInsertRowid || await db.prepare('SELECT bom_id FROM bom_header WHERE product_code=?').get(code)?.bom_id;
        if (!bomId) continue;
        // skip if already has items
        const existing = await db.prepare('SELECT COUNT(*) as c FROM bom_items WHERE bom_id=?').get(bomId);
        if (existing.c > 0) continue;
        let sort = 0;
        // raw material
        if (info['제지사'] || info['원자재코드']) {
          await insI.run(bomId, 'material', info['원자재코드']||'', info['원재료용지명']||'', info['제지사']||'', '원재료', 1, info['절']||'', info['조판']||'', sort++);
        }
        // post-processes
        for (const proc of processes) {
          if (info[proc]) {
            await insI.run(bomId, 'process', '', '', info[proc], proc, 1, '', '', sort++);
          }
        }
        count++;
      }
    });
    await txn();
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
      const dbRows = await db.prepare("SELECT product_code FROM products").all();
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
      const dbRows = await db.prepare("SELECT product_code FROM products").all();
      const dbCodes = new Set(dbRows.map(r => r.product_code));
      const upd = db.prepare(`UPDATE products SET material_code=?, material_name=?, cut_spec=?, jopan=?, paper_maker=?, updated_at=datetime('now','localtime') WHERE product_code=?`);
      let updated = 0, skipped = 0;
      const txn = db.transaction(async () => {
        for (const [code, info] of Object.entries(piData)) {
          if (dbCodes.has(code)) {
            await upd.run(
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
      await txn();
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

  // DD 자동 동기화 비활성화 — 사용자가 본 ERP에서 디디 품목을 직접 등록함
  if (pathname === '/api/dd/sync-status' && method === 'GET') {
    fail(res, 410, 'DD 자동 동기화는 비활성화되었습니다. 품목관리에서 디디 법인 품목을 직접 등록하세요.');
    return;
  }
  if (pathname === '/api/dd/sync' && method === 'POST') {
    fail(res, 410, 'DD 자동 동기화는 비활성화되었습니다. 품목관리에서 디디 법인 품목을 직접 등록하세요.');
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
      const nameRows = await db.prepare(`SELECT product_code, product_name FROM products WHERE product_code IN (${topCodes.map(()=>'?').join(',')})`).all(...topCodes);
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
      const txn = db.transaction(async () => {
        await db.exec('DELETE FROM china_price_tiers');
        const ins = db.prepare('INSERT INTO china_price_tiers (product_code, product_type, qty_tier, unit_price, currency, effective_date) VALUES (?,?,?,?,?,?)');
        for (const p of products) {
          if (!p.product_code || !p.tiers || !p.tiers.length) continue;
          totalProducts++;
          for (const t of p.tiers) {
            await ins.run(p.product_code, p.product_type || 'Card', t.qty || 0, t.price || 0, 'CNY', new Date().toISOString().slice(0,10));
            totalRows++;
          }
        }
      });
      await txn();
      ok(res, { imported: totalRows, products: totalProducts });
    } catch (e) {
      fail(res, 500, '단가 업로드 오류: ' + e.message);
    }
    return;
  }

  // ── 중국 재고 DB API ──
  if (pathname === '/api/china-inventory/upload' && method === 'POST') {
    try {
      const b = await readJSON(req);
      const items = b.items || [];
      if (!items.length) { fail(res, 400, 'items 배열 필요'); return; }
      const txn = db.transaction(async () => {
        await db.exec('DELETE FROM china_inventory');
        const ins = db.prepare('INSERT INTO china_inventory (product_code, product_name, cn_stock, incoming_qty, incoming_date, po_no, order_qty, order_date, due_date, received_qty, unproduced_qty, is_complete) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)');
        for (const r of items) {
          await ins.run(
            r.product_code, r.product_name || '', r.cn_stock || 0, r.incoming_qty || 0, r.incoming_date || '',
            r.po_no || '', r.order_qty || 0, r.order_date || '', r.due_date || '',
            r.received_qty || 0, r.unproduced_qty || 0, r.is_complete || 'N'
          );
        }
      });
      await txn();
      ok(res, { uploaded: items.length });
    } catch (e) {
      fail(res, 500, '중국 재고 업로드 오류: ' + e.message);
    }
    return;
  }

  if (pathname === '/api/china-inventory' && method === 'GET') {
    try {
      const rows = await db.prepare('SELECT * FROM china_inventory ORDER BY product_code').all();
      ok(res, rows);
    } catch (e) {
      fail(res, 500, e.message);
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
    const plans = await db.prepare(q).all(...params);

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
    const txn = db.transaction(async () => {
      for (const it of items) {
        await upsert.run(it.plan_month, it.product_code, it.product_name||'', it.brand||'', it.planned_qty||0, it.confirmed||0, it.notes||'');
      }
    });
    await txn();
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
      await db.prepare(`UPDATE production_plan SET ${sets.join(',')} WHERE plan_id=?`).run(...vals);
    }
    ok(res, { updated: parseInt(planPut[1]) });
    return;
  }

  // DELETE /api/plans?month=YYYY-MM — 월별 초기화
  if (pathname === '/api/plans' && method === 'DELETE') {
    const month = parsed.searchParams.get('month');
    if (month) {
      const r = await db.prepare('DELETE FROM production_plan WHERE plan_month=?').run(month);
      ok(res, { deleted: r.changes, month });
    } else {
      const r = await db.prepare('DELETE FROM production_plan').run();
      ok(res, { deleted: r.changes });
    }
    return;
  }

  const planDel = pathname.match(/^\/api\/plans\/(\d+)$/);
  if (planDel && method === 'DELETE') {
    await db.prepare('DELETE FROM production_plan WHERE plan_id=?').run(parseInt(planDel[1]));
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

    const txn = db.transaction(async () => {
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
        await upsert.run(month, code, '', brand, planned, note);
        count++;
      }
    });
    await txn();
    ok(res, { generated: count, month, method: methodUsed });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  MRP API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/mrp/run' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
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
      const hRows = await db.prepare('SELECT DISTINCT product_code FROM order_history').all();
      if (hRows.length > 0) {
        histCodes = new Set(hRows.map(r => r.product_code));
      }
    }
    // Get plans for the month
    let plans = await db.prepare('SELECT * FROM production_plan WHERE plan_month=? AND planned_qty>0').all(month);
    if (histCodes) {
      const before = plans.length;
      plans = plans.filter(p => histCodes.has(p.product_code));
      console.log(`MRP 발주이력 필터: ${before} → ${plans.length} (${before - plans.length}개 제외)`);
    }
    // Clear previous results for this month
    await db.prepare('DELETE FROM mrp_result WHERE plan_month=?').run(month);
    const insR = db.prepare('INSERT INTO mrp_result (plan_month, product_code, material_code, material_name, vendor_name, process_type, gross_req, on_hand, on_order, net_req, order_qty, unit) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)');
    let resultCount = 0;
    const txn = db.transaction(async () => {
      for (const plan of plans) {
        const bom = await db.prepare('SELECT bi.* FROM bom_items bi JOIN bom_header bh ON bi.bom_id=bh.bom_id WHERE bh.product_code=? ORDER BY bi.sort_order').all(plan.product_code);
        if (!bom.length) continue;
        for (const item of bom) {
          const gross = plan.planned_qty * (item.qty_per || 1);
          // on_hand: for material, look up by material_code in ERP; for process, 0
          let onHand = 0;
          if (item.item_type === 'material' && item.material_code) {
            // find products using this material and sum their available stock
            const relatedBoms = await db.prepare('SELECT bh.product_code FROM bom_header bh JOIN bom_items bi ON bh.bom_id=bi.bom_id WHERE bi.material_code=?').all(item.material_code);
            // Use the current product's ERP available stock as proxy
            const erpItem = erpMap[plan.product_code];
            onHand = erpItem ? Math.max(0, erpItem['가용재고'] || 0) : 0;
          }
          // on_order: sum of outstanding PO qty for this material/product
          let onOrder = 0;
          const lookupCode = item.material_code || plan.product_code;
          const poRows = await db.prepare("SELECT SUM(pi.ordered_qty - pi.received_qty) as pending FROM po_items pi JOIN po_header ph ON pi.po_id=ph.po_id WHERE pi.product_code=? AND ph.status NOT IN ('완료','취소')").get(lookupCode);
          onOrder = poRows?.pending || 0;
          const net = Math.max(0, gross - onHand - onOrder);
          const orderQty = net > 0 ? Math.ceil(net / roundUnit) * roundUnit : 0;
          await insR.run(month, plan.product_code, item.material_code||'', item.material_name||'', item.vendor_name||'', item.process_type||'', gross, onHand, onOrder, net, orderQty, item.unit||'EA');
          resultCount++;
        }
      }
    });
    await txn();
    ok(res, { plan_month: month, results: resultCount, history_filter: histCodes ? histCodes.size : 0 });
    return;
  }

  if (pathname === '/api/mrp/results' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const month = parsed.searchParams.get('month') || '';
    let q = 'SELECT * FROM mrp_result';
    const params = [];
    if (month) { q += ' WHERE plan_month=?'; params.push(month); }
    q += ' ORDER BY vendor_name, product_code';
    ok(res, await db.prepare(q).all(...params));
    return;
  }

  // DELETE /api/mrp/results?month=YYYY-MM — MRP 결과 초기화
  if (pathname === '/api/mrp/results' && method === 'DELETE') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const month = parsed.searchParams.get('month');
    if (month) {
      const r = await db.prepare('DELETE FROM mrp_result WHERE plan_month=?').run(month);
      ok(res, { deleted: r.changes, month });
    } else {
      const r = await db.prepare('DELETE FROM mrp_result').run();
      ok(res, { deleted: r.changes });
    }
    return;
  }

  // ── MRP Calculate (BOM Explosion from Work Orders + Sales Orders) ──
  if (pathname === '/api/mrp/calculate' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const b = await readJSON(req);
    const roundUnit = b.round_unit || 1;

    // 1) Pending work orders (not completed/cancelled)
    const pendingWOs = await db.prepare(
      "SELECT wo_id, product_code, product_name, ordered_qty, produced_qty FROM work_orders WHERE status NOT IN ('completed','cancelled')"
    ).all();

    // 2) Confirmed / in_production sales orders with their items
    const confirmedSOs = await db.prepare(
      "SELECT so.id, soi.product_code, soi.product_name, soi.qty FROM sales_orders so JOIN sales_order_items soi ON so.id=soi.order_id WHERE so.status IN ('confirmed','in_production') AND soi.qty > 0"
    ).all();

    // 3) Build gross requirements per material via BOM explosion
    const grossMap = {}; // material_code -> { product_name, gross_req }

    const explodeBOM = async (productCode, productName, qty) => {
      const bomRows = await db.prepare(
        "SELECT bi.material_code, bi.material_name, bi.qty_per, bi.unit FROM bom_items bi JOIN bom_header bh ON bi.bom_id=bh.bom_id WHERE bh.product_code=? AND bi.item_type='material' AND bi.material_code IS NOT NULL AND bi.material_code != '' ORDER BY bi.sort_order"
      ).all(productCode);
      if (!bomRows.length) return;
      for (const bi of bomRows) {
        const need = qty * (bi.qty_per || 1);
        if (!grossMap[bi.material_code]) {
          grossMap[bi.material_code] = { product_name: bi.material_name || '', unit: bi.unit || 'EA', gross_req: 0 };
        }
        grossMap[bi.material_code].gross_req += need;
      }
    };

    // Explode from work orders (remaining qty = ordered - produced)
    for (const wo of pendingWOs) {
      const remaining = Math.max(0, (wo.ordered_qty || 0) - (wo.produced_qty || 0));
      if (remaining > 0 && wo.product_code) {
        await explodeBOM(wo.product_code, wo.product_name, remaining);
      }
    }

    // Explode from sales orders
    for (const so of confirmedSOs) {
      if (so.qty > 0 && so.product_code) {
        await explodeBOM(so.product_code, so.product_name, so.qty);
      }
    }

    // 4) Get on-hand inventory per material (sum across all warehouses)
    const invRows = await db.prepare(
      "SELECT product_code, SUM(quantity) AS total_qty FROM warehouse_inventory GROUP BY product_code"
    ).all();
    const invMap = {};
    for (const r of invRows) { invMap[r.product_code] = r.total_qty || 0; }

    // 5) Get on-order (pending PO) per material
    const poRows = await db.prepare(
      "SELECT pi.product_code, SUM(pi.ordered_qty - pi.received_qty) AS pending FROM po_items pi JOIN po_header ph ON pi.po_id=ph.po_id WHERE ph.status NOT IN ('완료','취소','cancelled','received') AND pi.ordered_qty > pi.received_qty GROUP BY pi.product_code"
    ).all();
    const poMap = {};
    for (const r of poRows) { poMap[r.product_code] = r.pending || 0; }

    // 6) Build result array
    const materials = [];
    let shortageCount = 0;
    for (const [matCode, info] of Object.entries(grossMap)) {
      const onHand = invMap[matCode] || 0;
      const onOrder = poMap[matCode] || 0;
      const netReq = Math.max(0, info.gross_req - onHand - onOrder);
      const orderQty = netReq > 0 && roundUnit > 1 ? Math.ceil(netReq / roundUnit) * roundUnit : netReq;
      const status = netReq > 0 ? 'shortage' : 'sufficient';
      if (netReq > 0) shortageCount++;
      materials.push({
        product_code: matCode,
        product_name: info.product_name,
        unit: info.unit,
        gross_req: info.gross_req,
        on_hand: onHand,
        on_order: onOrder,
        net_req: netReq,
        order_qty: orderQty,
        status
      });
    }

    // Sort: shortages first, then by net_req descending
    materials.sort((a, b) => (b.net_req - a.net_req) || a.product_code.localeCompare(b.product_code));

    ok(res, {
      materials,
      summary: { total_materials: materials.length, shortage_count: shortageCount },
      sources: { work_orders: pendingWOs.length, sales_orders: confirmedSOs.length }
    });
    return;
  }

  // ── MRP Shortage (items with net_req > 0 only) ──
  if (pathname === '/api/mrp/shortage' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const pendingWOs = await db.prepare(
      "SELECT wo_id, product_code, product_name, ordered_qty, produced_qty FROM work_orders WHERE status NOT IN ('completed','cancelled')"
    ).all();
    const confirmedSOs = await db.prepare(
      "SELECT so.id, soi.product_code, soi.product_name, soi.qty FROM sales_orders so JOIN sales_order_items soi ON so.id=soi.order_id WHERE so.status IN ('confirmed','in_production') AND soi.qty > 0"
    ).all();

    const grossMap = {};
    const explodeBOM = async (productCode, qty) => {
      const bomRows = await db.prepare(
        "SELECT bi.material_code, bi.material_name, bi.qty_per, bi.unit FROM bom_items bi JOIN bom_header bh ON bi.bom_id=bh.bom_id WHERE bh.product_code=? AND bi.item_type='material' AND bi.material_code IS NOT NULL AND bi.material_code != '' ORDER BY bi.sort_order"
      ).all(productCode);
      for (const bi of bomRows) {
        const need = qty * (bi.qty_per || 1);
        if (!grossMap[bi.material_code]) {
          grossMap[bi.material_code] = { product_name: bi.material_name || '', unit: bi.unit || 'EA', gross_req: 0 };
        }
        grossMap[bi.material_code].gross_req += need;
      }
    };

    for (const wo of pendingWOs) {
      const remaining = Math.max(0, (wo.ordered_qty || 0) - (wo.produced_qty || 0));
      if (remaining > 0 && wo.product_code) await explodeBOM(wo.product_code, remaining);
    }
    for (const so of confirmedSOs) {
      if (so.qty > 0 && so.product_code) await explodeBOM(so.product_code, so.qty);
    }

    const invRows = await db.prepare("SELECT product_code, SUM(quantity) AS total_qty FROM warehouse_inventory GROUP BY product_code").all();
    const invMap = {};
    for (const r of invRows) { invMap[r.product_code] = r.total_qty || 0; }

    const poRows = await db.prepare(
      "SELECT pi.product_code, SUM(pi.ordered_qty - pi.received_qty) AS pending FROM po_items pi JOIN po_header ph ON pi.po_id=ph.po_id WHERE ph.status NOT IN ('완료','취소','cancelled','received') AND pi.ordered_qty > pi.received_qty GROUP BY pi.product_code"
    ).all();
    const poMap = {};
    for (const r of poRows) { poMap[r.product_code] = r.pending || 0; }

    const shortages = [];
    for (const [matCode, info] of Object.entries(grossMap)) {
      const onHand = invMap[matCode] || 0;
      const onOrder = poMap[matCode] || 0;
      const netReq = Math.max(0, info.gross_req - onHand - onOrder);
      if (netReq > 0) {
        shortages.push({
          product_code: matCode,
          product_name: info.product_name,
          unit: info.unit,
          gross_req: info.gross_req,
          on_hand: onHand,
          on_order: onOrder,
          net_req: netReq,
          status: 'shortage'
        });
      }
    }
    shortages.sort((a, b) => (b.net_req - a.net_req) || a.product_code.localeCompare(b.product_code));

    ok(res, {
      materials: shortages,
      summary: { total_materials: shortages.length, shortage_count: shortages.length }
    });
    return;
  }

  // Create POs from MRP results
  if (pathname === '/api/mrp/create-po' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const b = await readJSON(req);
    const ids = b.result_ids || [];
    if (!ids.length) { res.writeHead(400); res.end(JSON.stringify({error:'result_ids required'})); return; }
    const results = await db.prepare(`SELECT * FROM mrp_result WHERE result_id IN (${ids.map(()=>'?').join(',')}) AND order_qty > 0`).all(...ids);
    // Group by vendor + process_type
    const groups = {};
    for (const r of results) {
      const key = (r.vendor_name||'미지정') + '|' + (r.process_type === '원재료' ? '원재료' : '후공정');
      if (!groups[key]) groups[key] = [];
      groups[key].push(r);
    }
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const cnt = await db.prepare("SELECT COUNT(*) as c FROM po_header WHERE po_number LIKE ?").get('PO-'+today+'%');
    let seq = (cnt?.c || 0) + 1;
    const created = [];
    const txn = db.transaction(async () => {
      for (const [key, items] of Object.entries(groups)) {
        const [vendor, poType] = key.split('|');
        const poNum = `PO-${today}-${String(seq++).padStart(3,'0')}`;
        const totalQty = items.reduce((s,i) => s + i.order_qty, 0);
        // origin/legal_entity: 첫 번째 품목 기준
        const _mrpSelCols = _hasEntity.products ? 'origin, legal_entity' : 'origin';
        const _mrpFirstProd = await db.prepare(`SELECT ${_mrpSelCols} FROM products WHERE product_code=?`).get(items[0].product_code || '');
        const _mrpOrigin = (_mrpFirstProd && _mrpFirstProd.origin) || '';
        const _mrpEntity = (_mrpFirstProd && _mrpFirstProd.legal_entity === 'dd') ? 'dd' : 'barunson';
        const r = _hasEntity.po_header
          ? await db.prepare('INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, origin, legal_entity, po_date) VALUES (?,?,?,?,?,?,?,?,date(\'now\',\'localtime\'))').run(poNum, poType, vendor, '대기', totalQty, 'MRP 자동생성', _mrpOrigin, _mrpEntity)
          : await db.prepare('INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, origin, po_date) VALUES (?,?,?,?,?,?,?,date(\'now\',\'localtime\'))').run(poNum, poType, vendor, '대기', totalQty, 'MRP 자동생성', _mrpOrigin);
        const poId = r.lastInsertRowid;
        for (const item of items) {
          await db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec) VALUES (?,?,?,?,?,?)').run(poId, item.product_code, '', item.process_type, item.order_qty, item.material_name);
          await db.prepare('UPDATE mrp_result SET status=? WHERE result_id=?').run('ordered', item.result_id);
        }
        created.push({ po_number: poNum, vendor, po_type: poType, items: items.length });
      }
    });
    await txn();
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
      const rows = await db.prepare('SELECT DISTINCT product_code FROM order_history ORDER BY product_code').all();
      ok(res, rows.map(r => r.product_code));
    } else if (mode === 'today') {
      // 오늘 발주된 품목코드 + 수량
      const today = new Date().toISOString().slice(0,10).replace(/-/g,'-');
      const rows = await db.prepare('SELECT product_code, SUM(order_qty) as total_qty FROM order_history WHERE order_date = ? GROUP BY product_code').all(today);
      const map = {};
      rows.forEach(r => { map[r.product_code] = r.total_qty; });
      ok(res, map);
    } else {
      const rows = await db.prepare('SELECT * FROM order_history ORDER BY order_date DESC, history_id DESC LIMIT 5000').all();
      ok(res, rows);
    }
    return;
  }

  // GET /api/order-history/stats — 통계
  if (pathname === '/api/order-history/stats' && method === 'GET') {
    const total = (await db.prepare('SELECT COUNT(*) as cnt FROM order_history').get()).cnt;
    const codes = (await db.prepare('SELECT COUNT(DISTINCT product_code) as cnt FROM order_history').get()).cnt;
    const sheets = (await db.prepare("SELECT DISTINCT source_sheet FROM order_history WHERE source_sheet != ''").all()).map(r => r.source_sheet);
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

    const txn = db.transaction(async () => {
      if (clearExisting) {
        await db.prepare('DELETE FROM order_history').run();
      }
      let count = 0;
      for (const r of rows) {
        const code = (r.product_code || r[5] || '').toString().trim();
        if (!code) continue;
        await ins.run(
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
    const imported = await txn();

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
      const r = await db.prepare(`DELETE FROM order_history WHERE source_sheet IN (${ph})`).run(...b.source_sheets);
      ok(res, { deleted: r.changes, source_sheets: b.source_sheets });
    } else {
      await db.prepare('DELETE FROM order_history').run();
      ok(res, { deleted: true });
    }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  PRODUCT NOTES (품목 특이사항)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/product-notes' && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM product_notes').all();
    const map = {};
    rows.forEach(r => { map[r.product_code] = { note_type: r.note_type, note_text: r.note_text, op_category: r.op_category || '' }; });
    ok(res, map);
    return;
  }

  const pnMatch = pathname.match(/^\/api\/product-notes\/(.+)$/);
  if (pnMatch && method === 'PUT') {
    const code = decodeURIComponent(pnMatch[1]);
    const b = await readJSON(req);
    const noteType = b.note_type || '';
    const noteText = b.note_text || '';
    const opCategory = b.op_category || '';
    if (!noteType && !noteText && !opCategory) {
      await db.prepare('DELETE FROM product_notes WHERE product_code=?').run(code);
    } else {
      await db.prepare("INSERT INTO product_notes (product_code, note_type, note_text, op_category, updated_at) VALUES (?,?,?,?,datetime('now','localtime')) ON CONFLICT(product_code) DO UPDATE SET note_type=excluded.note_type, note_text=excluded.note_text, op_category=excluded.op_category, updated_at=excluded.updated_at").run(code, noteType, noteText, opCategory);
    }
    ok(res, { saved: true, product_code: code });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  원장 매핑 API (ledger_code_map)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/ledger-map' && method === 'GET') {
    try {
      const vendorCode = parsed.searchParams.get('vendor_code') || '';
      let rows;
      if (vendorCode) {
        rows = await db.prepare('SELECT * FROM ledger_code_map WHERE vendor_code=? ORDER BY vendor_item_code').all(vendorCode);
      } else {
        rows = await db.prepare('SELECT * FROM ledger_code_map ORDER BY vendor_code, vendor_item_code').all();
      }
      ok(res, rows);
    } catch (e) { fail(res, 500, e.message); }
    return;
  }

  if (pathname === '/api/ledger-map' && method === 'POST') {
    try {
      const b = await readJSON(req);
      const items = b.items || [];
      for (const it of items) {
        if (!it.vendor_code || !it.vendor_item_code || !it.xerp_item_code) continue;
        await db.prepare("INSERT INTO ledger_code_map (vendor_code, vendor_item_code, vendor_item_name, xerp_item_code, xerp_item_name) VALUES (?,?,?,?,?) ON CONFLICT(vendor_code, vendor_item_code) DO UPDATE SET xerp_item_code=excluded.xerp_item_code, xerp_item_name=excluded.xerp_item_name, vendor_item_name=excluded.vendor_item_name").run(
          it.vendor_code, it.vendor_item_code, it.vendor_item_name || '', it.xerp_item_code, it.xerp_item_name || ''
        );
      }
      ok(res, { saved: items.length });
    } catch (e) { fail(res, 500, e.message); }
    return;
  }

  if (pathname === '/api/ledger-map' && method === 'DELETE') {
    try {
      const b = await readJSON(req);
      await db.prepare('DELETE FROM ledger_code_map WHERE vendor_code=? AND vendor_item_code=?').run(b.vendor_code, b.vendor_item_code);
      ok(res, { deleted: true });
    } catch (e) { fail(res, 500, e.message); }
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  DEFECT / QUALITY MANAGEMENT API (불량 관리)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/defects/summary — 불량 현황 요약
  if (pathname === '/api/defects/summary' && method === 'GET') {
    const byStatus = await db.prepare(`
      SELECT status, COUNT(*) as count FROM defects GROUP BY status
    `).all();
    const byVendor = await db.prepare(`
      SELECT vendor_name, COUNT(*) as defect_count, SUM(defect_qty) as total_defect_qty
      FROM defects GROUP BY vendor_name ORDER BY defect_count DESC
    `).all();
    const byType = await db.prepare(`
      SELECT defect_type, COUNT(*) as count FROM defects WHERE defect_type != '' GROUP BY defect_type ORDER BY count DESC
    `).all();
    const since30 = new Date();
    since30.setDate(since30.getDate() - 30);
    const since30str = since30.toISOString().slice(0, 10);
    const recent30 = await db.prepare(`
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
    if (sp.get('entity') && sp.get('entity') !== 'all' && _hasEntity.defects) { q += ' AND legal_entity=?'; args.push(sp.get('entity')); }
    q += ' ORDER BY defect_date DESC, created_at DESC LIMIT 200';
    ok(res, await db.prepare(q).all(...args));
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
    const lastRow = await db.prepare(
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
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'registered')`);
    const insertLog = db.prepare(`INSERT INTO defect_logs
      (defect_id, defect_number, action, from_status, to_status, actor, details)
      VALUES (?,?,?,?,?,?,?)`);

    const tx = db.transaction(async () => {
      const info = await insertDefect.run(
        defect_number,
        body.po_id || null,
        body.po_number || '',
        vendor_name,
        product_code,
        body.product_name || '',
        defect_date,
        body.defect_type || '',
        body.defect_qty || 0,
        body.order_qty || 0,
        body.severity || 'minor',
        description,
        body.photo_url || '',
        body.claim_type || '',
        body.claim_amount || 0,
      );
      await insertLog.run(
        info.lastInsertRowid,
        defect_number,
        'registered',
        '',
        'registered',
        body.actor || '',
        '불량 접수',
      );
      // 클레임 금액 > 0 → 정산 자동 생성
      const claimAmt = Number(body.claim_amount) || 0;
      if (claimAmt > 0) {
        await db.prepare(`INSERT INTO defect_settlements
          (defect_id, defect_number, vendor_name, claim_amount, settled_amount, balance, status, notes)
          VALUES (?,?,?,?,0,?,?,?)`).run(
          info.lastInsertRowid, defect_number, vendor_name,
          claimAmt, claimAmt, 'open',
          `자동생성: ${body.claim_type || ''} ${body.description || ''}`.trim()
        );
      }
      return info.lastInsertRowid;
    });
    const newId = await tx();
    ok(res, { id: newId, defect_number });
    return;
  }

  // GET /api/defects/:id — 불량 상세 + 이력
  const defectIdMatch = pathname.match(/^\/api\/defects\/(\d+)$/);
  if (defectIdMatch && method === 'GET') {
    const defectId = parseInt(defectIdMatch[1]);
    const defect = await db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    const logs = await db.prepare('SELECT * FROM defect_logs WHERE defect_id=? ORDER BY created_at ASC').all(defectId);
    ok(res, { ...defect, logs });
    return;
  }

  // PUT /api/defects/:id — 불량 수정 (상태 변경 포함)
  const defectPutMatch = pathname.match(/^\/api\/defects\/(\d+)$/);
  if (defectPutMatch && method === 'PUT') {
    const defectId = parseInt(defectPutMatch[1]);
    const defect = await db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
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

    const tx = db.transaction(async () => {
      // Auto-set resolved_date when resolving
      if (body.status === 'resolved' && !body.resolved_date && !defect.resolved_date) {
        const today = new Date().toISOString().slice(0, 10);
        sets.splice(sets.length - 1, 0, 'resolved_date=?');
        vals.splice(vals.length - 1, 0, today);
      }
      await db.prepare(`UPDATE defects SET ${sets.join(',')} WHERE id=?`).run(...vals);
      // claim_amount 변경 시 settlement 동기화 (상계 적용 안 된 건만)
      if (body.claim_amount !== undefined) {
        const newAmt = Number(body.claim_amount) || 0;
        const existing = await db.prepare('SELECT * FROM defect_settlements WHERE defect_id=?').get(defectId);
        if (existing) {
          const settled = Number(existing.settled_amount) || 0;
          if (settled === 0) {
            // 정산 적용 전 → 금액만 업데이트
            const newBalance = newAmt;
            const newStatus = newAmt > 0 ? 'open' : 'cancelled';
            await db.prepare(`UPDATE defect_settlements SET claim_amount=?, balance=?, status=?, updated_at=datetime('now','localtime') WHERE id=?`)
              .run(newAmt, newBalance, newStatus, existing.id);
          }
          // 이미 일부 정산된 건은 수정 금지 (감사 추적)
        } else if (newAmt > 0) {
          await db.prepare(`INSERT INTO defect_settlements
            (defect_id, defect_number, vendor_name, claim_amount, settled_amount, balance, status, notes)
            VALUES (?,?,?,?,0,?,?,?)`).run(
            defectId, defect.defect_number || '', defect.vendor_name || '',
            newAmt, newAmt, 'open', 'PUT 자동생성'
          );
        }
      }
      if (statusChanged) {
        const actionLabel = body.status === 'in_progress' ? '처리 시작' :
                            body.status === 'resolved'    ? '처리 완료' : '상태 변경';
        await insertLog.run(
          defectId, defect.defect_number,
          actionLabel,
          defect.status, body.status,
          body.actor || '',
          body.resolution || body.details || ''
        );
      }
    });
    await tx();
    ok(res, { id: defectId, status: body.status || defect.status });
    return;
  }

  // POST /api/defects/:id/log — 불량 수동 메모/이력 추가
  const defectLogMatch = pathname.match(/^\/api\/defects\/(\d+)\/log$/);
  if (defectLogMatch && method === 'POST') {
    const defectId = parseInt(defectLogMatch[1]);
    const defect = await db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    const body = await readJSON(req);
    if (!body.action) { fail(res, 400, 'action 필수'); return; }
    await db.prepare(`INSERT INTO defect_logs (defect_id, defect_number, action, from_status, to_status, actor, details)
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
    const defect = await db.prepare('SELECT * FROM defects WHERE id=?').get(defectId);
    if (!defect) { fail(res, 404, '불량 접수 건 없음'); return; }
    if (!defect.product_code) { fail(res, 400, 'product_code가 없어 발주를 생성할 수 없습니다'); return; }

    const poNumber = await generatePoNumber();
    const defectQty = defect.defect_qty || 0;
    const notes = `불량처리 발주 (${defect.defect_number}) - ${defect.description || '불량 재작업'}`;

    // origin 결정
    const _defOriginProd = await db.prepare('SELECT origin FROM products WHERE product_code=?').get(defect.product_code);
    const _defOrigin = (_defOriginProd && _defOriginProd.origin) || '';

    const tx = db.transaction(async () => {
      // PO 헤더 생성
      const hdrInfo = await db.prepare(`
        INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, defect_id, defect_number, origin, po_date)
        VALUES (?, 'post_process', ?, '대기', ?, ?, ?, ?, ?, date('now','localtime'))
      `).run(poNumber, defect.vendor_name || '', defectQty, notes, defectId, defect.defect_number || '', _defOrigin);

      const poId = hdrInfo.lastInsertRowid;

      // PO 품목 추가
      await db.prepare(`
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
      await db.prepare(`
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

    const poId = await tx();
    ok(res, { po_id: poId, po_number: poNumber, defect_id: defectId, defect_number: defect.defect_number });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  DEFECT SETTLEMENTS API (불량 클레임 자동 상계)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/settlements — 정산 목록 (필터: vendor_name, status)
  if (pathname === '/api/settlements' && method === 'GET') {
    const sp = parsed.searchParams;
    let q = 'SELECT * FROM defect_settlements WHERE 1=1';
    const args = [];
    if (sp.get('vendor_name')) { q += ' AND vendor_name=?'; args.push(sp.get('vendor_name')); }
    if (sp.get('status'))      { q += ' AND status=?';      args.push(sp.get('status')); }
    q += ' ORDER BY status ASC, created_at DESC LIMIT 500';
    const rows = await db.prepare(q).all(...args);
    // PostgreSQL bigint/numeric → number
    rows.forEach(r => {
      r.claim_amount = Number(r.claim_amount) || 0;
      r.settled_amount = Number(r.settled_amount) || 0;
      r.balance = Number(r.balance) || 0;
    });
    ok(res, rows);
    return;
  }

  // GET /api/settlements/summary — 업체별 미정산 합계
  if (pathname === '/api/settlements/summary' && method === 'GET') {
    const byVendor = await db.prepare(`
      SELECT vendor_name,
        COUNT(*) FILTER (WHERE status IN ('open','partial')) as open_count,
        COALESCE(SUM(CASE WHEN status IN ('open','partial') THEN balance ELSE 0 END), 0) as open_balance,
        COALESCE(SUM(claim_amount), 0) as total_claim,
        COALESCE(SUM(settled_amount), 0) as total_settled
      FROM defect_settlements
      GROUP BY vendor_name
      HAVING COUNT(*) FILTER (WHERE status IN ('open','partial')) > 0
      ORDER BY open_balance DESC
    `).all().catch(async () => {
      // 폴백 (FILTER 미지원 환경): 두 단계 쿼리
      const rows = await db.prepare(`
        SELECT vendor_name, status, claim_amount, settled_amount, balance FROM defect_settlements
      `).all();
      const map = {};
      rows.forEach(r => {
        const v = r.vendor_name;
        if (!map[v]) map[v] = { vendor_name: v, open_count: 0, open_balance: 0, total_claim: 0, total_settled: 0 };
        map[v].total_claim += Number(r.claim_amount) || 0;
        map[v].total_settled += Number(r.settled_amount) || 0;
        if (r.status === 'open' || r.status === 'partial') {
          map[v].open_count++;
          map[v].open_balance += Number(r.balance) || 0;
        }
      });
      return Object.values(map).filter(x => x.open_count > 0).sort((a, b) => b.open_balance - a.open_balance);
    });
    byVendor.forEach(r => {
      r.open_count = Number(r.open_count) || 0;
      r.open_balance = Number(r.open_balance) || 0;
      r.total_claim = Number(r.total_claim) || 0;
      r.total_settled = Number(r.total_settled) || 0;
    });
    const totals = {
      vendors: byVendor.length,
      open_count: byVendor.reduce((s, r) => s + r.open_count, 0),
      open_balance: byVendor.reduce((s, r) => s + r.open_balance, 0)
    };
    ok(res, { totals, byVendor });
    return;
  }

  // POST /api/settlements/sync — defects.claim_amount > 0 중 누락분 일괄 생성 (멱등)
  if (pathname === '/api/settlements/sync' && method === 'POST') {
    const missing = await db.prepare(`
      SELECT d.id, d.defect_number, d.vendor_name, d.claim_amount, d.claim_type, d.description
      FROM defects d
      WHERE COALESCE(d.claim_amount,0) > 0
        AND NOT EXISTS (SELECT 1 FROM defect_settlements s WHERE s.defect_id = d.id)
    `).all();
    let created = 0;
    for (const d of missing) {
      const amt = Number(d.claim_amount) || 0;
      if (amt <= 0) continue;
      await db.prepare(`INSERT INTO defect_settlements
        (defect_id, defect_number, vendor_name, claim_amount, settled_amount, balance, status, notes)
        VALUES (?,?,?,?,0,?,?,?)`).run(
        d.id, d.defect_number || '', d.vendor_name || '',
        amt, amt, 'open',
        `sync 자동생성: ${d.claim_type || ''} ${d.description || ''}`.trim()
      );
      created++;
    }
    ok(res, { created, scanned: missing.length });
    return;
  }

  // POST /api/settlements/:id/apply — 정산 적용 (PO 또는 수동 정산)
  const settleApplyMatch = pathname.match(/^\/api\/settlements\/(\d+)\/apply$/);
  if (settleApplyMatch && method === 'POST') {
    const id = parseInt(settleApplyMatch[1]);
    const body = await readJSON(req);
    const settle = await db.prepare('SELECT * FROM defect_settlements WHERE id=?').get(id);
    if (!settle) { fail(res, 404, '정산 건 없음'); return; }
    const balance = Number(settle.balance) || 0;
    if (balance <= 0 || settle.status === 'closed' || settle.status === 'cancelled') {
      fail(res, 400, '이미 정산 완료/취소된 건입니다'); return;
    }
    const applyAmt = Math.min(Number(body.amount) || balance, balance);
    if (applyAmt <= 0) { fail(res, 400, '정산 금액 오류'); return; }
    const newSettled = (Number(settle.settled_amount) || 0) + applyAmt;
    const newBalance = (Number(settle.claim_amount) || 0) - newSettled;
    const newStatus = newBalance <= 0.01 ? 'closed' : 'partial';
    await db.prepare(`UPDATE defect_settlements
      SET settled_amount=?, balance=?, status=?,
          applied_po_id=COALESCE(?, applied_po_id),
          applied_po_number=COALESCE(?, applied_po_number),
          applied_at=datetime('now','localtime'),
          applied_by=?,
          notes=CASE WHEN ?='' THEN notes ELSE notes || ' | ' || ? END,
          updated_at=datetime('now','localtime')
      WHERE id=?`).run(
      newSettled, newBalance, newStatus,
      body.po_id || null, body.po_number || null,
      body.actor || 'system',
      body.notes || '', body.notes || '',
      id
    );
    // defect 이력 로그
    if (settle.defect_id) {
      await db.prepare(`INSERT INTO defect_logs
        (defect_id, defect_number, action, from_status, to_status, actor, details)
        VALUES (?,?,?,?,?,?,?)`).run(
        settle.defect_id, settle.defect_number,
        '정산 적용', settle.status, newStatus,
        body.actor || 'system',
        `${applyAmt.toLocaleString()}원 정산` + (body.po_number ? ` (PO ${body.po_number})` : '')
      );
    }
    ok(res, { id, applied: applyAmt, new_balance: newBalance, new_status: newStatus });
    return;
  }

  // POST /api/settlements/:id/cancel — 정산 취소
  const settleCancelMatch = pathname.match(/^\/api\/settlements\/(\d+)\/cancel$/);
  if (settleCancelMatch && method === 'POST') {
    const id = parseInt(settleCancelMatch[1]);
    const body = await readJSON(req).catch(() => ({}));
    const settle = await db.prepare('SELECT * FROM defect_settlements WHERE id=?').get(id);
    if (!settle) { fail(res, 404, '정산 건 없음'); return; }
    await db.prepare(`UPDATE defect_settlements SET status='cancelled', updated_at=datetime('now','localtime'),
      notes=notes || ' | 취소: ' || ? WHERE id=?`).run(body.reason || '', id);
    ok(res, { id, cancelled: true });
    return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  INCOMING INSPECTION API (수입검사)
  // ════════════════════════════════════════════════════════════════════

  // GET /api/inspections
  if (pathname === '/api/inspections' && method === 'GET') {
    try {
      const rows = await db.prepare('SELECT * FROM incoming_inspections ORDER BY created_at DESC LIMIT 200').all();
      ok(res, rows);
    } catch (e) {
      if (e.message.includes('does not exist')) ok(res, []);
      else fail(res, 500, e.message);
    }
    return;
  }

  // POST /api/inspections
  if (pathname === '/api/inspections' && method === 'POST') {
    const body = await readJSON(req);
    const passRate = body.total_qty > 0 ? Math.round(body.pass_qty / body.total_qty * 1000) / 10 : 0;
    const result = body.fail_qty > 0 ? (passRate < 90 ? 'rejected' : 'conditional') : 'passed';
    let info;
    try {
    info = await db.prepare(`INSERT INTO incoming_inspections (po_id, po_number, vendor_name, inspection_date, inspector, result, items_json, total_qty, pass_qty, fail_qty, pass_rate, notes)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      body.po_id || null, body.po_number || '', body.vendor_name || '',
      body.inspection_date || new Date().toISOString().slice(0, 10),
      body.inspector || '', result, JSON.stringify(body.items || []),
      body.total_qty || 0, body.pass_qty || 0, body.fail_qty || 0, passRate, body.notes || ''
    );
    // 불합격 시 자동 NCR 생성
    if (result === 'rejected' || result === 'conditional') {
      const ncrNum = 'NCR' + new Date().toISOString().slice(2, 10).replace(/-/g, '') + '-' + String(info.lastInsertRowid).padStart(3, '0');
      await db.prepare(`INSERT INTO ncr (ncr_number, inspection_id, po_id, vendor_name, product_code, ncr_type, description, status, severity)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(
        ncrNum, info.lastInsertRowid, body.po_id || null, body.vendor_name || '',
        body.product_code || '', 'incoming',
        `수입검사 ${result === 'rejected' ? '불합격' : '조건부합격'}: 불량 ${body.fail_qty}건 / 전체 ${body.total_qty}건 (합격률 ${passRate}%)`,
        'open', result === 'rejected' ? 'critical' : 'minor'
      );
    }
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'inspection_create', 'inspections', info.lastInsertRowid, `수입검사: ${body.po_number || ''} → ${result}`, clientIP);
    ok(res, { inspection_id: info.lastInsertRowid, result, pass_rate: passRate });
    } catch (e) {
      if (e.message.includes('does not exist')) fail(res, 500, 'incoming_inspections 테이블이 없습니다. DB 관리자에게 테이블 생성을 요청하세요.');
      else fail(res, 500, e.message);
    }
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
    ok(res, await db.prepare(sql).all(...params));
    return;
  }

  // POST /api/ncr
  if (pathname === '/api/ncr' && method === 'POST') {
    const body = await readJSON(req);
    const ncrNum = 'NCR' + new Date().toISOString().slice(2, 10).replace(/-/g, '') + '-' + String(Date.now()).slice(-4);
    const info = await db.prepare(`INSERT INTO ncr (ncr_number, defect_id, inspection_id, po_id, vendor_name, product_code, ncr_type, description, severity, responsible, due_date)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(
      ncrNum, body.defect_id || null, body.inspection_id || null, body.po_id || null,
      body.vendor_name || '', body.product_code || '', body.ncr_type || 'process',
      body.description || '', body.severity || 'minor', body.responsible || '', body.due_date || ''
    );
    await db.prepare("INSERT INTO ncr_logs (ncr_id, action, to_status, actor, details) VALUES (?,'created','open',?,?)").run(info.lastInsertRowid, body.actor || '', 'NCR 생성');
    ok(res, { ncr_id: info.lastInsertRowid, ncr_number: ncrNum });
    return;
  }

  // PUT /api/ncr/:id — NCR 상태 변경 (open→analysis→action→closed)
  const ncrPut = pathname.match(/^\/api\/ncr\/(\d+)$/);
  if (ncrPut && method === 'PUT') {
    const ncrId = parseInt(ncrPut[1]);
    const ncr = await db.prepare('SELECT * FROM ncr WHERE ncr_id=?').get(ncrId);
    if (!ncr) { fail(res, 404, 'NCR not found'); return; }
    const body = await readJSON(req);
    const sets = [], vals = [];
    ['status','root_cause','corrective_action','preventive_action','responsible','due_date','severity','description'].forEach(f => {
      if (body[f] !== undefined) { sets.push(`${f}=?`); vals.push(body[f]); }
    });
    if (body.status === 'closed' && !ncr.closed_at) { sets.push("closed_at=NOW()"); }
    sets.push("updated_at=NOW()");
    vals.push(ncrId);
    await db.prepare(`UPDATE ncr SET ${sets.join(',')} WHERE ncr_id=?`).run(...vals);
    if (body.status && body.status !== ncr.status) {
      const labels = { analysis: '원인분석 중', action: '시정조치 중', closed: '종결' };
      await db.prepare("INSERT INTO ncr_logs (ncr_id, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?)").run(
        ncrId, labels[body.status] || '상태변경', ncr.status, body.status, body.actor || '', body.details || ''
      );
    }
    ok(res, { ncr_id: ncrId, status: body.status || ncr.status });
    return;
  }

  // GET /api/ncr/:id
  const ncrGet = pathname.match(/^\/api\/ncr\/(\d+)$/);
  if (ncrGet && method === 'GET') {
    const ncr = await db.prepare('SELECT * FROM ncr WHERE ncr_id=?').get(parseInt(ncrGet[1]));
    if (!ncr) { fail(res, 404, 'NCR not found'); return; }
    ncr.logs = await db.prepare('SELECT * FROM ncr_logs WHERE ncr_id=? ORDER BY created_at ASC').all(ncr.ncr_id);
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
      ok(res, await db.prepare('SELECT * FROM vendor_scorecard WHERE vendor_name=? ORDER BY eval_month DESC').all(vendor));
    } else {
      // 최신 월 기준 전체 업체 스코어카드
      const latest = await db.prepare('SELECT MAX(eval_month) as m FROM vendor_scorecard').get();
      if (latest && latest.m) {
        ok(res, await db.prepare('SELECT * FROM vendor_scorecard WHERE eval_month=? ORDER BY total_score DESC').all(latest.m));
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
    const vendors = await db.prepare('SELECT DISTINCT vendor_name FROM po_header WHERE po_date LIKE ? AND vendor_name IS NOT NULL').all(monthLike);
    const results = [];
    for (const { vendor_name } of vendors) {
      if (!vendor_name) continue;
      // 납기 준수율
      const totalPO = (await db.prepare("SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date LIKE ? AND status != 'cancelled'").get(vendor_name, monthLike)).cnt;
      const ontimePO = (await db.prepare("SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date LIKE ? AND status IN ('received','os_pending') AND (due_date IS NULL OR updated_at <= due_date || ' 23:59:59')").get(vendor_name, monthLike)).cnt;
      const deliveryScore = totalPO > 0 ? Math.round(ontimePO / totalPO * 100) : 100;
      // 품질 점수
      const defectCount = (await db.prepare("SELECT COUNT(*) as cnt FROM defects WHERE vendor_name=? AND defect_date LIKE ?").get(vendor_name, monthLike)).cnt;
      const qualityScore = Math.max(0, 100 - defectCount * 10);
      // 종합
      const totalScore = Math.round(deliveryScore * 0.5 + qualityScore * 0.4 + 80 * 0.1); // 가격은 기본 80점
      await db.prepare(`INSERT OR REPLACE INTO vendor_scorecard (vendor_name, eval_month, delivery_score, quality_score, price_score, total_score, total_po, ontime_po, total_defects)
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
    const rows = await db.prepare(`SELECT * FROM production_requests WHERE ${where} ORDER BY created_at DESC`).all(...vals);
    const byStatus = await db.prepare(`SELECT status, COUNT(*) as count FROM production_requests GROUP BY status`).all();
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
    await db.prepare(`INSERT INTO production_request_logs (request_id, request_number, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?,?)`)
      .run(info.lastInsertRowid, num, '생산요청 등록', '', 'requested', body.requester || 'system', `${body.product_name} ${body.requested_qty || 0}부`);
    ok(res, { id: info.lastInsertRowid, request_number: num });
    return;
  }

  // GET /api/production-requests/:id — 상세
  const prDetailMatch = pathname.match(/^\/api\/production-requests\/(\d+)$/);
  if (prDetailMatch && method === 'GET') {
    const prId = parseInt(prDetailMatch[1]);
    const pr = await db.prepare('SELECT * FROM production_requests WHERE id=?').get(prId);
    if (!pr) { fail(res, 404, '요청 없음'); return; }
    const logs = await db.prepare('SELECT * FROM production_request_logs WHERE request_id=? ORDER BY created_at ASC').all(prId);
    ok(res, { ...pr, logs });
    return;
  }

  // PUT /api/production-requests/:id — 수정/상태변경
  if (prDetailMatch && method === 'PUT') {
    const prId = parseInt(prDetailMatch[1]);
    const pr = await db.prepare('SELECT * FROM production_requests WHERE id=?').get(prId);
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

    await db.prepare(`UPDATE production_requests SET ${sets.join(',')} WHERE id=?`).run(...vals);

    if (statusChanged) {
      const statusNames = { requested:'요청등록', design_confirmed:'디자인확인', data_confirmed:'데이터확인', in_production:'생산진행', completed:'완료', cancelled:'취소' };
      await db.prepare(`INSERT INTO production_request_logs (request_id, request_number, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?,?)`)
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
    const pr = await db.prepare('SELECT * FROM production_requests WHERE id=?').get(prId);
    if (!pr) { fail(res, 404, '요청 없음'); return; }
    const body = await readJSON(req);
    await db.prepare(`INSERT INTO production_request_logs (request_id, request_number, action, from_status, to_status, actor, details) VALUES (?,?,?,?,?,?,?)`)
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
    const rows = await db.prepare(`SELECT * FROM product_spec_master WHERE ${where} ORDER BY product_type, spec_name`).all(...vals);
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
    await db.prepare(`UPDATE product_spec_master SET ${sets.join(',')} WHERE id=?`).run(...vals);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/specs/:id — 삭제
  if (specPutMatch && method === 'DELETE') {
    const specId = parseInt(specPutMatch[1]);
    await db.prepare('DELETE FROM product_spec_master WHERE id=?').run(specId);
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
    ok(res, await db.prepare(sql).all(...params));
    return;
  }

  // POST /api/accessories — 부속품 추가
  if (pathname === '/api/accessories' && method === 'POST') {
    const b = await readJSON(req);
    const info = await db.prepare(`INSERT INTO accessories (acc_code,acc_name,acc_type,current_stock,min_stock,unit,vendor,memo,origin) VALUES (?,?,?,?,?,?,?,?,?)`).run(
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
    const fields = [], values = [];
    for (const col of ['acc_code','acc_name','acc_type','current_stock','min_stock','unit','vendor','memo','origin']) {
      if (b[col] !== undefined) { fields.push(`${col}=?`); values.push(b[col]); }
    }
    fields.push(`updated_at=datetime('now','localtime')`);
    values.push(id);
    await db.prepare(`UPDATE accessories SET ${fields.join(',')} WHERE id=?`).run(...values);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/accessories/:id
  const accDel = pathname.match(/^\/api\/accessories\/(\d+)$/);
  if (accDel && method === 'DELETE') {
    await db.prepare('DELETE FROM product_accessories WHERE acc_id=?').run(accDel[1]);
    await db.prepare('DELETE FROM accessories WHERE id=?').run(accDel[1]);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/accessories/:id/products — 부속품을 사용하는 제품 목록
  const accProdGet = pathname.match(/^\/api\/accessories\/(\d+)\/products$/);
  if (accProdGet && method === 'GET') {
    const rows = await db.prepare(`SELECT p.product_code, p.product_name, pa.qty_per, pa.id AS pa_id FROM product_accessories pa LEFT JOIN products p ON p.product_code=pa.product_code WHERE pa.acc_id=? ORDER BY pa.product_code`).all(accProdGet[1]);
    ok(res, rows);
    return;
  }

  // GET /api/products/:code/accessories — 제품별 부속품
  const prodAccGet = pathname.match(/^\/api\/products\/([^/]+)\/accessories$/);
  if (prodAccGet && method === 'GET') {
    const code = decodeURIComponent(prodAccGet[1]);
    const rows = await db.prepare(`SELECT a.*, pa.qty_per, pa.id AS link_id FROM accessories a JOIN product_accessories pa ON a.id=pa.acc_id WHERE pa.product_code=? ORDER BY a.acc_type, a.acc_name`).all(code);
    ok(res, rows);
    return;
  }

  // POST /api/products/:code/accessories — 제품에 부속품 연결
  const prodAccPost = pathname.match(/^\/api\/products\/([^/]+)\/accessories$/);
  if (prodAccPost && method === 'POST') {
    const code = decodeURIComponent(prodAccPost[1]);
    const b = await readJSON(req);
    try {
      const info = await db.prepare(`INSERT OR REPLACE INTO product_accessories (product_code, acc_id, qty_per) VALUES (?,?,?)`).run(code, b.acc_id, b.qty_per||1);
      ok(res, { id: info.lastInsertRowid });
    } catch(e) { fail(res, 400, e.message); }
    return;
  }

  // DELETE /api/product-accessories/:id — 연결 제거
  const prodAccDel = pathname.match(/^\/api\/product-accessories\/(\d+)$/);
  if (prodAccDel && method === 'DELETE') {
    await db.prepare('DELETE FROM product_accessories WHERE id=?').run(prodAccDel[1]);
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
    const rows = await db.prepare(`SELECT * FROM tasks WHERE ${where} ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 WHEN 'low' THEN 4 ELSE 5 END, due_date ASC, created_at DESC`).all(...vals);
    ok(res, rows);
    return;
  }

  // GET /api/tasks/:id — 단건
  if (taskMatch && method === 'GET') {
    const row = await db.prepare('SELECT * FROM tasks WHERE id=?').get(parseInt(taskMatch[1]));
    if (!row) { fail(res, 404, 'Not found'); return; }
    const comments = await db.prepare('SELECT * FROM task_comments WHERE task_id=? ORDER BY created_at ASC').all(row.id);
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
      String((await db.prepare("SELECT COUNT(*) as c FROM tasks").get()).c + 1).padStart(3,'0');
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
    await db.prepare(`UPDATE tasks SET ${sets.join(',')} WHERE id=?`).run(...vals2);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'task_update', 'tasks', id, `업무 수정: ${b.status ? '상태→'+b.status : ''}${b.title ? ' 제목→'+b.title : ''}`, clientIP);
    ok(res, { updated: true });
    return;
  }

  // DELETE /api/tasks/:id — 삭제
  if (taskMatch && method === 'DELETE') {
    const id = parseInt(taskMatch[1]);
    await db.prepare('DELETE FROM task_comments WHERE task_id=?').run(id);
    await db.prepare('DELETE FROM tasks WHERE id=?').run(id);
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'task_delete', 'tasks', id, `업무 삭제`, clientIP);
    ok(res, { deleted: true });
    return;
  }

  // GET /api/tasks/:id/comments — 댓글 목록
  if (taskCommentMatch && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM task_comments WHERE task_id=? ORDER BY created_at ASC').all(parseInt(taskCommentMatch[1]));
    ok(res, rows);
    return;
  }

  // POST /api/tasks/:id/comments — 댓글 추가
  if (taskCommentMatch && method === 'POST') {
    const b = await readJSON(req);
    if (!b.content) { fail(res, 400, 'content 필수'); return; }
    const info = await db.prepare('INSERT INTO task_comments (task_id, author, content) VALUES (?,?,?)').run(
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
    await db.prepare('DELETE FROM task_steps WHERE task_id=?').run(taskId);
    const steps = b.custom_steps && b.custom_steps.length ? b.custom_steps : tpl.steps;
    const insert = db.prepare('INSERT INTO task_steps (task_id, step_order, step_name, step_type) VALUES (?,?,?,?)');
    for (let i = 0; i < steps.length; i++) { const s = steps[i]; await insert.run(taskId, i, s.name, s.type || 'text'); }
    await db.prepare("UPDATE tasks SET template_id=? WHERE id=?").run(b.template_id, taskId);
    ok(res, { created: steps.length });
    return;
  }

  // GET /api/tasks/:id/steps — 단계 목록
  const stepsMatch = pathname.match(/^\/api\/tasks\/(\d+)\/steps$/);
  if (stepsMatch && method === 'GET') {
    const rows = await db.prepare('SELECT * FROM task_steps WHERE task_id=? ORDER BY step_order').all(parseInt(stepsMatch[1]));
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
    await db.prepare(`UPDATE task_steps SET ${sets.join(',')} WHERE id=?`).run(...vals);
    // 모든 단계 완료 시 task 상태 자동 업데이트
    const step = await db.prepare('SELECT task_id FROM task_steps WHERE id=?').get(stepId);
    if (step) {
      const total = (await db.prepare('SELECT COUNT(*) as c FROM task_steps WHERE task_id=?').get(step.task_id)).c;
      const done = (await db.prepare("SELECT COUNT(*) as c FROM task_steps WHERE task_id=? AND is_done=1").get(step.task_id)).c;
      if (total > 0 && done === total) {
        await db.prepare("UPDATE tasks SET status='done', completed_at=datetime('now','localtime') WHERE id=?").run(step.task_id);
      } else if (done > 0) {
        await db.prepare("UPDATE tasks SET status='in_progress' WHERE id=? AND status='todo'").run(step.task_id);
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
      ? await db.prepare('SELECT * FROM gift_sets ORDER BY set_name').all()
      : await db.prepare('SELECT * FROM gift_sets WHERE status=? ORDER BY set_name').all(status);
    const bomStmt = db.prepare('SELECT * FROM gift_set_bom WHERE set_id=? ORDER BY item_type, id');
    // 생산재고 = 전체 assembly 합산
    const totalAssemblyStmt = db.prepare("SELECT COALESCE(SUM(qty),0) as total FROM gift_set_transactions WHERE set_id=? AND tx_type='assembly'");
    // 오늘 생산량
    const todayAssemblyStmt = db.prepare("SELECT COALESCE(SUM(qty),0) as total FROM gift_set_transactions WHERE set_id=? AND tx_type='assembly' AND created_at::date=CURRENT_DATE");

    // XERP 출고재고: 캐시 사용 (10분 간격 갱신)
    const now = Date.now();
    if (now - giftSetShipmentCacheTime > 600000) {
      // 백그라운드 캐시 갱신 (응답 블로킹 안 함)
      const xerpCodes = sets.map(s => (s.xerp_code || '').trim()).filter(Boolean);
      if (xerpCodes.length && xerpPool) {
        (async () => {
          try {
            const req = xerpPool.request();
            const placeholders = (await Promise.all(xerpCodes.map(async (c, i) => { req.input(`xc${i}`, sql.VarChar(50), c); return `@xc${i}`; }))).join(',');
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

    // 금일 출고 (오늘 날짜 shipped된 스케줄)
    const todayStr = new Date().toLocaleDateString('en-CA');
    const day7Str = new Date(Date.now() + 7*86400000).toLocaleDateString('en-CA');
    const todayShipStmt = db.prepare("SELECT COALESCE(SUM(shipped_qty),0) as total FROM gift_shipment_schedule WHERE set_id=? AND ship_date=? AND status='shipped'");
    // 출고 예정 (오늘~7일, 미출고 planned)
    const upcomingShipStmt = db.prepare("SELECT ship_date, SUM(planned_qty) as qty FROM gift_shipment_schedule WHERE set_id=? AND status='planned' AND ship_date >= ? AND ship_date <= ? GROUP BY ship_date ORDER BY ship_date");
    const upcomingTotalStmt = db.prepare("SELECT COALESCE(SUM(planned_qty),0) as total FROM gift_shipment_schedule WHERE set_id=? AND status='planned' AND ship_date >= ? AND ship_date <= ?");

    for (const s of sets) {
      s.bom = await bomStmt.all(s.id);
      const totalAssembly = (await totalAssemblyStmt.get(s.id))?.total || 0;
      const todayAssembly = (await todayAssemblyStmt.get(s.id))?.total || 0;
      const xerpCode = (s.xerp_code || '').trim();
      const totalShipped = xerpCode ? (xerpShipments[xerpCode] || 0) : 0;
      // 금일 출고 + 출고예정
      const todayShipped = (await todayShipStmt.get(s.id, todayStr))?.total || 0;
      const upcomingDays = await upcomingShipStmt.all(s.id, todayStr, day7Str);
      const upcomingTotal = (await upcomingTotalStmt.get(s.id, todayStr, day7Str))?.total || 0;
      // 재고 계산
      s.production_stock = totalAssembly;                              // 생산재고 (조립 누적)
      s.shipped_stock = totalShipped;                                  // 출고재고 (XERP 누적)
      s.today_shipped = todayShipped;                                  // 금일 출고
      s.today_assembled = todayAssembly;                               // 금일 생산
      s.upcoming_shipments = upcomingDays;                             // 출고예정 일별 내역
      s.upcoming_total = upcomingTotal;                                // 출고예정 합계 (7일)
      s.remaining_stock = s.base_stock + totalAssembly - totalShipped; // 잔여재고
      s.available_stock = s.remaining_stock - upcomingTotal;           // 가용재고 (잔여 - 출고예정)
      s.current_stock = s.remaining_stock;
      // 소비기한 관련: 최소 소비기한과 D-day 계산
      const expiryRows = await db.prepare("SELECT expiry_date, SUM(qty) as qty FROM gift_set_transactions WHERE set_id=? AND tx_type IN ('assembly','base') AND expiry_date!='' AND expiry_date IS NOT NULL GROUP BY expiry_date ORDER BY expiry_date").all(s.id);
      s.expiry_batches = expiryRows.filter(r => r.expiry_date);
      if (s.expiry_date) {
        const today = new Date().toISOString().slice(0, 10);
        const daysLeft = Math.ceil((new Date(s.expiry_date) - new Date(today)) / 86400000);
        s.expiry_days_left = daysLeft;
      }
    }
    ok(res, sets);
    return;
  }

  // POST /api/gift-sets — 세트 등록
  if (pathname === '/api/gift-sets' && method === 'POST') {
    const body = await readJSON(req);
    const { set_code, set_name, description, base_stock, xerp_code, bom, expiry_date } = body;
    if (!set_code || !set_name) { fail(res, 400, '세트코드와 이름은 필수입니다'); return; }
    const existing = await db.prepare('SELECT id FROM gift_sets WHERE set_code=?').get(set_code);
    if (existing) { fail(res, 409, '이미 존재하는 세트코드입니다'); return; }
    const initStock = parseInt(base_stock) || 0;
    const result = await db.prepare('INSERT INTO gift_sets (set_code, set_name, description, base_stock, current_stock, xerp_code, expiry_date) VALUES (?,?,?,?,?,?,?) RETURNING id').get(set_code, set_name, description || '', initStock, initStock, xerp_code || '', expiry_date || '');
    const setId = result.id;
    if (initStock > 0) {
      await db.prepare('INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo) VALUES (?,?,?,?,?)').run(setId, 'base', initStock, body.operator || '', '기초재고 설정');
    }
    if (Array.isArray(bom) && bom.length) {
      const ins = db.prepare('INSERT OR IGNORE INTO gift_set_bom (set_id, item_type, item_code, item_name, qty_per, unit) VALUES (?,?,?,?,?,?)');
      for (const b of bom) await ins.run(setId, b.item_type || 'material', b.item_code || '', b.item_name || '', b.qty_per || 1, b.unit || 'EA');
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
      await db.prepare(`UPDATE gift_sets SET ${sets.join(',')} WHERE id=?`).run(...vals);
    }
    if (Array.isArray(body.bom)) {
      await db.prepare('DELETE FROM gift_set_bom WHERE set_id=?').run(id);
      const ins = db.prepare('INSERT OR IGNORE INTO gift_set_bom (set_id, item_type, item_code, item_name, qty_per, unit) VALUES (?,?,?,?,?,?)');
      for (const b of body.bom) await ins.run(id, b.item_type || 'material', b.item_code || '', b.item_name || '', b.qty_per || 1, b.unit || 'EA');
    }
    ok(res, { updated: true });
    return;
  }

  // POST /api/gift-sets/:id/transaction — 입고/출고/기초재고/조정
  const gsTx = pathname.match(/^\/api\/gift-sets\/(\d+)\/transaction$/);
  if (gsTx && method === 'POST') {
    const id = parseInt(gsTx[1]);
    const body = await readJSON(req);
    const { tx_type, qty, operator, memo, expiry_date } = body;
    if (!['base', 'assembly', 'shipment', 'adjust'].includes(tx_type)) { fail(res, 400, '유효하지 않은 거래유형'); return; }
    const amount = parseInt(qty);
    if (!amount || amount <= 0) { fail(res, 400, '수량은 1 이상이어야 합니다'); return; }
    const gs = await db.prepare('SELECT * FROM gift_sets WHERE id=?').get(id);
    if (!gs) { fail(res, 404, '세트를 찾을 수 없습니다'); return; }

    // ── shipment(판매출고) 시 재고 부족 사전 검증 ──
    let bomItems = [];
    let bomShortage = [];
    if (tx_type === 'shipment') {
      // 1) 세트 자체 재고 부족 체크
      if (gs.current_stock < amount && !body.force) {
        fail(res, 400, `세트 재고 부족: ${gs.set_name || gs.set_code} (보유:${gs.current_stock}, 요청:${amount})`);
        return;
      }
      // 2) BOM 기반 원부재료 재고 부족 체크
      bomItems = await db.prepare('SELECT * FROM gift_set_bom WHERE set_id=?').all(id);
      if (bomItems.length > 0) {
        const defaultWh = await db.prepare("SELECT id FROM warehouses WHERE is_default=1 LIMIT 1").get();
        const whId = defaultWh?.id || 1;
        for (const bom of bomItems) {
          const needed = bom.qty_per * amount;
          const inv = await db.prepare('SELECT quantity FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?').get(whId, bom.item_code);
          const available = inv?.quantity || 0;
          if (available < needed) {
            bomShortage.push({ item_code: bom.item_code, item_name: bom.item_name, needed, available, short: needed - available });
          }
        }
        if (bomShortage.length > 0 && !body.force) {
          fail(res, 400, `원부재료 재고 부족: ${bomShortage.map(s => `${s.item_name||s.item_code} (필요:${s.needed}, 보유:${s.available}, 부족:${s.short})`).join(', ')}`);
          return;
        }
      }
    }

    const txRun = db.transaction(async () => {
      await db.prepare('INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo, expiry_date) VALUES (?,?,?,?,?,?)').run(id, tx_type, amount, operator || '', memo || '', expiry_date || '');
      // 소비기한 갱신: 가장 가까운 소비기한을 세트에 저장 (assembly 입고 시)
      if (expiry_date && (tx_type === 'assembly' || tx_type === 'base')) {
        const currentExpiry = gs.expiry_date || '';
        if (!currentExpiry || expiry_date < currentExpiry) {
          await db.prepare("UPDATE gift_sets SET expiry_date=? WHERE id=?").run(expiry_date, id);
        }
      }
      let newStock;
      if (tx_type === 'base') {
        newStock = amount;
        await db.prepare("UPDATE gift_sets SET current_stock=?, updated_at=datetime('now','localtime') WHERE id=?").run(amount, id);
        await db.prepare("UPDATE gift_sets SET base_stock=? WHERE id=?").run(amount, id);
      } else if (tx_type === 'assembly') {
        newStock = gs.current_stock + amount;
        await db.prepare('UPDATE gift_sets SET current_stock=current_stock+?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, id);
      } else if (tx_type === 'shipment') {
        newStock = gs.current_stock - amount;
        await db.prepare('UPDATE gift_sets SET current_stock=current_stock-?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, id);

        // ── BOM 역산: 원부재료 창고재고 자동차감 (백플러시) ──
        if (bomItems.length > 0) {
          const defaultWh = await db.prepare("SELECT id FROM warehouses WHERE is_default=1 LIMIT 1").get();
          const whId = defaultWh?.id || 1;
          for (const bom of bomItems) {
            const deductQty = bom.qty_per * amount;
            await db.prepare('UPDATE warehouse_inventory SET quantity=quantity-?, updated_at=datetime(\'now\',\'localtime\') WHERE warehouse_id=? AND product_code=?').run(deductQty, whId, bom.item_code);
          }
        }
      } else {
        newStock = gs.current_stock + amount;
        await db.prepare('UPDATE gift_sets SET current_stock=current_stock+?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, id);
      }
      return newStock;
    });
    const newStock = await txRun();
    // 응답에 BOM 차감 내역 포함
    const bomDeducted = (tx_type === 'shipment' && bomItems.length > 0) ? bomItems.map(b => ({ item_code: b.item_code, item_name: b.item_name, deducted: b.qty_per * amount })) : [];
    ok(res, { new_stock: newStock, bom_deducted: bomDeducted, shortage_warning: bomShortage });
    return;
  }

  // GET /api/gift-sets/:id/transactions — 이력 조회
  const gsTxList = pathname.match(/^\/api\/gift-sets\/(\d+)\/transactions$/);
  if (gsTxList && method === 'GET') {
    const id = parseInt(gsTxList[1]);
    const date = parsed.searchParams.get('date') || new Date().toLocaleDateString('en-CA');
    const limit = parseInt(parsed.searchParams.get('limit')) || 200;
    const rows = await db.prepare("SELECT * FROM gift_set_transactions WHERE set_id=? AND date(created_at)=? ORDER BY created_at DESC LIMIT ?").all(id, date, limit);
    ok(res, rows);
    return;
  }

  // POST /api/gift-sales/sync — 판매 동기화 (구글시트/외부 데이터 → BOM 역산 차감)
  // body: { sales: [{ set_code: 'SET-XXX', qty: 10, order_ref: '주문번호', memo: '' }, ...] }
  if (pathname === '/api/gift-sales/sync' && method === 'POST') {
    const body = await readJSON(req);
    const sales = body.sales || [];
    if (!sales.length) { fail(res, 400, 'sales 배열이 필요합니다'); return; }
    const defaultWh = await db.prepare("SELECT id FROM warehouses WHERE is_default=1 LIMIT 1").get();
    const whId = defaultWh?.id || 1;
    const results = [];
    for (const sale of sales) {
      const gs = await db.prepare('SELECT * FROM gift_sets WHERE set_code=?').get(sale.set_code);
      if (!gs) { results.push({ set_code: sale.set_code, status: 'error', message: '세트 미등록' }); continue; }
      const amount = parseInt(sale.qty) || 0;
      if (amount <= 0) { results.push({ set_code: sale.set_code, status: 'skip', message: '수량 0' }); continue; }
      // 세트 자체 재고 부족 체크
      if (gs.current_stock < amount && !body.force) {
        results.push({ set_code: sale.set_code, status: 'shortage', message: `세트 재고 부족 (보유:${gs.current_stock}, 요청:${amount})` });
        continue;
      }
      // BOM 조회 + 재고 부족 검증
      const bomItems = await db.prepare('SELECT * FROM gift_set_bom WHERE set_id=?').all(gs.id);
      const shortage = [];
      for (const bom of bomItems) {
        const needed = bom.qty_per * amount;
        const inv = await db.prepare('SELECT quantity FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?').get(whId, bom.item_code);
        const available = inv?.quantity || 0;
        if (available < needed) shortage.push({ item_code: bom.item_code, item_name: bom.item_name, needed, available });
      }
      if (shortage.length > 0 && !body.force) {
        results.push({ set_code: sale.set_code, status: 'shortage', shortage });
        continue;
      }
      // 트랜잭션: 세트 재고 차감 + BOM 원부재료 차감
      const tx = db.transaction(async () => {
        await db.prepare('INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo) VALUES (?,?,?,?,?)').run(
          gs.id, 'shipment', amount, body.operator || 'sync', sale.memo || `판매동기화: ${sale.order_ref || ''}`);
        await db.prepare('UPDATE gift_sets SET current_stock=current_stock-?, updated_at=datetime(\'now\',\'localtime\') WHERE id=?').run(amount, gs.id);
        const deducted = [];
        for (const bom of bomItems) {
          const deductQty = bom.qty_per * amount;
          await db.prepare('UPDATE warehouse_inventory SET quantity=quantity-?, updated_at=datetime(\'now\',\'localtime\') WHERE warehouse_id=? AND product_code=?').run(deductQty, whId, bom.item_code);
          deducted.push({ item_code: bom.item_code, item_name: bom.item_name, deducted: deductQty });
        }
        return { new_stock: gs.current_stock - amount, deducted };
      });
      const result = await tx();
      results.push({ set_code: sale.set_code, status: 'ok', qty: amount, ...result });
    }
    ok(res, { synced: results.filter(r => r.status === 'ok').length, total: sales.length, results });
    return;
  }

  // GET /api/gift-sets/production-capacity — 최대 생산가능수량
  if (pathname === '/api/gift-sets/production-capacity' && method === 'GET') {
    const sets = await db.prepare("SELECT * FROM gift_sets WHERE status='active' ORDER BY set_name").all();
    const bomStmt = db.prepare('SELECT * FROM gift_set_bom WHERE set_id=?');
    const accStmt = db.prepare("SELECT current_stock FROM accessories WHERE acc_code=? OR acc_name=? LIMIT 1");
    const xerpProducts = (xerpInventoryCache && xerpInventoryCache.products) ? xerpInventoryCache.products : [];
    const result = [];
    for (const s of sets) {
      const bomItems = await bomStmt.all(s.id);
      let maxProduction = Infinity;
      let bottleneck = null;
      const components = [];
      for (const b of bomItems) {
        let available = 0;
        if (b.item_type === 'material') {
          const xp = xerpProducts.find(p => (p['제품코드'] || '') === b.item_code);
          available = xp ? (xp['가용재고'] || 0) : 0;
        } else {
          const acc = await accStmt.get(b.item_code, b.item_name);
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
  //  더기프트 출고 스케줄 API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/gift-shipment-schedule — 출고 스케줄 목록 (기간/상태 필터)
  if (pathname === '/api/gift-shipment-schedule' && method === 'GET') {
    const from = parsed.searchParams.get('from') || new Date().toISOString().slice(0,10);
    const to = parsed.searchParams.get('to') || '';
    const status = parsed.searchParams.get('status') || '';
    const setId = parsed.searchParams.get('set_id') || '';
    let q = 'SELECT * FROM gift_shipment_schedule WHERE ship_date >= ?';
    const p = [from];
    if (to) { q += ' AND ship_date <= ?'; p.push(to); }
    if (status) { q += ' AND status = ?'; p.push(status); }
    if (setId) { q += ' AND set_id = ?'; p.push(setId); }
    q += ' ORDER BY ship_date ASC, id ASC';
    const rows = await db.prepare(q).all(...p);
    ok(res, rows); return;
  }

  // POST /api/gift-shipment-schedule — 출고 스케줄 등록
  if (pathname === '/api/gift-shipment-schedule' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.set_id || !body.ship_date || !body.planned_qty) { fail(res, 400, 'set_id, ship_date, planned_qty required'); return; }
    const gs = await db.prepare('SELECT * FROM gift_sets WHERE id=?').get(body.set_id);
    const info = await db.prepare("INSERT INTO gift_shipment_schedule (set_id, set_code, set_name, ship_date, planned_qty, order_ref, recipient, address, notes) VALUES (?,?,?,?,?,?,?,?,?)")
      .run(body.set_id, gs?.set_code || '', gs?.set_name || '', body.ship_date, body.planned_qty, body.order_ref || '', body.recipient || '', body.address || '', body.notes || '');
    ok(res, { id: info.lastInsertRowid }); return;
  }

  // POST /api/gift-shipment-schedule/:id/ship — 출고 처리 (planned → shipped)
  const gssShipMatch = pathname.match(/^\/api\/gift-shipment-schedule\/(\d+)\/ship$/);
  if (gssShipMatch && method === 'POST') {
    const id = gssShipMatch[1];
    const body = await readJSON(req);
    const sched = await db.prepare('SELECT * FROM gift_shipment_schedule WHERE id=?').get(id);
    if (!sched) { fail(res, 404, 'Schedule not found'); return; }
    const shippedQty = body.shipped_qty || sched.planned_qty;
    await db.prepare("UPDATE gift_shipment_schedule SET status='shipped', shipped_qty=?, updated_at=datetime('now','localtime') WHERE id=?").run(shippedQty, id);
    // gift_set_transactions에 출고 기록
    await db.prepare("INSERT INTO gift_set_transactions (set_id, tx_type, qty, operator, memo) VALUES (?,?,?,?,?)").run(
      sched.set_id, 'shipment', shippedQty, body.operator || '', `출고: ${sched.recipient || ''} ${sched.order_ref || ''}`);
    ok(res, { shipped: true, qty: shippedQty }); return;
  }

  // DELETE /api/gift-shipment-schedule/:id — 출고 스케줄 삭제 (planned만)
  const gssDelMatch = pathname.match(/^\/api\/gift-shipment-schedule\/(\d+)$/);
  if (gssDelMatch && method === 'DELETE') {
    const id = gssDelMatch[1];
    await db.prepare("UPDATE gift_shipment_schedule SET status='cancelled', updated_at=datetime('now','localtime') WHERE id=? AND status='planned'").run(id);
    ok(res, { cancelled: true }); return;
  }

  // GET /api/gift-production-summary — 생산재고 종합 대시보드
  if (pathname === '/api/gift-production-summary' && method === 'GET') {
    const sets = await db.prepare("SELECT * FROM gift_sets WHERE status='active' ORDER BY set_name").all();
    const result = [];
    for (const s of sets) {
      const totalAssembly = (await db.prepare("SELECT COALESCE(SUM(qty),0) as t FROM gift_set_transactions WHERE set_id=? AND tx_type='assembly'").get(s.id)).t;
      const totalShipment = (await db.prepare("SELECT COALESCE(SUM(qty),0) as t FROM gift_set_transactions WHERE set_id=? AND tx_type='shipment'").get(s.id)).t;
      const todayShipped = (await db.prepare("SELECT COALESCE(SUM(shipped_qty),0) as t FROM gift_shipment_schedule WHERE set_id=? AND ship_date=date('now','localtime') AND status='shipped'").get(s.id)).t;
      const todayAssembly = (await db.prepare("SELECT COALESCE(SUM(qty),0) as t FROM gift_set_transactions WHERE set_id=? AND tx_type='assembly' AND date(created_at)=date('now','localtime')").get(s.id)).t;
      const upcoming = await db.prepare("SELECT ship_date, SUM(planned_qty) as qty FROM gift_shipment_schedule WHERE set_id=? AND status='planned' AND ship_date >= date('now','localtime') AND ship_date <= date('now','localtime','+7 days') GROUP BY ship_date ORDER BY ship_date").all(s.id);
      const upcomingTotal = upcoming.reduce((sum, d) => sum + d.qty, 0);
      const productionStock = s.base_stock + totalAssembly - totalShipment; // 기초 + 생산 - 출고
      result.push({
        id: s.id, set_code: s.set_code, set_name: s.set_name,
        base_stock: s.base_stock,
        production_stock: productionStock,
        today_assembled: todayAssembly,
        today_shipped: todayShipped,
        upcoming_days: upcoming,
        upcoming_total: upcomingTotal,
        remaining_stock: productionStock - todayShipped,
        available_stock: productionStock - todayShipped - upcomingTotal,
        need_to_produce: Math.max(0, upcomingTotal - (productionStock - todayShipped))
      });
    }
    ok(res, result); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  다중 창고 관리 API
  // ════════════════════════════════════════════════════════════════════

  // GET /api/warehouses — 창고 목록
  if (pathname === '/api/warehouses' && method === 'GET') {
    const rows = await db.prepare("SELECT * FROM warehouses ORDER BY is_default DESC, id ASC").all();
    ok(res, rows);
    return;
  }

  // POST /api/warehouses — 창고 등록
  if (pathname === '/api/warehouses' && method === 'POST') {
    const body = await readJSON(req);
    const { code, name, location, description } = body;
    if (!code || !name) { fail(res, 400, '창고코드와 이름은 필수입니다'); return; }
    try {
      await db.prepare("INSERT INTO warehouses (code, name, location, description) VALUES (?, ?, ?, ?)").run(code, name, location || '', description || '');
      ok(res, { message: '창고 등록 완료' });
    } catch (e) {
      if (e.message.includes('UNIQUE') || e.message.includes('duplicate key') || e.message.includes('unique constraint')) fail(res, 409, '이미 존재하는 창고코드입니다');
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
    await db.prepare(`UPDATE warehouses SET ${fields.join(', ')} WHERE id=?`).run(...vals);
    ok(res, { message: '창고 수정 완료' });
    return;
  }

  // DELETE /api/warehouses/:id — 창고 삭제
  const whDelMatch = pathname.match(/^\/api\/warehouses\/(\d+)$/);
  if (whDelMatch && method === 'DELETE') {
    const whId = parseInt(whDelMatch[1]);
    const wh = await db.prepare("SELECT * FROM warehouses WHERE id=?").get(whId);
    if (!wh) { fail(res, 404, '창고를 찾을 수 없습니다'); return; }
    if (wh.is_default) { fail(res, 400, '기본 창고는 삭제할 수 없습니다'); return; }
    const invCount = await db.prepare("SELECT COUNT(*) as cnt FROM warehouse_inventory WHERE warehouse_id=? AND quantity>0").get(whId);
    if (invCount.cnt > 0) { fail(res, 400, '재고가 남아있는 창고는 삭제할 수 없습니다. 먼저 재고를 이동해주세요.'); return; }
    await db.prepare("DELETE FROM warehouse_inventory WHERE warehouse_id=?").run(whId);
    await db.prepare("DELETE FROM warehouses WHERE id=?").run(whId);
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
      rows = await db.prepare(sql).all(...args);
    } else {
      // 전체: 품목별 합산 + 창고별 내역
      let sql = `SELECT wi.product_code, wi.product_name,
        SUM(wi.quantity) as total_qty,
        GROUP_CONCAT(w.name || ':' || wi.quantity, ' | ') as breakdown
        FROM warehouse_inventory wi JOIN warehouses w ON wi.warehouse_id=w.id`;
      const args = [];
      if (search) { sql += " WHERE wi.product_code LIKE ? OR wi.product_name LIKE ?"; args.push(`%${search}%`, `%${search}%`); }
      sql += " GROUP BY wi.product_code, wi.product_name ORDER BY wi.product_code";
      rows = await db.prepare(sql).all(...args);
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
    const existing = await db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(warehouse_id, product_code);
    if (existing) {
      await db.prepare("UPDATE warehouse_inventory SET quantity=?, product_name=?, updated_at=datetime('now','localtime') WHERE id=?").run(qty, product_name || existing.product_name, existing.id);
    } else {
      await db.prepare("INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity) VALUES (?, ?, ?, ?)").run(warehouse_id, product_code, product_name || '', qty);
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
    const tx = db.transaction(async (list) => {
      let cnt = 0;
      for (const it of list) {
        await upsert.run(warehouse_id, it.product_code, it.product_name || '', parseInt(it.quantity) || 0);
        cnt++;
      }
      return cnt;
    });
    const count = await tx(items);
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
    const fromInv = await db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(from_warehouse, product_code);
    if (!fromInv || fromInv.quantity < qty) {
      fail(res, 400, `출발 창고 재고 부족 (현재: ${fromInv ? fromInv.quantity : 0})`); return;
    }

    // 창고명 조회
    const fromWh = await db.prepare("SELECT name FROM warehouses WHERE id=?").get(from_warehouse);
    const toWh = await db.prepare("SELECT name FROM warehouses WHERE id=?").get(to_warehouse);
    const now = new Date().toISOString().slice(0, 10);
    const autoMemo = memo || `${now} ${qty}개 ${fromWh ? fromWh.name : ''}→${toWh ? toWh.name : ''}`;

    const tx = db.transaction(async () => {
      // 출발 창고 차감 + 메모 업데이트
      const fromMemo = `${now} ${qty}개 출고→${toWh ? toWh.name : ''}`;
      await db.prepare("UPDATE warehouse_inventory SET quantity=quantity-?, memo=?, updated_at=datetime('now','localtime') WHERE warehouse_id=? AND product_code=?").run(qty, fromMemo, from_warehouse, product_code);
      // 도착 창고 추가 + 메모 업데이트
      const toMemo = `${now} ${qty}개 입고←${fromWh ? fromWh.name : ''}`;
      const toInv = await db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(to_warehouse, product_code);
      if (toInv) {
        await db.prepare("UPDATE warehouse_inventory SET quantity=quantity+?, memo=?, updated_at=datetime('now','localtime') WHERE warehouse_id=? AND product_code=?").run(qty, toMemo, to_warehouse, product_code);
      } else {
        await db.prepare("INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity, memo) VALUES (?, ?, ?, ?, ?)").run(to_warehouse, product_code, product_name || fromInv.product_name || '', qty, toMemo);
      }
      // 이력 기록
      await db.prepare("INSERT INTO warehouse_transfers (from_warehouse, to_warehouse, product_code, product_name, quantity, operator, memo) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .run(from_warehouse, to_warehouse, product_code, product_name || fromInv.product_name || '', qty, operator || '', autoMemo);
    });
    await tx();
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
    else { where.push(`t.created_at >= (NOW() - INTERVAL '${days} days')::text`); }
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
    const rows = await db.prepare(sql).all(...args);
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
      dateFilter = `t.created_at >= (NOW() - INTERVAL '${days} days')::text`;
    }

    // 총 건수/수량
    const total = await db.prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(quantity),0) as total_qty FROM warehouse_transfers t WHERE ${dateFilter}`).get(...args);
    // 오늘 건수
    const today = await db.prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(quantity),0) as total_qty FROM warehouse_transfers t WHERE t.created_at::date=CURRENT_DATE`).get();
    // 최다 이동 품목
    const topItem = await db.prepare(`SELECT product_code, MAX(product_name) as product_name, SUM(quantity) as total_qty, COUNT(*) as cnt FROM warehouse_transfers t WHERE ${dateFilter} GROUP BY product_code ORDER BY total_qty DESC LIMIT 1`).get(...args);
    // 창고 간 흐름 TOP5
    const flows = await db.prepare(`SELECT fw.name as from_name, tw.name as to_name, COUNT(*) as cnt, SUM(t.quantity) as total_qty
      FROM warehouse_transfers t
      JOIN warehouses fw ON t.from_warehouse=fw.id
      JOIN warehouses tw ON t.to_warehouse=tw.id
      WHERE ${dateFilter}
      GROUP BY t.from_warehouse, t.to_warehouse, fw.name, tw.name
      ORDER BY total_qty DESC LIMIT 5`).all(...args);
    // 담당자별
    const operators = await db.prepare(`SELECT operator, COUNT(*) as cnt, SUM(quantity) as total_qty FROM warehouse_transfers t WHERE ${dateFilter} AND operator!='' GROUP BY operator ORDER BY cnt DESC LIMIT 5`).all(...args);

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
    const existing = await db.prepare("SELECT * FROM warehouse_inventory WHERE warehouse_id=? AND product_code=?").get(warehouse_id, product_code);
    const beforeQty = existing ? existing.quantity : 0;
    const diff = newQty - beforeQty;
    const adjType = diff > 0 ? 'increase' : diff < 0 ? 'decrease' : 'no_change';

    const tx = db.transaction(async () => {
      if (existing) {
        await db.prepare("UPDATE warehouse_inventory SET quantity=?, updated_at=datetime('now','localtime') WHERE id=?").run(newQty, existing.id);
      } else {
        await db.prepare("INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity) VALUES (?, ?, ?, ?)").run(warehouse_id, product_code, product_name || '', newQty);
      }
      await db.prepare("INSERT INTO warehouse_adjustments (warehouse_id, product_code, product_name, adj_type, before_qty, after_qty, diff_qty, reason, operator) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .run(warehouse_id, product_code, product_name || (existing ? existing.product_name : ''), adjType, beforeQty, newQty, diff, reason || '', operator || '');
    });
    await tx();
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
    else { where.push(`a.created_at >= (NOW() - INTERVAL '${days} days')::text`); }
    if (to_date) { where.push("a.created_at <= ?"); args.push(to_date + ' 23:59:59'); }
    if (wh) { where.push("a.warehouse_id = ?"); args.push(parseInt(wh)); }
    if (search) { where.push("(a.product_code LIKE ? OR a.product_name LIKE ?)"); args.push(`%${search}%`, `%${search}%`); }

    const rows = await db.prepare(`SELECT a.*, w.name as warehouse_name
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
        const prods = await db.prepare("SELECT product_code, product_name FROM products").all();
        for (const p of prods) localProducts[p.product_code] = p.product_name;
      } catch(e) {}
      const defaultWh = await db.prepare("SELECT id FROM warehouses WHERE is_default=1 LIMIT 1").get();
      if (!defaultWh) { fail(res, 500, '기본 창고가 설정되지 않았습니다'); return; }

      const upsert = db.prepare(`INSERT INTO warehouse_inventory (warehouse_id, product_code, product_name, quantity)
        VALUES (?, ?, ?, ?) ON CONFLICT(warehouse_id, product_code) DO UPDATE SET quantity=excluded.quantity, product_name=excluded.product_name, updated_at=datetime('now','localtime')`);
      const tx = db.transaction(async (rows) => {
        let cnt = 0;
        for (const r of rows) {
          await upsert.run(defaultWh.id, r.product_code, localProducts[r.product_code] || r.product_code, parseInt(r.quantity) || 0);
          cnt++;
        }
        return cnt;
      });
      const count = await tx(result.recordset);
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
      pool = new sql.ConnectionPool(barShopConfig);
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
    const giftSets = await db.prepare("SELECT xerp_code, set_name FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
    if (!giftSets.length) return { order_count: 0, total_sales: 0, total_qty: 0, items: 0 };
    const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
    if (!xerpCodes.length) return { order_count: 0, total_sales: 0, total_qty: 0, items: 0 };
    const req = pool.request();
    req.input('startDate', sql.NVarChar(16), startYMD);
    req.input('endDate', sql.NVarChar(16), endYMD);
    const placeholders = (await Promise.all(xerpCodes.map(async (c, i) => { req.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }))).join(',');
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
    const giftSets = await db.prepare("SELECT xerp_code FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
    const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
    if (!xerpCodes.length) return [];
    const req = pool.request();
    req.input('startDate', sql.NVarChar(16), startYMD);
    req.input('endDate', sql.NVarChar(16), endYMD);
    const placeholders = (await Promise.all(xerpCodes.map(async (c, i) => { req.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }))).join(',');
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
    const giftSets = await db.prepare("SELECT xerp_code, set_name FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
    const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
    if (!xerpCodes.length) return [];
    const codeNameMap = {};
    giftSets.forEach(g => { codeNameMap[g.xerp_code.trim()] = g.set_name; });
    const req = pool.request();
    req.input('startDate', sql.NVarChar(16), startYMD);
    req.input('endDate', sql.NVarChar(16), endYMD);
    const placeholders = (await Promise.all(xerpCodes.map(async (c, i) => { req.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }))).join(',');
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

    // 법인별 분리 합산 (바른손 = XERP + 더기프트, 디얼디어 = DD)
    result.today.barunson = { sales: (result.today.xerp.sales||0) + (result.today.gift.sales||0), orders: (result.today.xerp.orders||0) + (result.today.gift.orders||0) };
    result.today.deardear = { sales: result.today.dd.sales||0, orders: result.today.dd.orders||0 };
    result.thisMonth.barunson = { sales: (result.thisMonth.xerp.sales||0) + (result.thisMonth.gift.sales||0), orders: (result.thisMonth.xerp.orders||0) + (result.thisMonth.gift.orders||0) };
    result.thisMonth.deardear = { sales: result.thisMonth.dd.sales||0, orders: result.thisMonth.dd.orders||0 };
    result.lastMonth.barunson = { sales: (result.lastMonth.xerp.sales||0) + (result.lastMonth.gift.sales||0), orders: (result.lastMonth.xerp.orders||0) + (result.lastMonth.gift.orders||0) };
    result.lastMonth.deardear = { sales: result.lastMonth.dd.sales||0, orders: result.lastMonth.dd.orders||0 };
    // 그룹 합계 (참고용 — UI에서는 법인별 분리 표시)
    result.today.total = { sales: result.today.barunson.sales + result.today.deardear.sales, orders: result.today.barunson.orders + result.today.deardear.orders };
    result.thisMonth.total = { sales: result.thisMonth.barunson.sales + result.thisMonth.deardear.sales, orders: result.thisMonth.barunson.orders + result.thisMonth.deardear.orders };
    result.lastMonth.total = { sales: result.lastMonth.barunson.sales + result.lastMonth.deardear.sales, orders: result.lastMonth.barunson.orders + result.lastMonth.deardear.orders };
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
        const giftSets = await db.prepare("SELECT xerp_code FROM gift_sets WHERE status='active' AND xerp_code != ''").all();
        const xerpCodes = giftSets.map(g => g.xerp_code.trim()).filter(Boolean);
        if (xerpCodes.length) {
          const req2 = pool.request();
          req2.input('s', sql.NVarChar(16), startYMD);
          req2.input('e', sql.NVarChar(16), endYMD);
          const ph = (await Promise.all(xerpCodes.map(async (c, i) => { req2.input(`gc${i}`, sql.VarChar(50), c); return `@gc${i}`; }))).join(',');
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
    const d1 = await db.prepare("DELETE FROM sales_daily_cache WHERE sale_date >= date('now', '-7 days')").run();
    const d2 = await db.prepare("DELETE FROM sales_monthly_cache WHERE sale_month >= TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYY-MM')").run();
    const d3 = await db.prepare("DELETE FROM sales_product_cache WHERE sale_month >= TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYY-MM')").run();
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
      const ts = await db.prepare("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status").all();
      ts.forEach(async t => { taskStats.total += t.cnt; if(t.status==='done') taskStats.done=t.cnt; if(t.status==='in_progress') taskStats.in_progress=t.cnt; });
      const dc = await db.prepare("SELECT COUNT(*) as cnt FROM defects WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10));
      defectCount = dc ? dc.cnt : 0;
      try { const pp = await db.prepare("SELECT COALESCE(SUM(amount),0) as total FROM post_process_history WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10)); postProcessTotal = pp ? pp.total : 0; } catch(e){}
    } catch(e) {}

    // PO 현황
    let poStats = { total: 0, pending: 0 };
    try {
      const pc = await db.prepare("SELECT COUNT(*) as cnt FROM purchase_orders WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10));
      poStats.total = pc ? pc.cnt : 0;
      const pp = await db.prepare("SELECT COUNT(*) as cnt FROM purchase_orders WHERE status IN ('pending','ordered','partial')").get();
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
      const ppRow = await db.prepare(`SELECT COALESCE(SUM(amount),0) AS total FROM post_process_history
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
          const placeholders = (await Promise.all(uniqueCodes.map(async (c, i) => { req.input(`c${i}`, sql.VarChar(30), c); return `@c${i}`; }))).join(',');
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
      const ppRow = await db.prepare(ppSql).get(...ppParams);
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
    const total = (await db.prepare(`SELECT COUNT(*) as cnt FROM notices WHERE ${where}`).get(...params)).cnt;
    const rows = await db.prepare(`SELECT * FROM notices WHERE ${where} ORDER BY is_pinned DESC, created_at DESC LIMIT ? OFFSET ?`)
      .all(...params, limit, offset);
    // 읽음 여부 추가
    const reads = await db.prepare(`SELECT notice_id FROM notice_reads WHERE user_id = ?`).all(decoded.userId);
    const readSet = new Set(reads.map(r => r.notice_id));
    rows.forEach(r => { r.is_read = readSet.has(r.id) ? 1 : 0; });
    ok(res, { notices: rows, total, page, limit, totalPages: Math.ceil(total / limit) }); return;
  }

  // ── GET /api/notices/popup ── 활성 팝업 공지 (로그인 시 표시)
  if (pathname === '/api/notices/popup' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const popups = await db.prepare(`SELECT n.* FROM notices n
      WHERE n.status = 'active' AND n.is_popup = 1
        AND (n.popup_start IS NULL OR n.popup_start <= ?)
        AND (n.popup_end IS NULL OR n.popup_end >= ?)
      ORDER BY n.created_at DESC`).all(now, now);
    // 사용자가 이미 닫은 팝업 제외
    const dismissed = await db.prepare(`SELECT notice_id FROM notice_reads WHERE user_id = ? AND popup_dismissed = 1`).all(decoded.userId);
    const dismissedSet = new Set(dismissed.map(r => r.notice_id));
    const active = popups.filter(p => !dismissedSet.has(p.id));
    ok(res, { popups: active }); return;
  }

  // ── POST /api/notices/popup/:id/dismiss ── 팝업 닫기 (오늘 하루 안보기)
  if (pathname.match(/^\/api\/notices\/popup\/(\d+)\/dismiss$/) && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const noticeId = parseInt(pathname.match(/\/(\d+)\/dismiss/)[1], 10);
    await db.prepare(`INSERT INTO notice_reads (notice_id, user_id, popup_dismissed) VALUES (?, ?, 1)
      ON CONFLICT(notice_id, user_id) DO UPDATE SET popup_dismissed = 1, read_at = datetime('now','localtime')`)
      .run(noticeId, decoded.userId);
    ok(res, { message: '팝업 닫기 완료' }); return;
  }

  // ── GET /api/notices/:id ── 공지 상세 + 조회수 증가
  if (pathname.match(/^\/api\/notices\/(\d+)$/) && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const notice = await db.prepare("SELECT * FROM notices WHERE id = ?").get(id);
    if (!notice) { fail(res, 404, '공지를 찾을 수 없습니다'); return; }
    // 조회수 증가
    await db.prepare("UPDATE notices SET view_count = view_count + 1 WHERE id = ?").run(id);
    notice.view_count += 1;
    // 읽음 처리
    await db.prepare(`INSERT INTO notice_reads (notice_id, user_id) VALUES (?, ?)
      ON CONFLICT(notice_id, user_id) DO UPDATE SET read_at = datetime('now','localtime')`)
      .run(id, decoded.userId);
    ok(res, notice); return;
  }

  // ── POST /api/notices/release ── 릴리스 노트 자동 게시 (admin만)
  if (pathname === '/api/notices/release' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한 필요'); return; }
    const body = await readJSON(req);
    const { version, title, content } = body;
    // content가 없으면 whats-new 디렉토리에서 최신 파일을 읽어서 사용
    let noteContent = content || '';
    if (!noteContent) {
      try {
        const wnDir = path.join(__dir, 'whats-new');
        if (fs.existsSync(wnDir)) {
          const files = fs.readdirSync(wnDir).filter(f => f.startsWith('WHATS-NEW-')).sort().reverse();
          if (files.length > 0) noteContent = fs.readFileSync(path.join(wnDir, files[0]), 'utf8');
        }
      } catch (_) {}
    }
    const noteTitle = title || `🔄 시스템 업데이트 ${version || ''}`.trim();
    const noticeId = await postSystemNotice(noteTitle, noteContent, { category: 'update', is_pinned: 1 });
    auditLog(decoded.userId, decoded.username, 'release_notice', 'notices', noticeId, `릴리스: ${noteTitle}`, clientIP);
    ok(res, { id: noticeId, message: '릴리스 공지 등록 완료' }); return;
  }

  // ── POST /api/notices ── 공지 작성 (admin만)
  if (pathname === '/api/notices' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'admin') { fail(res, 403, '관리자 권한 필요'); return; }
    const body = await readJSON(req);
    const { title, content, category, is_popup, popup_start, popup_end, is_pinned } = body;
    if (!title) { fail(res, 400, '제목 필수'); return; }
    const r = await db.prepare(`INSERT INTO notices (title, content, category, is_popup, popup_start, popup_end, is_pinned, author_id, author_name)
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
    await db.prepare(`UPDATE notices SET title=COALESCE(?,title), content=COALESCE(?,content),
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
    await db.prepare("UPDATE notices SET status = 'deleted', updated_at = datetime('now','localtime') WHERE id = ?").run(id);
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
      const txn = db.transaction(async (items) => {
        for (const r of items) {
          const info = await stmt.run(
            (r.invoice_no||'').trim(), (r.invoice_date||'').replace(/-/g,'').trim(),
            (r.ar_ap||'AP').trim(), (r.cs_name||'').trim(), (r.cs_reg_no||'').replace(/-/g,'').trim(),
            parseFloat(r.supply_amt)||0, parseFloat(r.vat_amt)||0, parseFloat(r.total_amt)||0,
            (r.item_name||'').trim(), (r.remark||'').trim(),
            (r.electronic||'Y').trim(), decoded.name || decoded.email || ''
          );
          if (info.changes > 0) inserted++; else skipped++;
        }
      });
      await txn(rows);
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
    const totalCount = (await db.prepare('SELECT COUNT(*) AS cnt FROM hometax_invoices WHERE ' + where).get(...params)).cnt;
    const invoices = await db.prepare('SELECT * FROM hometax_invoices WHERE ' + where + ' ORDER BY invoice_date DESC, id DESC LIMIT ? OFFSET ?').all(...params, limit, offset);
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
    const info = await db.prepare('DELETE FROM hometax_invoices WHERE invoice_date >= ? AND invoice_date <= ?').run(from, to);
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
      const txn = db.transaction(async (items) => {
        for (const r of items) {
          await stmt.run(
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
      await txn(rows);
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
    const items = await db.prepare('SELECT * FROM material_prices WHERE ' + where + ' ORDER BY vendor_name, product_code, apply_month DESC').all(...params);
    // 제지사 목록
    const vendors = (await db.prepare('SELECT DISTINCT vendor_name FROM material_prices ORDER BY vendor_name').all()).map(r => r.vendor_name);
    // 적용월 목록
    const months = (await db.prepare('SELECT DISTINCT apply_month FROM material_prices ORDER BY apply_month DESC').all()).map(r => r.apply_month);
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
    if (vendor) { where += ' AND m.vendor_name = ?'; params.push(vendor); }
    if (search) { where += ' AND (m.product_code LIKE ? OR m.product_name LIKE ?)'; params.push('%'+search+'%','%'+search+'%'); }
    const items = await db.prepare(`SELECT m.* FROM material_prices m
      INNER JOIN (SELECT product_code, vendor_name, MAX(apply_month) AS max_month FROM material_prices GROUP BY product_code, vendor_name) g
      ON m.product_code=g.product_code AND m.vendor_name=g.vendor_name AND m.apply_month=g.max_month
      WHERE ${where} ORDER BY m.vendor_name, m.product_code`).all(...params);

    // XERP MI(매입입고) 실가격 병합 — material_prices에 없는 BP코드도 표시
    let xerpMerged = false;
    try {
      const pool = await ensureXerpPool();
      if (pool) {
        const now = new Date();
        const s6m = new Date(now); s6m.setMonth(s6m.getMonth() - 6);
        const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
        const xReq = pool.request().input('from6m', fmt(s6m)).input('toNow', fmt(now));
        let xSearch = '';
        if (search) { xSearch = " AND (RTRIM(i.ItemCode) LIKE @xs)"; xReq.input('xs', '%'+search+'%'); }
        const xr = await xReq.query(`
          SELECT RTRIM(i.ItemCode) AS item_code,
                 COUNT(*) AS txn_count,
                 SUM(i.InoutQty) AS total_qty,
                 SUM(i.InoutAmnt) AS total_amt,
                 CASE WHEN SUM(i.InoutQty)>0 THEN SUM(i.InoutAmnt)/SUM(i.InoutQty) ELSE 0 END AS avg_price,
                 MAX(i.InoutPrice) AS last_price,
                 MAX(h.InoutDate) AS last_date
          FROM mmInoutItem i WITH(NOLOCK)
          JOIN mmInoutHeader h WITH(NOLOCK) ON i.SiteCode=h.SiteCode AND i.InoutNo=h.InoutNo AND i.InoutGubun=h.InoutGubun
          WHERE h.SiteCode='BK10' AND h.InoutGubun='MI'
            AND h.InoutDate>=@from6m AND h.InoutDate<=@toNow
            AND RTRIM(i.ItemCode) LIKE 'BP%' ${xSearch}
          GROUP BY RTRIM(i.ItemCode)
          ORDER BY RTRIM(i.ItemCode)
        `);
        // 품목명 매핑 (product_info에서 원자재코드→원재료용지명)
        const pi = getProductInfo();
        const nameMap = {};
        for (const [, info] of Object.entries(pi || {})) {
          const mc = (info['원자재코드']||'').trim();
          const mn = (info['원재료용지명']||'').trim();
          const maker = (info['제지사']||'').trim();
          if (mc && mn) nameMap[mc] = { name: mn, vendor: maker };
        }
        const existingCodes = new Set(items.map(it => it.product_code));
        for (const xr2 of (xr.recordset || [])) {
          const code = xr2.item_code;
          const avg = Math.round(xr2.avg_price || 0);
          const info = nameMap[code] || {};
          // 기존 항목에 XERP 실매입가 추가
          const existing = items.find(it => it.product_code === code);
          if (existing) {
            existing.xerp_price = avg;
            existing.xerp_qty = xr2.total_qty;
            existing.xerp_last_date = xr2.last_date;
          } else if (!vendor || (info.vendor || '').includes(vendor)) {
            // material_prices에 없는 BP코드 — XERP 데이터로 추가
            items.push({
              product_code: code,
              product_name: info.name || code,
              spec: '',
              unit: 'R',
              vendor_name: info.vendor || '(XERP)',
              list_price: 0,
              apply_price: avg,
              discount_rate: 0,
              apply_month: xr2.last_date ? xr2.last_date.slice(0,6) : '',
              xerp_price: avg,
              xerp_qty: xr2.total_qty,
              xerp_last_date: xr2.last_date,
              source: 'xerp'
            });
          }
        }
        items.sort((a,b) => (a.vendor_name||'').localeCompare(b.vendor_name||'') || (a.product_code||'').localeCompare(b.product_code||''));
        xerpMerged = true;
      }
    } catch(e) { console.warn('material-price/latest XERP 병합 실패:', e.message); }

    const vendors = [...new Set(items.map(it => it.vendor_name).filter(Boolean))].sort();
    ok(res, { items, vendors, count: items.length, xerp_merged: xerpMerged }); return;
  }

  // ── GET /api/material-price/trend ── 품목별 단가추이 (월별)
  if (pathname === '/api/material-price/trend' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    try {
      // 모든 적용월 가져오기
      const monthRows = await db.prepare('SELECT DISTINCT apply_month FROM material_prices WHERE apply_month != \'\' ORDER BY apply_month').all();
      const months = monthRows.map(r => r.apply_month);

      // 품목별+제지사별 전체 이력
      const allRows = await db.prepare(`
        SELECT product_code, product_name, vendor_name, apply_month, apply_price, list_price
        FROM material_prices WHERE apply_month != '' ORDER BY product_code, vendor_name, apply_month
      `).all();

      // 그룹핑: product_code + vendor_name 조합별
      const groups = {};
      for (const r of allRows) {
        const key = r.product_code + '||' + r.vendor_name;
        if (!groups[key]) groups[key] = { product_code: r.product_code, product_name: r.product_name, vendor_name: r.vendor_name, monthly_prices: {} };
        groups[key].monthly_prices[r.apply_month] = r.apply_price || r.list_price || 0;
        if (r.product_name) groups[key].product_name = r.product_name;
      }

      // 변동 계산 (최초 vs 최종)
      const items = Object.values(groups).map(g => {
        const validMonths = months.filter(m => g.monthly_prices[m] && g.monthly_prices[m] > 0);
        let change = null, change_pct = null;
        if (validMonths.length >= 2) {
          const first = g.monthly_prices[validMonths[0]];
          const last = g.monthly_prices[validMonths[validMonths.length - 1]];
          change = Math.round(last - first);
          change_pct = first > 0 ? Math.round((last - first) / first * 1000) / 10 : null;
        }
        return { ...g, change, change_pct };
      });

      ok(res, { months, items, count: items.length });
    } catch (e) {
      console.error('material-price/trend 오류:', e.message);
      fail(res, 500, e.message);
    }
    return;
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
    const allPrices = await db.prepare(`SELECT m.* FROM material_prices m
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

  // ── GET /api/material-price/xerp-trend ── XERP 실매입 단가 월별 추이 (거래처별/품목별)
  if (pathname === '/api/material-price/xerp-trend' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const months = parseInt(qs.get('months') || '12');
    const vendorFilter = qs.get('vendor') || '';
    const search = qs.get('search') || '';
    // 거래처 코드→이름 매핑 (기본 + 동적 확장)
    const vendorCodeMap = {'2015259':'대한통상','2100005':'두성종이','2100013':'삼원특수지상사','2100006':'서경','2013391':'한솔PNS'};
    const vendorCodes = Object.keys(vendorCodeMap);
    // 동적으로 poOrderHeader에서 신규 거래처 코드 수집
    try {
      const dynVendors = await pool.request().query("SELECT DISTINCT RTRIM(CsCode) AS code FROM poOrderHeader WHERE SiteCode='BK10' AND RTRIM(CsCode) NOT IN ('2015259','2100005','2100013','2100006','2013391')");
      for (const dv of (dynVendors.recordset||[])) { if (dv.code && !vendorCodeMap[dv.code]) { vendorCodeMap[dv.code] = dv.code; vendorCodes.push(dv.code); } }
    } catch(_) {}
    try {
      const pool = await ensureXerpPool();
      const now = new Date();
      const startDate = new Date(now.getFullYear(), now.getMonth() - months + 1, 1);
      const from = startDate.getFullYear() + String(startDate.getMonth()+1).padStart(2,'0') + '01';
      const to = now.getFullYear() + String(now.getMonth()+1).padStart(2,'0') + String(now.getDate()).padStart(2,'0');

      const req2 = pool.request().input('from', from).input('to', to);
      let extraWhere = '';
      if (vendorFilter) {
        // 거래처명으로 코드 역매핑
        const matchCode = Object.entries(vendorCodeMap).find(([c,n]) => n.includes(vendorFilter));
        if (matchCode) { extraWhere += " AND RTRIM(h.CsCode) = @vendorCode"; req2.input('vendorCode', matchCode[0]); }
      }
      if (search) { extraWhere += " AND (RTRIM(i.ItemCode) LIKE @search OR RTRIM(i.ItemSpec) LIKE @search)"; req2.input('search', '%'+search+'%'); }

      const r = await req2.query(`
        SELECT RTRIM(h.CsCode) AS vendor_code, RTRIM(i.ItemCode) AS item_code,
               MAX(RTRIM(i.ItemSpec)) AS item_spec,
               LEFT(h.OrderDate,6) AS ym,
               SUM(i.OrderQty) AS total_qty, SUM(i.OrderAmnt) AS total_amt,
               CASE WHEN SUM(i.OrderQty)>0 THEN SUM(i.OrderAmnt)/SUM(i.OrderQty) ELSE 0 END AS avg_price
        FROM poOrderHeader h WITH(NOLOCK)
        JOIN poOrderItem i WITH(NOLOCK) ON h.SiteCode=i.SiteCode AND h.OrderNo=i.OrderNo
        WHERE h.SiteCode='BK10' AND h.OrderDate>=@from AND h.OrderDate<=@to
          AND RTRIM(h.CsCode) IN (${vendorCodes.map(c => "'"+c+"'").join(',')}) ${extraWhere}
        GROUP BY RTRIM(h.CsCode), RTRIM(i.ItemCode), LEFT(h.OrderDate,6)
        ORDER BY RTRIM(h.CsCode), RTRIM(i.ItemCode), LEFT(h.OrderDate,6)
      `);

      // 품목명 보충
      const pi = getProductInfo();
      const nameMap = {};
      if (pi) { for (const [, info] of Object.entries(pi)) { const mc=(info['원자재코드']||'').trim(); const mn=(info['원재료용지명']||info['원재료명']||'').trim(); if(mc&&mn&&!nameMap[mc]) nameMap[mc]=mn; } }

      // 월 목록
      const monthSet = new Set();
      const rows = r.recordset || [];
      rows.forEach(row => monthSet.add(row.ym));
      const monthList = [...monthSet].sort();

      // 그룹핑: vendor_code + item_code
      const groups = {};
      for (const row of rows) {
        const vName = vendorCodeMap[row.vendor_code] || row.vendor_code;
        const key = vName + '||' + row.item_code;
        if (!groups[key]) groups[key] = { item_code: row.item_code, item_name: nameMap[row.item_code] || row.item_spec || row.item_code, vendor_name: vName, monthly: {} };
        groups[key].monthly[row.ym] = { qty: row.total_qty, amt: row.total_amt, avg_price: Math.round(row.avg_price) };
      }

      // 변동 계산
      const items = Object.values(groups).map(g => {
        const validMonths = monthList.filter(m => g.monthly[m] && g.monthly[m].avg_price > 0);
        let change = null, change_pct = null;
        if (validMonths.length >= 2) {
          const first = g.monthly[validMonths[0]].avg_price;
          const last = g.monthly[validMonths[validMonths.length - 1]].avg_price;
          change = last - first;
          change_pct = first > 0 ? Math.round((last - first) / first * 1000) / 10 : null;
        }
        return { ...g, change, change_pct };
      });

      const vendors = [...new Set(Object.values(groups).map(g => g.vendor_name))].sort();
      ok(res, { months: monthList, items, vendors, count: items.length, from, to });
    } catch (e) {
      console.error('material-price/xerp-trend 오류:', e.message);
      fail(res, 500, 'XERP 매입단가 조회 실패: ' + e.message);
    }
    return;
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
    const info = await db.prepare('DELETE FROM material_prices WHERE ' + where).run(...params);
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
    const orders = await db.prepare(`SELECT * FROM work_orders WHERE ${where} ORDER BY created_at DESC LIMIT 200`).all();
    const summary = await db.prepare(`SELECT status, COUNT(*) AS cnt FROM work_orders GROUP BY status`).all();
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
    const result = await stmt.run(woNum, body.request_id||null, body.product_code||'', body.product_name||'', body.brand||'', body.ordered_qty||0, 'planned', body.priority||'normal', body.start_date||now.toISOString().slice(0,10), body.due_date||'', body.printer_vendor||'', body.post_vendor||'', body.paper_type||'', body.notes||'', decoded.username||'');
    await db.prepare(`INSERT INTO work_order_logs (wo_id, wo_number, action, to_status, actor, details) VALUES (?,?,?,?,?,?)`).run(result.lastInsertRowid, woNum, 'created', 'planned', decoded.username||'', '작업지시 생성');
    ok(res, { wo_id: result.lastInsertRowid, wo_number: woNum }); return;
  }

  // ── GET /api/work-orders/:id ── 작업지시 상세
  if (pathname.match(/^\/api\/work-orders\/(\d+)$/) && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const wo = await db.prepare('SELECT * FROM work_orders WHERE wo_id = ?').get(id);
    const logs = await db.prepare('SELECT * FROM work_order_logs WHERE wo_id = ? ORDER BY created_at DESC').all(id);
    if (!wo) { fail(res, 404, '작업지시 없음'); return; }
    ok(res, { order: wo, logs }); return;
  }

  // ── PUT /api/work-orders/:id ── 작업지시 수정 (상태변경, 실적등록)
  if (pathname.match(/^\/api\/work-orders\/(\d+)$/) && method === 'PUT') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const id = parseInt(pathname.match(/\/(\d+)$/)[1], 10);
    const body = await readJSON(req);
    const wo = await db.prepare('SELECT * FROM work_orders WHERE wo_id = ?').get(id);
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
      await db.prepare(`UPDATE work_orders SET ${updates.join(',')} WHERE wo_id=?`).run(...params);
    }
    // 로그
    let action = 'updated';
    let details = '';
    if (body.status && body.status !== oldStatus) { action = 'status_change'; details = oldStatus + ' → ' + body.status; }
    else if (body.produced_qty !== undefined) { action = 'production_report'; details = '생산수량: ' + body.produced_qty + ', 불량: ' + (body.defect_qty||0); }
    else if (body.cost_material !== undefined || body.cost_labor !== undefined) { action = 'cost_update'; details = '원가 업데이트'; }
    await db.prepare(`INSERT INTO work_order_logs (wo_id, wo_number, action, from_status, to_status, qty_change, actor, details) VALUES (?,?,?,?,?,?,?,?)`).run(id, wo.wo_number, action, oldStatus, body.status||oldStatus, body.produced_qty||0, decoded.username||'', details);
    ok(res, { message: '업데이트 완료' }); return;
  }

  // ── GET /api/work-orders/stats ── 작업지시 통계
  if (pathname === '/api/work-orders/stats' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const statusSummary = await db.prepare(`SELECT status, COUNT(*) AS cnt, SUM(ordered_qty) AS total_ordered, SUM(produced_qty) AS total_produced, SUM(defect_qty) AS total_defect, SUM(cost_total) AS total_cost FROM work_orders GROUP BY status`).all();
    const monthlyOrders = await db.prepare(`SELECT TO_CHAR(created_at::timestamp, 'YYYY-MM') AS ym, COUNT(*) AS cnt, SUM(ordered_qty) AS total_qty FROM work_orders GROUP BY TO_CHAR(created_at::timestamp, 'YYYY-MM') ORDER BY ym DESC LIMIT 12`).all();
    const recentCompleted = await db.prepare(`SELECT * FROM work_orders WHERE status='completed' ORDER BY completed_date DESC LIMIT 10`).all();
    ok(res, { statusSummary, monthlyOrders, recentCompleted }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  공정 타입 마스터 API (Process Types)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/process-types' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const cat = qs.get('category');
    const hasTable = await checkProcessTypesTable();
    let rows;
    if (hasTable) {
      if (cat) rows = await db.prepare("SELECT * FROM process_types WHERE category=? ORDER BY sort_order, id").all(cat);
      else rows = await db.prepare("SELECT * FROM process_types ORDER BY category, sort_order, id").all();
    } else {
      const mem = _processTypesInMemory || [..._defaultPostTypes, ..._defaultBomTypes];
      rows = cat ? mem.filter(p => p.category === cat) : mem;
      rows = rows.sort((a,b) => a.sort_order - b.sort_order);
    }
    ok(res, rows); return;
  }

  if (pathname === '/api/process-types' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.name) { fail(res, 400, '공정명 필수'); return; }
    const cat = body.category || 'post';
    const hasTable = await checkProcessTypesTable();
    if (hasTable) {
      const maxSort = await db.prepare("SELECT MAX(sort_order) AS mx FROM process_types WHERE category=?").get(cat);
      const nextSort = (maxSort?.mx || 0) + 1;
      try {
        const r = await db.prepare("INSERT INTO process_types (name,category,group_name,icon,sort_order,default_vendor) VALUES (?,?,?,?,?,?)").run(
          body.name, cat, body.group_name||'', body.icon||'⚙️', body.sort_order || nextSort, body.default_vendor||''
        );
        invalidatePostColsCache();
        ok(res, { id: r.lastInsertRowid, message: '공정 추가 완료' });
      } catch(e) {
        if (e.message?.includes('UNIQUE') || e.message?.includes('unique') || e.message?.includes('duplicate')) fail(res, 409, '이미 존재하는 공정명입니다');
        else fail(res, 500, e.message);
      }
    } else {
      // 인메모리 모드
      if (!_processTypesInMemory) _processTypesInMemory = [..._defaultPostTypes, ..._defaultBomTypes];
      if (_processTypesInMemory.find(p => p.name === body.name && p.category === cat)) { fail(res, 409, '이미 존재하는 공정명입니다'); return; }
      const maxId = Math.max(..._processTypesInMemory.map(p => p.id), 0);
      const maxSort = Math.max(..._processTypesInMemory.filter(p => p.category === cat).map(p => p.sort_order), 0);
      const newPt = { id: maxId + 1, name: body.name, category: cat, group_name: body.group_name||'', icon: body.icon||'⚙️', sort_order: body.sort_order || maxSort + 1, is_active: 1, default_vendor: body.default_vendor||'' };
      _processTypesInMemory.push(newPt);
      saveProcessTypesToFile();
      invalidatePostColsCache();
      ok(res, { id: newPt.id, message: '공정 추가 완료' });
    }
    return;
  }

  const ptPut = pathname.match(/^\/api\/process-types\/(\d+)$/);
  if (ptPut && method === 'PUT') {
    const id = parseInt(ptPut[1]);
    const body = await readJSON(req);
    const hasTable = await checkProcessTypesTable();
    if (hasTable) {
      const sets = [], vals = [];
      if (body.name !== undefined) { sets.push('name=?'); vals.push(body.name); }
      if (body.group_name !== undefined) { sets.push('group_name=?'); vals.push(body.group_name); }
      if (body.icon !== undefined) { sets.push('icon=?'); vals.push(body.icon); }
      if (body.sort_order !== undefined) { sets.push('sort_order=?'); vals.push(body.sort_order); }
      if (body.is_active !== undefined) { sets.push('is_active=?'); vals.push(body.is_active); }
      if (body.default_vendor !== undefined) { sets.push('default_vendor=?'); vals.push(body.default_vendor); }
      if (sets.length === 0) { fail(res, 400, '변경할 항목 없음'); return; }
      vals.push(id);
      await db.prepare(`UPDATE process_types SET ${sets.join(',')} WHERE id=?`).run(...vals);
    } else {
      if (!_processTypesInMemory) _processTypesInMemory = [..._defaultPostTypes, ..._defaultBomTypes];
      const pt = _processTypesInMemory.find(p => p.id === id);
      if (pt) {
        if (body.name !== undefined) pt.name = body.name;
        if (body.group_name !== undefined) pt.group_name = body.group_name;
        if (body.icon !== undefined) pt.icon = body.icon;
        if (body.sort_order !== undefined) pt.sort_order = body.sort_order;
        if (body.is_active !== undefined) pt.is_active = body.is_active;
        if (body.default_vendor !== undefined) pt.default_vendor = body.default_vendor;
        saveProcessTypesToFile();
      }
    }
    invalidatePostColsCache();
    ok(res, { updated: id }); return;
  }

  const ptDel = pathname.match(/^\/api\/process-types\/(\d+)$/);
  if (ptDel && method === 'DELETE') {
    const id = parseInt(ptDel[1]);
    const hasTable = await checkProcessTypesTable();
    if (hasTable) {
      const pt = await db.prepare("SELECT name FROM process_types WHERE id=?").get(id);
      if (pt) {
        const used = await db.prepare("SELECT COUNT(*) AS cnt FROM product_process_map WHERE process_type=?").get(pt.name);
        if (used && used.cnt > 0) {
          await db.prepare("UPDATE process_types SET is_active=0 WHERE id=?").run(id);
          invalidatePostColsCache();
          ok(res, { deactivated: id, reason: `${used.cnt}개 제품에서 사용 중이어서 비활성화됨` }); return;
        }
      }
      await db.prepare("DELETE FROM process_types WHERE id=?").run(id);
    } else {
      if (!_processTypesInMemory) _processTypesInMemory = [..._defaultPostTypes, ..._defaultBomTypes];
      _processTypesInMemory = _processTypesInMemory.filter(p => p.id !== id);
      saveProcessTypesToFile();
    }
    invalidatePostColsCache();
    ok(res, { deleted: id }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  알림센터 API (Notifications)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/notifications' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const rows = await db.prepare("SELECT * FROM notifications WHERE user_id IS NULL OR user_id = ? ORDER BY created_at DESC LIMIT 100").all(uid);
    ok(res, rows); return;
  }
  if (pathname === '/api/notifications/unread-count' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const r = await db.prepare("SELECT COUNT(*) AS cnt FROM notifications WHERE (user_id IS NULL OR user_id = ?) AND is_read = 0").get(uid);
    ok(res, { count: r.cnt }); return;
  }
  if (pathname.match(/^\/api\/notifications\/read\/(\d+)$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/notifications\/read\/(\d+)$/)[1];
    await db.prepare("UPDATE notifications SET is_read = 1 WHERE id = ?").run(id);
    ok(res, { updated: true }); return;
  }
  if (pathname === '/api/notifications/read-all' && method === 'POST') {
    const uid = currentUser ? currentUser.userId : 0;
    await db.prepare("UPDATE notifications SET is_read = 1 WHERE (user_id IS NULL OR user_id = ?) AND is_read = 0").run(uid);
    ok(res, { updated: true }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  전자결재 API (Approvals)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/approvals/pending-count' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const r = await db.prepare("SELECT COUNT(*) AS cnt FROM approval_lines al JOIN approvals a ON a.id=al.approval_id WHERE al.approver_id=? AND al.status='pending' AND a.status='pending'").get(uid);
    ok(res, { count: r.cnt }); return;
  }
  if (pathname === '/api/approvals' && method === 'GET') {
    const uid = currentUser ? currentUser.userId : 0;
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const tab = qs.get('tab') || 'my'; // my/pending/done
    let rows = [];
    if (tab === 'my') rows = await db.prepare("SELECT * FROM approvals WHERE requester_id=? ORDER BY created_at DESC LIMIT 200").all(uid);
    else if (tab === 'pending') rows = await db.prepare("SELECT a.* FROM approvals a JOIN approval_lines al ON a.id=al.approval_id WHERE al.approver_id=? AND al.status='pending' AND a.status='pending' ORDER BY a.created_at DESC").all(uid);
    else rows = await db.prepare("SELECT a.* FROM approvals a JOIN approval_lines al ON a.id=al.approval_id WHERE al.approver_id=? AND al.status IN ('approved','rejected') ORDER BY al.acted_at DESC LIMIT 200").all(uid);
    ok(res, rows); return;
  }
  if (pathname === '/api/approvals' && method === 'POST') {
    const body = await readJSON(req);
    const uid = currentUser ? currentUser.userId : 0;
    const uname = currentUser ? currentUser.username : 'system';
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const seq = (await db.prepare("SELECT COUNT(*) AS cnt FROM approvals WHERE approval_no LIKE ?").get('AP-'+today+'%')).cnt + 1;
    const no = 'AP-'+today+'-'+String(seq).padStart(3,'0');
    const lines = body.lines || []; // [{approver_id, approver_name}]
    // approver_name만 있고 approver_id가 없으면 users에서 자동 조회
    for (const ln of lines) {
      if (!ln.approver_id && ln.approver_name) {
        const u = await db.prepare("SELECT user_id, display_name, username FROM users WHERE display_name = ? OR username = ? LIMIT 1").get(ln.approver_name, ln.approver_name);
        if (u) { ln.approver_id = u.user_id; ln.approver_name = u.display_name || u.username; }
      }
    }
    const info = await db.prepare("INSERT INTO approvals (approval_no,doc_type,doc_ref,title,content,amount,status,requester_id,requester_name,current_step,total_steps) VALUES (?,?,?,?,?,?,?,?,?,1,?)").run(
      no, body.doc_type||'general', body.doc_ref||'', body.title||'', body.content||'', body.amount||0, 'pending', uid, uname, Math.max(lines.length,1));
    const aid = info.lastInsertRowid;
    for (let i = 0; i < lines.length; i++) {
      const ln = lines[i];
      await db.prepare("INSERT INTO approval_lines (approval_id,step_order,approver_id,approver_name,role) VALUES (?,?,?,?,?)").run(aid, i+1, ln.approver_id||null, ln.approver_name||'', ln.role||'approver');
    }
    // 첫 번째 결재자에게 알림
    if (lines.length > 0) createNotification(lines[0].approver_id, 'approval', '결재 요청: '+body.title, uname+'님이 결재를 요청했습니다.', 'approval');
    auditLog(decoded.userId, uname, 'approval_create', 'approvals', aid, `결재상신: ${no} "${body.title}" (${body.type||'일반'})`, clientIP);
    ok(res, { id: aid, approval_no: no }); return;
  }
  if (pathname.match(/^\/api\/approvals\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/approvals\/(\d+)$/)[1];
    const row = await db.prepare("SELECT * FROM approvals WHERE id=?").get(id);
    if (!row) { fail(res, 404, '결재 문서를 찾을 수 없습니다'); return; }
    const lines = await db.prepare("SELECT * FROM approval_lines WHERE approval_id=? ORDER BY step_order").all(id);
    ok(res, { ...row, lines }); return;
  }
  if (pathname.match(/^\/api\/approvals\/(\d+)\/approve$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/approvals\/(\d+)\/approve$/)[1];
    const body = await readJSON(req);
    const uid = currentUser ? currentUser.userId : 0;
    const ap = await db.prepare("SELECT * FROM approvals WHERE id=?").get(id);
    if (!ap || ap.status !== 'pending') { fail(res, 400, '결재 불가 상태'); return; }
    const line = await db.prepare("SELECT * FROM approval_lines WHERE approval_id=? AND step_order=? AND status='pending'").get(id, ap.current_step);
    if (!line) { fail(res, 403, '결재 권한이 없습니다'); return; }
    await db.prepare("UPDATE approval_lines SET status='approved', comment=?, acted_at=datetime('now','localtime') WHERE id=?").run(body.comment||'', line.id);
    // 다음 단계 또는 최종 승인
    const nextLine = await db.prepare("SELECT * FROM approval_lines WHERE approval_id=? AND step_order>? AND status='pending' ORDER BY step_order LIMIT 1").get(id, line.step_order);
    if (nextLine) {
      await db.prepare("UPDATE approvals SET current_step=?, updated_at=datetime('now','localtime') WHERE id=?").run(nextLine.step_order, id);
      createNotification(nextLine.approver_id, 'approval', '결재 요청: '+ap.title, '다음 단계 결재를 요청합니다.', 'approval');
    } else {
      await db.prepare("UPDATE approvals SET status='approved', updated_at=datetime('now','localtime') WHERE id=?").run(id);
      createNotification(ap.requester_id, 'approval', '결재 승인: '+ap.title, '요청하신 결재가 최종 승인되었습니다.', 'approval');
    }
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'approval_approve', 'approvals', id, `결재승인: ${ap.approval_no||id} "${ap.title}"`, clientIP);
    ok(res, { approved: true }); return;
  }
  if (pathname.match(/^\/api\/approvals\/(\d+)\/reject$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/approvals\/(\d+)\/reject$/)[1];
    const body = await readJSON(req);
    const uid = currentUser ? currentUser.userId : 0;
    const ap = await db.prepare("SELECT * FROM approvals WHERE id=?").get(id);
    if (!ap || ap.status !== 'pending') { fail(res, 400, '결재 불가 상태'); return; }
    const line = await db.prepare("SELECT * FROM approval_lines WHERE approval_id=? AND step_order=? AND status='pending'").get(id, ap.current_step);
    if (!line) { fail(res, 403, '결재 권한이 없습니다'); return; }
    await db.prepare("UPDATE approval_lines SET status='rejected', comment=?, acted_at=datetime('now','localtime') WHERE id=?").run(body.comment||'', line.id);
    await db.prepare("UPDATE approvals SET status='rejected', updated_at=datetime('now','localtime') WHERE id=?").run(id);
    createNotification(ap.requester_id, 'approval', '결재 반려: '+ap.title, (body.comment||'사유 없음'), 'approval');
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'approval_reject', 'approvals', id, `결재반려: ${ap.approval_no||id} "${ap.title}" 사유:${body.comment||'없음'}`, clientIP);
    ok(res, { rejected: true }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  수주관리 API (Sales Orders)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/sales-orders/summary' && method === 'GET') {
    const quote_count = (await db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE order_type='quote' AND status NOT IN ('cancelled')").get()).cnt;
    const order_amount = (await db.prepare("SELECT COALESCE(SUM(total_amount),0) AS amt FROM sales_orders WHERE order_type='sales' AND status NOT IN ('cancelled','delivered')").get()).amt;
    const shipped_count = (await db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE status='shipped'").get()).cnt;
    const unshipped_count = (await db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE order_type='sales' AND status IN ('draft','confirmed','in_production')").get()).cnt;
    const bySource = await db.prepare("SELECT source, COUNT(*) AS cnt, COALESCE(SUM(total_amount),0) AS amt FROM sales_orders GROUP BY source").all();
    const syncDD = await db.prepare("SELECT last_sync, status FROM sync_meta WHERE key='sales-dd'").get();
    const syncXerp = await db.prepare("SELECT last_sync, status FROM sync_meta WHERE key='sales-xerp'").get();
    ok(res, { quote_count, order_amount, shipped_count, unshipped_count, by_source: bySource, sync: { dd: syncDD, xerp: syncXerp } }); return;
  }
  if (pathname === '/api/sales-orders' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const status = qs.get('status') || '';
    const type = qs.get('type') || '';
    const search = qs.get('search') || '';
    let where = '1=1';
    const params = [];
    if (status && status !== 'all') { where += " AND status=?"; params.push(status); }
    if (type) { where += " AND order_type=?"; params.push(type); }
    if (search) { where += " AND (order_no LIKE ? OR customer_name LIKE ?)"; params.push('%'+search+'%', '%'+search+'%'); }
    const rows = await db.prepare('SELECT * FROM sales_orders WHERE '+where+' ORDER BY created_at DESC LIMIT 200').all(...params);
    ok(res, rows); return;
  }
  if (pathname === '/api/sales-orders' && method === 'POST') {
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const prefix = (body.order_type === 'quote') ? 'QT' : 'SO';
    const seq = (await db.prepare("SELECT COUNT(*) AS cnt FROM sales_orders WHERE order_no LIKE ?").get(prefix+'-'+today+'%')).cnt + 1;
    const no = prefix+'-'+today+'-'+String(seq).padStart(3,'0');
    const items = body.items || [];
    const totalQty = items.reduce(function(s,i){return s+(i.qty||0);},0);
    const totalAmt = items.reduce(function(s,i){return s+(i.amount||(i.qty||0)*(i.unit_price||0));},0);
    const taxAmt = body.tax_amount || Math.round(totalAmt * 0.1);
    const info = await db.prepare("INSERT INTO sales_orders (order_no,order_type,status,customer_name,customer_contact,customer_tel,order_date,delivery_date,total_qty,total_amount,tax_amount,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      no, body.order_type||'quote', 'draft', body.customer_name||'', body.customer_contact||'', body.customer_tel||'', body.order_date||new Date().toISOString().slice(0,10), body.delivery_date||'', totalQty, totalAmt, taxAmt, body.notes||'', uname);
    const oid = info.lastInsertRowid;
    items.forEach(async function(it) {
      await db.prepare("INSERT INTO sales_order_items (order_id,product_code,product_name,spec,unit_price,qty,amount,notes) VALUES (?,?,?,?,?,?,?,?)").run(oid, it.product_code||'', it.product_name||'', it.spec||'', it.unit_price||0, it.qty||0, it.amount||(it.qty||0)*(it.unit_price||0), it.notes||'');
    });
    ok(res, { id: oid, order_no: no }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)$/)[1];
    const row = await db.prepare("SELECT * FROM sales_orders WHERE id=?").get(id);
    if (!row) { fail(res, 404, '수주를 찾을 수 없습니다'); return; }
    const items = await db.prepare("SELECT * FROM sales_order_items WHERE order_id=?").all(id);
    ok(res, { ...row, items }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)$/) && method === 'PUT') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)$/)[1];
    const body = await readJSON(req);
    await db.prepare("UPDATE sales_orders SET customer_name=COALESCE(?,customer_name), customer_contact=COALESCE(?,customer_contact), customer_tel=COALESCE(?,customer_tel), delivery_date=COALESCE(?,delivery_date), notes=COALESCE(?,notes), updated_at=datetime('now','localtime') WHERE id=?").run(body.customer_name, body.customer_contact, body.customer_tel, body.delivery_date, body.notes, id);
    if (body.items) {
      await db.prepare("DELETE FROM sales_order_items WHERE order_id=?").run(id);
      for (const it of body.items) { await db.prepare("INSERT INTO sales_order_items (order_id,product_code,product_name,spec,unit_price,qty,amount,notes) VALUES (?,?,?,?,?,?,?,?)").run(id, it.product_code||'', it.product_name||'', it.spec||'', it.unit_price||0, it.qty||0, it.amount||((it.qty||0)*(it.unit_price||0)), it.notes||''); }
      const totalQty = body.items.reduce(function(s,i){return s+(i.qty||0);},0);
      const totalAmt = body.items.reduce(function(s,i){return s+(i.amount||((i.qty||0)*(i.unit_price||0)));},0);
      const taxAmt = body.tax_amount || Math.round(totalAmt * 0.1);
      await db.prepare("UPDATE sales_orders SET total_qty=?, total_amount=?, tax_amount=?, updated_at=datetime('now','localtime') WHERE id=?").run(totalQty, totalAmt, taxAmt, id);
    }
    ok(res, { updated: true }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)\/confirm$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)\/confirm$/)[1];
    const row = await db.prepare("SELECT * FROM sales_orders WHERE id=?").get(id);
    if (!row) { fail(res, 404, '문서 없음'); return; }
    const newType = row.order_type === 'quote' ? 'sales' : row.order_type;
    await db.prepare("UPDATE sales_orders SET order_type=?, status='confirmed', updated_at=datetime('now','localtime') WHERE id=?").run(newType, id);
    ok(res, { confirmed: true }); return;
  }
  if (pathname.match(/^\/api\/sales-orders\/(\d+)\/ship$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/sales-orders\/(\d+)\/ship$/)[1];
    await db.prepare("UPDATE sales_orders SET status='shipped', shipped_date=date('now','localtime'), updated_at=datetime('now','localtime') WHERE id=?").run(id);
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
      const tx = db.transaction(async function() {
        rows.forEach(async function(r) {
          const oNo = 'DD-' + (r.order_number || r.id);
          const st = stateMap[r.order_state] || 'draft';
          const shipDate = (r.shipping_state === 'Y' || r.order_state === 'D') ? (r.created_at ? r.created_at.toISOString().slice(0,10) : '') : '';
          await upsert.run(oNo, 'sales', st, 'DD고객', r.total_qty||0, r.total_money||0,
            r.created_at ? r.created_at.toISOString().slice(0,10) : '', shipDate,
            (r.product_names||'').substring(0,200), 'dd', String(r.id));
        });
      });
      await tx();
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('sales-dd',datetime('now','localtime'),?,'ok')").run(rows.length);
      ok(res, { synced: rows.length, source: 'DD' }); return;
    } catch(e) {
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,status,message) VALUES ('sales-dd',datetime('now','localtime'),'error',?)").run(e.message);
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
      const tx = db.transaction(async function() {
        Object.keys(byDate).forEach(async function(d) {
          const v = byDate[d];
          const dateStr = d.length===8 ? d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8) : d;
          await upsert.run('XERP-'+d, 'sales', 'delivered', 'XERP매출', v.qty, v.amount, dateStr,
            '채널: '+Array.from(v.dept).join(',')+'  건수: '+v.count, 'xerp', d, 'sync');
        });
      });
      await tx();
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('sales-xerp',datetime('now','localtime'),?,'ok')").run(Object.keys(byDate).length);
      ok(res, { synced: Object.keys(byDate).length, raw_records: rows.length, source: 'XERP' }); return;
    } catch(e) {
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,status,message) VALUES ('sales-xerp',datetime('now','localtime'),'error',?)").run(e.message);
      fail(res, 500, 'XERP 동기화 실패: ' + e.message); return;
    }
  }
  if (pathname === '/api/sync-meta' && method === 'GET') {
    const rows = await db.prepare("SELECT * FROM sync_meta ORDER BY key").all();
    ok(res, rows); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Lot/시리얼 추적 API (Lot Tracking)
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/lots/summary' && method === 'GET') {
    const total = (await db.prepare("SELECT COUNT(*) AS cnt FROM batch_master").get()).cnt;
    const active = (await db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE current_qty > 0").get()).cnt;
    const held = (await db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE quality_status='HOLD'").get()).cnt;
    const totalQty = (await db.prepare("SELECT COALESCE(SUM(current_qty),0) AS qty FROM batch_master").get()).qty;
    const expiring = (await db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE exp_date IS NOT NULL AND exp_date != '' AND exp_date <= (CURRENT_DATE + INTERVAL '30 days')::text AND current_qty > 0").get()).cnt;
    ok(res, { total, active, held, totalQty, expiring, active_lots: active, total_qty: totalQty, held_lots: held, expiring_soon: expiring }); return;
  }
  if (pathname === '/api/lots' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const product = qs.get('product') || '';
    const warehouse = qs.get('warehouse') || '';
    const status = qs.get('status') || '';
    const search = qs.get('search') || '';
    const entity = qs.get('entity') || '';
    let where = '1=1';
    if (product) where += " AND product_code='" + product.replace(/'/g,'') + "'";
    if (warehouse) where += " AND warehouse='" + warehouse.replace(/'/g,'') + "'";
    if (status && status !== 'all') where += " AND quality_status='" + status.replace(/'/g,'') + "'";
    if (search) where += " AND (batch_number LIKE '%" + search.replace(/'/g,'') + "%' OR product_name LIKE '%" + search.replace(/'/g,'') + "%')";
    if (entity && entity !== 'all' && _hasEntity.batch_master) where += " AND legal_entity='" + entity.replace(/'/g,'') + "'";
    const rows = await db.prepare('SELECT *, batch_id AS id, quality_status AS status, exp_date AS expiry_date FROM batch_master WHERE '+where+' ORDER BY created_at DESC LIMIT 300').all();
    ok(res, rows); return;
  }
  if (pathname === '/api/lots' && method === 'POST') {
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    const today = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const seq = (await db.prepare("SELECT COUNT(*) AS cnt FROM batch_master WHERE batch_number LIKE ?").get('LOT-'+today+'%')).cnt + 1;
    const batchNo = 'LOT-'+today+'-'+String(seq).padStart(3,'0');
    const info = await db.prepare("INSERT INTO batch_master (batch_number,product_code,product_name,vendor_name,vendor_lot,received_date,po_number,received_qty,current_qty,quality_status,warehouse,mfg_date,exp_date,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      batchNo, body.product_code||'', body.product_name||'', body.vendor_name||'', body.vendor_lot||'', body.received_date||new Date().toISOString().slice(0,10), body.po_number||'', body.received_qty||0, body.received_qty||0, body.quality_status||'GOOD', body.warehouse||'본사', body.mfg_date||'', body.exp_date||'', body.notes||'', uname);
    const bid = info.lastInsertRowid;
    await db.prepare("INSERT INTO batch_transactions (batch_id,batch_number,txn_type,product_code,qty,qty_before,qty_after,to_warehouse,reference_no,actor,notes) VALUES (?,?,'receipt',?,?,0,?,?,?,?,?)").run(bid, batchNo, body.product_code||'', body.received_qty||0, body.received_qty||0, body.warehouse||'본사', body.po_number||'', uname, '입고 등록');
    ok(res, { id: bid, batch_number: batchNo }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/lots\/(\d+)$/)[1];
    const row = await db.prepare("SELECT * FROM batch_master WHERE batch_id=?").get(id);
    if (!row) { fail(res, 404, 'Lot를 찾을 수 없습니다'); return; }
    const txns = await db.prepare("SELECT * FROM batch_transactions WHERE batch_id=? ORDER BY created_at DESC").all(id);
    const inspections = await db.prepare("SELECT * FROM batch_inspections WHERE batch_id=? ORDER BY insp_date DESC").all(id);
    ok(res, { ...row, transactions: txns, inspections }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)$/) && method === 'PUT') {
    const id = pathname.match(/^\/api\/lots\/(\d+)$/)[1];
    const body = await readJSON(req);
    await db.prepare("UPDATE batch_master SET quality_status=COALESCE(?,quality_status), warehouse=COALESCE(?,warehouse), notes=COALESCE(?,notes), updated_at=datetime('now','localtime') WHERE batch_id=?").run(body.quality_status, body.warehouse, body.notes, id);
    ok(res, { updated: true }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)\/transaction$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/lots\/(\d+)\/transaction$/)[1];
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    const lot = await db.prepare("SELECT * FROM batch_master WHERE batch_id=?").get(id);
    if (!lot) { fail(res, 404, 'Lot 없음'); return; }
    const qtyBefore = lot.current_qty;
    let qtyAfter = qtyBefore;
    const txnType = body.txn_type || 'usage';
    if (txnType === 'usage' || txnType === 'transfer') qtyAfter = qtyBefore - (body.qty || 0);
    else if (txnType === 'receipt' || txnType === 'return') qtyAfter = qtyBefore + (body.qty || 0);
    else if (txnType === 'quality_hold') qtyAfter = qtyBefore;
    await db.prepare("INSERT INTO batch_transactions (batch_id,batch_number,txn_type,txn_date,from_warehouse,to_warehouse,product_code,qty,qty_before,qty_after,reference_no,actor,notes) VALUES (?,?,?,datetime('now','localtime'),?,?,?,?,?,?,?,?,?)").run(
      id, lot.batch_number, txnType, body.from_warehouse||lot.warehouse, body.to_warehouse||'', lot.product_code, body.qty||0, qtyBefore, qtyAfter, body.reference_no||'', uname, body.notes||'');
    await db.prepare("UPDATE batch_master SET current_qty=?, warehouse=COALESCE(?,warehouse), updated_at=datetime('now','localtime') WHERE batch_id=?").run(qtyAfter, body.to_warehouse||null, id);
    if (txnType === 'quality_hold') await db.prepare("UPDATE batch_master SET quality_status='HOLD' WHERE batch_id=?").run(id);
    ok(res, { txn_type: txnType, qty_before: qtyBefore, qty_after: qtyAfter }); return;
  }
  if (pathname.match(/^\/api\/lots\/(\d+)\/inspect$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/lots\/(\d+)\/inspect$/)[1];
    const body = await readJSON(req);
    const uname = currentUser ? currentUser.username : 'system';
    await db.prepare("INSERT INTO batch_inspections (batch_id,batch_number,insp_date,inspector,insp_type,sample_size,defects_found,defect_desc,result,next_action,notes,created_by) VALUES (?,?,datetime('now','localtime'),?,?,?,?,?,?,?,?,?)").run(
      id, body.batch_number||'', body.inspector||uname, body.insp_type||'RECEIVING', body.sample_size||0, body.defects_found||0, body.defect_desc||'', body.result||'PASS', body.next_action||'OK', body.notes||'', uname);
    if (body.result === 'FAIL') await db.prepare("UPDATE batch_master SET quality_status='HOLD', updated_at=datetime('now','localtime') WHERE batch_id=?").run(id);
    else if (body.result === 'PASS') await db.prepare("UPDATE batch_master SET quality_status='GOOD', updated_at=datetime('now','localtime') WHERE batch_id=?").run(id);
    ok(res, { inspected: true }); return;
  }
  if (pathname.match(/^\/api\/lots\/trace\/(.+)$/) && method === 'GET') {
    const bn = decodeURIComponent(pathname.match(/^\/api\/lots\/trace\/(.+)$/)[1]);
    const lot = await db.prepare("SELECT * FROM batch_master WHERE batch_number=?").get(bn);
    if (!lot) { fail(res, 404, 'Lot 없음'); return; }
    const txns = await db.prepare("SELECT * FROM batch_transactions WHERE batch_number=? ORDER BY created_at").all(bn);
    const insps = await db.prepare("SELECT * FROM batch_inspections WHERE batch_number=? ORDER BY insp_date").all(bn);
    ok(res, { lot, transactions: txns, inspections: insps }); return;
  }

  // ── 선입선출(FIFO) + 소비기한 API ──
  if (pathname === '/api/lots/fifo' && method === 'GET') {
    try {
      // 더기프트/답례품 Lot만 (current_qty > 0, exp_date 있는 것 우선)
      const rows = await db.prepare(`
        SELECT b.*, p.origin
        FROM batch_master b
        LEFT JOIN products p ON b.product_code = p.product_code
        WHERE b.current_qty > 0
        ORDER BY
          CASE WHEN b.exp_date IS NOT NULL AND b.exp_date != '' THEN 0 ELSE 1 END,
          b.exp_date ASC,
          b.received_date ASC,
          b.batch_id ASC
      `).all();

      // 품목별 FIFO 순서 + 소비기한 요약
      const byProduct = {};
      const today = new Date().toISOString().slice(0,10);
      let totalExpiring = 0, totalExpired = 0, totalActive = 0;
      rows.forEach(r => {
        const code = r.product_code || '';
        if (!byProduct[code]) byProduct[code] = { product_code: code, product_name: r.product_name || '', lots: [], total_qty: 0, origin: r.origin || '' };
        const daysToExpiry = r.exp_date ? Math.ceil((new Date(r.exp_date) - new Date(today)) / 86400000) : null;
        byProduct[code].lots.push({ ...r, days_to_expiry: daysToExpiry });
        byProduct[code].total_qty += (r.current_qty || 0);
        if (daysToExpiry !== null && daysToExpiry <= 0) totalExpired++;
        else if (daysToExpiry !== null && daysToExpiry <= 30) totalExpiring++;
        totalActive++;
      });

      ok(res, {
        products: Object.values(byProduct),
        summary: { total_lots: totalActive, expiring_30d: totalExpiring, expired: totalExpired, products: Object.keys(byProduct).length }
      });
    } catch (e) { fail(res, 500, e.message); }
    return;
  }

  // FIFO 출고 처리 (가장 오래된 Lot부터 차감)
  if (pathname === '/api/lots/fifo-consume' && method === 'POST') {
    try {
      const body = await readJSON(req);
      const { product_code, qty, reference, notes } = body;
      if (!product_code || !qty) { fail(res, 400, 'product_code, qty 필수'); return; }
      const uname = currentUser ? currentUser.username : 'system';

      // FIFO 순서: 유효기한 빠른 것 → 입고일 빠른 것
      const lots = await db.prepare(`
        SELECT * FROM batch_master
        WHERE product_code = ? AND current_qty > 0 AND quality_status = 'GOOD'
        ORDER BY
          CASE WHEN exp_date IS NOT NULL AND exp_date != '' THEN 0 ELSE 1 END,
          exp_date ASC, received_date ASC, batch_id ASC
      `).all(product_code);

      let remaining = qty;
      const consumed = [];
      const txn = db.transaction(async () => {
        for (const lot of lots) {
          if (remaining <= 0) break;
          const use = Math.min(lot.current_qty, remaining);
          const after = lot.current_qty - use;
          await db.prepare("UPDATE batch_master SET current_qty=?, updated_at=datetime('now','localtime') WHERE batch_id=?").run(after, lot.batch_id);
          await db.prepare("INSERT INTO batch_transactions (batch_id,batch_number,txn_type,product_code,qty,qty_before,qty_after,reference_no,actor,notes) VALUES (?,?,'usage',?,?,?,?,?,?,?)").run(
            lot.batch_id, lot.batch_number, product_code, use, lot.current_qty, after, reference || '', uname, notes || 'FIFO 출고');
          consumed.push({ batch_number: lot.batch_number, exp_date: lot.exp_date, used: use, remaining: after });
          remaining -= use;
        }
      });
      await txn();

      ok(res, { consumed, total_used: qty - remaining, unfulfilled: remaining > 0 ? remaining : 0 });
    } catch (e) { fail(res, 500, e.message); }
    return;
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
      const tx1 = db.transaction(async function() {
        invRows.forEach(async function(r) {
          const bn = 'XERP-' + (r.item_code||'').trim();
          await upsertLot.run(bn, (r.item_code||'').trim(), (r.item_name||'').trim(), (r.wh_code||'BK10').trim(), r.oh_qty||0, r.oh_qty||0);
        });
      });
      await tx1();
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
      const tx2 = db.transaction(async function() {
        txnRows.forEach(async function(r) {
          const bn = 'XERP-' + (r.item_code||'').trim();
          const d = (r.InoutDate||'').toString().trim();
          const dateStr = d.length===8 ? d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6,8) : d;
          await insTxn.run(bn, typeMap[r.InoutGubun]||'usage', dateStr, (r.item_code||'').trim(), r.qty||0, 'XERP-'+r.InoutGubun+'-'+d, 'sync', (r.item_name||'').trim());
        });
      });
      await tx2();
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('lot-xerp',datetime('now','localtime'),?,'ok')").run(invRows.length);
      ok(res, { lots_synced: invRows.length, transactions_synced: txnRows.length, source: 'XERP' }); return;
    } catch(e) {
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,status,message) VALUES ('lot-xerp',datetime('now','localtime'),'error',?)").run(e.message);
      fail(res, 500, 'Lot XERP 동기화 실패: ' + e.message); return;
    }
  }

  // ── 예산관리: XERP GL 실적 자동 집계 ──
  if (pathname === '/api/budget/sync-actual' && method === 'POST') {
    try {
      const qs = new URL(req.url, 'http://localhost').searchParams;
      const year = qs.get('year') || new Date().getFullYear().toString();
      // gl_balance_cache 갱신 (이미 있으면 그걸 사용)
      const budgets = await db.prepare("SELECT DISTINCT acc_code, month FROM budgets WHERE year=? AND acc_code IS NOT NULL AND acc_code != ''").all(year);
      let updated = 0;
      budgets.forEach(async function(b) {
        const ym = year + '-' + b.month;
        const cache = await db.prepare("SELECT period_dr, period_cr FROM gl_balance_cache WHERE acc_code=? AND year_month=?").get(b.acc_code, ym);
        if (cache) {
          const budget = await db.prepare("SELECT budget_type FROM budgets WHERE year=? AND month=? AND acc_code=?").get(year, b.month, b.acc_code);
          const actual = budget && budget.budget_type === 'revenue' ? cache.period_cr : cache.period_dr;
          await db.prepare("UPDATE budgets SET actual_amount=?, updated_at=datetime('now','localtime') WHERE year=? AND month=? AND acc_code=?").run(actual, year, b.month, b.acc_code);
          updated++;
        }
      });
      await db.prepare("INSERT OR REPLACE INTO sync_meta (key,last_sync,record_count,status) VALUES ('budget-actual',datetime('now','localtime'),?,'ok')").run(updated);
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
    const rows = await db.prepare("SELECT * FROM budgets WHERE year=? ORDER BY month, acc_code").all(year);
    ok(res, rows); return;
  }
  if (pathname === '/api/budget/save' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [];
    const upsert = db.prepare("INSERT INTO budgets (year,month,acc_code,acc_name,budget_type,budget_amount,notes) VALUES (?,?,?,?,?,?,?) ON CONFLICT(year,month,acc_code) DO UPDATE SET budget_amount=excluded.budget_amount, acc_name=excluded.acc_name, budget_type=excluded.budget_type, notes=excluded.notes, updated_at=datetime('now','localtime')");
    const tx = db.transaction(async function() { for (const it of items) { await upsert.run(it.year, it.month, it.acc_code||'', it.acc_name||'', it.budget_type||'expense', it.budget_amount||0, it.notes||''); } });
    await tx();
    if (currentUser) auditLog(currentUser.userId, currentUser.username, 'budget_save', 'budgets', items[0]?.year||'', `예산편성: ${items.length}건 (${items[0]?.year||''})`, clientIP);
    ok(res, { saved: items.length }); return;
  }
  if (pathname === '/api/budget/vs-actual' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const month = qs.get('month') || String(new Date().getMonth()+1).padStart(2,'0');
    const budgets = await db.prepare("SELECT * FROM budgets WHERE year=? AND month=? ORDER BY acc_code").all(year, month);
    // 실적은 gl_balance_cache에서 가져오기 시도
    const ym = year + '-' + month;
    budgets.forEach(async function(b) {
      const cache = await db.prepare("SELECT period_dr, period_cr FROM gl_balance_cache WHERE acc_code=? AND year_month=?").get(b.acc_code, ym);
      if (cache) b.actual_amount = (b.budget_type === 'expense') ? cache.period_dr : cache.period_cr;
    });
    ok(res, budgets); return;
  }
  if (pathname === '/api/budget/summary' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const year = qs.get('year') || new Date().getFullYear().toString();
    const rows = await db.prepare("SELECT month, budget_type, SUM(budget_amount) AS total_budget, SUM(actual_amount) AS total_actual FROM budgets WHERE year=? GROUP BY month, budget_type ORDER BY month").all(year);
    ok(res, rows); return;
  }
  if (pathname === '/api/cash/daily' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || new Date(Date.now()-30*86400000).toISOString().slice(0,10);
    const to = qs.get('to') || new Date().toISOString().slice(0,10);
    const rows = await db.prepare("SELECT * FROM daily_cash WHERE cash_date BETWEEN ? AND ? ORDER BY cash_date, acc_code").all(from, to);
    ok(res, rows); return;
  }
  if (pathname === '/api/cash/daily' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [body];
    const upsert = db.prepare("INSERT INTO daily_cash (cash_date,acc_code,acc_name,inflow,outflow,balance,notes) VALUES (?,?,?,?,?,?,?) ON CONFLICT(cash_date,acc_code) DO UPDATE SET inflow=excluded.inflow, outflow=excluded.outflow, balance=excluded.balance, notes=excluded.notes");
    for (const it of items) { await upsert.run(it.cash_date, it.acc_code||'', it.acc_name||'', it.inflow||0, it.outflow||0, it.balance||0, it.notes||''); }
    ok(res, { saved: items.length }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  생산실적 API (Work Order Results)
  // ════════════════════════════════════════════════════════════════════

  if (pathname.match(/^\/api\/work-orders\/(\d+)\/result$/) && method === 'POST') {
    const woid = pathname.match(/^\/api\/work-orders\/(\d+)\/result$/)[1];
    const body = await readJSON(req);
    await db.prepare("INSERT INTO work_order_results (work_order_id,result_date,good_qty,defect_qty,worker_name,work_hours,notes) VALUES (?,?,?,?,?,?,?)").run(
      woid, body.result_date||new Date().toISOString().slice(0,10), body.good_qty||0, body.defect_qty||0, body.worker_name||'', body.work_hours||0, body.notes||'');
    // 작업지시에 누적 반영
    const totals = await db.prepare("SELECT COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect FROM work_order_results WHERE work_order_id=?").get(woid);
    await db.prepare("UPDATE work_orders SET produced_qty=?, defect_qty=?, updated_at=datetime('now','localtime') WHERE wo_id=?").run(totals.good, totals.defect, woid);
    ok(res, { saved: true, total_good: totals.good, total_defect: totals.defect }); return;
  }
  if (pathname.match(/^\/api\/work-orders\/(\d+)\/results$/) && method === 'GET') {
    const woid = pathname.match(/^\/api\/work-orders\/(\d+)\/results$/)[1];
    const rows = await db.prepare("SELECT * FROM work_order_results WHERE work_order_id=? ORDER BY result_date DESC").all(woid);
    ok(res, rows); return;
  }
  if (pathname === '/api/work-orders/daily-report' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const date = qs.get('date') || new Date().toISOString().slice(0,10);
    const rows = await db.prepare("SELECT r.*, w.wo_number, w.product_name FROM work_order_results r JOIN work_orders w ON w.wo_id=r.work_order_id WHERE r.result_date=? ORDER BY r.created_at DESC").all(date);
    const summary = await db.prepare("SELECT COUNT(*) AS cnt, COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect, COALESCE(SUM(work_hours),0) AS hours FROM work_order_results WHERE result_date=?").get(date);
    ok(res, { total_good: summary.good, total_defect: summary.defect, total_hours: summary.hours, order_count: summary.cnt, results: rows }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Phase 1: 경영자 통합 대시보드 API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/exec/dashboard-full' && method === 'GET') {
    const now = new Date();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const fmtDate = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
    const s = fmtDate(monthStart), e = fmtDate(now);
    const today = now.toISOString().slice(0,10);
    const sources = { xerp: 'unknown', bar_shop1: 'unknown', dd: 'unknown', sqlite: 'ok' };

    // 1) 매출 KPI (XERP + bar_shop1)
    let salesKpi = { total_sales: 0, total_supply: 0, total_fee: 0, order_count: 0, prev_month_sales: 0 };
    let barKpi = { total_revenue: 0, total_cost: 0, order_count: 0 };
    try {
      const pool = await ensureXerpPool();
      const r = await pool.request().input('s',s).input('e',e).query(`SELECT COUNT(DISTINCT h_orderid) AS orders, ISNULL(SUM(h_sumPrice),0) AS sales, ISNULL(SUM(h_offerPrice),0) AS supply, ISNULL(SUM(FeeAmnt),0) AS fee FROM ERP_SalesData WITH(NOLOCK) WHERE h_date>=@s AND h_date<=@e`);
      const row = r.recordset[0]||{}; salesKpi.total_sales=row.sales||0; salesKpi.total_supply=row.supply||0; salesKpi.total_fee=row.fee||0; salesKpi.order_count=row.orders||0;
      const pm=new Date(now.getFullYear(),now.getMonth()-1,1); const pmEnd=new Date(now.getFullYear(),now.getMonth(),0);
      const r2=await pool.request().input('s',fmtDate(pm)).input('e',fmtDate(pmEnd)).query(`SELECT ISNULL(SUM(h_sumPrice),0) AS t FROM ERP_SalesData WITH(NOLOCK) WHERE h_date>=@s AND h_date<=@e`);
      salesKpi.prev_month_sales=(r2.recordset[0]||{}).t||0; sources.xerp='ok';
    } catch(_){ sources.xerp='error'; }
    try {
      await withBarShop1Pool(async (pool)=>{
        const r=await pool.request().input('s',s).input('e',e+' 23:59:59').query(`SELECT SUM(i.item_sale_price*i.item_count) AS rev, SUM(i.item_price*i.item_count) AS cost, COUNT(DISTINCT o.order_seq) AS cnt FROM custom_order o WITH(NOLOCK) JOIN custom_order_item i WITH(NOLOCK) ON o.order_seq=i.order_seq WHERE o.order_date>=@s AND o.order_date<=@e AND o.status_seq>=1 AND i.item_sale_price>0`);
        const row=r.recordset[0]||{}; barKpi.total_revenue=row.rev||0; barKpi.total_cost=row.cost||0; barKpi.order_count=row.cnt||0;
      }); sources.bar_shop1='ok';
    } catch(_){ sources.bar_shop1='error'; }

    // DD 매출
    let ddKpi = { order_count: 0, total_sales: 0 };
    try {
      const ddP = await ensureDdPool();
      if (ddP) {
        const sDate=s.substring(0,4)+'-'+s.substring(4,6)+'-'+s.substring(6,8);
        const eDate=e.substring(0,4)+'-'+e.substring(4,6)+'-'+e.substring(6,8);
        const [rows]=await ddP.query('SELECT COUNT(*) AS cnt, IFNULL(SUM(total_price),0) AS sales FROM orders WHERE created_at>=? AND created_at<DATE_ADD(?,INTERVAL 1 DAY) AND order_state IN ("P","D","F")',[sDate,eDate]);
        ddKpi.order_count=(rows[0]||{}).cnt||0; ddKpi.total_sales=(rows[0]||{}).sales||0; sources.dd='ok';
      }
    } catch(_){ sources.dd='error'; }

    // 법인별 매출 분리 (바른손 vs 디얼디어)
    const barunsonSales = salesKpi.total_sales + barKpi.total_revenue;  // 바른손 법인
    const ddSales = ddKpi.total_sales;                                   // 디얼디어 법인
    const totalSales = barunsonSales + ddSales;                          // 그룹 합계(참고용)
    const totalCost = barKpi.total_cost + salesKpi.total_fee;
    const grossProfit = totalSales - totalCost;
    const marginRate = totalSales > 0 ? Math.round(grossProfit/totalSales*1000)/10 : 0;
    const salesGrowth = salesKpi.prev_month_sales > 0 ? Math.round((salesKpi.total_sales - salesKpi.prev_month_sales)/salesKpi.prev_month_sales*1000)/10 : 0;

    // 2) 구매 KPI
    let poKpi = { total: 0, pending: 0, overdue: 0, this_month_amount: 0 };
    try {
      poKpi.total = (await db.prepare("SELECT COUNT(*) AS c FROM po_header").get()||{}).c||0;
      poKpi.pending = (await db.prepare("SELECT COUNT(*) AS c FROM po_header WHERE status IN ('draft','sent','confirmed','partial')").get()||{}).c||0;
      poKpi.overdue = (await db.prepare("SELECT COUNT(*) AS c FROM po_header WHERE status IN ('sent','confirmed','partial') AND due_date < ?").get(today)||{}).c||0;
      const amt = await db.prepare("SELECT COALESCE(SUM(pi.unit_price*pi.ordered_qty),0) AS t FROM po_header ph JOIN po_items pi ON ph.po_id=pi.po_id WHERE ph.po_date >= ?").get(monthStart.toISOString().slice(0,10));
      poKpi.this_month_amount = amt ? amt.t : 0;
    } catch(_){}

    // 3) 재고 KPI
    let invKpi = { total_items: 0, low_stock: 0, expiring_soon: 0, total_value: 0 };
    try {
      invKpi.total_items = (await db.prepare("SELECT COUNT(*) AS c FROM batch_master WHERE current_qty > 0").get()||{}).c||0;
      const lowRules = await db.prepare("SELECT sr.product_code, sr.min_qty FROM safety_stock_rules sr").all();
      for (const r of lowRules) {
        const stock = await db.prepare("SELECT COALESCE(SUM(current_qty),0) AS q FROM batch_master WHERE product_code=? AND quality_status='GOOD'").get(r.product_code);
        if (stock && stock.q < r.min_qty) invKpi.low_stock++;
      }
      invKpi.expiring_soon = (await db.prepare("SELECT COUNT(*) AS c FROM batch_master WHERE exp_date IS NOT NULL AND exp_date != '' AND exp_date <= date('now','+30 days') AND current_qty > 0").get()||{}).c||0;
    } catch(_){}

    // 4) 생산 KPI
    let prodKpi = { active_wo: 0, completed_wo: 0, today_good: 0, today_defect: 0, defect_rate: 0 };
    try {
      prodKpi.active_wo = (await db.prepare("SELECT COUNT(*) AS c FROM work_orders WHERE status='in_progress'").get()||{}).c||0;
      prodKpi.completed_wo = (await db.prepare("SELECT COUNT(*) AS c FROM work_orders WHERE status='completed' AND completed_date >= ?").get(monthStart.toISOString().slice(0,10))||{}).c||0;
      const todayProd = await db.prepare("SELECT COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect FROM work_order_results WHERE result_date=?").get(today);
      if (todayProd) { prodKpi.today_good=todayProd.good; prodKpi.today_defect=todayProd.defect; }
      const monthProd = await db.prepare("SELECT COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect FROM work_order_results WHERE result_date >= ?").get(monthStart.toISOString().slice(0,10));
      if (monthProd && (monthProd.good+monthProd.defect)>0) prodKpi.defect_rate = Math.round(monthProd.defect/(monthProd.good+monthProd.defect)*1000)/10;
    } catch(_){}

    // 5) 회계 KPI
    let acctKpi = { budget_total: 0, actual_total: 0, budget_exec_rate: 0, ar_total: 0, ap_total: 0 };
    try {
      const yr = String(now.getFullYear());
      const budSum = await db.prepare("SELECT COALESCE(SUM(budget_amount),0) AS b, COALESCE(SUM(actual_amount),0) AS a FROM budgets WHERE year=?").get(yr);
      if (budSum) { acctKpi.budget_total=budSum.b; acctKpi.actual_total=budSum.a; acctKpi.budget_exec_rate=budSum.b>0?Math.round(budSum.a/budSum.b*1000)/10:0; }
    } catch(_){}

    // 6) 업무 KPI
    let taskKpi = { total: 0, done: 0, in_progress: 0, overdue: 0 };
    try {
      const ts = await db.prepare("SELECT status, COUNT(*) AS c FROM tasks GROUP BY status").all();
      ts.forEach(async t => { taskKpi.total+=t.c; if(t.status==='done') taskKpi.done=t.c; if(t.status==='in_progress') taskKpi.in_progress=t.c; });
      taskKpi.overdue = (await db.prepare("SELECT COUNT(*) AS c FROM tasks WHERE status != 'done' AND due_date < ? AND due_date IS NOT NULL AND due_date != ''").get(today)||{}).c||0;
    } catch(_){}

    // 7) 결재 KPI
    let approvalKpi = { pending: 0, approved_month: 0, rejected_month: 0 };
    try {
      approvalKpi.pending = (await db.prepare("SELECT COUNT(*) AS c FROM approvals WHERE status='pending'").get()||{}).c||0;
      approvalKpi.approved_month = (await db.prepare("SELECT COUNT(*) AS c FROM approvals WHERE status='approved' AND updated_at >= ?").get(monthStart.toISOString().slice(0,10))||{}).c||0;
    } catch(_){}

    // 8) 불량 KPI
    let defectKpi = { month_count: 0, month_qty: 0, top_vendor: '' };
    try {
      const dc = await db.prepare("SELECT COUNT(*) AS c, COALESCE(SUM(defect_qty),0) AS q FROM defects WHERE created_at >= ?").get(monthStart.toISOString().slice(0,10));
      if (dc) { defectKpi.month_count=dc.c; defectKpi.month_qty=dc.q; }
      const tv = await db.prepare("SELECT vendor_name, COUNT(*) AS c FROM defects WHERE created_at >= ? GROUP BY vendor_name ORDER BY c DESC LIMIT 1").get(monthStart.toISOString().slice(0,10));
      if (tv) defectKpi.top_vendor = tv.vendor_name;
    } catch(_){}

    // 9) 제조원가 최신
    let costKpi = { avg_unit_cost: 0, total_material: 0, total_labor: 0 };
    try {
      const cc = await db.prepare("SELECT COALESCE(AVG(unit_cost),0) AS u, COALESCE(SUM(material_cost),0) AS m, COALESCE(SUM(labor_cost),0) AS l FROM mfg_cost_cards WHERE calc_date >= ?").get(monthStart.toISOString().slice(0,10));
      if (cc) { costKpi.avg_unit_cost=Math.round(cc.u); costKpi.total_material=cc.m; costKpi.total_labor=cc.l; }
    } catch(_){}

    // 10) 설비 가동률
    let eqKpi = { total: 0, active: 0, maintenance: 0, avg_oee: 0 };
    try {
      eqKpi.total = (await db.prepare("SELECT COUNT(*) AS c FROM equipment").get()||{}).c||0;
      eqKpi.active = (await db.prepare("SELECT COUNT(*) AS c FROM equipment WHERE status='active'").get()||{}).c||0;
      eqKpi.maintenance = (await db.prepare("SELECT COUNT(*) AS c FROM equipment WHERE status='maintenance'").get()||{}).c||0;
    } catch(_){}

    ok(res, {
      sources, period: { start: s, end: e, month: (now.getMonth()+1)+'월', year: now.getFullYear() },
      sales: { ...salesKpi, bar: barKpi, dd: ddKpi, total: totalSales, barunson_total: barunsonSales, dd_total: ddSales, gross_profit: grossProfit, margin_rate: marginRate, growth: salesGrowth },
      purchasing: poKpi, inventory: invKpi, production: prodKpi,
      accounting: acctKpi, tasks: taskKpi, approvals: approvalKpi,
      quality: defectKpi, cost: costKpi, equipment: eqKpi
    }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Phase 2: PDF 생성 API (견적서/거래명세서/세금계산서)
  // ════════════════════════════════════════════════════════════════════

  if (pathname.match(/^\/api\/pdf\/quotation\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/pdf\/quotation\/(\d+)$/)[1];
    const order = await db.prepare("SELECT * FROM sales_orders WHERE id=?").get(id);
    if (!order) { fail(res, 404, 'Not Found'); return; }
    const items = await db.prepare("SELECT * FROM sales_order_items WHERE order_id=?").all(id);
    ok(res, { order, items, doc_type: order.order_type === 'quote' ? '견적서' : '수주확인서',
      company: { name: '바른컴퍼니(주)', tel: '02-2103-2600', fax: '02-2103-2609', addr: '서울시 용산구 한강대로 366 트윈시티남산2' }
    }); return;
  }

  if (pathname.match(/^\/api\/pdf\/invoice\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/pdf\/invoice\/(\d+)$/)[1];
    const doc = await db.prepare("SELECT * FROM trade_document WHERE id=?").get(id);
    if (!doc) { fail(res, 404, 'Not Found'); return; }
    ok(res, { doc, doc_type: '거래명세서',
      company: { name: '바른컴퍼니(주)', tel: '02-2103-2600', fax: '02-2103-2609', addr: '서울시 용산구 한강대로 366 트윈시티남산2' }
    }); return;
  }

  if (pathname.match(/^\/api\/pdf\/tax-invoice\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/pdf\/tax-invoice\/(\d+)$/)[1];
    const inv = await db.prepare("SELECT * FROM hometax_invoices WHERE id=?").get(id);
    if (!inv) { fail(res, 404, 'Not Found'); return; }
    ok(res, { invoice: inv, doc_type: '세금계산서' }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Phase 3: 안전재고 + 재고실사 API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/safety-stock' && method === 'GET') {
    const rules = await db.prepare("SELECT * FROM safety_stock_rules ORDER BY product_code").all();
    // 현재 재고와 조인
    const result = await Promise.all(rules.map(async r => {
      const stock = await db.prepare("SELECT COALESCE(SUM(current_qty),0) AS qty FROM batch_master WHERE product_code=? AND quality_status='GOOD'").get(r.product_code);
      return { ...r, current_qty: stock ? stock.qty : 0, is_below: stock ? stock.qty < r.min_qty : false };
    }));
    ok(res, result); return;
  }

  if (pathname === '/api/safety-stock' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [body];
    const upsert = db.prepare("INSERT INTO safety_stock_rules (product_code,product_name,min_qty,reorder_qty,reorder_point,lead_time_days,warehouse,auto_po,vendor_name) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(product_code) DO UPDATE SET product_name=excluded.product_name, min_qty=excluded.min_qty, reorder_qty=excluded.reorder_qty, reorder_point=excluded.reorder_point, lead_time_days=excluded.lead_time_days, warehouse=excluded.warehouse, auto_po=excluded.auto_po, vendor_name=excluded.vendor_name, updated_at=datetime('now','localtime')");
    for (const it of items) { await upsert.run(it.product_code, it.product_name||'', it.min_qty||0, it.reorder_qty||0, it.reorder_point||0, it.lead_time_days||7, it.warehouse||'', it.auto_po?1:0, it.vendor_name||''); }
    ok(res, { saved: items.length }); return;
  }

  if (pathname === '/api/safety-stock/check' && method === 'POST') {
    const rules = await db.prepare("SELECT * FROM safety_stock_rules").all();
    const alerts = [];
    for (const r of rules) {
      const stock = await db.prepare("SELECT COALESCE(SUM(current_qty),0) AS qty FROM batch_master WHERE product_code=? AND quality_status='GOOD'").get(r.product_code);
      const qty = stock ? stock.qty : 0;
      if (qty <= r.reorder_point || qty < r.min_qty) {
        alerts.push({ product_code: r.product_code, product_name: r.product_name, current_qty: qty, min_qty: r.min_qty, reorder_point: r.reorder_point, shortage: r.min_qty - qty });
        createNotification(null, 'alert', '안전재고 부족: ' + (r.product_name||r.product_code), r.product_code + ' 현재 ' + qty + '개 (최소 ' + r.min_qty + '개)', 'safety-stock');
      }
    }
    ok(res, { checked: rules.length, alerts }); return;
  }

  if (pathname === '/api/safety-stock/import-from-xerp' && method === 'POST') {
    // XERP 재고에서 안전재고 규칙 자동 생성 (현재재고의 30%를 min_qty로 설정)
    try {
      const pool = await ensureXerpPool();
      const r = await pool.request().query(`SELECT RTRIM(ItemCode) AS code, RTRIM(ItemName) AS name, OhQty FROM mmInventory WITH(NOLOCK) WHERE SiteCode='BK10' AND OhQty > 0`);
      const upsert = db.prepare("INSERT OR IGNORE INTO safety_stock_rules (product_code,product_name,min_qty,reorder_qty,reorder_point) VALUES (?,?,?,?,?)");
      let cnt = 0;
      for (const row of r.recordset||[]) {
        const minQ = Math.max(1, Math.round(row.OhQty * 0.3));
        await upsert.run(row.code, row.name, minQ, Math.round(row.OhQty * 0.5), Math.round(row.OhQty * 0.4));
        cnt++;
      }
      ok(res, { imported: cnt }); return;
    } catch(e) { fail(res, 503, 'XERP 연결 실패: ' + e.message); return; }
  }

  // 재고실사
  if (pathname === '/api/cycle-count' && method === 'GET') {
    const plans = await db.prepare("SELECT * FROM cycle_count_plans ORDER BY created_at DESC").all();
    const result = await Promise.all(plans.map(async p => {
      const items = await db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN counted_qty IS NOT NULL THEN 1 ELSE 0 END) AS counted, SUM(ABS(variance)) AS total_variance FROM cycle_count_items WHERE plan_id=?").get(p.id);
      return { ...p, item_count: items.total||0, counted_count: items.counted||0, total_variance: items.total_variance||0 };
    }));
    ok(res, result); return;
  }

  if (pathname === '/api/cycle-count' && method === 'POST') {
    const body = await readJSON(req);
    const planNo = 'CC-' + new Date().toISOString().slice(0,10).replace(/-/g,'') + '-' + String(Math.floor(Math.random()*900)+100);
    const r = await db.prepare("INSERT INTO cycle_count_plans (plan_no,plan_date,warehouse,note,created_by) VALUES (?,?,?,?,?)").run(planNo, body.plan_date||new Date().toISOString().slice(0,10), body.warehouse||'', body.note||'', body.created_by||'');
    const planId = r.lastInsertRowid;
    // 아이템 자동 생성: 해당 창고의 모든 재고품목
    const stocks = await db.prepare("SELECT product_code, product_name, COALESCE(SUM(current_qty),0) AS sys_qty FROM batch_master WHERE current_qty > 0 " + (body.warehouse ? "AND warehouse=?" : "") + " GROUP BY product_code").all(...(body.warehouse ? [body.warehouse] : []));
    const ins = db.prepare("INSERT INTO cycle_count_items (plan_id,product_code,product_name,system_qty) VALUES (?,?,?,?)");
    for (const s of stocks) { await ins.run(planId, s.product_code, s.product_name, s.sys_qty); }
    ok(res, { plan_id: planId, plan_no: planNo, items: stocks.length }); return;
  }

  if (pathname.match(/^\/api\/cycle-count\/(\d+)$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/cycle-count\/(\d+)$/)[1];
    const plan = await db.prepare("SELECT * FROM cycle_count_plans WHERE id=?").get(id);
    if (!plan) { fail(res, 404, 'Not Found'); return; }
    const items = await db.prepare("SELECT * FROM cycle_count_items WHERE plan_id=? ORDER BY product_code").all(id);
    ok(res, { ...plan, items }); return;
  }

  if (pathname.match(/^\/api\/cycle-count\/(\d+)\/count$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/cycle-count\/(\d+)\/count$/)[1];
    const body = await readJSON(req);
    const items = body.items || [];
    const upd = db.prepare("UPDATE cycle_count_items SET counted_qty=?, variance=?-system_qty, note=? WHERE plan_id=? AND product_code=?");
    for (const it of items) { await upd.run(it.counted_qty, it.counted_qty, it.note||'', id, it.product_code); }
    ok(res, { updated: items.length }); return;
  }

  if (pathname.match(/^\/api\/cycle-count\/(\d+)\/complete$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/cycle-count\/(\d+)\/complete$/)[1];
    const items = await db.prepare("SELECT * FROM cycle_count_items WHERE plan_id=? AND counted_qty IS NOT NULL AND variance != 0").all(id);
    // 재고 조정 반영
    for (const it of items) {
      const batch = await db.prepare("SELECT batch_id, current_qty FROM batch_master WHERE product_code=? AND quality_status='GOOD' ORDER BY received_date DESC LIMIT 1").get(it.product_code);
      if (batch) {
        const newQty = batch.current_qty + it.variance;
        await db.prepare("UPDATE batch_master SET current_qty=?, updated_at=datetime('now','localtime') WHERE batch_id=?").run(Math.max(0, newQty), batch.batch_id);
        await db.prepare("INSERT INTO batch_transactions (batch_id,batch_number,txn_type,txn_date,product_code,qty,qty_before,qty_after,reference_no,actor,notes) VALUES (?,?,?,?,?,?,?,?,?,?,?)").run(
          batch.batch_id, '', 'adjust', new Date().toISOString().slice(0,10), it.product_code, Math.abs(it.variance), batch.current_qty, Math.max(0, newQty), 'CC-'+id, '', '재고실사 조정');
        await db.prepare("UPDATE cycle_count_items SET adjusted=1 WHERE id=?").run(it.id);
      }
    }
    await db.prepare("UPDATE cycle_count_plans SET status='completed', completed_at=datetime('now','localtime') WHERE id=?").run(id);
    createNotification(null, 'system', '재고실사 완료', 'CC-'+id+' 실사 완료, '+items.length+'건 조정', 'cycle-count');
    ok(res, { adjusted: items.length }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Phase 4: 제조원가 계산 API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/mfg-cost/rates' && method === 'GET') {
    const rates = await db.prepare("SELECT * FROM cost_rates ORDER BY rate_type, rate_key").all();
    ok(res, rates); return;
  }

  if (pathname === '/api/mfg-cost/rates' && method === 'POST') {
    const body = await readJSON(req);
    const items = body.items || [body];
    const upsert = db.prepare("INSERT INTO cost_rates (rate_type,rate_key,rate_value,unit,notes) VALUES (?,?,?,?,?) ON CONFLICT(rate_type,rate_key) DO UPDATE SET rate_value=excluded.rate_value, unit=excluded.unit, notes=excluded.notes, updated_at=NOW()");
    for (const it of items) { await upsert.run(it.rate_type, it.rate_key, it.rate_value||0, it.unit||'', it.notes||''); }
    ok(res, { saved: items.length }); return;
  }

  if (pathname.match(/^\/api\/mfg-cost\/calculate\/(.+)$/) && method === 'POST') {
    const productCode = decodeURIComponent(pathname.match(/^\/api\/mfg-cost\/calculate\/(.+)$/)[1]);
    const body = await readJSON(req);
    const qty = body.qty || 1;

    // BOM에서 재료비 계산
    const bom = await db.prepare("SELECT * FROM bom_header WHERE product_code=?").get(productCode);
    if (!bom) { fail(res, 404, 'BOM not found for ' + productCode); return; }
    const bomItems = await db.prepare("SELECT * FROM bom_items WHERE bom_id=?").all(bom.bom_id);

    let materialCost = 0, outsourceCost = 0;
    const materialDetails = [], processDetails = [];
    for (const item of bomItems) {
      if (item.item_type === 'material') {
        // material_prices에서 최신 단가 조회
        const price = await db.prepare("SELECT apply_price FROM material_prices WHERE product_code=? ORDER BY apply_month DESC LIMIT 1").get(item.material_code || item.product_code);
        const unitPrice = price ? price.apply_price : 0;
        const cost = unitPrice * (item.qty_per || 1) * qty;
        materialCost += cost;
        materialDetails.push({ code: item.material_code||item.product_code, name: item.material_name, qty_per: item.qty_per, unit_price: unitPrice, cost });
      } else if (item.item_type === 'process') {
        // post_process_price에서 단가 조회
        const price = await db.prepare("SELECT unit_price FROM post_process_price WHERE vendor_name=? AND process_type=? ORDER BY effective_from DESC LIMIT 1").get(item.vendor_name||'', item.process_type||'');
        const unitPrice = price ? price.unit_price : 0;
        const cost = unitPrice * (item.qty_per || 1) * qty;
        outsourceCost += cost;
        processDetails.push({ process: item.process_type, vendor: item.vendor_name, qty_per: item.qty_per, unit_price: unitPrice, cost });
      }
    }

    // 노무비: work_order_results에서 실적 기반 또는 표준
    const laborRate = (await db.prepare("SELECT rate_value FROM cost_rates WHERE rate_type='labor' AND rate_key='default'").get()||{}).rate_value || 25000;
    const overheadRate = (await db.prepare("SELECT rate_value FROM cost_rates WHERE rate_type='overhead' AND rate_key='rate'").get()||{}).rate_value || 15;

    // 최근 작업지시의 실제 노무시간 기반
    const woResult = await db.prepare("SELECT COALESCE(SUM(work_hours),0) AS hours, COALESCE(SUM(good_qty),0) AS good FROM work_order_results r JOIN work_orders w ON w.wo_id=r.work_order_id WHERE w.product_code=? ORDER BY r.result_date DESC LIMIT 10").get(productCode);
    let laborCost = 0;
    if (woResult && woResult.good > 0) {
      const hoursPerUnit = woResult.hours / woResult.good;
      laborCost = hoursPerUnit * laborRate * qty;
    } else {
      laborCost = laborRate * 0.5 * qty; // 기본 30분/개
    }

    const overheadCost = (materialCost + laborCost) * overheadRate / 100;
    const totalCost = materialCost + laborCost + overheadCost + outsourceCost;
    const unitCost = qty > 0 ? Math.round(totalCost / qty) : 0;

    // 저장
    const calcDate = new Date().toISOString().slice(0,10);
    await db.prepare("INSERT INTO mfg_cost_cards (product_code,product_name,calc_date,material_cost,labor_cost,overhead_cost,outsource_cost,total_cost,unit_cost,qty,source) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(product_code,calc_date) DO UPDATE SET material_cost=excluded.material_cost, labor_cost=excluded.labor_cost, overhead_cost=excluded.overhead_cost, outsource_cost=excluded.outsource_cost, total_cost=excluded.total_cost, unit_cost=excluded.unit_cost, qty=excluded.qty").run(
      productCode, bom.product_name||'', calcDate, materialCost, laborCost, overheadCost, outsourceCost, totalCost, unitCost, qty, 'auto');

    ok(res, {
      product_code: productCode, product_name: bom.product_name, qty, calc_date: calcDate,
      material: { total: materialCost, details: materialDetails },
      labor: { total: laborCost, rate: laborRate, hours_per_unit: woResult && woResult.good > 0 ? woResult.hours/woResult.good : 0.5 },
      overhead: { total: overheadCost, rate: overheadRate },
      outsource: { total: outsourceCost, details: processDetails },
      total_cost: totalCost, unit_cost: unitCost
    }); return;
  }

  if (pathname === '/api/mfg-cost/cards' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || new Date(Date.now()-90*86400000).toISOString().slice(0,10);
    const cards = await db.prepare("SELECT * FROM mfg_cost_cards WHERE calc_date >= ? ORDER BY calc_date DESC, product_code").all(from);
    ok(res, cards); return;
  }

  if (pathname === '/api/mfg-cost/summary' && method === 'GET') {
    const latest = await db.prepare(`SELECT product_code, MAX(product_name) AS product_name, MAX(calc_date) AS calc_date,
      MAX(material_cost) AS material_cost, MAX(labor_cost) AS labor_cost, MAX(overhead_cost) AS overhead_cost,
      MAX(outsource_cost) AS outsource_cost, MAX(total_cost) AS total_cost, MAX(unit_cost) AS unit_cost, MAX(qty) AS qty
      FROM mfg_cost_cards GROUP BY product_code ORDER BY MAX(product_name)`).all();
    const totals = await db.prepare(`SELECT COALESCE(SUM(material_cost),0) AS material, COALESCE(SUM(labor_cost),0) AS labor,
      COALESCE(SUM(overhead_cost),0) AS overhead, COALESCE(SUM(outsource_cost),0) AS outsource, COALESCE(SUM(total_cost),0) AS total
      FROM (SELECT product_code, MAX(material_cost) AS material_cost, MAX(labor_cost) AS labor_cost, MAX(overhead_cost) AS overhead_cost, MAX(outsource_cost) AS outsource_cost, MAX(total_cost) AS total_cost FROM mfg_cost_cards
      WHERE calc_date = (SELECT MAX(calc_date) FROM mfg_cost_cards c2 WHERE c2.product_code=mfg_cost_cards.product_code) GROUP BY product_code) sub`).get();
    ok(res, { cards: latest, totals: totals||{} }); return;
  }

  if (pathname === '/api/mfg-cost/calculate-all' && method === 'POST') {
    // 모든 BOM 제품의 원가를 일괄 계산
    const boms = await db.prepare("SELECT product_code, product_name FROM bom_header").all();
    let calculated = 0;
    const laborRate = (await db.prepare("SELECT rate_value FROM cost_rates WHERE rate_type='labor' AND rate_key='default'").get()||{}).rate_value || 25000;
    const overheadRate = (await db.prepare("SELECT rate_value FROM cost_rates WHERE rate_type='overhead' AND rate_key='rate'").get()||{}).rate_value || 15;
    const calcDate = new Date().toISOString().slice(0,10);

    const tx = db.transaction(async () => {
      for (const bom of boms) {
        const items = await db.prepare("SELECT * FROM bom_items WHERE bom_id=(SELECT bom_id FROM bom_header WHERE product_code=?)").all(bom.product_code);
        let mat = 0, out = 0;
        for (const it of items) {
          if (it.item_type === 'material') {
            const p = await db.prepare("SELECT apply_price FROM material_prices WHERE product_code=? ORDER BY apply_month DESC LIMIT 1").get(it.material_code||it.product_code);
            mat += (p ? p.apply_price : 0) * (it.qty_per||1);
          } else if (it.item_type === 'process') {
            const p = await db.prepare("SELECT unit_price FROM post_process_price WHERE vendor_name=? AND process_type=? ORDER BY effective_from DESC LIMIT 1").get(it.vendor_name||'', it.process_type||'');
            out += (p ? p.unit_price : 0) * (it.qty_per||1);
          }
        }
        const wo = await db.prepare("SELECT COALESCE(SUM(work_hours),0) AS h, COALESCE(SUM(good_qty),0) AS g FROM work_order_results r JOIN work_orders w ON w.wo_id=r.work_order_id WHERE w.product_code=?").get(bom.product_code);
        const lab = wo && wo.g > 0 ? (wo.h/wo.g)*laborRate : laborRate*0.5;
        const oh = (mat+lab)*overheadRate/100;
        const total = mat+lab+oh+out;
        await db.prepare("INSERT INTO mfg_cost_cards (product_code,product_name,calc_date,material_cost,labor_cost,overhead_cost,outsource_cost,total_cost,unit_cost,qty,source) VALUES (?,?,?,?,?,?,?,?,?,1,'batch') ON CONFLICT(product_code,calc_date) DO UPDATE SET material_cost=excluded.material_cost, labor_cost=excluded.labor_cost, overhead_cost=excluded.overhead_cost, outsource_cost=excluded.outsource_cost, total_cost=excluded.total_cost, unit_cost=excluded.unit_cost").run(
          bom.product_code, bom.product_name, calcDate, mat, lab, oh, out, total, Math.round(total));
        calculated++;
      }
    });
    await tx();
    ok(res, { calculated }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Phase 5: RBAC 권한관리 API
  // ════════════════════════════════════════════════════════════════════

  if (pathname === '/api/rbac/roles' && method === 'GET') {
    const roles = (await db.prepare("SELECT DISTINCT role FROM role_permissions ORDER BY role").all()).map(r => r.role);
    const result = await Promise.all(roles.map(async role => {
      const perms = await db.prepare("SELECT permission, resource FROM role_permissions WHERE role=? AND granted=1").all(role);
      const userCount = (await db.prepare("SELECT COUNT(*) AS c FROM users WHERE role=?").get(role)||{}).c||0;
      return { role, permissions: perms, user_count: userCount };
    }));
    ok(res, result); return;
  }

  if (pathname === '/api/rbac/roles' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.role) { fail(res, 400, 'role 필수'); return; }
    // 기존 권한 삭제 후 재생성
    await db.prepare("DELETE FROM role_permissions WHERE role=?").run(body.role);
    const ins = db.prepare("INSERT INTO role_permissions (role,permission,resource) VALUES (?,?,?)");
    for (const p of (body.permissions || [])) { await ins.run(body.role, p.permission||'read', p.resource||'*'); }
    ok(res, { saved: true }); return;
  }

  if (pathname === '/api/rbac/permissions' && method === 'GET') {
    // 모든 가능한 권한 목록
    const resources = ALL_PAGES.map(p => p.id);
    const permissions = ['read', 'write', 'delete', 'approve', 'export'];
    ok(res, { resources, permissions }); return;
  }

  if (pathname === '/api/rbac/user-permissions' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const userId = qs.get('user_id');
    if (!userId) { fail(res, 400, 'user_id 필수'); return; }
    const user = await db.prepare("SELECT user_id, username, display_name, role FROM users WHERE user_id=?").get(userId);
    if (!user) { fail(res, 404, 'User not found'); return; }
    const perms = await db.prepare("SELECT permission, resource FROM role_permissions WHERE role=? AND granted=1").all(user.role);
    ok(res, { user, permissions: perms }); return;
  }

  if (pathname === '/api/rbac/check' && method === 'POST') {
    const body = await readJSON(req);
    const perms = await db.prepare("SELECT * FROM role_permissions WHERE role=? AND granted=1 AND (resource=? OR resource='*') AND (permission=? OR permission='*')").get(body.role||'', body.resource||'', body.permission||'read');
    ok(res, { allowed: !!perms }); return;
  }

  // ════════════════════════════════════════════════════════════════════
  //  Phase 6: 공정 라우팅 + 설비관리 API
  // ════════════════════════════════════════════════════════════════════

  // 공정 라우팅
  if (pathname === '/api/process-routing' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const productCode = qs.get('product_code');
    if (productCode) {
      const routes = await db.prepare("SELECT r.*, e.eq_name FROM process_routing r LEFT JOIN equipment e ON e.id=r.equipment_id WHERE r.product_code=? ORDER BY r.step_no").all(productCode);
      ok(res, routes);
    } else {
      const all = await db.prepare("SELECT product_code, COUNT(*) AS step_count, GROUP_CONCAT(process_name,' → ') AS flow FROM process_routing GROUP BY product_code ORDER BY product_code").all();
      ok(res, all);
    }
    return;
  }

  if (pathname === '/api/process-routing' && method === 'POST') {
    const body = await readJSON(req);
    if (!body.product_code || !body.steps) { fail(res, 400, 'product_code, steps 필수'); return; }
    await db.prepare("DELETE FROM process_routing WHERE product_code=?").run(body.product_code);
    const ins = db.prepare("INSERT INTO process_routing (product_code,step_no,process_name,process_type,equipment_id,vendor_name,std_time_min,setup_time_min,notes) VALUES (?,?,?,?,?,?,?,?,?)");
    for (let i = 0; i < body.steps.length; i++) { const s = body.steps[i]; await ins.run(body.product_code, i+1, s.process_name, s.process_type||'internal', s.equipment_id||null, s.vendor_name||'', s.std_time_min||0, s.setup_time_min||0, s.notes||''); }
    ok(res, { saved: body.steps.length }); return;
  }

  if (pathname === '/api/process-routing/import-from-bom' && method === 'POST') {
    // BOM의 공정 항목에서 자동으로 라우팅 생성
    const boms = await db.prepare("SELECT DISTINCT bom_id, product_code FROM bom_header").all();
    let imported = 0;
    const ins = db.prepare("INSERT OR IGNORE INTO process_routing (product_code,step_no,process_name,process_type,vendor_name,std_time_min) VALUES (?,?,?,?,?,?)");
    for (const b of boms) {
      const procs = await db.prepare("SELECT * FROM bom_items WHERE bom_id=? AND item_type='process' ORDER BY sort_order").all(b.bom_id);
      procs.forEach(async (p,i) => {
        await ins.run(b.product_code, i+1, p.process_type||p.material_name||'공정'+(i+1), p.vendor_name?'outsource':'internal', p.vendor_name||'', 30);
        imported++;
      });
    }
    ok(res, { imported }); return;
  }

  // 공정별 실적
  if (pathname === '/api/process-results' && method === 'POST') {
    const body = await readJSON(req);
    await db.prepare("INSERT INTO process_results (wo_id,routing_id,step_no,process_name,equipment_id,start_time,end_time,good_qty,defect_qty,worker_name,status,notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)").run(
      body.wo_id, body.routing_id||null, body.step_no, body.process_name, body.equipment_id||null, body.start_time||'', body.end_time||'', body.good_qty||0, body.defect_qty||0, body.worker_name||'', body.status||'completed', body.notes||'');

    // 설비 가동 로그 자동 생성
    if (body.equipment_id && body.start_time && body.end_time) {
      const start = new Date(body.start_time); const end = new Date(body.end_time);
      const duration = (end - start) / 60000;
      if (duration > 0) {
        await db.prepare("INSERT INTO equipment_logs (equipment_id,log_date,log_type,start_time,end_time,duration_min,reason,worker_name) VALUES (?,?,?,?,?,?,?,?)").run(
          body.equipment_id, (body.start_time||'').slice(0,10), 'production', body.start_time, body.end_time, duration, body.process_name||'', body.worker_name||'');
      }
    }

    // 해당 WO의 공정 진행률 업데이트
    const routing = await db.prepare("SELECT COUNT(*) AS total FROM process_routing WHERE product_code=(SELECT product_code FROM work_orders WHERE wo_id=?)").get(body.wo_id);
    const completed = await db.prepare("SELECT COUNT(DISTINCT step_no) AS done FROM process_results WHERE wo_id=? AND status='completed'").get(body.wo_id);
    if (routing && completed && routing.total > 0 && completed.done >= routing.total) {
      // 모든 공정 완료 → 작업지시 완료 처리
      const totalGood = (await db.prepare("SELECT MIN(good_qty) AS g FROM process_results WHERE wo_id=? AND status='completed'").get(body.wo_id)||{}).g||0;
      await db.prepare("UPDATE work_orders SET status='completed', produced_qty=?, completed_date=datetime('now','localtime'), updated_at=datetime('now','localtime') WHERE wo_id=?").run(totalGood, body.wo_id);
      createNotification(null, 'system', '작업지시 완료', 'WO-'+body.wo_id+' 모든 공정 완료', 'work-order');
    }
    ok(res, { saved: true }); return;
  }

  if (pathname === '/api/process-results' && method === 'GET') {
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const woId = qs.get('wo_id');
    if (woId) {
      const results = await db.prepare("SELECT pr.*, e.eq_name FROM process_results pr LEFT JOIN equipment e ON e.id=pr.equipment_id WHERE pr.wo_id=? ORDER BY pr.step_no").all(woId);
      ok(res, results);
    } else {
      const results = await db.prepare("SELECT pr.*, w.wo_number, w.product_name, e.eq_name FROM process_results pr JOIN work_orders w ON w.wo_id=pr.wo_id LEFT JOIN equipment e ON e.id=pr.equipment_id ORDER BY pr.created_at DESC LIMIT 100").all();
      ok(res, results);
    }
    return;
  }

  // 설비 관리
  if (pathname === '/api/equipment' && method === 'GET') {
    const eqs = await db.prepare("SELECT * FROM equipment ORDER BY eq_code").all();
    // 각 설비의 이번달 가동 통계
    const monthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0,10);
    const result = await Promise.all(eqs.map(async eq => {
      const logs = await db.prepare("SELECT log_type, COALESCE(SUM(duration_min),0) AS total_min FROM equipment_logs WHERE equipment_id=? AND log_date >= ? GROUP BY log_type").all(eq.id, monthStart);
      const stats = {};
      logs.forEach(l => stats[l.log_type] = l.total_min);
      const prodMin = stats.production || 0;
      const downMin = stats.downtime || 0;
      const maintMin = stats.maintenance || 0;
      const totalMin = prodMin + downMin + maintMin;
      const availability = totalMin > 0 ? Math.round(prodMin / totalMin * 1000) / 10 : 0;
      return { ...eq, stats: { production_min: prodMin, downtime_min: downMin, maintenance_min: maintMin, availability } };
    }));
    ok(res, result); return;
  }

  if (pathname === '/api/equipment' && method === 'POST') {
    const body = await readJSON(req);
    if (body.id) {
      await db.prepare("UPDATE equipment SET eq_name=?,eq_type=?,location=?,status=?,manufacturer=?,model=?,capacity_per_hour=?,notes=?,updated_at=datetime('now','localtime') WHERE id=?").run(
        body.eq_name, body.eq_type||'', body.location||'', body.status||'active', body.manufacturer||'', body.model||'', body.capacity_per_hour||0, body.notes||'', body.id);
      ok(res, { updated: true }); return;
    }
    const code = body.eq_code || 'EQ-' + String(Date.now()).slice(-6);
    await db.prepare("INSERT INTO equipment (eq_code,eq_name,eq_type,location,status,purchase_date,manufacturer,model,capacity_per_hour,notes) VALUES (?,?,?,?,?,?,?,?,?,?)").run(
      code, body.eq_name||'', body.eq_type||'', body.location||'', body.status||'active', body.purchase_date||'', body.manufacturer||'', body.model||'', body.capacity_per_hour||0, body.notes||'');
    ok(res, { created: true, eq_code: code }); return;
  }

  if (pathname.match(/^\/api\/equipment\/(\d+)\/oee$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/equipment\/(\d+)\/oee$/)[1];
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || new Date(Date.now()-30*86400000).toISOString().slice(0,10);
    const to = qs.get('to') || new Date().toISOString().slice(0,10);
    const eq = await db.prepare("SELECT * FROM equipment WHERE id=?").get(id);
    if (!eq) { fail(res, 404, 'Not Found'); return; }

    // OEE = Availability × Performance × Quality
    const logs = await db.prepare("SELECT log_type, COALESCE(SUM(duration_min),0) AS mins FROM equipment_logs WHERE equipment_id=? AND log_date BETWEEN ? AND ? GROUP BY log_type").all(id, from, to);
    const logMap = {}; logs.forEach(l => logMap[l.log_type] = l.mins);
    const prodMin = logMap.production || 0;
    const downMin = logMap.downtime || 0;
    const plannedMin = prodMin + downMin + (logMap.maintenance || 0);
    const availability = plannedMin > 0 ? prodMin / plannedMin : 0;

    // Performance: 실제 생산량 vs 이론적 최대 생산량
    const results = await db.prepare("SELECT COALESCE(SUM(good_qty),0) AS good, COALESCE(SUM(defect_qty),0) AS defect FROM process_results WHERE equipment_id=? AND created_at >= ? AND created_at <= ?").get(id, from, to + ' 23:59:59');
    const totalProd = (results.good||0) + (results.defect||0);
    const maxProd = eq.capacity_per_hour > 0 ? eq.capacity_per_hour * (prodMin / 60) : totalProd || 1;
    const performance = maxProd > 0 ? Math.min(1, totalProd / maxProd) : 0;

    // Quality
    const quality = totalProd > 0 ? (results.good||0) / totalProd : 1;

    const oee = Math.round(availability * performance * quality * 1000) / 10;

    // 일별 추이
    const daily = await db.prepare("SELECT log_date, log_type, SUM(duration_min) AS mins FROM equipment_logs WHERE equipment_id=? AND log_date BETWEEN ? AND ? GROUP BY log_date, log_type ORDER BY log_date").all(id, from, to);

    ok(res, {
      equipment: eq, period: { from, to },
      oee, availability: Math.round(availability*1000)/10, performance: Math.round(performance*1000)/10, quality: Math.round(quality*1000)/10,
      production: { good: results.good||0, defect: results.defect||0, total: totalProd, max_capacity: Math.round(maxProd) },
      time: { production_min: prodMin, downtime_min: downMin, maintenance_min: logMap.maintenance||0, planned_min: plannedMin },
      daily
    }); return;
  }

  if (pathname.match(/^\/api\/equipment\/(\d+)\/log$/) && method === 'POST') {
    const id = pathname.match(/^\/api\/equipment\/(\d+)\/log$/)[1];
    const body = await readJSON(req);
    await db.prepare("INSERT INTO equipment_logs (equipment_id,log_date,log_type,start_time,end_time,duration_min,reason,worker_name,notes) VALUES (?,?,?,?,?,?,?,?,?)").run(
      id, body.log_date||new Date().toISOString().slice(0,10), body.log_type||'downtime', body.start_time||'', body.end_time||'', body.duration_min||0, body.reason||'', body.worker_name||'', body.notes||'');
    // 설비 상태 자동 업데이트
    if (body.log_type === 'maintenance') {
      await db.prepare("UPDATE equipment SET status='maintenance', updated_at=datetime('now','localtime') WHERE id=?").run(id);
    } else if (body.log_type === 'production') {
      await db.prepare("UPDATE equipment SET status='active', updated_at=datetime('now','localtime') WHERE id=?").run(id);
    }
    ok(res, { saved: true }); return;
  }

  if (pathname.match(/^\/api\/equipment\/(\d+)\/logs$/) && method === 'GET') {
    const id = pathname.match(/^\/api\/equipment\/(\d+)\/logs$/)[1];
    const qs = new URL(req.url, 'http://localhost').searchParams;
    const from = qs.get('from') || new Date(Date.now()-30*86400000).toISOString().slice(0,10);
    const logs = await db.prepare("SELECT * FROM equipment_logs WHERE equipment_id=? AND log_date >= ? ORDER BY log_date DESC, start_time DESC").all(id, from);
    ok(res, logs); return;
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
      const tx = db.transaction(async () => {
        for (const row of result.recordset) {
          const code = row.acc_code.trim();
          if (!code) continue;
          const cls = classifyAccount(code);
          await upsert.run(code, cls.acc_name, cls.acc_type, cls.acc_group, cls.parent_code, cls.depth, cls.sort_order);
          seeded++;
        }
      });
      await tx();
    } catch (e) {
      sources.xerp = 'error: ' + e.message;
    }
    const total = (await db.prepare('SELECT COUNT(*) AS cnt FROM gl_account_map').get()).cnt;
    ok(res, { seeded, total, sources }); return;
  }

  // ── GET /api/acct/accounts ── 계정과목 트리 (SQLite)
  if (pathname === '/api/acct/accounts' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    const accounts = await db.prepare(`SELECT acc_code, acc_name, acc_type, acc_group, parent_code, depth, sort_order, is_active
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
      await db.prepare(`UPDATE gl_account_map SET acc_name=?, updated_at=datetime('now','localtime') WHERE acc_code=?`).run(body.acc_name, code);
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
    (await db.prepare('SELECT acc_code, acc_name, acc_type, acc_group FROM gl_account_map').all()).forEach(a => { accMap[a.acc_code] = a; });
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
    (await db.prepare('SELECT acc_code, acc_name FROM gl_account_map').all()).forEach(a => { accMap[a.acc_code] = a.acc_name; });
    const csMap = {};
    (await db.prepare('SELECT cs_code, cs_name FROM cs_code_cache').all()).forEach(c => { csMap[c.cs_code] = c.cs_name; });
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
    const accInfo = await db.prepare('SELECT acc_type, acc_name FROM gl_account_map WHERE acc_code=?').get(acc) || {};
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
    await db.prepare('SELECT acc_code, acc_name, acc_type, acc_group, sort_order FROM gl_account_map').all()
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
    const txBal = db.transaction(async () => {
      const ym = year + String(month).padStart(2, '0');
      for (const r of rows) { await upsertBal.run(r.acc_code, ym, r.opening_dr, r.opening_cr, r.period_dr, r.period_cr, r.closing_dr, r.closing_cr); }
    });
    try { await txBal(); } catch (e) { /* 캐시 저장 실패는 무시 */ }
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
    await db.prepare('SELECT acc_code, acc_name, acc_type, acc_group, sort_order FROM gl_account_map').all()
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

  // ── POST /api/acct/journal-entry ── 수동 분개 생성
  if (pathname === '/api/acct/journal-entry' && method === 'POST') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    try {
      const body = await readJSON(req);
      const { entry_date, description, lines } = body;
      if (!entry_date) { fail(res, 400, '전표일자를 입력하세요'); return; }
      if (!lines || !Array.isArray(lines) || lines.length === 0) { fail(res, 400, '분개 라인을 입력하세요'); return; }
      let totalDebit = 0, totalCredit = 0;
      for (const ln of lines) {
        if (!ln.acc_code) { fail(res, 400, '계정코드가 누락된 라인이 있습니다'); return; }
        totalDebit += (parseFloat(ln.debit) || 0);
        totalCredit += (parseFloat(ln.credit) || 0);
      }
      if (Math.abs(totalDebit - totalCredit) > 0.5) { fail(res, 400, '차변합계와 대변합계가 일치하지 않습니다 (차변: ' + totalDebit + ', 대변: ' + totalCredit + ')'); return; }
      // entry_no 생성: JE-YYYYMMDD-NNN (트랜잭션으로 보호)
      const dateStr = entry_date.replace(/-/g, '');
      const prefix = 'JE-' + dateStr + '-';
      const createEntry = db.transaction(async () => {
        const last = await db.prepare("SELECT entry_no FROM journal_entries WHERE entry_no LIKE ? ORDER BY entry_no DESC LIMIT 1").get(prefix + '%');
        let seq = 1;
        if (last && last.entry_no) {
          const parts = last.entry_no.split('-');
          seq = parseInt(parts[parts.length - 1], 10) + 1;
        }
        const entry_no = prefix + String(seq).padStart(3, '0');
        const ins = db.prepare("INSERT INTO journal_entries (entry_no, entry_date, description, total_amount, status, created_by) VALUES (?,?,?,?,?,?)");
        const result = await ins.run(entry_no, entry_date, description || '', totalDebit, 'posted', decoded.name || decoded.username || '');
        const entryId = result.lastInsertRowid;
        const insLine = db.prepare("INSERT INTO journal_entry_lines (entry_id, line_no, acc_code, acc_name, debit, credit, description) VALUES (?,?,?,?,?,?,?)");
        for (let i = 0; i < lines.length; i++) {
          const ln = lines[i];
          await insLine.run(entryId, i + 1, ln.acc_code, ln.acc_name || '', parseFloat(ln.debit) || 0, parseFloat(ln.credit) || 0, ln.description || '');
        }
        return { id: entryId, entry_no, total_amount: totalDebit };
      });
      const entryResult = await createEntry();
      ok(res, entryResult); return;
    } catch (e) { fail(res, 500, '수동 분개 생성 실패: ' + e.message); return; }
  }

  // ── GET /api/acct/journal-entries ── 수동 분개 목록
  if (pathname === '/api/acct/journal-entries' && method === 'GET') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    try {
      const qs = new URL(req.url, 'http://localhost').searchParams;
      const from = qs.get('from') || '';
      const to = qs.get('to') || '';
      let sql = "SELECT e.*, (SELECT COUNT(*) FROM journal_entry_lines WHERE entry_id=e.id) AS line_count FROM journal_entries e WHERE 1=1";
      const params = [];
      if (from) { sql += " AND e.entry_date >= ?"; params.push(from); }
      if (to) { sql += " AND e.entry_date <= ?"; params.push(to); }
      sql += " ORDER BY e.entry_date DESC, e.id DESC";
      const entries = await db.prepare(sql).all(...params);
      // 각 entry에 lines 포함 (N+1 방지: 한 번에 모든 lines 조회 후 그룹핑)
      if (entries.length > 0) {
        const entryIds = entries.map(e => e.id);
        const placeholders = entryIds.map(() => '?').join(',');
        const allLines = await db.prepare("SELECT * FROM journal_entry_lines WHERE entry_id IN (" + placeholders + ") ORDER BY entry_id, line_no").all(...entryIds);
        const linesMap = {};
        for (const ln of allLines) {
          if (!linesMap[ln.entry_id]) linesMap[ln.entry_id] = [];
          linesMap[ln.entry_id].push(ln);
        }
        for (const e of entries) { e.lines = linesMap[e.id] || []; }
      } else {
        for (const e of entries) { e.lines = []; }
      }
      ok(res, { entries }); return;
    } catch (e) { fail(res, 500, '수동 분개 조회 실패: ' + e.message); return; }
  }

  // ── DELETE /api/acct/journal-entries/:id ── 수동 분개 삭제
  if (pathname.match(/^\/api\/acct\/journal-entries\/(\d+)$/) && method === 'DELETE') {
    const token = extractToken(req); const decoded = token ? verifyToken(token) : null;
    if (!decoded) { fail(res, 401, '인증 필요'); return; }
    try {
      const id = parseInt(pathname.match(/^\/api\/acct\/journal-entries\/(\d+)$/)[1], 10);
      const entry = await db.prepare("SELECT * FROM journal_entries WHERE id=?").get(id);
      if (!entry) { fail(res, 404, '분개 전표를 찾을 수 없습니다'); return; }
      if (entry.status !== 'posted') { fail(res, 400, '삭제할 수 없는 상태입니다: ' + entry.status); return; }
      await db.prepare("DELETE FROM journal_entry_lines WHERE entry_id=?").run(id);
      await db.prepare("DELETE FROM journal_entries WHERE id=?").run(id);
      ok(res, { deleted: id }); return;
    } catch (e) { fail(res, 500, '수동 분개 삭제 실패: ' + e.message); return; }
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
  const acceptEncoding = (req && req.headers && req.headers['accept-encoding']) || '';
  if (acceptEncoding.includes('gzip')) {
    headers['Content-Encoding'] = 'gzip';
    res.writeHead(200, headers);
    fs.createReadStream(filePath).pipe(zlib.createGzip()).pipe(res);
  } else {
    res.writeHead(200, headers);
    fs.createReadStream(filePath).pipe(res);
  }
}

// ── 자동발주 스케줄러 ──────────────────────────────────
async function runAutoOrderScheduler() {
  console.log(`[자동발주 스케줄러] ${new Date().toLocaleString('ko-KR')} 실행 시작`);

  const items = await db.prepare('SELECT * FROM auto_order_items WHERE enabled=1').all();
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
        weeklyVendorCountSch[vendor] = (await db.prepare(`SELECT COUNT(*) as cnt FROM po_header WHERE vendor_name=? AND po_date>=? AND status!='cancelled' AND status!='취소'`).get(vendor, mondayStrSch)).cnt;
      }
      if (weeklyVendorCountSch[vendor] >= 6) {
        console.log(`[자동발주] 스킵: ${item.product_code} — ${vendor} 주간 한도 초과 (${weeklyVendorCountSch[vendor]}/6건)`);
        continue;
      }
    }

    // 미완료 PO 스킵 (중복발주 방지)
    const pendingPO = await db.prepare(`
      SELECT h.po_number, h.status FROM po_header h
      JOIN po_items i ON i.po_id = h.po_id
      WHERE i.product_code = ? AND h.status IN ('draft','발송','확인','수령중','OS등록대기','sent')
      LIMIT 1
    `).get(item.product_code);
    if (pendingPO) {
      console.log(`[자동발주] 스킵: ${item.product_code} — 미완료 PO (${pendingPO.po_number})`);
      continue;
    }

    // 입고완료 but XERP 미동기화 (OS번호 미등록) → 스킵
    const receivedNotSynced = await db.prepare(`
      SELECT h.po_number, SUM(COALESCE(i.received_qty,0)) as recv_qty FROM po_header h
      JOIN po_items i ON i.po_id = h.po_id
      WHERE i.product_code = ? AND h.status = 'received'
        AND (h.os_number IS NULL OR h.os_number = '')
      GROUP BY h.po_number LIMIT 1
    `).get(item.product_code);
    if (receivedNotSynced) {
      console.log(`[자동발주] 스킵: ${item.product_code} — 입고완료 XERP미동기화 (${receivedNotSynced.po_number}, ${receivedNotSynced.recv_qty}개)`);
      continue;
    }

    // 발주수량 = 월출고량 - 가용재고 (천단위 올림)
    const shortage = Math.max(monthly - avail, 0);
    const orderQty = shortage > 0 ? Math.ceil(shortage / 1000) * 1000 : 0;
    if (orderQty <= 0) continue;

    // PO 생성 (status='sent'로 바로 발송 상태)
    const poNumber = await generatePoNumber();
    // origin 결정
    const _schedOriginProd = await db.prepare('SELECT origin FROM products WHERE product_code=?').get(item.product_code);
    const _schedOrigin = (_schedOriginProd && _schedOriginProd.origin) || '한국';
    const tx = db.transaction(async () => {
      const hdr = await db.prepare(`INSERT INTO po_header (po_number, po_type, vendor_name, status, total_qty, notes, material_status, process_status, origin, po_date)
        VALUES (?,?,?,?,?,?,?,?,?,date('now','localtime'))`).run(
        poNumber, '자동발주', item.vendor_name || '', 'sent', orderQty, '자동발주 스케줄러', 'sent', 'waiting', _schedOrigin
      );
      await db.prepare('INSERT INTO po_items (po_id, product_code, brand, process_type, ordered_qty, spec, notes) VALUES (?,?,?,?,?,?,?)').run(
        hdr.lastInsertRowid, item.product_code, p['브랜드'] || '', '', orderQty, '', '자동발주'
      );
      await db.prepare('UPDATE auto_order_items SET last_ordered_at=? WHERE id=?').run(new Date().toISOString(), item.id);
      return { po_id: Number(hdr.lastInsertRowid), po_number: poNumber };
    });
    const result = await tx();

    // 활동 로그
    logPOActivity(result.po_id, 'auto_order', {
      actor_type: 'scheduler',
      to_status: 'sent',
      details: `자동발주: ${item.product_code} ${orderQty}매 → ${item.vendor_name || '미지정'} (가용재고: ${avail}, 월출고: ${monthly})`
    });

    // 거래명세서 자동 생성
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(result.po_id);
    const poItems = await db.prepare('SELECT * FROM po_items WHERE po_id=?').all(result.po_id);
    const docItems = poItems.map(it => ({
      product_code: it.product_code, product_name: it.brand || '',
      qty: it.ordered_qty, unit_price: 0, amount: 0, spec: it.spec || ''
    }));
    const vendorRow = await db.prepare('SELECT type FROM vendors WHERE name=?').get(po.vendor_name);
    const vendorType = vendorRow ? vendorRow.type : 'material';
    try {
      await db.prepare("INSERT INTO trade_document (po_id, po_number, vendor_name, vendor_type, items_json, status) VALUES (?,?,?,?,?,'sent')")
        .run(result.po_id, poNumber, po.vendor_name, vendorType, JSON.stringify(docItems));
    } catch(e) { console.log('[자동발주] 거래명세서 생성 오류:', e.message); }

    // 이메일 발송
    const vendorInfo = await db.prepare('SELECT * FROM vendors WHERE name=?').get(item.vendor_name);
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
  const schedules = await db.prepare(
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
    const po = await db.prepare('SELECT * FROM po_header WHERE po_id=?').get(sch.po_id);
    const items = await db.prepare('SELECT * FROM po_items WHERE po_id=?').all(sch.po_id);
    if (!po) continue;

    // 이메일 발송 (후공정 업체에게)
    try {
      const postVendorForCc = await db.prepare('SELECT email_cc FROM vendors WHERE name=?').get(sch.post_vendor_name);
      const emailResult = await sendPOEmail(po, items, sch.post_vendor_email, sch.post_vendor_name, true, postVendorForCc ? postVendorForCc.email_cc : '');
      console.log(`[출고일 체크] 후공정 이메일 발송: ${sch.po_number} → ${sch.post_vendor_name} (${sch.post_vendor_email})`, emailResult);

      // auto_email_sent = 1 업데이트
      await db.prepare("UPDATE vendor_shipment_schedule SET auto_email_sent=1, updated_at=datetime('now','localtime') WHERE id=?").run(sch.id);

      // 후공정 상태 업데이트: process_status → 'sent'
      await db.prepare("UPDATE po_header SET process_status='sent' WHERE po_id=? AND process_status='waiting'").run(sch.po_id);

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

  // due_date가 오늘~3일 후인 미완료 PO 조회 (PG/SQLite 양쪽 호환 위해 JS에서 날짜 계산)
  const d3 = new Date(Date.now() + 3 * 86400000).toISOString().slice(0, 10);
  const upcomingPOs = await db.prepare(`
    SELECT h.po_id, h.po_number, h.vendor_name, h.due_date as expected_date, h.total_qty, h.po_date
    FROM po_header h
    WHERE h.due_date >= ? AND h.due_date <= ?
      AND h.status NOT IN ('received','cancelled','os_pending')
  `).all(today, d3);

  if (!upcomingPOs.length) {
    console.log('[납기알림] 임박 건 없음');
    return;
  }

  console.log(`[납기알림] 임박 ${upcomingPOs.length}건 발견`);

  for (const po of upcomingPOs) {
    const dDay = Math.round((new Date(po.expected_date) - new Date(today)) / 86400000);

    // 내부 알림 (사내 담당자) — 동일 PO+날짜 중복 방지
    try {
      const dedupLink = `procurement#po=${po.po_id}&d=${today}`;
      const existed = await db.prepare(
        "SELECT id FROM notifications WHERE link=? LIMIT 1"
      ).get(dedupLink);
      if (!existed) {
        const tag = dDay <= 0 ? '오늘 입고예정' : `D-${dDay}`;
        await createNotification(
          null,
          'po',
          `입고임박: ${po.po_number} (${tag})`,
          `${po.vendor_name} · 납기 ${po.expected_date} · 수량 ${po.total_qty}`,
          dedupLink
        );
        console.log(`[납기알림] 내부알림 생성: ${po.po_number} (D-${dDay})`);
      }
    } catch (e) {
      console.warn('[납기알림] 내부알림 실패:', e.message);
    }

    const vendor = await db.prepare('SELECT email, name FROM vendors WHERE name = ?').get(po.vendor_name);
    if (!vendor || !vendor.email) {
      console.log(`[납기알림] ${po.po_number}: 거래처 이메일 없음 (${po.vendor_name})`);
      continue;
    }

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
async function _safeRunAutoOrder() {
  try {
    await runAutoOrderScheduler();
  } catch (e) {
    logger.error('[자동발주 스케줄러 실패]', e.message, e.stack);
    try { sendSlack(`🔴 *자동발주 스케줄러 실패*\n\`\`\`${e.message}\n${(e.stack||'').slice(0,1000)}\`\`\``); } catch(_){}
  }
  try { await runShipmentEmailCheck(); } catch(e) { logger.error('[출고 이메일 체크 실패]', e.message); }
  try { await runDeadlineAlertCheck(); } catch(e) { logger.error('[납기 알림 체크 실패]', e.message); }
  // 자동발주 실행 완료 후 Slack 요약 전송 (약 1분 여유)
  setTimeout(() => { runDailyPOSummary().catch(e => logger.error('[일일 발주 요약 실패]', e.message)); }, 60 * 1000);
}
function scheduleAutoOrder() {
  const now = new Date();
  const next9am = new Date(now);
  next9am.setHours(9, 0, 0, 0);
  if (now >= next9am) next9am.setDate(next9am.getDate() + 1);

  const msUntilNext = next9am - now;
  console.log(`[자동발주 스케줄러] 다음 실행: ${next9am.toLocaleString('ko-KR')} (${Math.round(msUntilNext / 60000)}분 후)`);

  setTimeout(() => {
    _safeRunAutoOrder();
    // 이후 24시간마다 반복
    setInterval(_safeRunAutoOrder, 24 * 60 * 60 * 1000);
  }, msUntilNext);
}

// ── 매일 9시 발주 현황 Slack 요약 ─────────────────────────────────
// 자동발주 스케줄러 실행 직후(약 1분 뒤) 호출됨
async function runDailyPOSummary() {
  if (!_slackWebhookUrl) return;
  const today = new Date().toISOString().slice(0, 10);

  // 1) 오늘 자동 생성된 PO 집계
  let todayPOs = [];
  try {
    todayPOs = await db.prepare(
      "SELECT po_number, vendor_name, total_qty, origin, notes FROM po_header WHERE po_date=? AND notes LIKE '%자동발주%'"
    ).all(today);
  } catch(e) { logger.warn('[일일 요약] 오늘 PO 조회 실패:', e.message); }

  // 2) 자동발주 설정 품목 중 긴급/위험 분류 (재고 파일 기반)
  let urgent = 0, warning = 0, totalEnabled = 0;
  try {
    const items = await db.prepare('SELECT product_code FROM auto_order_items WHERE enabled=1').all();
    totalEnabled = items.length;
    let inv = [];
    try {
      const raw = JSON.parse(fs.readFileSync(path.join(__dir, 'erp_smart_inventory.json'), 'utf8'));
      inv = raw.products || raw.data || (Array.isArray(raw) ? raw : []);
    } catch(_) {}
    const invMap = {};
    for (const p of inv) invMap[p['품목코드']] = p;
    for (const it of items) {
      const p = invMap[it.product_code] || invMap[(it.product_code||'').toUpperCase()];
      if (!p) continue;
      const avail = typeof p['가용재고']==='number' ? p['가용재고'] : 0;
      const daily = p['_xerpDaily'] || 0;
      if (daily <= 0) continue;
      const remainDays = avail / daily;
      if (remainDays <= 14) urgent++;
      else if (remainDays <= 21) warning++;
    }
  } catch(e) { logger.warn('[일일 요약] 긴급도 분류 실패:', e.message); }

  // 3) Slack 메시지 작성
  const dateLabel = new Date().toLocaleDateString('ko-KR', { year: 'numeric', month: 'long', day: 'numeric', weekday: 'short' });
  let msg = `📋 *ERP 일일 발주 요약 — ${dateLabel}*\n\n`;
  msg += `🤖 *자동발주 생성: ${todayPOs.length}건*\n`;
  if (todayPOs.length > 0) {
    const preview = todayPOs.slice(0, 10).map(po => `  • \`${po.po_number}\` ${po.vendor_name || '(미지정)'} — ${(po.total_qty||0).toLocaleString()}매 [${po.origin||'?'}]`).join('\n');
    msg += preview;
    if (todayPOs.length > 10) msg += `\n  _...외 ${todayPOs.length - 10}건_`;
    msg += '\n\n';
  } else {
    msg += `  _오늘 자동 생성된 발주 없음_\n\n`;
  }
  msg += `📊 *자동발주 모니터링: ${totalEnabled}개 품목*\n`;
  msg += `  🔴 긴급 (14일 이하): *${urgent}개*\n`;
  msg += `  🟡 위험 (21일 이하): *${warning}개*\n`;
  msg += `  🟢 안전: ${Math.max(totalEnabled - urgent - warning, 0)}개\n`;
  msg += `\n<https://docker-manager.barunsoncard.com/c/s-c-erp/|ERP 바로가기>`;

  await sendSlack(msg);
  console.log('[일일 발주 요약] Slack 전송 완료');
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

} // end startServer()

startServer().catch(e => { console.error('❌ 서버 시작 실패:', e); process.exit(1); });
