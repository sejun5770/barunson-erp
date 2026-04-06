// routes/_ctx.js — 라우트 모듈이 공유하는 컨텍스트 컨테이너
// serve_inv2.js에서 초기화 후 주입됨

const ctx = {
  // DB connections
  db: null,           // SQLite (better-sqlite3)
  xerpPool: null,     // MSSQL pool (getter)
  ddPool: null,       // MySQL pool (getter)

  // Response helpers
  ok: null,
  fail: null,
  jsonRes: null,
  readBody: null,
  readJSON: null,
  parseMultipart: null,

  // Auth helpers
  signToken: null,
  verifyToken: null,
  extractToken: null,

  // Logging
  auditLog: null,
  logError: null,
  logPOActivity: null,

  // Notification
  createNotification: null,

  // PO helpers
  generatePoNumber: null,

  // Permission
  hasPermission: null,
  ALL_PAGES: null,
  ROLE_PERMISSIONS: null,

  // SMTP
  getSmtpTransporter: () => ctx._smtpTransporter,
  _smtpTransporter: null,
  SMTP_FROM: '',

  // Caches
  xerpInventoryCache: null,
  salesKpiCache: null,
  costSummaryCache: null,
  acctStatsCache: null,
  trialBalanceCache: null,

  // Product helpers
  getProductInfo: null,
  getLastVendorPrice: null,

  // Constants
  KNOWN_ACCOUNTS: null,
  DEPT_GUBUN_LABELS: null,
  BRAND_LABELS: null,

  // External modules
  bcrypt: null,
  jwt: null,
  sql: null,
  nodemailer: null,
  fs: null,
  path: null,

  // Ensure XERP connection
  ensureXerpPool: null,

  // Journal entry auto-creation (cross-module)
  createJournalEntry: null,
};

module.exports = ctx;
