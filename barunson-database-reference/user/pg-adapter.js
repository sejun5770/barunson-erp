/**
 * PostgreSQL 어댑터 — better-sqlite3 호환 API
 *
 * 사용법:
 *   const db = require('./pg-adapter');
 *   await db.connect();
 *
 *   // better-sqlite3와 동일한 동기식 패턴 (내부적으로 캐시된 pool 사용)
 *   const row = await db.prepare('SELECT * FROM users WHERE id = ?').get(1);
 *   const rows = await db.prepare('SELECT * FROM users').all();
 *   const info = await db.prepare('INSERT INTO users (name) VALUES (?)').run('test');
 *
 *   // transaction
 *   await db.transaction(async () => {
 *     await db.prepare('INSERT ...').run(...);
 *     await db.prepare('UPDATE ...').run(...);
 *   })();
 */
const { Pool } = require('pg');

let pool = null;

// SQLite ? 플레이스홀더를 PostgreSQL $1, $2 ... 로 변환
function convertPlaceholders(sql) {
  let idx = 0;
  // ?를 문자열 리터럴 안에서는 변환하지 않음
  let result = '';
  let inString = false;
  let stringChar = '';
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inString) {
      result += ch;
      if (ch === stringChar && sql[i + 1] !== stringChar) inString = false;
      else if (ch === stringChar && sql[i + 1] === stringChar) { result += sql[++i]; }
    } else if (ch === "'" || ch === '"') {
      inString = true;
      stringChar = ch;
      result += ch;
    } else if (ch === '?') {
      idx++;
      result += '$' + idx;
    } else {
      result += ch;
    }
  }
  return result;
}

// SQLite 함수를 PostgreSQL로 변환
function convertSqliteFunctions(sql) {
  let s = sql;
  // datetime('now','localtime') → NOW()
  s = s.replace(/datetime\(\s*'now'\s*,\s*'localtime'\s*\)/gi, 'NOW()');
  s = s.replace(/datetime\(\s*'now'\s*\)/gi, 'NOW()');
  // date('now','localtime') → CURRENT_DATE
  s = s.replace(/date\(\s*'now'\s*,\s*'localtime'\s*\)/gi, 'CURRENT_DATE');
  s = s.replace(/date\(\s*'now'\s*\)/gi, 'CURRENT_DATE');
  // AUTOINCREMENT (in rare inline DDL)
  s = s.replace(/INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT/gi, 'SERIAL PRIMARY KEY');
  // || for string concat is same in PostgreSQL — OK
  // IFNULL → COALESCE
  s = s.replace(/\bIFNULL\s*\(/gi, 'COALESCE(');
  // GROUP_CONCAT → STRING_AGG
  s = s.replace(/\bGROUP_CONCAT\s*\(\s*([^,)]+)\s*,\s*('[^']*')\s*\)/gi, 'STRING_AGG($1::TEXT, $2)');
  s = s.replace(/\bGROUP_CONCAT\s*\(\s*([^)]+)\s*\)/gi, "STRING_AGG($1::TEXT, ',')");
  // REPLACE INTO → INSERT ... ON CONFLICT DO UPDATE (handled per-case)
  // GLOB → LIKE (rare)
  return s;
}

function convertSql(sql) {
  return convertSqliteFunctions(convertPlaceholders(sql));
}

// Statement 객체 — prepare()의 반환값
class Statement {
  constructor(sql) {
    this._originalSql = sql;
    this._sql = convertSql(sql);
  }

  async get(...params) {
    const result = await pool.query(this._sql, params);
    return result.rows[0] || undefined;
  }

  async all(...params) {
    const result = await pool.query(this._sql, params);
    return result.rows;
  }

  async run(...params) {
    const result = await pool.query(this._sql, params);
    return {
      changes: result.rowCount,
      lastInsertRowid: result.rows && result.rows[0] ? result.rows[0].id : undefined
    };
  }
}

// pragma는 PostgreSQL에서 불필요 — 무시
function pragma(_cmd) {
  // no-op
}

// prepare 함수
function prepare(sql) {
  return new Statement(sql);
}

// exec — DDL 실행
async function exec(sql) {
  const converted = convertSqliteFunctions(sql);
  // AUTOINCREMENT 변환
  let s = converted.replace(/INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT/gi, 'SERIAL PRIMARY KEY');
  // REAL → DOUBLE PRECISION
  s = s.replace(/\bREAL\b/g, 'DOUBLE PRECISION');
  // 여러 문장 분리하여 실행
  const statements = s.split(';').map(st => st.trim()).filter(st => st.length > 0);
  for (const stmt of statements) {
    try {
      await pool.query(stmt);
    } catch (e) {
      // CREATE TABLE IF NOT EXISTS에서 이미 존재하면 무시
      if (!e.message.includes('already exists')) {
        console.warn('[pg-adapter exec warn]', e.message.split('\n')[0]);
      }
    }
  }
}

// transaction
function transaction(fn) {
  return async (...args) => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // transaction 내에서는 pool 대신 client 사용
      const prevPool = pool;
      pool = { query: (...a) => client.query(...a) };
      const result = await fn(...args);
      pool = prevPool;
      await client.query('COMMIT');
      return result;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  };
}

async function connect(config) {
  pool = new Pool({
    host: config.host || process.env.PG_HOST || 'onely-postgres',
    port: parseInt(config.port || process.env.PG_PORT || '5432'),
    user: config.user || process.env.PG_USER || 'onely',
    password: config.password || process.env.PG_PASSWORD || 'onely',
    database: config.database || process.env.PG_DATABASE || 'sc_erp',
    max: 10,
    idleTimeoutMillis: 30000,
  });
  // 연결 테스트
  const client = await pool.connect();
  client.release();
  console.log('[pg-adapter] PostgreSQL 연결 완료');
}

async function close() {
  if (pool) await pool.end();
}

module.exports = {
  connect,
  close,
  prepare,
  exec,
  pragma,
  transaction,
  // pool 직접 접근 (필요 시)
  get pool() { return pool; },
};
