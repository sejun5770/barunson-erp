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
  // SQLite INTEGER PRIMARY KEY auto-increments; in PG needs SERIAL
  s = s.replace(/\bid\s+INTEGER\s+PRIMARY\s+KEY\b(?!\s+AUTOINCREMENT)/gi, 'id SERIAL PRIMARY KEY');
  // || for string concat is same in PostgreSQL — OK
  // IFNULL → COALESCE
  s = s.replace(/\bIFNULL\s*\(/gi, 'COALESCE(');
  // GROUP_CONCAT → STRING_AGG
  s = s.replace(/\bGROUP_CONCAT\s*\(\s*([^,)]+)\s*,\s*('[^']*')\s*\)/gi, 'STRING_AGG($1::TEXT, $2)');
  s = s.replace(/\bGROUP_CONCAT\s*\(\s*([^)]+)\s*\)/gi, "STRING_AGG($1::TEXT, ',')");
  // INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
  s = s.replace(/\bINSERT\s+OR\s+IGNORE\s+INTO\b/gi, 'INSERT INTO');
  // INSERT OR REPLACE → INSERT ... ON CONFLICT DO UPDATE (단순 upsert)
  s = s.replace(/\bINSERT\s+OR\s+REPLACE\s+INTO\b/gi, 'INSERT INTO');
  // REPLACE INTO → INSERT INTO
  s = s.replace(/\bREPLACE\s+INTO\b/gi, 'INSERT INTO');

  // ON CONFLICT 자동 추가: INSERT INTO ... VALUES (...) 뒤에 ON CONFLICT DO NOTHING 추가
  // INSERT OR IGNORE 였던 것만 (원래 sql에 OR IGNORE가 있었는지 체크)
  if (/\bINSERT\s+OR\s+IGNORE\b/i.test(sql)) {
    // VALUES(...) 뒤에 ON CONFLICT DO NOTHING 추가 (이미 ON CONFLICT가 없을 때)
    if (!/ON\s+CONFLICT/i.test(s)) {
      s = s.replace(/(\)\s*)$/, ') ON CONFLICT DO NOTHING');
      // run/get/all 전에 세미콜론 없으므로 VALUES 다음에 추가
      if (!/ON\s+CONFLICT/i.test(s)) {
        s = s.replace(/(VALUES\s*\([^)]*\))(?!\s*ON)/i, '$1 ON CONFLICT DO NOTHING');
      }
    }
  }

  // INSERT OR REPLACE → ON CONFLICT DO UPDATE SET (모든 컬럼)
  if (/\bINSERT\s+OR\s+REPLACE\b/i.test(sql) || /\bREPLACE\s+INTO\b/i.test(sql)) {
    if (!/ON\s+CONFLICT/i.test(s)) {
      // 컬럼 추출
      const colMatch = s.match(/INSERT\s+INTO\s+\S+\s*\(([^)]+)\)/i);
      if (colMatch) {
        const cols = colMatch[1].split(',').map(c => c.trim().replace(/[@"]/g, ''));
        const firstCol = cols[0];
        const updateCols = cols.slice(1).map(c => `"${c}"=EXCLUDED."${c}"`).join(', ');
        if (updateCols) {
          s = s.replace(/(VALUES\s*\([^)]*\))(?!\s*ON)/i, `$1 ON CONFLICT ("${firstCol}") DO UPDATE SET ${updateCols}`);
        }
      }
    }
  }

  // Named parameters (@name) → positional (pg doesn't support named)
  // This is handled separately if needed

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
    // INSERT 문에 RETURNING 자동 추가 (lastInsertRowid 지원)
    let sql = this._sql;
    if (/^\s*INSERT\s/i.test(sql) && !/RETURNING/i.test(sql)) {
      const m = sql.match(/INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)/i);
      const table = m ? m[1] : '';
      const cols = m ? m[2] : '';
      const pkMap = { po_header:'po_id', po_items:'item_id', users:'user_id', vendors:'vendor_id', products:'id', receipts:'id', defects:'id', bom_header:'bom_id', bom_items:'bom_item_id', accessories:'id', work_orders:'wo_id', work_order_logs:'log_id', work_order_results:'id', product_notes:'product_code', product_field_history:'id', gift_sets:'id', gift_set_bom:'id', gift_set_transactions:'id', error_logs:'id', audit_log:'id', batch_master:'batch_id', batch_transactions:'txn_id', batch_inspections:'insp_id', incoming_inspections:'inspection_id', ncr:'ncr_id', ncr_logs:'id', notices:'id', cost_rates:'id', notice_reads:'id', vendor_notes:'id', approvals:'id', approval_lines:'id', sales_orders:'id', sales_order_items:'id', budgets:'id', daily_cash:'id', notifications:'id', journal_entries:'id', journal_entry_lines:'id', vendor_defect_reports:'id', receipt_items:'receipt_item_id', gift_assembly:'id', gift_assembly_materials:'id', china_shipment_log:'id', po_drafts:'id', trade_document:'id' };
      const pk = pkMap[table] || 'id';
      // If PK column is 'id' and not in the INSERT columns, add auto-generated id
      if (pk === 'id' && cols && !cols.split(',').map(c=>c.trim()).includes('id')) {
        const colList = cols.trim();
        const valMatch = sql.match(/VALUES\s*\(([^)]+)\)/i);
        if (valMatch) {
          sql = sql.replace(`(${colList})`, `(id, ${colList})`);
          sql = sql.replace(`(${valMatch[1]})`, `((SELECT COALESCE(MAX(id),0)+1 FROM ${table}), ${valMatch[1]})`);
        }
      }
      sql = sql.replace(/;?\s*$/, '') + ' RETURNING ' + pk;
    }
    const result = await pool.query(sql, params);
    const firstRow = result.rows && result.rows[0];
    return {
      changes: result.rowCount,
      lastInsertRowid: firstRow ? Number(firstRow[Object.keys(firstRow)[0]]) : undefined
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
  let s = convertSqliteFunctions(sql);
  // AUTOINCREMENT 변환
  s = s.replace(/INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT/gi, 'SERIAL PRIMARY KEY');
  // SQLite INTEGER PRIMARY KEY auto-increments; in PG needs SERIAL
  s = s.replace(/\bid\s+INTEGER\s+PRIMARY\s+KEY\b(?!\s+AUTOINCREMENT)/gi, 'id SERIAL PRIMARY KEY');
  // REAL → DOUBLE PRECISION
  s = s.replace(/\bREAL\b/g, 'DOUBLE PRECISION');
  // INSERT OR IGNORE/REPLACE
  s = s.replace(/\bINSERT\s+OR\s+IGNORE\s+INTO\b/gi, 'INSERT INTO');
  s = s.replace(/\bINSERT\s+OR\s+REPLACE\s+INTO\b/gi, 'INSERT INTO');
  // DEFAULT (expr) → DEFAULT expr
  s = s.replace(/DEFAULT\s+\(NOW\(\)\)/gi, 'DEFAULT NOW()');
  s = s.replace(/DEFAULT\s+\(CURRENT_DATE\)/gi, 'DEFAULT CURRENT_DATE');
  // ON CONFLICT DO NOTHING for INSERT (from OR IGNORE)
  if (/\bINSERT\s+OR\s+IGNORE\b/i.test(sql) && !/ON\s+CONFLICT/i.test(s)) {
    s = s.replace(/(VALUES\s*\([^)]*\))(?!\s*ON)/i, '$1 ON CONFLICT DO NOTHING');
  }
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
    const prevPool = pool;
    try {
      await client.query('BEGIN');
      // transaction 내에서는 pool 대신 client 사용
      pool = { query: (...a) => client.query(...a) };
      const result = await fn(...args);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      try { await client.query('ROLLBACK'); } catch(re) {}
      throw e;
    } finally {
      pool = prevPool; // 반드시 복원 (에러 여부 무관)
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
