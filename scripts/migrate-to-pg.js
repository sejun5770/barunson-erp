#!/usr/bin/env node
/**
 * SQLite → PostgreSQL 마이그레이션 스크립트
 * 실행: node scripts/migrate-to-pg.js
 *
 * 환경변수:
 *   PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
 *   SQLITE_PATH (기본: /app/data/orders.db)
 */
const { Client } = require('pg');
const Database = require('better-sqlite3');
const fs = require('fs');

const SQLITE_PATH = process.env.SQLITE_PATH || '/app/data/orders.db';
const pgConfig = {
  host: process.env.PG_HOST || '172.17.0.1',
  port: parseInt(process.env.PG_PORT || '5432'),
  user: process.env.PG_USER || 'onely',
  password: process.env.PG_PASSWORD || 'onely',
  database: process.env.PG_DATABASE || 'sc_erp',
};

// SQLite → PostgreSQL 타입 매핑
function convertCreateTable(sql, tableName) {
  let pg = sql;

  // INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
  pg = pg.replace(/(\w+)\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT/gi, '$1 SERIAL PRIMARY KEY');
  // INTEGER PRIMARY KEY (without AUTOINCREMENT) → SERIAL PRIMARY KEY
  pg = pg.replace(/(\w+)\s+INTEGER\s+PRIMARY\s+KEY(?!\s*,|\s*\))/gi, '$1 SERIAL PRIMARY KEY');

  // datetime('now','localtime') → NOW()
  pg = pg.replace(/datetime\('now',\s*'localtime'\)/gi, 'NOW()');
  pg = pg.replace(/datetime\('now'\)/gi, 'NOW()');

  // TEXT → TEXT (same)
  // INTEGER → INTEGER (same)
  // REAL → DOUBLE PRECISION
  pg = pg.replace(/\bREAL\b/g, 'DOUBLE PRECISION');

  // BOOLEAN 처리 — SQLite uses INTEGER for booleans
  // Keep as INTEGER, PostgreSQL will handle it

  // Remove trailing comma before closing paren (SQLite allows it)
  pg = pg.replace(/,\s*\)/g, '\n)');

  // Handle column additions appended with comma (e.g., ", origin TEXT DEFAULT '한국'")
  // These appear as extra columns after CREATE TABLE

  return pg;
}

function convertIndex(sql) {
  // SQLite index syntax is mostly compatible with PostgreSQL
  return sql;
}

async function main() {
  console.log('=== SQLite → PostgreSQL 마이그레이션 시작 ===');
  console.log('SQLite:', SQLITE_PATH);
  console.log('PostgreSQL:', `${pgConfig.host}:${pgConfig.port}/${pgConfig.database}`);

  // SQLite 연결
  const sqlite = new Database(SQLITE_PATH, { readonly: true });
  sqlite.pragma('journal_mode = WAL');

  // PostgreSQL 연결
  const pg = new Client(pgConfig);
  await pg.connect();
  console.log('PostgreSQL 연결 완료');

  // 1. 기존 테이블 삭제 (clean migration)
  const existingTables = (await pg.query(
    "SELECT tablename FROM pg_tables WHERE schemaname='public'"
  )).rows.map(r => r.tablename);

  if (existingTables.length > 0) {
    console.log(`기존 테이블 ${existingTables.length}개 삭제 중...`);
    await pg.query('DROP SCHEMA public CASCADE');
    await pg.query('CREATE SCHEMA public');
  }

  // 2. 스키마 변환 및 생성
  const tables = sqlite.prepare(
    "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
  ).all();

  console.log(`\n=== 테이블 ${tables.length}개 생성 ===`);
  let created = 0;
  let failed = 0;
  const failedTables = [];

  for (const t of tables) {
    try {
      const pgSql = convertCreateTable(t.sql, t.name);
      await pg.query(pgSql);
      created++;
    } catch (e) {
      // 첫 시도 실패 시, 더 공격적인 변환 시도
      try {
        let pgSql = convertCreateTable(t.sql, t.name);
        // UNIQUE constraint on column definition → separate constraint
        // Remove problematic defaults
        pgSql = pgSql.replace(/DEFAULT\s+\(NOW\(\)\)/gi, 'DEFAULT NOW()');
        await pg.query(pgSql);
        created++;
      } catch (e2) {
        console.error(`❌ ${t.name}: ${e2.message.split('\n')[0]}`);
        failedTables.push({ name: t.name, sql: t.sql, error: e2.message });
        failed++;
      }
    }
  }
  console.log(`✅ 생성 완료: ${created}개, ❌ 실패: ${failed}개`);

  // 실패한 테이블 수동 처리
  for (const ft of failedTables) {
    console.log(`\n--- 실패 테이블 재시도: ${ft.name} ---`);
    // 완전 수동 변환
    try {
      const cols = sqlite.prepare(`PRAGMA table_info('${ft.name}')`).all();
      let createSql = `CREATE TABLE "${ft.name}" (\n`;
      const colDefs = [];
      for (const col of cols) {
        let type = col.type.toUpperCase();
        let def = '';

        if (col.pk && type.includes('INTEGER')) {
          type = 'SERIAL';
          def = 'PRIMARY KEY';
        } else {
          if (type === 'REAL') type = 'DOUBLE PRECISION';
          if (type === '') type = 'TEXT';

          if (col.notnull && !col.dflt_value) def = 'NOT NULL';
          else if (col.notnull && col.dflt_value) def = `NOT NULL DEFAULT ${col.dflt_value}`;
          else if (col.dflt_value) def = `DEFAULT ${col.dflt_value}`;
        }

        // Fix datetime defaults
        def = def.replace(/datetime\('now',\s*'localtime'\)/gi, 'NOW()');
        def = def.replace(/datetime\('now'\)/gi, 'NOW()');
        def = def.replace(/DEFAULT\s+\(NOW\(\)\)/gi, 'DEFAULT NOW()');

        colDefs.push(`  "${col.name}" ${type} ${def}`.trim());
      }
      createSql += colDefs.join(',\n') + '\n)';
      await pg.query(createSql);
      console.log(`✅ ${ft.name} 재시도 성공`);
      created++;
      failed--;
    } catch (e3) {
      console.error(`❌ ${ft.name} 최종 실패: ${e3.message.split('\n')[0]}`);
    }
  }

  // 3. 인덱스 생성
  const indexes = sqlite.prepare(
    "SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name"
  ).all();

  console.log(`\n=== 인덱스 ${indexes.length}개 생성 ===`);
  let idxOk = 0;
  for (const idx of indexes) {
    try {
      await pg.query(idx.sql);
      idxOk++;
    } catch (e) {
      // 인덱스 실패는 무시 (테이블이 없거나 이미 존재)
    }
  }
  console.log(`✅ 인덱스 ${idxOk}개 생성 완료`);

  // 4. 데이터 마이그레이션
  console.log('\n=== 데이터 마이그레이션 ===');
  const pgTables = (await pg.query(
    "SELECT tablename FROM pg_tables WHERE schemaname='public'"
  )).rows.map(r => r.tablename);

  let totalRows = 0;
  for (const tableName of pgTables) {
    const rows = sqlite.prepare(`SELECT * FROM "${tableName}"`).all();
    if (rows.length === 0) continue;

    const cols = Object.keys(rows[0]);
    // SERIAL 컬럼 확인 (PK auto-increment)
    const pgCols = (await pg.query(
      `SELECT column_name, column_default FROM information_schema.columns WHERE table_name=$1 AND table_schema='public'`,
      [tableName]
    )).rows;
    const serialCols = pgCols.filter(c => c.column_default && c.column_default.startsWith('nextval')).map(c => c.column_name);

    // SERIAL 컬럼 포함하여 INSERT (시퀀스는 나중에 재설정)
    const placeholders = cols.map((_, i) => `$${i + 1}`).join(', ');
    const insertSql = `INSERT INTO "${tableName}" (${cols.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`;

    let inserted = 0;
    for (const row of rows) {
      try {
        const values = cols.map(c => row[c]);
        await pg.query(insertSql, values);
        inserted++;
      } catch (e) {
        if (inserted === 0) {
          console.error(`  ⚠️ ${tableName}: ${e.message.split('\n')[0]}`);
        }
      }
    }
    if (inserted > 0) {
      console.log(`  ${tableName}: ${inserted}건`);
      totalRows += inserted;

      // SERIAL 시퀀스 재설정
      for (const sc of serialCols) {
        try {
          await pg.query(`SELECT setval(pg_get_serial_sequence('"${tableName}"', '${sc}'), COALESCE((SELECT MAX("${sc}") FROM "${tableName}"), 0) + 1, false)`);
        } catch (_) {}
      }
    }
  }

  console.log(`\n=== 마이그레이션 완료 ===`);
  console.log(`테이블: ${pgTables.length}개, 데이터: ${totalRows}건`);

  sqlite.close();
  await pg.end();
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
