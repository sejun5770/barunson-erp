// 더기프트 17개 거래처의 email_cc 를 일괄 기본값으로 갱신한다.
// - snapshot JSON 을 수정한다 (다음 배포 시 seed UPSERT 로 운영 DB 반영).
// - 즉시 적용용 SQL 도 출력한다.

const fs = require('fs');
const path = require('path');

const DEFAULT_CC = 'kyongwon.seo@barunn.net, jiwon.chu@barunn.net, sojeong.eo@barunn.net, hayeon.seok@barunn.net, sejun.song@barunn.net, suhyeon.kim@barunn.net, eunji.kang@barunn.net';
const TYPE = '더기프트';

const snapPath = path.resolve(__dirname, '..', 'barunson-database-reference', 'user', 'snapshots', 'vendors_snapshot.json');
const sqlOutPath = path.resolve(__dirname, 'update-thegift-email-cc.sql');

const raw = JSON.parse(fs.readFileSync(snapPath, 'utf-8'));
const rows = raw.data || [];

const now = new Date().toISOString();
const targets = [];
for (const v of rows) {
  if (v.type === TYPE) {
    v.email_cc = DEFAULT_CC;
    v.updated_at = now;
    targets.push(v);
  }
}

fs.writeFileSync(snapPath, JSON.stringify(raw), 'utf-8');

const ids = targets.map((v) => v.vendor_id).sort((a, b) => a - b);
const sql = [
  `-- 더기프트 ${targets.length}개 거래처 email_cc 일괄 갱신 (${now})`,
  `-- snapshot 과 동기화하려면 운영 dm-berp SQLite 에 실행 후 재배포 (snapshot UPSERT 가 동일 값을 다시 덮어씀).`,
  '',
  `UPDATE vendors`,
  `   SET email_cc = '${DEFAULT_CC.replace(/'/g, "''")}',`,
  `       updated_at = '${now}'`,
  ` WHERE type = '${TYPE}'`,
  `   AND vendor_id IN (${ids.join(', ')});`,
  '',
].join('\n');
fs.writeFileSync(sqlOutPath, sql, 'utf-8');

console.log(`[snapshot] ${targets.length}건 갱신 → ${path.relative(process.cwd(), snapPath)}`);
console.log(`[sql] 즉시 적용용 → ${path.relative(process.cwd(), sqlOutPath)}`);
console.log('대상 거래처:');
for (const v of targets) console.log(`  - ${v.vendor_id}\t${v.name}`);
