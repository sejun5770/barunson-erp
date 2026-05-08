const http = require('http');
const crypto = require('crypto');
const token = crypto.createHash('sha256').update('sejun.song@barunn.net' + 'barun-company-portal-2026').digest('hex').slice(0,16);

function api(method, path, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : '';
    const opts = { hostname:'localhost', port:12026, path, method, headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(data)} };
    const req = http.request(opts, res => { let b=''; res.on('data',c=>b+=c); res.on('end',()=>{ try{resolve(JSON.parse(b))}catch(e){resolve(b)} }); });
    req.on('error',reject); if(data)req.write(data); req.end();
  });
}

(async () => {
  const vn = encodeURIComponent('코리아패키지');
  const em = encodeURIComponent('sejun.song@barunn.net');

  console.log('--- 1. GET lead time (initial) ---');
  const r1 = await api('GET', `/api/vendor-portal/lead-time?email=${em}&token=${token}&vendor_name=${vn}`);
  console.log('  ok:', r1.ok, '| count:', r1.data && r1.data.length);
  if (r1.data) r1.data.forEach(d => console.log(`  ${d.process_type}: ${d.default_days}d`));

  console.log('\n--- 2. POST save lead times ---');
  const r2 = await api('POST', '/api/vendor-portal/lead-time', {
    email:'sejun.song@barunn.net', token, vendor_name:'코리아패키지',
    lead_times: [
      {process_type:'코팅', default_days:2, adjusted_days:3, adjusted_reason:'코팅기 수리중'},
      {process_type:'박', default_days:2, adjusted_days:4, adjusted_reason:'금박 특수처리'},
    ]
  });
  console.log('  saved:', r2.data && r2.data.saved);

  console.log('\n--- 3. GET after save ---');
  const r3 = await api('GET', `/api/vendor-portal/lead-time?email=${em}&token=${token}&vendor_name=${vn}`);
  if (r3.data) {
    r3.data.forEach(d => {
      const days = d.adjusted_days != null ? d.adjusted_days : d.default_days;
      const mod = d.adjusted_days != null && d.adjusted_days !== d.default_days;
      console.log(`  ${d.process_type}: ${days}d${mod ? ` (default:${d.default_days}, reason:${d.adjusted_reason})` : ''}`);
    });
  }

  console.log('\n--- 4. Auth fail test ---');
  const r4 = await api('GET', `/api/vendor-portal/lead-time?email=wrong%40test.com&token=bad&vendor_name=${vn}`);
  console.log('  ok:', r4.ok, '| error:', r4.error);

  console.log('\n=== DONE ===');
})();
