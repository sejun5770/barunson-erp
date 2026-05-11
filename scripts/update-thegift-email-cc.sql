-- 더기프트 17개 거래처 email_cc 일괄 갱신 (2026-05-11T00:47:26.357Z)
-- snapshot 과 동기화하려면 운영 dm-berp SQLite 에 실행 후 재배포 (snapshot UPSERT 가 동일 값을 다시 덮어씀).

UPDATE vendors
   SET email_cc = 'kyongwon.seo@barunn.net, jiwon.chu@barunn.net, sojeong.eo@barunn.net, hayeon.seok@barunn.net, sejun.song@barunn.net, suhyeon.kim@barunn.net, eunji.kang@barunn.net',
       updated_at = '2026-05-11T00:47:26.357Z'
 WHERE type = '더기프트'
   AND vendor_id IN (9, 10, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 171, 172);
