var SHEET_NAME = "★생산발주현황_바른컴퍼니";
var START_ROW = 2979;

function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var action = data.action || "append";

    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) {
      return ContentService.createTextOutput(JSON.stringify({ok:false,error:"sheet not found"}))
        .setMimeType(ContentService.MimeType.JSON);
    }

    // ── 이메일 발송 ──
    if (action === "sendEmail") {
      var to = data.to || "";
      var subject = data.subject || "";
      var htmlBody = data.html || "";
      if (!to || !subject) {
        return ContentService.createTextOutput(JSON.stringify({ok:false,error:"to and subject required"}))
          .setMimeType(ContentService.MimeType.JSON);
      }
      try {
        var mailOptions = {
          to: to,
          subject: subject,
          htmlBody: htmlBody
        };
        // 첨부파일 처리 (HTML→PDF 변환)
        if (data.attachment && data.attachment.content) {
          try {
            var htmlBlob = Utilities.newBlob(data.attachment.content, 'text/html', 'temp.html');
            var tempFile = DriveApp.createFile(htmlBlob);
            var pdfBlob = tempFile.getAs('application/pdf');
            var pdfName = (data.attachment.name || 'attachment.html').replace(/\.html?$/i, '.pdf');
            pdfBlob.setName(pdfName);
            mailOptions.attachments = [pdfBlob];
            tempFile.setTrashed(true);
          } catch(pdfErr) {
            // PDF 변환 실패 시 HTML 그대로 첨부
            var blob = Utilities.newBlob(data.attachment.content, 'text/html', data.attachment.name || 'attachment.html');
            mailOptions.attachments = [blob];
          }
        }
        GmailApp.sendEmail(to, subject, '', mailOptions);
        return ContentService.createTextOutput(JSON.stringify({
          ok: true,
          action: "sendEmail",
          to: to,
          subject: subject
        })).setMimeType(ContentService.MimeType.JSON);
      } catch(mailErr) {
        return ContentService.createTextOutput(JSON.stringify({ok:false,error:mailErr.toString()}))
          .setMimeType(ContentService.MimeType.JSON);
      }
    }

    // ── 취소 처리: 취소선 + 빨간글씨 ──
    if (action === "cancel") {
      var codes = data.product_codes || [];
      var orderDate = data.order_date || "";
      if (!codes.length) {
        return ContentService.createTextOutput(JSON.stringify({ok:false,error:"no product_codes"}))
          .setMimeType(ContentService.MimeType.JSON);
      }

      var lastRow = sheet.getLastRow();
      var searchStart = Math.max(START_ROW, 3);
      if (lastRow < searchStart) {
        return ContentService.createTextOutput(JSON.stringify({ok:true,formatted:0,message:"no data rows"}))
          .setMimeType(ContentService.MimeType.JSON);
      }

      // F열 = 품목코드(6), A열 = 발주일(1)
      var dataRange = sheet.getRange(searchStart, 1, lastRow - searchStart + 1, 25);
      var values = dataRange.getValues();
      var formatted = 0;
      var codeSet = {};
      for (var c = 0; c < codes.length; c++) codeSet[String(codes[c]).trim()] = true;

      // 날짜 비교용 정규화 함수
      function normalizeDate(v) {
        if (!v) return "";
        if (v instanceof Date) {
          var y = v.getFullYear();
          var m = ("0" + (v.getMonth()+1)).slice(-2);
          var d = ("0" + v.getDate()).slice(-2);
          return y + "-" + m + "-" + d;
        }
        return String(v).trim().replace(/\//g, "-").slice(0, 10);
      }

      for (var i = 0; i < values.length; i++) {
        var rowCode = String(values[i][5]).trim(); // F열 (인덱스 5)

        if (codeSet[rowCode]) {
          // 날짜 필터 (선택적)
          if (orderDate) {
            var rowDate = normalizeDate(values[i][0]);
            if (rowDate && rowDate !== orderDate) continue;
          }

          var rowNum = searchStart + i;
          var range = sheet.getRange(rowNum, 1, 1, 25);
          range.setFontColor("#FF0000");
          range.setFontLine("line-through");
          formatted++;
        }
      }

      return ContentService.createTextOutput(JSON.stringify({
        ok: true,
        action: "cancel",
        formatted: formatted,
        codes: codes
      })).setMimeType(ContentService.MimeType.JSON);
    }

    // ── 행 삭제: 지정 범위 내용+서식 초기화 ──
    if (action === "clear") {
      var startRow = data.start_row || START_ROW;
      var endRow = data.end_row || sheet.getLastRow();
      if (endRow < startRow) {
        return ContentService.createTextOutput(JSON.stringify({ok:true,cleared:0}))
          .setMimeType(ContentService.MimeType.JSON);
      }
      var range = sheet.getRange(startRow, 1, endRow - startRow + 1, 25);
      range.clearContent();
      range.clearFormat();
      return ContentService.createTextOutput(JSON.stringify({
        ok: true,
        action: "clear",
        cleared: endRow - startRow + 1,
        from: startRow,
        to: endRow
      })).setMimeType(ContentService.MimeType.JSON);
    }

    // ── 기본: 행 추가 ──
    var rows = data.rows || [];
    if (!rows.length) {
      return ContentService.createTextOutput(JSON.stringify({ok:false,error:"no rows"}))
        .setMimeType(ContentService.MimeType.JSON);
    }

    var lastRow = sheet.getLastRow();
    var writeRow = Math.max(START_ROW, lastRow + 1);

    var output = [];
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      output.push([
        r.order_date || "",
        "",
        r.os_no || "",
        r.warehouse_order || "",
        r.product_name || "",
        r.product_code || "",
        r.actual_qty || "",
        r.material_code || "",
        r.material_name || "",
        r.paper_maker || "",
        r.vendor_code || "",
        r.qty || "",
        r.cut_spec || "",
        r.plate_spec || "",
        r.cutting || "",
        r.printing || "",
        r.foil_emboss || "",
        r.thomson || "",
        r.envelope_proc || "",
        r.seari || "",
        r.laser || "",
        r.silk || "",
        r.outsource || "",
        r.order_qty || "",
        r.product_spec || ""
      ]);
    }

    var writeRange = sheet.getRange(writeRow, 1, output.length, output[0].length);
    writeRange.setValues(output);
    writeRange.setBackground("#FFF9C4"); // 연한 노란색

    return ContentService.createTextOutput(JSON.stringify({
      ok: true,
      written: output.length,
      start_row: writeRow
    })).setMimeType(ContentService.MimeType.JSON);
  } catch(err) {
    return ContentService.createTextOutput(JSON.stringify({ok:false,error:err.toString()}))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  return ContentService.createTextOutput(JSON.stringify({
    ok: true,
    message: "Barunson Order History API",
    sheet: SHEET_NAME
  })).setMimeType(ContentService.MimeType.JSON);
}

// 권한 승인용 테스트 함수 — 이 함수를 실행하면 메일 권한 팝업이 뜹니다
function testMailAuth() {
  MailApp.sendEmail({
    to: "sejun.song@barunn.net",
    subject: "[테스트] 바른컴퍼니 발주시스템 메일 권한 확인",
    htmlBody: "<h2>메일 발송 권한이 정상 승인되었습니다!</h2><p>이 메일이 수신되면 발주 이메일 발송 기능이 정상 작동합니다.</p>"
  });
  Logger.log("테스트 메일 발송 완료");
}

function extractProductCodes() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var codes = {};
  var sheets = ["★생산발주현황_바른컴퍼니", "★생산발주현황_디얼디어"];
  for (var s = 0; s < sheets.length; s++) {
    var sheet = ss.getSheetByName(sheets[s]);
    if (!sheet) continue;
    var data = sheet.getRange("F3:F" + sheet.getLastRow()).getValues();
    for (var i = 0; i < data.length; i++) {
      var v = String(data[i][0]).trim();
      if (v && v !== "" && v !== "undefined") codes[v] = 1;
    }
  }
  var result = Object.keys(codes).sort();
  Logger.log("Total unique codes: " + result.length);
}
