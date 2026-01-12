// ver2.1
function getGA4Report7days() {
  // GA4のプロパティIDを入力してください（数値のみ）
  const propertyId = '331542258';
  // プロパティIDが設定されていない場合はエラーを表示
  if (propertyId === 'YOUR_GA4_PROPERTY_ID' || isNaN(propertyId)) {
    Logger.log('エラー: 有効なGA4プロパティIDを設定してください。数値のみが有効です。');
    return;
  }
  const request = {
    property: 'properties/' + propertyId,
    dimensions: [
      {name: 'sessionSourceMedium'},
      {name: 'sessionManualCampaignName'},
      {name: 'sessionManualTerm'},
      {name: 'sessionManualAdContent'},
      {name: 'sessionGoogleAdsQuery'},
      {name: 'date'},
      {name: 'eventName'}
    ],
    metrics: [
      {name: 'eventCount'}
    ],
    dateRanges: [{
      startDate: '7daysAgo',
      endDate: 'yesterday'
    }],
    dimensionFilter: {
      andGroup: {
        expressions: [
          {
            filter: {
              fieldName: 'sessionSource',
              stringFilter: {
                value: 'meta',
                matchType: 'EXACT'
              }
            }
          },
          {
            filter: {
              fieldName: 'sessionMedium',
              stringFilter: {
                value: 'infeed',
                matchType: 'EXACT'
              }
            }
          }
        ]
      }
    }
  };
  try {
    // レポートの実行
    const report = AnalyticsData.Properties.runReport(request, 'properties/' + propertyId);
    // 結果の処理
    if (report && report.rows) {
      const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
      let sheet = spreadsheet.getSheetByName('GA');
      // ヘッダー（Google Ads Queryを追加）
      const headers = ['Date', 'Source / Medium', 'Campaign', 'Keyword', 'Ad Content', 'Google Ads Query', 'CV Prospect All', 'CV Seminar All', 'CV Contract All'];
      // 'GA'シートが存在しない場合は新規作成
      if (!sheet) {
        sheet = spreadsheet.insertSheet('GA');
        // ヘッダーの書き込み
        sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      }
      // 既存のデータを取得
      let existingData = [];
      const lastRow = sheet.getLastRow();
      if (lastRow > 1) {
        existingData = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
      }
      // データの集計
      const aggregatedData = {};
      report.rows.forEach(row => {
        const key = row.dimensionValues.slice(0, 6).map(d => d.value).join('|');
        if (!aggregatedData[key]) {
          aggregatedData[key] = {
            dimensions: row.dimensionValues.slice(0, 6).map(d => d.value),
            cvProspectAll: 0,
            cvSeminarAll: 0,
            cvContractAll: 0
          };
        }

        const eventName = row.dimensionValues[6].value;
        const eventCount = parseInt(row.metricValues[0].value);

        // イベント名に応じてカウントを振り分け
        switch(eventName) {
          case 'cv_prospect_all':
            aggregatedData[key].cvProspectAll = eventCount;
            break;
          case 'cv_seminar_all':
            aggregatedData[key].cvSeminarAll = eventCount;
            break;
          case 'cv_contract_all':
            aggregatedData[key].cvContractAll = eventCount;
            break;
        }
      });
      // 新しいデータを配列に変換
      let newRows = Object.values(aggregatedData).map(data => {
        const dateStr = data.dimensions[5]; // YYYYMMDD形式の日付文字列
        const formattedDate = dateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1/$2/$3'); // YYYY/MM/DD形式に変換
        return [
          formattedDate,
          data.dimensions[0],  // sessionSourceMedium
          data.dimensions[1],  // sessionManualCampaignName
          data.dimensions[2],  // sessionManualTerm (Keyword)
          data.dimensions[3],  // sessionManualAdContent
          data.dimensions[4],  // sessionGoogleAdsQuery
          data.cvProspectAll,
          data.cvSeminarAll,
          data.cvContractAll
        ];
      });
      // 既存のデータから過去8日分を削除し、新しいデータを追加
      const eightDaysAgo = new Date();
      eightDaysAgo.setDate(eightDaysAgo.getDate() - 8);
      const updatedData = existingData.filter(row => new Date(row[0]) < eightDaysAgo);
      updatedData.push(...newRows);
      // 日付でソート（昇順）
      updatedData.sort((a, b) => new Date(a[0]) - new Date(b[0]));
      // データをクリアしてから書き込み
      sheet.clearContents();
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      if (updatedData.length > 0) {
        sheet.getRange(2, 1, updatedData.length, headers.length).setValues(updatedData);
        // 数値列にカンマ区切りを設定（3つのCVカラム）
        sheet.getRange(2, 7, updatedData.length, 3).setNumberFormat('#,##0');
      }
      Logger.log('Report data updated for the last 7 days with source "meta" and medium "infeed" filter and sorted by date.');
    } else {
      Logger.log('No data returned from the report.');
    }
  } catch (e) {
    Logger.log('Error running report: ' + e);
  }
}
