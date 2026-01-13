// 日付指定でGA4レポートをBigQueryに保存

// 設定
const CONFIG_DATERANGE = {
  propertyId: '331542258',
  projectId: 'cyberace-mad',
  datasetId: 'trocco_justsystem',
  tableId: 'ga4_justsystems_google_ads_query',
  // ↓ここで日付を指定（YYYY-MM-DD形式）
  startDate: '2026-01-01',
  endDate: '2026-01-05'
};

/**
 * 日付指定でレポートを取得してBigQueryに保存（日付ごとにループ）
 */
function getGA4ReportByDateRange() {
  const { propertyId, projectId, datasetId, tableId, startDate, endDate } = CONFIG_DATERANGE;

  if (propertyId === 'YOUR_GA4_PROPERTY_ID' || isNaN(propertyId)) {
    Logger.log('エラー: 有効なGA4プロパティIDを設定してください。');
    return;
  }

  if (!startDate || !endDate) {
    Logger.log('エラー: CONFIG_DATERANGEのstartDateとendDateを設定してください（YYYY-MM-DD形式）');
    return;
  }

  // 日付リストを生成
  const dates = getDateRange_(startDate, endDate);
  Logger.log('取得期間: ' + startDate + ' 〜 ' + endDate + ' (' + dates.length + '日間)');

  // テーブル確認
  ensureTableExists_daterange_(projectId, datasetId, tableId);

  let totalRows = 0;

  // 日付ごとにループ
  for (const targetDate of dates) {
    const rowCount = fetchAndInsertForDate_daterange_(propertyId, projectId, datasetId, tableId, targetDate);
    totalRows += rowCount;
  }

  Logger.log('合計 ' + totalRows + ' 行のデータを挿入しました。');

  // 重複削除
  if (totalRows > 0) {
    deduplicateTable_daterange_(projectId, datasetId, tableId);
    Logger.log('重複データを削除しました。');
  }

  Logger.log('BigQueryへのデータ更新が完了しました。');
}

/**
 * 開始日から終了日までの日付配列を生成
 */
function getDateRange_(startDate, endDate) {
  const dates = [];
  const start = new Date(startDate);
  const end = new Date(endDate);

  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    dates.push(formatDate_daterange_(new Date(d)));
  }

  return dates;
}

/**
 * 日付をYYYY-MM-DD形式にフォーマット
 */
function formatDate_daterange_(date) {
  const y = date.getFullYear();
  const m = ('0' + (date.getMonth() + 1)).slice(-2);
  const d = ('0' + date.getDate()).slice(-2);
  return y + '-' + m + '-' + d;
}

/**
 * 指定日のデータを取得してBigQueryに挿入
 */
function fetchAndInsertForDate_daterange_(propertyId, projectId, datasetId, tableId, targetDate) {
  const fetchedAt = new Date().toISOString();
  let allRows = [];
  let offset = 0;
  const limit = 100000;

  // ページネーションループ
  while (true) {
    const request = {
      property: 'properties/' + propertyId,
      dimensions: [
        {name: 'sessionSourceMedium'},
        {name: 'sessionManualCampaignName'},
        {name: 'sessionManualTerm'},
        {name: 'sessionGoogleAdsQuery'},
        {name: 'date'},
        {name: 'eventName'}
      ],
      metrics: [
        {name: 'eventCount'}
      ],
      dateRanges: [{
        startDate: targetDate,
        endDate: targetDate
      }],
      limit: limit,
      offset: offset
    };

    try {
      const report = AnalyticsData.Properties.runReport(request, 'properties/' + propertyId);

      if (!report || !report.rows || report.rows.length === 0) {
        break;
      }

      Logger.log(targetDate + ': API取得 ' + report.rows.length + ' 行 (offset: ' + offset + ')');
      allRows = allRows.concat(report.rows);

      if (report.rows.length < limit) {
        break;
      }
      offset += limit;
    } catch (e) {
      Logger.log('Error fetching ' + targetDate + ': ' + e);
      break;
    }
  }

  if (allRows.length === 0) {
    Logger.log(targetDate + ': データなし');
    return 0;
  }

  // データの集計
  const aggregatedData = {};
  allRows.forEach(row => {
    const key = row.dimensionValues.slice(0, 5).map(d => d.value).join('|');
    if (!aggregatedData[key]) {
      aggregatedData[key] = {
        dimensions: row.dimensionValues.slice(0, 5).map(d => d.value),
        cvProspectAll: 0,
        cvSeminarAll: 0,
        cvContractAll: 0
      };
    }

    const eventName = row.dimensionValues[5].value;
    const eventCount = parseInt(row.metricValues[0].value);

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

  const rows = Object.values(aggregatedData).map(data => {
    const dateStr = data.dimensions[4];
    const formattedDate = dateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
    return {
      json: {
        date: formattedDate,
        session_source_medium: data.dimensions[0],
        session_manual_campaign_name: data.dimensions[1],
        session_manual_term: data.dimensions[2],
        session_google_ads_query: data.dimensions[3],
        cv_prospect_all: data.cvProspectAll,
        cv_seminar_all: data.cvSeminarAll,
        cv_contract_all: data.cvContractAll,
        fetched_at: fetchedAt
      }
    };
  });

  if (rows.length > 0) {
    insertRowsToBigQuery_daterange_(projectId, datasetId, tableId, rows);
    Logger.log(targetDate + ': ' + rows.length + ' 行を挿入');
  }

  return rows.length;
}

function ensureTableExists_daterange_(projectId, datasetId, tableId) {
  try {
    BigQuery.Tables.get(projectId, datasetId, tableId);
    Logger.log('テーブルは既に存在します: ' + tableId);
  } catch (e) {
    const table = {
      tableReference: {
        projectId: projectId,
        datasetId: datasetId,
        tableId: tableId
      },
      schema: {
        fields: [
          {name: 'date', type: 'DATE', mode: 'REQUIRED'},
          {name: 'session_source_medium', type: 'STRING', mode: 'NULLABLE'},
          {name: 'session_manual_campaign_name', type: 'STRING', mode: 'NULLABLE'},
          {name: 'session_manual_term', type: 'STRING', mode: 'NULLABLE'},
          {name: 'session_google_ads_query', type: 'STRING', mode: 'NULLABLE'},
          {name: 'cv_prospect_all', type: 'INTEGER', mode: 'NULLABLE'},
          {name: 'cv_seminar_all', type: 'INTEGER', mode: 'NULLABLE'},
          {name: 'cv_contract_all', type: 'INTEGER', mode: 'NULLABLE'},
          {name: 'fetched_at', type: 'TIMESTAMP', mode: 'REQUIRED'}
        ]
      },
      timePartitioning: {
        type: 'DAY',
        field: 'date'
      }
    };
    BigQuery.Tables.insert(table, projectId, datasetId);
    Logger.log('テーブルを作成しました: ' + tableId);
  }
}

function insertRowsToBigQuery_daterange_(projectId, datasetId, tableId, rows) {
  const response = BigQuery.Tabledata.insertAll({rows: rows}, projectId, datasetId, tableId);
  if (response.insertErrors && response.insertErrors.length > 0) {
    Logger.log('挿入エラー: ' + JSON.stringify(response.insertErrors));
    throw new Error('BigQueryへの挿入中にエラーが発生しました');
  }
}

function deduplicateTable_daterange_(projectId, datasetId, tableId) {
  const query = `
    DELETE FROM \`${projectId}.${datasetId}.${tableId}\`
    WHERE STRUCT(date, session_source_medium, session_manual_campaign_name, session_manual_term, session_google_ads_query, fetched_at)
    NOT IN (
      SELECT AS STRUCT date, session_source_medium, session_manual_campaign_name, session_manual_term, session_google_ads_query, MAX(fetched_at) as fetched_at
      FROM \`${projectId}.${datasetId}.${tableId}\`
      GROUP BY date, session_source_medium, session_manual_campaign_name, session_manual_term, session_google_ads_query
    )
  `;
  BigQuery.Jobs.query({query: query, useLegacySql: false}, projectId);
}
