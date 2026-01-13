// 日付指定でGA4レポートをBigQueryに保存

// 設定
const CONFIG_DATERANGE = {
  propertyId: '331542258',
  projectId: 'cyberace-mad',
  datasetId: 'trocco_justsystem',
  tableId: 'ga4_justsystems_google_ads_query',
  tempTableId: 'ga4_justsystems_google_ads_query_temp',
  // ↓ここで日付を指定（YYYY-MM-DD形式）
  startDate: '2025-01-01',
  endDate: '2025-01-01'
};

/**
 * 日付指定でレポートを取得してBigQueryに保存（日付ごとにループ）
 */
function getGA4ReportByDateRange() {
  const { propertyId, projectId, datasetId, tableId, tempTableId, startDate, endDate } = CONFIG_DATERANGE;

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

  // tempテーブルを作成（または再作成）
  createTempTable_daterange_(projectId, datasetId, tempTableId);

  let totalRows = 0;

  // 日付ごとにループ
  for (const targetDate of dates) {
    const rowCount = fetchAndInsertForDate_daterange_(propertyId, projectId, datasetId, tempTableId, targetDate);
    totalRows += rowCount;
  }

  Logger.log('合計 ' + totalRows + ' 行のデータをtempテーブルに挿入しました。');

  // tempテーブルから本テーブルへ重複除外してマージ
  if (totalRows > 0) {
    mergeToMainTable_daterange_(projectId, datasetId, tableId, tempTableId);
    Logger.log('本テーブルへのマージが完了しました。');
  }

  // tempテーブルを削除
  deleteTempTable_daterange_(projectId, datasetId, tempTableId);

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
 * 指定日のデータを取得してtempテーブルに挿入
 */
function fetchAndInsertForDate_daterange_(propertyId, projectId, datasetId, tempTableId, targetDate) {
  const fetchedAt = new Date().toISOString();
  let allRows = [];
  let offset = 0;
  const limit = 100000;

  // ページネーションループ
  while (true) {
    const request = {
      property: 'properties/' + propertyId,
      dimensions: [
        {name: 'googleAdsCampaignName'},
        {name: 'googleAdsAdGroupName'},
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

      // サンプリング情報をログ出力
      if (offset === 0) {
        if (report.metadata) {
          if (report.metadata.samplingMetadatas && report.metadata.samplingMetadatas.length > 0) {
            Logger.log(targetDate + ': ⚠️ サンプリングあり: ' + JSON.stringify(report.metadata.samplingMetadatas));
          } else {
            Logger.log(targetDate + ': ✓ サンプリングなし');
          }
          if (report.metadata.dataLossFromOtherRow) {
            Logger.log(targetDate + ': ⚠️ (other)行へのデータ損失あり');
          }
        }
        if (report.rowCount) {
          Logger.log(targetDate + ': 総行数(rowCount): ' + report.rowCount);
        }
      }

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

  // GA4データをそのまま変換（集計なし）
  const rows = allRows.map(row => {
    const dateStr = row.dimensionValues[3].value;
    const formattedDate = dateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
    return {
      json: {
        date: formattedDate,
        google_ads_campaign_name: row.dimensionValues[0].value,
        google_ads_ad_group_name: row.dimensionValues[1].value,
        google_ads_query: row.dimensionValues[2].value,
        event_name: row.dimensionValues[4].value,
        event_count: parseInt(row.metricValues[0].value),
        fetched_at: fetchedAt
      }
    };
  });

  if (rows.length > 0) {
    insertRowsToBigQuery_daterange_(projectId, datasetId, tempTableId, rows);
    Logger.log(targetDate + ': ' + rows.length + ' 行を挿入');
  }

  return rows.length;
}

/**
 * tempテーブルを作成（既存の場合は削除して再作成）
 */
function createTempTable_daterange_(projectId, datasetId, tempTableId) {
  // 既存のtempテーブルを削除
  try {
    BigQuery.Tables.remove(projectId, datasetId, tempTableId);
    Logger.log('既存のtempテーブルを削除しました');
  } catch (e) {
    // テーブルが存在しない場合は無視
  }

  // tempテーブルを作成
  const table = {
    tableReference: {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tempTableId
    },
    schema: {
      fields: [
        {name: 'date', type: 'DATE', mode: 'REQUIRED'},
        {name: 'google_ads_campaign_name', type: 'STRING', mode: 'NULLABLE'},
        {name: 'google_ads_ad_group_name', type: 'STRING', mode: 'NULLABLE'},
        {name: 'google_ads_query', type: 'STRING', mode: 'NULLABLE'},
        {name: 'event_name', type: 'STRING', mode: 'NULLABLE'},
        {name: 'event_count', type: 'INTEGER', mode: 'NULLABLE'},
        {name: 'fetched_at', type: 'TIMESTAMP', mode: 'REQUIRED'}
      ]
    }
  };
  BigQuery.Tables.insert(table, projectId, datasetId);
  Logger.log('tempテーブルを作成しました: ' + tempTableId);
}

function insertRowsToBigQuery_daterange_(projectId, datasetId, tableId, rows) {
  const response = BigQuery.Tabledata.insertAll({rows: rows}, projectId, datasetId, tableId);
  if (response.insertErrors && response.insertErrors.length > 0) {
    Logger.log('挿入エラー: ' + JSON.stringify(response.insertErrors));
    throw new Error('BigQueryへの挿入中にエラーが発生しました');
  }
}

/**
 * tempテーブルを削除
 */
function deleteTempTable_daterange_(projectId, datasetId, tempTableId) {
  try {
    BigQuery.Tables.remove(projectId, datasetId, tempTableId);
    Logger.log('tempテーブルを削除しました: ' + tempTableId);
  } catch (e) {
    Logger.log('tempテーブル削除エラー: ' + e);
  }
}

/**
 * tempテーブルから本テーブルへ重複除外してマージ
 */
function mergeToMainTable_daterange_(projectId, datasetId, tableId, tempTableId) {
  // 本テーブルが存在するか確認
  let tableExists = false;
  try {
    BigQuery.Tables.get(projectId, datasetId, tableId);
    tableExists = true;
  } catch (e) {
    tableExists = false;
  }

  let query;
  if (tableExists) {
    // 既存テーブルがある場合：UNION ALLしてマージ
    query = `
      CREATE OR REPLACE TABLE \`${projectId}.${datasetId}.${tableId}\`
      PARTITION BY date
      AS
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY date, google_ads_campaign_name, google_ads_ad_group_name, google_ads_query, event_name
            ORDER BY fetched_at DESC
          ) as row_num
        FROM (
          SELECT * FROM \`${projectId}.${datasetId}.${tableId}\`
          UNION ALL
          SELECT * FROM \`${projectId}.${datasetId}.${tempTableId}\`
        )
      )
      WHERE row_num = 1
    `;
  } else {
    // 本テーブルがない場合：tempテーブルからそのまま作成
    query = `
      CREATE OR REPLACE TABLE \`${projectId}.${datasetId}.${tableId}\`
      PARTITION BY date
      AS
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY date, google_ads_campaign_name, google_ads_ad_group_name, google_ads_query, event_name
            ORDER BY fetched_at DESC
          ) as row_num
        FROM \`${projectId}.${datasetId}.${tempTableId}\`
      )
      WHERE row_num = 1
    `;
    Logger.log('本テーブルが存在しないため、新規作成します');
  }

  BigQuery.Jobs.query({query: query, useLegacySql: false}, projectId);
}
