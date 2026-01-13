// ver3.0 - BigQuery対応版
function getGA4Report7days() {
  // GA4のプロパティIDを入力してください（数値のみ）
  const propertyId = '331542258';

  // BigQuery設定
  const projectId = 'cyberace-mad';
  const datasetId = 'trocco_justsystem';
  const tableId = 'ga4_justsystems_google_ads_query';

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
    }]
  };

  try {
    // レポートの実行
    const report = AnalyticsData.Properties.runReport(request, 'properties/' + propertyId);

    // 結果の処理
    if (report && report.rows) {
      // テーブルが存在しない場合は作成
      ensureTableExists_(projectId, datasetId, tableId);

      // 取得日時
      const fetchedAt = new Date().toISOString();

      // データの集計
      const aggregatedData = {};
      report.rows.forEach(row => {
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

      // BigQueryに挿入するデータを作成
      const rows = Object.values(aggregatedData).map(data => {
        const dateStr = data.dimensions[4]; // YYYYMMDD形式の日付文字列
        const formattedDate = dateStr.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'); // YYYY-MM-DD形式に変換
        return {
          json: {
            date: formattedDate,
            source_medium: data.dimensions[0],
            campaign: data.dimensions[1],
            keyword: data.dimensions[2],
            google_ads_query: data.dimensions[3],
            cv_prospect_all: data.cvProspectAll,
            cv_seminar_all: data.cvSeminarAll,
            cv_contract_all: data.cvContractAll,
            fetched_at: fetchedAt
          }
        };
      });

      // BigQueryにデータを挿入
      if (rows.length > 0) {
        insertRowsToBigQuery_(projectId, datasetId, tableId, rows);
        Logger.log(rows.length + ' 行のデータをBigQueryに挿入しました。');

        // 重複削除
        deduplicateTable_(projectId, datasetId, tableId);
        Logger.log('重複データを削除しました。');
      }

      Logger.log('BigQueryへのデータ更新が完了しました。');
    } else {
      Logger.log('No data returned from the report.');
    }
  } catch (e) {
    Logger.log('Error running report: ' + e);
  }
}

/**
 * テーブルが存在しない場合は作成する
 */
function ensureTableExists_(projectId, datasetId, tableId) {
  try {
    BigQuery.Tables.get(projectId, datasetId, tableId);
    Logger.log('テーブルは既に存在します: ' + tableId);
  } catch (e) {
    // テーブルが存在しない場合は作成
    const table = {
      tableReference: {
        projectId: projectId,
        datasetId: datasetId,
        tableId: tableId
      },
      schema: {
        fields: [
          {name: 'date', type: 'DATE', mode: 'REQUIRED'},
          {name: 'source_medium', type: 'STRING', mode: 'NULLABLE'},
          {name: 'campaign', type: 'STRING', mode: 'NULLABLE'},
          {name: 'keyword', type: 'STRING', mode: 'NULLABLE'},
          {name: 'google_ads_query', type: 'STRING', mode: 'NULLABLE'},
          {name: 'cv_prospect_all', type: 'INTEGER', mode: 'NULLABLE'},
          {name: 'cv_seminar_all', type: 'INTEGER', mode: 'NULLABLE'},
          {name: 'cv_contract_all', type: 'INTEGER', mode: 'NULLABLE'},
          {name: 'fetched_at', type: 'TIMESTAMP', mode: 'REQUIRED'}
        ]
      }
    };
    BigQuery.Tables.insert(table, projectId, datasetId);
    Logger.log('テーブルを作成しました: ' + tableId);
  }
}

/**
 * BigQueryにデータを挿入する
 */
function insertRowsToBigQuery_(projectId, datasetId, tableId, rows) {
  const insertAllRequest = {
    rows: rows
  };

  const response = BigQuery.Tabledata.insertAll(insertAllRequest, projectId, datasetId, tableId);

  if (response.insertErrors && response.insertErrors.length > 0) {
    Logger.log('挿入エラー: ' + JSON.stringify(response.insertErrors));
    throw new Error('BigQueryへの挿入中にエラーが発生しました');
  }
}

/**
 * 重複データを削除する（date, source_medium, campaign, keyword, google_ads_queryが同じ行で最新のfetched_atのみ残す）
 */
function deduplicateTable_(projectId, datasetId, tableId) {
  const query = `
    CREATE OR REPLACE TABLE \`${projectId}.${datasetId}.${tableId}\` AS
    SELECT * EXCEPT(row_num)
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY date, source_medium, campaign, keyword, google_ads_query
          ORDER BY fetched_at DESC
        ) as row_num
      FROM \`${projectId}.${datasetId}.${tableId}\`
    )
    WHERE row_num = 1
  `;

  const request = {
    query: query,
    useLegacySql: false
  };

  BigQuery.Jobs.query(request, projectId);
}
