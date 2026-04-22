/**
 * Google Apps Script - AI POD Feedback API
 * 
 * This script acts as a serverless backend for collecting feedback.
 * Deploy as Web App: Deploy > New deployment > Web app > Anyone can access
 * 
 * Copy the Web App URL and update FEEDBACK_API in detail.html
 */

const BQ_PROJECT = 'wmt-driver-insights';
const BQ_DATASET = 'Chirag_dx';
const BQ_TABLE = 'AI_POD_FEEDBACK';

/**
 * Handle POST requests (submit feedback)
 */
function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    
    // Validate required fields
    if (!data.sales_order_num || !data.po_num) {
      return jsonResponse({ success: false, message: 'Missing required fields' }, 400);
    }
    
    // Insert into BigQuery
    const row = {
      sales_order_num: data.sales_order_num,
      po_num: data.po_num,
      driver_id: data.driver_id || '',
      slot_date: data.slot_date || null,
      ai_result: data.ai_result || '',
      feedback_correct: data.feedback_correct === true,
      feedback_notes: data.feedback_notes || '',
      feedback_user: data.feedback_user || Session.getActiveUser().getEmail() || 'unknown',
      feedback_timestamp: new Date().toISOString(),
    };
    
    insertRowToBigQuery(row);
    
    return jsonResponse({
      success: true,
      message: 'Feedback submitted successfully',
      row_id: `${data.sales_order_num}_${data.po_num}`
    });
    
  } catch (error) {
    console.error('Error in doPost:', error);
    return jsonResponse({ success: false, message: error.toString() }, 500);
  }
}

/**
 * Handle GET requests (health check or export)
 */
function doGet(e) {
  const action = e.parameter.action;
  
  if (action === 'export') {
    return exportFeedbackCSV();
  }
  
  // Health check
  return jsonResponse({ status: 'ok', service: 'AI POD Feedback API (Apps Script)' });
}

/**
 * Insert a row into BigQuery
 */
function insertRowToBigQuery(row) {
  const tableId = `${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}`;
  
  // Format for BigQuery insertAll
  const insertRequest = {
    rows: [{
      insertId: `${row.sales_order_num}_${row.po_num}_${Date.now()}`,
      json: row
    }]
  };
  
  const response = BigQuery.Tabledata.insertAll(insertRequest, BQ_PROJECT, BQ_DATASET, BQ_TABLE);
  
  if (response.insertErrors && response.insertErrors.length > 0) {
    throw new Error('BigQuery insert error: ' + JSON.stringify(response.insertErrors));
  }
  
  return response;
}

/**
 * Export all feedback as CSV
 */
function exportFeedbackCSV() {
  const query = `
    SELECT *
    FROM \`${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\`
    ORDER BY feedback_timestamp DESC
    LIMIT 10000
  `;
  
  const request = { query: query, useLegacySql: false };
  const result = BigQuery.Jobs.query(request, BQ_PROJECT);
  
  if (!result.rows || result.rows.length === 0) {
    return ContentService.createTextOutput('No feedback data').setMimeType(ContentService.MimeType.TEXT);
  }
  
  // Build CSV
  const headers = result.schema.fields.map(f => f.name);
  let csv = headers.join(',') + '\n';
  
  result.rows.forEach(row => {
    const values = row.f.map(cell => {
      const val = cell.v || '';
      // Escape quotes and wrap in quotes if contains comma
      if (val.toString().includes(',') || val.toString().includes('"')) {
        return '"' + val.toString().replace(/"/g, '""') + '"';
      }
      return val;
    });
    csv += values.join(',') + '\n';
  });
  
  return ContentService.createTextOutput(csv).setMimeType(ContentService.MimeType.CSV);
}

/**
 * Helper: Create JSON response with CORS headers
 */
function jsonResponse(data, statusCode = 200) {
  const output = ContentService.createTextOutput(JSON.stringify(data));
  output.setMimeType(ContentService.MimeType.JSON);
  return output;
}

/**
 * Test function - run this to verify BigQuery connection
 */
function testConnection() {
  try {
    const query = `SELECT COUNT(*) as count FROM \`${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\``;
    const request = { query: query, useLegacySql: false };
    const result = BigQuery.Jobs.query(request, BQ_PROJECT);
    console.log('Connection test successful. Row count:', result.rows[0].f[0].v);
    return true;
  } catch (error) {
    console.error('Connection test failed:', error);
    return false;
  }
}
