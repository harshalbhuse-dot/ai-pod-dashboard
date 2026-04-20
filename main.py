"""AI POD Dashboard — FastAPI server.

Serves:
  GET  /         → summary dashboard (pre-generated static HTML)
  GET  /detail   → driver detail lookup page
  GET  /api/driver?driver_id=&date_from=&date_to=  → JSON row data

Run:
  uvicorn main:app --host 0.0.0.0 --port 8001 --reload
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None  # type: ignore

app   = FastAPI(title="AI POD Dashboard")
HERE  = Path(__file__).parent
BQ_TABLE = "wmt-driver-insights.Chirag_dx.AI_POD_VERIFICATION"

_client: Optional[object] = None  # lazy-init BigQuery client


def _bq() -> "bigquery.Client":
    global _client
    if _client is None:
        if bigquery is None:
            raise HTTPException(500, "google-cloud-bigquery not installed")
        _client = bigquery.Client()
    return _client  # type: ignore


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def summary_page():
    html = HERE / "ai_pod_report.html"
    if not html.exists():
        return JSONResponse(
            {"error": "Report not generated yet. Run: python generate_report.py"},
            status_code=503,
        )
    return FileResponse(html, media_type="text/html")


@app.get("/detail", include_in_schema=False)
def detail_page():
    return FileResponse(HERE / "templates" / "detail.html", media_type="text/html")


# ---------------------------------------------------------------------------
# API — driver detail query
# ---------------------------------------------------------------------------
@app.get("/api/driver")
def driver_detail(
    driver_id: str  = Query(..., description="Driver user ID"),
    date_from:  str = Query(..., description="Start date YYYY-MM-DD"),
    date_to:    str = Query(..., description="End date   YYYY-MM-DD"),
):
    if not driver_id.strip():
        raise HTTPException(400, "driver_id is required")

    sql = f"""
        SELECT
            DRVR_USER_ID                                          AS driver_id,
            SRC_SALES_ORDER_NUM                                   AS sales_order_num,
            PO_NUM                                                AS po_num,
            CAST(created_date AS STRING)                         AS created_date,
            COALESCE(pod_url, '')                                AS pod_url,
            COALESCE(ai_result, '')                              AS ai_result,
            COALESCE(LOWER(suspected_fraud), '')                 AS suspected_fraud,
            COALESCE(LOWER(photo_taken_inside_vehicle), '')      AS inside_vehicle,
            COALESCE(LOWER(profanity_detected), '')              AS profanity,
            COALESCE(Missing_PO, 0)                              AS missing_po
        FROM `{BQ_TABLE}`
        WHERE DRVR_USER_ID    = @driver_id
          AND created_date   BETWEEN @date_from AND @date_to
        ORDER BY created_date DESC, SRC_SALES_ORDER_NUM
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("driver_id", "STRING", driver_id.strip()),
            bigquery.ScalarQueryParameter("date_from", "DATE",   date_from),
            bigquery.ScalarQueryParameter("date_to",   "DATE",   date_to),
        ]
    )

    rows = list(_bq().query(sql, job_config=job_config).result())
    data = [dict(r) for r in rows]
    return {"rows": data, "count": len(data)}
