#!/usr/bin/env python3
"""FastAPI backend for AI POD feedback collection.

Endpoints:
    POST /feedback       - Submit feedback for a POD record
    GET  /feedback       - Get all feedback (for export)
    GET  /feedback/csv   - Export feedback as CSV download

Usage:
    uvicorn feedback_api:app --host 0.0.0.0 --port 8086 --reload
"""
from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: Run: pip install google-cloud-bigquery")
    raise

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BQ_PROJECT = "wmt-driver-insights"
BQ_DATASET = "Chirag_dx"
BQ_FEEDBACK_TABLE = "AI_POD_FEEDBACK"
BQ_FULL_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_FEEDBACK_TABLE}"

# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="AI POD Feedback API",
    description="Collect user feedback on AI POD verification results",
    version="1.0.0",
)

# Allow CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# BigQuery client (initialized lazily)
_bq_client: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=BQ_PROJECT)
    return _bq_client


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class FeedbackSubmission(BaseModel):
    sales_order_num: str
    po_num: str
    driver_id: str
    slot_date: str  # YYYY-MM-DD
    ai_result: str  # Original AI result (acceptable/unacceptable)
    feedback_correct: bool  # True = AI was correct, False = AI was wrong
    feedback_notes: Optional[str] = None
    feedback_user: Optional[str] = None  # Optional: who submitted


class FeedbackResponse(BaseModel):
    success: bool
    message: str
    row_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Ensure feedback table exists
# ---------------------------------------------------------------------------
def ensure_feedback_table():
    """Create the feedback table if it doesn't exist."""
    client = get_bq_client()
    
    schema = [
        bigquery.SchemaField("sales_order_num", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("po_num", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("driver_id", "STRING"),
        bigquery.SchemaField("slot_date", "DATE"),
        bigquery.SchemaField("ai_result", "STRING"),
        bigquery.SchemaField("feedback_correct", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("feedback_notes", "STRING"),
        bigquery.SchemaField("feedback_user", "STRING"),
        bigquery.SchemaField("feedback_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table_ref = bigquery.Table(BQ_FULL_TABLE, schema=schema)
    
    try:
        client.get_table(table_ref)
        print(f"[OK] Feedback table exists: {BQ_FULL_TABLE}")
    except Exception as e:
        # Table doesn't exist, create it
        try:
            table = client.create_table(table_ref)
            print(f"[OK] Created feedback table: {BQ_FULL_TABLE}")
        except Exception as create_err:
            # Table might have been created by another process
            if "Already Exists" in str(create_err):
                print(f"[OK] Feedback table already exists: {BQ_FULL_TABLE}")
            else:
                raise create_err


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    """Ensure feedback table exists on startup."""
    ensure_feedback_table()


@app.get("/")
async def root():
    return {"status": "ok", "service": "AI POD Feedback API"}


@app.post("/feedback", response_model=FeedbackResponse)
async def submit_feedback(feedback: FeedbackSubmission):
    """Submit feedback for a POD record."""
    try:
        client = get_bq_client()
        
        row = {
            "sales_order_num": feedback.sales_order_num,
            "po_num": feedback.po_num,
            "driver_id": feedback.driver_id,
            "slot_date": feedback.slot_date,
            "ai_result": feedback.ai_result,
            "feedback_correct": feedback.feedback_correct,
            "feedback_notes": feedback.feedback_notes or "",
            "feedback_user": feedback.feedback_user or os.environ.get("USERNAME", "unknown"),
            "feedback_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
        errors = client.insert_rows_json(BQ_FULL_TABLE, [row])
        
        if errors:
            raise HTTPException(status_code=500, detail=f"BQ insert error: {errors}")
        
        row_id = f"{feedback.sales_order_num}_{feedback.po_num}"
        return FeedbackResponse(
            success=True,
            message="Feedback submitted successfully",
            row_id=row_id,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feedback")
async def get_all_feedback(limit: int = 1000):
    """Get all feedback records."""
    try:
        client = get_bq_client()
        
        sql = f"""
            SELECT *
            FROM `{BQ_FULL_TABLE}`
            ORDER BY feedback_timestamp DESC
            LIMIT {limit}
        """
        
        rows = list(client.query(sql).result())
        
        return {
            "count": len(rows),
            "feedback": [
                {
                    "sales_order_num": r.sales_order_num,
                    "po_num": r.po_num,
                    "driver_id": r.driver_id,
                    "slot_date": str(r.slot_date) if r.slot_date else None,
                    "ai_result": r.ai_result,
                    "feedback_correct": r.feedback_correct,
                    "feedback_notes": r.feedback_notes,
                    "feedback_user": r.feedback_user,
                    "feedback_timestamp": r.feedback_timestamp.isoformat() if r.feedback_timestamp else None,
                }
                for r in rows
            ],
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feedback/csv")
async def export_feedback_csv():
    """Export all feedback as CSV download."""
    try:
        client = get_bq_client()
        
        sql = f"""
            SELECT
                sales_order_num,
                po_num,
                driver_id,
                CAST(slot_date AS STRING) AS slot_date,
                ai_result,
                feedback_correct,
                feedback_notes,
                feedback_user,
                CAST(feedback_timestamp AS STRING) AS feedback_timestamp
            FROM `{BQ_FULL_TABLE}`
            ORDER BY feedback_timestamp DESC
        """
        
        rows = list(client.query(sql).result())
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            "sales_order_num", "po_num", "driver_id", "slot_date",
            "ai_result", "feedback_correct", "feedback_notes",
            "feedback_user", "feedback_timestamp"
        ])
        
        # Data rows
        for r in rows:
            writer.writerow([
                r.sales_order_num,
                r.po_num,
                r.driver_id,
                r.slot_date,
                r.ai_result,
                r.feedback_correct,
                r.feedback_notes,
                r.feedback_user,
                r.feedback_timestamp,
            ])
        
        output.seek(0)
        
        filename = f"ai_pod_feedback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8086)
