"""FastAPI backend for AI POD Feedback - shared multi-user storage.

Run: uvicorn feedback_api:app --host 0.0.0.0 --port 8086
Endpoints:
  GET  /feedback?so={so}&po={po}  - Get feedback for a specific order
  GET  /feedback/bulk?keys=so1|po1,so2|po2,...  - Get multiple feedbacks
  POST /feedback  - Submit feedback
  GET  /health    - Health check
"""

import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import bigquery

app = FastAPI(title="AI POD Feedback API", version="1.0.0")

# Allow CORS for GitHub Pages
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, lock this down
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# BigQuery config
PROJECT_ID = "wmt-driver-insights"
DATASET = "Chirag_dx"
TABLE = "AI_POD_FEEDBACK"
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"

client = bigquery.Client(project=PROJECT_ID)


class FeedbackSubmission(BaseModel):
    sales_order_num: str
    po_num: str
    driver_id: str
    slot_date: str  # YYYY-MM-DD
    ai_result: str
    feedback_correct: bool
    feedback_notes: Optional[str] = ""
    feedback_user: Optional[str] = "anonymous"


class FeedbackResponse(BaseModel):
    sales_order_num: str
    po_num: str
    driver_id: Optional[str] = None
    slot_date: Optional[str] = None
    ai_result: Optional[str] = None
    feedback_correct: Optional[bool] = None
    feedback_notes: Optional[str] = None
    feedback_user: Optional[str] = None
    feedback_timestamp: Optional[str] = None
    found: bool = False


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "ai-pod-feedback"}


@app.get("/feedback", response_model=FeedbackResponse)
def get_feedback(so: str = Query(..., description="Sales Order Number"),
                 po: str = Query(..., description="PO Number")):
    """Get feedback for a single order."""
    query = f"""
    SELECT *
    FROM `{FULL_TABLE}`
    WHERE sales_order_num = @so AND po_num = @po
    ORDER BY feedback_timestamp DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("so", "STRING", so),
            bigquery.ScalarQueryParameter("po", "STRING", po),
        ]
    )
    results = list(client.query(query, job_config=job_config).result())
    
    if not results:
        return FeedbackResponse(sales_order_num=so, po_num=po, found=False)
    
    row = results[0]
    return FeedbackResponse(
        sales_order_num=row.sales_order_num,
        po_num=row.po_num,
        driver_id=row.driver_id,
        slot_date=str(row.slot_date) if row.slot_date else None,
        ai_result=row.ai_result,
        feedback_correct=row.feedback_correct,
        feedback_notes=row.feedback_notes,
        feedback_user=row.feedback_user,
        feedback_timestamp=str(row.feedback_timestamp) if row.feedback_timestamp else None,
        found=True
    )


@app.get("/feedback/bulk")
def get_feedback_bulk(keys: str = Query(..., description="Comma-separated so|po pairs")):
    """Get feedback for multiple orders at once.
    
    keys format: "so1|po1,so2|po2,so3|po3"
    Returns: {"so1|po1": {...}, "so2|po2": {...}, ...}
    """
    pairs = [k.strip().split("|") for k in keys.split(",") if "|" in k]
    if not pairs:
        return {}
    
    # Build WHERE clause for all pairs
    conditions = " OR ".join(
        f"(sales_order_num = '{so}' AND po_num = '{po}')"
        for so, po in pairs
    )
    
    query = f"""
    SELECT *
    FROM `{FULL_TABLE}`
    WHERE {conditions}
    """
    
    results = {}
    for row in client.query(query).result():
        key = f"{row.sales_order_num}|{row.po_num}"
        # Keep latest feedback per key
        if key not in results or (row.feedback_timestamp and 
            (not results[key].get("feedback_timestamp") or 
             str(row.feedback_timestamp) > results[key]["feedback_timestamp"])):
            results[key] = {
                "sales_order_num": row.sales_order_num,
                "po_num": row.po_num,
                "driver_id": row.driver_id,
                "slot_date": str(row.slot_date) if row.slot_date else None,
                "ai_result": row.ai_result,
                "feedback_correct": row.feedback_correct,
                "feedback_notes": row.feedback_notes,
                "feedback_user": row.feedback_user,
                "feedback_timestamp": str(row.feedback_timestamp) if row.feedback_timestamp else None,
                "found": True
            }
    
    return results


@app.post("/feedback")
def submit_feedback(fb: FeedbackSubmission):
    """Submit or update feedback for an order."""
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Use MERGE to upsert (update if exists, insert if not)
    query = f"""
    MERGE `{FULL_TABLE}` T
    USING (SELECT @so AS so, @po AS po) S
    ON T.sales_order_num = S.so AND T.po_num = S.po
    WHEN MATCHED THEN
      UPDATE SET
        driver_id = @driver_id,
        slot_date = @slot_date,
        ai_result = @ai_result,
        feedback_correct = @feedback_correct,
        feedback_notes = @feedback_notes,
        feedback_user = @feedback_user,
        feedback_timestamp = @timestamp
    WHEN NOT MATCHED THEN
      INSERT (sales_order_num, po_num, driver_id, slot_date, ai_result,
              feedback_correct, feedback_notes, feedback_user, feedback_timestamp)
      VALUES (@so, @po, @driver_id, @slot_date, @ai_result,
              @feedback_correct, @feedback_notes, @feedback_user, @timestamp)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("so", "STRING", fb.sales_order_num),
            bigquery.ScalarQueryParameter("po", "STRING", fb.po_num),
            bigquery.ScalarQueryParameter("driver_id", "STRING", fb.driver_id),
            bigquery.ScalarQueryParameter("slot_date", "DATE", fb.slot_date),
            bigquery.ScalarQueryParameter("ai_result", "STRING", fb.ai_result),
            bigquery.ScalarQueryParameter("feedback_correct", "BOOL", fb.feedback_correct),
            bigquery.ScalarQueryParameter("feedback_notes", "STRING", fb.feedback_notes or ""),
            bigquery.ScalarQueryParameter("feedback_user", "STRING", fb.feedback_user or "anonymous"),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
        ]
    )
    
    try:
        client.query(query, job_config=job_config).result()
        return {
            "success": True,
            "message": "Feedback saved",
            "key": f"{fb.sales_order_num}|{fb.po_num}",
            "timestamp": timestamp
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8086)
