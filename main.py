"""AI POD Dashboard — FastAPI server (local use).

Serves:
  GET  /                   → ai_pod_report.html  (summary)
  GET  /detail.html        → detail.html          (driver lookup)
  GET  /data/{file}.json   → pre-generated driver JSON files

Run:
  uvicorn main:app --host 0.0.0.0 --port 8001 --reload
"""
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app  = FastAPI(title="AI POD Dashboard")
HERE = Path(__file__).parent

# Serve pre-generated driver data files
DATA = HERE / "data"
DATA.mkdir(exist_ok=True)
app.mount("/data", StaticFiles(directory=DATA), name="data")


@app.get("/", include_in_schema=False)
def summary_page():
    return FileResponse(HERE / "ai_pod_report.html", media_type="text/html")


@app.get("/detail.html", include_in_schema=False)
def detail_page():
    return FileResponse(HERE / "detail.html", media_type="text/html")


# Alias without extension for convenience
@app.get("/detail", include_in_schema=False)
def detail_alias():
    return FileResponse(HERE / "detail.html", media_type="text/html")
