#!/usr/bin/env python3
"""AI POD Dashboard — Daily Refresh Script (Elements-compatible).

This script:
  1. Queries BigQuery: 90-day summary + 14-day order-level detail
  2. Generates ai_pod_report.html with embedded data
  3. Writes 256 driver shard files into data/
  4. Commits and pushes everything to GitHub Pages

Usage:
    python refresh_report.py

Requirements:
    pip install google-cloud-bigquery db-dtypes pyarrow

Environment variables (optional):
    GOOGLE_APPLICATION_CREDENTIALS  — path to GCP service account JSON key
                                       (omit if using gcloud ADC or Workload Identity)
    GITHUB_PAT                       — PAT for git push (if not embedded in remote URL)

Scheduling on Elements (cron example):
    0 9 * * * cd /path/to/ai_pod_dashboard && python refresh_report.py >> refresh.log 2>&1
"""

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: google-cloud-bigquery not installed.")
    print("Run: pip install google-cloud-bigquery db-dtypes pyarrow")
    sys.exit(1)

# Import everything we need from the main generation module — DRY.
try:
    from generate_report import (
        fetch_data,
        fetch_orders,
        write_driver_files,
        generate_html,
        OUT_FILE,
        DATA_DIR,
    )
except ImportError as e:
    print(f"ERROR: Could not import generate_report.py: {e}")
    print("Make sure refresh_report.py is in the same directory as generate_report.py")
    sys.exit(1)

# __file__ is undefined in Jupyter / Elements notebooks — fall back to cwd.
# Make sure your notebook kernel is started from inside the repo directory.
try:
    REPO_DIR = Path(__file__).parent.resolve()
except NameError:
    REPO_DIR = Path.cwd()

BRANCH = "main"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line, flush=True)


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------
def _git(*args) -> tuple[int, str, str]:
    result = subprocess.run(
        ["git"] + list(args),
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def git_commit_and_push() -> bool:
    """Stage ai_pod_report.html + all shard files, commit, push.

    Returns True if a push was made, False if nothing changed.
    """
    rc, changed, _ = _git("status", "--porcelain")
    if not changed:
        log("No changes detected — data unchanged since last refresh.")
        return False

    log(f"Changed files: {len(changed.splitlines())} file(s)")

    # Stage HTML + shard data directory
    rc, _, err = _git("add", "ai_pod_report.html", "data")
    if rc != 0:
        raise RuntimeError(f"git add failed: {err}")

    # Commit
    ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"chore: daily refresh [{ts}]"
    rc, _, err = _git("commit", "-m", msg)
    if rc != 0:
        raise RuntimeError(f"git commit failed: {err}")
    log(f"Committed: {msg}")

    # Push
    rc, _, err = _git("push", "origin", BRANCH)
    if rc != 0:
        raise RuntimeError(f"git push failed: {err}")
    log(f"Pushed to origin/{BRANCH} — GitHub Pages will deploy in ~2 min.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    sep = "=" * 62
    log(sep)
    log("AI POD Dashboard Refresh — Starting")
    log(sep)

    # 1. BigQuery — 90-day summary
    log("Connecting to BigQuery...")
    client = bigquery.Client()

    log("Query 1/2: AI_POD_VERIFICATION daily grain (last 90 days)...")
    driver_ids, max_date, compact = fetch_data(client)
    log(f"  max_date={max_date}  drivers={len(driver_ids):,}  rows={len(compact):,}")

    # 2. BigQuery — 14-day order-level detail
    log("Query 2/2: Order-level detail (last 14 days)...")
    by_driver = fetch_orders(client)
    gen_ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    n_drivers = write_driver_files(by_driver, gen_ts)
    log(f"  {n_drivers:,} drivers → {len(list(DATA_DIR.glob('*.json')))} shard files written.")

    # 3. Generate summary HTML
    log("Generating ai_pod_report.html...")
    html = generate_html(driver_ids, max_date, compact)
    OUT_FILE.write_text(html, encoding="utf-8")
    size_kb = OUT_FILE.stat().st_size / 1024
    log(f"  Saved {OUT_FILE.name} ({size_kb:.0f} KB)")

    # 4. Commit + push
    log("Committing and pushing to GitHub Pages...")
    pushed = git_commit_and_push()

    log(sep)
    if pushed:
        log("SUCCESS — Dashboard updated on GitHub Pages!")
        log("  Summary : https://harshalbhuse-dot.github.io/ai-pod-dashboard/ai_pod_report.html")
        log("  Lookup  : https://harshalbhuse-dot.github.io/ai-pod-dashboard/detail.html")
    else:
        log("DONE — No data changes, nothing pushed.")
    log(sep)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"FATAL: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
