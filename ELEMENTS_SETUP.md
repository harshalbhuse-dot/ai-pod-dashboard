# AI POD Dashboard — Elements Deployment Guide

This guide removes the laptop dependency by running the daily refresh on **Elements** (Walmart's internal job scheduler). Elements lives inside the corporate network, so it can reach BigQuery natively — no VPN required.

**Live Dashboard:**
- Summary : https://harshalbhuse-dot.github.io/ai-pod-dashboard/ai_pod_report.html
- Driver Lookup: https://harshalbhuse-dot.github.io/ai-pod-dashboard/detail.html

---

## How It Works

```
Elements cron (daily 9 AM UTC)
  └─ python refresh_report.py
        ├─ Query 1: AI_POD_VERIFICATION 90-day summary  (~80s)
        ├─ Query 2: AI_POD_VERIFICATION 14-day orders   (~60s)
        ├─ Generate ai_pod_report.html (10.5 MB)
        ├─ Write 256 shard files → data/
        └─ git commit + push → GitHub Pages live in ~2 min

Total runtime: ~2.5 minutes
```

---

## One-Time Setup on Elements

### Step 1 — Clone the repo

```bash
git clone https://github.com/harshalbhuse-dot/ai-pod-dashboard.git
cd ai-pod-dashboard
```

### Step 2 — Set up Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install google-cloud-bigquery db-dtypes pyarrow
```

### Step 3 — Configure GCP credentials

The script queries `wmt-driver-insights.Chirag_dx.AI_POD_VERIFICATION`.
Use whichever auth method is available on your Elements instance:

**Option A — Service Account key file (recommended for scheduled jobs):**
```bash
# Upload your service account JSON key to Elements, then:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Test it works:
python3 -c "from google.cloud import bigquery; c = bigquery.Client(); print('BQ OK')"
```

**Option B — Application Default Credentials (interactive login):**
```bash
gcloud auth application-default login
# Follow the browser flow to authenticate
```

### Step 4 — Verify the GitHub PAT is in the remote URL

The PAT is already embedded in the remote URL from the local laptop setup.
Verify it’s there:
```bash
git remote get-url origin
# Should show: https://ghp_...@github.com/harshalbhuse-dot/ai-pod-dashboard.git
```

If the PAT is missing or expired, set a new one:
```bash
git remote set-url origin https://<YOUR_PAT>@github.com/harshalbhuse-dot/ai-pod-dashboard.git
```
Generate a PAT at: https://github.com/settings/tokens
Required scope: **repo** (read + write)

### Step 5 — Do a test run

```bash
source .venv/bin/activate
python3 refresh_report.py
```

Expected output:
```
[2026-04-21 09:00:00 UTC] ==============================================================
[2026-04-21 09:00:00 UTC] AI POD Dashboard Refresh — Starting
[2026-04-21 09:00:00 UTC] ==============================================================
[2026-04-21 09:00:01 UTC] Connecting to BigQuery...
[2026-04-21 09:01:21 UTC] Query 1/2: AI_POD_VERIFICATION daily grain (last 90 days)...
[2026-04-21 09:01:21 UTC]   max_date=2026-04-20  drivers=37,206  rows=405,414
[2026-04-21 09:02:25 UTC] Query 2/2: Order-level detail (last 14 days)...
[2026-04-21 09:02:25 UTC]   17,274 drivers → 256 shard files written.
[2026-04-21 09:02:26 UTC] Generating ai_pod_report.html...
[2026-04-21 09:02:26 UTC]   Saved ai_pod_report.html (10891 KB)
[2026-04-21 09:02:26 UTC] Committing and pushing to GitHub Pages...
[2026-04-21 09:02:40 UTC] Committed: chore: daily refresh [2026-04-21 09:02 UTC]
[2026-04-21 09:02:55 UTC] Pushed to origin/main — GitHub Pages will deploy in ~2 min.
[2026-04-21 09:02:55 UTC] ==============================================================
[2026-04-21 09:02:55 UTC] SUCCESS — Dashboard updated on GitHub Pages!
```

### Step 6 — Schedule the cron job on Elements

Daily at **9:00 AM UTC** (4:00 AM CDT / 3:00 AM CST):

```bash
crontab -e
```

Add this line:
```cron
0 9 * * * cd /path/to/ai-pod-dashboard && .venv/bin/python3 refresh_report.py >> refresh.log 2>&1
```

Replace `/path/to/ai-pod-dashboard` with the actual path on Elements.

If you need to pass the GOOGLE_APPLICATION_CREDENTIALS env var in cron:
```cron
0 9 * * * export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json; cd /path/to/ai-pod-dashboard && .venv/bin/python3 refresh_report.py >> refresh.log 2>&1
```

---

## Files Involved

| File | Purpose |
|------|---------|
| `refresh_report.py` | Elements entry point — run this on the cron |
| `generate_report.py` | BQ queries + HTML generation (imported by refresh_report.py) |
| `ai_pod_report.html` | Generated summary page (committed to git each run) |
| `detail.html` | Static driver lookup page (only changes when edited) |
| `data/*.json` | 256 shard files for driver lookup (committed each run) |
| `refresh.log` | Rolling log of all refresh runs |
| `refresh.bat` | **Windows-only** — legacy laptop Task Scheduler script (no longer primary) |

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `google.cloud.bigquery not found` | `pip install google-cloud-bigquery db-dtypes pyarrow` |
| `Could not automatically determine credentials` | Set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login` |
| `Timeout: 300s exceeded` | BQ query took too long — check network/VPN, retry |
| `git push failed: Authentication failed` | GitHub PAT expired — generate a new one and update the remote URL |
| `git push failed: index.lock` | Previous run crashed mid-commit — delete `.git/index.lock` and retry |
| `No changes detected` | BQ data unchanged since last run — normal on days with no new data |

---

## Disabling the Laptop Task Scheduler (once Elements is running)

Once the Elements cron is confirmed working, disable the Windows Task Scheduler job to avoid double-refreshes:

```powershell
Disable-ScheduledTask -TaskName 'AIPODDashboardRefresh'
```

To re-enable if needed:
```powershell
Enable-ScheduledTask -TaskName 'AIPODDashboardRefresh'
```

---

## Data Volume (for capacity planning)

| Metric | Value |
|--------|-------|
| BQ rows (90-day summary) | ~405k driver-day rows |
| BQ rows (14-day orders) | ~496k order rows |
| Shard files generated | 256 files (~67 MB total) |
| Summary HTML size | ~10.9 MB |
| Total runtime | ~2.5 minutes |
| Git push size (compressed deltas) | ~5 MB/day |

---

## Owner

**Harshal Bhuse** — LMD Analytics  
Dashboard table: `wmt-driver-insights.Chirag_dx.AI_POD_VERIFICATION`
