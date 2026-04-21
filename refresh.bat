@echo off
:: ============================================================
:: AI POD Dashboard — Daily Refresh Script
:: Runs generate_report.py, commits, and pushes to GitHub.
:: Scheduled daily via Windows Task Scheduler.
:: ============================================================
setlocal

set DASHBOARD=C:\Users\H0B08S2\Documents\puppy_workspace\ai_pod_dashboard
set LOG=%DASHBOARD%\refresh_task.log
set PYTHON=%DASHBOARD%\.venv\Scripts\python.exe

echo [%date% %time%] Starting daily refresh >> "%LOG%"

cd /d "%DASHBOARD%"
if errorlevel 1 (
    echo [%date% %time%] ERROR: could not cd to dashboard dir >> "%LOG%"
    exit /b 1
)

:: 1. Regenerate report + driver JSON files
echo [%date% %time%] Running generate_report.py ... >> "%LOG%"
"%PYTHON%" generate_report.py >> "%LOG%" 2>&1
if errorlevel 1 (
    echo [%date% %time%] ERROR: generate_report.py failed >> "%LOG%"
    exit /b 1
)

:: 2. Git commit + push (summary HTML + all driver JSON files)
echo [%date% %time%] Committing and pushing ... >> "%LOG%"
git add ai_pod_report.html detail.html data\ >> "%LOG%" 2>&1
git commit -m "chore: daily refresh %date%" >> "%LOG%" 2>&1
git push origin main >> "%LOG%" 2>&1
if errorlevel 1 (
    echo [%date% %time%] ERROR: git push failed >> "%LOG%"
    exit /b 1
)

echo [%date% %time%] Done! Dashboard updated on GitHub Pages. >> "%LOG%"
endlocal
