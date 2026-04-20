#!/usr/bin/env python3
"""Generate self-contained AI POD Verification Dashboard HTML.

Usage:
    python generate_report.py

Output:
    ai_pod_report.html
"""
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: Run: pip install google-cloud-bigquery db-dtypes pyarrow")
    sys.exit(1)

BQ_TABLE = "wmt-driver-insights.Chirag_dx.AI_POD_VERIFICATION"
OUT_FILE = Path(__file__).parent / "ai_pod_report.html"


# ---------------------------------------------------------------------------
# 1. BigQuery — daily grain per driver (last 90 days)
# ---------------------------------------------------------------------------
def _n(v):
    """BQ value → int (None → 0)."""
    if v is None: return 0
    if isinstance(v, Decimal): return int(v)
    return int(v)


def fetch_data(client: bigquery.Client) -> tuple[list[str], str, list[list]]:
    """Returns (driver_ids, max_date_str, compact_rows).

    compact_rows format — each row is:
        [driver_idx, day_offset, t, a, u, iv, fr, pr, mp]

    day_offset: 0 = max_date (most recent), 89 = 90 days ago.
    Stored as integers — much smaller than full JSON objects.
    """
    sql = f"""
        SELECT
          COALESCE(DRVR_USER_ID, 'UNKNOWN')                      AS driver_id,
          CAST(created_date AS STRING)                           AS dt,
          COUNT(*)                                               AS t,
          COUNTIF(ai_result = 'acceptable')                      AS a,
          COUNTIF(ai_result = 'unacceptable')                    AS u,
          COUNTIF(LOWER(photo_taken_inside_vehicle) = 'yes')     AS iv,
          COUNTIF(LOWER(suspected_fraud) = 'yes')                AS fr,
          COUNTIF(LOWER(profanity_detected) = 'yes')             AS pr,
          COUNTIF(Missing_PO = 1)                                AS mp
        FROM `{BQ_TABLE}`
        WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
          AND created_date IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 2 DESC, 1
    """
    raw_rows = list(client.query(sql).result())
    print(f"  Fetched {len(raw_rows):,} driver-day rows")

    # Build driver index (sorted for stable output)
    driver_ids = sorted({str(r.driver_id) for r in raw_rows})
    drv_idx    = {d: i for i, d in enumerate(driver_ids)}

    # Max date for day-offset calculation (0 = most recent)
    all_dates = [str(r.dt) for r in raw_rows]
    max_date  = max(all_dates)

    from datetime import date as date_cls
    max_dt = date_cls.fromisoformat(max_date)

    compact = []
    for r in raw_rows:
        row_dt  = date_cls.fromisoformat(str(r.dt))
        day_off = (max_dt - row_dt).days          # 0 = max_date, 89 = oldest
        compact.append([
            drv_idx[str(r.driver_id)],
            day_off,
            _n(r.t), _n(r.a), _n(r.u),
            _n(r.iv), _n(r.fr), _n(r.pr), _n(r.mp),
        ])

    print(f"  {len(driver_ids):,} unique drivers | max date: {max_date}")
    return driver_ids, max_date, compact


# ---------------------------------------------------------------------------
# 2. HTML Template
# ---------------------------------------------------------------------------
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>AI POD Verification Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  .chart-wrap{position:relative;height:280px}
  .pill{cursor:pointer;border-radius:9999px;padding:5px 14px;font-size:.8rem;
        font-weight:600;border:2px solid #0053e2;transition:all .15s;white-space:nowrap}
  .pill.active{background:#0053e2;color:#fff}
  .pill:not(.active){background:#fff;color:#0053e2}
  .sort-btn{cursor:pointer;user-select:none}
  .sort-btn:hover{color:#0053e2}
  th{white-space:nowrap}
  input[type=date]{border:1px solid #d1d5db;border-radius:6px;padding:4px 8px;
                   font-size:.8rem;cursor:pointer}
  input[type=date]:focus{outline:none;border-color:#0053e2;box-shadow:0 0 0 2px #0053e222}
</style>
</head>
<body class="bg-gray-50 min-h-screen text-gray-800">

<!-- HEADER -->
<header class="px-6 py-4 shadow flex items-center gap-3 text-white" style="background:#0053e2">
  <div>
    <h1 class="text-lg font-bold">AI POD Verification Dashboard</h1>
    <p class="text-blue-200 text-xs">Driver-level Proof of Delivery analysis &bull; LMD Analytics</p>
  </div>
  <div class="ml-auto text-right">
    <div class="text-blue-200 text-xs">Data as of</div>
    <div class="text-white text-xs font-mono">__GENERATED_AT__</div>
  </div>
</header>

<!-- CONTROLS -->
<div class="bg-white border-b shadow-sm px-6 py-4">
  <div class="max-w-7xl mx-auto flex flex-wrap items-center gap-x-6 gap-y-3">

    <!-- Preset pills -->
    <div class="flex items-center gap-2 flex-wrap">
      <span class="text-xs font-bold text-gray-500">TIME WINDOW</span>
      <button class="pill active" onclick="setPreset(7)"  id="pill_7" >Last 7 days</button>
      <button class="pill"        onclick="setPreset(14)" id="pill_14">Last 14 days</button>
      <button class="pill"        onclick="setPreset(30)" id="pill_30">Last 30 days</button>
      <button class="pill"        onclick="setPreset(90)" id="pill_90">Last 90 days</button>
    </div>

    <!-- Custom date range -->
    <div class="flex items-center gap-2">
      <span class="text-xs font-bold text-gray-500">CUSTOM RANGE</span>
      <input type="date" id="dt_from" onchange="setCustom()" />
      <span class="text-xs text-gray-400">to</span>
      <input type="date" id="dt_to"   onchange="setCustom()" />
      <button onclick="clearCustom()"
        class="text-xs text-blue-600 hover:underline font-semibold">Clear</button>
    </div>

    <!-- Driver search -->
    <div class="ml-auto flex items-center gap-2">
      <label class="text-xs font-bold text-gray-500">SEARCH DRIVER</label>
      <input id="search" type="text" placeholder="Driver ID..."
        class="border rounded-lg px-3 py-1.5 text-sm w-52 focus:outline-none focus:ring-2 focus:ring-blue-400"
        oninput="render()" />
    </div>

  </div>
</div>

<main class="max-w-7xl mx-auto px-6 py-6 space-y-6">

  <!-- KPI CARDS -->
  <div class="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-4">
    <div class="bg-white rounded-xl shadow p-5 text-center">
      <div class="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">Active Drivers</div>
      <div class="text-3xl font-bold" style="color:#0053e2" id="kpi_drivers">&mdash;</div>
    </div>
    <div class="bg-white rounded-xl shadow p-5 text-center">
      <div class="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">Total PODs</div>
      <div class="text-3xl font-bold" style="color:#0053e2" id="kpi_pods">&mdash;</div>
    </div>
    <div class="bg-white rounded-xl shadow p-5 text-center">
      <div class="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">Acceptable Rate</div>
      <div class="text-3xl font-bold text-green-600" id="kpi_rate">&mdash;</div>
    </div>
    <div class="bg-white rounded-xl shadow p-5 text-center">
      <div class="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">Unacceptable PODs</div>
      <div class="text-3xl font-bold text-red-500" id="kpi_unacceptable">&mdash;</div>
    </div>
    <div class="bg-white rounded-xl shadow p-5 text-center">
      <div class="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">Avg Missing Orders</div>
      <div class="text-xs text-gray-400 mb-1">per driver</div>
      <div class="text-3xl font-bold text-orange-500" id="kpi_missing">&mdash;</div>
    </div>
  </div>

  <!-- DONUT CHART (full width, centred) -->
  <div class="bg-white rounded-xl shadow p-5 max-w-lg mx-auto">
    <h2 class="text-sm font-bold text-gray-700 mb-1">Unacceptable Image Breakdown</h2>
    <p class="text-xs text-gray-400 mb-4">Why PODs were flagged as unacceptable</p>
    <div class="chart-wrap"><canvas id="c_donut"></canvas></div>
  </div>

  <!-- DRIVER TABLE -->
  <div class="bg-white rounded-xl shadow overflow-hidden">
    <div class="px-5 py-4 border-b flex items-center justify-between">
      <h2 class="text-sm font-bold text-gray-700">Driver Performance Table</h2>
      <span class="text-xs text-gray-400" id="row_count"></span>
    </div>
    <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead class="bg-gray-50 text-xs text-gray-500 uppercase">
          <tr>
            <th class="px-4 py-3 text-left  sort-btn" onclick="sortBy(0)">Driver ID          <span id="si0"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(1)">Total PODs         <span id="si1"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(2)">Acceptable         <span id="si2"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(3)">Unacceptable       <span id="si3"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(4)">Unacceptable %     <span id="si4"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(5)">🚗 Inside Vehicle   <span id="si5"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(6)">🚨 Suspected Fraud <span id="si6"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(7)">🚫 Profanity        <span id="si7"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(8)">Missing Orders     <span id="si8"></span></th>
          </tr>
        </thead>
        <tbody id="tbl_body">
          <tr><td colspan="9" class="text-center text-gray-400 py-12">Loading...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</main>

<footer class="text-center text-xs text-gray-400 py-6">
  AI POD Verification Dashboard &bull; LMD Analytics &bull;
  <code>wmt-driver-insights.Chirag_dx.AI_POD_VERIFICATION</code>
</footer>

<script>
// ── Embedded data ───────────────────────────────────────────────────────────
const DRIVERS  = __DRIVERS__;   // ["D12345", ...] — 37k driver IDs
const MAX_DATE = "__MAX_DATE__"; // e.g. "2026-04-20"
// Each row: [driver_idx, day_offset, t, a, u, iv, fr, pr, mp]
// day_offset: 0 = MAX_DATE (newest), 89 = oldest
const DAILY    = __DAILY__;

// ── State ─────────────────────────────────────────────────────────────────
let activePreset = 7;     // active preset days (null = custom)
let customFrom   = null;  // ISO date string or null
let customTo     = null;
let sortCol      = 3;     // default: sort by unacceptable desc
let sortAsc      = false;
const charts     = {};

// ── Date helpers ───────────────────────────────────────────────────────────
function dateToOffset(isoDate) {
  // Returns day_offset (0 = MAX_DATE). Negative = future (not in data).
  const maxMs  = new Date(MAX_DATE).getTime();
  const dateMs = new Date(isoDate).getTime();
  return Math.round((maxMs - dateMs) / 86400000);
}

function getOffsetRange() {
  // Returns [minOff, maxOff] inclusive — rows with day_offset in this range.
  if (customFrom && customTo) {
    const fromOff = dateToOffset(customTo);   // closer to 0
    const toOff   = dateToOffset(customFrom); // further from 0
    return [Math.max(0, fromOff), Math.min(89, toOff)];
  }
  return [0, activePreset - 1];
}

// ── Preset window control ────────────────────────────────────────────────────
function setPreset(days) {
  activePreset = days;
  customFrom   = null;
  customTo     = null;
  document.getElementById('dt_from').value = '';
  document.getElementById('dt_to').value   = '';
  [7,14,30,90].forEach(d =>
    document.getElementById('pill_' + d).classList.toggle('active', d === days)
  );
  render();
}

function setCustom() {
  const from = document.getElementById('dt_from').value;
  const to   = document.getElementById('dt_to').value;
  if (!from || !to) return;  // wait until both are set
  customFrom   = from;
  customTo     = to;
  activePreset = null;
  [7,14,30,90].forEach(d =>
    document.getElementById('pill_' + d).classList.remove('active')
  );
  render();
}

function clearCustom() {
  document.getElementById('dt_from').value = '';
  document.getElementById('dt_to').value   = '';
  setPreset(7);
}

// ── Aggregate DAILY to driver level ───────────────────────────────────────────
function getRows() {
  const [minOff, maxOff] = getOffsetRange();
  const search = document.getElementById('search').value.trim().toLowerCase();
  const acc    = new Float64Array(DRIVERS.length * 8); // flat buffer: t,a,u,iv,fr,pr,mp per driver
  const seen   = new Uint8Array(DRIVERS.length);

  for (let i = 0; i < DAILY.length; i++) {
    const r   = DAILY[i];
    const off = r[1];
    if (off < minOff || off > maxOff) continue;
    const di  = r[0];
    const base= di * 8;
    seen[di]    = 1;
    acc[base]   += r[2]; // t
    acc[base+1] += r[3]; // a
    acc[base+2] += r[4]; // u
    acc[base+3] += r[5]; // iv
    acc[base+4] += r[6]; // fr
    acc[base+5] += r[7]; // pr
    acc[base+6] += r[8]; // mp
  }

  const rows = [];
  for (let di = 0; di < DRIVERS.length; di++) {
    if (!seen[di]) continue;
    const id = DRIVERS[di];
    if (search && !id.toLowerCase().includes(search)) continue;
    const base = di * 8;
    const t    = acc[base],   a  = acc[base+1], u  = acc[base+2];
    const iv   = acc[base+3], fr = acc[base+4], pr = acc[base+5], mp = acc[base+6];
    rows.push({ id, t, a, u, iv, fr, pr, mp, rate: t > 0 ? u/t*100 : 0 });
  }
  return rows;
}

// ── KPIs ─────────────────────────────────────────────────────────────────
function renderKPIs(rows) {
  let pods = 0, acc = 0, unacc = 0, mp = 0;
  rows.forEach(r => { pods += r.t; acc += r.a; unacc += r.u; mp += r.mp; });
  const avgMp = rows.length > 0 ? (mp / rows.length).toFixed(2) : '—';
  document.getElementById('kpi_drivers').textContent      = rows.length.toLocaleString();
  document.getElementById('kpi_pods').textContent         = pods.toLocaleString();
  document.getElementById('kpi_rate').textContent         = pods ? (acc/pods*100).toFixed(1)+'%' : '—';
  document.getElementById('kpi_unacceptable').textContent = unacc.toLocaleString();
  document.getElementById('kpi_missing').textContent      = avgMp;
}

// ── Donut chart ─────────────────────────────────────────────────────────────
function renderDonut(rows) {
  const iv = rows.reduce((s,r)=>s+r.iv,0);
  const fr = rows.reduce((s,r)=>s+r.fr,0);
  const pr = rows.reduce((s,r)=>s+r.pr,0);
  const cfg = {
    type:'doughnut',
    data:{
      labels:['Inside Vehicle','Suspected Fraud','Profanity'],
      datasets:[{data:[iv,fr,pr],
        backgroundColor:['#0053e2','#ea1100','#ffc220'],borderWidth:2}]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'bottom',labels:{font:{size:12},padding:16}}}
    }
  };
  if(charts.donut){charts.donut.data=cfg.data;charts.donut.update();}
  else charts.donut=new Chart(document.getElementById('c_donut'),cfg);
}

// ── Table ─────────────────────────────────────────────────────────────────
const SI = {asc:' ▲', desc:' ▼', none:' ⇅'};

function sortBy(col) {
  sortAsc = (sortCol === col) ? !sortAsc : false;
  sortCol = col;
  render();
}

function updateSortIcons() {
  for (let i = 0; i <= 8; i++) {
    const el = document.getElementById('si' + i);
    if (el) el.textContent = i === sortCol
      ? (sortAsc ? SI.asc : SI.desc) : SI.none;
  }
}

function rateColor(rate) {
  if (rate === 0)  return 'text-green-600';
  if (rate < 5)    return 'text-yellow-600';
  if (rate < 15)   return 'text-orange-500';
  return 'text-red-600 font-bold';
}

function renderTable(rows) {
  const keyMap = [null,'t','a','u','rate','iv','fr','pr','mp'];
  const sorted = [...rows].sort((a,b) => {
    if (sortCol === 0) return sortAsc
      ? a.id.localeCompare(b.id) : b.id.localeCompare(a.id);
    const k = keyMap[sortCol];
    return sortAsc ? a[k]-b[k] : b[k]-a[k];
  });

  document.getElementById('row_count').textContent = `${sorted.length.toLocaleString()} drivers`;
  document.getElementById('tbl_body').innerHTML = sorted.length === 0
    ? '<tr><td colspan="9" class="text-center text-gray-400 py-12">No drivers found.</td></tr>'
    : sorted.map((r,i) => `
    <tr class="${i%2?'bg-gray-50':'bg-white'} border-t border-gray-100 hover:bg-blue-50 transition">
      <td class="px-4 py-2.5 font-mono text-xs font-semibold" style="color:#0053e2">${r.id}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.t.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono text-green-600">${r.a.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono text-red-500">${r.u.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono ${rateColor(r.rate)}">${r.rate.toFixed(1)}%</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.iv.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.fr.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.pr.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.mp.toLocaleString()}</td>
    </tr>`).join('');
}

// ── Main render ───────────────────────────────────────────────────────────
function render() {
  const rows = getRows();
  renderKPIs(rows);
  renderDonut(rows);
  updateSortIcons();
  renderTable(rows);
}

// Init date picker bounds
(function initDates() {
  const maxD  = new Date(MAX_DATE);
  const minD  = new Date(maxD); minD.setDate(maxD.getDate() - 89);
  const fmt   = d => d.toISOString().slice(0,10);
  document.getElementById('dt_from').min = fmt(minD);
  document.getElementById('dt_from').max = fmt(maxD);
  document.getElementById('dt_to').min   = fmt(minD);
  document.getElementById('dt_to').max   = fmt(maxD);
}());

render();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# 3. Generate
# ---------------------------------------------------------------------------
def generate_html(driver_ids: list[str], max_date: str, compact: list[list]) -> str:
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        HTML
        .replace("__DRIVERS__",     json.dumps(driver_ids, separators=(",", ":")))
        .replace("__MAX_DATE__",    max_date)
        .replace("__DAILY__",       json.dumps(compact,    separators=(",", ":")))
        .replace("__GENERATED_AT__", gen)
    )


if __name__ == "__main__":
    print("Connecting to BigQuery...")
    client = bigquery.Client()
    print("Querying AI_POD_VERIFICATION (daily grain, last 90 days)...")
    driver_ids, max_date, compact = fetch_data(client)
    print("Generating HTML...")
    html = generate_html(driver_ids, max_date, compact)
    OUT_FILE.write_text(html, encoding="utf-8")
    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"  Saved: {OUT_FILE}  ({size_kb:.0f} KB)")
    print("Done!")
