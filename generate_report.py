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
# 1. BigQuery — single query pivoting all 4 windows at driver level
# ---------------------------------------------------------------------------
def _n(v):
    """BQ value → compact int (None → 0)."""
    if v is None: return 0
    if isinstance(v, Decimal): return int(v)
    return int(v)


def fetch_data(client: bigquery.Client) -> list[list]:
    """One query, all 4 windows.  Returns list of compact arrays:
    [driver_id, t7,a7,u7,iv7,fr7,pr7, t14,a14,u14,iv14,fr14,pr14,
                t30,a30,u30,iv30,fr30,pr30, t90,a90,u90,iv90,fr90,pr90]
    """
    sql = f"""
        SELECT
          COALESCE(DRVR_USER_ID, 'UNKNOWN') AS driver_id,
          -- 7-day
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL  7 DAY))                                              AS t7,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL  7 DAY) AND ai_result = 'acceptable')                 AS a7,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL  7 DAY) AND ai_result = 'unacceptable')               AS u7,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL  7 DAY) AND LOWER(photo_taken_inside_vehicle)='yes')  AS iv7,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL  7 DAY) AND LOWER(suspected_fraud)='yes')             AS fr7,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL  7 DAY) AND LOWER(profanity_detected)='yes')          AS pr7,
          -- 14-day
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY))                                              AS t14,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND ai_result = 'acceptable')                 AS a14,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND ai_result = 'unacceptable')               AS u14,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND LOWER(photo_taken_inside_vehicle)='yes')  AS iv14,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND LOWER(suspected_fraud)='yes')             AS fr14,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND LOWER(profanity_detected)='yes')          AS pr14,
          -- 30-day
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))                                              AS t30,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND ai_result = 'acceptable')                 AS a30,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND ai_result = 'unacceptable')               AS u30,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND LOWER(photo_taken_inside_vehicle)='yes')  AS iv30,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND LOWER(suspected_fraud)='yes')             AS fr30,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND LOWER(profanity_detected)='yes')          AS pr30,
          -- 90-day
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))                                              AS t90,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND ai_result = 'acceptable')                 AS a90,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND ai_result = 'unacceptable')               AS u90,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND LOWER(photo_taken_inside_vehicle)='yes')  AS iv90,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND LOWER(suspected_fraud)='yes')             AS fr90,
          COUNTIF(created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND LOWER(profanity_detected)='yes')          AS pr90
        FROM `{BQ_TABLE}`
        WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
          AND created_date IS NOT NULL
        GROUP BY 1
        HAVING t90 > 0
        ORDER BY u90 DESC
    """
    fields = ['t7','a7','u7','iv7','fr7','pr7',
              't14','a14','u14','iv14','fr14','pr14',
              't30','a30','u30','iv30','fr30','pr30',
              't90','a90','u90','iv90','fr90','pr90']
    rows = client.query(sql).result()
    data = [[str(r.driver_id)] + [_n(getattr(r, f)) for f in fields] for r in rows]
    print(f"  Fetched {len(data):,} drivers")
    return data


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
  .chart-wrap{position:relative;height:260px}
  .pill{cursor:pointer;border-radius:9999px;padding:5px 16px;font-size:.8rem;
        font-weight:600;border:2px solid #0053e2;transition:all .15s;white-space:nowrap}
  .pill.active{background:#0053e2;color:#fff}
  .pill:not(.active){background:#fff;color:#0053e2}
  .sort-btn{cursor:pointer;user-select:none}
  .sort-btn:hover{color:#0053e2}
  th{white-space:nowrap}
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
  <div class="max-w-7xl mx-auto flex flex-wrap items-center gap-4">
    <div class="flex items-center gap-2 flex-wrap">
      <span class="text-xs font-bold text-gray-500">TIME WINDOW</span>
      <button class="pill active" onclick="setWindow(0)"  id="pill_0" >Last 7 days</button>
      <button class="pill"        onclick="setWindow(1)"  id="pill_1" >Last 14 days</button>
      <button class="pill"        onclick="setWindow(2)"  id="pill_2" >Last 30 days</button>
      <button class="pill"        onclick="setWindow(3)"  id="pill_3" >Last 90 days</button>
    </div>
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
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
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
  </div>

  <!-- CHARTS ROW -->
  <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
    <div class="bg-white rounded-xl shadow p-5">
      <h2 class="text-sm font-bold text-gray-700 mb-1">Unacceptable Image Breakdown</h2>
      <p class="text-xs text-gray-400 mb-4">Why PODs were flagged as unacceptable</p>
      <div class="chart-wrap"><canvas id="c_donut"></canvas></div>
    </div>
    <div class="bg-white rounded-xl shadow p-5">
      <h2 class="text-sm font-bold text-gray-700 mb-1">Top 10 Drivers &mdash; Unacceptable Count</h2>
      <p class="text-xs text-gray-400 mb-4">Stacked by violation type</p>
      <div class="chart-wrap"><canvas id="c_bar"></canvas></div>
    </div>
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
            <th class="px-4 py-3 text-left sort-btn"  onclick="sortBy(0)">Driver ID <span id="si0"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(1)">Total PODs <span id="si1"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(2)">Acceptable <span id="si2"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(3)">Unacceptable <span id="si3"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(4)">Unacceptable % <span id="si4"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(5)">🚗 Inside Vehicle <span id="si5"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(6)">🚨 Suspected Fraud <span id="si6"></span></th>
            <th class="px-4 py-3 text-right sort-btn" onclick="sortBy(7)">🚫 Profanity <span id="si7"></span></th>
          </tr>
        </thead>
        <tbody id="tbl_body">
          <tr><td colspan="8" class="text-center text-gray-400 py-12">Loading...</td></tr>
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
// ── Data ─────────────────────────────────────────────────────────────────
// Each row: [driver_id, t7,a7,u7,iv7,fr7,pr7, t14,a14,u14,iv14,fr14,pr14,
//                       t30,a30,u30,iv30,fr30,pr30, t90,a90,u90,iv90,fr90,pr90]
const RAW = __RAW_DATA__;

// Window offsets into each row: [total, acceptable, unacceptable, iv, fraud, profanity]
const W_OFF = { 0:[1,2,3,4,5,6], 1:[7,8,9,10,11,12], 2:[13,14,15,16,17,18], 3:[19,20,21,22,23,24] };
const W_LABELS = { 0:'Last 7 days', 1:'Last 14 days', 2:'Last 30 days', 3:'Last 90 days' };

// ── State ─────────────────────────────────────────────────────────────────
let win     = 0;   // current window index
let sortCol = 3;   // default sort: unacceptable count
let sortAsc = false;
const charts = {};

// ── Window toggle ─────────────────────────────────────────────────────────
function setWindow(w) {
  win = w;
  [0,1,2,3].forEach(i =>
    document.getElementById('pill_' + i).classList.toggle('active', i === w)
  );
  render();
}

// ── Get view rows ──────────────────────────────────────────────────────────
function getRows() {
  const [ot, oa, ou, oiv, ofr, opr] = W_OFF[win];
  const search = document.getElementById('search').value.trim().toLowerCase();
  return RAW
    .filter(r => r[ot] > 0 && (!search || r[0].toLowerCase().includes(search)))
    .map(r => ({
      id:   r[0],
      t:    r[ot],
      a:    r[oa],
      u:    r[ou],
      iv:   r[oiv],
      fr:   r[ofr],
      pr:   r[opr],
      rate: r[ot] > 0 ? r[ou] / r[ot] * 100 : 0,
    }));
}

// ── KPIs ─────────────────────────────────────────────────────────────────
function renderKPIs(rows) {
  let pods = 0, acc = 0, unacc = 0;
  rows.forEach(r => { pods += r.t; acc += r.a; unacc += r.u; });
  document.getElementById('kpi_drivers').textContent     = rows.length.toLocaleString();
  document.getElementById('kpi_pods').textContent        = pods.toLocaleString();
  document.getElementById('kpi_rate').textContent        = pods ? (acc/pods*100).toFixed(1)+'%' : '—';
  document.getElementById('kpi_unacceptable').textContent= unacc.toLocaleString();
}

// ── Charts ────────────────────────────────────────────────────────────────
function renderDonut(rows) {
  const iv = rows.reduce((s,r) => s+r.iv, 0);
  const fr = rows.reduce((s,r) => s+r.fr, 0);
  const pr = rows.reduce((s,r) => s+r.pr, 0);
  const cfg = {
    type:'doughnut',
    data:{
      labels:['Inside Vehicle','Suspected Fraud','Profanity'],
      datasets:[{data:[iv,fr,pr],
        backgroundColor:['#0053e2','#ea1100','#ffc220'],borderWidth:2}]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'bottom',labels:{font:{size:11}}}}
    }
  };
  if(charts.donut){charts.donut.data=cfg.data;charts.donut.update();}
  else charts.donut=new Chart(document.getElementById('c_donut'),cfg);
}

function renderBar(rows) {
  const top=[...rows].sort((a,b)=>b.u-a.u).slice(0,10);
  const cfg={
    type:'bar',
    data:{
      labels:top.map(r=>r.id),
      datasets:[
        {label:'Inside Vehicle', data:top.map(r=>r.iv),backgroundColor:'#0053e2'},
        {label:'Suspected Fraud',data:top.map(r=>r.fr),backgroundColor:'#ea1100'},
        {label:'Profanity',      data:top.map(r=>r.pr),backgroundColor:'#ffc220'},
      ]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{labels:{font:{size:10}}}},
      scales:{
        x:{stacked:true,ticks:{font:{size:9},maxRotation:45}},
        y:{stacked:true,ticks:{font:{size:9}}}
      }
    }
  };
  if(charts.bar){charts.bar.data=cfg.data;charts.bar.update();}
  else charts.bar=new Chart(document.getElementById('c_bar'),cfg);
}

// ── Table ─────────────────────────────────────────────────────────────────
const SI = {asc:' ▲',desc:' ▼',none:' ⇅'};

function sortBy(col) {
  sortAsc = (sortCol===col) ? !sortAsc : false;
  sortCol = col;
  render();
}

function updateSortIcons() {
  for(let i=0;i<=7;i++){
    const el=document.getElementById('si'+i);
    if(el) el.textContent=i===sortCol?(sortAsc?SI.asc:SI.desc):SI.none;
  }
}

function rateColor(rate){
  if(rate===0)       return 'text-green-600';
  if(rate<5)         return 'text-yellow-600';
  if(rate<15)        return 'text-orange-500';
  return 'text-red-600 font-bold';
}

function renderTable(rows){
  // sort
  const keyMap=[null,'t','a','u','rate','iv','fr','pr'];
  const sorted=[...rows].sort((a,b)=>{
    if(sortCol===0) return sortAsc?a.id.localeCompare(b.id):b.id.localeCompare(a.id);
    const k=keyMap[sortCol];
    return sortAsc?a[k]-b[k]:b[k]-a[k];
  });

  document.getElementById('row_count').textContent=`${sorted.length.toLocaleString()} drivers`;
  document.getElementById('tbl_body').innerHTML=sorted.length===0
    ?'<tr><td colspan="8" class="text-center text-gray-400 py-12">No drivers found.</td></tr>'
    :sorted.map((r,i)=>`
    <tr class="${i%2?'bg-gray-50':'bg-white'} border-t border-gray-100 hover:bg-blue-50 transition">
      <td class="px-4 py-2.5 font-mono text-xs font-semibold" style="color:#0053e2">${r.id}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.t.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono text-green-600">${r.a.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono text-red-500">${r.u.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono ${rateColor(r.rate)}">${r.rate.toFixed(1)}%</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.iv.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.fr.toLocaleString()}</td>
      <td class="px-4 py-2.5 text-right font-mono">${r.pr.toLocaleString()}</td>
    </tr>`).join('');
}

// ── Main render ───────────────────────────────────────────────────────────
function render(){
  const rows=getRows();
  renderKPIs(rows);
  renderDonut(rows);
  renderBar(rows);
  updateSortIcons();
  renderTable(rows);
}

render();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# 3. Generate
# ---------------------------------------------------------------------------
def generate_html(data: list[list]) -> str:
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        HTML
        .replace("__RAW_DATA__",     json.dumps(data, separators=(",", ":")))
        .replace("__GENERATED_AT__", gen)
    )


if __name__ == "__main__":
    print("Connecting to BigQuery...")
    client = bigquery.Client()
    print("Querying AI_POD_VERIFICATION (all 4 windows in one shot)...")
    data = fetch_data(client)
    print("Generating HTML...")
    html = generate_html(data)
    OUT_FILE.write_text(html, encoding="utf-8")
    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"  Saved: {OUT_FILE}  ({size_kb:.0f} KB)")
    print("Done!")
