/**
 * rc_charts_backup.js
 * 日常模式六张历史线图备份（2026-04-23 从 index.html 中移除）
 * 包含：HTML结构、CSS样式、JS逻辑
 * 如需恢复，将对应部分粘回 index.html
 */

// ════════════════════════════════════════════════════
// HTML 结构（放在 #richang-view 内，价格卡片之后）
// ════════════════════════════════════════════════════
/*
  <!-- 铂金 3 张 -->
  <div class="sym-label pt">▸ 铂金 Platinum</div>
  <div class="charts-6grid">
    <div class="chart-card">
      <div class="chart-header">
        <span class="chart-title">PT · 5 分钟</span>
        <span class="chart-count" id="rc-pt-5min-count">--</span>
      </div>
      <div class="chart-loading" id="rc-pt-5min-loading">加载中...</div>
      <div class="chart-scroll" id="rc-pt-5min-scroll" style="display:none">
        <div class="chart-inner">
          <div class="yaxis-wrap"><canvas id="rc-pt-5min-yaxis"></canvas></div>
          <div class="chart-body"><canvas id="rc-pt-5min"></canvas></div>
        </div>
      </div>
    </div>
    <div class="chart-card">
      <div class="chart-header">
        <span class="chart-title">PT · 15 分钟</span>
        <span class="chart-count" id="rc-pt-15min-count">--</span>
      </div>
      <div class="chart-loading" id="rc-pt-15min-loading">加载中...</div>
      <div class="chart-scroll" id="rc-pt-15min-scroll" style="display:none">
        <div class="chart-inner">
          <div class="yaxis-wrap"><canvas id="rc-pt-15min-yaxis"></canvas></div>
          <div class="chart-body"><canvas id="rc-pt-15min"></canvas></div>
        </div>
      </div>
    </div>
    <div class="chart-card">
      <div class="chart-header">
        <span class="chart-title">PT · 1 小时</span>
        <span class="chart-count" id="rc-pt-1hour-count">--</span>
      </div>
      <div class="chart-loading" id="rc-pt-1hour-loading">加载中...</div>
      <div class="chart-scroll" id="rc-pt-1hour-scroll" style="display:none">
        <div class="chart-inner">
          <div class="yaxis-wrap"><canvas id="rc-pt-1hour-yaxis"></canvas></div>
          <div class="chart-body"><canvas id="rc-pt-1hour"></canvas></div>
        </div>
      </div>
    </div>
  </div>

  <!-- 钯金 3 张 -->
  <div class="sym-label pd">▸ 钯金 Palladium</div>
  <div class="charts-6grid">
    <div class="chart-card">
      <div class="chart-header">
        <span class="chart-title">PD · 5 分钟</span>
        <span class="chart-count" id="rc-pd-5min-count">--</span>
      </div>
      <div class="chart-loading" id="rc-pd-5min-loading">加载中...</div>
      <div class="chart-scroll" id="rc-pd-5min-scroll" style="display:none">
        <div class="chart-inner">
          <div class="yaxis-wrap"><canvas id="rc-pd-5min-yaxis"></canvas></div>
          <div class="chart-body"><canvas id="rc-pd-5min"></canvas></div>
        </div>
      </div>
    </div>
    <div class="chart-card">
      <div class="chart-header">
        <span class="chart-title">PD · 15 分钟</span>
        <span class="chart-count" id="rc-pd-15min-count">--</span>
      </div>
      <div class="chart-loading" id="rc-pd-15min-loading">加载中...</div>
      <div class="chart-scroll" id="rc-pd-15min-scroll" style="display:none">
        <div class="chart-inner">
          <div class="yaxis-wrap"><canvas id="rc-pd-15min-yaxis"></canvas></div>
          <div class="chart-body"><canvas id="rc-pd-15min"></canvas></div>
        </div>
      </div>
    </div>
    <div class="chart-card">
      <div class="chart-header">
        <span class="chart-title">PD · 1 小时</span>
        <span class="chart-count" id="rc-pd-1hour-count">--</span>
      </div>
      <div class="chart-loading" id="rc-pd-1hour-loading">加载中...</div>
      <div class="chart-scroll" id="rc-pd-1hour-scroll" style="display:none">
        <div class="chart-inner">
          <div class="yaxis-wrap"><canvas id="rc-pd-1hour-yaxis"></canvas></div>
          <div class="chart-body"><canvas id="rc-pd-1hour"></canvas></div>
        </div>
      </div>
    </div>
  </div>
*/

// ════════════════════════════════════════════════════
// CSS 样式（放在 <style> 块内）
// ════════════════════════════════════════════════════
/*
    .charts-6grid { display: grid; grid-template-columns: 1fr; gap: 1rem; }

    .chart-scroll {
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: thin;
      scrollbar-color: var(--border) transparent;
      width: 100%;
      height: 120px;
    }
    @media (min-width: 640px)  { .chart-scroll { height: 140px; } }
    @media (min-width: 960px)  { .chart-scroll { height: 160px; } }
    .chart-scroll::-webkit-scrollbar { height: 4px; }
    .chart-scroll::-webkit-scrollbar-track { background: transparent; }
    .chart-scroll::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

    .chart-inner { display: flex; height: 100%; }
    .yaxis-wrap  { flex-shrink: 0; width: 52px; height: 100%; position: relative; }
    .yaxis-wrap canvas { position: absolute; top: 0; left: 0; width: 52px !important; height: 100% !important; }
    .chart-body  { flex: 1; height: 100%; position: relative; overflow: hidden; }
    .chart-body canvas { display: block; height: 100% !important; }

    .sym-label {
      font-size: 0.78rem; font-weight: 700;
      letter-spacing: 0.08em; margin: 0.8rem 0 0.4rem; padding-left: 0.2rem;
    }
    .sym-label.pt { color: var(--pt-color); }
    .sym-label.pd { color: var(--pd-color); }
*/

// ════════════════════════════════════════════════════
// JS 逻辑（放在 <script> 块内）
// ════════════════════════════════════════════════════

const PX_PER_SLOT = 32;
const TICK_STEP_RC = 0.5;

const DAY_SECTIONS_RC = [
  { h: 9,  m: 0,  eh: 10, em: 15 },
  { h: 10, m: 30, eh: 11, em: 30 },
  { h: 13, m: 30, eh: 15, em: 0  },
];

function _sectionSlotsRC(startH, startM, endH, endM, stepMin) {
  const slots = [];
  let ch = startH, cm = startM;
  while (ch < endH || (ch === endH && cm <= endM)) {
    slots.push({ h: ch, m: cm });
    cm += stepMin;
    if (cm >= 60) { ch += (cm / 60) | 0; cm %= 60; }
  }
  return slots;
}

function _daySlotsRC(intervalMin) {
  const out = [];
  for (const s of DAY_SECTIONS_RC)
    out.push(..._sectionSlotsRC(s.h, s.m, s.eh, s.em, intervalMin));
  return out;
}

function buildMultiDayAxisRC(days, intervalMin) {
  const step = intervalMin;
  const today = new Date();
  const fmtDate = (dt, h, m) => {
    const y  = dt.getFullYear();
    const mo = String(dt.getMonth() + 1).padStart(2, "0");
    const da = String(dt.getDate()).padStart(2, "0");
    const hh = String(h).padStart(2, "0");
    const mm = String(m).padStart(2, "0");
    return `${y}-${mo}-${da} ${hh}:${mm}`;
  };
  const labels = [], dayIdx = [];
  for (let d = days - 1; d >= 0; d--) {
    const dt = new Date(today);
    dt.setDate(dt.getDate() - d);
    const daySlots = _daySlotsRC(step);
    for (const s of daySlots) {
      labels.push(fmtDate(dt, s.h, s.m));
      dayIdx.push(d);
    }
  }
  return { labels, dayIdx };
}

const SLOTS_PER_DAY_RC = {
  "5min":  _daySlotsRC(5).length,
  "15min": _daySlotsRC(15).length,
  "1hour": _daySlotsRC(60).length,
};
const KEEP_DAYS_RC = { "5min": 2, "15min": 3, "1hour": 10 };
const FIXED_AXIS_RC = {};
for (const [iv, days] of Object.entries(KEEP_DAYS_RC)) {
  FIXED_AXIS_RC[iv] = buildMultiDayAxisRC(days, iv === "1hour" ? 60 : iv === "15min" ? 15 : 5);
}

function xTickCallbackRC(value, index, dayIdxArr, totalSlots) {
  const str = String(value);
  let step = 1;
  if (totalSlots >= 120)     step = 5;
  else if (totalSlots >= 60) step = 3;
  else if (totalSlots >= 30) step = 2;
  if (index % step !== 0) return "";
  const time = str.split(" ")[1];
  if (index === 0 || (dayIdxArr[index] !== dayIdxArr[index - 1])) {
    const parts = str.split(" ");
    if (parts.length === 2) return parts[0].slice(5) + " " + parts[1];
  }
  return time;
}

function rcChartOptionsRC(color, yAxisId, dayIdxArr, totalSlots) {
  return {
    type: "line",
    data: { labels: [], datasets: [{ data: [], borderColor: color, backgroundColor: "transparent", fill: false, tension: 0.35, borderWidth: 1.8, pointRadius: 0, spanGaps: true }] },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { grid: { color: "rgba(45,49,72,0.5)" }, ticks: { color: "#6b7280", autoSkip: false, maxRotation: 45, font: { size: 10 },
          callback: function(value, index) { return xTickCallbackRC(String(value), index, dayIdxArr, totalSlots); } } },
        y: { id: yAxisId, position: "left", grid: { color: "rgba(45,49,72,0.5)" }, ticks: { color: "#6b7280", font: { size: 10 }, callback: v => v.toFixed(2), stepSize: TICK_STEP_RC } },
      },
      plugins: { legend: { display: false }, tooltip: { backgroundColor: "#1a1d27", borderColor: "#2d3148", borderWidth: 1, titleColor: "#e2e4ea", bodyColor: "#9ca3af" } },
    },
  };
}

function yAxisOptionsRC(yAxisId) {
  return {
    type: "line",
    data: { labels: [], datasets: [{ data: [], borderColor: "transparent", backgroundColor: "transparent", fill: false, tension: 0, borderWidth: 0, pointRadius: 0 }] },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false, interaction: { mode: false },
      scales: { x: { display: false }, y: { id: yAxisId, position: "left", ticks: { color: "#6b7280", font: { size: 10 }, callback: v => v.toFixed(2), stepSize: TICK_STEP_RC }, grid: { display: false } } },
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
    },
  };
}

function syncYRangeRC(charts) {
  const allVals = [];
  for (const c of charts) {
    if (!c) continue;
    allVals.push(...c.data.datasets[0].data.filter(v => v != null && !isNaN(v)));
  }
  if (!allVals.length) return;
  const lo = Math.round(Math.min(...allVals) / TICK_STEP_RC) * TICK_STEP_RC;
  const hi = Math.round(Math.max(...allVals) / TICK_STEP_RC) * TICK_STEP_RC;
  for (const c of charts) {
    if (!c) continue;
    c.options.scales.y.min = lo - TICK_STEP_RC;
    c.options.scales.y.max = hi + TICK_STEP_RC;
    c.update("none");
  }
}

const rcChartsRC = {};

function makeRcChartRC(key, color, canvasId, yaxisId, slotCount) {
  const mainCanvas  = document.getElementById(canvasId);
  const yaxisCanvas = document.getElementById(yaxisId);
  const dayIdx      = FIXED_AXIS_RC[key.split("-")[1]]?.dayIdx || [];
  const mainChart   = new Chart(mainCanvas.getContext("2d"), rcChartOptionsRC(color, key, dayIdx, slotCount));
  const yaxisChart  = new Chart(yaxisCanvas.getContext("2d"), yAxisOptionsRC(key));
  const w = slotCount * PX_PER_SLOT;
  mainCanvas.style.width = `${w}px`;
  const bodyEl = mainCanvas.closest(".chart-body");
  if (bodyEl) bodyEl.style.width = `${w}px`;
  return { main: mainChart, yaxis: yaxisChart };
}

function initRcChartsRC() {
  const specs = [
    ["pt","5min","#f59e0b","rc-pt-5min","rc-pt-5min-yaxis",SLOTS_PER_DAY_RC["5min"]*KEEP_DAYS_RC["5min"]],
    ["pt","15min","#f59e0b","rc-pt-15min","rc-pt-15min-yaxis",SLOTS_PER_DAY_RC["15min"]*KEEP_DAYS_RC["15min"]],
    ["pt","1hour","#f59e0b","rc-pt-1hour","rc-pt-1hour-yaxis",SLOTS_PER_DAY_RC["1hour"]*KEEP_DAYS_RC["1hour"]],
    ["pd","5min","#818cf8","rc-pd-5min","rc-pd-5min-yaxis",SLOTS_PER_DAY_RC["5min"]*KEEP_DAYS_RC["5min"]],
    ["pd","15min","#818cf8","rc-pd-15min","rc-pd-15min-yaxis",SLOTS_PER_DAY_RC["15min"]*KEEP_DAYS_RC["15min"]],
    ["pd","1hour","#818cf8","rc-pd-1hour","rc-pd-1hour-yaxis",SLOTS_PER_DAY_RC["1hour"]*KEEP_DAYS_RC["1hour"]],
  ];
  for (const [sym, iv, color, cid, yid, cnt] of specs) {
    const k = `${sym}-${iv}`;
    if (!rcChartsRC[k]) rcChartsRC[k] = makeRcChartRC(k, color, cid, yid, cnt);
  }
}

async function loadRcChartRC(sym, interval) {
  const key      = `${sym}-${interval}`;
  const scrollId = `rc-${sym}-${interval}-scroll`;
  const loadId   = `rc-${sym}-${interval}-loading`;
  const countId  = `rc-${sym}-${interval}-count`;
  const symFull  = `KQ.m@GFEX.${sym}`;
  const pair     = rcChartsRC[key];
  if (!pair) return;
  const { main, yaxis } = pair;
  const { labels } = FIXED_AXIS_RC[interval];
  try {
    const res  = await fetch(`${window.location.origin}/history?symbol=${encodeURIComponent(symFull)}&interval_=${interval}&limit=200&days=${KEEP_DAYS_RC[interval]}`);
    const json = await res.json();
    const recs = json.records || [];
    const priceMap = {};
    for (const r of recs) priceMap[r.datetime] = r.price;
    let last = null;
    const prices = labels.map(l => (priceMap[l] != null ? (last = priceMap[l]) : last));
    main.data.labels = labels;
    main.data.datasets[0].data = prices;
    main.update("none");
    syncYRangeRC([main, yaxis]);
    document.getElementById(loadId).style.display = "none";
    document.getElementById(scrollId).style.display = "block";
    document.getElementById(countId).textContent = `${prices.filter(v => v != null).length} 条`;
  } catch (e) {
    document.getElementById(loadId).textContent = "加载失败: " + e.message;
  }
}

function loadAllRcChartsRC() {
  for (const sym of ["pt", "pd"])
    for (const iv of ["5min", "15min", "1hour"])
      loadRcChartRC(sym, iv);
}
