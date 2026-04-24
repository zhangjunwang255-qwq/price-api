/**
 * jb_charts_backup.js
 * 竞标模式两张独立线图备份（2026-04-24 从 index.html 中移除）
 * 包含：HTML结构、CSS样式、JS逻辑
 * 如需恢复，将对应部分粘回 index.html
 */

// ════════════════════════════════════════════════════
// CSS 样式（放在 <style> 块内）
// ════════════════════════════════════════════════════
/*
    .jb-charts-grid {
      display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;
      width: 100%; max-width: 100%;
    }
    @media (max-width: 640px) { .jb-charts-grid { grid-template-columns: 1fr; } }
    .jb-chart-wrap { width: 100%; overflow: hidden; }
    .jb-chart { width: 100% !important; height: 200px !important; }
    @media (min-width: 960px) { .jb-chart { height: 240px !important; } }
*/

// ════════════════════════════════════════════════════
// HTML 结构（放在 #jingbiao-view 内，价格卡片之后）
// ════════════════════════════════════════════════════
/*
  <div class="chart-card">
    <div class="chart-header">
      <span class="chart-title">实时走势</span>
      <span class="chart-count" id="jb-count">0 条</span>
    </div>
    <div class="jb-charts-grid">
      <div class="jb-chart-wrap"><canvas id="jb-pt-chart"></canvas></div>
      <div class="jb-chart-wrap"><canvas id="jb-pd-chart"></canvas></div>
    </div>
  </div>
*/

// ════════════════════════════════════════════════════
// JS 逻辑（放在 <script> 块内）
// ════════════════════════════════════════════════════

const MAX_JB = 80;

const jbBuf = [];
let jbPtChart = null;
let jbPdChart = null;

function createChartOpts(label, color) {
  return {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label,
        data: [],
        borderColor: color,
        backgroundColor: color.replace(')', ',0.08)').replace('rgb', 'rgba'),
        fill: false,
        tension: 0.35,
        borderWidth: 1.8,
        pointRadius: 0,
        spanGaps: true,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: {
          grid: { color: "rgba(45,49,72,0.5)" },
          ticks: { color: "#6b7280", maxTicksLimit: 6, maxRotation: 0, font: { size: 9 } },
        },
        y: {
          grid: { color: "rgba(45,49,72,0.5)" },
          ticks: { color: "#6b7280", font: { size: 9 }, callback: v => v.toFixed(2) },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#1a1d27", borderColor: "#2d3148", borderWidth: 1,
          titleColor: "#e2e4ea", bodyColor: "#9ca3af",
        },
      },
    },
  };
}

function initJbCharts() {
  jbPtChart = new Chart(
    document.getElementById("jb-pt-chart").getContext("2d"),
    createChartOpts("铂金 PT", "#f59e0b")
  );
  jbPdChart = new Chart(
    document.getElementById("jb-pd-chart").getContext("2d"),
    createChartOpts("钯金 PD", "#818cf8")
  );
}

// 在 pollJingbiao() 中追加缓冲并更新图表的逻辑：
/*
    if (ptPrice != null || pdPrice != null) {
      jbBuf.push({ t: now, pt: ptPrice, pd: pdPrice });
      if (jbBuf.length > MAX_JB) jbBuf.shift();

      if (!jbPtChart) initJbCharts();

      const labels = jbBuf.map(b => ts(b.t));
      jbPtChart.data.labels = labels;
      jbPtChart.data.datasets[0].data = jbBuf.map(b => b.pt);
      jbPtChart.update("none");

      jbPdChart.data.labels = labels;
      jbPdChart.data.datasets[0].data = jbBuf.map(b => b.pd);
      jbPdChart.update("none");

      document.getElementById("jb-count").textContent = `${jbBuf.length} 条`;
    }
*/
