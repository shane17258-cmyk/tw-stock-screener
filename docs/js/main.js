const state = {
  summary: null,
  currentTab: 'monthly',
  currentSymbol: null,
  currentTimeframe: 'daily',
  chart: null
};

const COLORS = {
  MA5: '#FF6B6B', MA10: '#FFD93D', MA20: '#6BCB77',
  MA60: '#4D96FF', MA120: '#9B59B6'
};
const MA_LABELS = { ma5: 'MA5', ma10: 'MA10', ma20: 'MA20', ma60: 'MA60', ma120: 'MA120' };

document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });
  document.querySelectorAll('.detail-tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTimeframe(btn.dataset.timeframe));
  });
  document.getElementById('backBtn').addEventListener('click', showListView);
  loadData();
});

async function loadData() {
  document.getElementById('loading').classList.remove('hidden');
  document.getElementById('errorMsg').classList.add('hidden');
  try {
    const resp = await fetch('data/summary.json?_=' + Date.now());
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    state.summary = await resp.json();
    document.getElementById('updateTime').textContent = '更新時間: ' + state.summary.update_date;
    renderTable();
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('listView').classList.remove('hidden');
  } catch (e) {
    document.getElementById('loading').classList.add('hidden');
    const err = document.getElementById('errorMsg');
    err.textContent = '⚠️ 無法載入數據，請確認 data/summary.json 已產生。錯誤: ' + e.message;
    err.classList.remove('hidden');
  }
}

function switchTab(tab) {
  state.currentTab = tab;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  renderTable();
}

function renderTable() {
  const tbody = document.getElementById('tableBody');
  const emptyMsg = document.getElementById('emptyMsg');
  const countSpan = document.getElementById('stockCount');
  tbody.innerHTML = '';

  if (!state.summary) return;

  const tab = state.currentTab;
  const key = tab === 'monthly' ? 'monthly' : 'weekly';
  const stocks = state.summary[key] || [];
  const countLabel = tab === 'monthly' ? '月線' : '周線';

  countSpan.textContent = `${countLabel}符合: ${stocks.length} 檔 / 分析: ${state.summary.total_analyzed || '-'} 檔`;

  if (stocks.length === 0) {
    emptyMsg.textContent = `目前沒有符合${countLabel}篩選條件的股票`;
    emptyMsg.style.display = 'block';
    return;
  }
  emptyMsg.style.display = 'none';

  stocks.forEach(s => {
    const tr = document.createElement('tr');
    tr.className = 'stock-row';
    tr.addEventListener('click', () => showDetail(s.symbol));
    tr.innerHTML = `
      <td>${s.symbol}</td>
      <td>${s.name}</td>
      <td>${s.close.toFixed(2)}</td>
      <td>${s.ma5.toFixed(2)}</td>
      <td>${s.ma10.toFixed(2)}</td>
      <td>${s.ma20.toFixed(2)}</td>
      <td>${s.ma60.toFixed(2)}</td>
      <td>${s.ma120.toFixed(2)}</td>
      <td>${s.deviation_max.toFixed(2)}%</td>
    `;
    tbody.appendChild(tr);
  });
}

async function showDetail(symbol) {
  state.currentSymbol = symbol;
  state.currentTimeframe = 'daily';
  document.getElementById('listView').classList.add('hidden');
  document.getElementById('detailView').classList.remove('hidden');
  document.getElementById('loading').classList.remove('hidden');
  document.getElementById('stockTitle').textContent = `${symbol} 載入中...`;

  document.querySelectorAll('.detail-tab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.timeframe === 'daily');
  });

  try {
    const resp = await fetch(`data/detail/${symbol}.json?_=${Date.now()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const detail = await resp.json();
    document.getElementById('stockTitle').textContent = `${detail.name} (${detail.symbol})`;
    document.getElementById('loading').classList.add('hidden');
    renderChart(detail, state.currentTimeframe);
  } catch (e) {
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('stockTitle').textContent = `${symbol} - 載入失敗`;
    console.error(e);
  }
}

function showListView() {
  if (state.chart) { state.chart.dispose(); state.chart = null; }
  document.getElementById('detailView').classList.add('hidden');
  document.getElementById('listView').classList.remove('hidden');
  state.currentSymbol = null;
}

function switchTimeframe(timeframe) {
  state.currentTimeframe = timeframe;
  document.querySelectorAll('.detail-tab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.timeframe === timeframe);
  });
  loadDetailChart();
}

async function loadDetailChart() {
  if (!state.currentSymbol) return;
  document.getElementById('loading').classList.remove('hidden');
  try {
    const resp = await fetch(`data/detail/${state.currentSymbol}.json?_=${Date.now()}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const detail = await resp.json();
    document.getElementById('loading').classList.add('hidden');
    renderChart(detail, state.currentTimeframe);
  } catch (e) {
    document.getElementById('loading').classList.add('hidden');
    console.error(e);
  }
}

function renderChart(detail, timeframe) {
  if (state.chart) { state.chart.dispose(); state.chart = null; }

  const data = detail[timeframe];
  if (!data || !data.dates || data.dates.length === 0) {
    document.getElementById('chartContainer').innerHTML = '<div style="padding:40px;text-align:center;color:#a0aec0;">無圖表數據</div>';
    return;
  }

  const container = document.getElementById('chartContainer');
  state.chart = echarts.init(container);

  const dates = data.dates;
  const kData = dates.map((_, i) => [
    data.open[i], data.close[i], data.low[i], data.high[i]
  ]);
  const volumeData = dates.map((_, i) => [
    data.open[i], data.close[i], data.low[i], data.high[i],
    data.volume[i]
  ]);

  const series = [{
    name: 'K線',
    type: 'candlestick',
    data: kData,
    itemStyle: { color: '#ef5350', color0: '#26a69a', borderColor: '#ef5350', borderColor0: '#26a69a' }
  }];

  const maConfigs = [
    { key: 'ma5', label: 'MA5', color: COLORS.MA5 },
    { key: 'ma10', label: 'MA10', color: COLORS.MA10 },
    { key: 'ma20', label: 'MA20', color: COLORS.MA20 },
    { key: 'ma60', label: 'MA60', color: COLORS.MA60 },
    { key: 'ma120', label: 'MA120', color: COLORS.MA120 }
  ];

  maConfigs.forEach(m => {
    const values = data[m.key];
    if (values && values.some(v => v !== null)) {
      series.push({
        name: m.label, type: 'line', data: values,
        smooth: true, lineStyle: { width: 1.5, color: m.color },
        symbol: 'none', connectNulls: false,
        z: 2
      });
    }
  });

  const volColors = dates.map((_, i) =>
    data.close[i] >= data.open[i] ? '#26a69a' : '#ef5350'
  );
  series.push({
    name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1,
    data: data.volume, itemStyle: { color: params => volColors[params.dataIndex] }
  });

  const option = {
    tooltip: {
      trigger: 'axis', axisPointer: { type: 'cross' },
      backgroundColor: 'rgba(255,255,255,0.95)',
      borderColor: '#e2e8f0', borderWidth: 1,
      textStyle: { color: '#1a1a2e', fontSize: 12 },
      formatter: function(params) {
        const date = params[0].axisValue;
        let html = `<strong>${date}</strong><br/>`;
        params.forEach(p => {
          if (p.seriesName === 'K線') {
            const d = p.data;
            html += `開: ${d[0].toFixed(2)} 收: ${d[1].toFixed(2)} 低: ${d[2].toFixed(2)} 高: ${d[3].toFixed(2)}<br/>`;
          } else if (p.seriesName === '成交量') {
            html += `成交量: ${(p.data / 1000).toFixed(0)}K<br/>`;
          } else if (p.value !== null && p.value !== undefined) {
            html += `${p.marker} ${p.seriesName}: ${p.value.toFixed(2)}<br/>`;
          }
        });
        return html;
      }
    },
    grid: [
      { left: '8%', right: '8%', top: '8%', height: '58%' },
      { left: '8%', right: '8%', bottom: '12%', height: '14%' }
    ],
    xAxis: [
      { type: 'category', data: dates, gridIndex: 0, axisLine: { lineStyle: { color: '#e2e8f0' } }, axisLabel: { fontSize: 10 } },
      { type: 'category', data: dates, gridIndex: 1, axisLabel: { show: false }, axisLine: { show: false } }
    ],
    yAxis: [
      { type: 'value', gridIndex: 0, scale: true, splitLine: { lineStyle: { color: '#f0f0f0', type: 'dashed' } }, axisLabel: { fontSize: 10 } },
      { type: 'value', gridIndex: 1, splitLine: { show: false }, axisLabel: { show: false } }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 50, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start: 50, end: 100, bottom: '2%', height: 8, borderColor: '#e2e8f0', fillerColor: 'rgba(26,26,46,0.1)', handleStyle: { color: '#1a1a2e' } }
    ],
    series: series,
    legend: {
      data: ['K線', ...maConfigs.map(m => m.label)],
      top: '1%', left: 'center', icon: 'roundRect',
      itemWidth: 14, itemHeight: 8,
      textStyle: { fontSize: 11, color: '#4a5568' }
    }
  };

  state.chart.setOption(option);
  state.chart.on('click', () => {});
  window.addEventListener('resize', () => { if (state.chart) state.chart.resize(); });
}
