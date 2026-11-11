document.addEventListener('DOMContentLoaded', function () {
  const data = window.SHIFTY_BENCHMARK_DATA || { datasets: [], versions: [] };
  const datasets = data.datasets.filter(dataset => dataset.runs.length);
  const chart = document.getElementById('shifty-bench-chart');
  if (!chart) return;
  if (!datasets.length) {
    chart.textContent = 'No recorded benchmark results are available.';
    return;
  }

  const escape = value => String(value).replace(/[&<>"']/g, character => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[character]);
  const revision = run => run.revision
    ? run.revision.slice(0, 8) + (run.metadata && run.metadata.dirty ? ' (modified)' : '')
    : 'revision unavailable';
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');
  function isDark() {
    const theme = document.body.dataset.theme || 'auto';
    return theme === 'dark' || (theme === 'auto' && prefersDark.matches);
  }

  function drawChart() {
    const color = isDark() ? '#bbb' : '#444';
    const grid = isDark() ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)';
    const traces = [];
    const layout = {
      barmode: 'group',
      xaxis: { title: 'Version', tickangle: -35, categoryorder: 'array',
        categoryarray: data.versions, anchor: datasets.length === 1 ? 'y' : 'y' + datasets.length },
      legend: { orientation: 'h', y: 1.15 },
      annotations: [],
      margin: { l: 75, r: 20, t: 65, b: 90 },
      paper_bgcolor: 'transparent', plot_bgcolor: 'transparent', font: { color }
    };
    const components = [
      { key: 'infer_mean_ms', label: 'Inference only', color: isDark() ? '#3987e5' : '#2a78d6' },
      { key: 'mean_ms', label: 'Inference + validation', color: isDark() ? '#d95926' : '#eb6834' }
    ];
    const gap = 0.18;
    const height = (1 - gap * (datasets.length - 1)) / datasets.length;
    datasets.forEach((dataset, index) => {
      const axis = index === 0 ? 'y' : 'y' + (index + 1);
      const top = 1 - index * (height + gap);
      layout[index === 0 ? 'yaxis' : 'yaxis' + (index + 1)] = {
        domain: [Math.max(0, top - height), top], title: 'seconds per model',
        rangemode: 'tozero', gridcolor: grid
      };
      layout.annotations.push({ text: dataset.label, x: 0, y: top + 0.02,
        xref: 'paper', yref: 'paper', xanchor: 'left', showarrow: false });
      components.forEach(component => {
        traces.push({
          type: 'bar', name: component.label, legendgroup: component.key,
          showlegend: index === 0, yaxis: axis, marker: { color: component.color },
          x: dataset.runs.map(run => run.version),
          y: dataset.runs.map(run => run[component.key] === null ? null : run[component.key] / 1000),
          customdata: dataset.runs.map(run => [run.n_models, dataset.n_models, revision(run),
            run.complete ? 'full corpus' : 'partial or different model set']),
          hovertemplate: component.label + ': %{y:.3f} s per model' +
            '<br>%{x}, %{customdata[0]}/%{customdata[1]} models' +
            '<br>%{customdata[2]}, %{customdata[3]}<extra>' + dataset.label + '</extra>'
        });
      });
    });
    if (window.Plotly) {
      Plotly.react(chart, traces, layout, { responsive: true, displayModeBar: false });
    } else {
      chart.textContent = 'The chart could not load. Recorded values are available below.';
    }
  }
  drawChart();
  new MutationObserver(drawChart).observe(document.body,
    { attributes: true, attributeFilter: ['data-theme'] });
  prefersDark.addEventListener('change', drawChart);

  const datasetSelect = document.getElementById('bench-dataset');
  const versionSelect = document.getElementById('bench-version');
  const baselineSelect = document.getElementById('bench-baseline');
  datasetSelect.innerHTML = datasets.map(dataset => '<option value="' + dataset.dataset + '">' +
    escape(dataset.label) + '</option>').join('');
  function selectedDataset() {
    return datasets.find(dataset => dataset.dataset === datasetSelect.value);
  }
  function populateVersions() {
    const runs = selectedDataset().runs;
    const options = runs.map(run => '<option value="' + escape(run.version) + '">' +
      escape(run.version + (run.version === 'HEAD' ? ' (' + revision(run) + ')' : '')) + '</option>').join('');
    versionSelect.innerHTML = options;
    baselineSelect.innerHTML = '<option value="">No comparison</option>' + options;
    const releases = runs.filter(run => run.version !== 'HEAD');
    const latest = releases.length ? releases[releases.length - 1] : runs[runs.length - 1];
    versionSelect.value = latest.version;
    const previous = runs[runs.indexOf(latest) - 1];
    baselineSelect.value = previous ? previous.version : '';
    renderTable();
  }
  function geometricMean(values) {
    return values.length ? Math.exp(values.reduce((sum, value) => sum + Math.log(value), 0) / values.length) : null;
  }
  function changeCell(ratio) {
    if (ratio === null) return '<td>—</td>';
    const change = (ratio - 1) * 100;
    const className = change < -1 ? 'bench-faster' : (change > 1 ? 'bench-slower' : '');
    return '<td class="' + className + '">' + (change > 0 ? '+' : '') + change.toFixed(1) + '%</td>';
  }
  function renderTable() {
    const runs = selectedDataset().runs;
    const current = runs.find(run => run.version === versionSelect.value);
    const baseline = runs.find(run => run.version === baselineSelect.value);
    const ratios = [];
    const rows = Object.keys(current.models).sort().map(name => {
      const model = current.models[name];
      const previous = baseline && baseline.models[name];
      const ratio = previous ? model.val_ms / previous.val_ms : null;
      if (ratio !== null) ratios.push(ratio);
      return '<tr><td><code>' + escape(name) + '</code></td><td>' + model.val_ms.toFixed(1) + '</td><td>' +
        model.val_spread_ms.toFixed(1) + '</td>' + (baseline ? changeCell(ratio) : '') + '</tr>';
    }).join('');
    document.getElementById('bench-tables').innerHTML = '<div class="bench-table-wrap"><table class="bench-table">' +
      '<thead><tr><th>Model</th><th>Inference + validation (ms)</th><th>Spread (ms)</th>' +
      (baseline ? '<th>vs ' + escape(baseline.version) + '</th>' : '') + '</tr></thead><tbody>' + rows +
      '<tr class="bench-summary"><th>Geometric mean</th><td>' + current.geomean_ms.toFixed(1) + '</td><td>—</td>' +
      (baseline ? changeCell(geometricMean(ratios)) : '') + '</tr></tbody></table></div>' +
      (baseline ? '<p>Comparison uses ' + ratios.length + ' models present in both recordings.</p>' : '');
  }
  datasetSelect.addEventListener('change', populateVersions);
  versionSelect.addEventListener('change', renderTable);
  baselineSelect.addEventListener('change', renderTable);
  populateVersions();
});
