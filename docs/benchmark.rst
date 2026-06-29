Benchmarks
==========

Performance of Shifty's ``validate`` pipeline (inference + validation) across
real building models, tracked over each release.

The **timeseries** shows the geometric mean of raw ``validate`` wall-clock time
(ms) across all models in each dataset.

The **per-model tables** show exact ``validate`` times for the two most recent
releases.  The ``%`` column flags regressions or improvements per model, and
the ``geomean`` row summarises the overall change between the two versions.

.. raw:: html

   <script src="https://cdn.plot.ly/plotly-2.32.0.min.js" charset="utf-8"></script>
   <script src="_static/benchmark_data.js"></script>
   <div id="shifty-bench-chart" style="width:100%;height:380px;margin-bottom:2rem;"></div>
   <script>
   (function () {
     var data = window.SHIFTY_BENCHMARK_DATA || {};
     var series = data.timeseries || [];

     var COLORS = { brick: '#3b7dd8', s223: '#e06b2d' };
     var NAMES  = { brick: 'Brick', s223: 'ASHRAE 223P' };

     var traces = [];
     series.forEach(function (ts) {
       var ds  = ts.dataset;
       var pts = (ts.points || []).filter(function (p) { return p.geomean_ms > 0; });
       if (!pts.length) return;
       traces.push({
         x: pts.map(function (p) { return p.version; }),
         y: pts.map(function (p) { return p.geomean_ms; }),
         mode: 'lines+markers',
         name: NAMES[ds] || ds,
         line:   { color: COLORS[ds], width: 2 },
         marker: { size: 7, color: COLORS[ds] },
         hovertemplate: '%{y:.0f} ms<extra>' + (NAMES[ds] || ds) + '</extra>',
       });
     });

     if (!traces.length) {
       document.getElementById('shifty-bench-chart').innerHTML =
         '<p style="padding:1rem;font-style:italic">Benchmark data not yet generated. ' +
         'Run <code>uv run benchmark/process_results.py</code> to produce it.</p>';
       return;
     }

     function axisColor() {
       return document.documentElement.dataset.theme === 'dark' ? '#aaa' : '#555';
     }
     function gridColor() {
       return document.documentElement.dataset.theme === 'dark'
         ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)';
     }

     function makeLayout() {
       return {
         xaxis: { title: { text: 'Version', standoff: 8 }, tickangle: -35,
                  gridcolor: gridColor(), color: axisColor() },
         yaxis: { title: { text: 'Geometric mean  validate  ms', standoff: 8 },
                  rangemode: 'tozero', gridcolor: gridColor(), color: axisColor() },
         legend: { x: 0.01, y: 0.99, bgcolor: 'rgba(0,0,0,0)' },
         margin: { l: 70, r: 20, t: 16, b: 80 },
         hovermode: 'x unified',
         paper_bgcolor: 'transparent',
         plot_bgcolor:  'transparent',
         font: { color: axisColor() },
       };
     }

     Plotly.newPlot('shifty-bench-chart', traces, makeLayout(),
                    { responsive: true, displayModeBar: false });

     new MutationObserver(function () {
       Plotly.relayout('shifty-bench-chart', makeLayout());
     }).observe(document.documentElement,
                { attributes: true, attributeFilter: ['data-theme'] });
   })();
   </script>

Per-model results
-----------------

.. raw:: html

   <div id="bench-tables"></div>
   <script>
   (function () {
     var data = window.SHIFTY_BENCHMARK_DATA || {};
     if (!data.current_version) {
       document.getElementById('bench-tables').innerHTML =
         '<p><em>No benchmark data found.</em></p>';
       return;
     }

     function pctCell(pct) {
       if (pct === null || pct === undefined) return '<td>—</td>';
       var cls = pct < -1 ? 'bench-faster' : (pct > 1 ? 'bench-slower' : '');
       return '<td class="' + cls + '">' + (pct > 0 ? '+' : '') + pct.toFixed(1) + '%</td>';
     }

     function buildTable(dataset, label) {
       var rows = (data.tables || {})[dataset] || [];
       if (!rows.length) return '';
       var prev = data.prev_version || '';
       var curr = data.current_version || '';

       var h = '<h3>' + label + '</h3>';
       h += '<div class="bench-table-wrap"><table class="bench-table"><thead><tr>';
       h += '<th>Model</th><th>' + curr + '  validate ms</th>';
       if (prev) h += '<th>vs ' + prev + '</th>';
       h += '</tr></thead><tbody>';

       rows.forEach(function (r) {
         var isSum = r.model === '__geomean__';
         var tag = isSum ? 'th' : 'td';
         var label = isSum ? 'geomean' : '<code>' + r.model + '</code>';
         h += '<tr' + (isSum ? ' class="bench-summary"' : '') + '>';
         h += '<' + tag + '>' + label + '</' + tag + '>';
         h += '<' + tag + '>' + r.val_ms.toLocaleString() + ' ms</' + tag + '>';
         if (prev) h += pctCell(r.pct_change_val).replace('<td', '<' + tag).replace('</td>', '</' + tag + '>');
         h += '</tr>';
       });
       h += '</tbody></table></div>';
       return h;
     }

     document.getElementById('bench-tables').innerHTML =
       buildTable('brick', 'Brick') + buildTable('s223', 'ASHRAE 223P');
   })();
   </script>

.. _run_history:

Regenerating benchmark data
---------------------------

.. code-block:: bash

   ./benchmark/run_history.sh
   uv run benchmark/process_results.py
   cd docs && make html
