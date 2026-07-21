Benchmarks
==========

Performance of Shifty's ``validate`` pipeline (inference + validation) across
real building models, tracked over each release.

The chart below tracks each release against a fixed baseline and shows the
**spread across the model corpus**, not just the headline average — so a
release that speeds up some models while regressing others is visible as a
tall box rather than being averaged away.

.. raw:: html

   <script src="https://cdn.plot.ly/plotly-2.32.0.min.js" charset="utf-8"></script>
   <div id="shifty-bench-chart" style="width:100%;height:420px;"></div>
   <p id="shifty-bench-caption" class="bench-caption"></p>
   <script>
   // benchmark_data.js is loaded via html_js_files (for cache-busting) at the
   // end of <body>, so wait for DOMContentLoaded before reading its global.
   document.addEventListener('DOMContentLoaded', function () {
     var data = window.SHIFTY_BENCHMARK_DATA || {};
     var series = data.timeseries || [];

     var COLORS = { brick: '#3b7dd8', s223: '#e06b2d' };
     var NAMES  = { brick: 'Brick', s223: 'ASHRAE 223P' };

     // Ordered list of versions that actually carry data, and a lookup from
     // version -> x position. Boxes are drawn at numeric x (rather than at the
     // category label) so the geomean line can be nudged by the *same* offset
     // and land dead-centre on its group's boxes.
     var versions = [];
     (data.versions || []).forEach(function (v) {
       var present = series.some(function (ts) {
         return (ts.points || []).some(function (p) {
           return p.version === v && (p.rel || []).length;
         });
       });
       if (present) versions.push(v);
     });
     var xOf = {};
     versions.forEach(function (v, i) { xOf[v] = i; });

     var withData = series.filter(function (ts) {
       return (ts.points || []).some(function (p) { return (p.rel || []).length; });
     });
     // Two datasets sit side by side; a lone dataset stays centred.
     var SPAN = 0.19;
     function offsetFor(i) {
       if (withData.length < 2) return 0;
       return -SPAN + (2 * SPAN) * (i / (withData.length - 1));
     }

     var traces = [];
     withData.forEach(function (ts, di) {
       var ds     = ts.dataset;
       var label  = NAMES[ds] || ds;
       var off    = offsetFor(di);
       var pts    = (ts.points || []).filter(function (p) {
         return (p.rel || []).length && xOf[p.version] !== undefined;
       });
       if (!pts.length) return;

       // One box per version: every model's time divided by its own baseline.
       var bx = [], by = [], btext = [], babs = [];
       pts.forEach(function (p) {
         p.rel.forEach(function (r, k) {
           bx.push(xOf[p.version] + off);
           by.push(r);
           btext.push((p.models || [])[k] || '');
           babs.push((p.abs_ms || [])[k]);
         });
       });

       traces.push({
         type: 'box',
         x: bx,
         y: by,
         text: btext,
         customdata: babs,
         name: label,
         legendgroup: ds,
         width: 2 * SPAN * 0.78,
         marker: { color: COLORS[ds], size: 4, opacity: 0.8 },
         line: { color: COLORS[ds], width: 1.5 },
         fillcolor: 'rgba(0,0,0,0)',
         boxpoints: 'outliers',
         hoveron: 'boxes+points',
         hovertemplate:
           '%{text}<br>%{y:.3f}x baseline  (%{customdata:,} ms)' +
           '<extra>' + label + '</extra>',
       });

       // Geomean overlay — the single-number trend, same series as before.
       traces.push({
         type: 'scatter',
         mode: 'lines+markers',
         x: pts.map(function (p) { return xOf[p.version] + off; }),
         y: pts.map(function (p) { return p.geomean_rel; }),
         customdata: pts.map(function (p) { return p.geomean_ms; }),
         name: label + ' geomean',
         legendgroup: ds,
         showlegend: false,
         line:   { color: COLORS[ds], width: 2.5 },
         marker: { size: 6, color: COLORS[ds] },
         hovertemplate:
           'geomean %{y:.3f}x baseline  (%{customdata:,.0f} ms)' +
           '<extra>' + label + '</extra>',
       });
     });

     if (!traces.length) {
       document.getElementById('shifty-bench-chart').innerHTML =
         '<p style="padding:1rem;font-style:italic">Benchmark data not yet generated. ' +
         'Run <code>uv run benchmark/process_results.py</code> to produce it.</p>';
       return;
     }

     // Baseline reference at 1.00x. Drawn as a trace rather than a layout
     // shape because shape coordinates are in log units on a log axis, which
     // is an easy thing to get silently wrong.
     traces.push({
       type: 'scatter',
       mode: 'lines',
       x: [-0.5, versions.length - 0.5],
       y: [1, 1],
       line: { color: 'rgba(128,128,128,0.75)', width: 1, dash: 'dot' },
       hoverinfo: 'skip',
       showlegend: false,
     });

     function axisColor() {
       return document.documentElement.dataset.theme === 'dark' ? '#aaa' : '#555';
     }
     function gridColor() {
       return document.documentElement.dataset.theme === 'dark'
         ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)';
     }

     function makeLayout() {
       return {
         boxmode: 'overlay',
         xaxis: { title: { text: 'Version', standoff: 8 }, tickangle: -35,
                  tickmode: 'array',
                  tickvals: versions.map(function (v, i) { return i; }),
                  ticktext: versions,
                  range: [-0.5, versions.length - 0.5],
                  gridcolor: gridColor(), color: axisColor() },
         // Log scale: these are ratios, so a 2x regression and a 2x speedup
         // should read as equal distances from the 1.00x baseline. It also
         // stops a single large regression (v0.2.3) from flattening every
         // other release into an unreadable band.
         yaxis: { title: { text: 'validate time relative to baseline', standoff: 8 },
                  type: 'log',
                  tickmode: 'array',
                  tickvals: [0.25, 0.5, 0.75, 1, 1.5, 2, 4, 8],
                  ticktext: ['0.25x', '0.5x', '0.75x', '1.00x', '1.5x',
                             '2x', '4x', '8x'],
                  gridcolor: gridColor(), color: axisColor() },
         legend: { x: 0.01, y: 0.06, orientation: 'h', bgcolor: 'rgba(0,0,0,0)' },
         margin: { l: 70, r: 20, t: 16, b: 80 },
         hovermode: 'closest',
         paper_bgcolor: 'transparent',
         plot_bgcolor:  'transparent',
         font: { color: axisColor() },
       };
     }

     Plotly.newPlot('shifty-bench-chart', traces, makeLayout(),
                    { responsive: true, displayModeBar: false });

     // Caption names the actual baseline release and model counts, so it stays
     // accurate as more versions are benchmarked.
     (function writeCaption() {
       // Corpus size = the largest run, not the latest: an in-progress or
       // partial benchmark run would otherwise understate the model count.
       var parts = withData.map(function (ts) {
         var n = (ts.points || []).reduce(function (mx, p) {
           return Math.max(mx, (p.models || []).length);
         }, 0);
         return n + ' ' + (NAMES[ts.dataset] || ts.dataset) + ' models';
       });
       var base = (withData[0] || {}).baseline_version || versions[0] || 'the first release';
       document.getElementById('shifty-bench-caption').innerHTML =
         '<strong>How to read this chart.</strong> Each box summarises one release ' +
         'over the whole model corpus (' + parts.join(', ') + '). ' +
         'Every model is divided by <em>its own</em> time in ' + base + ', so ' +
         '<code>1.00x</code> (dotted line) is that baseline and lower is faster — ' +
         'normalising this way cancels out the fact that some buildings are far ' +
         'larger than others, leaving only the effect of the release itself. ' +
         'The box spans the middle 50% of models with the median inside it, the ' +
         'whiskers reach the rest, and individually plotted points are outliers ' +
         '(hover to see which model). The solid line is the geometric mean, the ' +
         'same headline number tracked before. ' +
         '<strong>A box that is short and low means the release sped up the whole ' +
         'corpus evenly; a tall box means it helped some models much more than ' +
         'others.</strong> Times are wall-clock <code>validate</code> ' +
         '(inference + validation), median of 3 runs per model.';
     })();

     new MutationObserver(function () {
       Plotly.relayout('shifty-bench-chart', makeLayout());
     }).observe(document.documentElement,
                { attributes: true, attributeFilter: ['data-theme'] });
   });
   </script>

Per-model results
-----------------

These tables show exact ``validate`` times for the two most recent releases.
The ``%`` column flags regressions or improvements per model, and the
``geomean`` row summarises the overall change between the two versions.

.. raw:: html

   <div id="bench-tables"></div>
   <script>
   document.addEventListener('DOMContentLoaded', function () {
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
   });
   </script>

.. _run_history:

Regenerating benchmark data
---------------------------

.. code-block:: bash

   ./benchmark/run_history.sh
   uv run benchmark/process_results.py
   cd docs && make html
