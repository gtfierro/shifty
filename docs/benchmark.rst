Benchmarks
==========

Performance of Shifty's ``validate`` pipeline (inference + validation) across
real building models, tracked over each release.

Wall-clock ``validate`` time is dominated by a **fixed setup cost** — preparing
the shapes graph — that is paid no matter how small the data graph is.  A
16-triple Brick model still takes ~3.7 s.  So the raw number mostly reflects
model size, and a single average hides *which part* of the engine a release
improved.

Each bar below is the measured time to validate one model, averaged over the
corpus, split into the three things that time is spent on.

.. raw:: html

   <script src="https://cdn.plot.ly/plotly-2.32.0.min.js" charset="utf-8"></script>
   <div id="shifty-bench-chart" style="width:100%;height:640px;"></div>
   <p id="shifty-bench-caption" class="bench-caption"></p>
   <details class="bench-details">
     <summary>Show the numbers</summary>
     <div id="shifty-bench-breakdown"></div>
   </details>
   <script>
   // benchmark_data.js is loaded via html_js_files (for cache-busting) at the
   // end of <body>, so wait for DOMContentLoaded before reading its global.
   document.addEventListener('DOMContentLoaded', function () {
     var data = window.SHIFTY_BENCHMARK_DATA || {};
     var series = (data.timeseries || []).filter(function (ts) {
       return (ts.points || []).some(function (p) { return p.total_s > 0; });
     });

     var NAMES = { brick: 'Brick', s223: 'ASHRAE 223P' };

     // Categorical slots 1-3, validated for both surfaces (CVD dE 9.2 light /
     // 9.4 dark, normal-vision 24.0 / 20.9). Dark is a separate step for the
     // dark surface, not an automatic flip.
     var COMPONENTS = [
       { key: 'setup_s',    label: 'Fixed setup', light: '#2a78d6', dark: '#3987e5' },
       { key: 'infer_s',    label: 'Inference',   light: '#eb6834', dark: '#d95926' },
       { key: 'validate_s', label: 'Validation',  light: '#1baf7a', dark: '#199e70' },
     ];

     // Runs predating the infer-timing change record no inference component;
     // those fall back to a two-part stack rather than drawing a bogus zero.
     var hasInfer = series.some(function (ts) {
       return (ts.points || []).some(function (p) {
         return p.infer_s !== null && p.infer_s !== undefined;
       });
     });
     var parts = COMPONENTS.filter(function (c) {
       return c.key !== 'infer_s' || hasInfer;
     });

     // Furo stamps the theme on <body>, not <html>, and its third state
     // "auto" defers to the OS. Reading the wrong element silently pins the
     // chart to light colours on a dark page.
     var prefersDark = window.matchMedia('(prefers-color-scheme: dark)');
     function isDark() {
       var theme = document.body.dataset.theme || 'auto';
       if (theme === 'dark')  return true;
       if (theme === 'light') return false;
       return prefersDark.matches;
     }
     function axisColor()    { return isDark() ? '#aaa' : '#555'; }
     function gridColor()    { return isDark() ? 'rgba(255,255,255,0.08)'
                                               : 'rgba(0,0,0,0.08)'; }
     // Drawn as each segment's border to leave a gap between fills, so
     // adjacent segments never bleed into one another.
     function surfaceColor() { return isDark() ? '#1a1a19' : '#ffffff'; }

     if (!series.length) {
       document.getElementById('shifty-bench-chart').innerHTML =
         '<p style="padding:1rem;font-style:italic">Benchmark data not yet generated. ' +
         'Run <code>uv run benchmark/process_results.py</code> to produce it.</p>';
       return;
     }

     // One panel per dataset: the corpora differ in size, so a shared y-scale
     // would flatten the smaller one. The x-axis is shared.
     var PANELS = series.map(function (ts, i) {
       return { ts: ts, yaxis: i === 0 ? 'y' : 'y' + (i + 1) };
     });

     // The recorded seconds are corpus totals. Charting them puts ~190 s on the
     // axis for a corpus whose models each take ~4 s, which reads as wrong even
     // though it isn't -- so divide through by the model count and show the
     // mean per model, the number a reader actually recognises. Means are used
     // rather than medians because only means stay additive: the segment means
     // sum to the total mean, medians would not.
     function perModel(p, key) {
       return p.n_models ? (p[key] || 0) / p.n_models : 0;
     }

     function buildTraces() {
       var traces = [];
       PANELS.forEach(function (panel, pi) {
         var pts = (panel.ts.points || []).filter(function (p) { return p.total_s > 0; });
         parts.forEach(function (comp) {
           traces.push({
             type: 'bar',
             x: pts.map(function (p) { return p.version; }),
             y: pts.map(function (p) { return perModel(p, comp.key); }),
             customdata: pts.map(function (p) {
               return [(p[comp.key] || 0) / p.total_s * 100,
                       perModel(p, 'total_s'), p.total_s];
             }),
             name: comp.label,
             legendgroup: comp.key,
             showlegend: pi === 0,
             xaxis: 'x',
             yaxis: panel.yaxis,
             marker: {
               color: isDark() ? comp.dark : comp.light,
               line: { color: surfaceColor(), width: 1.5 },
             },
             hovertemplate: comp.label + ' %{y:.2f}s per model' +
                            ' (%{customdata[0]:.0f}% of %{customdata[1]:.2f}s)' +
                            '<br><span style="font-size:0.85em">whole corpus: ' +
                            '%{customdata[2]:.0f}s</span>' +
                            '<extra>' + (NAMES[panel.ts.dataset] || panel.ts.dataset) +
                            ' %{x}</extra>',
           });
         });
       });
       return traces;
     }

     function makeLayout() {
       var n = PANELS.length;
       var gap = 0.14;
       var h = (1 - gap * (n - 1)) / n;
       var layout = {
         barmode: 'stack',
         bargap: 0.25,
         xaxis: { anchor: PANELS[n - 1].yaxis, tickangle: -35,
                  title: { text: 'Version', standoff: 8 },
                  gridcolor: gridColor(), color: axisColor() },
         legend: { orientation: 'h', x: 0, y: 1.15, xanchor: 'left',
                   bgcolor: 'rgba(0,0,0,0)' },
         annotations: [],
         margin: { l: 70, r: 20, t: 68, b: 80 },
         hovermode: 'closest',
         paper_bgcolor: 'transparent',
         plot_bgcolor:  'transparent',
         font: { color: axisColor() },
       };
       PANELS.forEach(function (panel, i) {
         // Panels are laid out top-down, so the first dataset sits highest.
         var top = 1 - i * (h + gap);
         var key = i === 0 ? 'yaxis' : 'yaxis' + (i + 1);
         layout[key] = {
           // Clamp: the running subtraction lands a hair below zero on the
           // last panel, and Plotly rejects a domain outside [0, 1].
           domain: [Math.max(0, top - h), Math.min(1, top)],
           title: { text: 'seconds per model', standoff: 8 },
           rangemode: 'tozero',
           gridcolor: gridColor(), color: axisColor(),
         };
         layout.annotations.push({
           text: '<b>' + (NAMES[panel.ts.dataset] || panel.ts.dataset) + '</b>' +
                 ' — mean time to validate one model',
           x: 0, y: top + 0.012, xref: 'paper', yref: 'paper',
           xanchor: 'left', yanchor: 'bottom', showarrow: false,
           font: { size: 13, color: axisColor() },
         });
       });
       return layout;
     }

     Plotly.newPlot('shifty-bench-chart', buildTraces(), makeLayout(),
                    { responsive: true, displayModeBar: false });

     (function writeCaption() {
       var sizes = series.map(function (ts) {
         var n = (ts.points || []).reduce(function (mx, p) {
           return Math.max(mx, p.n_models || 0);
         }, 0);
         return n + ' ' + (NAMES[ts.dataset] || ts.dataset) + ' models';
       });
       var body =
         '<strong>How to read this chart.</strong> Each bar is the measured time to ' +
         'validate a single model with that release, averaged across the corpus (' +
         sizes.join(', ') + '), so bar height is the elapsed seconds you would ' +
         'actually wait and shorter is better. Hover any segment for that ' +
         'release&rsquo;s whole-corpus total. ' +
         'The segments split where that time goes. <em>Fixed setup</em> is the work ' +
         'done before any data is looked at — preparing the shapes graph — measured ' +
         'as the inference time of the corpus&rsquo;s smallest model, which at 16 ' +
         'triples is essentially pure startup. ';
       if (hasInfer) {
         body +=
           '<em>Inference</em> and <em>validation</em> are separated by timing ' +
           '<code>shifty infer</code> alongside <code>shifty validate</code>. The ' +
           '<code>validate</code> timing is cumulative — it runs inference internally ' +
           '— so inference is <code>infer − setup</code> and validation is ' +
           '<code>validate − infer</code>, and the three segments add up to the ' +
           'measured total exactly rather than being modelled. ';
       } else {
         body +=
           'Inference is not broken out here — these results predate per-phase ' +
           'timing, so the non-setup time is shown as one block. ';
       }
       body +=
         '<strong>The point: for the Brick corpus most of the wall clock is startup, ' +
         'not data</strong> — its models are small (median ~600 triples) against a ' +
         '229k-triple shapes closure, so per-model startup dominates and the two ' +
         'big engine wins are visible only in the thinner segments: v0.1.4 cut ' +
         'validation, and v0.2.1 cut inference. Times are medians of 3 runs per ' +
         'model, and each model pays setup once because every run is a fresh ' +
         'process — amortising that across models is a separate win available to ' +
         'the library API.';
       document.getElementById('shifty-bench-caption').innerHTML = body;
     })();

     // Table view: relief for the light-mode contrast warning on the validation
     // hue, and the accessible equivalent of the chart.
     (function writeTable() {
       var html = '';
       series.forEach(function (ts) {
         var pts = (ts.points || []).filter(function (p) { return p.total_s > 0; });
         if (!pts.length) return;
         html += '<h4>' + (NAMES[ts.dataset] || ts.dataset) + '</h4>';
         html += '<div class="bench-table-wrap"><table class="bench-table"><thead><tr><th>Version</th>';
         parts.forEach(function (c) { html += '<th>' + c.label + '</th>'; });
         html += '<th>Per model</th><th>Whole corpus</th></tr></thead><tbody>';
         pts.forEach(function (p) {
           html += '<tr><td><code>' + p.version + '</code></td>';
           parts.forEach(function (c) {
             html += '<td>' + perModel(p, c.key).toFixed(2) + ' s</td>';
           });
           html += '<td>' + perModel(p, 'total_s').toFixed(2) + ' s</td>';
           html += '<td>' + p.total_s.toFixed(1) + ' s</td></tr>';
         });
         html += '</tbody></table></div>';
       });
       document.getElementById('shifty-bench-breakdown').innerHTML = html;
     })();

     function repaint() {
       Plotly.react('shifty-bench-chart', buildTraces(), makeLayout());
     }
     new MutationObserver(repaint).observe(document.body,
       { attributes: true, attributeFilter: ['data-theme'] });
     // Also follow the OS while the toggle sits in its "auto" state.
     if (prefersDark.addEventListener) {
       prefersDark.addEventListener('change', repaint);
     }
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
