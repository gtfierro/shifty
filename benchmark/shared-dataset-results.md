# Shared dataset and demand-driven index results

This measures the implementation of [the shared-dataset plan](../development_docs/12-shared-datasets-and-demand-driven-indexes.md) on commit `c102812` against two same-lockfile baselines: pre-session `4118547` for the full Brick and s223 collections, and the immediate pre-change branch `c6d0b1d` for generated and session-lifecycle cases. The optimized CLI was copied to `/private/tmp/shifty-c102812-cli` before measurement (SHA-256 `80756e95fd318fcf08e404e1102b777a930aeba72993be7ed35a663250c5aac9`). All versions use Cargo.lock SHA-256 `4d93f0ad6d3a0c15653f133209a145f224e5815f638a4d2ff94bf1100b8704c3`.

Each CLI condition used one discarded warmup and five cold-process samples per version, alternating versions sequentially. Times include shape load, compilation, session construction, operation, and export. Tables show medians, maximum absolute deviation from each median, and median peak RSS. The commands and machine-readable results were produced with `compare_shared_datasets.py` and `summarize_shared_datasets.py` in this directory. The JSON files remain under `/private/tmp/` with names `shifty-compare-{brick,s223,synthetic}-c102812-vs411.json` and `shifty-compare-synthetic-c102812-vsc6.json`.

| Comparison | Cases per operation | Median new/old inference | Validation | Report | Median new/old peak RSS (infer / validate / report) |
| --- | ---: | ---: | ---: | ---: | --- |
| Brick vs `4118547` | 45 | 0.724 | 0.688 | 0.722 | 0.501 / 0.512 / 0.515 |
| s223 vs `4118547` | 19 | 0.681 | 0.699 | 0.777 | 0.473 / 0.502 / 0.548 |
| Generated vs `4118547` | 10 | 0.690 | 0.800 | 0.840 | lower in every operation |
| Generated vs `c6d0b1d` | 10 | 0.659 | 0.749 | 0.805 | 0.435 / 0.565 / 0.571 |

Every operation median in these four comparisons improved. All 45 Brick and 19 s223 inferred graphs and report graphs are RDF-isomorphic to `4118547`; generated inference is equal in all cases. The generated large-source/small-data report differs only in a message literal that embeds a process-local blank-node label. Human-readable validation output is compared as text by the harness and therefore flags `$shapesGraph` bindings newly shown in diagnostics, process-local blank-node identifiers, and result ordering; the corresponding report graphs preserve the findings. This is a comparison limitation, not a claim of byte-for-byte validation-output equality.

The compiled path's profiling output shows zero full-data Store builds, one shared source encoding, one session dataset build, no eager materialized union or evaluated-graph projection, primary pair-buffer bytes, selective index decisions with estimated and actual bytes, scan candidate rows, and reach-cache activity. Source index bytes are accounted once per compilation. The synthetic matrix varies source/data size, predicate density and wildcard demand, forward/reverse access, graph topology, native/fallback SPARQL, named-shapes reads, no rules, and multi-round triple/SPARQL rules. Base-only, full-index, demand-driven, budget, and mutation semantics also have deterministic Rust tests.

## Warm-session ownership comparison

These release examples compare `c102812` with `c6d0b1d`, using the same s223 shapes and data fixtures, one warmup, five alternating samples, and one compilation per process. Lifecycle mode builds two sessions, validates repeatedly, and edits a snapshot. Many-session mode retains 20 live sessions sharing the compilation. JSON results are `/private/tmp/shifty-sessions-{lifecycle,lifecycle-noinfer,many20-infer,many20-noinfer}-c102812-vsc6.json`.

| Condition | Old / new median elapsed (ms) | Old / new peak RSS (MiB) |
| --- | ---: | ---: |
| Lifecycle with inference and edited snapshot | 3280 / 1567 | 610 / 255 |
| Lifecycle without inference | 1340 / 1206 | 313 / 252 |
| 20 live sessions with inference | 16106 / 4596 | 1330 / 311 |
| 20 live sessions without inference | 4837 / 3232 | 935 / 298 |

The lifecycle substage for repeated validation rose from 57.7 to 71.0 ms despite lower end-to-end time. First validation fell from 223.9 to 135.8 ms, second-session validation from 357.9 to 276.1 ms, and edited-session validation from 223.0 to 133.9 ms. Repeating the lifecycle benchmark without inference still shows the repeated-call regression (51.2 to 65.2 ms), so inference batch commits are not its cause. This measured validation cost remains a tuning target; the responsible validation operator has not yet been isolated. Source storage stays shared, while session-local query state and the asserted-data snapshot still have per-session cost.

## Full-suite per-workload measurements

The following tables are generated from the complete result JSON. In the `Comparison equal` column, validation means literal CLI text equality; for inference and reports it means RDF graph isomorphism.

### Brick vs `4118547`

| Case | Operation | Old median ± spread (ms) | New median ± spread (ms) | New/old | Old/new RSS (MiB) | Comparison equal |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| bldg1 | infer | 1439.8 ± 99.0 | 1031.6 ± 76.0 | 0.72 | 761 / 383 | yes |
| bldg1 | validate | 1888.0 ± 154.0 | 1255.7 ± 91.0 | 0.67 | 760 / 389 | no |
| bldg1 | report | 2138.8 ± 78.4 | 1617.9 ± 92.3 | 0.76 | 761 / 391 | yes |
| bldg10 | infer | 1437.1 ± 54.0 | 1021.4 ± 8.3 | 0.71 | 759 / 382 | yes |
| bldg10 | validate | 1703.5 ± 131.2 | 1164.4 ± 23.4 | 0.68 | 758 / 387 | no |
| bldg10 | report | 2165.6 ± 209.9 | 1602.3 ± 123.8 | 0.74 | 758 / 388 | yes |
| bldg11 | infer | 2290.1 ± 36.2 | 1742.6 ± 86.7 | 0.76 | 887 / 423 | yes |
| bldg11 | validate | 3246.1 ± 311.1 | 2184.8 ± 286.1 | 0.67 | 915 / 515 | no |
| bldg11 | report | 5962.0 ± 41.5 | 4987.7 ± 144.5 | 0.84 | 937 / 531 | yes |
| bldg12 | infer | 1668.6 ± 19.9 | 1221.6 ± 18.8 | 0.73 | 802 / 399 | yes |
| bldg12 | validate | 2176.4 ± 44.3 | 1492.4 ± 35.4 | 0.69 | 801 / 424 | no |
| bldg12 | report | 3260.7 ± 16.1 | 2552.7 ± 16.4 | 0.78 | 803 / 430 | yes |
| bldg13 | infer | 1447.5 ± 42.5 | 1064.6 ± 20.4 | 0.74 | 768 / 384 | yes |
| bldg13 | validate | 1741.1 ± 22.0 | 1210.4 ± 18.0 | 0.70 | 768 / 394 | no |
| bldg13 | report | 2265.5 ± 24.2 | 1635.0 ± 11.3 | 0.72 | 769 / 396 | yes |
| bldg14 | infer | 1362.5 ± 11.4 | 975.4 ± 17.3 | 0.72 | 750 / 378 | yes |
| bldg14 | validate | 1612.2 ± 21.1 | 1110.0 ± 13.8 | 0.69 | 752 / 382 | no |
| bldg14 | report | 1907.2 ± 89.6 | 1316.2 ± 21.5 | 0.69 | 750 / 382 | yes |
| bldg15 | infer | 1838.9 ± 12.5 | 1343.6 ± 11.3 | 0.73 | 831 / 406 | yes |
| bldg15 | validate | 2542.8 ± 106.5 | 1681.1 ± 57.4 | 0.66 | 833 / 459 | no |
| bldg15 | report | 4206.8 ± 154.4 | 3300.0 ± 45.5 | 0.78 | 854 / 468 | yes |
| bldg16 | infer | 1392.5 ± 19.1 | 994.9 ± 7.0 | 0.71 | 753 / 378 | yes |
| bldg16 | validate | 1629.3 ± 21.0 | 1114.3 ± 7.9 | 0.68 | 751 / 382 | no |
| bldg16 | report | 1931.8 ± 4.7 | 1344.4 ± 10.3 | 0.70 | 753 / 382 | yes |
| bldg17 | infer | 1368.6 ± 25.3 | 984.7 ± 9.7 | 0.72 | 752 / 377 | yes |
| bldg17 | validate | 1657.1 ± 212.1 | 1214.6 ± 180.7 | 0.73 | 751 / 382 | no |
| bldg17 | report | 1906.0 ± 7.3 | 1332.7 ± 24.4 | 0.70 | 751 / 383 | yes |
| bldg18 | infer | 1681.0 ± 23.7 | 1223.0 ± 8.2 | 0.73 | 808 / 400 | yes |
| bldg18 | validate | 2187.6 ± 5.6 | 1506.2 ± 32.9 | 0.69 | 808 / 435 | no |
| bldg18 | report | 3385.5 ± 20.8 | 2671.7 ± 30.4 | 0.79 | 817 / 445 | yes |
| bldg19 | infer | 1389.5 ± 20.4 | 1012.7 ± 13.6 | 0.73 | 756 / 380 | yes |
| bldg19 | validate | 1655.7 ± 11.0 | 1144.8 ± 18.1 | 0.69 | 755 / 385 | no |
| bldg19 | report | 2013.6 ± 19.4 | 1417.0 ± 20.1 | 0.70 | 755 / 388 | yes |
| bldg2 | infer | 1413.4 ± 17.5 | 1017.6 ± 11.3 | 0.72 | 758 / 381 | yes |
| bldg2 | validate | 1692.2 ± 17.4 | 1165.0 ± 30.5 | 0.69 | 758 / 388 | no |
| bldg2 | report | 2091.2 ± 22.4 | 1510.7 ± 18.8 | 0.72 | 759 / 393 | yes |
| bldg20 | infer | 1406.0 ± 22.6 | 1027.2 ± 3.5 | 0.73 | 760 / 382 | yes |
| bldg20 | validate | 1695.8 ± 57.2 | 1173.2 ± 12.1 | 0.69 | 761 / 388 | no |
| bldg20 | report | 2116.0 ± 26.0 | 1514.8 ± 11.3 | 0.72 | 760 / 390 | yes |
| bldg21 | infer | 1392.7 ± 33.3 | 1008.9 ± 2.9 | 0.72 | 755 / 379 | yes |
| bldg21 | validate | 1643.4 ± 34.4 | 1139.7 ± 18.9 | 0.69 | 756 / 385 | no |
| bldg21 | report | 1970.4 ± 8.2 | 1390.3 ± 9.9 | 0.71 | 754 / 385 | yes |
| bldg22 | infer | 1381.1 ± 8.3 | 995.2 ± 258.0 | 0.72 | 756 / 380 | yes |
| bldg22 | validate | 1654.4 ± 33.0 | 1145.1 ± 91.4 | 0.69 | 754 / 384 | no |
| bldg22 | report | 2004.2 ± 11.3 | 1410.4 ± 22.8 | 0.70 | 756 / 386 | yes |
| bldg23 | infer | 1373.7 ± 22.7 | 1001.9 ± 15.7 | 0.73 | 753 / 379 | yes |
| bldg23 | validate | 1617.0 ± 16.5 | 1117.9 ± 30.8 | 0.69 | 753 / 384 | no |
| bldg23 | report | 1933.9 ± 21.3 | 1371.0 ± 8.3 | 0.71 | 752 / 385 | yes |
| bldg24 | infer | 1362.7 ± 16.3 | 975.0 ± 5.6 | 0.72 | 749 / 377 | yes |
| bldg24 | validate | 1610.8 ± 24.6 | 1096.7 ± 10.2 | 0.68 | 751 / 380 | no |
| bldg24 | report | 1873.6 ± 13.8 | 1294.0 ± 14.9 | 0.69 | 751 / 381 | yes |
| bldg25 | infer | 1410.5 ± 10.5 | 1019.0 ± 18.8 | 0.72 | 753 / 380 | yes |
| bldg25 | validate | 1653.4 ± 18.2 | 1134.0 ± 11.3 | 0.69 | 752 / 383 | no |
| bldg25 | report | 2121.7 ± 163.7 | 1411.8 ± 41.7 | 0.67 | 754 / 385 | yes |
| bldg26 | infer | 1522.5 ± 123.6 | 1081.0 ± 162.6 | 0.71 | 755 / 380 | yes |
| bldg26 | validate | 1686.6 ± 40.5 | 1214.8 ± 191.8 | 0.72 | 754 / 384 | no |
| bldg26 | report | 2003.5 ± 77.1 | 1403.9 ± 41.2 | 0.70 | 756 / 385 | yes |
| bldg27 | infer | 1455.7 ± 12.2 | 1060.2 ± 27.5 | 0.73 | 762 / 383 | yes |
| bldg27 | validate | 1744.0 ± 46.9 | 1210.1 ± 40.4 | 0.69 | 761 / 391 | no |
| bldg27 | report | 2218.0 ± 47.9 | 1578.2 ± 18.8 | 0.71 | 762 / 392 | yes |
| bldg28 | infer | 1451.1 ± 19.7 | 1059.9 ± 10.4 | 0.73 | 772 / 385 | yes |
| bldg28 | validate | 1768.9 ± 16.9 | 1215.0 ± 21.4 | 0.69 | 770 / 395 | no |
| bldg28 | report | 2334.1 ± 15.0 | 1702.4 ± 17.4 | 0.73 | 770 / 397 | yes |
| bldg29 | infer | 1384.4 ± 31.8 | 992.9 ± 15.3 | 0.72 | 753 / 378 | yes |
| bldg29 | validate | 1621.0 ± 19.7 | 1120.9 ± 15.6 | 0.69 | 752 / 382 | no |
| bldg29 | report | 1939.7 ± 97.4 | 1348.8 ± 28.8 | 0.70 | 754 / 382 | yes |
| bldg3 | infer | 1370.7 ± 10.7 | 986.8 ± 10.8 | 0.72 | 750 / 378 | yes |
| bldg3 | validate | 1628.4 ± 21.0 | 1120.1 ± 10.5 | 0.69 | 752 / 383 | no |
| bldg3 | report | 1905.0 ± 18.5 | 1325.0 ± 18.3 | 0.70 | 751 / 383 | yes |
| bldg30 | infer | 1642.0 ± 15.8 | 1185.6 ± 8.5 | 0.72 | 799 / 395 | yes |
| bldg30 | validate | 2022.0 ± 25.9 | 1401.2 ± 15.5 | 0.69 | 799 / 414 | no |
| bldg30 | report | 3087.3 ± 18.0 | 2402.9 ± 19.8 | 0.78 | 803 / 429 | yes |
| bldg31 | infer | 1406.3 ± 11.4 | 1008.5 ± 19.4 | 0.72 | 749 / 378 | yes |
| bldg31 | validate | 1655.1 ± 89.2 | 1138.5 ± 43.0 | 0.69 | 751 / 381 | no |
| bldg31 | report | 1936.2 ± 296.0 | 1338.2 ± 68.1 | 0.69 | 751 / 382 | yes |
| bldg32 | infer | 1802.1 ± 54.4 | 1344.6 ± 93.4 | 0.75 | 818 / 405 | yes |
| bldg32 | validate | 2377.3 ± 113.3 | 1559.8 ± 54.2 | 0.66 | 816 / 437 | no |
| bldg32 | report | 3691.6 ± 80.1 | 3067.8 ± 217.9 | 0.83 | 833 / 455 | yes |
| bldg33 | infer | 1358.3 ± 7.9 | 987.9 ± 12.6 | 0.73 | 751 / 377 | yes |
| bldg33 | validate | 1814.5 ± 105.7 | 1173.2 ± 319.6 | 0.65 | 753 / 382 | no |
| bldg33 | report | 2075.8 ± 924.8 | 1476.5 ± 261.0 | 0.71 | 751 / 382 | yes |
| bldg34 | infer | 1604.2 ± 120.7 | 1147.3 ± 126.9 | 0.72 | 779 / 389 | yes |
| bldg34 | validate | 1925.4 ± 79.2 | 1323.8 ± 96.6 | 0.69 | 776 / 401 | no |
| bldg34 | report | 2516.8 ± 11.6 | 1857.1 ± 25.1 | 0.74 | 777 / 408 | yes |
| bldg35 | infer | 1395.2 ± 13.8 | 1009.9 ± 11.4 | 0.72 | 757 / 380 | yes |
| bldg35 | validate | 1676.3 ± 30.9 | 1155.9 ± 13.5 | 0.69 | 758 / 386 | no |
| bldg35 | report | 2078.8 ± 68.9 | 1502.9 ± 82.0 | 0.72 | 756 / 388 | yes |
| bldg36 | infer | 1477.6 ± 89.2 | 1054.0 ± 20.0 | 0.71 | 766 / 382 | yes |
| bldg36 | validate | 1783.5 ± 203.9 | 1218.4 ± 44.2 | 0.68 | 769 / 392 | no |
| bldg36 | report | 2287.1 ± 18.4 | 1660.5 ± 25.2 | 0.73 | 771 / 395 | yes |
| bldg37 | infer | 2826.2 ± 241.3 | 2077.8 ± 33.8 | 0.74 | 926 / 456 | yes |
| bldg37 | validate | 4052.5 ± 87.3 | 2736.4 ± 75.3 | 0.68 | 992 / 556 | no |
| bldg37 | report | 7117.0 ± 65.0 | 5883.9 ± 33.9 | 0.83 | 997 / 578 | yes |
| bldg38 | infer | 1417.3 ± 32.9 | 1025.8 ± 9.1 | 0.72 | 752 / 378 | yes |
| bldg38 | validate | 1658.1 ± 31.5 | 1143.8 ± 11.1 | 0.69 | 751 / 382 | no |
| bldg38 | report | 2030.4 ± 133.6 | 1381.1 ± 70.2 | 0.68 | 753 / 382 | yes |
| bldg39 | infer | 1441.6 ± 409.2 | 1068.0 ± 71.4 | 0.74 | 762 / 382 | yes |
| bldg39 | validate | 1728.1 ± 195.6 | 1186.0 ± 25.3 | 0.69 | 760 / 390 | no |
| bldg39 | report | 2149.9 ± 20.2 | 1545.0 ± 17.3 | 0.72 | 761 / 391 | yes |
| bldg4 | infer | 1464.4 ± 30.1 | 1079.5 ± 47.2 | 0.74 | 770 / 386 | yes |
| bldg4 | validate | 1787.3 ± 50.1 | 1223.1 ± 198.0 | 0.68 | 770 / 395 | no |
| bldg4 | report | 2315.3 ± 55.1 | 1693.3 ± 29.1 | 0.73 | 771 / 398 | yes |
| bldg40 | infer | 1613.7 ± 44.8 | 1170.7 ± 21.8 | 0.73 | 791 / 393 | yes |
| bldg40 | validate | 2007.7 ± 57.7 | 1373.0 ± 30.7 | 0.68 | 789 / 412 | no |
| bldg40 | report | 2906.8 ± 80.7 | 2208.1 ± 8.2 | 0.76 | 791 / 419 | yes |
| bldg41 | infer | 1391.8 ± 41.4 | 997.2 ± 35.0 | 0.72 | 753 / 380 | yes |
| bldg41 | validate | 1623.7 ± 44.1 | 1159.5 ± 34.8 | 0.71 | 754 / 384 | no |
| bldg41 | report | 1958.1 ± 21.9 | 1378.3 ± 37.6 | 0.70 | 753 / 384 | yes |
| bldg42 | infer | 1491.0 ± 82.7 | 1066.0 ± 38.2 | 0.71 | 763 / 382 | yes |
| bldg42 | validate | 1780.7 ± 87.6 | 1225.3 ± 68.1 | 0.69 | 765 / 391 | no |
| bldg42 | report | 2220.3 ± 90.9 | 1603.2 ± 97.8 | 0.72 | 764 / 394 | yes |
| bldg43 | infer | 1521.6 ± 22.0 | 1110.7 ± 9.0 | 0.73 | 777 / 387 | yes |
| bldg43 | validate | 1863.1 ± 37.2 | 1285.3 ± 37.8 | 0.69 | 781 / 404 | no |
| bldg43 | report | 2553.9 ± 125.5 | 1911.1 ± 52.0 | 0.75 | 777 / 405 | yes |
| bldg44 | infer | 1498.9 ± 86.7 | 1078.0 ± 13.4 | 0.72 | 772 / 386 | yes |
| bldg44 | validate | 1909.1 ± 102.0 | 1257.9 ± 172.6 | 0.66 | 772 / 399 | no |
| bldg44 | report | 2475.8 ± 134.4 | 1796.4 ± 75.5 | 0.73 | 775 / 400 | yes |
| bldg5 | infer | 1720.4 ± 105.7 | 1247.2 ± 33.4 | 0.72 | 806 / 399 | yes |
| bldg5 | validate | 2208.5 ± 64.8 | 1517.8 ± 275.0 | 0.69 | 805 / 434 | no |
| bldg5 | report | 3381.0 ± 40.4 | 2765.4 ± 133.1 | 0.82 | 817 / 443 | yes |
| bldg6 | infer | 1738.3 ± 5.1 | 1263.4 ± 24.5 | 0.73 | 813 / 401 | yes |
| bldg6 | validate | 2326.8 ± 152.8 | 1565.8 ± 46.0 | 0.67 | 815 / 439 | no |
| bldg6 | report | 3607.4 ± 196.1 | 2854.7 ± 35.2 | 0.79 | 828 / 452 | yes |
| bldg7 | infer | 1413.5 ± 17.3 | 1035.0 ± 51.7 | 0.73 | 756 / 381 | yes |
| bldg7 | validate | 1662.9 ± 17.3 | 1148.1 ± 35.3 | 0.69 | 757 / 386 | no |
| bldg7 | report | 2085.8 ± 72.0 | 1469.5 ± 36.8 | 0.70 | 758 / 389 | yes |
| bldg8 | infer | 1475.7 ± 49.9 | 1067.4 ± 9.9 | 0.72 | 771 / 385 | yes |
| bldg8 | validate | 1840.5 ± 56.0 | 1269.9 ± 100.1 | 0.69 | 773 / 395 | no |
| bldg8 | report | 2324.9 ± 18.3 | 1691.1 ± 571.7 | 0.73 | 771 / 398 | yes |
| bldg9 | infer | 1745.9 ± 33.2 | 1326.7 ± 110.9 | 0.76 | 808 / 401 | yes |
| bldg9 | validate | 2171.0 ± 27.6 | 1489.0 ± 16.5 | 0.69 | 807 / 432 | no |
| bldg9 | report | 3380.1 ± 14.7 | 2663.4 ± 20.1 | 0.79 | 815 / 442 | yes |
| smc | infer | 1622.8 ± 18.5 | 1175.9 ± 25.3 | 0.72 | 801 / 395 | yes |
| smc | validate | 2025.3 ± 9.2 | 1399.3 ± 14.7 | 0.69 | 799 / 415 | no |
| smc | report | 3107.3 ± 543.9 | 2415.9 ± 36.6 | 0.78 | 802 / 430 | yes |


### s223 vs `4118547`

| Case | Operation | Old median ± spread (ms) | New median ± spread (ms) | New/old | Old/new RSS (MiB) | Comparison equal |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| NIST-HPL | infer | 741.7 ± 45.3 | 500.0 ± 30.7 | 0.67 | 445 / 211 | yes |
| NIST-HPL | validate | 832.9 ± 14.9 | 539.4 ± 29.2 | 0.65 | 444 / 219 | yes |
| NIST-HPL | report | 1109.5 ± 19.4 | 811.8 ± 9.4 | 0.73 | 455 / 242 | yes |
| NIST-IBAL | infer | 891.4 ± 5.7 | 609.8 ± 25.1 | 0.68 | 476 / 220 | yes |
| NIST-IBAL | validate | 1207.1 ± 20.5 | 896.1 ± 3.7 | 0.74 | 476 / 254 | no |
| NIST-IBAL | report | 2979.9 ± 47.9 | 2659.1 ± 66.3 | 0.89 | 655 / 437 | yes |
| design-patterns | infer | 689.5 ± 10.0 | 469.7 ± 15.1 | 0.68 | 443 / 209 | yes |
| design-patterns | validate | 803.4 ± 17.7 | 511.0 ± 16.8 | 0.64 | 442 / 216 | no |
| design-patterns | report | 945.4 ± 12.9 | 645.1 ± 12.7 | 0.68 | 442 / 225 | yes |
| guideline36-2021-A-1 | infer | 690.8 ± 16.8 | 455.8 ± 23.4 | 0.66 | 441 / 209 | yes |
| guideline36-2021-A-1 | validate | 783.1 ± 17.2 | 514.8 ± 17.6 | 0.66 | 440 / 214 | yes |
| guideline36-2021-A-1 | report | 852.2 ± 11.5 | 565.2 ± 16.7 | 0.66 | 440 / 215 | yes |
| guideline36-2021-A-2 | infer | 693.3 ± 6.6 | 465.9 ± 20.3 | 0.67 | 441 / 209 | yes |
| guideline36-2021-A-2 | validate | 800.9 ± 19.7 | 503.9 ± 24.7 | 0.63 | 441 / 214 | no |
| guideline36-2021-A-2 | report | 900.8 ± 9.4 | 609.6 ± 22.3 | 0.68 | 440 / 218 | yes |
| guideline36-2021-A-3 | infer | 702.0 ± 19.0 | 469.6 ± 10.7 | 0.67 | 441 / 209 | yes |
| guideline36-2021-A-3 | validate | 807.3 ± 12.2 | 514.7 ± 20.3 | 0.64 | 440 / 215 | no |
| guideline36-2021-A-3 | report | 913.5 ± 16.1 | 628.5 ± 20.3 | 0.69 | 442 / 221 | yes |
| guideline36-2021-A-4 | infer | 692.7 ± 11.8 | 473.1 ± 15.8 | 0.68 | 441 / 209 | yes |
| guideline36-2021-A-4 | validate | 808.1 ± 20.3 | 523.6 ± 13.8 | 0.65 | 442 / 215 | no |
| guideline36-2021-A-4 | report | 929.2 ± 6.5 | 624.7 ± 19.8 | 0.67 | 441 / 222 | yes |
| guideline36-2021-A-7 | infer | 688.6 ± 10.3 | 464.4 ± 15.3 | 0.67 | 440 / 209 | yes |
| guideline36-2021-A-7 | validate | 792.4 ± 9.8 | 523.8 ± 13.7 | 0.66 | 440 / 215 | no |
| guideline36-2021-A-7 | report | 879.3 ± 21.0 | 575.8 ± 21.9 | 0.65 | 440 / 217 | yes |
| guideline36-2021-A-8 | infer | 690.9 ± 22.0 | 468.1 ± 13.7 | 0.68 | 441 / 209 | yes |
| guideline36-2021-A-8 | validate | 796.6 ± 24.8 | 520.7 ± 15.7 | 0.65 | 440 / 215 | no |
| guideline36-2021-A-8 | report | 878.2 ± 7.1 | 584.2 ± 17.4 | 0.67 | 440 / 217 | yes |
| guideline36-2021-A-9 | infer | 746.1 ± 17.2 | 512.7 ± 11.5 | 0.69 | 447 / 212 | yes |
| guideline36-2021-A-9 | validate | 904.7 ± 5.6 | 640.8 ± 12.0 | 0.71 | 447 / 225 | no |
| guideline36-2021-A-9 | report | 1265.7 ± 9.9 | 983.0 ± 8.5 | 0.78 | 463 / 254 | yes |
| lbnl-bdg3-1 | infer | 1325.0 ± 12.0 | 1047.7 ± 14.2 | 0.79 | 550 / 248 | yes |
| lbnl-bdg3-1 | validate | 1894.5 ± 18.8 | 1577.1 ± 30.7 | 0.83 | 572 / 329 | no |
| lbnl-bdg3-1 | report | 5594.9 ± 32.7 | 5263.9 ± 55.3 | 0.94 | 929 / 699 | yes |
| lbnl-bdg4-1 | infer | 1043.0 ± 9.7 | 743.2 ± 8.5 | 0.71 | 503 / 231 | yes |
| lbnl-bdg4-1 | validate | 1462.3 ± 21.2 | 1117.9 ± 9.4 | 0.76 | 528 / 295 | no |
| lbnl-bdg4-1 | report | 4422.4 ± 24.8 | 4073.0 ± 37.1 | 0.92 | 800 / 581 | yes |
| lbnl-example-radiant | infer | 778.4 ± 18.4 | 572.3 ± 17.5 | 0.74 | 451 / 213 | yes |
| lbnl-example-radiant | validate | 903.8 ± 6.8 | 635.4 ± 21.5 | 0.70 | 450 / 227 | no |
| lbnl-example-radiant | report | 1500.7 ± 23.2 | 1250.0 ± 19.8 | 0.83 | 509 / 293 | yes |
| nist-bdg1-1 | infer | 759.7 ± 24.3 | 505.5 ± 20.3 | 0.67 | 452 / 214 | yes |
| nist-bdg1-1 | validate | 936.8 ± 7.4 | 655.1 ± 15.0 | 0.70 | 452 / 228 | yes |
| nist-bdg1-1 | report | 1494.8 ± 17.3 | 1203.6 ± 17.3 | 0.81 | 497 / 281 | yes |
| nrel-example | infer | 1095.0 ± 13.0 | 826.1 ± 15.5 | 0.75 | 480 / 222 | yes |
| nrel-example | validate | 1379.3 ± 21.5 | 1082.2 ± 3.3 | 0.78 | 485 / 266 | no |
| nrel-example | report | 3478.1 ± 20.3 | 3143.7 ± 13.8 | 0.90 | 684 / 467 | yes |
| pnnl-bdg1-2 | infer | 707.4 ± 21.0 | 471.9 ± 17.5 | 0.67 | 445 / 210 | yes |
| pnnl-bdg1-2 | validate | 867.9 ± 3.8 | 584.9 ± 1.5 | 0.67 | 444 / 222 | no |
| pnnl-bdg1-2 | report | 1073.1 ± 16.0 | 789.1 ± 11.0 | 0.74 | 448 / 238 | yes |
| pnnl-bdg2-1 | infer | 1823.4 ± 27.5 | 1296.4 ± 81.9 | 0.71 | 647 / 275 | yes |
| pnnl-bdg2-1 | validate | 4257.3 ± 646.9 | 3726.3 ± 116.3 | 0.88 | 942 / 620 | no |
| pnnl-bdg2-1 | report | 12844.6 ± 72.0 | 12463.8 ± 256.7 | 0.97 | 1793 / 1548 | yes |
| pnnl-bdg3-2 | infer | 1884.4 ± 273.0 | 1239.5 ± 142.0 | 0.66 | 625 / 270 | yes |
| pnnl-bdg3-2 | validate | 2692.9 ± 22.9 | 2128.5 ± 11.9 | 0.79 | 695 / 403 | no |
| pnnl-bdg3-2 | report | 10235.4 ± 55.9 | 9689.7 ± 12.6 | 0.95 | 1476 / 1229 | yes |
| scb-vrf | infer | 986.0 ± 6.9 | 748.7 ± 13.9 | 0.76 | 488 / 226 | yes |
| scb-vrf | validate | 1328.7 ± 34.0 | 1051.4 ± 72.2 | 0.79 | 494 / 273 | no |
| scb-vrf | report | 2934.5 ± 17.3 | 2655.2 ± 13.2 | 0.90 | 655 / 446 | yes |


### Generated vs `c6d0b1d`

| Case | Operation | Old median ± spread (ms) | New median ± spread (ms) | New/old | Old/new RSS (MiB) | Comparison equal |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| tiny-source-large-data | infer | 74.3 ± 4.9 | 45.3 ± 6.1 | 0.61 | 83 / 27 | yes |
| tiny-source-large-data | validate | 84.2 ± 8.1 | 62.6 ± 2.6 | 0.74 | 108 / 54 | yes |
| tiny-source-large-data | report | 96.0 ± 4.1 | 72.8 ± 3.2 | 0.76 | 108 / 53 | yes |
| large-source-small-data | infer | 29.0 ± 2.1 | 17.6 ± 1.0 | 0.61 | 26 / 11 | yes |
| large-source-small-data | validate | 31.7 ± 3.3 | 20.7 ± 0.7 | 0.65 | 30 / 16 | yes |
| large-source-small-data | report | 41.3 ± 0.5 | 29.7 ± 1.5 | 0.72 | 30 / 16 | no |
| both-large-dense-demand | infer | 102.2 ± 3.6 | 54.2 ± 4.0 | 0.53 | 101 / 34 | yes |
| both-large-dense-demand | validate | 127.4 ± 5.1 | 85.0 ± 6.8 | 0.67 | 134 / 70 | yes |
| both-large-dense-demand | report | 212.1 ± 11.3 | 170.3 ± 15.0 | 0.80 | 129 / 63 | yes |
| source-data-overlap | infer | 21.7 ± 0.4 | 15.3 ± 0.5 | 0.71 | 22 / 10 | yes |
| source-data-overlap | validate | 25.8 ± 2.2 | 19.5 ± 0.5 | 0.75 | 28 / 16 | yes |
| source-data-overlap | report | 31.8 ± 2.3 | 25.6 ± 2.0 | 0.81 | 28 / 16 | yes |
| mixed-native-sparql | infer | 18.2 ± 0.2 | 13.7 ± 0.5 | 0.75 | 19 / 9 | yes |
| mixed-native-sparql | validate | 38.4 ± 0.5 | 33.0 ± 5.1 | 0.86 | 28 / 18 | yes |
| mixed-native-sparql | report | 40.6 ± 2.4 | 35.6 ± 0.2 | 0.88 | 27 / 20 | yes |
| many-predicates-wildcard | infer | 17.5 ± 0.4 | 13.3 ± 0.3 | 0.76 | 18 / 8 | yes |
| many-predicates-wildcard | validate | 39.4 ± 1.4 | 34.9 ± 0.5 | 0.89 | 34 / 25 | yes |
| many-predicates-wildcard | report | 77.8 ± 0.8 | 71.7 ± 1.6 | 0.92 | 64 / 56 | yes |
| fallback-named-shapes | infer | 26.0 ± 0.7 | 17.0 ± 0.6 | 0.65 | 25 / 11 | yes |
| fallback-named-shapes | validate | 48.0 ± 3.1 | 37.6 ± 1.6 | 0.78 | 36 / 23 | yes |
| fallback-named-shapes | report | 50.9 ± 0.7 | 41.9 ± 0.7 | 0.82 | 35 / 25 | yes |
| no-active-rules | infer | 16.2 ± 1.7 | 13.5 ± 0.9 | 0.83 | 17 / 9 | yes |
| no-active-rules | validate | 20.9 ± 1.7 | 16.5 ± 0.7 | 0.79 | 22 / 13 | yes |
| no-active-rules | report | 22.4 ± 1.2 | 18.5 ± 2.1 | 0.82 | 21 / 13 | yes |
| triple-rule-rounds | infer | 43.7 ± 2.3 | 29.0 ± 1.8 | 0.66 | 36 / 15 | yes |
| triple-rule-rounds | validate | 42.0 ± 7.9 | 28.0 ± 1.3 | 0.67 | 44 / 22 | yes |
| triple-rule-rounds | report | 53.5 ± 2.2 | 38.4 ± 1.9 | 0.72 | 44 / 22 | yes |
| sparql-rule-rounds | infer | 71.4 ± 43.7 | 39.0 ± 5.9 | 0.55 | 48 / 18 | yes |
| sparql-rule-rounds | validate | 79.1 ± 5.2 | 40.4 ± 1.4 | 0.51 | 56 / 25 | yes |
| sparql-rule-rounds | report | 80.4 ± 5.2 | 48.1 ± 2.5 | 0.60 | 56 / 25 | yes |
