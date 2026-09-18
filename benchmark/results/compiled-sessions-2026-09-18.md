# Compiled session lifecycle measurements

Exploratory single-process runs on arm64 macOS 26.6.2 with Rust 1.98.1, using
the release build of `crates/shifty-engine/examples/bench_sessions.rs`.
`/usr/bin/time -l` supplied peak resident memory. Times are milliseconds;
"repeat" is the mean of three validations on the same prepared session. Each
row ran in a fresh process. The edit deleted one asserted triple, then created
and validated a new session while retaining the original sessions.

| Workload | Inference | Load shapes | Compile | First session | First validate | Repeat | Second session | Second validate | Edited session | Edited validate | Peak RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Small AF fixtures | Off | 0.15 | 0.19 | 0.00 | 0.25 | 0.01 | 0.00 | 0.05 | 0.01 | 0.16 | 5.2 |
| Small AF fixtures | On | 0.13 | 0.17 | 0.88 | 0.21 | 0.01 | 0.09 | 0.04 | 0.08 | 0.10 | 5.4 |
| Brick closure, bldg1 / bldg2 | Off | 517.93 | 133.19 | 0.00 | 317.74 | 123.40 | 0.00 | 324.13 | 0.11 | 313.49 | 441.5 |
| Brick closure, bldg1 / bldg2 | On | 534.06 | 130.29 | 1064.35 | 363.63 | 154.80 | 1028.60 | 360.88 | 1023.31 | 336.54 | 939.9 |
| 223P closure, NIST bdg1 / PNNL bdg1 | Off | 294.17 | 50.92 | 0.00 | 230.25 | 51.50 | 0.00 | 189.58 | 0.36 | 217.53 | 300.3 |
| 223P closure, NIST bdg1 / PNNL bdg1 | On | 290.00 | 50.08 | 603.24 | 225.59 | 57.43 | 525.82 | 194.89 | 538.18 | 231.27 | 579.2 |

The first validation builds the lazy index and plan. Repeated validation reuses
them. Inference shifts work into session construction and raises peak memory;
the Brick inference run peaked near 940 MiB. After its first validation, the
Brick process held about 830 MiB RSS; retaining a second prepared session added
about 47 MiB. The 223P inference run added about 43 MiB. These increments are
smaller than another full shapes graph, a useful check on sharing, but a single
run and allocator-retained memory cannot prove the absence of every duplicate.

To reproduce, build the example in release mode and run each workload in a
separate process under `/usr/bin/time -l` (or `-v` on Linux):

```sh
cargo build --release -p shifty-engine --example bench_sessions
/usr/bin/time -l target/release/examples/bench_sessions \
  benchmark/brick/Brick-closure.ttl \
  benchmark/brick/models/bldg1.ttl \
  benchmark/brick/models/bldg2.ttl --infer
```
