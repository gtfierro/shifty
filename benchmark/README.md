# Benchmarks

Timing benchmarks for inference and validation against real building models.

## Running

Each script builds the release binary automatically, then benchmarks every
model in the corresponding `models/` directory:

```sh
./benchmark/bench_brick.sh   # 45 Brick models
./benchmark/bench_s223.sh    # 19 ASHRAE 223P models
```

Each script prints an aligned table of mean ± stddev wall-clock times (ms) for
three operations across every model in the corresponding `models/` directory:

- `infer`    — SHACL-AF rule inference (`shifty infer`)
- `infer+val` — inference + validation summary (`shifty validate`)
- `report`   — inference + validation + W3C `sh:ValidationReport` output
               (`shifty validate --report`)

Set `BENCH_ITERS` to control the number of timed runs (default: 3). Models are
iterated in a stable, locale-independent (sorted) order so repeated runs line up
row-for-row.

## Comparing two runs

`benchcmp.sh` compares two bench outputs, benchcmp-style:

```sh
./benchmark/bench_brick.sh > old.txt
# …change something, rebuild…
./benchmark/bench_brick.sh > new.txt
./benchmark/benchcmp.sh old.txt new.txt
```

For each column (infer, validate, report) it prints a per-model table of old ms,
new ms, and the percentage delta (new relative to old; negative is faster),
followed by a geometric-mean delta. Rows are matched by model name, and models
present in only one file are reported on stderr.

## Data

```
benchmark/
  brick/
    Brick.ttl              # Brick ontology
    Brick-closure.ttl      # Brick + all transitive OWL imports (used by scripts)
    models/                # 45 real building models
  s223/
    223p.ttl               # ASHRAE 223P ontology
    223p-closure.ttl       # 223P + all transitive OWL imports (used by scripts)
    models/                # 19 real building models (NIST, LBNL, NREL, PNNL)
```

The `*-closure.ttl` files are pre-computed and passed as the `--shapes` argument
so the engine does not need to fetch remote imports at benchmark time.
