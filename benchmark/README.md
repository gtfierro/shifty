# Benchmarks

Timing benchmarks for inference and validation against real building models.

## Running

Build the release binary first, then run either script from the repo root:

```sh
cargo build --release
./benchmark/bench_brick.sh   # 45 Brick models
./benchmark/bench_s223.sh    # 19 ASHRAE 223P models
```

Each script prints an aligned table of mean ± stddev wall-clock times (ms) for
both `infer` and `validate` across every model in the corresponding `models/`
directory. Set `BENCH_ITERS` to control the number of timed runs (default: 3).

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
