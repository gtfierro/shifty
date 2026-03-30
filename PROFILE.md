# Profiling

## CPU Profiler (macOS Instruments)

```bash
cargo instruments -l
cargo instruments --release -t 'CPU Profiler' -- validate --data-file air_quality_sensor_example.ttl --shapes-file Brick.ttl
```

## Shape Flamegraph

Generate a shape-focused flamegraph from structured trace events:

```bash
./shape-flamegraph.sh ttl/223p.ttl ttl/small-223p.ttl
```

This defaults to validation-only timing by passing `--run-inference=false` to `shifty validate`.
Use `--include-rules` as the optional third argument if you want rule execution folded into the flamegraph as well.

The script writes:
- `trace.jsonl`
- `folded.txt`
- `shape_flamegraph.svg`

You also need:
- `python3`
- `inferno-flamegraph` (`cargo install inferno`)
