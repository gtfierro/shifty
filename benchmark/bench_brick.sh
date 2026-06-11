#!/usr/bin/env bash
# Benchmark infer and validate against every model in brick/models/
# Outputs an aligned table to stdout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SHAPES="$SCRIPT_DIR/brick/Brick-closure.ttl"
MODELS_DIR="$SCRIPT_DIR/brick/models"
BINARY="$ROOT/target/release/shacl"

ITERATIONS="${BENCH_ITERS:-3}"

# Build release binary once.
echo "building release binary…" >&2
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" 2>&1 | tail -3 >&2

# Column widths
COL_MODEL=32
COL_MS=10
COL_SD=6

sep() { printf '%0.s-' $(seq 1 "$1"); }

# Header
printf "%-${COL_MODEL}s  %${COL_MS}s  %${COL_SD}s  %${COL_MS}s  %${COL_SD}s\n" \
    "model" "infer ms" "+/-" "validate ms" "+/-"
printf "%-${COL_MODEL}s  %${COL_MS}s  %${COL_SD}s  %${COL_MS}s  %${COL_SD}s\n" \
    "$(sep $COL_MODEL)" "$(sep $COL_MS)" "$(sep $COL_SD)" "$(sep $COL_MS)" "$(sep $COL_SD)"

# time_cmd <cmd...> -> elapsed milliseconds on stdout
time_cmd() {
    local start end
    start=$(date +%s%N)
    "$@" > /dev/null 2>&1
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

# mean_stddev <cmd...> -> "MEAN STDDEV" (two integers) on stdout
mean_stddev() {
    local samples=() t i total=0 sum_sq=0 diff mean stddev
    for i in $(seq 1 "$ITERATIONS"); do
        t=$(time_cmd "$@")
        samples+=("$t")
        total=$(( total + t ))
    done
    mean=$(( total / ITERATIONS ))
    for t in "${samples[@]}"; do
        diff=$(( t - mean ))
        sum_sq=$(( sum_sq + diff * diff ))
    done
    stddev=$(awk "BEGIN { printf \"%d\", int(sqrt($sum_sq / $ITERATIONS) + 0.5) }")
    echo "$mean $stddev"
}

for model in "$MODELS_DIR"/*.ttl; do
    name="$(basename "$model")"

    read -r infer_ms infer_sd <<< "$(mean_stddev "$BINARY" infer \
        --shapes "$SHAPES" \
        --data   "$model")"

    read -r val_ms val_sd <<< "$(mean_stddev "$BINARY" validate \
        --shapes "$SHAPES" \
        --data   "$model")"

    printf "%-${COL_MODEL}s  %${COL_MS}d  %${COL_SD}d  %${COL_MS}d  %${COL_SD}d\n" \
        "$name" "$infer_ms" "$infer_sd" "$val_ms" "$val_sd"
done
