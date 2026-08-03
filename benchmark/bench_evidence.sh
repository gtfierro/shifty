#!/usr/bin/env bash
# Evidence-tracing overhead across the Brick and 223P model corpora.
#
# For every model: prepare one validator snapshot (parse, inference,
# normalization, indexing — all untimed), then time conformance-only execution
# against dual-evidence execution from that same snapshot.
#
# Usage:
#   ./benchmark/bench_evidence.sh                  # both stock suites -> CSV
#   ./benchmark/bench_evidence.sh brick            # one suite
#   ./benchmark/bench_evidence.sh lubm             # generated LUBM corpus,
#                                                  # see lubm/generate.sh
#   BENCH_ITERS=9 ./benchmark/bench_evidence.sh > results/evidence.csv
#
# BENCH_ITERS (default 5) is the timed iteration count, the same for every
# model. BENCH_BUDGET_MS is an escape hatch for corpora too slow for a fixed
# count: set it to a millisecond budget and the count adapts down to
# BENCH_MIN_ITERS (default 3). It is 0 (off) by default.
#
# Then: ./benchmark/summarize_evidence.py results/evidence.csv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ITERATIONS="${BENCH_ITERS:-5}"
MIN_ITERS="${BENCH_MIN_ITERS:-3}"
BUDGET_MS="${BENCH_BUDGET_MS:-0}"
SUITES=("$@")
if [ ${#SUITES[@]} -eq 0 ]; then
    SUITES=(brick s223)
fi

BIN="$ROOT/target/release/examples/bench_evidence"

echo "building bench_evidence…" >&2
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" \
    -p shifty-engine --example bench_evidence 2>&1 | tail -3 >&2

header_flag=""
for suite in "${SUITES[@]}"; do
    case "$suite" in
        brick) shapes="$SCRIPT_DIR/brick/Brick-closure.ttl" ;;
        s223)  shapes="$SCRIPT_DIR/s223/223p-closure.ttl" ;;
        lubm)  shapes="$SCRIPT_DIR/lubm/lubm-closure.ttl"
               if [ ! -f "$shapes" ]; then
                   echo "lubm suite not generated yet: run ./benchmark/lubm/generate.sh" >&2
                   exit 2
               fi ;;
        *) echo "unknown suite: $suite (expected 'brick', 's223', or 'lubm')" >&2; exit 2 ;;
    esac

    echo "=== $suite ===" >&2
    # shellcheck disable=SC2086
    "$BIN" \
        --shapes "$shapes" \
        --models "$SCRIPT_DIR/$suite/models" \
        --suite "$suite" \
        --iters "$ITERATIONS" \
        --budget-ms "$BUDGET_MS" \
        --min-iters "$MIN_ITERS" \
        $header_flag
    header_flag="--no-header"
done
