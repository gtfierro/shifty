#!/usr/bin/env bash
# Benchmark infer and validate against every model in brick/models/
# Outputs an aligned table to stdout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "building release binary…" >&2
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" 2>&1 | tail -3 >&2

exec "$SCRIPT_DIR/bench_common.sh" \
    "$SCRIPT_DIR/brick/Brick-closure.ttl" \
    "$SCRIPT_DIR/brick/models" \
    "$ROOT/target/release/shifty"
