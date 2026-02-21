#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/compiled-shacl}"
BIN_NAME="${BIN_NAME:-brickexe}"
SHAPES_FILE="${SHAPES_FILE:-$ROOT_DIR/ttl/Brick.ttl}"
DATA_FILE="${DATA_FILE:-${1:-}}"

mkdir -p "$OUT_DIR"

echo "Compiling with srcgen specialized backend (default compiler track)..."
cargo run -p cli --features shacl-compiler -- compile \
  --shapes-file "$SHAPES_FILE" \
  --backend specialized \
  --out-dir "$OUT_DIR" \
  --bin-name "$BIN_NAME" \
  --shifty-path "$ROOT_DIR/lib" \
  --plan-out "$OUT_DIR/srcgen.ir.json" \
  --release

BIN_PATH="$OUT_DIR/target/release/$BIN_NAME"
echo "Built executable: $BIN_PATH"
echo "Wrote SrcGenIR: $OUT_DIR/srcgen.ir.json"

if [[ -n "$DATA_FILE" ]]; then
  echo "Running compiled binary with generated inference enabled..."
  "$BIN_PATH" --run-inference=true "$DATA_FILE"
else
  echo "No data file supplied. To run validation with inference:"
  echo "  $BIN_PATH --run-inference=true <data.ttl>"
fi
