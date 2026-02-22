#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/compiled-shacl-223p}"
BIN_NAME="${BIN_NAME:-s223pexe}"
SHAPES_FILE="${SHAPES_FILE:-$ROOT_DIR/ttl/223p.ttl}"
DATA_FILE="${DATA_FILE:-${1:-}}"
FORCE_REFRESH="${FORCE_REFRESH:-true}"
TEMPORARY_ONTOENV="${TEMPORARY_ONTOENV:-true}"

mkdir -p "$OUT_DIR"

echo "Compiling with srcgen specialized backend (default compiler track)..."
compile_args=(
  --shapes-file "$SHAPES_FILE"
  --backend specialized
  --out-dir "$OUT_DIR"
  --bin-name "$BIN_NAME"
  --shifty-path "$ROOT_DIR/lib"
  --plan-out "$OUT_DIR/srcgen.ir.json"
  --release
)

if [[ "$FORCE_REFRESH" == "true" ]]; then
  compile_args+=(--force-refresh)
fi

if [[ "$TEMPORARY_ONTOENV" == "true" ]]; then
  compile_args+=(--temporary)
fi

cargo run -p cli --features shacl-compiler -- compile \
  "${compile_args[@]}"

BIN_PATH="$OUT_DIR/target/release/$BIN_NAME"
echo "Built executable: $BIN_PATH"
echo "Wrote SrcGenIR: $OUT_DIR/srcgen.ir.json"

if [[ -n "$DATA_FILE" ]]; then
  REPORT_FILE="$OUT_DIR/validation-report.ttl"
  echo "Running compiled binary with generated inference enabled..."
  echo "Report file: $REPORT_FILE"
  if "$BIN_PATH" --run-inference=true --fail-on-violations "$DATA_FILE" | tee "$REPORT_FILE"; then
    echo "Validation conforms."
  else
    status=$?
    if [[ $status -eq 2 ]]; then
      echo "Validation failed (violations found)."
      exit 2
    fi
    exit "$status"
  fi
else
  echo "No data file supplied. To run validation with inference:"
  echo "  $BIN_PATH --run-inference=true --fail-on-violations <data.ttl>"
fi
