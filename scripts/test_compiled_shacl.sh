#!/usr/bin/env bash
set -euo pipefail

SHAPES_FILE="ttl/Brick.ttl"
DATA_FILE="ttl/small-brick-model.ttl"
OUT_DIR="test-compiled-shacl"
BIN_NAME="myshapes"

time cargo run -p cli -- compile --shapes-file "$SHAPES_FILE" --out-dir "$OUT_DIR" --bin-name "$BIN_NAME"

BIN_PATH="./$BIN_NAME"
if [[ ! -x "$BIN_PATH" && -x "$OUT_DIR/$BIN_NAME" ]]; then
  BIN_PATH="$OUT_DIR/$BIN_NAME"
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "error: compiled binary not found at ./$BIN_NAME or $OUT_DIR/$BIN_NAME" >&2
  exit 1
fi

RESULT="$($BIN_PATH "$DATA_FILE")"
printf '%s\n' "$RESULT"
