#!/usr/bin/env bash
set -euo pipefail

SHAPES_FILE="ttl/Brick.ttl"
DATA_FILE="ttl/small-brick-model.ttl"
OUT_DIR="test-compiled-shacl"
BIN_NAME="myshapes"

export CCACHE_DIR="${CCACHE_DIR:-/tmp/ccache}"
export CCACHE_TEMPDIR="${CCACHE_TEMPDIR:-/tmp/ccache-tmp}"
mkdir -p "$CCACHE_DIR" "$CCACHE_TEMPDIR"

echo "ROCKSDB_DYNAMIC=${ROCKSDB_DYNAMIC:-}"
echo "ROCKSDB_STATIC=${ROCKSDB_STATIC:-}"
echo "PKG_CONFIG_PATH=${PKG_CONFIG_PATH:-}"

if [[ -d ".pkgconfig" ]]; then
  if [[ -n "${PKG_CONFIG_PATH:-}" ]]; then
    export PKG_CONFIG_PATH="$PWD/.pkgconfig:$PKG_CONFIG_PATH"
  else
    export PKG_CONFIG_PATH="$PWD/.pkgconfig"
  fi
fi

echo "PKG_CONFIG_PATH(after)=${PKG_CONFIG_PATH:-}"
if command -v pkg-config >/dev/null 2>&1; then
  echo "pkg-config rocksdb libdir: $(pkg-config --variable=libdir rocksdb 2>/dev/null || echo unknown)"
  echo "pkg-config rocksdb libs: $(pkg-config --libs rocksdb 2>/dev/null || echo unknown)"
fi

time cargo run -p cli -- compile --shapes-file "$SHAPES_FILE" --out-dir "$OUT_DIR" --bin-name "$BIN_NAME" --release

BIN_PATH="$OUT_DIR/target/release/$BIN_NAME"
if [[ ! -x "$BIN_PATH" && -x "$OUT_DIR/$BIN_NAME" ]]; then
  BIN_PATH="$OUT_DIR/$BIN_NAME"
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "error: compiled binary not found at ./$BIN_NAME or $OUT_DIR/$BIN_NAME" >&2
  exit 1
fi

RESULT="$($BIN_PATH "$DATA_FILE")"
printf '%s\n' "$RESULT"
