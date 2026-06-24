#!/usr/bin/env bash
# Build the shifty WebAssembly module and generate JS/TS bindings into ./pkg.
#
# Requirements:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-bindgen-cli --version 0.2.125   # must match Cargo.lock
#   (optional) wasm-opt from binaryen, for a smaller bundle
#
# Usage: crates/shifty-wasm/build.sh [--target web|bundler|nodejs]
set -euo pipefail

TARGET="web"
if [[ "${1:-}" == "--target" ]]; then
  TARGET="${2:?--target needs a value}"
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CRATE_DIR="$ROOT/crates/shifty-wasm"
WASM="$ROOT/target/wasm32-unknown-unknown/wasm-release/shifty_wasm.wasm"
OUT="$CRATE_DIR/pkg"

echo ">> cargo build (wasm32-unknown-unknown, wasm-release)"
cargo build -p shifty-wasm --target wasm32-unknown-unknown --profile wasm-release

echo ">> wasm-bindgen (--target $TARGET)"
wasm-bindgen "$WASM" --out-dir "$OUT" --target "$TARGET"

if command -v wasm-opt >/dev/null 2>&1; then
  echo ">> wasm-opt -Oz"
  # Enable the post-MVP features the Rust toolchain emits (bulk-memory, etc.);
  # otherwise wasm-opt's validator rejects the module.
  wasm-opt -Oz \
    --enable-bulk-memory --enable-bulk-memory-opt \
    --enable-nontrapping-float-to-int --enable-sign-ext \
    --enable-mutable-globals --enable-multivalue --enable-reference-types \
    "$OUT/shifty_wasm_bg.wasm" -o "$OUT/shifty_wasm_bg.wasm"
else
  echo ">> wasm-opt not found; skipping size optimization (install binaryen for a smaller bundle)"
fi

echo ">> done -> $OUT"
ls -lh "$OUT"
