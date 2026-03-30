#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 <shapes.ttl> <data.ttl> [--include-rules]"
    exit 1
fi

SHAPES_FILE=$1
DATA_FILE=$2
INCLUDE_RULES=${3:-}
TRACE_FILE="trace.jsonl"
FOLDED_FILE="folded.txt"
SVG_FILE="shape_flamegraph.svg"
SHAPES_JSON_FILE="trace.shapes.json"
SHAPE_CBD_FILE="trace.shape-cbd.ttl"

if [ ! -f "target/release/shifty" ]; then
    echo "Building shifty in release mode..."
    cargo build -p cli --release
fi

if [ ! -f "generate_flamegraph.py" ]; then
    echo "Error: generate_flamegraph.py not found in current directory."
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 not found."
    exit 1
fi

if ! command -v inferno-flamegraph >/dev/null 2>&1; then
    echo "Error: inferno-flamegraph not found. Install it with: cargo install inferno"
    exit 1
fi

GENERATOR_ARGS=()
VALIDATE_ARGS=()
if [ -n "$INCLUDE_RULES" ]; then
    if [ "$INCLUDE_RULES" != "--include-rules" ]; then
        echo "Error: unsupported optional argument. Use --include-rules"
        echo "Usage: $0 <shapes.ttl> <data.ttl> [--include-rules]"
        exit 1
    fi
    VALIDATE_ARGS+=(--run-inference)
    GENERATOR_ARGS+=(--include-rules)
fi

echo "Running SHACL validation with tracing (serial mode)..."
RAYON_NUM_THREADS=1 ./target/release/shifty validate     --shapes-file "$SHAPES_FILE"     --data-file "$DATA_FILE"     --trace-jsonl "$TRACE_FILE"     --format dump     "${VALIDATE_ARGS[@]}" > /dev/null

echo "Processing trace into folded stacks..."
python3 generate_flamegraph.py "$TRACE_FILE" "${GENERATOR_ARGS[@]}" > "$FOLDED_FILE"

echo "Generating flamegraph SVG..."
inferno-flamegraph < "$FOLDED_FILE" > "$SVG_FILE"

echo "Success! Flamegraph generated: $SVG_FILE"
echo "Shape metadata: $SHAPES_JSON_FILE"
echo "Shape CBD: $SHAPE_CBD_FILE"
