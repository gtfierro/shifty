#!/usr/bin/env bash
# Run benchmarks for every release tag >= v0.1.0 and save results under
# benchmark/results/<tag>/{brick,s223}.txt.
#
# Models and shapes are always taken from *this* checkout so comparisons are
# apples-to-apples. Only the binary being benchmarked changes per version.
#
# Usage:
#   ./benchmark/run_history.sh              # run all tags >= v0.1.0
#   ./benchmark/run_history.sh v0.1.5       # start from a specific tag
#   BENCH_ITERS=5 ./benchmark/run_history.sh  # more iterations (default 3)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

START_TAG="${1:-v0.1.0}"

# Collect release tags >= START_TAG using version-sort (skip non-version tags)
TAGS=$(git -C "$ROOT" tag | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | awk -v start="$START_TAG" \
    'found || $0==start { found=1; print }')

if [[ -z "$TAGS" ]]; then
    echo "No tags found >= $START_TAG" >&2
    exit 1
fi

WORKTREE_BASE="$(mktemp -d -t shifty-bench-XXXXXX)"
trap 'git -C "$ROOT" worktree prune; rm -rf "$WORKTREE_BASE"' EXIT

for tag in $TAGS; do
    BRICK_OUT="$RESULTS_DIR/$tag/brick.txt"
    S223_OUT="$RESULTS_DIR/$tag/s223.txt"

    if [[ -f "$BRICK_OUT" && -f "$S223_OUT" ]]; then
        echo "[$tag] already done, skipping" >&2
        continue
    fi

    echo "[$tag] checking out…" >&2
    WT="$WORKTREE_BASE/$tag"
    git -C "$ROOT" worktree add --quiet "$WT" "$tag"

    echo "[$tag] building release binary…" >&2
    if ! cargo build --release --quiet --manifest-path "$WT/Cargo.toml" 2>&1; then
        echo "[$tag] build FAILED — skipping" >&2
        git -C "$ROOT" worktree remove --force "$WT" 2>/dev/null || true
        continue
    fi

    BINARY="$WT/target/release/shifty"
    if [[ ! -x "$BINARY" ]]; then
        echo "[$tag] binary not found after build — skipping" >&2
        git -C "$ROOT" worktree remove --force "$WT" 2>/dev/null || true
        continue
    fi

    mkdir -p "$RESULTS_DIR/$tag"

    # Use the slim history bench (validate-only, 1 iteration) to keep
    # runtime manageable across many versions. Use bench_common.sh directly
    # (with BENCH_ITERS=3) for the current version for higher precision.
    BENCH_SCRIPT="${BENCH_SCRIPT:-$SCRIPT_DIR/bench_history.sh}"

    echo "[$tag] benchmarking Brick…" >&2
    if ! BENCH_ITERS="${BENCH_ITERS:-1}" bash "$BENCH_SCRIPT" \
        "$SCRIPT_DIR/brick/Brick-closure.ttl" \
        "$SCRIPT_DIR/brick/models" \
        "$BINARY" > "$BRICK_OUT" 2>&1; then
        echo "[$tag] Brick benchmark FAILED" >&2
        rm -f "$BRICK_OUT"
    fi

    echo "[$tag] benchmarking S223…" >&2
    if ! BENCH_ITERS="${BENCH_ITERS:-1}" bash "$BENCH_SCRIPT" \
        "$SCRIPT_DIR/s223/223p-closure.ttl" \
        "$SCRIPT_DIR/s223/models" \
        "$BINARY" > "$S223_OUT" 2>&1; then
        echo "[$tag] S223 benchmark FAILED" >&2
        rm -f "$S223_OUT"
    fi

    echo "[$tag] removing worktree…" >&2
    git -C "$ROOT" worktree remove --force "$WT" 2>/dev/null || true

    echo "[$tag] done" >&2
done

echo "All done. Results in $RESULTS_DIR" >&2
