#!/usr/bin/env bash
# Run benchmarks for every release tag >= v0.1.0, then for the current
# checkout, saving results under benchmark/results/<tag>/{brick,s223}.txt.
#
# Models and shapes are always taken from *this* checkout so comparisons are
# apples-to-apples. Only the binary being benchmarked changes per version.
#
# Tagged results are reused when already present; the trailing HEAD entry is
# always rerun, since HEAD moves and a stale entry would silently misreport
# unreleased work.
#
# Usage:
#   ./benchmark/run_history.sh              # all tags >= v0.1.0, then HEAD
#   ./benchmark/run_history.sh v0.1.5       # start from a specific tag
#   BENCH_HEAD=0 ./benchmark/run_history.sh   # tags only, skip HEAD
#   BENCH_ITERS=5 ./benchmark/run_history.sh  # more samples (default 3; median reported)
#
# To benchmark only the current checkout (minutes, not hours):
#   BENCH_ONLY_HEAD=1 ./benchmark/run_history.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

START_TAG="${1:-v0.1.0}"

# Results directory (and chart label) for the current checkout.
HEAD_LABEL="HEAD"

# Collect release tags >= START_TAG using version-sort (skip non-version tags)
TAGS=$(git -C "$ROOT" tag | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | awk -v start="$START_TAG" \
    'found || $0==start { found=1; print }')

if [[ -z "$TAGS" ]]; then
    echo "No tags found >= $START_TAG" >&2
    exit 1
fi

mapfile -t TAG_LIST <<< "$TAGS"

# Benchmark the current checkout last, under the pseudo-tag "HEAD". Without it
# unreleased work is invisible: every other entry is built from a release tag,
# so changes only show up once they are tagged. Set BENCH_HEAD=0 to skip, or
# BENCH_ONLY_HEAD=1 to refresh just that entry without rerunning the history.
BENCH_HEAD="${BENCH_HEAD:-1}"
BENCH_ONLY_HEAD="${BENCH_ONLY_HEAD:-0}"
if [[ "$BENCH_ONLY_HEAD" == 1 ]]; then
    TAG_LIST=("$HEAD_LABEL")
elif [[ "$BENCH_HEAD" == 1 ]]; then
    TAG_LIST+=("$HEAD_LABEL")
fi

TOTAL_TAGS=${#TAG_LIST[@]}

BENCH_ITERS="${BENCH_ITERS:-3}"
# Use the slim history bench (infer + validate, no report) to keep runtime
# manageable across many versions. It reports the median of BENCH_ITERS runs
# (default 3) so a single slow run can't skew a version's recorded time.
BENCH_SCRIPT="${BENCH_SCRIPT:-$SCRIPT_DIR/bench_history.sh}"

count_models() { find "$1" -maxdepth 1 -name '*.ttl' | wc -l; }
BRICK_MODELS=$(count_models "$SCRIPT_DIR/brick/models")
S223_MODELS=$(count_models "$SCRIPT_DIR/s223/models")

RUN_START=$(date +%s)
TAGS_RUN=0        # tags actually benchmarked (skips excluded) — drives the ETA
BENCH_SECONDS=0   # wall time spent on those tags
declare -a DONE_TAGS=() SKIPPED_TAGS=() FAILED_TAGS=()

# hms <seconds> -> H:MM:SS
hms() { printf '%d:%02d:%02d' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# log <message…> — prefixed with total elapsed time and tag position
log() { printf '[%s] [%-7s %2d/%2d] %s\n' "$(hms $(( $(date +%s) - RUN_START )))" \
    "$tag" "$tag_idx" "$TOTAL_TAGS" "$*" >&2; }

echo "Benchmarking $TOTAL_TAGS tags (${TAG_LIST[0]} … ${TAG_LIST[-1]}): \
$BRICK_MODELS Brick + $S223_MODELS S223 models, $BENCH_ITERS iterations each" >&2

WORKTREE_BASE="$(mktemp -d -t shifty-bench-XXXXXX)"
trap 'git -C "$ROOT" worktree prune; rm -rf "$WORKTREE_BASE"' EXIT

# HEAD is benchmarked in place, so it has no worktree to discard.
drop_worktree() {
    [[ "$1" == "$HEAD_LABEL" ]] && return 0
    git -C "$ROOT" worktree remove --force "$2" 2>/dev/null || true
}

tag_idx=0
for tag in "${TAG_LIST[@]}"; do
    tag_idx=$(( tag_idx + 1 ))
    BRICK_OUT="$RESULTS_DIR/$tag/brick.txt"
    S223_OUT="$RESULTS_DIR/$tag/s223.txt"

    # A tag's results can be reused because the tag cannot move. HEAD can, so
    # its results are always regenerated rather than silently going stale.
    if [[ "$tag" != "$HEAD_LABEL" && -f "$BRICK_OUT" && -f "$S223_OUT" ]]; then
        log "already done, skipping"
        SKIPPED_TAGS+=("$tag")
        continue
    fi

    TAG_START=$(date +%s)

    if [[ "$tag" == "$HEAD_LABEL" ]]; then
        # Build the checkout as it stands, uncommitted changes included --
        # that is what "benchmark HEAD" is for. Say so, so a dirty tree is
        # never mistaken for the committed state.
        WT="$ROOT"
        if [[ -n "$(git -C "$ROOT" status --porcelain -- ':!benchmark/results')" ]]; then
            log "using current checkout at $(git -C "$ROOT" rev-parse --short HEAD) (working tree DIRTY)"
        else
            log "using current checkout at $(git -C "$ROOT" rev-parse --short HEAD) (clean)"
        fi
    else
        log "checking out…"
        WT="$WORKTREE_BASE/$tag"
        git -C "$ROOT" worktree add --quiet "$WT" "$tag"
    fi

    log "building release binary…"
    BUILD_START=$(date +%s)
    if ! cargo build --release --quiet --manifest-path "$WT/Cargo.toml" 2>&1; then
        log "build FAILED — skipping"
        FAILED_TAGS+=("$tag (build)")
        drop_worktree "$tag" "$WT"
        continue
    fi
    log "built in $(( $(date +%s) - BUILD_START ))s"

    BINARY="$WT/target/release/shifty"
    if [[ ! -x "$BINARY" ]]; then
        log "binary not found after build — skipping"
        FAILED_TAGS+=("$tag (no binary)")
        drop_worktree "$tag" "$WT"
        continue
    fi

    mkdir -p "$RESULTS_DIR/$tag"

    # Progress from the bench script arrives on stderr and passes straight
    # through to the terminal; stdout is the results table.
    log "benchmarking Brick ($BRICK_MODELS models)…"
    SUITE_START=$(date +%s)
    if BENCH_ITERS="$BENCH_ITERS" BENCH_PROGRESS=1 bash "$BENCH_SCRIPT" \
        "$SCRIPT_DIR/brick/Brick-closure.ttl" \
        "$SCRIPT_DIR/brick/models" \
        "$BINARY" > "$BRICK_OUT"; then
        log "Brick done in $(( $(date +%s) - SUITE_START ))s"
    else
        log "Brick benchmark FAILED"
        FAILED_TAGS+=("$tag (brick)")
        rm -f "$BRICK_OUT"
    fi

    log "benchmarking S223 ($S223_MODELS models)…"
    SUITE_START=$(date +%s)
    if BENCH_ITERS="$BENCH_ITERS" BENCH_PROGRESS=1 bash "$BENCH_SCRIPT" \
        "$SCRIPT_DIR/s223/223p-closure.ttl" \
        "$SCRIPT_DIR/s223/models" \
        "$BINARY" > "$S223_OUT"; then
        log "S223 done in $(( $(date +%s) - SUITE_START ))s"
    else
        log "S223 benchmark FAILED"
        FAILED_TAGS+=("$tag (s223)")
        rm -f "$S223_OUT"
    fi

    drop_worktree "$tag" "$WT"

    TAG_SECONDS=$(( $(date +%s) - TAG_START ))
    TAGS_RUN=$(( TAGS_RUN + 1 ))
    BENCH_SECONDS=$(( BENCH_SECONDS + TAG_SECONDS ))
    DONE_TAGS+=("$tag")

    # ETA assumes remaining tags cost the average of the ones already run.
    # Tags whose results already existed are excluded from both sides.
    REMAINING=0
    for t in "${TAG_LIST[@]:$tag_idx}"; do
        # HEAD is always rerun, so existing results never make it "done".
        if [[ "$t" == "$HEAD_LABEL" ]]; then
            REMAINING=$(( REMAINING + 1 ))
        elif [[ ! -f "$RESULTS_DIR/$t/brick.txt" || ! -f "$RESULTS_DIR/$t/s223.txt" ]]; then
            REMAINING=$(( REMAINING + 1 ))
        fi
    done
    if (( REMAINING > 0 )); then
        log "done in $(hms "$TAG_SECONDS") — $REMAINING tag(s) left, ETA $(hms $(( REMAINING * BENCH_SECONDS / TAGS_RUN )))"
    else
        log "done in $(hms "$TAG_SECONDS")"
    fi
done

printf 'All done in %s. %d benchmarked, %d skipped, %d failed. Results in %s\n' \
    "$(hms $(( $(date +%s) - RUN_START )))" \
    "${#DONE_TAGS[@]}" "${#SKIPPED_TAGS[@]}" "${#FAILED_TAGS[@]}" "$RESULTS_DIR" >&2
if (( ${#FAILED_TAGS[@]} > 0 )); then
    printf 'Failures: %s\n' "${FAILED_TAGS[*]}" >&2
fi
