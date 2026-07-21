#!/usr/bin/env bash
# Minimal benchmark for historical data collection.
# Only times `shifty validate`; infer and report columns are emitted as zeroes.
# Outputs a table in the same format as bench_common.sh so process_results.py
# can parse it, but infer_ms and rep_ms will be 0.
#
# Usage: bench_history.sh <SHAPES> <MODELS_DIR> <BINARY>
#
# Set BENCH_PROGRESS=1 to emit per-model progress on stderr (stdout stays a
# clean table, so callers can redirect it to a results file unchanged).

set -euo pipefail

SHAPES="$1"
MODELS_DIR="$2"
BINARY="$3"

ITERATIONS="${BENCH_ITERS:-3}"
PROGRESS="${BENCH_PROGRESS:-0}"

COL_MODEL=32
COL_MS=10
COL_SD=6

sep() { printf '%*s' "$1" '' | tr ' ' '-'; }

hdr_fmt="%-${COL_MODEL}s  %${COL_MS}s  %${COL_SD}s  %${COL_MS}s  %${COL_SD}s  %${COL_MS}s  %${COL_SD}s\n"
row_fmt="%-${COL_MODEL}s  %${COL_MS}d  %${COL_SD}d  %${COL_MS}d  %${COL_SD}d  %${COL_MS}d  %${COL_SD}d\n"

# shellcheck disable=SC2059
printf "$hdr_fmt" "model" "infer ms" "+/-" "infer+val ms" "+/-" "report ms" "+/-"
# shellcheck disable=SC2059
printf "$hdr_fmt" "$(sep $COL_MODEL)" "$(sep $COL_MS)" "$(sep $COL_SD)" "$(sep $COL_MS)" "$(sep $COL_SD)" "$(sep $COL_MS)" "$(sep $COL_SD)"

time_cmd() {
    local start end
    start=$(date +%s%N)
    "$@" > /dev/null 2>&1
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

# median_spread <cmd...> -> "MEDIAN SPREAD" (two integers) on stdout.
# The median is robust to the occasional slow run (GC, scheduler, thermal);
# SPREAD is the largest one-sided deviation from the median, so "median +/-
# spread" brackets every sample.
median_spread() {
    local samples=() t sorted n median lo hi dlo dhi spread
    for _ in $(seq 1 "$ITERATIONS"); do
        samples+=("$(time_cmd "$@")")
    done
    mapfile -t sorted < <(printf '%s\n' "${samples[@]}" | sort -n)
    n=${#sorted[@]}
    median=${sorted[$(( n / 2 ))]}          # middle sample (upper-middle if even)
    lo=${sorted[0]}; hi=${sorted[$(( n - 1 ))]}
    dlo=$(( median - lo )); dhi=$(( hi - median ))
    spread=$(( dlo > dhi ? dlo : dhi ))
    echo "$median $spread"
}

# Progress lines go to stderr so stdout stays a parseable table. Each model
# prints its label before timing starts and its result after, so a slow model
# is visible while it is still running rather than only once it finishes.
progress_start() { [[ "$PROGRESS" == 1 ]] && printf '      [%2d/%2d] %-34s' "$1" "$2" "$3" >&2; return 0; }
progress_end()   { [[ "$PROGRESS" == 1 ]] && printf '%7d ms +/- %-6d (%ds elapsed)\n' "$1" "$2" "$3" >&2; return 0; }

mapfile -t MODELS < <(printf '%s\n' "$MODELS_DIR"/*.ttl | LC_ALL=C sort)
TOTAL=${#MODELS[@]}
idx=0

for model in "${MODELS[@]}"; do
    name="$(basename "$model")"
    idx=$(( idx + 1 ))
    progress_start "$idx" "$TOTAL" "$name"
    started=$(date +%s)
    read -r val_ms val_sd <<< "$(median_spread "$BINARY" validate --shapes "$SHAPES" --data "$model")"
    progress_end "$val_ms" "$val_sd" "$(( $(date +%s) - started ))"
    # shellcheck disable=SC2059
    printf "$row_fmt" "$name" 0 0 "$val_ms" "$val_sd" 0 0
done
