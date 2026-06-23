#!/usr/bin/env bash
# Shared benchmark core: infer + validate + validate --report for every .ttl
# in MODELS_DIR. Called by bench_brick.sh and bench_s223.sh.
#
# Usage: bench_common.sh <SHAPES> <MODELS_DIR> <BINARY>

set -euo pipefail

SHAPES="$1"
MODELS_DIR="$2"
BINARY="$3"

ITERATIONS="${BENCH_ITERS:-3}"

COL_MODEL=32
COL_MS=10
COL_SD=6

sep() { printf '%*s' "$1" '' | tr ' ' '-'; }

hdr_fmt="%-${COL_MODEL}s  %${COL_MS}s  %${COL_SD}s  %${COL_MS}s  %${COL_SD}s  %${COL_MS}s  %${COL_SD}s\n"
row_fmt="%-${COL_MODEL}s  %${COL_MS}d  %${COL_SD}d  %${COL_MS}d  %${COL_SD}d  %${COL_MS}d  %${COL_SD}d\n"

# Header. The "report" column times `validate --report`, i.e. the full
# infer + validate + W3C ValidationReport (N-Triples) pipeline.
# shellcheck disable=SC2059
printf "$hdr_fmt" "model" "infer ms" "+/-" "infer+val ms" "+/-" "report ms" "+/-"
# shellcheck disable=SC2059
printf "$hdr_fmt" "$(sep $COL_MODEL)" "$(sep $COL_MS)" "$(sep $COL_SD)" "$(sep $COL_MS)" "$(sep $COL_SD)" "$(sep $COL_MS)" "$(sep $COL_SD)"

# time_cmd <cmd...> -> elapsed milliseconds on stdout
time_cmd() {
    local start end
    start=$(date +%s%N)
    "$@" > /dev/null 2>&1
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

# mean_stddev <cmd...> -> "MEAN STDDEV" (two integers) on stdout
mean_stddev() {
    local samples=() t i total=0 sum_sq=0 diff mean stddev
    for i in $(seq 1 "$ITERATIONS"); do
        t=$(time_cmd "$@")
        samples+=("$t")
        total=$(( total + t ))
    done
    mean=$(( total / ITERATIONS ))
    for t in "${samples[@]}"; do
        diff=$(( t - mean ))
        sum_sq=$(( sum_sq + diff * diff ))
    done
    stddev=$(awk -v ss="$sum_sq" -v n="$ITERATIONS" 'BEGIN { printf "%d", int(sqrt(ss/n)+0.5) }')
    echo "$mean $stddev"
}

# Iterate models in a stable, locale-independent order so repeated runs (and
# the benchcmp script) line up row-for-row. Model names contain no spaces.
for model in $(printf '%s\n' "$MODELS_DIR"/*.ttl | LC_ALL=C sort); do
    name="$(basename "$model")"
    validate_args=("$BINARY" validate --shapes "$SHAPES" --data "$model")

    read -r infer_ms infer_sd <<< "$(mean_stddev "$BINARY" infer --shapes "$SHAPES" --data "$model")"
    read -r val_ms   val_sd   <<< "$(mean_stddev "${validate_args[@]}")"
    read -r rep_ms   rep_sd   <<< "$(mean_stddev "${validate_args[@]}" --report)"

    # shellcheck disable=SC2059
    printf "$row_fmt" "$name" "$infer_ms" "$infer_sd" "$val_ms" "$val_sd" "$rep_ms" "$rep_sd"
done
