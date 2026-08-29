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

# time_cmd <cmd...> -> elapsed milliseconds on stdout.
#
# Do the timing inside Perl so the measured interval only covers the command.
# GNU `date +%s%N` is not portable to macOS, where `%N` is printed literally.
time_cmd() {
    perl -MTime::HiRes=clock_gettime,CLOCK_MONOTONIC -e '
        my $start = clock_gettime(CLOCK_MONOTONIC);
        my $pid = fork();
        die "fork failed: $!\n" unless defined $pid;
        if ($pid == 0) {
            open STDOUT, ">", "/dev/null" or die "open /dev/null: $!\n";
            open STDERR, ">", "/dev/null" or die "open /dev/null: $!\n";
            exec @ARGV;
            exit 127;
        }
        waitpid($pid, 0);
        exit(($? >> 8) || 1) if $? != 0;
        my $elapsed = clock_gettime(CLOCK_MONOTONIC) - $start;
        printf "%.0f\n", $elapsed * 1000;
    ' -- "$@"
}

# median_spread <cmd...> -> "MEDIAN SPREAD" (two integers) on stdout.
# The median is robust to the occasional slow run (GC, scheduler, thermal);
# SPREAD is the largest one-sided deviation from the median, so "median +/-
# spread" brackets every sample.
median_spread() {
    local samples=() sorted=() n median lo hi dlo dhi spread sample
    for _ in $(seq 1 "$ITERATIONS"); do
        samples+=("$(time_cmd "$@")")
    done
    while IFS= read -r sample; do
        sorted+=("$sample")
    done < <(printf '%s\n' "${samples[@]}" | sort -n)
    n=${#sorted[@]}
    median=${sorted[$(( n / 2 ))]}          # middle sample (upper-middle if even)
    lo=${sorted[0]}; hi=${sorted[$(( n - 1 ))]}
    dlo=$(( median - lo )); dhi=$(( hi - median ))
    spread=$(( dlo > dhi ? dlo : dhi ))
    echo "$median $spread"
}

# Iterate models in a stable, locale-independent order so repeated runs (and
# the benchcmp script) line up row-for-row. Model names contain no spaces.
for model in $(printf '%s\n' "$MODELS_DIR"/*.ttl | LC_ALL=C sort); do
    name="$(basename "$model")"
    validate_args=("$BINARY" validate --shapes "$SHAPES" --data "$model")

    read -r infer_ms infer_sd <<< "$(median_spread "$BINARY" infer --shapes "$SHAPES" --data "$model")"
    read -r val_ms   val_sd   <<< "$(median_spread "${validate_args[@]}")"
    read -r rep_ms   rep_sd   <<< "$(median_spread "${validate_args[@]}" --report)"

    # shellcheck disable=SC2059
    printf "$row_fmt" "$name" "$infer_ms" "$infer_sd" "$val_ms" "$val_sd" "$rep_ms" "$rep_sd"
done
