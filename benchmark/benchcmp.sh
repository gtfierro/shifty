#!/usr/bin/env bash
# Compare two outputs of bench_brick.sh / bench_s223.sh, benchcmp-style.
#
# Usage:
#   ./benchmark/bench_brick.sh > old.txt
#   # …change something, rebuild…
#   ./benchmark/bench_brick.sh > new.txt
#   ./benchmark/benchcmp.sh old.txt new.txt
#
# For each timed column (infer, infer+val, report) it prints a per-model table
# of old ms, new ms, and the percentage delta (new relative to old; negative is
# faster), followed by a geometric-mean delta across all models. Rows are
# matched by model name, so the two files do not have to list models in the
# same order — though the bench scripts now emit them sorted, so they do.

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "usage: $(basename "$0") <old.txt> <new.txt>" >&2
    exit 2
fi

OLD="$1"
NEW="$2"
for f in "$OLD" "$NEW"; do
    [ -f "$f" ] || { echo "error: no such file: $f" >&2; exit 1; }
done

awk -v old_name="$OLD" -v new_name="$NEW" '
# A data row is any line whose first field is a model file name (ends in .ttl).
# Columns: model infer infer_sd infer+val infer+val_sd report report_sd
FNR == NR {
    if ($1 ~ /\.ttl$/) {
        oi[$1] = $2; ov[$1] = $4; orp[$1] = $6
        if (!($1 in seen_old)) { seen_old[$1] = 1; order[++count] = $1 }
    }
    next
}
{
    if ($1 ~ /\.ttl$/) { ni[$1] = $2; nv[$1] = $4; nrp[$1] = $6; in_new[$1] = 1 }
}

function dashes(k,   s) { s = ""; while (length(s) < k) s = s "-"; return s }

# new relative to old, as a signed percentage string. Negative == faster.
function delta(o, n) {
    if (o == 0) return (n == 0 ? "0.00%" : "n/a")
    return sprintf("%+.2f%%", (n - o) / o * 100)
}

function section(label, o, n,   i, m, gm_sum, gm_n, gm) {
    printf "== %s ==\n", label
    printf "%-*s  %10s  %10s  %10s\n", MW, "model", "old ms", "new ms", "delta"
    printf "%-*s  %10s  %10s  %10s\n", MW, dashes(MW), dashes(10), dashes(10), dashes(10)
    gm_sum = 0; gm_n = 0
    for (i = 1; i <= count; i++) {
        m = order[i]
        if (!(m in in_new)) continue
        printf "%-*s  %10d  %10d  %10s\n", MW, m, o[m], n[m], delta(o[m], n[m])
        if (o[m] > 0 && n[m] > 0) { gm_sum += log(n[m] / o[m]); gm_n++ }
    }
    if (gm_n > 0) {
        gm = exp(gm_sum / gm_n)
        printf "%-*s  %10s  %10s  %10s\n", MW, "[geomean]", "", "", sprintf("%+.2f%%", (gm - 1) * 100)
    }
    print ""
}

END {
    MW = 32
    printf "old: %s\n", old_name
    printf "new: %s\n\n", new_name

    section("infer",    oi,  ni)
    section("infer+val", ov,  nv)
    section("report",   orp, nrp)

    # Note any models that did not appear in both files.
    for (i = 1; i <= count; i++) if (!(order[i] in in_new)) printf "only in old: %s\n", order[i] > "/dev/stderr"
    for (m in in_new) if (!(m in seen_old))                 printf "only in new: %s\n", m            > "/dev/stderr"
}
' "$OLD" "$NEW"
