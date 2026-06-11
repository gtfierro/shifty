#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v cargo-flamegraph >/dev/null 2>&1; then
  echo "error: cargo-flamegraph is not installed; run: cargo install flamegraph" >&2
  exit 1
fi

if ! command -v perf >/dev/null 2>&1; then
  echo "error: perf is not installed" >&2
  exit 1
fi

output="${FLAMEGRAPH_OUTPUT:-flamegraph.svg}"
frequency="${FLAMEGRAPH_FREQUENCY:-99}"
stack_size="${FLAMEGRAPH_STACK_SIZE:-16384}"

if [[ $# -eq 0 ]]; then
  cli_args=(
    validate
    --shapes "$repo_root/benchmark/brick/Brick-closure.ttl"
    --data "$repo_root/benchmark/brick/models/bldg1.ttl"
  )
else
  cli_args=("$@")
fi

mkdir -p "$(dirname "$output")"

cargo flamegraph \
  --package shifty-cli \
  --bin shacl \
  --cmd "record -e cpu-clock -F $frequency --call-graph dwarf,$stack_size -g" \
  --no-inline \
  --deterministic \
  --title "shacl ${cli_args[*]}" \
  --output "$output" \
  -- "${cli_args[@]}"
