#!/bin/bash

# Benchmark inference performance on production-like building models
# These are real building models from LBNL, NREL, NIST, and PNNL

set -e

# Use persistent onto-env environment (not temporary)
export TEMPORARY_ONTOENV=false

BINARY="./target/release/shifty"

echo "Building release binary..."
echo "Using persistent onto-env environment (TEMPORARY_ONTOENV=false)"
cargo build --release

echo ""
echo "=========================================================================="
echo "Inference Benchmarks - Production Building Models"
echo "=========================================================================="
echo ""

# S223 Benchmarks - Real building models
echo "===================================="
echo "S223: LBNL Building 3 (823KB model)"
echo "===================================="
hyperfine --warmup 1 --runs 5 \
  "$BINARY validate --shapes-file ttl/223p.ttl --data-file benchmark/s223/models/lbnl-bdg3-1.ttl --inference"

echo ""
echo "===================================="
echo "S223: PNNL Building 2 (2.6MB model)"
echo "===================================="
hyperfine --warmup 1 --runs 5 \
  "$BINARY validate --shapes-file ttl/223p.ttl --data-file benchmark/s223/models/pnnl-bdg2-1.ttl --inference"

echo ""
echo "===================================="
echo "S223: NREL Example (314KB model)"
echo "===================================="
hyperfine --warmup 1 --runs 5 \
  "$BINARY validate --shapes-file ttl/223p.ttl --data-file benchmark/s223/models/nrel-example.ttl --inference"

echo ""
# Brick Benchmarks - Real building models
echo "===================================="
echo "Brick: Building 11 (511KB model)"
echo "===================================="
hyperfine --warmup 1 --runs 5 \
  "$BINARY validate --shapes-file benchmark/brick/Brick.ttl --data-file benchmark/brick/models/bldg11.ttl --inference"

echo ""
echo "===================================="
echo "Brick: Building 15 (356KB model)"
echo "===================================="
hyperfine --warmup 1 --runs 5 \
  "$BINARY validate --shapes-file benchmark/brick/Brick.ttl --data-file benchmark/brick/models/bldg15.ttl --inference"

echo ""
echo "===================================="
echo "Brick: Building 18 (217KB model)"
echo "===================================="
hyperfine --warmup 1 --runs 5 \
  "$BINARY validate --shapes-file benchmark/brick/Brick.ttl --data-file benchmark/brick/models/bldg18.ttl --inference"

echo ""
echo "=========================================================================="
echo "Benchmark complete!"
echo "=========================================================================="
echo ""
echo "Note: These are real building models from:"
echo "  - LBNL (Lawrence Berkeley National Laboratory)"
echo "  - PNNL (Pacific Northwest National Laboratory)"
echo "  - NREL (National Renewable Energy Laboratory)"
echo "  - NIST (National Institute of Standards and Technology)"
echo ""
