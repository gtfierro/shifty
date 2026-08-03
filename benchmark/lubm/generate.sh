#!/usr/bin/env bash
# Build the LUBM suite: fetch the univ-bench ontology and the UBA data
# generator, generate N universities, and convert everything to Turtle.
#
#   ./benchmark/lubm/generate.sh            # 5 universities (~600k triples)
#   ./benchmark/lubm/generate.sh 25         # 25 universities
#   LUBM_SEED=7 ./benchmark/lubm/generate.sh 10
#
# Produces, under benchmark/lubm/:
#   univ-bench.ttl        the ontology
#   lubm-closure.ttl      ontology + shapes.ttl, the --shapes argument
#   models/univN.ttl      one model per university, the benchmark corpus
#
# Needs: java (the UBA generator is a Java tool), curl, unzip, and uv (for the
# RDF/XML -> Turtle conversion). Nothing here is vendored: LUBM is distributed
# by Lehigh, and its licence is theirs to state.

set -euo pipefail

UNIVERSITIES="${1:-5}"
SEED="${LUBM_SEED:-0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$SCRIPT_DIR/.uba"
MODELS="$SCRIPT_DIR/models"

UBA_URL="http://swat.cse.lehigh.edu/projects/lubm/uba1.7.zip"
ONTOLOGY_URL="http://swat.cse.lehigh.edu/onto/univ-bench.owl"

need() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "error: $1 is required but not installed" >&2
        exit 1
    }
}
need java
need curl
need uv

mkdir -p "$WORK" "$MODELS"

# ---- 1. ontology --------------------------------------------------------
if [ ! -f "$WORK/univ-bench.owl" ]; then
    echo "fetching univ-bench ontology…" >&2
    curl -fsSL "$ONTOLOGY_URL" -o "$WORK/univ-bench.owl"
fi

# ---- 2. generator -------------------------------------------------------
if [ ! -f "$WORK/uba/Generator.class" ] && [ ! -f "$WORK/uba1.7.zip" ]; then
    echo "fetching UBA generator…" >&2
    curl -fsSL "$UBA_URL" -o "$WORK/uba1.7.zip"
    (cd "$WORK" && unzip -oq uba1.7.zip)
fi

# ---- 3. generate --------------------------------------------------------
# UBA writes University0_0.owl, University0_1.owl, … into the working directory,
# one file per department, so the per-university models are re-assembled below.
GENERATED="$WORK/generated"
if [ ! -d "$GENERATED" ] || [ -z "$(ls -A "$GENERATED" 2>/dev/null)" ]; then
    echo "generating $UNIVERSITIES universities (seed $SEED)…" >&2
    mkdir -p "$GENERATED"
    (
        cd "$GENERATED"
        java -cp "$WORK:$WORK/classes:$WORK/src" edu.lehigh.swat.bench.uba.Generator \
            -univ "$UNIVERSITIES" \
            -index 0 \
            -seed "$SEED" \
            -onto "http://swat.cse.lehigh.edu/onto/univ-bench.owl"
    )
fi

# ---- 4. convert ---------------------------------------------------------
# UBA emits RDF/XML; the benchmark harness reads Turtle. Departments are merged
# per university so one model equals one university.
echo "converting to Turtle…" >&2
uv run --quiet --with rdflib - "$GENERATED" "$MODELS" "$WORK" <<'PYTHON'
import sys
from collections import defaultdict
from pathlib import Path

from rdflib import Graph

generated, models, work = (Path(argument) for argument in sys.argv[1:4])

by_university = defaultdict(list)
for source in sorted(generated.glob("University*.owl")):
    # University<N>_<dept>.owl
    by_university[source.stem.split("_")[0]].append(source)

for university, sources in sorted(by_university.items()):
    graph = Graph()
    for source in sources:
        graph.parse(source, format="xml")
    target = models / f"{university.lower()}.ttl"
    graph.serialize(target, format="turtle")
    print(f"  {target.name}: {len(graph)} triples", file=sys.stderr)

ontology = Graph()
ontology.parse(work / "univ-bench.owl", format="xml")
ontology.serialize(work / "univ-bench.ttl", format="turtle")
PYTHON

cp "$WORK/univ-bench.ttl" "$SCRIPT_DIR/univ-bench.ttl"

# ---- 5. closure ---------------------------------------------------------
# The harness passes one --shapes file, and `sh:class` needs the ontology's
# subClassOf edges in the same graph to resolve through the hierarchy.
{
    cat "$SCRIPT_DIR/univ-bench.ttl"
    echo
    cat "$SCRIPT_DIR/shapes.ttl"
} > "$SCRIPT_DIR/lubm-closure.ttl"

echo >&2
echo "wrote $(ls "$MODELS" | wc -l | tr -d ' ') model(s) to $MODELS" >&2
echo "run: ./benchmark/bench_evidence.sh lubm > lubm.csv" >&2
