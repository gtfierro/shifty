# LUBM suite

The Brick and 223P corpora are real buildings at fixed sizes. LUBM adds the axis
they cannot: a *generator*, so evidence cost can be measured against data size
on demand rather than against whatever models exist.

LUBM ships an OWL ontology and the UBA data generator; its own workload is a set
of SPARQL queries, not shapes. `shapes.ttl` here supplies the SHACL side.

## Building the suite

```sh
./benchmark/lubm/generate.sh        # 5 universities, ~600k triples
./benchmark/lubm/generate.sh 25     # scale up
./benchmark/bench_evidence.sh lubm > lubm.csv
```

Needs `java` (UBA is a Java tool), `curl`, `unzip`, and `uv`. Nothing is
vendored here — the generator and ontology are downloaded from Lehigh on first
run, since their distribution terms are Lehigh's to state. Generated data and
the fetched tools land in `models/` and `.uba/`, both of which should stay out
of version control.

`LUBM_SEED` fixes the generator's seed (default 0), so a corpus is reproducible
across machines.

## What the shapes cover

They were chosen so the suite exercises the constructs whose *evidence differs
in kind*, not merely to be plausible constraints:

| Construct | Evidence it produces |
|---|---|
| `sh:class` through the taxonomy | traverses `type · subClassOf*` — the path shape that dominated Brick |
| `sh:minCount` | `CountLow`, partitioned into qualifying matches and rejected candidates |
| `sh:maxCount` | `CountHigh`, carrying the excess values |
| `sh:qualifiedMinCount` | qualified partition over candidate values |
| `sh:datatype`, `sh:nodeKind` | `Atom` leaves |
| `sh:or` | held branches on satisfaction, every branch on failure |
| `sh:not` | crosses to the opposite polarity |
| path with no cardinality | `ForAllHeld` — vacuously true on an empty path |

Generated LUBM data conforms to the ontology but deliberately not to all of
these: UBA omits `ub:emailAddress` and `ub:telephone` on some staff, leaves some
publications authorless, and grants undergraduate degrees from universities it
never describes. A corpus where everything passes would exercise only the
satisfaction half of the evidence interface.

## Scaling study

Because university count is a parameter, the suite answers a question the fixed
corpora cannot — whether evidence overhead is constant in data size:

```sh
for n in 1 2 5 10 20; do
    ./benchmark/lubm/generate.sh "$n"
    ./benchmark/bench_evidence.sh lubm | tail -n +2 >> scaling.csv
done
```

On the Brick and 223P corpora overhead is *not* constant — it rises from ~1.4x
on the smallest models to ~5x on the largest — so a controlled sweep is worth
having.
