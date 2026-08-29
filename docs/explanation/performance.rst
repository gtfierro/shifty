What evidence costs
===================

Evidence tracing is the part of Shifty that costs something. Deciding
conformance answers one bit per ``(statement, focus)`` pair and may stop at the
first thing that settles it; materializing evidence must build the whole
derivation and keep it.

This page records what that costs, which entry point to reach for, and —
because several of the answers are counter-intuitive — how each optimization
was measured and what was learned when a hypothesis failed.

Choosing an entry point
-----------------------

Four entry points share one prepared snapshot, so parsing, inference,
normalization, stratification, indexing, and SPARQL preparation are paid once:

.. code-block:: text

   validate_conformance()   counts only, no evidence
   find_failures()          counts + which pairs failed
   explain(pair)            evidence for one pair
   validate()               evidence for every selected pair

Most callers want to know why something *failed*, and failures are a small
minority of pairs — 8,047 of 286,705 across the Brick corpus. For them,
``find_failures`` followed by ``explain`` on each result costs far less than
``validate``:

.. list-table::
   :header-rows: 1

   * - Model
     - Failing / selected
     - find + explain
     - vs. ``validate``
     - vs. conformance
   * - ``bldg1.ttl``
     - 48 / 2,800
     - 273 ms
     - 0.42x
     - 1.03x
   * - ``bldg11.ttl``
     - 1,051 / 32,813
     - 1,444 ms
     - 0.25x
     - 1.34x

So explaining every failure costs 3–34% over deciding conformance, against
2.5–5.4x for explaining everything. ``explain`` returns exactly what
``validate`` would have produced for that pair — same evidence, same authored
fan-out — which the test suite pins with an exact comparison.

Reach for ``validate`` when satisfaction evidence for *passing* pairs is
actually wanted; that is the case ``explain`` cannot make cheaper, because
there is no small subset to restrict to.

``explain`` does not re-run target selection: it takes a pair as already
selected, since re-deriving the selection costs what the whole pass costs.
Pairs should come from ``find_failures`` or an earlier run. The constraint
catalog is fixed per snapshot rather than per pair, so it comes separately from
``constraints()`` — on a small 223P model the catalog is 57% of a whole run's
serialized bytes, which would swamp a single pair.

These four are methods on the Rust ``PreparedEvidenceValidator``. The Python
bindings currently expose the whole-run ``validate`` only, so the cheap failure
path is a Rust-level API today.

Where the cost goes
-------------------

``probe_evidence_cost`` attributes one model rather than timing a corpus:

.. code-block:: sh

   cargo run --release -p shifty-engine --example probe_evidence_cost -- \
       --shapes benchmark/brick/Brick-closure.ttl \
       --data benchmark/brick/models/bldg1.ttl

Two numbers explain most of what it reports.

**Evidence re-derives what conformance already decided.** On ``bldg1`` the
evidence pass makes 117,360 visits to retain 24,103 nodes, and the distinct
``(shape, focus)`` conclusions behind them number only 23,172 — a factor of
5.1. On ``bldg11`` it is 1,445,102 visits against 366,554 distinct conclusions,
a factor of 3.9. During that same pass the conformance memo is *hitting* 75–82%
of the time: the engine already knows the answer for these pairs and
materializes the derivation again anyway.

**The cost concentrates in a few shapes, and grows per focus.**
``Brick#Tag`` alone is 52% of materialization on ``bldg1`` and 31% on
``bldg11``, at 900 and 5,634 visits per focus node respectively. Per-focus work
grows with total model size, which is why overhead is not a constant tax: it
rises from roughly 2.5x on small models to 5.4x on large ones.

Serialized size
---------------

A run is large for three measured reasons, which dominate on different corpora.
``compact`` addresses all three losslessly; ``sharing()`` reports the first two
for any run.

.. list-table::
   :header-rows: 1

   * - Cause
     - Brick (median)
     - 223P (median)
   * - Repeated RDF terms
     - 498x
     - 68x
   * - Repeated tagged subtrees
     - 5.41x
     - 1.78x
   * - The constraint catalog
     - Fixed per run regardless of findings
     - 57% of a small 223P run

Terms, not subtrees, are the dominant lever on both corpora. How much either is
worth, though, is a property of the corpus and not of the encoding: Brick models
restate a small vocabulary across many similar assets, while 223P models are
more varied, and the gap between 498x and 68x follows from that rather than from
anything the encoder does. It shows up in the result — the compact encoding is
76% smaller on Brick and 50% smaller on 223P, where the fixed catalog is a much
larger share of a smaller run.

Concretely, on ``bldg1.ttl``: 243,249 term occurrences of 548 distinct terms,
105,673 evidence-node occurrences of 19,765 distinct nodes, taking the run from
33.1 MB to 9.5 MB with the catalog and 7.3 MB without.

Measure sharing with ``shifty_engine::sharing()`` rather than by comparing
table sizes to node counts. The tables an encoding writes also hold catalog
entries that no evidence occurrence refers to, so comparing across those
denominators is not measuring the same thing on both sides. ``sharing()`` counts
inside the interner, against the same predicates the encoder interns by, so a
quoted ratio cannot drift from what compaction collapses.

Which tagged node is being counted
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The subtree ratio above is over *all* tagged nodes, and that is the right
denominator for the encoding and the wrong one for anything else. Three enums
serialize as ``{"type", "details"}``: ``Witness`` and ``SatTrace``, which are
validation judgments, and ``PathSupport``, which is a certificate recording how
a value was reached. The encoder interns all three. Split apart over the corpus:

.. list-table::
   :header-rows: 1

   * - Suite
     - Judgments
     - Path support
     - Support share of occurrences
   * - Brick
     - 1.31x
     - 66.2x
     - 78.7%
   * - 223P
     - 1.31x
     - 5.73x
     - 39.1%

**Validation judgments barely repeat at all, and repeat equally on both
corpora.** Every bit of the 5.41x/1.78x contrast was path support — how many
certificates the evidence carries, and how heavily those certificates repeat.
Reading the combined ratio as "how much do validation results repeat" is not
approximately right; it is measuring a different thing that happens to dominate
the sum.

``sharing()`` splits them: ``result_occurrences``/``distinct_results`` for
judgments, ``support_occurrences``/``distinct_support`` for certificates, and
``support_share`` for the mixture. The partition is exact by construction — a
tagged node is a judgment exactly when its payload carries a ``shape``, which
every ``Witness`` and ``SatTrace`` variant does and no ``PathSupport`` does — and
``result_occurrences`` is checked against ``run.walk()``, an independent
traversal written against the types.

Sharing across independently addressable results
------------------------------------------------

``shifty_engine::result_sharing()`` answers the question the byte-level ratios
cannot: how much of the derivation is reached from more than one result a caller
can ask for. It walks the typed evidence and never serializes, so its judgment
counts are an independent check on the encoder's — ``bench_evidence`` asserts
they agree on every model.

**The addressable unit is the normalized request.** A run reports one record per
*authored* ``(statement, focus)`` pair, because a report has to name the
statement its reader wrote. Two authored statements that normalize together are
one request, evaluated once; reporting it twice is duplication, not sharing.
Counting sharing across authored records would therefore book source
traceability as a saving. ``result_sharing`` counts across normalized requests
and reports ``duplicate_records`` separately, so the cost of preserving
authorship is visible rather than folded in. ``divergent_duplicates`` — records
that answer one request with unequal evidence — must be zero, and is checked.

**The interior address is** ``(constraint, node, polarity)``: the shape memo's
``(ShapeId, Term)`` key plus which way it came out. *Constraint* and *node*
rather than *shape* and *focus*, because ``focus`` names the top-level selected
node and interior judgments are usually about some other node reached from it.

**A key addresses evidence only if it determines evidence.** It may not:
``Witness::Atom`` carries ``reached_by`` and ``produced_by``, which describe the
derivation rather than the judgment, so one key can occur with several payloads.
``divergent_keys`` over ``multi_occurrence_keys`` measures how often it does.
This is the number that decides whether evidence construction can be memoized on
the judgment the way conformance already is — a boolean memo key is sufficient
for evidence only where divergence is negligible.

What the corpus says
~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * -
     - Brick
     - 223P
   * - Requests behind authored records
     - 286,705 of 401,457 (+40.0% duplication, 0 divergent)
     - 204,733 of 282,438 (+38.0%, 0 divergent)
   * - Judgments reached from 2+ requests
     - 1.0%, covering 6.2% of occurrences
     - 4.0%, covering 20.7%
   * - Requests per distinct judgment
     - 1.06
     - 1.17
   * - Repeated keys carrying 2+ payloads
     - 10.7%, covering 41.9% of occurrences
     - 16.3%, covering 9.6%
   * - Bracket (lossless → key-addressed)
     - 1.31x → 2.13x
     - 1.31x → 1.43x

**Sharing across addressable results is close to absent, and what looks like
sharing is one constant.** The most widely shared payload on every model
measured is ``SatTrace::Irrefutable`` — 44 bytes of ``⊤`` — reached from 8,856
requests on ``bldg12`` and 15,874 on ``nrel-example``. The *next* most shared
payload on ``bldg12`` is reached from two. Shared payloads there are 1.0% of
distinct judgments and 0.1% of evidence bytes. 223P has a genuine tail — several
multi-kilobyte subtrees reached from 13 requests each, 5.9% of bytes — but
nothing that would carry a claim about structure sharing between results.

So the compact encoding's 80% size reduction is not a cross-result sharing
result. It comes from repeated terms and repeated path certificates *within*
results. It is a serialization technique, and belongs in the writeup as one.

**Evidence is not a function of the judgment.** On Brick only 10.7% of repeated
keys carry more than one payload, but those keys account for 41.9% of all
judgment occurrences — divergence concentrates in exactly the keys that repeat
most. A ``(constraint, node) -> bool`` memo is therefore sufficient for
conformance and *insufficient* for evidence: the same judgment, reached along a
different path, legitimately records different ``reached_by``/``produced_by``.
Caching evidence on the conformance memo's key would return the wrong
derivation, not merely a differently-formatted one.

The bracket makes the size of the loss concrete: 1.31x is collapsed losslessly
today, 2.13x would be collapsible by a judgment-keyed memo on Brick, and the gap
is unreachable without either widening the key to include derivation context or
accepting that evidence is context-free when it is not.

Quote ``result_redundancy`` as achieved. Quote ``key_redundancy`` only next to
``divergence_fraction``.

The optimizations
-----------------

In order, with what each was worth. Two of the four began as a hypothesis that
measurement contradicted; those are recorded as such, because the reasoning
that produced them is easy to repeat.

Ordering path values (reproducibility)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``PathBackend`` yields ``HashSet``s, whose iteration order varies between
instances, and evidence was built straight from one. Two ``validate`` calls
over a single prepared snapshot therefore disagreed — and because ``CountHigh``
names the values past ``max`` as excess, they blamed *different values* for the
same violation. Not a cosmetic ordering difference, and not reproducible: no
artifact derived from a run could be regenerated.

Fixed by ordering at the two points where path values enter evidence
(``succ_with_support``, which every ``Count`` goes through, and the object scan
in ``closed_offenders``) under one total order on terms. RDF defines no order
across term kinds, so ``compare_terms`` fixes an arbitrary but stable one.

Conformance never calls either path — counts and satisfaction are
order-independent — which is why this stayed invisible until evidence began
being serialized. Cost is below noise (``bldg11``: 5,894 ms to 5,878 ms), and
compaction got about 4% *better*, since value sets differing only in order were
distinct JSON and could not be hash-consed together. Determinism and sharing
turn out to be the same property.

Interning without allocating per occurrence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compaction cost more than the validation it encoded — 17.9 s against 5.9 s on
``bldg11`` — which puts it out of reach of any caller wanting it inline. Two
allocations per tree position were responsible: the interner built its hash key
with ``value.to_string()``, serializing every occurrence into a fresh ``String``
and dropping it on a hit (roughly six million allocations for 371k distinct
entries), and the interning walk collected each object and array into a *new*
container. Now it hashes structurally and rewrites children in place: 686 ms to
323 ms on ``bldg1``, 17.9 s to 9.8 s on ``bldg11``.

*The estimate was 5–8x and the result was under 2x.* The probe now reports the
split, which shows why: on ``bldg11``, 2.4 s building the intermediate
``serde_json::Value`` + 7.0 s interning + 1.6 s emitting. What remains is
structural rather than wasteful, and removing it means interning *during*
serialization instead of after — a different change, not a tuning pass.

Not to be confused with the arena compaction of the normalizer, which rebuilds
the shape arena and is unrelated.

The shape memo's key
~~~~~~~~~~~~~~~~~~~~

``holds_memoized`` built a ``(ShapeId, Term)`` tuple before looking the memo up,
so every probe allocated a ``Term`` — including the 75–82% that hit and threw it
away. Keying by shape first lets a hit borrow the term instead; the maps also
hash with ``Fx`` rather than SipHash, their keys being shape ids and terms from
an already-parsed graph rather than adversarial input.

Worth 3–5% of evidence time and 4–15% of conformance time. *This began as the
hypothesis that key handling was the bottleneck, and it is not.* The reasoning
error is worth recording: ``probe_evidence_cost`` reports
``per-memo-lookup cost`` and ``per-visit cost``, and reading them as marginal
costs suggests a cache probe costs about what a visit costs. Both are
``evidence_ms / count`` — the same wall clock over two different denominators —
so they cannot be compared to each other, and neither measures hashing.
Interning terms to integers is therefore *not* a prerequisite for anything
else; the 3.9–5.1x gap between visits and distinct conclusions is real
re-derivation, and closing it needs a cache of evidence values, not a cheaper
probe.

A consequence for measurement: conformance gained more than evidence did, which
*raises* the reported evidence-overhead ratio even though both arms got faster.
The ratio is sensitive to its denominator, so absolute times belong beside it in
anything reported.

Explaining one pair at a time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Described under `Choosing an entry point`_ above. The conformance pass is
generic over an observer so that ``validate_conformance`` still costs exactly
what it did — it is the baseline the overhead benchmark divides by, and
inflating it would flatter every ratio measured against it.

What is not done
----------------

**Materializing a DAG rather than a tree.** The 3.9–5.1x gap between visits and
distinct ``(shape, focus)`` conclusions is the largest remaining runtime cost,
and stratification would make sharing sound by guaranteeing the derivation
relation is acyclic.

*The obvious version of this does not work, and* ``result_sharing`` *is what
established that.* The plan was to extend the conformance memo's value from
``bool`` to an evidence handle, on the reasoning that the memo hitting on a pair
proves the evidence for it is shareable. It does not: on Brick, keys carrying
more than one payload account for 41.9% of judgment occurrences. The memo's key
determines the *answer* and not the *derivation*, so an evidence handle stored
under it would hand back a witness that reached the node another way. Any real
attempt has to widen the key with whatever ``reached_by``/``produced_by``
distinguishes the payloads, and that is a different and larger change — and
worth substantially less, since the collapse it could reach is 2.13x rather than
the 3.9–5.1x the visit counts suggest.

**Interning during serialization.** Compaction still hands itself an
intermediate ``serde_json::Value``, which is 2.4 s of the 9.8 s on ``bldg11``.

**Peak memory.** ``bldg11`` (35,567 triples) peaks at about 8.5 GB. Both items
above bear on it; ``explain`` avoids it entirely for the failure-inspection
case.

**A compression baseline.** The compact encoding has never been compared
against ``gzip``/``zstd`` on the full run. Until it is, the honest claim for it
is random-accessible structure sharing that stays JSON, not compression.

Reproducing
-----------

Corpus-wide timings, including the sharing and size columns:

.. code-block:: sh

   ./benchmark/bench_evidence.sh > results.csv
   uv run benchmark/summarize_evidence.py results.csv --per-model

Single-model attribution, including the on-demand comparison and the compaction
split:

.. code-block:: sh

   cargo run --release -p shifty-engine --example probe_evidence_cost -- \
       --shapes benchmark/brick/Brick-closure.ttl \
       --data benchmark/brick/models/bldg11.ttl

The two are not interchangeable.
``bench_evidence`` reports the median of ten rotated, paired rounds over a warmed snapshot and retains the raw samples in its CSV.
``probe_evidence_cost`` reports one cold pass.
Absolute times differ substantially between them, so a ratio from one should never be compared against a ratio from the other.
Figures quoted on this page come from the probe unless stated otherwise.
