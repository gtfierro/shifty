//! Compact, lossless encoding of an [`EvidenceRun`](crate::EvidenceRun).
//!
//! Full evidence is large for three measured reasons, and they dominate on
//! different corpora:
//!
//!   * **Repeated terms.** The same IRIs recur at every mention. On
//!     `brick/models/bldg1.ttl` (2,650 triples) 243,249 term occurrences are
//!     548 distinct terms — a 444× redundancy, and the single largest lever.
//!     It grows with the corpus: `bldg11.ttl` reaches 998×.
//!   * **Repeated subtrees.** The same `(constraint, node)` conclusion is
//!     reached through many parents, and each occurrence is written out in
//!     full. On the same model 105,673 evidence-node occurrences are 19,765
//!     distinct nodes — 5.3×, and roughly constant across the Brick corpus.
//!   * **The constraint catalog.** Both arenas are dumped on every run
//!     regardless of how many findings there are. On a small 223P model
//!     (`guideline36-2021-A-1.ttl`, 146 triples) that fixed cost is 2.02 MB of
//!     a 3.52 MB run — 57%.
//!
//! Together, on `bldg1.ttl`, a 33.1 MB run encodes to 9.8 MB with its catalog
//! and 7.6 MB without. [`sharing`] reports the two redundancy factors for a run
//! directly, measured against the very predicates the encoder interns by, so a
//! quoted ratio cannot drift from what compaction actually collapses.
//!
//! This encoding removes both without losing anything: evidence nodes and RDF
//! terms are hash-consed into tables and referenced by index, and the catalog
//! is optional for callers that already hold the schema. [`expand`] reconstructs
//! the original run exactly.
//!
//! The encoding is structural rather than typed: it interns any tagged
//! `{"type", "details"}` object and any RDF term, so it follows the evidence
//! vocabulary automatically instead of mirroring every variant. That keeps one
//! definition of the evidence types and gives the Python bindings the same
//! format for free.

use crate::witness::EvidenceRun;
use rustc_hash::{FxHashMap, FxHasher};
use serde_json::{Map, Value, json};
use std::hash::Hasher;

/// Field name marking an interned evidence-node reference.
const NODE_REF: &str = "#";
/// Field name marking an interned term reference.
const TERM_REF: &str = "~";
/// Bumped when the encoding changes shape.
const VERSION: u64 = 1;

/// Encode `run` in the compact form.
///
/// With `include_catalog` false the constraint catalog is omitted; decoding
/// then needs it supplied out of band ([`expand_with_catalog`]). Callers that
/// already hold the schema the run was produced against should omit it.
pub fn compact(run: &EvidenceRun, include_catalog: bool) -> serde_json::Result<Value> {
    Ok(compact_value(serde_json::to_value(run)?, include_catalog))
}

/// Encode an already-serialized run.
///
/// The typed form is not needed to compact — callers holding a run only as JSON
/// (the language bindings, a stored artifact) can encode it without a typed
/// round-trip.
pub fn compact_value(value: Value, include_catalog: bool) -> Value {
    encode(value, include_catalog).0
}

/// How often the interned entries of a run repeat.
///
/// Counted over the evidence alone — the `statements` — because that is the
/// part that grows with the corpus. The catalog is a fixed per-run cost and
/// would flatter the ratio: it is interned into the same tables, so folding it
/// in adds distinct entries without adding evidence occurrences.
///
/// Tagged nodes are split into two families, because they answer different
/// questions and the mixture is dominated by the wrong one. *Result* nodes are
/// [`Witness`](crate::witness::Witness) and [`SatTrace`](crate::witness::SatTrace)
/// — one validation judgment each. *Support* nodes are
/// [`PathSupport`](crate::witness::PathSupport) certificates, which say how a
/// value was reached and are not judgments about anything. Both serialize as
/// `{"type", "details"}` and the encoder interns both, so the combined
/// [`node_redundancy`](Self::node_redundancy) is a statement about encoding
/// cost only. Sharing *between validation results* is
/// [`result_redundancy`](Self::result_redundancy), and on the Brick corpus the
/// two differ by several fold because support nodes are the large majority of
/// occurrences.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sharing {
    /// Tagged nodes of either family as written out by the full encoding.
    pub node_occurrences: usize,
    /// Distinct such nodes, i.e. the size of the node table.
    pub distinct_nodes: usize,
    /// Occurrences of tagged nodes that carry a validation judgment.
    pub result_occurrences: usize,
    /// Distinct such nodes.
    pub distinct_results: usize,
    /// Occurrences of path-support certificates.
    pub support_occurrences: usize,
    /// Distinct such certificates.
    pub distinct_support: usize,
    /// RDF terms as written out by the full encoding.
    pub term_occurrences: usize,
    /// Distinct such terms, i.e. the size of the term table.
    pub distinct_terms: usize,
}

impl Sharing {
    /// Occurrences per distinct node over both families; 1.0 when nothing
    /// repeats.
    ///
    /// What the compact encoding collapses. Not a measure of sharing between
    /// validation results — see [`result_redundancy`](Self::result_redundancy).
    pub fn node_redundancy(&self) -> f64 {
        ratio(self.node_occurrences, self.distinct_nodes)
    }

    /// Occurrences per distinct validation-judgment node.
    pub fn result_redundancy(&self) -> f64 {
        ratio(self.result_occurrences, self.distinct_results)
    }

    /// Occurrences per distinct path-support certificate.
    pub fn support_redundancy(&self) -> f64 {
        ratio(self.support_occurrences, self.distinct_support)
    }

    /// Occurrences per distinct term; 1.0 when nothing repeats.
    pub fn term_redundancy(&self) -> f64 {
        ratio(self.term_occurrences, self.distinct_terms)
    }

    /// Share of tagged-node occurrences that are path support rather than
    /// validation judgments.
    pub fn support_share(&self) -> f64 {
        if self.node_occurrences == 0 {
            0.0
        } else {
            self.support_occurrences as f64 / self.node_occurrences as f64
        }
    }
}

fn ratio(occurrences: usize, distinct: usize) -> f64 {
    if distinct == 0 {
        1.0
    } else {
        occurrences as f64 / distinct as f64
    }
}

/// Measure how much a run's evidence repeats, without keeping the encoding.
///
/// Reported against the same predicates the encoder interns by, so the ratio
/// cannot drift from what compaction actually collapses.
pub fn sharing(run: &EvidenceRun) -> serde_json::Result<Sharing> {
    Ok(encode(serde_json::to_value(run)?, false).1)
}

fn encode(mut value: Value, include_catalog: bool) -> (Value, Sharing) {
    let conforms = value.get("conforms").cloned().unwrap_or(json!(false));
    let catalog = value
        .get_mut("constraints")
        .map(Value::take)
        .unwrap_or(Value::Null);

    let mut terms = Interner::default();
    let mut nodes = Interner::default();
    let mut families = Families::default();
    let statements = intern(
        value
            .get_mut("statements")
            .map(Value::take)
            .unwrap_or(json!([])),
        &mut terms,
        &mut nodes,
        &mut families,
    );
    // Read the counters before the catalog perturbs them: this is the sharing
    // among the evidence itself.
    let sharing = Sharing {
        node_occurrences: nodes.occurrences,
        distinct_nodes: nodes.table.len(),
        result_occurrences: families.result_occurrences,
        distinct_results: families.distinct_results,
        support_occurrences: families.support_occurrences,
        distinct_support: families.distinct_support,
        term_occurrences: terms.occurrences,
        distinct_terms: terms.table.len(),
    };
    // The catalog shares the same tables: its shapes are the very constraints
    // the evidence nodes refer to, so interning both together collapses the
    // overlap instead of writing each side out separately.
    let catalog = include_catalog.then(|| intern(catalog, &mut terms, &mut nodes, &mut families));

    let mut out = Map::new();
    out.insert("v".into(), json!(VERSION));
    out.insert("conforms".into(), conforms);
    out.insert("terms".into(), Value::Array(terms.table));
    out.insert("nodes".into(), Value::Array(nodes.table));
    out.insert("statements".into(), statements);
    if let Some(catalog) = catalog {
        out.insert("constraints".into(), catalog);
    }
    (Value::Object(out), sharing)
}

/// Serialize the compact encoding.
pub fn to_compact_json(run: &EvidenceRun, include_catalog: bool) -> serde_json::Result<String> {
    serde_json::to_string(&compact(run, include_catalog)?)
}

/// Reconstruct a run from its compact encoding, which must carry its catalog.
pub fn expand(value: &Value) -> Result<EvidenceRun, CompactError> {
    let catalog = value
        .get("constraints")
        .cloned()
        .ok_or(CompactError::MissingCatalog)?;
    expand_with_catalog(value, catalog)
}

/// Reconstruct a run whose catalog was omitted, using a catalog held elsewhere.
pub fn expand_with_catalog(value: &Value, catalog: Value) -> Result<EvidenceRun, CompactError> {
    serde_json::from_value(expand_value(value, catalog)?).map_err(CompactError::Decode)
}

/// Reconstruct the serialized run without decoding it into the typed form.
///
/// The inverse of [`compact_value`], for callers that only move JSON around.
pub fn expand_value(value: &Value, catalog: Value) -> Result<Value, CompactError> {
    let version = value.get("v").and_then(Value::as_u64);
    if version != Some(VERSION) {
        return Err(CompactError::Version(version));
    }
    let terms = array(value, "terms")?;
    let nodes = array(value, "nodes")?;
    let statements = value.get("statements").ok_or(CompactError::Malformed)?;

    // Node table entries reference only *earlier* entries, so expanding in
    // order lets each one reuse the already-expanded form of its children.
    let mut expanded: Vec<Value> = Vec::with_capacity(nodes.len());
    for node in nodes {
        let value = restore(node, terms, &expanded)?;
        expanded.push(value);
    }

    let mut out = Map::new();
    out.insert(
        "conforms".into(),
        value.get("conforms").cloned().unwrap_or(json!(false)),
    );
    // An interned catalog resolves through the tables; one supplied out of band
    // holds no references and passes through unchanged.
    out.insert("constraints".into(), restore(&catalog, terms, &expanded)?);
    out.insert("statements".into(), restore(statements, terms, &expanded)?);
    Ok(Value::Object(out))
}

/// Why a compact encoding could not be read back.
#[derive(Debug)]
pub enum CompactError {
    /// The encoding omitted its catalog; supply one with
    /// [`expand_with_catalog`].
    MissingCatalog,
    /// Encoded by a different version of this format.
    Version(Option<u64>),
    /// Required tables are absent or not arrays.
    Malformed,
    /// A table reference points outside the entries available at that point.
    InvalidReference { table: &'static str, id: usize },
    /// The reconstructed value is not a valid run.
    Decode(serde_json::Error),
}

impl std::fmt::Display for CompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCatalog => {
                write!(f, "compact evidence omits its constraint catalog")
            }
            Self::Version(found) => {
                write!(f, "compact evidence version {found:?}, expected {VERSION}")
            }
            Self::Malformed => write!(f, "compact evidence is missing its tables"),
            Self::InvalidReference { table, id } => {
                write!(f, "compact evidence has invalid {table} reference {id}")
            }
            Self::Decode(error) => write!(f, "compact evidence does not decode: {error}"),
        }
    }
}

impl std::error::Error for CompactError {}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value], CompactError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or(CompactError::Malformed)
}

#[derive(Default)]
struct Interner {
    table: Vec<Value>,
    /// Structural hash to the ids that hash there. Buckets hold more than one
    /// id only on a genuine hash collision, which the value comparison below
    /// then settles.
    index: FxHashMap<u64, Vec<u32>>,
    /// Every intern call, whether or not it was a first sight. The excess over
    /// `table.len()` is exactly what the encoding collapses.
    occurrences: usize,
}

impl Interner {
    /// The id for `value` and whether this call created it, inserting on first
    /// sight so that structurally identical entries collapse.
    ///
    /// Keyed by a structural hash rather than by the value's canonical text.
    /// Serializing each occurrence to a `String` to use as a key allocated once
    /// per *occurrence* and discarded it on every hit — and hits are the whole
    /// point of an interner. On `bldg11.ttl` that was roughly six million
    /// allocations for 371k distinct entries.
    fn intern(&mut self, value: Value) -> (usize, bool) {
        self.occurrences += 1;
        let hash = structural_hash(&value);
        if let Some(bucket) = self.index.get(&hash) {
            for &id in bucket {
                if self.table[id as usize] == value {
                    return (id as usize, false);
                }
            }
        }
        let id = self.table.len();
        self.table.push(value);
        self.index.entry(hash).or_default().push(id as u32);
        (id, true)
    }
}

/// Per-family node counts, split out of the single node table the encoding
/// needs. Every table entry belongs to exactly one family, so counting at
/// insertion partitions the table exactly.
#[derive(Default)]
struct Families {
    result_occurrences: usize,
    distinct_results: usize,
    support_occurrences: usize,
    distinct_support: usize,
}

impl Families {
    fn record(&mut self, result: bool, fresh: bool) {
        let (occurrences, distinct) = if result {
            (&mut self.result_occurrences, &mut self.distinct_results)
        } else {
            (&mut self.support_occurrences, &mut self.distinct_support)
        };
        *occurrences += 1;
        *distinct += usize::from(fresh);
    }
}

/// Hash a value by structure, without materializing it.
///
/// Agrees with `Value`'s own equality — including `Number`'s, which
/// distinguishes the integer and float representations rather than comparing
/// numerically — so equal values always land in the same bucket. Should that
/// ever cease to hold (`serde_json`'s `preserve_order` feature would make
/// object iteration order significant while equality stays order-independent),
/// the failure is a missed merge, not a wrong encoding: the value is stored
/// under a second id and expands to exactly the same tree.
fn structural_hash(value: &Value) -> u64 {
    let mut hasher = FxHasher::default();
    hash_into(value, &mut hasher);
    hasher.finish()
}

fn hash_into(value: &Value, hasher: &mut FxHasher) {
    match value {
        Value::Null => hasher.write_u8(0),
        Value::Bool(flag) => {
            hasher.write_u8(1);
            hasher.write_u8(u8::from(*flag));
        }
        Value::Number(number) => {
            hasher.write_u8(2);
            if let Some(unsigned) = number.as_u64() {
                hasher.write_u8(0);
                hasher.write_u64(unsigned);
            } else if let Some(signed) = number.as_i64() {
                hasher.write_u8(1);
                hasher.write_i64(signed);
            } else {
                hasher.write_u8(2);
                hasher.write_u64(number.as_f64().map_or(0, f64::to_bits));
            }
        }
        Value::String(text) => {
            hasher.write_u8(3);
            hasher.write(text.as_bytes());
        }
        Value::Array(items) => {
            hasher.write_u8(4);
            hasher.write_usize(items.len());
            items.iter().for_each(|item| hash_into(item, hasher));
        }
        Value::Object(map) => {
            hasher.write_u8(5);
            hasher.write_usize(map.len());
            for (key, child) in map {
                hasher.write(key.as_bytes());
                hash_into(child, hasher);
            }
        }
    }
}

/// An RDF term in SPARQL-JSON spelling: `{"type": "uri"|"bnode"|"literal", …}`.
/// Tagged evidence nodes also carry `type`, so the `details` key distinguishes
/// them.
fn is_term(map: &Map<String, Value>) -> bool {
    !map.contains_key("details")
        && map
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| matches!(kind, "uri" | "bnode" | "literal"))
        && map.contains_key("value")
}

/// A tagged evidence or path-support node.
fn is_node(map: &Map<String, Value>) -> bool {
    map.len() == 2 && map.contains_key("type") && map.contains_key("details")
}

/// Whether a tagged node carries a validation judgment rather than a path
/// certificate.
///
/// Decided by the presence of `shape` in the payload rather than by a list of
/// variant tags: every `Witness` and `SatTrace` variant carries a `shape`
/// field, while `PathSupport`'s payloads are a triple or an array of nested
/// certificates. A tag list would have to be revised whenever a variant is
/// added, and would misclassify silently when it was not; this follows the
/// vocabulary the same way the rest of the encoder does. `PathSupport::Empty`
/// is a unit variant, so it serializes without `details` and is never a node
/// at all.
fn is_result(value: &Value) -> bool {
    value
        .get("details")
        .and_then(Value::as_object)
        .is_some_and(|details| details.contains_key("shape"))
}

/// Replace every term and tagged node with a table reference, children first.
fn intern(
    mut value: Value,
    terms: &mut Interner,
    nodes: &mut Interner,
    families: &mut Families,
) -> Value {
    let (term, tagged) = match &value {
        Value::Object(map) => (is_term(map), is_node(map)),
        _ => (false, false),
    };
    if term {
        let (id, _) = terms.intern(value);
        return json!({ TERM_REF: id });
    }
    // Children are rewritten in place. Collecting them into a fresh `Map` or
    // `Vec` allocated a new container for every object and array in the run,
    // which is the whole tree — and the tree is what makes runs large enough
    // to want compacting in the first place.
    match &mut value {
        Value::Object(map) => {
            for child in map.values_mut() {
                let taken = std::mem::replace(child, Value::Null);
                *child = intern(taken, terms, nodes, families);
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                let taken = std::mem::replace(item, Value::Null);
                *item = intern(taken, terms, nodes, families);
            }
        }
        _ => {}
    }
    if tagged {
        // Classified before interning: the payload is still in hand, and
        // children have already been replaced by references, which leaves the
        // `shape` key untouched either way.
        let result = is_result(&value);
        let (id, fresh) = nodes.intern(value);
        families.record(result, fresh);
        json!({ NODE_REF: id })
    } else {
        value
    }
}

/// Resolve table references back into the original tree.
fn restore(value: &Value, terms: &[Value], nodes: &[Value]) -> Result<Value, CompactError> {
    match value {
        Value::Object(map) => {
            if let Some(id) = reference(map, TERM_REF) {
                return terms
                    .get(id)
                    .cloned()
                    .ok_or(CompactError::InvalidReference { table: "term", id });
            }
            if let Some(id) = reference(map, NODE_REF) {
                return nodes
                    .get(id)
                    .cloned()
                    .ok_or(CompactError::InvalidReference { table: "node", id });
            }
            Ok(Value::Object(
                map.iter()
                    .map(|(key, child)| Ok((key.clone(), restore(child, terms, nodes)?)))
                    .collect::<Result<Map<_, _>, CompactError>>()?,
            ))
        }
        Value::Array(items) => Ok(Value::Array(
            items
                .iter()
                .map(|item| restore(item, terms, nodes))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

fn reference(map: &Map<String, Value>, key: &str) -> Option<usize> {
    (map.len() == 1)
        .then(|| map.get(key))
        .flatten()
        .and_then(Value::as_u64)
        .map(|id| id as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate_with_evidence;
    use crate::witness::{PathSupport, SatTrace};
    use oxrdf::{NamedNode, Triple};
    use shifty_parse::{load_turtle, parse_turtle};

    // Exercises every term kind the encoding must recognize — IRIs, blank
    // nodes, plain/typed/tagged literals — alongside both evidence polarities.
    const TTL: &str = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetClass ex:T ;
          sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] ;
          sh:property [ sh:path ex:q ; sh:maxCount 1 ] ;
          sh:property [ sh:path ex:n ; sh:datatype xsd:integer ] ;
          sh:property [ sh:path ex:label ; sh:minCount 1 ] .
        ex:T2 a sh:NodeShape ; sh:targetClass ex:T ;
          sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] .
        ex:good a ex:T ; ex:p ex:c1 ; ex:q ex:z ;
          ex:n 3 ; ex:label "hello"@en ; ex:child [ ex:p ex:c1 ] .
        ex:bad a ex:T ; ex:q ex:z ; ex:q ex:y ; ex:n "not a number" .
        ex:c1 a ex:C .
    "#;

    fn run() -> EvidenceRun {
        let parsed = parse_turtle(TTL.as_bytes(), None).unwrap();
        let loaded = load_turtle(TTL.as_bytes(), None).unwrap();
        validate_with_evidence(&loaded.graph, &parsed.schema).unwrap()
    }

    #[test]
    fn compact_round_trips_exactly() {
        let original = run();
        let encoded = compact(&original, true).unwrap();
        let restored = expand(&encoded).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn catalog_can_be_carried_out_of_band() {
        let original = run();
        let encoded = compact(&original, false).unwrap();
        assert!(encoded.get("constraints").is_none());

        let catalog = serde_json::to_value(&original.constraints).unwrap();
        let restored = expand_with_catalog(&encoded, catalog).unwrap();
        assert_eq!(restored, original);
        assert!(matches!(
            expand(&encoded),
            Err(CompactError::MissingCatalog)
        ));
    }

    #[test]
    fn identical_subtrees_are_stored_once() {
        // ex:S and ex:T2 declare the same property constraint, and both select
        // the same two focus nodes, so the evidence repeats.
        let original = run();
        let encoded = compact(&original, true).unwrap();
        let nodes = encoded.get("nodes").unwrap().as_array().unwrap().len();
        let emitted = original.walk().len();
        assert!(
            nodes < emitted,
            "expected sharing: {nodes} distinct vs {emitted} emitted"
        );
    }

    #[test]
    fn compact_is_smaller_than_the_full_run() {
        let original = run();
        let full = original.to_json().unwrap().len();
        let packed = to_compact_json(&original, true).unwrap().len();
        assert!(
            packed < full,
            "compact {packed} not smaller than full {full}"
        );
    }

    #[test]
    fn sharing_counts_the_evidence_the_encoder_interns() {
        let original = run();
        let measured = sharing(&original).unwrap();

        // Distinct counts are the tables the encoding actually writes, and the
        // catalog must not inflate them: it is interned into the same tables,
        // so measuring off the full encoding would report more distinct nodes
        // than the evidence has occurrences.
        let encoded = compact(&original, false).unwrap();
        let table = |key: &str| encoded.get(key).unwrap().as_array().unwrap().len();
        assert_eq!(measured.distinct_nodes, table("nodes"));
        assert_eq!(measured.distinct_terms, table("terms"));
        assert_eq!(
            table("nodes"),
            compact(&original, true)
                .unwrap()
                .get("nodes")
                .unwrap()
                .as_array()
                .unwrap()
                .len()
                .min(table("nodes")),
            "the catalog only ever adds entries"
        );

        // Occurrences exceed distinct entries exactly when something repeats,
        // which is what the fixture is built to do.
        assert!(measured.node_occurrences > measured.distinct_nodes);
        assert!(measured.term_occurrences > measured.distinct_terms);
        assert!(measured.node_redundancy() > 1.0);
        assert!(measured.term_redundancy() > 1.0);
    }

    // The split exists because the combined node counts are dominated by path
    // support, so anything read off them as "sharing between results" is
    // reading the wrong family. Two things pin it: the partition is exact, and
    // the result side agrees with an independent typed traversal that never
    // touches JSON.
    #[test]
    fn the_node_families_partition_and_agree_with_the_typed_walk() {
        let original = run();
        let measured = sharing(&original).unwrap();

        assert_eq!(
            measured.result_occurrences + measured.support_occurrences,
            measured.node_occurrences,
            "every tagged node belongs to exactly one family"
        );
        assert_eq!(
            measured.distinct_results + measured.distinct_support,
            measured.distinct_nodes,
            "the families partition the node table"
        );

        // `walk` visits `Witness`/`SatTrace` and nothing else, by a traversal
        // written against the types rather than the serialized form.
        assert_eq!(
            measured.result_occurrences,
            original.walk().len(),
            "result occurrences are exactly the evidence nodes"
        );
        assert!(
            measured.support_occurrences > 0,
            "the fixture reaches values by a path, so it has certificates"
        );
    }

    #[test]
    fn path_support_is_not_mistaken_for_a_judgment() {
        // `PathSupport::Edge` and a `Witness` are both `{type, details}`; only
        // the judgment carries a shape.
        let edge = serde_json::to_value(PathSupport::Edge(Triple::new(
            NamedNode::new("urn:s").unwrap(),
            NamedNode::new("urn:p").unwrap(),
            NamedNode::new("urn:o").unwrap(),
        )))
        .unwrap();
        assert!(is_node(edge.as_object().unwrap()));
        assert!(!is_result(&edge));

        assert!(!is_result(
            &serde_json::to_value(PathSupport::Chain(vec![])).unwrap()
        ));

        let judgment = serde_json::to_value(SatTrace::Irrefutable {
            shape: shifty_algebra::ShapeId(0),
        })
        .unwrap();
        assert!(is_node(judgment.as_object().unwrap()));
        assert!(is_result(&judgment));
    }

    // The structural hash replaced a canonical-text key, so what needs pinning
    // is that it still agrees with equality: one table entry per distinct
    // value, and never two distinct values sharing an id.
    #[test]
    fn interning_gives_one_entry_per_distinct_value() {
        let distinct = [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(-1),
            json!(1.5),
            json!(""),
            json!("0"),
            json!([]),
            json!([json!(0)]),
            json!([json!(0), json!(0)]),
            json!({}),
            json!({ "a": 0 }),
            json!({ "a": 0, "b": 1 }),
            json!({ "b": 0, "a": 1 }),
            json!({ "a": { "a": 0 } }),
        ];

        let mut interner = Interner::default();
        let first: Vec<(usize, bool)> = distinct
            .iter()
            .map(|value| interner.intern(value.clone()))
            .collect();
        assert_eq!(
            first,
            (0..distinct.len()).map(|id| (id, true)).collect::<Vec<_>>(),
            "every distinct value is a fresh insertion"
        );

        // Re-interning returns the original ids, reports no insertion, and adds
        // nothing to the table.
        let again: Vec<(usize, bool)> = distinct
            .iter()
            .map(|value| interner.intern(value.clone()))
            .collect();
        assert_eq!(
            again,
            first.iter().map(|&(id, _)| (id, false)).collect::<Vec<_>>()
        );
        assert_eq!(interner.table.len(), distinct.len());
        assert_eq!(interner.occurrences, 2 * distinct.len());

        // Every id resolves back to the value that produced it.
        for (value, &(id, _)) in distinct.iter().zip(first.iter()) {
            assert_eq!(&interner.table[id], value);
        }
    }

    #[test]
    fn colliding_values_stay_distinct() {
        // Force the collision path: two unequal values pushed into one bucket
        // must still receive different ids.
        let mut interner = Interner::default();
        let a = interner.intern(json!({ "type": "uri", "value": "urn:a" }));
        let b = interner.intern(json!({ "type": "uri", "value": "urn:b" }));
        assert_ne!(a, b);

        let bucket: Vec<u32> = interner.index.values().flatten().copied().collect();
        assert_eq!(bucket.len(), 2, "both ids are reachable from the index");
    }

    #[test]
    fn sharing_of_nothing_is_one() {
        assert_eq!(Sharing::default().node_redundancy(), 1.0);
        assert_eq!(Sharing::default().term_redundancy(), 1.0);
    }

    #[test]
    fn a_foreign_version_is_rejected() {
        let mut encoded = compact(&run(), true).unwrap();
        encoded["v"] = json!(VERSION + 1);
        assert!(matches!(expand(&encoded), Err(CompactError::Version(_))));
    }

    #[test]
    fn dangling_and_forward_references_are_rejected() {
        let dangling = json!({
            "v": VERSION,
            "conforms": false,
            "terms": [],
            "nodes": [],
            "statements": { (NODE_REF): 0 },
            "constraints": [],
        });
        assert!(matches!(
            expand_value(&dangling, json!([])),
            Err(CompactError::InvalidReference {
                table: "node",
                id: 0
            })
        ));

        let forward = json!({
            "v": VERSION,
            "conforms": false,
            "terms": [],
            "nodes": [{ (NODE_REF): 0 }],
            "statements": [],
            "constraints": [],
        });
        assert!(matches!(
            expand_value(&forward, json!([])),
            Err(CompactError::InvalidReference {
                table: "node",
                id: 0
            })
        ));
    }
}
