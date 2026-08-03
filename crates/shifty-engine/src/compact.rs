//! Compact, lossless encoding of an [`EvidenceRun`](crate::EvidenceRun).
//!
//! Full evidence is large for two measured reasons, and they dominate on
//! different corpora:
//!
//!   * **Repeated subtrees.** The same `(constraint, node)` conclusion is
//!     reached through many parents, and each occurrence is written out in
//!     full. On a Brick model 120,811 emitted evidence nodes reduce to 20,165
//!     distinct ones — a 6× redundancy that is ~57% of the serialized run.
//!   * **The constraint catalog.** Both arenas are dumped on every run
//!     regardless of how many findings there are. On a small 223P model that
//!     fixed cost is ~57% of the run.
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
use serde_json::{Map, Value, json};
use std::collections::HashMap;

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
    let mut value = serde_json::to_value(run)?;
    let catalog = value
        .get_mut("constraints")
        .map(Value::take)
        .unwrap_or(Value::Null);

    let mut terms = Interner::default();
    let mut nodes = Interner::default();
    let statements = intern(
        value.get_mut("statements").map(Value::take).unwrap_or(json!([])),
        &mut terms,
        &mut nodes,
    );
    // The catalog shares the same tables: its shapes are the very constraints
    // the evidence nodes refer to, so interning both together collapses the
    // overlap instead of writing each side out separately.
    let catalog = include_catalog.then(|| intern(catalog, &mut terms, &mut nodes));

    let mut out = Map::new();
    out.insert("v".into(), json!(VERSION));
    out.insert("conforms".into(), json!(run.conforms));
    out.insert("terms".into(), Value::Array(terms.table));
    out.insert("nodes".into(), Value::Array(nodes.table));
    out.insert("statements".into(), statements);
    if let Some(catalog) = catalog {
        out.insert("constraints".into(), catalog);
    }
    Ok(Value::Object(out))
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
        let value = restore(node, terms, &expanded);
        expanded.push(value);
    }

    let mut out = Map::new();
    out.insert(
        "conforms".into(),
        value.get("conforms").cloned().unwrap_or(json!(false)),
    );
    // An interned catalog resolves through the tables; one supplied out of band
    // holds no references and passes through unchanged.
    out.insert("constraints".into(), restore(&catalog, terms, &expanded));
    out.insert("statements".into(), restore(statements, terms, &expanded));
    serde_json::from_value(Value::Object(out)).map_err(CompactError::Decode)
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
    /// The reconstructed value is not a valid run.
    Decode(serde_json::Error),
}

impl std::fmt::Display for CompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCatalog => {
                write!(f, "compact evidence omits its constraint catalog")
            }
            Self::Version(found) => write!(
                f,
                "compact evidence version {found:?}, expected {VERSION}"
            ),
            Self::Malformed => write!(f, "compact evidence is missing its tables"),
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
    index: HashMap<String, usize>,
}

impl Interner {
    /// The id for `value`, inserting it on first sight. Keyed by the value's
    /// canonical text, so structurally identical entries collapse.
    fn intern(&mut self, value: Value) -> usize {
        let key = value.to_string();
        if let Some(&id) = self.index.get(&key) {
            return id;
        }
        let id = self.table.len();
        self.index.insert(key, id);
        self.table.push(value);
        id
    }
}

/// An RDF term in SPARQL-JSON spelling: `{"type": "uri"|"bnode"|"literal", …}`.
/// Tagged evidence nodes also carry `type`, so the `details` key distinguishes
/// them.
fn is_term(map: &Map<String, Value>) -> bool {
    !map.contains_key("details")
        && map.get("type").and_then(Value::as_str).is_some_and(|kind| {
            matches!(kind, "uri" | "bnode" | "literal")
        })
        && map.contains_key("value")
}

/// A tagged evidence, path-support, or shape node.
fn is_node(map: &Map<String, Value>) -> bool {
    map.len() == 2 && map.contains_key("type") && map.contains_key("details")
}

/// Replace every term and tagged node with a table reference, children first.
fn intern(value: Value, terms: &mut Interner, nodes: &mut Interner) -> Value {
    match value {
        Value::Object(map) => {
            if is_term(&map) {
                let id = terms.intern(Value::Object(map));
                return json!({ TERM_REF: id });
            }
            let tagged = is_node(&map);
            let interned: Map<String, Value> = map
                .into_iter()
                .map(|(key, child)| (key, intern(child, terms, nodes)))
                .collect();
            if tagged {
                let id = nodes.intern(Value::Object(interned));
                json!({ NODE_REF: id })
            } else {
                Value::Object(interned)
            }
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| intern(item, terms, nodes))
                .collect(),
        ),
        other => other,
    }
}

/// Resolve table references back into the original tree.
fn restore(value: &Value, terms: &[Value], nodes: &[Value]) -> Value {
    match value {
        Value::Object(map) => {
            if let Some(id) = reference(map, TERM_REF) {
                return terms.get(id).cloned().unwrap_or(Value::Null);
            }
            if let Some(id) = reference(map, NODE_REF) {
                return nodes.get(id).cloned().unwrap_or(Value::Null);
            }
            Value::Object(
                map.iter()
                    .map(|(key, child)| (key.clone(), restore(child, terms, nodes)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(
            items.iter().map(|item| restore(item, terms, nodes)).collect(),
        ),
        other => other.clone(),
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
        assert!(packed < full, "compact {packed} not smaller than full {full}");
    }

    #[test]
    fn a_foreign_version_is_rejected() {
        let mut encoded = compact(&run(), true).unwrap();
        encoded["v"] = json!(VERSION + 1);
        assert!(matches!(expand(&encoded), Err(CompactError::Version(_))));
    }
}
