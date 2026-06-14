//! The re-validation gate (`docs/06-repair.md` §8).
//!
//! A repair is sound only if it does not trade one violation for another, so the
//! gate is **whole-graph**, not focus-local: it re-validates `G ⊕ ΔG` and diffs
//! the violation set against `G`'s. It **decides nothing and applies nothing** —
//! it returns a [`RepairOutcome`] the driver reads.
//!
//! The verdict is a set difference over the existing [`validate`] oracle, keyed by
//! `(focus, statement)`. Because the contract is this delta (not a bare `v ⊨ φ`),
//! a cheaper *affected-set* re-validation can replace the implementation later
//! with identical semantics.

use crate::validate::{NonStratifiable, Violation, validate};
use oxrdf::{Graph, Term};
use serde::{Deserialize, Serialize};
use shifty_algebra::Schema;
use shifty_repair::GraphDelta;
use std::collections::{HashMap, HashSet};

/// The gate's verdict on a candidate `ΔG`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairOutcome {
    /// Violations present in `G` that `ΔG` removes.
    pub fixed: Vec<Violation>,
    /// New violations `ΔG` would cause (empty ⟺ sound).
    pub introduced: Vec<Violation>,
    /// Pre-existing violations still unaddressed.
    pub remaining: Vec<Violation>,
}

impl RepairOutcome {
    /// Sound iff it introduces no new violation anywhere.
    pub fn is_sound(&self) -> bool {
        self.introduced.is_empty()
    }

    /// Progress iff sound *and* it fixes at least one violation.
    pub fn is_progress(&self) -> bool {
        self.is_sound() && !self.fixed.is_empty()
    }
}

/// Validate `data ⊕ delta` against `schema` and diff the violations against
/// `data`'s. Errors only if the schema is not stratifiable (as `validate` does).
pub fn gate(
    data: &Graph,
    schema: &Schema,
    delta: &GraphDelta,
) -> Result<RepairOutcome, NonStratifiable> {
    let baseline = validate(data, schema)?.violations;
    let patched_graph = apply(data, delta);
    let patched = validate(&patched_graph, schema)?.violations;
    Ok(diff(baseline, patched))
}

/// `G ⊕ ΔG`: deletes first, then adds (so a re-add wins on conflict).
pub fn apply(data: &Graph, delta: &GraphDelta) -> Graph {
    let mut g = data.clone();
    for t in &delta.delete {
        g.remove(t);
    }
    for t in &delta.add {
        g.insert(t);
    }
    g
}

/// A violation's identity: which focus failed which statement. `validate` emits
/// at most one violation per such pair.
fn key(v: &Violation) -> (Term, usize) {
    (v.focus.clone(), v.statement)
}

fn diff(baseline: Vec<Violation>, patched: Vec<Violation>) -> RepairOutcome {
    let baseline_keys: HashSet<(Term, usize)> = baseline.iter().map(key).collect();
    let patched_keys: HashSet<(Term, usize)> = patched.iter().map(key).collect();

    let fixed = baseline
        .into_iter()
        .filter(|v| !patched_keys.contains(&key(v)))
        .collect();

    let mut introduced = Vec::new();
    let mut remaining = Vec::new();
    for v in patched {
        if baseline_keys.contains(&key(&v)) {
            remaining.push(v);
        } else {
            introduced.push(v);
        }
    }
    RepairOutcome {
        fixed,
        introduced,
        remaining,
    }
}

/// A driver-friendly key index, should a driver wish to look outcomes up.
pub fn outcome_index(outcome: &RepairOutcome) -> HashMap<(Term, usize), &'static str> {
    let mut m = HashMap::new();
    for v in &outcome.fixed {
        m.insert(key(v), "fixed");
    }
    for v in &outcome.remaining {
        m.insert(key(v), "remaining");
    }
    for v in &outcome.introduced {
        m.insert(key(v), "introduced");
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{NamedNode, Triple};
    use shifty_parse::{load_turtle, parse_turtle};

    const PREFIXES: &str = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix ex:  <http://ex/> .
    "#;

    fn schema_and_graph(ttl: &str) -> (Schema, Graph) {
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        (parsed.schema, loaded.graph)
    }

    fn t(s: &str, p: &str, o: &str) -> Triple {
        Triple::new(
            NamedNode::new(s).unwrap(),
            NamedNode::new(p).unwrap(),
            NamedNode::new(o).unwrap(),
        )
    }

    #[test]
    fn sound_repair_fixes_one_and_introduces_none() {
        // ex:x needs an ex:p; it has none.
        let (schema, graph) = schema_and_graph(&format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            "
        ));
        let delta = GraphDelta {
            add: vec![t("http://ex/x", "http://ex/p", "http://ex/y")],
            delete: vec![],
        };
        let outcome = gate(&graph, &schema, &delta).unwrap();
        assert!(outcome.is_sound());
        assert!(outcome.is_progress());
        assert_eq!(outcome.fixed.len(), 1);
        assert!(outcome.remaining.is_empty());
    }

    #[test]
    fn collateral_delete_is_caught_as_introduced() {
        // Both ex:x and ex:y need an ex:p; both have one. Deleting y's breaks y.
        let (schema, graph) = schema_and_graph(&format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x, ex:y ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:x ex:p ex:a .
            ex:y ex:p ex:b .
            "
        ));
        // a delete that fixes nothing and breaks ex:y.
        let delta = GraphDelta {
            add: vec![],
            delete: vec![t("http://ex/y", "http://ex/p", "http://ex/b")],
        };
        let outcome = gate(&graph, &schema, &delta).unwrap();
        assert!(!outcome.is_sound(), "introduces a violation at ex:y");
        assert_eq!(outcome.introduced.len(), 1);
        assert_eq!(outcome.introduced[0].focus.to_string(), "<http://ex/y>");
        assert!(outcome.fixed.is_empty());
    }

    #[test]
    fn noop_delta_over_conforming_graph_is_empty() {
        let (schema, graph) = schema_and_graph(&format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:x ex:p ex:y .
            "
        ));
        let outcome = gate(&graph, &schema, &GraphDelta::default()).unwrap();
        assert_eq!(outcome, RepairOutcome::default());
    }

    #[test]
    fn end_to_end_synthesized_repair_passes_the_gate() {
        use crate::synthesize::synthesize;
        use crate::witness::witness_violations;
        use shifty_repair::{Plan, instantiate};

        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype <http://www.w3.org/2001/XMLSchema#integer> ] .
            ex:x ex:p \"hello\" .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();

        let ws = witness_violations(&loaded.graph, &parsed.schema).unwrap();
        let tree = synthesize(&parsed.schema.arena, &ws[0]);

        // fill the replacement hole with a good integer.
        let mut plan = Plan::default();
        let hole = instantiate(&tree, &plan).open_holes[0].0;
        plan.binding.insert(
            hole,
            oxrdf::Literal::new_typed_literal("7", oxrdf::vocab::xsd::INTEGER).into(),
        );
        let delta = instantiate(&tree, &plan).delta;

        let outcome = gate(&loaded.graph, &parsed.schema, &delta).unwrap();
        assert!(outcome.is_progress(), "{outcome:?}");
        assert!(outcome.introduced.is_empty());
    }
}
