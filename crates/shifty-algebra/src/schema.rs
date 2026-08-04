//! A SHACL schema `S` (doc 00 §4).
//!
//! `S = { (sel, φ) }` is a set of selector/shape statements; we additionally
//! carry SHACL-AF rules. All shapes (targeted or merely referenced) live in the
//! shared [`ShapeArena`]; statements and rules reference them by [`ShapeId`].
//!
//! ```text
//! G ⊨ S  iff  ∀ v. ∀ (sel, φ) ∈ S.  (G,v ⊨ sel) ⟹ (G,v ⊨ φ)
//! ```

use crate::rule::Rule;
use crate::selector::Selector;
use crate::shape::{ShapeArena, ShapeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One `(selector, shape)` pair. A shape with several targets yields several
/// statements sharing the same `shape` id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statement {
    pub selector: Selector,
    pub shape: ShapeId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub arena: ShapeArena,
    pub statements: Vec<Statement>,
    pub rules: Vec<Rule>,
    /// IRI names for arena slots that came from named (non-blank) RDF nodes.
    /// Blank-node shapes have no entry here.
    ///
    /// A slot carries *every* authored name that reached it. Normalization
    /// collapses structurally identical shapes, so two named shapes stating the
    /// same constraint share one slot; keeping only one of their names would
    /// make a lookup by the other silently miss. Each list is sorted and
    /// deduplicated, so which name is "first" never depends on hash iteration
    /// order.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub names: HashMap<ShapeId, Vec<String>>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    /// The display name for `id`: the first of its authored names, or `None`
    /// for a blank-node shape. Deterministic, since [`names`](Self::names) is
    /// sorted — pick this over indexing `names` directly when one label is
    /// wanted, so a collapsed shape does not report an arbitrary one of its
    /// names.
    pub fn name_of(&self, id: ShapeId) -> Option<&str> {
        self.names.get(&id)?.first().map(String::as_str)
    }

    /// Every authored name that reached `id`, sorted. More than one when
    /// normalization collapsed several named shapes onto the same slot; empty
    /// for a blank-node shape. Match against this, never against
    /// [`name_of`](Self::name_of), or a lookup by a collapsed shape's other
    /// name misses.
    pub fn names_of(&self, id: ShapeId) -> &[String] {
        self.names.get(&id).map_or(&[], Vec::as_slice)
    }

    /// Record `name` for `id`, keeping the list sorted and deduplicated.
    pub fn add_name(&mut self, id: ShapeId, name: String) {
        let names = self.names.entry(id).or_default();
        if let Err(at) = names.binary_search(&name) {
            names.insert(at, name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::Path;
    use crate::shape::Shape;
    use crate::term::{NamedNode, NodeKindSet};

    /// Build a small recursive schema and confirm it survives a serde round-trip
    /// (cycles are encoded as plain indices, so JSON handles them fine).
    #[test]
    fn schema_serde_roundtrip_with_cycle() {
        let mut schema = Schema::new();
        let knows = NamedNode::new("http://ex/knows").unwrap();

        // S := (nodeKind IRI) ∧ (∃≥1 knows . S)
        let s = schema.arena.reserve();
        let kind = schema.arena.insert(Shape::TestKind(NodeKindSet::IRI));
        let reaches = schema.arena.insert(Shape::Count {
            path: Path::Pred(knows.clone()),
            min: Some(1),
            max: None,
            qualifier: s,
        });
        schema.arena.set(s, Shape::And(vec![kind, reaches]));

        schema.statements.push(Statement {
            selector: Selector::HasOut(knows),
            shape: s,
        });

        let json = serde_json::to_string(&schema).unwrap();
        let back: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, back);
    }
}
