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
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
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
