//! A SHACL schema `S` (doc 00 §4).
//!
//! `S = { (sel, φ) }` is a set of selector/shape statements; we additionally
//! carry SHACL-AF rules. All shapes (targeted or merely referenced) live in the
//! shared [`ShapeArena`]; statements and rules reference them by [`ShapeId`].
//!
//! ```text
//! G ⊨ S  iff  ∀ v. ∀ (sel, φ) ∈ S.  (G,v ⊨ sel) ⟹ (G,v ⊨ φ)
//! ```

use crate::import::Import;
use crate::prefix::Prefixes;
use crate::provenance::Origin;
use crate::rule::{Rule, RuleHead};
use crate::selector::Selector;
use crate::shape::{ShapeArena, ShapeId};
use crate::term::Term;
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
    /// Originating shapes-graph node for arena slots lowered from an RDF node
    /// (named or blank). Synthetic slots introduced by lowering have no entry.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sources: HashMap<ShapeId, Term>,
    /// The `@prefix` declarations of the document this schema was lowered from,
    /// used only to compact IRIs when rendering. Display metadata: evaluation
    /// never consults it, and an empty table costs legibility, not correctness.
    #[serde(default, skip_serializing_if = "Prefixes::is_empty")]
    pub prefixes: Prefixes,
    /// Documents this schema depends on (other Shapeshift files, SHACL shapes
    /// graphs, ontologies). Declarations only: resolution happens outside the
    /// algebra, so a schema can list its imports without fetching them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imports: Vec<Import>,
    /// Authoring provenance for slots built by a text front end: which named
    /// shape, field line, macro call, include, or boolean branch produced each
    /// slot. Parallel to [`sources`](Self::sources), which serves the same role
    /// for slots lowered from RDF. Display metadata; evaluation ignores it.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub origins: HashMap<ShapeId, Vec<Origin>>,
    /// Docstrings attached to slots (`doc "..."` in Shapeshift,
    /// `rdfs:comment`/`sh:description` on export). Display metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub docs: HashMap<ShapeId, String>,
}

/// The id translation produced by [`Schema::merge`]: `map[old.0]` is the slot
/// the merged-in schema's `old` slot now occupies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeRemap {
    pub map: Vec<ShapeId>,
}

impl ShapeRemap {
    pub fn get(&self, id: ShapeId) -> ShapeId {
        self.map[id.0 as usize]
    }
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

    /// Record an authoring origin for `id`, skipping exact duplicates.
    pub fn add_origin(&mut self, id: ShapeId, origin: Origin) {
        let origins = self.origins.entry(id).or_default();
        if !origins.contains(&origin) {
            origins.push(origin);
        }
    }

    /// Every authoring origin recorded for `id`.
    pub fn origins_of(&self, id: ShapeId) -> &[Origin] {
        self.origins.get(&id).map_or(&[], Vec::as_slice)
    }

    /// The slot carrying `name` among its authored names, if any. Linear in
    /// the number of named slots; meant for front ends and tools, not for the
    /// evaluator.
    pub fn slot_named(&self, name: &str) -> Option<ShapeId> {
        let mut found: Vec<ShapeId> = self
            .names
            .iter()
            .filter(|(_, names)| names.iter().any(|n| n == name))
            .map(|(id, _)| *id)
            .collect();
        found.sort();
        found.first().copied()
    }

    /// Append `other` into this schema, re-indexing every [`ShapeId`] it
    /// contains (arena children, selectors, rules, and every sidecar).
    ///
    /// Statements, rules, imports, names, sources, origins, and docs are all
    /// carried over; prefixes are merged with this schema's declarations taking
    /// precedence. Names are IRIs and therefore already globally scoped, so
    /// they are not renamed. Returns the translation for the caller to remap
    /// any ids of `other` it still holds.
    pub fn merge(&mut self, other: Schema) -> ShapeRemap {
        let offset = self.arena.len() as u32;
        let map: Vec<ShapeId> = (0..other.arena.len() as u32)
            .map(|i| ShapeId(i + offset))
            .collect();
        let f = |id: ShapeId| map.get(id.0 as usize).copied().unwrap_or(id);
        for i in 0..other.arena.len() {
            let shape = other.arena.get(ShapeId(i as u32)).remap(&f);
            self.arena.insert(shape);
        }
        for st in &other.statements {
            self.statements.push(crate::schema::Statement {
                selector: st.selector.remap(&f),
                shape: f(st.shape),
            });
        }
        for rule in &other.rules {
            self.rules.push(Rule {
                selector: rule.selector.remap(&f),
                conditions: rule.conditions.iter().map(|c| f(*c)).collect(),
                head: match &rule.head {
                    RuleHead::Triple {
                        subject,
                        predicate,
                        object,
                    } => RuleHead::Triple {
                        subject: subject.remap(&f),
                        predicate: predicate.remap(&f),
                        object: object.remap(&f),
                    },
                    RuleHead::Sparql(q) => RuleHead::Sparql(q.clone()),
                },
                order: rule.order,
                deactivated: rule.deactivated,
            });
        }
        for (id, names) in other.names {
            for name in names {
                self.add_name(f(id), name);
            }
        }
        for (id, source) in other.sources {
            self.sources.insert(f(id), source);
        }
        for (id, origins) in other.origins {
            for origin in origins {
                self.add_origin(f(id), origin.remap(&f));
            }
        }
        for (id, doc) in other.docs {
            self.docs.entry(f(id)).or_insert(doc);
        }
        for import in other.imports {
            if !self.imports.contains(&import) {
                self.imports.push(import);
            }
        }
        let mut decls: Vec<(String, String)> = self.prefixes.declarations().to_vec();
        for (p, ns) in other.prefixes.declarations() {
            if !decls.iter().any(|(q, _)| q == p) {
                decls.push((p.clone(), ns.clone()));
            }
        }
        self.prefixes = Prefixes::new(decls);
        ShapeRemap { map }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::Path;
    use crate::shape::Shape;
    use crate::term::{NamedNode, NodeKindSet};

    #[test]
    fn merge_reindexes_children_selectors_and_sidecars() {
        let knows = NamedNode::new("http://ex/knows").unwrap();
        let mut left = Schema::new();
        left.arena.insert(Shape::Top);

        let mut right = Schema::new();
        let s = right.arena.reserve();
        let n = right.arena.insert(Shape::Not(s));
        right.arena.set(
            s,
            Shape::Count {
                path: Path::Pred(knows.clone()),
                min: None,
                max: Some(0),
                qualifier: n,
            },
        );
        right.statements.push(Statement {
            selector: Selector::HasPath(Path::Id, s),
            shape: s,
        });
        right.add_name(s, "http://ex/S".into());
        right.docs.insert(s, "doc".into());

        let remap = left.merge(right);
        let s2 = remap.get(s);
        assert_eq!(s2, ShapeId(1));
        assert_eq!(left.name_of(s2), Some("http://ex/S"));
        assert_eq!(left.docs.get(&s2).map(String::as_str), Some("doc"));
        assert_eq!(left.statements[0].selector, Selector::HasPath(Path::Id, s2));
        match left.arena.get(s2) {
            Shape::Count { qualifier, .. } => {
                assert_eq!(left.arena.get(*qualifier), &Shape::Not(s2));
            }
            other => panic!("expected Count, got {other:?}"),
        }
        assert_eq!(left.slot_named("http://ex/S"), Some(s2));
    }

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
