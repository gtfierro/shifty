//! The shape grammar `φ` (doc 00 §3, Def. 4) and the shape arena.
//!
//! ```text
//! φ ::= ⊤ | test(c) | test(τ) | closed(Q) | eq(π,p) | disj(π,p)
//!     | ¬φ | φ ∧ φ′ | φ ∨ φ′ | ∃≥ⁿ π.φ | ∃≤ⁿ π.φ
//! ```
//!
//! Shapes form a *graph*, not a tree: SHACL shapes may reference one another
//! cyclically (via `sh:node` / `sh:property` / `sh:qualifiedValueShape`). We
//! therefore store shapes in a [`ShapeArena`] and nest them by [`ShapeId`]
//! index rather than by `Box`. The [`ShapeArena::reserve`]/[`ShapeArena::set`]
//! pair lets a shape reference itself before its body is built.
//!
//! The smart constructors apply only *light, always-sound* Boolean
//! simplifications; real normalization (NNF, unsat detection, CSE) is Layer 4.

use crate::path::Path;
use crate::severity::Severity;
use crate::sparql::SparqlConstraint;
use crate::term::{NamedNode, NodeKindSet, Term};
use crate::value_type::ValueType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// An index into a [`ShapeArena`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShapeId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Shape {
    /// Source-shape metadata. This wrapper is logically transparent, but keeps
    /// severity attached through normalization and physical planning.
    Annotated { severity: Severity, shape: ShapeId },
    /// `⊤` — trivially satisfied.
    Top,
    /// A reserved-but-unfilled arena slot, transient during construction of
    /// cyclic shapes. Kept distinct from [`Shape::Top`] so the smart
    /// constructors never absorb a forward/back reference that is still being
    /// built. A finalized schema contains no `Pending`.
    Pending,
    /// `test(c)` — equality with a constant. Widened from values to any `Term`
    /// (gap-analysis **V1**: node-valued `sh:hasValue` / `sh:in`).
    TestConst(Term),
    /// `test(τ)` — value-type / facet membership.
    TestType(ValueType),
    /// `sh:nodeKind` (gap-analysis **K1**).
    TestKind(NodeKindSet),
    /// `closed(Q)` — `Q` is the set of *allowed* predicates; any triple with a
    /// predicate outside `Q` violates the shape.
    Closed(BTreeSet<NamedNode>),
    /// `eq(π, p)` — `sh:equals`.
    Eq(Path, NamedNode),
    /// `disj(π, p)` — `sh:disjoint`.
    Disj(Path, NamedNode),
    /// `sh:lessThan` (gap-analysis **O1**).
    Lt(Path, NamedNode),
    /// `sh:lessThanOrEquals` (gap-analysis **O1**).
    Le(Path, NamedNode),
    /// `sh:uniqueLang` (gap-analysis **L1**).
    UniqueLang(Path),
    /// `¬φ`.
    Not(ShapeId),
    /// `φ ∧ φ′ ∧ …`.
    And(Vec<ShapeId>),
    /// `φ ∨ φ′ ∨ …`.
    Or(Vec<ShapeId>),
    /// `∃≥min π.φ ∧ ∃≤max π.φ` — qualified counting over `path`, with the
    /// `qualifier` shape applied to each value node. Subsumes `sh:minCount`,
    /// `sh:maxCount`, `sh:qualifiedValueShape`, and (via `∀`/`∃` sugar)
    /// `sh:node`/`sh:property` nesting.
    Count {
        path: Path,
        min: Option<u64>,
        max: Option<u64>,
        qualifier: ShapeId,
    },
    /// Opaque SPARQL-based constraint (gap-analysis **AF-C**), evaluated by the
    /// engine rather than reasoned about algebraically.
    Sparql(SparqlConstraint),
}

/// Arena of interned shapes. References between shapes are [`ShapeId`] indices,
/// which makes cyclic (recursive) schemas and structural sharing representable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShapeArena {
    shapes: Vec<Shape>,
}

impl ShapeArena {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.shapes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }

    pub fn get(&self, id: ShapeId) -> &Shape {
        &self.shapes[id.0 as usize]
    }

    /// Intern a shape verbatim, returning its id. No deduplication (CSE is
    /// Layer 5) and no normalization — prefer the smart constructors below when
    /// building, and use this only for already-normalized shapes.
    pub fn insert(&mut self, shape: Shape) -> ShapeId {
        let id = ShapeId(self.shapes.len() as u32);
        self.shapes.push(shape);
        id
    }

    /// Reserve a slot (initialized to [`Shape::Pending`]) whose id can be
    /// referenced before its body is known, then filled with [`set`](Self::set).
    /// This is how cyclic shapes are constructed.
    pub fn reserve(&mut self) -> ShapeId {
        self.insert(Shape::Pending)
    }

    /// Overwrite a (typically reserved) slot. Bypasses normalization by design.
    pub fn set(&mut self, id: ShapeId, shape: Shape) {
        self.shapes[id.0 as usize] = shape;
    }

    // ---- smart constructors (light, always-sound simplifications) ----

    pub fn top(&mut self) -> ShapeId {
        self.insert(Shape::Top)
    }

    /// `false`, represented as `¬⊤`.
    pub fn bottom(&mut self) -> ShapeId {
        let t = self.insert(Shape::Top);
        self.insert(Shape::Not(t))
    }

    /// `¬φ`, collapsing the involution `¬¬φ = φ`.
    pub fn not(&mut self, inner: ShapeId) -> ShapeId {
        if let Shape::Not(x) = self.get(inner) {
            return *x;
        }
        self.insert(Shape::Not(inner))
    }

    /// `φ ∧ …`, flattening nested `And` and dropping `⊤`. Empty ⇒ `⊤`.
    pub fn and(&mut self, parts: Vec<ShapeId>) -> ShapeId {
        let mut flat = Vec::with_capacity(parts.len());
        for p in parts {
            match self.get(p) {
                Shape::Top => {}
                Shape::And(inner) => flat.extend(inner.iter().copied()),
                _ => flat.push(p),
            }
        }
        match flat.len() {
            0 => self.insert(Shape::Top),
            1 => flat[0],
            _ => self.insert(Shape::And(flat)),
        }
    }

    /// `φ ∨ …`, flattening nested `Or`. Empty ⇒ `false`.
    pub fn or(&mut self, parts: Vec<ShapeId>) -> ShapeId {
        let mut flat = Vec::with_capacity(parts.len());
        for p in parts {
            match self.get(p) {
                Shape::Or(inner) => flat.extend(inner.iter().copied()),
                _ => flat.push(p),
            }
        }
        match flat.len() {
            0 => self.bottom(),
            1 => flat[0],
            _ => self.insert(Shape::Or(flat)),
        }
    }

    /// `sh:xone` as `⋁ᵢ (φᵢ ∧ ⋀_{j≠i} ¬φⱼ)` (exactly one holds).
    pub fn xone(&mut self, parts: Vec<ShapeId>) -> ShapeId {
        let mut terms = Vec::with_capacity(parts.len());
        for (i, &pi) in parts.iter().enumerate() {
            let mut conj = Vec::with_capacity(parts.len());
            conj.push(pi);
            for (j, &pj) in parts.iter().enumerate() {
                if i != j {
                    let neg = self.not(pj);
                    conj.push(neg);
                }
            }
            let term = self.and(conj);
            terms.push(term);
        }
        self.or(terms)
    }

    /// Qualified count `∃≥min π.φ ∧ ∃≤max π.φ`. Collapses to `⊤` when the bounds
    /// impose nothing (`min ∈ {None, 0}` and no `max`), since `∃≥0 π.φ` always
    /// holds.
    pub fn count(
        &mut self,
        path: Path,
        min: Option<u64>,
        max: Option<u64>,
        qualifier: ShapeId,
    ) -> ShapeId {
        if max.is_none() && matches!(min, None | Some(0)) {
            return self.insert(Shape::Top);
        }
        self.insert(Shape::Count {
            path,
            min,
            max,
            qualifier,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_is_involution() {
        let mut a = ShapeArena::new();
        let t = a.top();
        let n = a.not(t);
        let nn = a.not(n);
        assert_eq!(nn, t);
    }

    #[test]
    fn and_flattens_and_drops_top() {
        let mut a = ShapeArena::new();
        let t = a.top();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let inner = a.and(vec![k, t]); // ⊤ dropped ⇒ just k
        assert_eq!(inner, k);
        let l = a.insert(Shape::TestKind(NodeKindSet::LITERAL));
        let outer = a.and(vec![inner, l]);
        match a.get(outer) {
            Shape::And(parts) => assert_eq!(parts, &vec![k, l]),
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn empty_or_is_bottom() {
        let mut a = ShapeArena::new();
        let b = a.or(vec![]);
        match a.get(b) {
            Shape::Not(inner) => assert!(matches!(a.get(*inner), Shape::Top)),
            other => panic!("expected ¬⊤, got {other:?}"),
        }
    }

    #[test]
    fn count_zero_lower_bound_is_top() {
        let mut a = ShapeArena::new();
        let t = a.top();
        let p = Path::Pred(NamedNode::new("http://ex/p").unwrap());
        let c = a.count(p, Some(0), None, t);
        assert!(matches!(a.get(c), Shape::Top));
    }

    #[test]
    fn recursive_shape_via_reserve_set() {
        // S := ∃≥1 ex:knows . S
        let mut a = ShapeArena::new();
        let knows = NamedNode::new("http://ex/knows").unwrap();
        let s = a.reserve();
        a.set(
            s,
            Shape::Count {
                path: Path::Pred(knows),
                min: Some(1),
                max: None,
                qualifier: s,
            },
        );
        match a.get(s) {
            Shape::Count { qualifier, .. } => assert_eq!(*qualifier, s),
            other => panic!("expected self-referential Count, got {other:?}"),
        }
    }
}
