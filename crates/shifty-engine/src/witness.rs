//! The witnessing evaluator (`docs/06-repair.md` §5) — the structured, lossless
//! sibling of `explain`. For a focus node that fails a
//! statement it returns a [`Failure`]: the failed sub-DAG of `φ`, pruned to
//! exactly what did not hold, with the structural gap at each node. Its dual,
//! [`Satisfaction`], records *why* a shape currently holds, so a `Not(φ)` failure can
//! be repaired by breaking `φ`. One dispatcher produces both tagged polarities;
//! they are mutually recursive only in their data grammar through `Not`.
//!
//! This is the input to repair synthesis; it makes no repair decisions. It reuses
//! the `ShapeEvaluator` satisfaction oracle (`holds`) and its gfp back-edge
//! guard verbatim, so witnessing agrees with validation by construction.
//!
//! `evaluate_node` is the single polarity dispatcher. A failed conjunction
//! retains its failing children, a failed disjunction retains every branch that
//! failed, and qualified counts partition reached values into matches and
//! rejected candidates. `Not` crosses between [`Failure`] and [`Satisfaction`],
//! which is why both forms are represented explicitly instead of encoding one
//! as an inverted Boolean annotation.
//!
//! The result is intentionally a derivation tree over a shared shape graph, not
//! a general-purpose proof of every possible path. Each reached value carries a
//! concrete [`PathSupport`] certificate sufficient to explain why it was seen.
//! This makes an evidence run deterministic and useful to repair synthesis while
//! keeping the expensive "all alternative routes" problem outside this module's
//! contract.

use crate::frozen::FrozenIndexedDataset;
use crate::path::{PathBackend, node_of, pred, succ};
use crate::sparql::{SparqlDiagnostic, SparqlExecutor};
use crate::validate::{NonStratifiable, ShapeEvaluator, focus_nodes_with, uses_shapes_graph};
use oxrdf::{Graph, NamedNode, Term, Triple};
use serde::{Deserialize, Serialize};
use shifty_algebra::{ConstraintKind, Path, Schema, Selector, Shape, ShapeArena, ShapeId};
use shifty_opt::analyze;
use std::collections::{BTreeSet, HashSet, VecDeque};

/// Why one focus node failed one statement: the failed sub-structure of `φ`,
/// pruned to exactly the parts that did not hold. The input to repair synthesis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct FocusWitness {
    pub focus: Term,
    /// Index of the violated `(selector, shape)` statement in the schema.
    pub statement: usize,
    pub failure: Failure,
}

/// The relational (pairwise) leaf constraints — distinct from value-type atoms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum RelKind {
    Eq,
    Disj,
    Lt,
    Le,
    UniqueLang,
}

/// One concrete positive certificate that a node is a `π`-successor of its
/// parent. Every [`Edge`](PathSupport::Edge) is present in the evaluation graph.
///
/// This is deliberately *not* a complete deletion cut: an alternative or
/// cyclic path may have additional routes that are not represented here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(tag = "type", content = "details", rename_all = "snake_case")]
pub enum PathSupport {
    /// Reflexive (`Id`): the node reached itself; nothing to cut.
    Empty,
    /// A single triple edge used by this certificate.
    Edge(Triple),
    /// A chain (`Seq`/`Star` expansion) used by this certificate.
    Chain(Vec<PathSupport>),
    /// Multiple certificate branches when a caller explicitly retains them.
    /// The current path fold is allowed to return only the first successful
    /// syntactic alternative.
    Alt(Vec<PathSupport>),
}

/// A reached value that satisfied a qualified count's nested shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct QualifiedMatch {
    pub value: Term,
    pub path_support: PathSupport,
    pub satisfaction: Box<Satisfaction>,
}

/// A reached near-match that failed a qualified count's nested shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RejectedCandidate {
    pub value: Term,
    pub path_support: PathSupport,
    pub failure: Box<Failure>,
}

/// The failed sub-structure of `φ` (additive direction: what to *add*).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(tag = "type", content = "details", rename_all = "snake_case")]
pub enum Failure {
    /// A value-type leaf failed at `node` (TestConst / TestType / TestKind).
    /// `produced_by` names the triples that made `node` a value (`Some` for a
    /// value-scoped atom, `None` for one on the focus itself).
    Atom {
        shape: ShapeId,
        node: Term,
        observed: Vec<Term>,
        expected: Box<Shape>,
        reached_by: Path,
        produced_by: Option<PathSupport>,
    },
    /// A relational leaf failed (Eq / Disj / Lt / Le / UniqueLang): `lhs`/`rhs`
    /// carry the two compared value-sets with their support, `offending` the
    /// witnessing pairs/members.
    Relational {
        shape: ShapeId,
        node: Term,
        kind: RelKind,
        lhs: Vec<(Term, PathSupport)>,
        rhs: Vec<(Term, PathSupport)>,
        offending: Vec<(Term, Term)>,
    },
    /// `closed(Q)` failed: these (predicate, object) pairs are not allowed.
    Closed {
        shape: ShapeId,
        node: Term,
        offenders: Vec<(NamedNode, Term)>,
    },
    /// `¬φ` failed because `φ` holds at `node` — it must be falsified.
    Not {
        shape: ShapeId,
        node: Term,
        inner: Box<Satisfaction>,
    },
    /// Conjunction: every listed child failed and ALL must be repaired.
    All {
        shape: ShapeId,
        node: Term,
        failed: Vec<Failure>,
    },
    /// Disjunction: no branch held; repairing ANY ONE suffices.
    Any {
        shape: ShapeId,
        node: Term,
        branches: Vec<Failure>,
    },
    /// `∃≥min π.q` under-satisfied: `have` values match, `min` required.
    ///
    /// `sibling_qualifiers` collects the inner shapes of every `∀π.φ` universal
    /// (encoded as `∃≤0 π.¬φ`) on this same path that is *conjoined above* this
    /// count — including ones from sibling property shapes, i.e. a different `And`
    /// node. Those universals are vacuously satisfied when their path is empty, so
    /// they never witness as failures themselves, yet any value added to satisfy
    /// this `CountLow` must also conform to each of them.
    CountLow {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        have: u64,
        min: u64,
        qualifying_matches: Vec<QualifiedMatch>,
        rejected_candidates: Vec<RejectedCandidate>,
        sibling_qualifiers: Vec<ShapeId>,
    },
    /// `∃≤max π.q` over-satisfied. `matched` pairs each counted value with its
    /// support (so deletion cuts the right edges for `Seq`/`Star` paths).
    /// `per_value` is populated only for the `∀`-encoding (`∃≤0 π.¬inner`).
    CountHigh {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        matched: Vec<(Term, PathSupport)>,
        max: u64,
        excess_values: Vec<(Term, PathSupport)>,
        per_value: Vec<(Term, Failure)>,
    },
    /// Opaque SPARQL — no algebraic witness.
    Opaque {
        shape: ShapeId,
        node: Term,
        messages: Vec<Term>,
        diagnostic: Option<SparqlDiagnostic>,
    },
}

/// Compatibility spelling retained for the repair-facing API. New public code
/// should use [`Failure`], the vocabulary shared with [`Evidence`].
pub type Witness = Failure;

/// Why `φ` currently *holds* at a node (deletive direction: what to *delete*).
/// The dual of [`Failure`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(tag = "type", content = "details", rename_all = "snake_case")]
pub enum Satisfaction {
    /// `⊤` — vacuously true; no graph edit falsifies it.
    Irrefutable { shape: ShapeId },
    /// A value-type leaf holds at `node`; `produced_by` names the edges to cut.
    Atom {
        shape: ShapeId,
        node: Term,
        observed: Vec<Term>,
        expected: Box<Shape>,
        reached_by: Path,
        produced_by: PathSupport,
    },
    /// Conjunction holds because ALL children hold ⟹ break ANY ONE.
    AllHeld {
        shape: ShapeId,
        node: Term,
        children: Vec<Satisfaction>,
    },
    /// Disjunction holds because these branches hold ⟹ break EVERY one.
    AnyHeld {
        shape: ShapeId,
        node: Term,
        satisfied: Vec<Satisfaction>,
    },
    /// `∃[min..max] π.q` holds. `matches` carries each counted value with its
    /// concrete path certificate and q-satisfaction trace.
    CountHeld {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        matches: Vec<(Term, PathSupport, Satisfaction)>,
        observed_count: u64,
        min: Option<u64>,
        max: Option<u64>,
    },
    /// Universal encoding `∃≤0 π.¬q` holds because every reached value
    /// satisfies `q`. Each checked value retains its path certificate and
    /// satisfaction trace, including coinductive back-edges.
    ForAllHeld {
        shape: ShapeId,
        node: Term,
        path: Path,
        qualifier: ShapeId,
        values: Vec<(Term, PathSupport, Satisfaction)>,
    },
    /// `¬φ` holds because `φ` fails ⟹ make `φ` hold. Flips to the additive side.
    NotHeld {
        shape: ShapeId,
        node: Term,
        inner_fails: Box<Failure>,
    },
    /// Holds but cannot be falsified by data deletion in scope (closed / relational
    /// / opaque SPARQL).
    Blocked {
        shape: ShapeId,
        node: Term,
        reason: BlockReason,
    },
    /// Support reached only through a gfp back-edge: coinductively assumed true,
    /// with no finite set of facts to delete.
    Coinductive { shape: ShapeId, node: Term },
}

/// Compatibility spelling retained while repair internals migrate to the
/// unified evidence vocabulary. New public code should use [`Satisfaction`].
pub type SatTrace = Satisfaction;

/// The applicable evidence polarity for one selected `(statement, focus)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(tag = "status", content = "evidence", rename_all = "snake_case")]
pub enum Evidence {
    #[serde(rename = "pass")]
    Satisfaction(Satisfaction),
    #[serde(rename = "fail")]
    Failure(Failure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationStatus {
    Pass,
    Fail,
}

/// Stable, polarity-aware discriminant for an evidence node.
///
/// This is the shared enumeration used when evidence must be named without
/// copying its variant payload, including repair-origin links and language
/// bindings. It is distinct from repair choice kinds and candidate enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Irrefutable,
    AtomHeld,
    AllHeld,
    AnyHeld,
    CountHeld,
    AllValuesHeld,
    NotHeld,
    Blocked,
    Coinductive,
    AtomFailed,
    RelationalFailed,
    ClosedFailed,
    NotFailed,
    AllFailed,
    AnyFailed,
    CountLow,
    CountHigh,
    Opaque,
}

impl EvidenceKind {
    pub fn status(self) -> EvaluationStatus {
        match self {
            Self::Irrefutable
            | Self::AtomHeld
            | Self::AllHeld
            | Self::AnyHeld
            | Self::CountHeld
            | Self::AllValuesHeld
            | Self::NotHeld
            | Self::Blocked
            | Self::Coinductive => EvaluationStatus::Pass,
            Self::AtomFailed
            | Self::RelationalFailed
            | Self::ClosedFailed
            | Self::NotFailed
            | Self::AllFailed
            | Self::AnyFailed
            | Self::CountLow
            | Self::CountHigh
            | Self::Opaque => EvaluationStatus::Fail,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Irrefutable => "irrefutable",
            Self::AtomHeld => "atom_held",
            Self::AllHeld => "all_held",
            Self::AnyHeld => "any_held",
            Self::CountHeld => "count_held",
            Self::AllValuesHeld => "all_values_held",
            Self::NotHeld => "not_held",
            Self::Blocked => "blocked",
            Self::Coinductive => "coinductive",
            Self::AtomFailed => "atom_failed",
            Self::RelationalFailed => "relational_failed",
            Self::ClosedFailed => "closed_failed",
            Self::NotFailed => "not_failed",
            Self::AllFailed => "all_failed",
            Self::AnyFailed => "any_failed",
            Self::CountLow => "count_low",
            Self::CountHigh => "count_high",
            Self::Opaque => "opaque",
        }
    }
}

impl Evidence {
    pub fn status(&self) -> EvaluationStatus {
        match self {
            Self::Satisfaction(_) => EvaluationStatus::Pass,
            Self::Failure(_) => EvaluationStatus::Fail,
        }
    }

    /// Typed pre-order traversal. Variant child order, path value order, and
    /// source conjunction/disjunction order are stable.
    pub fn walk(&self) -> Vec<EvidenceNodeRef<'_>> {
        let mut out = Vec::new();
        walk_evidence(self, &mut out);
        out
    }

    pub fn supporting_triples(&self) -> Vec<Triple> {
        let mut triples = Vec::new();
        collect_support(self, &mut triples);
        dedup_stable(triples)
    }

    /// Positive path certificates referenced by the evidence in traversal
    /// order, deduplicated by structural equality.
    pub fn path_supports(&self) -> Vec<PathSupport> {
        let mut supports = Vec::new();
        collect_path_supports(self, &mut supports);
        supports.into_iter().fold(Vec::new(), |mut out, value| {
            if !out.contains(&value) {
                out.push(value);
            }
            out
        })
    }

    pub fn matched_values(&self) -> Vec<Term> {
        let mut values = Vec::new();
        collect_matched(self, &mut values);
        dedup_stable(values)
    }

    /// The values of [`matched_values`](Self::matched_values), grouped by the
    /// path each was counted along. Groups appear in traversal order and values
    /// within a group in match order, both deduplicated by first occurrence; a
    /// path counted at more than one evidence node yields one group holding
    /// every value counted along it.
    ///
    /// This reads the structured match records directly, so callers need not
    /// parse `explain()` text or re-derive values from supporting triples.
    pub fn matched_values_by_path(&self) -> Vec<(Path, Vec<Term>)> {
        let mut pairs = Vec::new();
        collect_matched_by_path(self, &mut pairs);
        let mut out: Vec<(Path, Vec<Term>)> = Vec::new();
        for (path, value) in pairs {
            match out.iter_mut().find(|(seen, _)| seen == path) {
                Some((_, values)) => {
                    if !values.contains(value) {
                        values.push(value.clone());
                    }
                }
                None => out.push((path.clone(), vec![value.clone()])),
            }
        }
        out
    }

    /// The matched values counted along `path`, in match order and
    /// deduplicated — the single-path projection of
    /// [`matched_values_by_path`](Self::matched_values_by_path). Empty when the
    /// evidence counted nothing along `path`.
    pub fn values_for_path(&self, path: &Path) -> Vec<Term> {
        let mut pairs = Vec::new();
        collect_matched_by_path(self, &mut pairs);
        dedup_stable(
            pairs
                .into_iter()
                .filter(|(seen, _)| *seen == path)
                .map(|(_, value)| value.clone())
                .collect(),
        )
    }

    pub fn missing_obligations(&self) -> Vec<MissingObligation> {
        let mut out = Vec::new();
        collect_missing(self, &mut out);
        out
    }

    pub fn offending_values(&self) -> Vec<Term> {
        let mut values = Vec::new();
        collect_offending(self, &mut values);
        dedup_stable(values)
    }

    pub fn source_constraints(&self) -> Vec<ShapeId> {
        dedup_stable(
            self.walk()
                .into_iter()
                .map(EvidenceNodeRef::constraint_id)
                .collect(),
        )
    }

    pub fn explain(&self) -> String {
        self.walk()
            .into_iter()
            .enumerate()
            .map(|(index, node)| format!("{index}: {} @{}", node.kind(), node.constraint_id().0))
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum EvidenceNodeRef<'a> {
    Satisfaction(&'a SatTrace),
    Failure(&'a Witness),
}

impl<'a> EvidenceNodeRef<'a> {
    pub fn constraint_id(self) -> ShapeId {
        match self {
            Self::Satisfaction(value) => satisfaction_constraint_id(value),
            Self::Failure(value) => failure_constraint_id(value),
        }
    }

    /// The node this judgment is about, completing the `(constraint, node)`
    /// address the shape memo (`validate.rs`) is keyed by.
    ///
    /// `None` only for [`SatTrace::Irrefutable`], which is `⊤` and so is about
    /// no node at all.
    pub fn node(self) -> Option<&'a Term> {
        match self {
            Self::Satisfaction(value) => satisfaction_node(value),
            Self::Failure(value) => Some(failure_node(value)),
        }
    }

    pub fn status(self) -> EvaluationStatus {
        self.evidence_kind().status()
    }

    pub fn evidence_kind(self) -> EvidenceKind {
        match self {
            Self::Satisfaction(value) => match value {
                SatTrace::Irrefutable { .. } => EvidenceKind::Irrefutable,
                SatTrace::Atom { .. } => EvidenceKind::AtomHeld,
                SatTrace::AllHeld { .. } => EvidenceKind::AllHeld,
                SatTrace::AnyHeld { .. } => EvidenceKind::AnyHeld,
                SatTrace::CountHeld { .. } => EvidenceKind::CountHeld,
                SatTrace::ForAllHeld { .. } => EvidenceKind::AllValuesHeld,
                SatTrace::NotHeld { .. } => EvidenceKind::NotHeld,
                SatTrace::Blocked { .. } => EvidenceKind::Blocked,
                SatTrace::Coinductive { .. } => EvidenceKind::Coinductive,
            },
            Self::Failure(value) => match value {
                Witness::Atom { .. } => EvidenceKind::AtomFailed,
                Witness::Relational { .. } => EvidenceKind::RelationalFailed,
                Witness::Closed { .. } => EvidenceKind::ClosedFailed,
                Witness::Not { .. } => EvidenceKind::NotFailed,
                Witness::All { .. } => EvidenceKind::AllFailed,
                Witness::Any { .. } => EvidenceKind::AnyFailed,
                Witness::CountLow { .. } => EvidenceKind::CountLow,
                Witness::CountHigh { .. } => EvidenceKind::CountHigh,
                Witness::Opaque { .. } => EvidenceKind::Opaque,
            },
        }
    }

    pub fn kind(self) -> &'static str {
        self.evidence_kind().as_str()
    }

    /// Immediate evidence children in stable semantic order.
    ///
    /// This is the shared grammar of failure and satisfaction evidence. `Not`
    /// crosses polarity; qualified counts can contain both satisfaction traces
    /// for matches and failure witnesses for rejected candidates. Callers that
    /// only need structure should use this instead of maintaining another
    /// variant-specific recursive walk.
    pub fn children(self) -> Vec<Self> {
        let mut out = Vec::new();
        self.for_each_child(|child| out.push(child));
        out
    }

    fn for_each_child(self, mut visit: impl FnMut(Self)) {
        match self {
            Self::Failure(value) => match value {
                Witness::Not { inner, .. } => visit(Self::Satisfaction(inner)),
                Witness::All { failed, .. } => {
                    failed.iter().for_each(|child| visit(Self::Failure(child)))
                }
                Witness::Any { branches, .. } => branches
                    .iter()
                    .for_each(|child| visit(Self::Failure(child))),
                Witness::CountLow {
                    qualifying_matches,
                    rejected_candidates,
                    ..
                } => {
                    qualifying_matches.iter().for_each(|item| {
                        visit(Self::Satisfaction(&item.satisfaction));
                    });
                    rejected_candidates
                        .iter()
                        .for_each(|item| visit(Self::Failure(&item.failure)));
                }
                Witness::CountHigh { per_value, .. } => per_value
                    .iter()
                    .for_each(|(_, child)| visit(Self::Failure(child))),
                Witness::Atom { .. }
                | Witness::Relational { .. }
                | Witness::Closed { .. }
                | Witness::Opaque { .. } => {}
            },
            Self::Satisfaction(value) => match value {
                SatTrace::AllHeld { children, .. } => children
                    .iter()
                    .for_each(|child| visit(Self::Satisfaction(child))),
                SatTrace::AnyHeld { satisfied, .. } => satisfied
                    .iter()
                    .for_each(|child| visit(Self::Satisfaction(child))),
                SatTrace::CountHeld { matches, .. } => matches
                    .iter()
                    .for_each(|(_, _, child)| visit(Self::Satisfaction(child))),
                SatTrace::ForAllHeld { values, .. } => values
                    .iter()
                    .for_each(|(_, _, child)| visit(Self::Satisfaction(child))),
                SatTrace::NotHeld { inner_fails, .. } => visit(Self::Failure(inner_fails)),
                SatTrace::Irrefutable { .. }
                | SatTrace::Atom { .. }
                | SatTrace::Blocked { .. }
                | SatTrace::Coinductive { .. } => {}
            },
        }
    }
}

/// One cardinality deficit: `node` has `observed_count` values along `path`
/// satisfying `qualifier`, and needs `required_count`.
///
/// Everything needed to describe the missing edge is here, so a driver never
/// has to read `explain()` to learn what would close the gap. `node` is not
/// always the focus — a count nested inside a rejected candidate reports its own
/// node — so filter on it when you mean deficits on the focus itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct MissingObligation {
    pub constraint_id: ShapeId,
    /// The node the deficit is about.
    pub node: Term,
    /// The path its values were counted along.
    pub path: Path,
    /// The shape each counted value must satisfy — what an added value has to
    /// conform to for the count to move.
    pub qualifier: ShapeId,
    pub observed_count: u64,
    pub required_count: u64,
    pub missing: u64,
}

fn dedup_stable<T: Clone + Eq + std::hash::Hash>(values: Vec<T>) -> Vec<T> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn failure_constraint_id(value: &Witness) -> ShapeId {
    match value {
        Witness::Atom { shape, .. }
        | Witness::Relational { shape, .. }
        | Witness::Closed { shape, .. }
        | Witness::Not { shape, .. }
        | Witness::All { shape, .. }
        | Witness::Any { shape, .. }
        | Witness::CountLow { shape, .. }
        | Witness::CountHigh { shape, .. }
        | Witness::Opaque { shape, .. } => *shape,
    }
}

fn satisfaction_constraint_id(value: &SatTrace) -> ShapeId {
    match value {
        SatTrace::Irrefutable { shape }
        | SatTrace::Atom { shape, .. }
        | SatTrace::AllHeld { shape, .. }
        | SatTrace::AnyHeld { shape, .. }
        | SatTrace::CountHeld { shape, .. }
        | SatTrace::ForAllHeld { shape, .. }
        | SatTrace::NotHeld { shape, .. }
        | SatTrace::Blocked { shape, .. }
        | SatTrace::Coinductive { shape, .. } => *shape,
    }
}

fn failure_node(value: &Witness) -> &Term {
    match value {
        Witness::Atom { node, .. }
        | Witness::Relational { node, .. }
        | Witness::Closed { node, .. }
        | Witness::Not { node, .. }
        | Witness::All { node, .. }
        | Witness::Any { node, .. }
        | Witness::CountLow { node, .. }
        | Witness::CountHigh { node, .. }
        | Witness::Opaque { node, .. } => node,
    }
}

fn satisfaction_node(value: &SatTrace) -> Option<&Term> {
    match value {
        SatTrace::Irrefutable { .. } => None,
        SatTrace::Atom { node, .. }
        | SatTrace::AllHeld { node, .. }
        | SatTrace::AnyHeld { node, .. }
        | SatTrace::CountHeld { node, .. }
        | SatTrace::ForAllHeld { node, .. }
        | SatTrace::NotHeld { node, .. }
        | SatTrace::Blocked { node, .. }
        | SatTrace::Coinductive { node, .. } => Some(node),
    }
}

fn walk_evidence<'a>(value: &'a Evidence, out: &mut Vec<EvidenceNodeRef<'a>>) {
    let root = match value {
        Evidence::Satisfaction(value) => EvidenceNodeRef::Satisfaction(value),
        Evidence::Failure(value) => EvidenceNodeRef::Failure(value),
    };
    walk_node(root, out);
}

fn walk_node<'a>(value: EvidenceNodeRef<'a>, out: &mut Vec<EvidenceNodeRef<'a>>) {
    out.push(value);
    value.for_each_child(|child| walk_node(child, out));
}

fn support_triples(value: &PathSupport, out: &mut Vec<Triple>) {
    match value {
        PathSupport::Empty => {}
        PathSupport::Edge(triple) => out.push(triple.clone()),
        PathSupport::Chain(children) | PathSupport::Alt(children) => {
            children
                .iter()
                .for_each(|child| support_triples(child, out));
        }
    }
}

fn collect_support(value: &Evidence, out: &mut Vec<Triple>) {
    for node in value.walk() {
        match node {
            EvidenceNodeRef::Satisfaction(SatTrace::Atom { produced_by, .. }) => {
                support_triples(produced_by, out);
            }
            EvidenceNodeRef::Satisfaction(SatTrace::CountHeld { matches, .. }) => matches
                .iter()
                .for_each(|(_, support, _)| support_triples(support, out)),
            EvidenceNodeRef::Satisfaction(SatTrace::ForAllHeld { values, .. }) => values
                .iter()
                .for_each(|(_, support, _)| support_triples(support, out)),
            EvidenceNodeRef::Failure(Witness::Atom {
                produced_by: Some(support),
                ..
            }) => support_triples(support, out),
            EvidenceNodeRef::Failure(Witness::Relational { lhs, rhs, .. }) => lhs
                .iter()
                .chain(rhs)
                .for_each(|(_, support)| support_triples(support, out)),
            EvidenceNodeRef::Failure(Witness::CountLow {
                qualifying_matches,
                rejected_candidates,
                ..
            }) => {
                qualifying_matches
                    .iter()
                    .for_each(|item| support_triples(&item.path_support, out));
                rejected_candidates
                    .iter()
                    .for_each(|item| support_triples(&item.path_support, out));
            }
            EvidenceNodeRef::Failure(Witness::CountHigh { matched, .. }) => matched
                .iter()
                .for_each(|(_, support)| support_triples(support, out)),
            _ => {}
        }
    }
}

fn collect_path_supports(value: &Evidence, out: &mut Vec<PathSupport>) {
    for node in value.walk() {
        match node {
            EvidenceNodeRef::Satisfaction(SatTrace::Atom { produced_by, .. }) => {
                out.push(produced_by.clone());
            }
            EvidenceNodeRef::Satisfaction(SatTrace::CountHeld { matches, .. }) => {
                out.extend(matches.iter().map(|(_, support, _)| support.clone()));
            }
            EvidenceNodeRef::Satisfaction(SatTrace::ForAllHeld { values, .. }) => {
                out.extend(values.iter().map(|(_, support, _)| support.clone()));
            }
            EvidenceNodeRef::Failure(Witness::Atom {
                produced_by: Some(support),
                ..
            }) => out.push(support.clone()),
            EvidenceNodeRef::Failure(Witness::Relational { lhs, rhs, .. }) => {
                out.extend(lhs.iter().chain(rhs).map(|(_, support)| support.clone()))
            }
            EvidenceNodeRef::Failure(Witness::CountLow {
                qualifying_matches,
                rejected_candidates,
                ..
            }) => {
                out.extend(
                    qualifying_matches
                        .iter()
                        .map(|item| item.path_support.clone()),
                );
                out.extend(
                    rejected_candidates
                        .iter()
                        .map(|item| item.path_support.clone()),
                );
            }
            EvidenceNodeRef::Failure(Witness::CountHigh { matched, .. }) => {
                out.extend(matched.iter().map(|(_, support)| support.clone()));
            }
            _ => {}
        }
    }
}

fn collect_matched(value: &Evidence, out: &mut Vec<Term>) {
    for node in value.walk() {
        match node {
            EvidenceNodeRef::Satisfaction(SatTrace::CountHeld { matches, .. }) => {
                out.extend(matches.iter().map(|(value, _, _)| value.clone()));
            }
            EvidenceNodeRef::Satisfaction(SatTrace::ForAllHeld { values, .. }) => {
                out.extend(values.iter().map(|(value, _, _)| value.clone()));
            }
            EvidenceNodeRef::Failure(Witness::CountLow {
                qualifying_matches, ..
            }) => out.extend(qualifying_matches.iter().map(|item| item.value.clone())),
            EvidenceNodeRef::Failure(Witness::CountHigh { matched, .. }) => {
                out.extend(matched.iter().map(|(value, _)| value.clone()));
            }
            _ => {}
        }
    }
}

/// The same match records [`collect_matched`] reads, each paired with the path
/// its containing node counted along.
fn collect_matched_by_path<'a>(value: &'a Evidence, out: &mut Vec<(&'a Path, &'a Term)>) {
    for node in value.walk() {
        match node {
            EvidenceNodeRef::Satisfaction(SatTrace::CountHeld { path, matches, .. }) => {
                out.extend(matches.iter().map(|(value, _, _)| (path, value)));
            }
            EvidenceNodeRef::Satisfaction(SatTrace::ForAllHeld { path, values, .. }) => {
                out.extend(values.iter().map(|(value, _, _)| (path, value)));
            }
            EvidenceNodeRef::Failure(Witness::CountLow {
                path,
                qualifying_matches,
                ..
            }) => out.extend(qualifying_matches.iter().map(|item| (path, &item.value))),
            EvidenceNodeRef::Failure(Witness::CountHigh { path, matched, .. }) => {
                out.extend(matched.iter().map(|(value, _)| (path, value)));
            }
            _ => {}
        }
    }
}

fn collect_missing(value: &Evidence, out: &mut Vec<MissingObligation>) {
    for node in value.walk() {
        if let EvidenceNodeRef::Failure(Witness::CountLow {
            shape,
            node,
            path,
            qualifier,
            have,
            min,
            ..
        }) = node
        {
            out.push(MissingObligation {
                constraint_id: *shape,
                node: node.clone(),
                path: path.clone(),
                qualifier: *qualifier,
                observed_count: *have,
                required_count: *min,
                missing: min - have,
            });
        }
    }
}

fn collect_offending(value: &Evidence, out: &mut Vec<Term>) {
    for node in value.walk() {
        match node {
            EvidenceNodeRef::Failure(Witness::Atom { node, .. }) => out.push(node.clone()),
            EvidenceNodeRef::Failure(Witness::Closed { offenders, .. }) => {
                out.extend(offenders.iter().map(|(_, value)| value.clone()));
            }
            EvidenceNodeRef::Failure(Witness::Relational { offending, .. }) => {
                out.extend(
                    offending
                        .iter()
                        .flat_map(|(left, right)| [left.clone(), right.clone()]),
                );
            }
            EvidenceNodeRef::Failure(Witness::CountLow {
                rejected_candidates,
                ..
            }) => out.extend(rejected_candidates.iter().map(|item| item.value.clone())),
            EvidenceNodeRef::Failure(Witness::CountHigh { excess_values, .. }) => {
                out.extend(excess_values.iter().map(|(value, _)| value.clone()));
            }
            _ => {}
        }
    }
}

/// A serializable catalog entry. Child shape ids in `constraint` resolve in
/// the containing source or normalized catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintRecord {
    pub id: ShapeId,
    pub constraint_kind: ConstraintKind,
    pub constraint: Shape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintCatalog {
    pub source: Vec<ConstraintRecord>,
    pub normalized: Vec<ConstraintRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceSummary {
    pub constraint_id: ShapeId,
    pub constraint_kind: ConstraintKind,
    pub status: EvaluationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildEvaluation {
    pub source_constraint_ref: ShapeId,
    pub normalized_constraint_ref: Option<ShapeId>,
    pub status: EvaluationStatus,
    pub evidence_summary: EvidenceSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EvaluationProgress {
    pub evaluated_children: Vec<ChildEvaluation>,
}

/// One selected focus under one authored statement. The evidence enum makes an
/// inconsistent status/evidence pairing unrepresentable; `status()` is derived.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusEvaluation {
    pub focus: Term,
    pub evidence: Evidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<EvaluationProgress>,
}

impl FocusEvaluation {
    pub fn status(&self) -> EvaluationStatus {
        self.evidence.status()
    }

    pub fn explain(&self) -> String {
        self.evidence.explain()
    }
}

/// Results are grouped by authored statement, including statements whose
/// selector chooses no focus nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementEvaluation {
    pub source_statement_id: usize,
    pub normalized_statement_id: Option<usize>,
    pub source_constraint_id: ShapeId,
    pub normalized_constraint_id: Option<ShapeId>,
    pub constraint_kind: ConstraintKind,
    pub constraint: Shape,
    pub selector: Selector,
    pub selected_foci: Vec<FocusEvaluation>,
}

impl StatementEvaluation {
    /// Authored constraint ids referenced by this statement and its immediate
    /// progress views, in first-occurrence order.
    pub fn source_constraints(&self) -> Vec<ShapeId> {
        let mut values = vec![self.source_constraint_id];
        for focus in &self.selected_foci {
            if let Some(progress) = &focus.progress {
                values.extend(
                    progress
                        .evaluated_children
                        .iter()
                        .map(|child| child.source_constraint_ref),
                );
            }
        }
        dedup_stable(values)
    }
}

/// The complete statement-oriented coverage horizon for one validation run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRun {
    pub conforms: bool,
    pub constraints: ConstraintCatalog,
    pub statements: Vec<StatementEvaluation>,
}

impl EvidenceRun {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    pub fn walk(&self) -> Vec<EvidenceNodeRef<'_>> {
        self.statements
            .iter()
            .flat_map(|statement| &statement.selected_foci)
            .flat_map(|focus| focus.evidence.walk())
            .collect()
    }
}

/// Why a holding shape admits no data-deletion repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum BlockReason {
    OpaqueSparql,
    /// Falsifying `closed(Q)` would need *adding* a disallowed predicate.
    ClosedNeedsAdd,
    /// Relational falsification is not synthesized in this cut.
    Unsupported,
}

type Stack = HashSet<(ShapeId, Term)>;

/// Strat-check and build the SPARQL executor shared by every witnessing entry
/// point. The executor's frozen dataset is built over `context` — the graph that
/// paths, class hierarchy, and SPARQL read; focus discovery is the caller's job
/// and reads the (possibly narrower) data graph. The caller derives
/// `backend`/`ShapeEvaluator` from the returned executor — those borrow it, so
/// they can't be bundled in here.
fn prepare(context: &Graph, schema: &Schema) -> Result<SparqlExecutor, NonStratifiable> {
    let strat = analyze(&schema.arena);
    if !strat.stratifiable {
        let components = strat
            .strata
            .iter()
            .filter(|s| !s.stratifiable)
            .map(|s| s.shapes.clone())
            .collect();
        return Err(NonStratifiable { components });
    }

    let uses_shapes = uses_shapes_graph(&schema.arena);
    let frozen = if uses_shapes {
        FrozenIndexedDataset::from_graphs(context, context)
    } else {
        FrozenIndexedDataset::from_graph(context)
    };
    Ok(SparqlExecutor::from_frozen(frozen, uses_shapes))
}

/// Witness every `(focus, statement)` that fails, mirroring `validate`'s driver.
/// Focus discovery uses `data`; paths, class hierarchy, and SPARQL are evaluated
/// against `context`, which should contain `data` (pass `data` again when there
/// is no separate shapes graph; for split inputs `context = data ∪ shapes`). The
/// witnessing dual of [`crate::validate_with_context`].
pub fn witness_violations(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
) -> Result<Vec<FocusWitness>, NonStratifiable> {
    let sparql = prepare(context, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut out = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = Stack::new();
            if let Some(failure) = witness(
                &mut evaluator,
                &v,
                st.shape,
                &Path::Id,
                None,
                &[],
                &mut stack,
            ) {
                out.push(FocusWitness {
                    focus: v,
                    statement: i,
                    failure,
                });
            }
        }
    }
    Ok(out)
}

/// The arena slot a named shape IRI refers to, if the schema names one. `iri` is
/// matched bare (no angle brackets), the form stored in [`Schema::names`].
pub fn shape_id_for_iri(schema: &Schema, iri: &str) -> Option<ShapeId> {
    schema
        .names
        .iter()
        .find_map(|(id, names)| names.iter().any(|name| name == iri).then_some(*id))
}

/// Witness only the `(focus, statement)` violations whose statement targets
/// `shape` — the shape-scoped sibling of [`witness_violations`]. Returns the
/// *failing* foci with their [`Witness`] trees; passing foci are the domain of
/// [`satisfy_shape`]. Use [`shape_id_for_iri`] to resolve an IRI to its
/// `ShapeId`.
pub fn witness_shape(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    shape: ShapeId,
) -> Result<Vec<FocusWitness>, NonStratifiable> {
    let sparql = prepare(context, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut out = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        if st.shape != shape {
            continue;
        }
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = Stack::new();
            if let Some(failure) = witness(
                &mut evaluator,
                &v,
                st.shape,
                &Path::Id,
                None,
                &[],
                &mut stack,
            ) {
                out.push(FocusWitness {
                    focus: v,
                    statement: i,
                    failure,
                });
            }
        }
    }
    Ok(out)
}

/// Why one focus node *satisfies* one statement: the [`SatTrace`] recording why
/// `φ` holds, including the values matched along each checked path. The
/// satisfaction-side dual of [`FocusWitness`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusSat {
    pub focus: Term,
    /// Index of the satisfied `(selector, shape)` statement in the schema.
    pub statement: usize,
    pub trace: SatTrace,
}

/// Trace every `(focus, statement)` that *holds* for a statement targeting
/// `shape` — the dual of [`witness_shape`]. Each [`FocusSat`] carries why the
/// node conforms, down to the values matched along every checked path (see
/// [`SatTrace`]). Use [`shape_id_for_iri`] to resolve an IRI to its `ShapeId`.
pub fn satisfy_shape(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    shape: ShapeId,
) -> Result<Vec<FocusSat>, NonStratifiable> {
    let sparql = prepare(context, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut out = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        if st.shape != shape {
            continue;
        }
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = Stack::new();
            if let Some(trace) =
                sat_trace(&mut evaluator, &v, st.shape, &Path::Id, None, &mut stack)
            {
                out.push(FocusSat {
                    focus: v,
                    statement: i,
                    trace,
                });
            }
        }
    }
    Ok(out)
}

/// Witness one specific `node` against one specific `shape` (by id) — the building
/// block for repairing a `ConformsTo` hole: bind it to a node, then witness that
/// node against the sub-shape and synthesize its repair. Returns `Ok(None)` when
/// the node already conforms (nothing to build), `Ok(Some(_))` otherwise. Paths
/// and class hierarchy are evaluated over `context`; there is no focus discovery
/// here (`node` is given), so unlike the other entry points this takes no
/// separate data graph.
///
/// The `statement` field of the returned [`FocusWitness`] is a sentinel
/// (`usize::MAX`): this is not a top-level statement. Traceable synthesis maps
/// it to an absent statement origin rather than exposing the sentinel.
pub fn witness_node(
    context: &Graph,
    schema: &Schema,
    node: &Term,
    shape: ShapeId,
) -> Result<Option<FocusWitness>, NonStratifiable> {
    let sparql = prepare(context, schema)?;
    let backend = sparql
        .frozen()
        .expect("witness executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);

    let mut stack = Stack::new();
    Ok(witness(
        &mut evaluator,
        node,
        shape,
        &Path::Id,
        None,
        &[],
        &mut stack,
    )
    .map(|failure| FocusWitness {
        focus: node.clone(),
        statement: usize::MAX,
        failure,
    }))
}

/// Reasons `φ` (slot `id`) fails at `node`. `None` ⟺ it holds (incl. on a gfp
/// back-edge). `reached_by` is the structured path from the focus; `produced_by`
/// is how `node` was reached from its parent value (for replace-in-place).
///
/// `scope` carries the universals `∀π.φ` (encoded `∃≤0 π.¬φ`) conjoined *above*
/// this point — every `And` ancestor contributes its universal children. They
/// hold vacuously when their path is empty, so they never witness as failures of
/// their own; a `CountLow` on the same path attaches them as `sibling_qualifiers`
/// so a value built to satisfy the count also satisfies them (`∀` and a sibling
/// count may live in *different* property shapes, hence different `And` nodes).
fn witness(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    reached_by: &Path,
    produced_by: Option<&PathSupport>,
    scope: &[(Path, ShapeId)],
    stack: &mut Stack,
) -> Option<Witness> {
    match evaluate_node(eval, node, id, reached_by, produced_by, scope, stack) {
        Evidence::Failure(value) => Some(value),
        Evidence::Satisfaction(_) => None,
    }
}

/// Peel transparent `sh:severity` (`Annotated`) wrappers to the shape beneath.
fn peel_annotated(arena: &ShapeArena, mut id: ShapeId) -> ShapeId {
    while let Shape::Annotated { shape, .. } = arena.get(id) {
        id = *shape;
    }
    id
}

/// If `id` is a universal `∀π.φ` (encoded `∃≤0 π.¬φ`, modulo `Annotated`
/// wrappers), its `(π, φ)`. `None` for any other shape. This is what an `And`
/// contributes to the scope its children witness under: such universals hold
/// vacuously on an empty path, yet any value later added there must satisfy `φ`.
fn as_universal(arena: &ShapeArena, id: ShapeId) -> Option<(Path, ShapeId)> {
    let Shape::Count {
        path,
        max: Some(0),
        qualifier,
        ..
    } = arena.get(peel_annotated(arena, id)).clone()
    else {
        return None;
    };
    match arena.get(peel_annotated(arena, qualifier)) {
        Shape::Not(inner) => Some((path, *inner)),
        _ => None,
    }
}

fn relational_witness(
    eval: &ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    shape: &Shape,
) -> Witness {
    let g = eval.backend();
    let (kind, lpath, rpred) = match shape {
        Shape::Eq(p, q) => (RelKind::Eq, p.clone(), Some(q.clone())),
        Shape::Disj(p, q) => (RelKind::Disj, p.clone(), Some(q.clone())),
        Shape::Lt(p, q) => (RelKind::Lt, p.clone(), Some(q.clone())),
        Shape::Le(p, q) => (RelKind::Le, p.clone(), Some(q.clone())),
        Shape::UniqueLang(p) => (RelKind::UniqueLang, p.clone(), None),
        _ => unreachable!("relational_witness on non-relational shape"),
    };
    let with_support = |g: &dyn PathBackend, p: &Path| -> Vec<(Term, PathSupport)> {
        succ_with_support(g, node, p)
    };
    let lhs = with_support(g, &lpath);
    let rhs = match &rpred {
        Some(q) => with_support(g, &Path::Pred(q.clone())),
        None => Vec::new(),
    };
    let offending = offending_pairs(kind, &lhs, &rhs);
    Witness::Relational {
        shape: id,
        node: node.clone(),
        kind,
        lhs,
        rhs,
        offending,
    }
}

/// The witnessing pairs/members for a failed relational constraint.
fn offending_pairs(
    kind: RelKind,
    lhs: &[(Term, PathSupport)],
    rhs: &[(Term, PathSupport)],
) -> Vec<(Term, Term)> {
    let lvals: Vec<&Term> = lhs.iter().map(|(v, _)| v).collect();
    let rvals: Vec<&Term> = rhs.iter().map(|(v, _)| v).collect();
    match kind {
        RelKind::Eq => lvals
            .iter()
            .filter(|v| !rvals.contains(v))
            .chain(rvals.iter().filter(|v| !lvals.contains(v)))
            .map(|v| ((*v).clone(), (*v).clone()))
            .collect(),
        RelKind::Disj => lvals
            .iter()
            .filter(|v| rvals.contains(v))
            .map(|v| ((*v).clone(), (*v).clone()))
            .collect(),
        RelKind::Lt | RelKind::Le => {
            // pairs (a, b) that fail a < b (resp. a ≤ b)
            let mut bad = Vec::new();
            for a in &lvals {
                for b in &rvals {
                    let ok = match crate::value::compare_terms(a, b) {
                        Some(std::cmp::Ordering::Less) => true,
                        Some(std::cmp::Ordering::Equal) => kind == RelKind::Le,
                        _ => false,
                    };
                    if !ok {
                        bad.push(((*a).clone(), (*b).clone()));
                    }
                }
            }
            bad
        }
        RelKind::UniqueLang => {
            // pairs of lhs values sharing a language tag
            let mut bad = Vec::new();
            for i in 0..lvals.len() {
                for j in (i + 1)..lvals.len() {
                    if let (Term::Literal(a), Term::Literal(b)) = (lvals[i], lvals[j])
                        && let (Some(la), Some(lb)) = (a.language(), b.language())
                        && la.eq_ignore_ascii_case(lb)
                    {
                        bad.push((lvals[i].clone(), lvals[j].clone()));
                    }
                }
            }
            bad
        }
    }
}

/// Support for why `φ` holds at `node`. `None` ⟺ it fails. The dual of [`witness`].
fn sat_trace(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    reached_by: &Path,
    produced_by: Option<&PathSupport>,
    stack: &mut Stack,
) -> Option<SatTrace> {
    match evaluate_node(eval, node, id, reached_by, produced_by, &[], stack) {
        Evidence::Satisfaction(value) => Some(value),
        Evidence::Failure(_) => None,
    }
}

/// Materialize both evidence polarities through one dispatcher. Composite
/// children and qualified-count candidates are evaluated once and partitioned
/// into the applicable canonical side; negation crosses sides directly.
fn evaluate_node(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    reached_by: &Path,
    produced_by: Option<&PathSupport>,
    scope: &[(Path, ShapeId)],
    stack: &mut Stack,
) -> Evidence {
    crate::profile::record_evidence_visit();
    let key = (id, node.clone());
    if !stack.insert(key.clone()) {
        return Evidence::Satisfaction(SatTrace::Coinductive {
            shape: id,
            node: node.clone(),
        });
    }

    let shape = eval.arena().get(id).clone();
    let evidence = match shape {
        Shape::Annotated { shape, .. } => {
            evaluate_node(eval, node, shape, reached_by, produced_by, scope, stack)
        }
        Shape::Top | Shape::Pending => Evidence::Satisfaction(SatTrace::Irrefutable { shape: id }),
        Shape::TestConst(_) | Shape::TestType(_) | Shape::TestKind(_) => {
            if eval.holds(node, id) {
                Evidence::Satisfaction(SatTrace::Atom {
                    shape: id,
                    node: node.clone(),
                    observed: vec![node.clone()],
                    expected: Box::new(eval.arena().get(id).clone()),
                    reached_by: reached_by.clone(),
                    produced_by: produced_by.cloned().unwrap_or(PathSupport::Empty),
                })
            } else {
                Evidence::Failure(Witness::Atom {
                    shape: id,
                    node: node.clone(),
                    observed: vec![node.clone()],
                    expected: Box::new(eval.arena().get(id).clone()),
                    reached_by: reached_by.clone(),
                    produced_by: produced_by.cloned(),
                })
            }
        }
        Shape::Eq(..) | Shape::Disj(..) | Shape::Lt(..) | Shape::Le(..) | Shape::UniqueLang(_) => {
            if eval.holds(node, id) {
                Evidence::Satisfaction(SatTrace::Blocked {
                    shape: id,
                    node: node.clone(),
                    reason: BlockReason::Unsupported,
                })
            } else {
                Evidence::Failure(relational_witness(eval, node, id, &shape))
            }
        }
        Shape::Closed(ref allowed) => {
            let offenders = closed_offenders(eval.backend(), node, allowed);
            if offenders.is_empty() {
                Evidence::Satisfaction(SatTrace::Blocked {
                    shape: id,
                    node: node.clone(),
                    reason: BlockReason::ClosedNeedsAdd,
                })
            } else {
                Evidence::Failure(Witness::Closed {
                    shape: id,
                    node: node.clone(),
                    offenders,
                })
            }
        }
        Shape::Sparql(constraint) => {
            if eval.holds(node, id) {
                Evidence::Satisfaction(SatTrace::Blocked {
                    shape: id,
                    node: node.clone(),
                    reason: BlockReason::OpaqueSparql,
                })
            } else {
                let violations = eval
                    .sparql()
                    .constraint_violations(&constraint, node)
                    .unwrap_or_default();
                let diagnostic = eval
                    .sparql()
                    .constraint_diagnostic(&constraint, node, &violations)
                    .ok();
                Evidence::Failure(Witness::Opaque {
                    shape: id,
                    node: node.clone(),
                    messages: constraint.messages,
                    diagnostic,
                })
            }
        }
        Shape::Expression(_) => {
            if eval.holds(node, id) {
                Evidence::Satisfaction(SatTrace::Blocked {
                    shape: id,
                    node: node.clone(),
                    reason: BlockReason::Unsupported,
                })
            } else {
                Evidence::Failure(Witness::Opaque {
                    shape: id,
                    node: node.clone(),
                    messages: Vec::new(),
                    diagnostic: None,
                })
            }
        }
        Shape::Not(child) => {
            match evaluate_node(eval, node, child, reached_by, produced_by, &[], stack) {
                Evidence::Failure(inner_fails) => Evidence::Satisfaction(SatTrace::NotHeld {
                    shape: id,
                    node: node.clone(),
                    inner_fails: Box::new(inner_fails),
                }),
                Evidence::Satisfaction(inner) => Evidence::Failure(Witness::Not {
                    shape: id,
                    node: node.clone(),
                    inner: Box::new(inner),
                }),
            }
        }
        Shape::And(children) => {
            let mut child_scope = scope.to_vec();
            child_scope.extend(
                children
                    .iter()
                    .filter_map(|&child| as_universal(eval.arena(), child)),
            );
            let evaluated: Vec<Evidence> = children
                .iter()
                .map(|&child| {
                    evaluate_node(
                        eval,
                        node,
                        child,
                        reached_by,
                        produced_by,
                        &child_scope,
                        stack,
                    )
                })
                .collect();
            if evaluated
                .iter()
                .all(|item| matches!(item, Evidence::Satisfaction(_)))
            {
                Evidence::Satisfaction(SatTrace::AllHeld {
                    shape: id,
                    node: node.clone(),
                    children: evaluated
                        .into_iter()
                        .map(|item| match item {
                            Evidence::Satisfaction(value) => value,
                            Evidence::Failure(_) => unreachable!(),
                        })
                        .collect(),
                })
            } else {
                Evidence::Failure(Witness::All {
                    shape: id,
                    node: node.clone(),
                    failed: evaluated
                        .into_iter()
                        .filter_map(|item| match item {
                            Evidence::Failure(value) => Some(value),
                            Evidence::Satisfaction(_) => None,
                        })
                        .collect(),
                })
            }
        }
        Shape::Or(children) => {
            let evaluated: Vec<Evidence> = children
                .iter()
                .map(|&child| {
                    evaluate_node(eval, node, child, reached_by, produced_by, scope, stack)
                })
                .collect();
            if evaluated
                .iter()
                .any(|item| matches!(item, Evidence::Satisfaction(_)))
            {
                Evidence::Satisfaction(SatTrace::AnyHeld {
                    shape: id,
                    node: node.clone(),
                    satisfied: evaluated
                        .into_iter()
                        .filter_map(|item| match item {
                            Evidence::Satisfaction(value) => Some(value),
                            Evidence::Failure(_) => None,
                        })
                        .collect(),
                })
            } else {
                Evidence::Failure(Witness::Any {
                    shape: id,
                    node: node.clone(),
                    branches: evaluated
                        .into_iter()
                        .map(|item| match item {
                            Evidence::Failure(value) => value,
                            Evidence::Satisfaction(_) => unreachable!(),
                        })
                        .collect(),
                })
            }
        }
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => {
            let reached = Path::seq(vec![reached_by.clone(), path.clone()]);
            let mut matches = Vec::new();
            let mut rejected = Vec::new();
            for (value, support) in succ_with_support(eval.backend(), node, &path) {
                match evaluate_node(
                    eval,
                    &value,
                    qualifier,
                    &reached,
                    Some(&support),
                    &[],
                    stack,
                ) {
                    Evidence::Satisfaction(satisfaction) => matches.push(QualifiedMatch {
                        value,
                        path_support: support,
                        satisfaction: Box::new(satisfaction),
                    }),
                    Evidence::Failure(failure) => rejected.push(RejectedCandidate {
                        value,
                        path_support: support,
                        failure: Box::new(failure),
                    }),
                }
            }
            let observed = matches.len() as u64;
            if min.is_some_and(|minimum| observed < minimum) {
                let minimum = min.expect("checked above");
                let sibling_qualifiers = scope
                    .iter()
                    .filter(|(candidate_path, _)| candidate_path == &path)
                    .map(|(_, inner)| *inner)
                    .collect();
                Evidence::Failure(Witness::CountLow {
                    shape: id,
                    node: node.clone(),
                    path,
                    qualifier,
                    have: observed,
                    min: minimum,
                    qualifying_matches: matches,
                    rejected_candidates: rejected,
                    sibling_qualifiers,
                })
            } else if max.is_some_and(|maximum| observed > maximum) {
                let maximum = max.expect("checked above");
                let matched: Vec<(Term, PathSupport)> = matches
                    .iter()
                    .map(|item| (item.value.clone(), item.path_support.clone()))
                    .collect();
                let excess_values = matched.iter().skip(maximum as usize).cloned().collect();
                let per_value = if maximum == 0 {
                    matches
                        .iter()
                        .filter_map(|item| match item.satisfaction.as_ref() {
                            SatTrace::NotHeld { inner_fails, .. } => {
                                Some((item.value.clone(), (**inner_fails).clone()))
                            }
                            _ => None,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                Evidence::Failure(Witness::CountHigh {
                    shape: id,
                    node: node.clone(),
                    path,
                    qualifier,
                    matched,
                    max: maximum,
                    excess_values,
                    per_value,
                })
            } else if min.is_none()
                && max == Some(0)
                && let Shape::Not(inner) = eval.arena().get(qualifier)
            {
                let values = rejected
                    .into_iter()
                    .filter_map(|candidate| match candidate.failure.as_ref() {
                        Witness::Not { inner: trace, .. } => {
                            Some((candidate.value, candidate.path_support, (**trace).clone()))
                        }
                        _ => None,
                    })
                    .collect();
                Evidence::Satisfaction(SatTrace::ForAllHeld {
                    shape: id,
                    node: node.clone(),
                    path,
                    qualifier: *inner,
                    values,
                })
            } else {
                let trace_matches = matches
                    .into_iter()
                    .map(|item| (item.value, item.path_support, *item.satisfaction))
                    .collect();
                Evidence::Satisfaction(SatTrace::CountHeld {
                    shape: id,
                    node: node.clone(),
                    path,
                    qualifier,
                    matches: trace_matches,
                    observed_count: observed,
                    min,
                    max,
                })
            }
        }
    };
    stack.remove(&key);
    evidence
}

pub(crate) fn materialize_evidence(
    eval: &mut ShapeEvaluator<'_>,
    node: &Term,
    shape: ShapeId,
) -> Evidence {
    evaluate_node(eval, node, shape, &Path::Id, None, &[], &mut Stack::new())
}

/// Predicates+objects on `node` not allowed by a closed shape's set `q`.
fn closed_offenders(
    g: &dyn PathBackend,
    node: &Term,
    q: &BTreeSet<NamedNode>,
) -> Vec<(NamedNode, Term)> {
    let allowed: HashSet<&NamedNode> = q.iter().collect();
    let mut out = Vec::new();
    // `out_predicates` is already ordered; the objects behind each are not, and
    // they are reported verbatim as the offending values. Ordered for the same
    // reason as `succ_with_support`.
    for p in g.out_predicates(node) {
        if !allowed.contains(&p) {
            let mut objects: Vec<Term> = g.objects(node, &p).into_iter().collect();
            objects.sort_by(compare_terms);
            out.extend(objects.into_iter().map(|object| (p.clone(), object)));
        }
    }
    out
}

/// Every `π`-successor of `from`, each paired with one concrete certificate.
///
/// Same values as [`succ`] and the same kind of certificate as
/// [`path_support`], but derived in a single traversal. Pairing `succ` with a
/// per-value `path_support` instead re-derives the whole route for every
/// candidate: for `sh:class` — which compiles to `type · subClassOf*` — that is
/// one hierarchy BFS per candidate value, and it dominated evidence
/// materialization on Brick.
///
/// Values are returned in first-reached order and deduplicated, so an
/// alternative or cycle keeps the first successful route exactly as
/// `path_support` does.
/// Path values with their certificates, in a deterministic order.
///
/// [`PathBackend`] yields `HashSet`s, and a `HashSet`'s iteration order varies
/// between instances, so evidence built straight from one differs between two
/// runs over the very same snapshot. That is not merely cosmetic: `CountHigh`
/// names the values past `max` as excess, so a different order reports a
/// different value as the offender. Ordering here — once, at the single point
/// where path values enter evidence — makes a run, and any artifact built from
/// it, reproducible.
///
/// Only evidence materialization pays for this. Conformance never calls it:
/// counts and satisfaction are order-independent, which is why the instability
/// was invisible until evidence started being serialized.
fn succ_with_support(g: &dyn PathBackend, from: &Term, path: &Path) -> Vec<(Term, PathSupport)> {
    let mut values = succ_with_support_unordered(g, from, path);
    values.sort_by(|(left, _), (right, _)| compare_terms(left, right));
    values
}

/// A total order on terms, for reproducibility rather than for meaning.
///
/// RDF defines no order across term kinds, so this fixes one: named nodes,
/// then blank nodes, then literals, each by its lexical form. Literals compare
/// on value, then datatype, then language, so that terms differing only in
/// their tag still order stably.
fn compare_terms(left: &Term, right: &Term) -> std::cmp::Ordering {
    fn rank(term: &Term) -> u8 {
        match term {
            Term::NamedNode(_) => 0,
            Term::BlankNode(_) => 1,
            Term::Literal(_) => 2,
        }
    }
    rank(left)
        .cmp(&rank(right))
        .then_with(|| match (left, right) {
            (Term::NamedNode(left), Term::NamedNode(right)) => left.as_str().cmp(right.as_str()),
            (Term::BlankNode(left), Term::BlankNode(right)) => left.as_str().cmp(right.as_str()),
            (Term::Literal(left), Term::Literal(right)) => left
                .value()
                .cmp(right.value())
                .then_with(|| left.datatype().as_str().cmp(right.datatype().as_str()))
                .then_with(|| left.language().cmp(&right.language())),
            _ => std::cmp::Ordering::Equal,
        })
}

fn succ_with_support_unordered(
    g: &dyn PathBackend,
    from: &Term,
    path: &Path,
) -> Vec<(Term, PathSupport)> {
    match path {
        Path::Id => vec![(from.clone(), PathSupport::Empty)],
        Path::Pred(q) => {
            let Some(subject) = node_of(from) else {
                return Vec::new();
            };
            g.objects(from, q)
                .into_iter()
                .map(|object| {
                    let edge = Triple::new(subject.clone(), q.clone(), object.clone());
                    (object, PathSupport::Edge(edge))
                })
                .collect()
        }
        // `Inverse(Pred)` is the common case and its certificate is the same
        // edge read backwards; anything else falls back to a per-value probe.
        Path::Inverse(inner) => match inner.as_ref() {
            Path::Pred(q) => g
                .subjects(q, from)
                .into_iter()
                .filter_map(|subject| {
                    let node = node_of(&subject)?;
                    let edge = Triple::new(node, q.clone(), from.clone());
                    Some((subject, PathSupport::Edge(edge)))
                })
                .collect(),
            _ => pred(g, from, inner)
                .into_iter()
                .map(|value| {
                    let support = path_support(g, from, path, &value).unwrap_or(PathSupport::Empty);
                    (value, support)
                })
                .collect(),
        },
        Path::Alt(branches) => {
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            for branch in branches {
                for (value, support) in succ_with_support(g, from, branch) {
                    if seen.insert(value.clone()) {
                        out.push((value, support));
                    }
                }
            }
            out
        }
        Path::Seq(steps) => {
            let Some((first, rest)) = steps.split_first() else {
                return vec![(from.clone(), PathSupport::Empty)];
            };
            let mut frontier: Vec<(Term, Vec<PathSupport>)> = Vec::new();
            let mut seen = HashSet::new();
            for (value, support) in succ_with_support(g, from, first) {
                if seen.insert(value.clone()) {
                    frontier.push((value, vec![support]));
                }
            }
            for step in rest {
                let mut next: Vec<(Term, Vec<PathSupport>)> = Vec::new();
                let mut seen = HashSet::new();
                for (value, chain) in &frontier {
                    for (reached, support) in succ_with_support(g, value, step) {
                        if seen.insert(reached.clone()) {
                            let mut chain = chain.clone();
                            chain.push(support);
                            next.push((reached, chain));
                        }
                    }
                }
                frontier = next;
            }
            frontier
                .into_iter()
                .map(|(value, chain)| (value, flatten_chain(chain)))
                .collect()
        }
        // One breadth-first walk records every reachable value with the route
        // that first reached it, replacing one walk per target.
        Path::Star(inner) => {
            let mut out = vec![(from.clone(), PathSupport::Empty)];
            let mut visited: HashSet<Term> = HashSet::from([from.clone()]);
            let mut queue: VecDeque<(Term, Vec<PathSupport>)> =
                VecDeque::from([(from.clone(), Vec::new())]);
            while let Some((current, chain)) = queue.pop_front() {
                for (next, support) in succ_with_support(g, &current, inner) {
                    if !visited.insert(next.clone()) {
                        continue;
                    }
                    let mut chain = chain.clone();
                    chain.push(support);
                    out.push((next.clone(), PathSupport::Chain(chain.clone())));
                    queue.push_back((next, chain));
                }
            }
            out
        }
    }
}

/// Splice one level of nested chains, matching `seq_support`'s shape.
fn flatten_chain(parts: Vec<PathSupport>) -> PathSupport {
    let mut chain = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            PathSupport::Empty => {}
            PathSupport::Chain(inner) => chain.extend(inner),
            other => chain.push(other),
        }
    }
    if chain.is_empty() {
        PathSupport::Empty
    } else {
        PathSupport::Chain(chain)
    }
}

/// The existing triples that make `to` a `π`-successor of `from`, if any. The
/// edges a deletion would cut to remove `to` from the value set.
fn path_support(g: &dyn PathBackend, from: &Term, path: &Path, to: &Term) -> Option<PathSupport> {
    crate::profile::record_path_probe();
    match path {
        Path::Id => (from == to).then_some(PathSupport::Empty),
        Path::Pred(q) => {
            // One triple lookup, not one materialized successor set: this is
            // called once per candidate value, so building the whole set here
            // makes a value-set of size `n` cost `O(n²)`.
            if g.contains(from, q, to) {
                let s = node_of(from)?;
                Some(PathSupport::Edge(Triple::new(s, q.clone(), to.clone())))
            } else {
                None
            }
        }
        Path::Inverse(p) => path_support(g, to, p, from),
        Path::Alt(ps) => ps.iter().find_map(|p| path_support(g, from, p, to)),
        Path::Seq(ps) => seq_support(g, from, ps, to),
        Path::Star(p) => star_support(g, from, p, to),
    }
}

fn seq_support(g: &dyn PathBackend, from: &Term, ps: &[Path], to: &Term) -> Option<PathSupport> {
    let Some((first, rest)) = ps.split_first() else {
        return (from == to).then_some(PathSupport::Empty);
    };
    for mid in succ(g, from, first) {
        let Some(head) = path_support(g, from, first, &mid) else {
            continue;
        };
        if let Some(tail) = seq_support(g, &mid, rest, to) {
            let mut chain = vec![head];
            match tail {
                PathSupport::Empty => {}
                PathSupport::Chain(v) => chain.extend(v),
                other => chain.push(other),
            }
            return Some(PathSupport::Chain(chain));
        }
    }
    None
}

fn star_support(g: &dyn PathBackend, from: &Term, p: &Path, to: &Term) -> Option<PathSupport> {
    if from == to {
        return Some(PathSupport::Empty);
    }
    let mut visited: HashSet<Term> = HashSet::from([from.clone()]);
    let mut queue: VecDeque<(Term, Vec<PathSupport>)> =
        VecDeque::from([(from.clone(), Vec::new())]);
    while let Some((cur, chain)) = queue.pop_front() {
        for next in succ(g, &cur, p) {
            let Some(edge) = path_support(g, &cur, p, &next) else {
                continue;
            };
            let mut chain2 = chain.clone();
            chain2.push(edge);
            if next == *to {
                return Some(PathSupport::Chain(chain2));
            }
            if visited.insert(next.clone()) {
                queue.push_back((next, chain2));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty_parse::{load_turtle, parse_turtle};

    const PREFIXES: &str = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:  <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    "#;

    fn run(ttl: &str) -> Vec<FocusWitness> {
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        witness_violations(&loaded.graph, &loaded.graph, &parsed.schema).expect("stratifiable")
    }

    /// Does any node in the witness tree satisfy `pred`?
    fn any(w: &Witness, pred: &impl Fn(&Witness) -> bool) -> bool {
        if pred(w) {
            return true;
        }
        match w {
            Witness::All { failed, .. } => failed.iter().any(|c| any(c, pred)),
            Witness::Any { branches, .. } => branches.iter().any(|c| any(c, pred)),
            Witness::CountHigh { per_value, .. } => per_value.iter().any(|(_, c)| any(c, pred)),
            _ => false,
        }
    }

    #[test]
    fn conforming_graph_yields_no_witnesses() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:x ex:p ex:y .
            "
        );
        assert!(run(&ttl).is_empty());
    }

    #[test]
    fn min_count_violation_is_count_low() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 2 ] .
            ex:x ex:p ex:y .
            "
        );
        let ws = run(&ttl);
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].focus.to_string(), "<http://ex/x>");
        assert!(any(&ws[0].failure, &|w| matches!(
            w,
            Witness::CountLow {
                have: 1,
                min: 2,
                ..
            }
        )));
    }

    /// Every `CountLow`'s `sibling_qualifiers`, in pre-order.
    fn count_low_siblings(w: &Witness) -> Vec<Vec<ShapeId>> {
        fn go(w: &Witness, out: &mut Vec<Vec<ShapeId>>) {
            match w {
                Witness::CountLow {
                    sibling_qualifiers, ..
                } => out.push(sibling_qualifiers.clone()),
                Witness::All { failed, .. } => failed.iter().for_each(|c| go(c, out)),
                Witness::Any { branches, .. } => branches.iter().for_each(|c| go(c, out)),
                Witness::CountHigh { per_value, .. } => {
                    per_value.iter().for_each(|(_, c)| go(c, out))
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        go(w, &mut out);
        out
    }

    #[test]
    fn cross_property_universals_attach_to_count_low() {
        // The min-count and the `sh:class` live in *separate* property shapes on the
        // same path — different `And` conjuncts. The class universal holds vacuously
        // (no values), so it never witnesses as a failure, yet a value added for the
        // count must still satisfy it: it must reach the `CountLow` across `And`s.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] ;
                sh:property [ sh:path ex:p ; sh:class ex:C ] .
            ex:x a ex:Thing .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let ws =
            witness_violations(&loaded.graph, &loaded.graph, &parsed.schema).expect("stratifiable");
        assert_eq!(ws.len(), 1);
        let sibs = count_low_siblings(&ws[0].failure);
        assert_eq!(sibs.len(), 1, "one CountLow");
        assert_eq!(sibs[0].len(), 1, "the class universal is attached");
        assert_eq!(
            class_of(sibs[0][0], &parsed.schema.arena),
            Some("http://ex/C".to_string()),
        );
    }

    #[test]
    fn disjoint_or_branch_universal_does_not_attach() {
        // The `sh:class` sits in a *different* `sh:or` branch than the count, so the
        // two are not conjoined — a value satisfying the count branch need not be a
        // class. Scope must not cross the disjunction: the CountLow carries no
        // sibling. (Both branches fail here, so the Or genuinely witnesses.)
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:or (
                    [ sh:path ex:p ; sh:minCount 2 ]
                    [ sh:path ex:p ; sh:class ex:C ]
                ) .
            ex:x a ex:Thing ; ex:p ex:y .
            "
        );
        let ws = run(&ttl);
        assert_eq!(ws.len(), 1);
        let sibs = count_low_siblings(&ws[0].failure);
        assert!(!sibs.is_empty(), "the count branch yields a CountLow");
        assert!(
            sibs.iter().all(|s| s.is_empty()),
            "no universal leaks across the disjunction: {sibs:?}",
        );
    }

    /// The class IRI of a `∃≥1 (rdf:type/subClassOf*).test(C)` shape, as a string.
    fn class_of(id: ShapeId, arena: &ShapeArena) -> Option<String> {
        match shifty_algebra::render::class_target_shape(id, arena)? {
            Term::NamedNode(n) => Some(n.as_str().to_string()),
            _ => None,
        }
    }

    #[test]
    fn datatype_violation_is_an_atom_with_support() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
            ex:x ex:p \"hello\" .
            "
        );
        let ws = run(&ttl);
        assert_eq!(ws.len(), 1);
        // The bad value is reached via ex:p, so its atom carries a cut edge.
        assert!(any(&ws[0].failure, &|w| matches!(
            w,
            Witness::Atom {
                produced_by: Some(PathSupport::Edge(_)),
                ..
            }
        )));
    }

    #[test]
    fn focus_level_nodekind_atom_has_no_support() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:nodeKind sh:IRI .
            ex:x ex:p \"v\" .
            ex:y ex:q ex:x .
            "
        );
        // ex:x is an IRI so it conforms; use a literal-targeted shape instead:
        let _ = ttl;
        let ttl2 = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:nodeKind sh:Literal .
            ex:x ex:p ex:y .
            "
        );
        let ws = run(&ttl2);
        assert_eq!(ws.len(), 1);
        assert!(matches!(
            ws[0].failure,
            Witness::Atom {
                produced_by: None,
                ..
            }
        ));
    }

    #[test]
    fn alternative_path_support_is_one_positive_certificate() {
        let ttl = format!(
            "{PREFIXES}
             ex:x ex:p ex:y ; ex:q ex:y ."
        );
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let x = Term::NamedNode(NamedNode::new("http://ex/x").unwrap());
        let y = Term::NamedNode(NamedNode::new("http://ex/y").unwrap());
        let path = Path::Alt(vec![
            Path::Pred(NamedNode::new("http://ex/p").unwrap()),
            Path::Pred(NamedNode::new("http://ex/q").unwrap()),
        ]);
        let support = path_support(&loaded.graph, &x, &path, &y).expect("reachable");
        let PathSupport::Edge(edge) = support else {
            panic!("Alt retains one successful route, got {support:?}")
        };
        assert!(loaded.graph.contains(edge.as_ref()));
    }

    fn collect_support_edges(support: &PathSupport, out: &mut Vec<Triple>) {
        match support {
            PathSupport::Edge(edge) => out.push(edge.clone()),
            PathSupport::Chain(parts) | PathSupport::Alt(parts) => {
                parts
                    .iter()
                    .for_each(|part| collect_support_edges(part, out));
            }
            PathSupport::Empty => {}
        }
    }

    #[test]
    fn traversal_certificates_match_per_value_probing() {
        // `succ_with_support` replaced `succ` + a `path_support` probe per
        // value. It must still return exactly the successor set, with a
        // certificate that probing would accept for each value.
        let ttl = format!(
            "{PREFIXES}
             ex:x ex:p ex:a ; ex:p ex:b ; ex:q ex:c .
             ex:a ex:r ex:d . ex:d ex:r ex:e .
             ex:b rdf:type ex:C .
             ex:C <http://www.w3.org/2000/01/rdf-schema#subClassOf> ex:D .
             ex:D <http://www.w3.org/2000/01/rdf-schema#subClassOf> ex:E .
             ex:back ex:p ex:x ."
        );
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let graph = &loaded.graph;
        let x = Term::NamedNode(NamedNode::new("http://ex/x").unwrap());
        let named = |iri: &str| NamedNode::new(iri).unwrap();
        let paths = vec![
            Path::Id,
            Path::Pred(named("http://ex/p")),
            Path::Inverse(Box::new(Path::Pred(named("http://ex/p")))),
            Path::Alt(vec![
                Path::Pred(named("http://ex/p")),
                Path::Pred(named("http://ex/q")),
            ]),
            Path::Seq(vec![
                Path::Pred(named("http://ex/p")),
                Path::Pred(named("http://ex/r")),
            ]),
            Path::Star(Path::Pred(named("http://ex/r")).into()),
            Path::Seq(vec![
                Path::Pred(named("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")),
                Path::Star(
                    Path::Pred(named("http://www.w3.org/2000/01/rdf-schema#subClassOf")).into(),
                ),
            ]),
        ];

        for path in paths {
            let derived = succ_with_support(graph, &x, &path);
            let values: HashSet<Term> = derived.iter().map(|(v, _)| v.clone()).collect();
            assert_eq!(values, succ(graph, &x, &path), "values differ for {path:?}");
            assert_eq!(derived.len(), values.len(), "duplicate value for {path:?}");
            for (value, support) in &derived {
                assert!(
                    path_support(graph, &x, &path, value).is_some(),
                    "probing rejects {value:?} for {path:?}"
                );
                let mut edges = Vec::new();
                collect_support_edges(support, &mut edges);
                for edge in edges {
                    assert!(
                        graph.contains(edge.as_ref()),
                        "certificate cites a missing triple for {path:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn non_stratifiable_schema_is_diagnosed() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:not [ sh:path ex:p ; sh:qualifiedValueShape ex:S ; sh:qualifiedMinCount 1 ] .
            ex:x ex:p ex:y .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        assert!(witness_violations(&loaded.graph, &loaded.graph, &parsed.schema).is_err());
    }

    #[test]
    fn witness_shape_and_satisfy_shape_scope_to_one_shape() {
        // Two targeted shapes; one focus fails ex:S, one passes ex:S, and a third
        // node fails an unrelated shape ex:T that the ex:S queries must ignore.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetClass ex:C ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:T a sh:NodeShape ; sh:targetClass ex:D ;
                sh:property [ sh:path ex:q ; sh:minCount 1 ] .
            ex:good a ex:C ; ex:p ex:y .
            ex:bad  a ex:C .
            ex:other a ex:D .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let schema = &parsed.schema;

        let s = shape_id_for_iri(schema, "http://ex/S").expect("ex:S is named");
        assert!(shape_id_for_iri(schema, "http://ex/missing").is_none());

        // Failures: just ex:bad, never the ex:T violation on ex:other.
        let fails = witness_shape(&loaded.graph, &loaded.graph, schema, s).expect("stratifiable");
        assert_eq!(fails.len(), 1);
        assert_eq!(fails[0].focus.to_string(), "<http://ex/bad>");

        // Satisfactions: just ex:good, with the matched value recorded.
        let sats = satisfy_shape(&loaded.graph, &loaded.graph, schema, s).expect("stratifiable");
        assert_eq!(sats.len(), 1);
        assert_eq!(sats[0].focus.to_string(), "<http://ex/good>");
        // ex:good holds because the ex:p count is met by ex:y.
        assert!(any_sat(&sats[0].trace, &|t| matches!(
            t,
            SatTrace::CountHeld { matches, .. } if matches.iter().any(|(v, _, _)| v.to_string() == "<http://ex/y>")
        )));
    }

    fn pred_path(iri: &str) -> Path {
        Path::Pred(NamedNode::new_unchecked(iri))
    }

    fn terms(values: &[Term]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn satisfaction_groups_matched_values_by_the_path_they_were_counted_along() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] ;
                sh:property [ sh:path ex:q ; sh:minCount 1 ] .
            ex:x ex:p ex:y1, ex:y2 ; ex:q ex:z .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let schema = &parsed.schema;
        let s = shape_id_for_iri(schema, "http://ex/S").expect("ex:S is named");
        let sats = satisfy_shape(&loaded.graph, &loaded.graph, schema, s).expect("stratifiable");
        let evidence = Evidence::Satisfaction(sats[0].trace.clone());

        // One group per counted path. Group order follows evidence traversal,
        // which is the planner's business, so look each path up by name.
        let grouped = evidence.matched_values_by_path();
        assert_eq!(grouped.len(), 2);
        let group = |iri: &str| {
            grouped
                .iter()
                .find(|(path, _)| *path == pred_path(iri))
                .map(|(_, values)| terms(values))
                .expect("path was counted")
        };
        assert_eq!(group("http://ex/p"), ["<http://ex/y1>", "<http://ex/y2>"]);
        assert_eq!(group("http://ex/q"), ["<http://ex/z>"]);

        // The single-path projection agrees with the grouping, and together the
        // groups partition `matched_values` in the same order.
        assert_eq!(
            terms(&evidence.values_for_path(&pred_path("http://ex/p"))),
            ["<http://ex/y1>", "<http://ex/y2>"],
        );
        assert_eq!(
            terms(&evidence.values_for_path(&pred_path("http://ex/q"))),
            ["<http://ex/z>"],
        );
        assert_eq!(
            grouped
                .into_iter()
                .flat_map(|(_, values)| values)
                .collect::<Vec<_>>(),
            evidence.matched_values(),
        );

        // A path this evidence never counted along is empty, not an error.
        assert!(
            evidence
                .values_for_path(&pred_path("http://ex/absent"))
                .is_empty()
        );
        assert!(evidence.values_for_path(&Path::Id).is_empty());
    }

    #[test]
    fn a_short_count_attributes_its_qualifying_matches_to_its_own_path() {
        // ex:p is short by one; ex:q is met. Both contribute matched values, and
        // each set must land under the path that counted it.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 3 ] ;
                sh:property [ sh:path ex:q ; sh:minCount 1 ] .
            ex:x ex:p ex:y1, ex:y2 ; ex:q ex:z .
            "
        );
        let ws = run(&ttl);
        assert_eq!(ws.len(), 1);
        let evidence = Evidence::Failure(ws[0].failure.clone());

        assert_eq!(
            terms(&evidence.values_for_path(&pred_path("http://ex/p"))),
            ["<http://ex/y1>", "<http://ex/y2>"],
        );
        assert!(
            evidence
                .values_for_path(&pred_path("http://ex/q"))
                .is_empty(),
            "a satisfied sibling is not part of the failure's evidence",
        );
    }

    #[test]
    fn a_deficit_names_the_node_path_and_qualifier_that_would_close_it() {
        // ex:x needs two ex:p values of class ex:C and has one; ex:near is
        // reached but rejected, and its own class check is short as well.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ;
                              sh:qualifiedValueShape [ sh:class ex:C ] ;
                              sh:qualifiedMinCount 2 ] .
            ex:x ex:p ex:good, ex:near .
            ex:good a ex:C .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let ws =
            witness_violations(&loaded.graph, &loaded.graph, &parsed.schema).expect("stratifiable");
        let obligations = Evidence::Failure(ws[0].failure.clone()).missing_obligations();

        // The deficit on the focus: one more ex:p value, of class ex:C.
        let on_focus: Vec<_> = obligations
            .iter()
            .filter(|item| item.node.to_string() == "<http://ex/x>")
            .collect();
        assert_eq!(on_focus.len(), 1);
        assert_eq!(on_focus[0].path, pred_path("http://ex/p"));
        assert_eq!((on_focus[0].observed_count, on_focus[0].missing), (1, 1));
        assert_eq!(
            shifty_algebra::render::describe_shape(&parsed.schema.arena, on_focus[0].qualifier),
            "instance of <http://ex/C>",
            "the qualifier says what an added value must satisfy",
        );

        // A count nested inside the rejected candidate reports *its* node, so
        // the two deficits are told apart without reading `explain()`.
        assert!(
            obligations
                .iter()
                .any(|item| item.node.to_string() == "<http://ex/near>"),
            "obligations: {obligations:?}",
        );
    }

    /// Does any node in the satisfaction trace satisfy `pred`?
    fn any_sat(t: &SatTrace, pred: &impl Fn(&SatTrace) -> bool) -> bool {
        if pred(t) {
            return true;
        }
        match t {
            SatTrace::AllHeld { children, .. } => children.iter().any(|c| any_sat(c, pred)),
            SatTrace::AnyHeld { satisfied, .. } => satisfied.iter().any(|c| any_sat(c, pred)),
            SatTrace::CountHeld { matches, .. } => matches.iter().any(|(_, _, c)| any_sat(c, pred)),
            SatTrace::ForAllHeld { values, .. } => values.iter().any(|(_, _, c)| any_sat(c, pred)),
            _ => false,
        }
    }

    #[test]
    fn shared_child_grammar_is_preorder_and_crosses_polarity_at_not() {
        let node = Term::NamedNode(NamedNode::new_unchecked("http://ex/x"));
        let failure = Evidence::Failure(Witness::All {
            shape: ShapeId(0),
            node: node.clone(),
            failed: vec![
                Witness::Not {
                    shape: ShapeId(1),
                    node: node.clone(),
                    inner: Box::new(SatTrace::Irrefutable { shape: ShapeId(2) }),
                },
                Witness::Opaque {
                    shape: ShapeId(3),
                    node: node.clone(),
                    messages: Vec::new(),
                    diagnostic: None,
                },
            ],
        });

        let root = failure.walk()[0];
        assert_eq!(
            root.children()
                .into_iter()
                .map(EvidenceNodeRef::kind)
                .collect::<Vec<_>>(),
            ["not_failed", "opaque"],
        );
        assert_eq!(
            failure
                .walk()
                .into_iter()
                .map(|item| (item.kind(), item.status()))
                .collect::<Vec<_>>(),
            [
                ("all_failed", EvaluationStatus::Fail),
                ("not_failed", EvaluationStatus::Fail),
                ("irrefutable", EvaluationStatus::Pass),
                ("opaque", EvaluationStatus::Fail),
            ],
        );

        let satisfaction = Evidence::Satisfaction(SatTrace::NotHeld {
            shape: ShapeId(4),
            node,
            inner_fails: Box::new(Witness::Opaque {
                shape: ShapeId(5),
                node: Term::NamedNode(NamedNode::new_unchecked("http://ex/x")),
                messages: Vec::new(),
                diagnostic: None,
            }),
        });
        assert_eq!(
            satisfaction
                .walk()
                .into_iter()
                .map(|item| (item.kind(), item.status()))
                .collect::<Vec<_>>(),
            [
                ("not_held", EvaluationStatus::Pass),
                ("opaque", EvaluationStatus::Fail),
            ],
        );
    }

    #[test]
    fn evidence_kind_is_an_exhaustive_polarity_discriminant() {
        let passing = [
            EvidenceKind::Irrefutable,
            EvidenceKind::AtomHeld,
            EvidenceKind::AllHeld,
            EvidenceKind::AnyHeld,
            EvidenceKind::CountHeld,
            EvidenceKind::AllValuesHeld,
            EvidenceKind::NotHeld,
            EvidenceKind::Blocked,
            EvidenceKind::Coinductive,
        ];
        let failing = [
            EvidenceKind::AtomFailed,
            EvidenceKind::RelationalFailed,
            EvidenceKind::ClosedFailed,
            EvidenceKind::NotFailed,
            EvidenceKind::AllFailed,
            EvidenceKind::AnyFailed,
            EvidenceKind::CountLow,
            EvidenceKind::CountHigh,
            EvidenceKind::Opaque,
        ];

        assert!(
            passing
                .iter()
                .all(|kind| kind.status() == EvaluationStatus::Pass)
        );
        assert!(
            failing
                .iter()
                .all(|kind| kind.status() == EvaluationStatus::Fail)
        );
        let names = passing
            .iter()
            .chain(&failing)
            .map(|kind| kind.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), passing.len() + failing.len());
    }
}
