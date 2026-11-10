//! What a universe holds and what a draw returns: operations, injections,
//! and cases.

use crate::nt::{self, Triple};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use shifty_algebra::ShapeId;
use shifty_repair::GraphDelta;
use std::collections::BTreeSet;
use std::fmt;
use std::rc::Rc;

/// What an [`Operation`] does to the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    /// Cut an edge that supports a value.
    Remove,
    /// Replace a value with one that fails, or satisfies, its qualifier.
    Substitute,
    /// Add a value along a path.
    AddValue,
    /// Add a triple that a closed shape disallows.
    AddForbidden,
}

impl OperationKind {
    pub const ALL: [Self; 4] = [
        Self::Remove,
        Self::Substitute,
        Self::AddValue,
        Self::AddForbidden,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Remove => "remove",
            Self::Substitute => "substitute",
            Self::AddValue => "add_value",
            Self::AddForbidden => "add_forbidden",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == name)
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The status an edit drives a node to, for one constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Polarity {
    Violate,
    Satisfy,
}

impl Polarity {
    pub fn flip(self) -> Self {
        match self {
            Self::Violate => Self::Satisfy,
            Self::Satisfy => Self::Violate,
        }
    }

    pub fn wanting(holds: bool) -> Self {
        if holds { Self::Satisfy } else { Self::Violate }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Violate => "violate",
            Self::Satisfy => "satisfy",
        }
    }
}

/// `node` must reach `polarity` for normalized constraint `constraint`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Requirement {
    pub polarity: Polarity,
    pub node: String,
    pub constraint: ShapeId,
}

impl Requirement {
    fn opposite(&self) -> Self {
        Self {
            polarity: self.polarity.flip(),
            ..self.clone()
        }
    }
}

/// One atomic edit of an [`Injection`]. It makes `node` reach `polarity`
/// for one sub-shape of the target: [`Polarity::Satisfy`] under a negation,
/// or where a universal breaks because a value starts to hold.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Operation {
    pub kind: OperationKind,
    pub node: String,
    pub polarity: Polarity,
    pub deletes: Vec<Triple>,
    pub adds: Vec<Triple>,
    /// What the edit does, for people: for example `substitute "RAT_01" →
    /// "RA" (fails sh:minLength 3)`.
    pub detail: String,
    /// The normalized sub-shape. Arena ids differ between processes, so it
    /// stays inside the crate.
    pub(crate) constraint: ShapeId,
    /// Polarities the edit depends on, such as that a substituted value
    /// fails its qualifier.
    pub(crate) requires: Vec<Requirement>,
}

impl Operation {
    pub(crate) fn requirements(&self) -> impl Iterator<Item = Requirement> + '_ {
        std::iter::once(Requirement {
            polarity: self.polarity,
            node: self.node.clone(),
            constraint: self.constraint,
        })
        .chain(self.requires.iter().cloned())
    }

    pub fn to_json(&self) -> Value {
        json!({
            "kind": self.kind.as_str(),
            "node": self.node,
            "polarity": self.polarity.as_str(),
            "delete": triples_json(&self.deletes),
            "add": triples_json(&self.adds),
            "detail": self.detail,
        })
    }
}

/// One way to break the reference graph: operations that together make
/// one `(focus, statement)` pair fail. Two injections with the same net
/// delta are the same injection, identified by [`key`](Self::key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Injection {
    /// The focus node, as an N-Triples term.
    pub focus: String,
    /// The authored statement index.
    pub statement: usize,
    /// The statement's shape name, when it is a named node.
    pub shape: Option<String>,
    pub operations: Vec<Operation>,
    /// The top-level conjunct of the statement this injection breaks: for a
    /// node shape, usually one property shape. Validation reports it as its
    /// own result.
    pub(crate) component: ShapeId,
    key: String,
}

impl Injection {
    pub(crate) fn new(
        focus: String,
        statement: usize,
        shape: Option<String>,
        component: ShapeId,
        operations: Vec<Operation>,
    ) -> Self {
        let mut expr = Self {
            focus,
            statement,
            shape,
            component,
            operations,
            key: String::new(),
        };
        let mut lines: Vec<String> = expr
            .deletes()
            .iter()
            .map(|t| format!("- {}", nt::line(t)))
            .collect();
        lines.extend(expr.adds().iter().map(|t| format!("+ {}", nt::line(t))));
        expr.key = lines.join("\n");
        expr
    }

    /// The injection's identity: its sorted net delta as `- s p o .` and
    /// `+ s p o .` lines. Stable across processes, and unique in a
    /// universe.
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn deletes(&self) -> BTreeSet<Triple> {
        self.operations
            .iter()
            .flat_map(|op| op.deletes.iter().cloned())
            .collect()
    }

    pub fn adds(&self) -> BTreeSet<Triple> {
        self.operations
            .iter()
            .flat_map(|op| op.adds.iter().cloned())
            .collect()
    }

    pub(crate) fn footprint(&self) -> BTreeSet<Triple> {
        let mut out = self.deletes();
        out.extend(self.adds());
        out
    }

    /// The `(focus, statement)` pair the injection makes fail.
    pub(crate) fn target(&self) -> (&str, usize) {
        (&self.focus, self.statement)
    }

    /// The predicates the injection edits.
    pub fn predicates(&self) -> BTreeSet<String> {
        self.footprint().into_iter().map(|t| t.1).collect()
    }

    pub fn kinds(&self) -> Vec<OperationKind> {
        self.operations.iter().map(|op| op.kind).collect()
    }

    /// Whether the two cannot be applied together, by the rule [`merge`]
    /// applies inside one injection: one deletes what the other adds, both
    /// replace one value differently, or they need opposite polarities of
    /// one `(node, constraint)`. Shared edits, and several injections
    /// breaking one property, are fine.
    pub(crate) fn conflicts_with(&self, other: &Self) -> bool {
        merge([self.operations.as_slice(), other.operations.as_slice()]).is_none()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "focus": self.focus,
            "statement": self.statement,
            "shape": self.shape,
            "key": self.key,
            "predicates": self.predicates().into_iter().collect::<Vec<_>>(),
            "operations": self.operations.iter().map(Operation::to_json).collect::<Vec<_>>(),
        })
    }
}

/// Merge a joint choice of alternatives. `None` when the parts contradict:
/// one deletes what another adds, two replace one value differently, or the
/// parts need opposite polarities.
pub(crate) fn merge<'a>(
    parts: impl IntoIterator<Item = &'a [Operation]>,
) -> Option<Vec<Operation>> {
    let mut ops: Vec<Operation> = Vec::new();
    let mut deletes: BTreeSet<&Triple> = BTreeSet::new();
    let mut adds: BTreeSet<&Triple> = BTreeSet::new();
    let mut reqs: BTreeSet<Requirement> = BTreeSet::new();
    let mut replaced: Vec<(&Triple, &[Triple])> = Vec::new();
    let mut seen: Vec<&Operation> = Vec::new();
    for part in parts {
        for op in part {
            if seen.contains(&op) {
                continue;
            }
            if op.adds.iter().any(|t| deletes.contains(t))
                || op.deletes.iter().any(|t| adds.contains(t))
            {
                return None;
            }
            if op.kind == OperationKind::Substitute
                && let Some(edge) = op.deletes.first()
            {
                match replaced.iter().find(|(e, _)| *e == edge) {
                    Some((_, new)) if *new != op.adds.as_slice() => return None,
                    Some(_) => {}
                    None => replaced.push((edge, &op.adds)),
                }
            }
            let new: Vec<Requirement> = op.requirements().collect();
            if new.iter().any(|r| reqs.contains(&r.opposite())) {
                return None;
            }
            seen.push(op);
            ops.push(op.clone());
            deletes.extend(&op.deletes);
            adds.extend(&op.adds);
            reqs.extend(new);
        }
    }
    Some(ops)
}

/// One failing `(focus, statement)` pair of a case's graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseFailure {
    pub focus: String,
    pub statement: usize,
    pub shape: Option<String>,
    /// Indices into [`Case::injections`] of the injections this failure
    /// realizes. Empty for a side effect: a pair that fails although no
    /// injection aimed at it.
    pub injections: Vec<usize>,
}

/// One benchmark case: a broken graph and its ground truth.
///
/// The injected delta `Δ` gives `G_test = G_ref ⊕ Δ`, and the repair `Δ⁻¹`
/// swaps its additions and deletions. `failures` lists every failing pair
/// the engine reports for `G_test`. It can exceed `k`, since one edit can
/// fail several statements.
#[derive(Clone)]
pub struct Case {
    /// The budget the case was drawn with.
    pub k: usize,
    pub seed: u64,
    /// The injections, in admission order. At most `k`.
    pub injections: Vec<Injection>,
    pub failures: Vec<CaseFailure>,
    deletes: BTreeSet<Triple>,
    adds: BTreeSet<Triple>,
    delta: GraphDelta,
    reference: Rc<BTreeSet<Triple>>,
}

impl fmt::Debug for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Case")
            .field("k", &self.k)
            .field("seed", &self.seed)
            .field("injections", &self.injections)
            .field("failures", &self.failures)
            .finish_non_exhaustive()
    }
}

impl Case {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        k: usize,
        seed: u64,
        injections: Vec<Injection>,
        failures: Vec<CaseFailure>,
        deletes: BTreeSet<Triple>,
        adds: BTreeSet<Triple>,
        delta: GraphDelta,
        reference: Rc<BTreeSet<Triple>>,
    ) -> Self {
        Self {
            k,
            seed,
            injections,
            failures,
            deletes,
            adds,
            delta,
            reference,
        }
    }

    /// The injected delta `Δ`.
    pub fn delta(&self) -> &GraphDelta {
        &self.delta
    }

    /// The ground-truth repair `Δ⁻¹`, which recovers `G_ref`.
    pub fn repair(&self) -> GraphDelta {
        GraphDelta {
            add: self.delta.delete.clone(),
            delete: self.delta.add.clone(),
        }
    }

    /// The triples `Δ` deletes, as N-Triples terms.
    pub fn deletes(&self) -> &BTreeSet<Triple> {
        &self.deletes
    }

    /// The triples `Δ` adds, as N-Triples terms.
    pub fn adds(&self) -> &BTreeSet<Triple> {
        &self.adds
    }

    /// `G_test`, in sorted N-Triples.
    pub fn graph_ntriples(&self) -> String {
        let mut out = String::new();
        let kept = self.reference.iter().filter(|t| !self.deletes.contains(*t));
        let mut all: BTreeSet<&Triple> = kept.collect();
        all.extend(self.adds.iter());
        for t in all {
            out.push_str(&nt::line(t));
            out.push('\n');
        }
        out
    }

    /// The ground-truth repair as a SPARQL 1.1 Update.
    ///
    /// SPARQL forbids blank nodes in `DELETE DATA`. When the repair deletes
    /// a blank-node triple, this falls back to `DELETE WHERE`, which binds
    /// blank nodes as variables.
    pub fn repair_sparql(&self) -> String {
        let mut parts = Vec::new();
        if !self.adds.is_empty() {
            let blank = self
                .adds
                .iter()
                .any(|t| [&t.0, &t.1, &t.2].iter().any(|x| x.starts_with("_:")));
            let body = self
                .adds
                .iter()
                .map(|t| {
                    if blank {
                        let var = |x: &str| match x.strip_prefix("_:") {
                            Some(label) => format!(
                                "?b_{}",
                                label
                                    .chars()
                                    .map(|c| if c.is_alphanumeric() || c == '_' {
                                        c
                                    } else {
                                        '_'
                                    })
                                    .collect::<String>()
                            ),
                            None => x.to_owned(),
                        };
                        format!("  {} {} {} .", var(&t.0), t.1, var(&t.2))
                    } else {
                        format!("  {}", nt::line(t))
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            let verb = if blank { "DELETE WHERE" } else { "DELETE DATA" };
            parts.push(format!("{verb} {{\n{body}\n}}"));
        }
        if !self.deletes.is_empty() {
            let body = self
                .deletes
                .iter()
                .map(|t| format!("  {}", nt::line(t)))
                .collect::<Vec<_>>()
                .join("\n");
            parts.push(format!("INSERT DATA {{\n{body}\n}}"));
        }
        let mut out = parts.join(" ;\n");
        out.push('\n');
        out
    }

    /// The case manifest written as `case.json`.
    pub fn to_json(&self) -> Value {
        json!({
            "k": self.k,
            "seed": self.seed,
            "delta": {"delete": triples_json(&self.deletes), "add": triples_json(&self.adds)},
            "repair": {"delete": triples_json(&self.adds), "add": triples_json(&self.deletes)},
            "injections": self.injections.iter().map(Injection::to_json).collect::<Vec<_>>(),
            "failures": self.failures.iter().map(|v| json!({
                "focus": v.focus,
                "statement": v.statement,
                "shape": v.shape,
                "injections": v.injections,
            })).collect::<Vec<_>>(),
        })
    }
}

fn triples_json<'a>(triples: impl IntoIterator<Item = &'a Triple>) -> Vec<Value> {
    triples
        .into_iter()
        .map(|t| json!([t.0, t.1, t.2]))
        .collect()
}
