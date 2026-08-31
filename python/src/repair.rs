//! Python bindings for the symbolic-repair API (`docs/06-repair.md`).
//!
//! These expose the repair *primitives* — witness → synthesize → enumerate
//! candidates → instantiate → gate → apply — as plain Python objects, so an
//! external driver can inspect the horizon of violations per focus node, list the
//! holes and their options, make its own choices, and gate before committing. The
//! library computes and gates; **it decides nothing**. No canned repair loop is
//! shipped here — the driver is yours to write in Python.

use crate::{
    Constraint, ConstraintKind, InputSpec, Violation, constraint_kind_to_py, constraint_to_py,
    graph_to_ntriples, parse_minimum_severity, parse_mode, py_value_error, term_text,
    violation_to_py,
};
use oxrdf::{Graph, Term};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use shifty_algebra::{Schema, Selector, Shape, ShapeArena, ShapeId};
use shifty_engine::{
    ConformanceOptions, Evidence as IrEvidence, EvidenceKind as IrEvidenceKind,
    EvidenceOrigin as IrEvidenceOrigin, FocusSat as IrSat, FocusWitness as IrFocus,
    PathSupport as IrPathSupport, PreparedEvidenceValidator, SatTrace, ValidationOptions, Witness,
    apply as engine_apply, candidates as engine_candidates, gate as engine_gate, graph_union,
    satisfy_shape, shape_id_for_iri, synthesize_with_origins, witness_node, witness_shape,
    witness_violations,
};
use shifty_repair::{
    Edit, EditOp, Hole as IrHole, HoleConstraint, NodeId, Plan, RepairTree as IrTree, Slot,
    instantiate,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

// ── shared rendering (ports of the CLI renderers; kept local to the binding) ────

fn path_str(p: &shifty_algebra::Path) -> String {
    shifty_algebra::render::path_to_string(p)
}

fn constraint_str(c: &HoleConstraint, arena: &ShapeArena) -> String {
    match c {
        HoleConstraint::AnyNode => "any node".to_string(),
        HoleConstraint::Fresh => "fresh node".to_string(),
        HoleConstraint::Const(t) => format!("= {t}"),
        HoleConstraint::Typed(vt) => shifty_algebra::render::value_type_to_string(vt),
        HoleConstraint::Kind(_) => "nodeKind".to_string(),
        HoleConstraint::OneOf(v) => format!("one of {} value(s)", v.len()),
        // Fully expand the sub-shape(s) so no bare `@id` pointers leak out.
        HoleConstraint::ConformsTo(s) => shifty_algebra::render::describe_shape(arena, *s),
        HoleConstraint::ConformsToAll(ss) => shifty_algebra::render::describe_shapes(arena, ss),
    }
}

fn slot_str(s: &Slot) -> String {
    match s {
        Slot::Bound(t) => t.to_string(),
        Slot::Open(h) => format!("?{}", h.0),
    }
}

fn edit_str(e: &Edit) -> String {
    let (sign, p) = match &e.op {
        EditOp::Add(p) => ("add", p),
        EditOp::Delete(p) => ("del", p),
    };
    format!(
        "{sign} {} {} {}",
        slot_str(&p.s),
        slot_str(&p.p),
        slot_str(&p.o)
    )
}

fn render_witness(w: &Witness, indent: usize, out: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    match w {
        Witness::Atom {
            node,
            reached_by,
            produced_by,
            ..
        } => out.push(format!(
            "{pad}Atom at {node} via {}{}",
            path_str(reached_by),
            if produced_by.is_some() {
                " [cuttable]"
            } else {
                ""
            }
        )),
        Witness::Relational {
            kind, offending, ..
        } => out.push(format!(
            "{pad}Relational {kind:?}: {} offending pair(s)",
            offending.len()
        )),
        Witness::Closed { offenders, .. } => {
            out.push(format!(
                "{pad}Closed: {} disallowed triple(s)",
                offenders.len()
            ));
            for (p, o) in offenders {
                out.push(format!("{pad}  - {p} {o}"));
            }
        }
        Witness::Not { inner, .. } => {
            out.push(format!("{pad}Not — falsify the inner shape:"));
            render_sat(inner, indent + 2, out);
        }
        Witness::All { failed, .. } => {
            out.push(format!("{pad}All — fix every:"));
            for f in failed {
                render_witness(f, indent + 2, out);
            }
        }
        Witness::Any { branches, .. } => {
            out.push(format!("{pad}Any — fix any one of:"));
            for b in branches {
                render_witness(b, indent + 2, out);
            }
        }
        Witness::CountLow {
            path, have, min, ..
        } => out.push(format!(
            "{pad}CountLow along {}: have {have}, need {min}",
            path_str(path)
        )),
        Witness::CountHigh {
            path,
            matched,
            max,
            per_value,
            ..
        } => {
            out.push(format!(
                "{pad}CountHigh along {}: {} match(es), max {max}",
                path_str(path),
                matched.len()
            ));
            for (v, sub) in per_value {
                out.push(format!("{pad}  value {v}:"));
                render_witness(sub, indent + 4, out);
            }
        }
        Witness::Opaque { .. } => out.push(format!("{pad}Opaque (SPARQL) — no algebraic witness")),
    }
}

fn render_sat(s: &SatTrace, indent: usize, out: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    match s {
        SatTrace::Irrefutable { .. } => out.push(format!("{pad}Irrefutable (⊤)")),
        SatTrace::Atom { node, .. } => {
            out.push(format!("{pad}Atom holds at {node} [cut to break]"))
        }
        SatTrace::AllHeld { children, .. } => {
            out.push(format!("{pad}AllHeld — break any one:"));
            for c in children {
                render_sat(c, indent + 2, out);
            }
        }
        SatTrace::AnyHeld { satisfied, .. } => {
            out.push(format!("{pad}AnyHeld — break every:"));
            for c in satisfied {
                render_sat(c, indent + 2, out);
            }
        }
        SatTrace::CountHeld { matches, .. } => {
            out.push(format!("{pad}CountHeld: {} match(es)", matches.len()))
        }
        SatTrace::ForAllHeld { values, .. } => {
            out.push(format!(
                "{pad}ForAllHeld: {} checked value(s)",
                values.len()
            ));
            for (_, _, trace) in values {
                render_sat(trace, indent + 2, out);
            }
        }
        SatTrace::NotHeld { inner_fails, .. } => {
            out.push(format!("{pad}NotHeld — make the inner shape hold:"));
            render_witness(inner_fails, indent + 2, out);
        }
        SatTrace::Blocked { reason, .. } => out.push(format!("{pad}Blocked: {reason:?}")),
        SatTrace::Coinductive { .. } => out.push(format!("{pad}Coinductive (gfp back-edge)")),
    }
}

fn render_tree(t: &IrTree, arena: &ShapeArena, indent: usize, out: &mut Vec<String>) {
    let pad = " ".repeat(indent);
    match t {
        IrTree::Noop(_) => out.push(format!("{pad}Noop")),
        IrTree::Blocked(_, r) => out.push(format!("{pad}Blocked: {r:?}")),
        IrTree::Edits { edits, holes, .. } => {
            out.push(format!("{pad}Edits:"));
            for e in edits {
                out.push(format!("{pad}  {}", edit_str(e)));
            }
            for (h, c) in holes {
                out.push(format!("{pad}  ?{} : {}", h.0, constraint_str(c, arena)));
            }
        }
        IrTree::All { children, .. } => {
            out.push(format!("{pad}All — do all:"));
            for c in children {
                render_tree(c, arena, indent + 2, out);
            }
        }
        IrTree::Any { children, .. } => {
            out.push(format!("{pad}Any — choose one:"));
            for c in children {
                render_tree(c, arena, indent + 2, out);
            }
        }
        IrTree::Repeat { body, min, max, .. } => {
            let hi = max.map_or_else(|| "∞".to_string(), |m| m.to_string());
            out.push(format!("{pad}Repeat [{min}..{hi}]:"));
            render_tree(body, arena, indent + 2, out);
        }
    }
}

// ── flat witness summary ────────────────────────────────────────────────────────

/// Exact kind of a structured evidence node. Unlike `WitnessKind` and
/// `SatKind`, which classify flattened driver summaries, this enum is a
/// one-to-one projection of the Rust evidence grammar.
#[pyclass(eq, eq_int, hash, frozen, name = "EvidenceKind")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyEvidenceKind {
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

impl From<IrEvidenceKind> for PyEvidenceKind {
    fn from(kind: IrEvidenceKind) -> Self {
        match kind {
            IrEvidenceKind::Irrefutable => Self::Irrefutable,
            IrEvidenceKind::AtomHeld => Self::AtomHeld,
            IrEvidenceKind::AllHeld => Self::AllHeld,
            IrEvidenceKind::AnyHeld => Self::AnyHeld,
            IrEvidenceKind::CountHeld => Self::CountHeld,
            IrEvidenceKind::AllValuesHeld => Self::AllValuesHeld,
            IrEvidenceKind::NotHeld => Self::NotHeld,
            IrEvidenceKind::Blocked => Self::Blocked,
            IrEvidenceKind::Coinductive => Self::Coinductive,
            IrEvidenceKind::AtomFailed => Self::AtomFailed,
            IrEvidenceKind::RelationalFailed => Self::RelationalFailed,
            IrEvidenceKind::ClosedFailed => Self::ClosedFailed,
            IrEvidenceKind::NotFailed => Self::NotFailed,
            IrEvidenceKind::AllFailed => Self::AllFailed,
            IrEvidenceKind::AnyFailed => Self::AnyFailed,
            IrEvidenceKind::CountLow => Self::CountLow,
            IrEvidenceKind::CountHigh => Self::CountHigh,
            IrEvidenceKind::Opaque => Self::Opaque,
        }
    }
}

impl From<PyEvidenceKind> for IrEvidenceKind {
    fn from(kind: PyEvidenceKind) -> Self {
        match kind {
            PyEvidenceKind::Irrefutable => Self::Irrefutable,
            PyEvidenceKind::AtomHeld => Self::AtomHeld,
            PyEvidenceKind::AllHeld => Self::AllHeld,
            PyEvidenceKind::AnyHeld => Self::AnyHeld,
            PyEvidenceKind::CountHeld => Self::CountHeld,
            PyEvidenceKind::AllValuesHeld => Self::AllValuesHeld,
            PyEvidenceKind::NotHeld => Self::NotHeld,
            PyEvidenceKind::Blocked => Self::Blocked,
            PyEvidenceKind::Coinductive => Self::Coinductive,
            PyEvidenceKind::AtomFailed => Self::AtomFailed,
            PyEvidenceKind::RelationalFailed => Self::RelationalFailed,
            PyEvidenceKind::ClosedFailed => Self::ClosedFailed,
            PyEvidenceKind::NotFailed => Self::NotFailed,
            PyEvidenceKind::AllFailed => Self::AllFailed,
            PyEvidenceKind::AnyFailed => Self::AnyFailed,
            PyEvidenceKind::CountLow => Self::CountLow,
            PyEvidenceKind::CountHigh => Self::CountHigh,
            PyEvidenceKind::Opaque => Self::Opaque,
        }
    }
}

impl PyEvidenceKind {
    fn as_str(self) -> &'static str {
        IrEvidenceKind::from(self).as_str()
    }

    fn status_str(self) -> &'static str {
        match IrEvidenceKind::from(self).status() {
            shifty_engine::EvaluationStatus::Pass => "pass",
            shifty_engine::EvaluationStatus::Fail => "fail",
        }
    }
}

#[pymethods]
impl PyEvidenceKind {
    #[getter]
    fn status(&self) -> &'static str {
        self.status_str()
    }

    fn __str__(&self) -> &'static str {
        self.as_str()
    }
}

/// The kind of a failing witness leaf — the enumerated discriminant of
/// [`WitnessAtom`]. `Not` marks a `¬φ` that holds and must be falsified; the
/// `Count*` variants an under-/over-satisfied cardinality.
#[pyclass(eq, eq_int, hash, frozen, name = "WitnessKind")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WitnessKind {
    Atom,
    Relational,
    Closed,
    CountLow,
    CountHigh,
    Not,
    Opaque,
}

/// One failing leaf of a witness, flattened. The AND/OR structure is preserved in
/// `explain`; this is the bag of leaves a driver can scan.
#[pyclass(get_all, name = "WitnessAtom")]
#[derive(Clone)]
pub struct WitnessAtom {
    /// The leaf kind (see [`WitnessKind`]).
    pub kind: WitnessKind,
    /// Exact structured evidence kind this flattened leaf projects from.
    pub evidence_kind: PyEvidenceKind,
    /// Algebra arena id of the failed constraint that produced this atom.
    pub constraint_id: u32,
    /// Stable semantic kind of that constraint.
    pub constraint_kind: ConstraintKind,
    /// The π path from the focus to the offending value, if any.
    pub path: Option<String>,
    /// The offending node/value, if any.
    pub value: Option<String>,
    /// A short human-readable description.
    pub detail: String,
}

#[pymethods]
impl WitnessAtom {
    fn __repr__(&self) -> String {
        format!(
            "WitnessAtom(kind={:?}, detail={:?})",
            self.kind, self.detail
        )
    }
}

fn witness_atom(
    arena: &ShapeArena,
    shape: ShapeId,
    kind: WitnessKind,
    path: Option<String>,
    value: Option<String>,
    detail: String,
) -> WitnessAtom {
    WitnessAtom {
        kind,
        evidence_kind: match kind {
            WitnessKind::Atom => PyEvidenceKind::AtomFailed,
            WitnessKind::Relational => PyEvidenceKind::RelationalFailed,
            WitnessKind::Closed => PyEvidenceKind::ClosedFailed,
            WitnessKind::CountLow => PyEvidenceKind::CountLow,
            WitnessKind::CountHigh => PyEvidenceKind::CountHigh,
            WitnessKind::Not => PyEvidenceKind::NotFailed,
            WitnessKind::Opaque => PyEvidenceKind::Opaque,
        },
        constraint_id: shape.0,
        constraint_kind: constraint_kind_to_py(shifty_algebra::ConstraintKind::of(arena, shape)),
        path,
        value,
        detail,
    }
}

fn witness_leaves(arena: &ShapeArena, w: &Witness, out: &mut Vec<WitnessAtom>) {
    match w {
        Witness::Atom {
            shape,
            node,
            reached_by,
            produced_by,
            ..
        } => out.push(witness_atom(
            arena,
            *shape,
            WitnessKind::Atom,
            Some(path_str(reached_by)),
            Some(node.to_string()),
            if produced_by.is_some() {
                "value-type test failed (edge is cuttable)".into()
            } else {
                "value-type test failed on the focus".into()
            },
        )),
        Witness::Relational {
            shape,
            kind,
            node,
            offending,
            ..
        } => out.push(witness_atom(
            arena,
            *shape,
            WitnessKind::Relational,
            None,
            Some(node.to_string()),
            format!("{kind:?}: {} offending pair(s)", offending.len()),
        )),
        Witness::Closed {
            shape,
            node,
            offenders,
        } => out.push(witness_atom(
            arena,
            *shape,
            WitnessKind::Closed,
            None,
            Some(node.to_string()),
            format!("{} disallowed triple(s)", offenders.len()),
        )),
        Witness::CountLow {
            shape,
            node,
            path,
            have,
            min,
            ..
        } => out.push(witness_atom(
            arena,
            *shape,
            WitnessKind::CountLow,
            Some(path_str(path)),
            Some(node.to_string()),
            format!("have {have}, need {min}"),
        )),
        Witness::CountHigh {
            shape,
            node,
            path,
            matched,
            max,
            per_value,
            ..
        } => {
            out.push(witness_atom(
                arena,
                *shape,
                WitnessKind::CountHigh,
                Some(path_str(path)),
                Some(node.to_string()),
                format!("{} match(es), max {max}", matched.len()),
            ));
            for (_, sub) in per_value {
                witness_leaves(arena, sub, out);
            }
        }
        Witness::Not { shape, node, .. } => out.push(witness_atom(
            arena,
            *shape,
            WitnessKind::Not,
            None,
            Some(node.to_string()),
            "a shape holds that must be falsified".into(),
        )),
        Witness::Opaque { shape, node, .. } => out.push(witness_atom(
            arena,
            *shape,
            WitnessKind::Opaque,
            None,
            Some(node.to_string()),
            "opaque SPARQL — no algebraic witness".into(),
        )),
        Witness::All { failed, .. } => {
            for f in failed {
                witness_leaves(arena, f, out);
            }
        }
        Witness::Any { branches, .. } => {
            for b in branches {
                witness_leaves(arena, b, out);
            }
        }
    }
}

/// The kind of a holding satisfaction leaf — the enumerated discriminant of
/// [`SatAtom`]. `Match` is a value that satisfied a counted path; `Blocked` a
/// leaf that holds but exposes no enumerable value set (closed / relational /
/// opaque SPARQL); `Coinductive` a gfp back-edge assumed true.
#[pyclass(eq, eq_int, hash, frozen, name = "SatKind")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SatKind {
    Atom,
    Match,
    Not,
    Blocked,
    Coinductive,
}

/// One holding leaf of a satisfaction trace, flattened — the satisfaction-side
/// dual of [`WitnessAtom`]. The AND/OR structure is preserved in `explain`.
#[pyclass(get_all, name = "SatAtom")]
#[derive(Clone)]
pub struct SatAtom {
    /// The leaf kind (see [`SatKind`]).
    pub kind: SatKind,
    /// Exact structured evidence kind whose payload produced this summary row.
    pub evidence_kind: PyEvidenceKind,
    /// The π path to the matched value, if any.
    pub path: Option<String>,
    /// The matched/holding node value, if any.
    pub value: Option<String>,
    /// A short human-readable description.
    pub detail: String,
}

#[pymethods]
impl SatAtom {
    fn __repr__(&self) -> String {
        format!("SatAtom(kind={:?}, detail={:?})", self.kind, self.detail)
    }
}

/// Flatten a satisfaction trace into its holding leaves — the dual of
/// [`witness_leaves`]. Surfaces each value matched along a checked path (the
/// `Match`/`Atom` leaves), so a driver can see *what data* made the focus
/// conform. Closed/relational/opaque leaves appear as `Blocked` (they hold but
/// expose no enumerable value set).
fn sat_leaves(s: &SatTrace, out: &mut Vec<SatAtom>) {
    match s {
        // Vacuously true: nothing was checked, nothing to surface.
        SatTrace::Irrefutable { .. } => {}
        SatTrace::Atom {
            node, reached_by, ..
        } => out.push(SatAtom {
            kind: SatKind::Atom,
            evidence_kind: PyEvidenceKind::AtomHeld,
            path: Some(path_str(reached_by)),
            value: Some(node.to_string()),
            detail: "value-type test holds".into(),
        }),
        SatTrace::CountHeld {
            path,
            matches,
            min,
            max,
            ..
        } => {
            let bounds = match (min, max) {
                (Some(lo), Some(hi)) => format!("[{lo}..{hi}]"),
                (Some(lo), None) => format!("[{lo}..]"),
                (None, Some(hi)) => format!("[..{hi}]"),
                (None, None) => "[..]".into(),
            };
            for (v, _, _) in matches {
                out.push(SatAtom {
                    kind: SatKind::Match,
                    evidence_kind: PyEvidenceKind::CountHeld,
                    path: Some(path_str(path)),
                    value: Some(v.to_string()),
                    detail: format!("matched value (count {bounds})"),
                });
            }
        }
        SatTrace::ForAllHeld { path, values, .. } => {
            for (value, _, trace) in values {
                out.push(SatAtom {
                    kind: SatKind::Match,
                    evidence_kind: PyEvidenceKind::AllValuesHeld,
                    path: Some(path_str(path)),
                    value: Some(value.to_string()),
                    detail: "checked value satisfies universal qualifier".into(),
                });
                sat_leaves(trace, out);
            }
        }
        SatTrace::AllHeld { children, .. } => {
            for c in children {
                sat_leaves(c, out);
            }
        }
        SatTrace::AnyHeld { satisfied, .. } => {
            for c in satisfied {
                sat_leaves(c, out);
            }
        }
        SatTrace::NotHeld { node, .. } => out.push(SatAtom {
            kind: SatKind::Not,
            evidence_kind: PyEvidenceKind::NotHeld,
            path: None,
            value: Some(node.to_string()),
            detail: "negation holds (the inner shape fails)".into(),
        }),
        SatTrace::Blocked { node, reason, .. } => out.push(SatAtom {
            kind: SatKind::Blocked,
            evidence_kind: PyEvidenceKind::Blocked,
            path: None,
            value: Some(node.to_string()),
            detail: format!("holds, no enumerable values ({reason:?})"),
        }),
        SatTrace::Coinductive { node, .. } => out.push(SatAtom {
            kind: SatKind::Coinductive,
            evidence_kind: PyEvidenceKind::Coinductive,
            path: None,
            value: Some(node.to_string()),
            detail: "assumed (gfp back-edge)".into(),
        }),
    }
}

// ── term parsing (for hole bindings) ────────────────────────────────────────────

/// Parse a single RDF term in N-Triples syntax — exactly what `Hole.candidates()`
/// returns, so a binding round-trips: `<iri>`, `"lit"`, `"lit"^^<dt>`, `"x"@en`,
/// `_:b`.
fn parse_term(s: &str) -> Result<Term, String> {
    let line = format!("<urn:x:s> <urn:x:p> {s} .");
    let loaded = shifty_parse::load_ntriples(line.as_bytes())
        .map_err(|e| format!("cannot parse term {s:?}: {e}"))?;
    loaded
        .graph
        .iter()
        .next()
        .map(|t| t.object.into_owned())
        .ok_or_else(|| format!("cannot parse term {s:?}"))
}

// ── the session ─────────────────────────────────────────────────────────────────

/// A repair session over one (schema, data-graph) pair. Holds the inferred
/// evaluation graph; reusable and immutable. `advance(delta)` returns a *new*
/// session over `G ⊕ ΔG` so a driver can step its own fixpoint loop.
#[pyclass(name = "RepairSession")]
pub struct RepairSession {
    schema: Arc<Schema>,
    provenance_schema: Arc<Schema>,
    provenance_statement_map: Arc<Vec<usize>>,
    /// The focus/output graph: data (+ inferred triples). What `to_graph` emits
    /// and where focus nodes and reuse candidates are drawn from.
    data: Arc<Graph>,
    /// The evaluation graph paths and class hierarchy read: `data ∪ shapes`. The
    /// shapes/ontology triples live here so `sh:class` can follow `subClassOf`
    /// into the hierarchy, but they never leak into the emitted data graph.
    context: Arc<Graph>,
    diagnostics: Vec<String>,
}

impl RepairSession {
    fn from_parts(
        schema: Arc<Schema>,
        provenance_schema: Arc<Schema>,
        provenance_statement_map: Arc<Vec<usize>>,
        data: Arc<Graph>,
        context: Arc<Graph>,
        diagnostics: Vec<String>,
    ) -> Self {
        Self {
            schema,
            provenance_schema,
            provenance_statement_map,
            data,
            context,
            diagnostics,
        }
    }

    /// Resolve a shape IRI (angle brackets optional) to its arena slot, erroring
    /// if the schema names no such shape. Shared by the shape-scoped queries.
    fn resolve_shape(&self, shape_iri: &str) -> PyResult<ShapeId> {
        let iri = shape_iri
            .trim()
            .trim_start_matches('<')
            .trim_end_matches('>');
        shape_id_for_iri(&self.schema, iri)
            .ok_or_else(|| py_value_error(format!("no shape named <{iri}> in the schema")))
    }

    fn provenance_statement(&self, raw_statement: usize) -> PyResult<usize> {
        self.provenance_statement_map
            .get(raw_statement)
            .copied()
            .ok_or_else(|| {
                py_value_error(format!(
                    "no provenance mapping for statement {raw_statement}"
                ))
            })
    }

    fn focus_witness_to_py(&self, py: Python<'_>, fw: IrFocus) -> PyResult<Py<FocusWitness>> {
        let statement_id = self.provenance_statement(fw.statement)?;
        let provenance_statement = self
            .provenance_schema
            .statements
            .get(statement_id)
            .ok_or_else(|| {
                py_value_error(format!(
                    "provenance statement {statement_id} is out of bounds"
                ))
            })?;
        let target = shifty_algebra::render::selector_to_string_in(
            &self.schema.statements[fw.statement].selector,
            &self.schema.arena,
        );
        Py::new(
            py,
            FocusWitness {
                focus: fw.focus.to_string(),
                statement: fw.statement,
                statement_id,
                constraint_id: provenance_statement.shape.0,
                constraint_kind: constraint_kind_to_py(shifty_algebra::ConstraintKind::of(
                    &self.provenance_schema.arena,
                    provenance_statement.shape,
                )),
                constraint: constraint_to_py(
                    py,
                    &self.provenance_schema.arena,
                    provenance_statement.shape,
                )?,
                target,
                inner: fw,
                schema: Arc::clone(&self.schema),
                selector_schema: Arc::clone(&self.schema),
                data: Arc::clone(&self.data),
            },
        )
    }
}

#[pymethods]
impl RepairSession {
    #[new]
    #[pyo3(signature = (
        shapes=None,
        shapes_path=None,
        shapes_format="auto",
        data=None,
        data_path=None,
        data_format="auto",
        run_infer=true,
        base=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        shapes: Option<PyBackedBytes>,
        shapes_path: Option<String>,
        shapes_format: &str,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        run_infer: bool,
        base: Option<String>,
    ) -> PyResult<Self> {
        let shapes_spec =
            InputSpec::new(shapes, shapes_path, shapes_format, "shapes").map_err(py_value_error)?;
        let data_spec = match (data, data_path) {
            (None, None) => None,
            (data, path) => {
                Some(InputSpec::new(data, path, data_format, "data").map_err(py_value_error)?)
            }
        };
        py.allow_threads(move || {
            let shapes_loaded = shapes_spec.load(base.as_deref())?;
            let parse_out = shifty_parse::parse_loaded(&shapes_loaded);
            parse_out
                .require_valid()
                .map_err(|error| error.to_string())?;
            let diagnostics = parse_out
                .diagnostics
                .iter()
                .map(ToString::to_string)
                .collect();
            let schema = parse_out.schema;
            let provenance = shifty_opt::normalize_with_mapping(&schema);
            let provenance_schema = provenance.schema;
            let provenance_statement_map = provenance.statement_map;

            let data_loaded = data_spec
                .map(|spec| spec.load(base.as_deref()))
                .transpose()?;
            let base_data = data_loaded
                .as_ref()
                .map_or(&shapes_loaded.graph, |d| &d.graph);

            // Mirror the CLI: run SHACL-AF inference before witnessing.
            let eval = if run_infer && !schema.rules.is_empty() {
                let out = match data_loaded.as_ref() {
                    Some(_) => {
                        shifty_engine::infer_graphs(base_data, &shapes_loaded.graph, &schema)
                    }
                    None => shifty_engine::infer(&shapes_loaded.graph, &schema),
                }
                .map_err(|e| format!("non-stratifiable schema: {e}"))?;
                out.graph
            } else {
                base_data.clone()
            };

            // Evaluation reads `data ∪ shapes` so paths and the class hierarchy
            // (e.g. `rdfs:subClassOf` for `sh:class`) resolve against the
            // shapes/ontology graph, while focus discovery and the emitted graph
            // stay the data graph. When the shapes embed the data, `eval` already
            // is the union, so share the Arc rather than building a second copy.
            let data = Arc::new(eval);
            let context = if data_loaded.is_some() {
                Arc::new(graph_union(&data, &shapes_loaded.graph))
            } else {
                Arc::clone(&data)
            };

            Ok(RepairSession::from_parts(
                Arc::new(schema),
                Arc::new(provenance_schema),
                Arc::new(provenance_statement_map),
                data,
                context,
                diagnostics,
            ))
        })
        .map_err(py_value_error)
    }

    #[getter]
    fn diagnostics(&self) -> Vec<String> {
        self.diagnostics.clone()
    }

    /// The horizon: one [`FocusWitness`] per `(focus, failed statement)`.
    fn witnesses(&self, py: Python<'_>) -> PyResult<Vec<Py<FocusWitness>>> {
        let raw = py
            .allow_threads(|| witness_violations(&self.data, &self.context, &self.schema))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for fw in raw {
            let statement_id = self.provenance_statement(fw.statement)?;
            if !seen.insert((fw.focus.clone(), statement_id)) {
                continue;
            }
            out.push(self.focus_witness_to_py(py, fw)?);
        }
        Ok(out)
    }

    /// The violation horizon for a single shape: one [`FocusWitness`] per failing
    /// `(focus, statement)` whose statement targets `shape_iri` (matched against
    /// the schema's shape IRIs; angle brackets optional). The shape-scoped
    /// counterpart of `witnesses()`; its satisfaction-side dual is
    /// `satisfactions_for`. Raises if no shape is named `shape_iri`.
    fn witnesses_for(&self, py: Python<'_>, shape_iri: &str) -> PyResult<Vec<Py<FocusWitness>>> {
        let shape = self.resolve_shape(shape_iri)?;
        let raw = py
            .allow_threads(|| witness_shape(&self.data, &self.context, &self.schema, shape))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for fw in raw {
            let statement_id = self.provenance_statement(fw.statement)?;
            if !seen.insert((fw.focus.clone(), statement_id)) {
                continue;
            }
            out.push(self.focus_witness_to_py(py, fw)?);
        }
        Ok(out)
    }

    /// The satisfaction horizon for a single shape: one [`FocusSatisfaction`] per
    /// *passing* `(focus, statement)` whose statement targets `shape_iri` — the
    /// dual of `witnesses_for`. Each entry records why the focus conforms,
    /// including the values matched along every checked path. Raises if no shape
    /// is named `shape_iri`.
    fn satisfactions_for(
        &self,
        py: Python<'_>,
        shape_iri: &str,
    ) -> PyResult<Vec<Py<FocusSatisfaction>>> {
        let shape = self.resolve_shape(shape_iri)?;
        let raw = py
            .allow_threads(|| satisfy_shape(&self.data, &self.context, &self.schema, shape))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        raw.into_iter()
            .map(|fs| {
                let statement_id = self.provenance_statement(fs.statement)?;
                let provenance_statement = &self.provenance_schema.statements[statement_id];
                let target = shifty_algebra::render::selector_to_string_in(
                    &self.schema.statements[fs.statement].selector,
                    &self.schema.arena,
                );
                Py::new(
                    py,
                    FocusSatisfaction {
                        focus: fs.focus.to_string(),
                        statement: fs.statement,
                        statement_id,
                        constraint_id: provenance_statement.shape.0,
                        constraint_kind: constraint_kind_to_py(shifty_algebra::ConstraintKind::of(
                            &self.provenance_schema.arena,
                            provenance_statement.shape,
                        )),
                        constraint: constraint_to_py(
                            py,
                            &self.provenance_schema.arena,
                            provenance_statement.shape,
                        )?,
                        target,
                        inner: fs,
                        selector_schema: Arc::clone(&self.schema),
                    },
                )
            })
            .collect()
    }

    /// Re-validate `G ⊕ ΔG` and diff the violation set against `G`'s — the gate.
    /// Decides and applies nothing; returns a [`RepairOutcome`].
    fn gate(&self, py: Python<'_>, delta: &RepairDelta) -> PyResult<RepairOutcome> {
        let outcome = py
            .allow_threads(|| engine_gate(&self.data, &self.context, &self.schema, &delta.inner))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        let sound = outcome.is_sound();
        let progress = outcome.is_progress();
        let to_py = |vs: &[shifty_engine::Violation]| -> PyResult<Vec<Py<Violation>>> {
            vs.iter()
                .map(|v| violation_to_py(py, v, &self.schema))
                .collect()
        };
        Ok(RepairOutcome {
            is_sound: sound,
            is_progress: progress,
            fixed: to_py(&outcome.fixed)?,
            introduced: to_py(&outcome.introduced)?,
            remaining: to_py(&outcome.remaining)?,
        })
    }

    /// The session's current graph as an N-Triples string (the Python layer
    /// parses it to rdflib). After `advance`, this is `G` with every accepted
    /// `ΔG` applied.
    fn current_ntriples(&self, py: Python<'_>) -> String {
        py.allow_threads(|| graph_to_ntriples(&self.data))
    }

    /// `G ⊕ ΔG` as an N-Triples string (the Python layer parses it to rdflib).
    fn apply_ntriples(&self, py: Python<'_>, delta: &RepairDelta) -> String {
        py.allow_threads(|| {
            let g = engine_apply(&self.data, &delta.inner);
            graph_to_ntriples(&g)
        })
    }

    /// Synthesize a repair tree that makes `node` conform to sub-shape `shape_id`
    /// — the building block for repairing a `conforms to @N` hole: bind the hole
    /// to a (fresh) node, then build that node out with this tree. Returns `None`
    /// if the node already conforms. `shape_id` is the integer from
    /// [`Hole.conforms_to`].
    fn repair_node_against(
        &self,
        py: Python<'_>,
        node: &str,
        shape_id: u32,
    ) -> PyResult<Option<RepairTree>> {
        let term = parse_term(node).map_err(py_value_error)?;
        let fw = py
            .allow_threads(|| witness_node(&self.context, &self.schema, &term, ShapeId(shape_id)))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        Ok(fw.map(|fw| {
            let synthesized = synthesize_with_origins(&self.schema.arena, &fw);
            RepairTree {
                inner: synthesized.tree,
                origins: synthesized.origins,
                schema: Arc::clone(&self.schema),
                data: Arc::clone(&self.data),
            }
        }))
    }

    /// A fully-expanded, human-readable definition of shape `shape_id` (the
    /// integer from `Hole.conforms_to` / `Hole.conforms_to_shapes`): every child
    /// shape inlined, no `@id` pointers. The lookup a driver uses to understand
    /// what a `conforms to` hole actually demands.
    fn describe_shape(&self, shape_id: u32) -> String {
        shifty_algebra::render::describe_shape(&self.schema.arena, ShapeId(shape_id))
    }

    /// A new session over `G ⊕ ΔG` (same schema, no re-inference) so a driver can
    /// accept a repair and re-witness from the patched graph. The delta patches
    /// both the data graph and the evaluation context (which contains it), so the
    /// next session evaluates against `(data ⊕ ΔG) ∪ shapes`.
    fn advance(&self, py: Python<'_>, delta: &RepairDelta) -> Self {
        let (next_data, next_context) = py.allow_threads(|| {
            (
                engine_apply(&self.data, &delta.inner),
                engine_apply(&self.context, &delta.inner),
            )
        });
        RepairSession::from_parts(
            Arc::clone(&self.schema),
            Arc::clone(&self.provenance_schema),
            Arc::clone(&self.provenance_statement_map),
            Arc::new(next_data),
            Arc::new(next_context),
            self.diagnostics.clone(),
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "RepairSession(statements={}, triples={})",
            self.schema.statements.len(),
            self.data.len()
        )
    }
}

/// What a statement's target selector picks out — the enumerated discriminant of
/// [`Target`]. `Class` is an `sh:targetClass`/implicit class target;
/// `SubjectsOf`/`ObjectsOf` are `sh:targetSubjectsOf`/`sh:targetObjectsOf`;
/// `Node` an `sh:targetNode`; `Path` a generic path target; `Sparql` a
/// SPARQL-based target.
#[pyclass(eq, eq_int, hash, frozen, name = "TargetKind")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum TargetKind {
    Class,
    SubjectsOf,
    ObjectsOf,
    Node,
    Path,
    Sparql,
}

/// A statement's target selector, decomposed for inspection: a [`TargetKind`]
/// discriminant plus the salient term(s), alongside the rendered string. The
/// structured counterpart of `FocusWitness.target` / `FocusSatisfaction.target`.
#[pyclass(get_all, name = "Target")]
#[derive(Clone)]
pub struct Target {
    /// What the selector targets (see [`TargetKind`]).
    pub kind: TargetKind,
    /// The salient term in N-Triples syntax: the class IRI (`Class`), the
    /// predicate (`SubjectsOf`/`ObjectsOf`), or the node (`Node`). `None` for
    /// `Path`/`Sparql`, whose payload is structural.
    pub value: Option<String>,
    /// The rendered π path, for a `Path` or `Class` target (else `None`).
    pub path: Option<String>,
    /// The whole selector rendered (e.g. `class(ex:Person)`) — the same string as
    /// the owning witness's `.target`.
    pub render: String,
}

#[pymethods]
impl Target {
    fn __repr__(&self) -> String {
        format!("Target(kind={:?}, render={:?})", self.kind, self.render)
    }

    fn __str__(&self) -> String {
        self.render.clone()
    }
}

/// Decompose a [`Selector`] into a structured [`Target`], resolving class targets
/// and qualifiers against the schema arena.
fn build_target(sel: &Selector, schema: &Schema) -> Target {
    let render = shifty_algebra::render::selector_to_string_in(sel, &schema.arena);
    if let Some(class) = shifty_algebra::render::class_target(sel, &schema.arena) {
        let path = match sel {
            Selector::HasPath(p, _) => Some(path_str(p)),
            _ => None,
        };
        return Target {
            kind: TargetKind::Class,
            value: Some(class.to_string()),
            path,
            render,
        };
    }
    match sel {
        Selector::HasOut(q) => Target {
            kind: TargetKind::SubjectsOf,
            value: Some(q.to_string()),
            path: None,
            render,
        },
        Selector::HasIn(q) => Target {
            kind: TargetKind::ObjectsOf,
            value: Some(q.to_string()),
            path: None,
            render,
        },
        Selector::IsConst(t) => Target {
            kind: TargetKind::Node,
            value: Some(t.to_string()),
            path: None,
            render,
        },
        Selector::HasPath(p, _) => Target {
            kind: TargetKind::Path,
            value: None,
            path: Some(path_str(p)),
            render,
        },
        Selector::Sparql(_) => Target {
            kind: TargetKind::Sparql,
            value: None,
            path: None,
            render,
        },
    }
}

#[pyclass(get_all, frozen, name = "EvidenceNode")]
pub struct PyEvidenceNode {
    /// Exact typed discriminant of this structured node.
    pub evidence_kind: PyEvidenceKind,
    /// Compatibility spelling of `evidence_kind` in snake case.
    pub kind: String,
    pub status: String,
    pub constraint_id: u32,
    json: String,
}

#[pymethods]
impl PyEvidenceNode {
    fn to_json(&self) -> String {
        self.json.clone()
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(py
            .import("json")?
            .call_method1("loads", (&self.json,))?
            .unbind())
    }
}

#[pyclass(name = "PathSupport")]
pub struct PyPathSupport {
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    triple: Option<String>,
    children: Vec<Py<PyPathSupport>>,
    json: String,
}

#[pymethods]
impl PyPathSupport {
    #[getter]
    fn children(&self, py: Python<'_>) -> Vec<Py<PyPathSupport>> {
        self.children
            .iter()
            .map(|value| value.clone_ref(py))
            .collect()
    }

    fn to_json(&self) -> String {
        self.json.clone()
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(py
            .import("json")?
            .call_method1("loads", (&self.json,))?
            .unbind())
    }
}

/// One cardinality deficit, with everything needed to describe the edge that
/// would close it: `node` has `observed_count` values along `path` satisfying
/// `qualifier`, and needs `required_count`.
#[pyclass(frozen, name = "MissingObligation")]
pub struct PyMissingObligation {
    /// Arena id of the count constraint that came up short.
    #[pyo3(get)]
    pub constraint_id: u32,
    /// The node the deficit is about. Not always the focus — a count nested
    /// inside a rejected candidate reports its own node — so compare it against
    /// the focus when you mean deficits on the focus itself.
    #[pyo3(get)]
    pub node: String,
    /// The path the values were counted along, rendered (`ex:p`, `^ex:p`,
    /// `<http://ex/a>/<http://ex/b>`). Pass it straight to `values_for_path` to
    /// see which values are already there.
    #[pyo3(get)]
    pub path: String,
    /// How many values along `path` already satisfy `qualifier`.
    #[pyo3(get)]
    pub observed_count: u64,
    /// How many the constraint demands.
    #[pyo3(get)]
    pub required_count: u64,
    /// `required_count - observed_count`: how many more values would close it.
    #[pyo3(get)]
    pub missing: u64,
    qualifier: Py<Constraint>,
}

#[pymethods]
impl PyMissingObligation {
    /// What each counted value must satisfy — the structured constraint an
    /// added value has to conform to for the count to move. Its `id` is the
    /// arena id accepted by `RepairSession.describe_shape`.
    #[getter]
    fn qualifier(&self, py: Python<'_>) -> Py<Constraint> {
        self.qualifier.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "MissingObligation(node={:?}, path={:?}, missing={})",
            self.node, self.path, self.missing
        )
    }
}

fn evidence_nodes(value: &IrEvidence) -> Vec<PyEvidenceNode> {
    value
        .walk()
        .into_iter()
        .map(|node| {
            let evidence = match node {
                shifty_engine::EvidenceNodeRef::Satisfaction(value) => {
                    IrEvidence::Satisfaction(value.clone())
                }
                shifty_engine::EvidenceNodeRef::Failure(value) => {
                    IrEvidence::Failure(value.clone())
                }
            };
            let evidence_kind = PyEvidenceKind::from(node.evidence_kind());
            PyEvidenceNode {
                evidence_kind,
                kind: evidence_kind.as_str().to_string(),
                status: evidence_kind.status_str().to_string(),
                constraint_id: node.constraint_id().0,
                json: evidence.to_json().expect("evidence node is serializable"),
            }
        })
        .collect()
}

fn path_support_to_py(py: Python<'_>, value: IrPathSupport) -> PyResult<Py<PyPathSupport>> {
    let (kind, triple, nested) = match &value {
        IrPathSupport::Empty => ("empty", None, Vec::new()),
        IrPathSupport::Edge(triple) => ("edge", Some(triple.to_string()), Vec::new()),
        IrPathSupport::Chain(children) => ("chain", None, children.clone()),
        IrPathSupport::Alt(children) => ("alt", None, children.clone()),
    };
    let children = nested
        .into_iter()
        .map(|child| path_support_to_py(py, child))
        .collect::<PyResult<Vec<_>>>()?;
    let json = serde_json::to_string(&value)
        .map_err(|error| py_value_error(format!("cannot serialize path support: {error}")))?;
    Py::new(
        py,
        PyPathSupport {
            kind: kind.to_string(),
            triple,
            children,
            json,
        },
    )
}

/// Does `query` name `path`? Accepts the rendered spelling `path_str` produces
/// (`ex:p`, `^ex:p`, `ex:a/ex:b`) and, for a single predicate step, the bare or
/// bracketed IRI — so a caller holding an IRI need not know the prefix table.
fn path_matches(path: &shifty_algebra::Path, query: &str) -> bool {
    if path_str(path) == query {
        return true;
    }
    match path {
        shifty_algebra::Path::Pred(pred) => {
            let iri = pred.as_str();
            query == iri
                || query
                    .strip_prefix('<')
                    .and_then(|inner| inner.strip_suffix('>'))
                    == Some(iri)
        }
        _ => false,
    }
}

/// Matched values counted along `query`, read from the structured match records
/// and rendered as RDF terms in match order without duplicates.
fn evidence_values_for_path(value: &IrEvidence, query: &str) -> Vec<String> {
    let query = query.trim();
    let mut seen = HashSet::new();
    value
        .matched_values_by_path()
        .into_iter()
        .filter(|(path, _)| path_matches(path, query))
        .flat_map(|(_, values)| values)
        .map(|value| value.to_string())
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn evidence_to_dict(py: Python<'_>, value: &IrEvidence) -> PyResult<Py<PyAny>> {
    let json = value
        .to_json()
        .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))?;
    Ok(py.import("json")?.call_method1("loads", (json,))?.unbind())
}

/// Why one focus node failed one statement, plus its repair tree.
#[pyclass(name = "Failure")]
pub struct FocusWitness {
    #[pyo3(get)]
    focus: String,
    #[pyo3(get)]
    statement: usize,
    /// Statement id shared with validation violations and reasons.
    #[pyo3(get)]
    statement_id: usize,
    /// Algebra arena id for the statement's top-level shape.
    #[pyo3(get)]
    constraint_id: u32,
    /// Stable semantic kind for the statement's top-level constraint.
    #[pyo3(get)]
    constraint_kind: ConstraintKind,
    /// The statement's top-level algebraic constraint/operator.
    #[pyo3(get)]
    constraint: Py<Constraint>,
    /// The statement's target selector, rendered (e.g. `class(ex:Person)`). See
    /// `selector` for the structured form.
    #[pyo3(get)]
    target: String,
    inner: IrFocus,
    schema: Arc<Schema>,
    selector_schema: Arc<Schema>,
    data: Arc<Graph>,
}

#[pymethods]
impl FocusWitness {
    /// The IRI of the statement's source shape, when the shape is a named
    /// (non-blank) RDF node. `None` for anonymous shapes.
    #[getter]
    fn shape_name(&self) -> Option<String> {
        let statement = &self.selector_schema.statements[self.statement];
        self.selector_schema
            .name_of(statement.shape)
            .map(str::to_string)
    }

    /// The target selector as a structured [`Target`] (its `kind`, the class /
    /// predicate / node it picks out, …) — the inspectable form of `.target`.
    #[getter]
    fn selector(&self) -> Target {
        build_target(
            &self.selector_schema.statements[self.statement].selector,
            &self.selector_schema,
        )
    }

    /// The failing leaves, flattened (AND/OR structure dropped; see `explain`).
    fn summary(&self) -> Vec<WitnessAtom> {
        let mut out = Vec::new();
        witness_leaves(&self.schema.arena, &self.inner.failure, &mut out);
        out
    }

    /// The full witness tree, rendered as indented text.
    fn explain(&self) -> String {
        let mut out = Vec::new();
        render_witness(&self.inner.failure, 0, &mut out);
        out.join("\n")
    }

    fn walk(&self) -> Vec<PyEvidenceNode> {
        evidence_nodes(&IrEvidence::Failure(self.inner.failure.clone()))
    }

    fn supporting_triples(&self) -> Vec<String> {
        IrEvidence::Failure(self.inner.failure.clone())
            .supporting_triples()
            .into_iter()
            .map(|value| value.to_string())
            .collect()
    }

    fn path_supports(&self, py: Python<'_>) -> PyResult<Vec<Py<PyPathSupport>>> {
        IrEvidence::Failure(self.inner.failure.clone())
            .path_supports()
            .into_iter()
            .map(|value| path_support_to_py(py, value))
            .collect()
    }

    /// Every value that qualified or was checked successfully along a counted
    /// path, deduplicated in traversal order. `values_for_path` narrows this to
    /// one path; `offending_values` is the complementary view.
    fn matched_values(&self) -> Vec<String> {
        IrEvidence::Failure(self.inner.failure.clone())
            .matched_values()
            .into_iter()
            .map(|value| value.to_string())
            .collect()
    }

    /// The subset of `matched_values` counted along `path`, in match order and
    /// without duplicates. `path` is the rendered form (`ex:p`, `^ex:p`,
    /// `ex:a/ex:b`) or, for a single predicate step, its IRI with or without
    /// angle brackets. Empty when nothing was counted along `path`.
    fn values_for_path(&self, path: &str) -> Vec<String> {
        evidence_values_for_path(&IrEvidence::Failure(self.inner.failure.clone()), path)
    }

    /// The cardinality deficits in this failure, each describing the edge that
    /// would close it — see [`PyMissingObligation`]. Every count that came up
    /// short is reported, including ones nested inside a rejected candidate, so
    /// compare `node` against `focus` when you mean the focus's own deficits.
    fn missing_obligations(&self, py: Python<'_>) -> PyResult<Vec<PyMissingObligation>> {
        IrEvidence::Failure(self.inner.failure.clone())
            .missing_obligations()
            .into_iter()
            .map(|value| {
                Ok(PyMissingObligation {
                    constraint_id: value.constraint_id.0,
                    node: value.node.to_string(),
                    path: path_str(&value.path),
                    observed_count: value.observed_count,
                    required_count: value.required_count,
                    missing: value.missing,
                    qualifier: constraint_to_py(py, &self.schema.arena, value.qualifier)?,
                })
            })
            .collect()
    }

    fn offending_values(&self) -> Vec<String> {
        IrEvidence::Failure(self.inner.failure.clone())
            .offending_values()
            .into_iter()
            .map(|value| value.to_string())
            .collect()
    }

    fn source_constraints(&self) -> Vec<u32> {
        vec![self.selector_schema.statements[self.statement].shape.0]
    }

    /// The IRI of the shape whose statement selected this focus, or `None` when
    /// that shape came from a blank node.
    #[getter]
    fn shape_iri(&self) -> Option<String> {
        statement_shape_iri(&self.selector_schema, self.statement)
    }

    fn to_json(&self) -> PyResult<String> {
        IrEvidence::Failure(self.inner.failure.clone())
            .to_json()
            .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        evidence_to_dict(py, &IrEvidence::Failure(self.inner.failure.clone()))
    }

    /// Synthesize the repair space (`RepairTree`) for this violation.
    fn repair_tree(&self, py: Python<'_>) -> RepairTree {
        let synthesized =
            py.allow_threads(|| synthesize_with_origins(&self.schema.arena, &self.inner));
        RepairTree {
            inner: synthesized.tree,
            origins: synthesized.origins,
            schema: Arc::clone(&self.schema),
            data: Arc::clone(&self.data),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Failure(focus={:?}, statement={})",
            self.focus, self.statement
        )
    }
}

/// Why one focus node *satisfies* a statement: the satisfaction-side dual of
/// [`FocusWitness`]. Carries why the node conforms, including the values matched
/// along each checked path. Yielded by [`RepairSession::satisfactions_for`].
#[pyclass(name = "Satisfaction")]
pub struct FocusSatisfaction {
    #[pyo3(get)]
    focus: String,
    #[pyo3(get)]
    statement: usize,
    /// Normalized/deduplicated statement identity.
    #[pyo3(get)]
    statement_id: usize,
    /// Algebra arena id for the normalized top-level constraint.
    #[pyo3(get)]
    constraint_id: u32,
    /// Stable semantic kind for the normalized top-level constraint.
    #[pyo3(get)]
    constraint_kind: ConstraintKind,
    /// The normalized top-level algebraic constraint/operator.
    #[pyo3(get)]
    constraint: Py<Constraint>,
    /// The statement's target selector, rendered (e.g. `class(ex:Person)`). See
    /// `selector` for the structured form.
    #[pyo3(get)]
    target: String,
    inner: IrSat,
    selector_schema: Arc<Schema>,
}

#[pymethods]
impl FocusSatisfaction {
    /// The IRI of the statement's source shape, when the shape is a named
    /// (non-blank) RDF node. `None` for anonymous shapes.
    #[getter]
    fn shape_name(&self) -> Option<String> {
        let statement = &self.selector_schema.statements[self.statement];
        self.selector_schema
            .name_of(statement.shape)
            .map(str::to_string)
    }

    /// The target selector as a structured [`Target`] — the inspectable form of
    /// `.target`, identical to the witness side for the same statement.
    #[getter]
    fn selector(&self) -> Target {
        build_target(
            &self.selector_schema.statements[self.statement].selector,
            &self.selector_schema,
        )
    }

    /// The satisfying leaves, flattened: one [`SatAtom`] per matched value /
    /// value-type test that held (AND/OR structure dropped; see `explain`).
    fn summary(&self) -> Vec<SatAtom> {
        let mut out = Vec::new();
        sat_leaves(&self.inner.trace, &mut out);
        out
    }

    /// The full satisfaction trace, rendered as indented text.
    fn explain(&self) -> String {
        let mut out = Vec::new();
        render_sat(&self.inner.trace, 0, &mut out);
        out.join("\n")
    }

    fn walk(&self) -> Vec<PyEvidenceNode> {
        evidence_nodes(&IrEvidence::Satisfaction(self.inner.trace.clone()))
    }

    fn supporting_triples(&self) -> Vec<String> {
        IrEvidence::Satisfaction(self.inner.trace.clone())
            .supporting_triples()
            .into_iter()
            .map(|value| value.to_string())
            .collect()
    }

    fn path_supports(&self, py: Python<'_>) -> PyResult<Vec<Py<PyPathSupport>>> {
        IrEvidence::Satisfaction(self.inner.trace.clone())
            .path_supports()
            .into_iter()
            .map(|value| path_support_to_py(py, value))
            .collect()
    }

    /// Every value that qualified or was checked successfully along a counted
    /// path, deduplicated in traversal order — the values that make this focus
    /// conform. `values_for_path` narrows this to one path.
    fn matched_values(&self) -> Vec<String> {
        IrEvidence::Satisfaction(self.inner.trace.clone())
            .matched_values()
            .into_iter()
            .map(|value| value.to_string())
            .collect()
    }

    /// The subset of `matched_values` counted along `path`, in match order and
    /// without duplicates. `path` is the rendered form (`ex:p`, `^ex:p`,
    /// `ex:a/ex:b`) or, for a single predicate step, its IRI with or without
    /// angle brackets. Empty when nothing was counted along `path`.
    fn values_for_path(&self, path: &str) -> Vec<String> {
        evidence_values_for_path(&IrEvidence::Satisfaction(self.inner.trace.clone()), path)
    }

    /// Always empty: a satisfaction has no unmet cardinality. Present so the
    /// two polarities answer the same projections.
    fn missing_obligations(&self) -> Vec<PyMissingObligation> {
        Vec::new()
    }

    fn offending_values(&self) -> Vec<String> {
        Vec::new()
    }

    fn source_constraints(&self) -> Vec<u32> {
        vec![self.selector_schema.statements[self.statement].shape.0]
    }

    /// The IRI of the shape whose statement selected this focus, or `None` when
    /// that shape came from a blank node.
    #[getter]
    fn shape_iri(&self) -> Option<String> {
        statement_shape_iri(&self.selector_schema, self.statement)
    }

    fn to_json(&self) -> PyResult<String> {
        IrEvidence::Satisfaction(self.inner.trace.clone())
            .to_json()
            .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        evidence_to_dict(py, &IrEvidence::Satisfaction(self.inner.trace.clone()))
    }

    fn __repr__(&self) -> String {
        format!(
            "Satisfaction(focus={:?}, statement={})",
            self.focus, self.statement
        )
    }
}

/// One selected `(authored statement, focus)` pair and exactly one evidence
/// polarity.
#[pyclass(name = "FocusEvaluation")]
pub struct FocusEvidence {
    #[pyo3(get)]
    focus: String,
    #[pyo3(get)]
    status: String,
    satisfaction: Option<Py<FocusSatisfaction>>,
    failure: Option<Py<FocusWitness>>,
    progress: Option<Py<PyEvaluationProgress>>,
}

#[pymethods]
impl FocusEvidence {
    #[getter]
    fn satisfaction(&self, py: Python<'_>) -> Option<Py<FocusSatisfaction>> {
        self.satisfaction.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn failure(&self, py: Python<'_>) -> Option<Py<FocusWitness>> {
        self.failure.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn evidence(&self, py: Python<'_>) -> Py<PyAny> {
        match (&self.satisfaction, &self.failure) {
            (Some(value), None) => value.clone_ref(py).into_any(),
            (None, Some(value)) => value.clone_ref(py).into_any(),
            _ => unreachable!("focus evaluation has exactly one evidence polarity"),
        }
    }

    #[getter]
    fn progress(&self, py: Python<'_>) -> Option<Py<PyEvaluationProgress>> {
        self.progress.as_ref().map(|value| value.clone_ref(py))
    }

    fn __repr__(&self) -> String {
        format!(
            "FocusEvaluation(focus={:?}, status={:?})",
            self.focus, self.status
        )
    }
}

#[pyclass(get_all, frozen, name = "ChildEvaluation")]
pub struct PyChildEvaluation {
    source_constraint_ref: u32,
    normalized_constraint_ref: Option<u32>,
    status: String,
    constraint_kind: ConstraintKind,
}

#[pyclass(name = "EvaluationProgress")]
pub struct PyEvaluationProgress {
    evaluated_children: Vec<Py<PyChildEvaluation>>,
}

#[pymethods]
impl PyEvaluationProgress {
    #[getter]
    fn evaluated_children(&self, py: Python<'_>) -> Vec<Py<PyChildEvaluation>> {
        self.evaluated_children
            .iter()
            .map(|value| value.clone_ref(py))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "EvaluationProgress(evaluated_children={})",
            self.evaluated_children.len()
        )
    }
}

/// One authored statement and every focus selected by it, including an empty
/// list when target selection found nothing.
#[pyclass(name = "StatementEvaluation")]
pub struct PyStatementEvaluation {
    #[pyo3(get)]
    source_statement_id: usize,
    #[pyo3(get)]
    normalized_statement_id: Option<usize>,
    #[pyo3(get)]
    source_constraint_id: u32,
    #[pyo3(get)]
    normalized_constraint_id: Option<u32>,
    #[pyo3(get)]
    constraint_kind: ConstraintKind,
    #[pyo3(get)]
    constraint: Py<Constraint>,
    #[pyo3(get)]
    target: String,
    selected_foci: Vec<Py<FocusEvidence>>,
    selector_schema: Arc<Schema>,
}

#[pymethods]
impl PyStatementEvaluation {
    /// The IRI of the statement's source shape, when the shape is a named
    /// (non-blank) RDF node. `None` for anonymous shapes.
    #[getter]
    fn shape_name(&self) -> Option<String> {
        let statement = &self.selector_schema.statements[self.source_statement_id];
        self.selector_schema
            .name_of(statement.shape)
            .map(str::to_string)
    }

    #[getter]
    fn selector(&self) -> Target {
        build_target(
            &self.selector_schema.statements[self.source_statement_id].selector,
            &self.selector_schema,
        )
    }

    /// The IRI of the shape this statement heads, or `None` when the shape came
    /// from a blank node. Use it to group a run by authored shape without
    /// re-validating; `EvidenceRun.covered_shapes()` lists the names in play.
    #[getter]
    fn shape_iri(&self) -> Option<String> {
        statement_shape_iri(&self.selector_schema, self.source_statement_id)
    }

    #[getter]
    fn selected_foci(&self, py: Python<'_>) -> Vec<Py<FocusEvidence>> {
        self.selected_foci
            .iter()
            .map(|value| value.clone_ref(py))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "StatementEvaluation(source_statement_id={}, selected_foci={})",
            self.source_statement_id,
            self.selected_foci.len()
        )
    }
}

/// Conformance totals for one pass over a prepared snapshot: how many
/// `(statement, focus)` pairs were selected and how they came out. No evidence
/// is materialized, so this costs strictly less than a full run.
///
/// The counts are over *normalized* statements, which is what makes the pass
/// cheap — a merged statement is decided once. A run instead reports one focus
/// row per *authored* statement, so its row count is the larger number whenever
/// common-subexpression elimination merged anything. `selected_pairs` equals the
/// distinct `(normalized_statement_id, focus)` pairs a run contains.
#[pyclass(get_all, frozen, name = "ConformanceRun")]
pub struct PyConformanceRun {
    /// Whether every selected pair passed.
    pub conforms: bool,
    /// Distinct normalized pairs target selection produced.
    pub selected_pairs: usize,
    /// Of those, how many held.
    pub passed: usize,
    /// Of those, how many did not. `find_failures` names them.
    pub failed: usize,
}

#[pymethods]
impl PyConformanceRun {
    fn __bool__(&self) -> bool {
        self.conforms
    }

    fn __repr__(&self) -> String {
        format!(
            "ConformanceRun(conforms={}, selected_pairs={}, passed={}, failed={})",
            self.conforms, self.selected_pairs, self.passed, self.failed
        )
    }
}

/// A handle naming one selected `(statement, focus)` pair — what `find_failures`
/// hands back and `explain` takes.
#[pyclass(frozen, name = "SelectedPair")]
pub struct PySelectedPair {
    inner: shifty_engine::SelectedPair,
}

#[pymethods]
impl PySelectedPair {
    /// The focus node, rendered.
    #[getter]
    fn focus(&self) -> String {
        self.inner.focus().to_string()
    }

    /// Index of the *normalized* statement this pair was decided against.
    ///
    /// Deliberately not called `statement`: everywhere else in the evidence API
    /// a bare statement id is an *authored* one (`Failure.statement`,
    /// `failure_for(focus, statement=...)`, `StatementEvaluation.source_statement_id`).
    /// Evidence is materialized against the normalized statement, so this is a
    /// different numbering and the two must not be crossed. See
    /// `source_statements` for the authored ids.
    #[getter]
    fn normalized_statement(&self) -> usize {
        self.inner.normalized_statement()
    }

    /// The authored statements selected for this normalized request, in source
    /// order. More than one when an unscoped request was formed from statements
    /// merged by common-subexpression elimination; a scoped request retains
    /// only the statements the caller selected.
    #[getter]
    fn source_statements(&self) -> Vec<usize> {
        self.inner.source_statements().to_vec()
    }

    fn __repr__(&self) -> String {
        format!(
            "SelectedPair(focus={:?}, normalized_statement={})",
            self.inner.focus().to_string(),
            self.inner.normalized_statement()
        )
    }
}

/// The options both evidence entry points take, parsed once.
fn validation_options(
    entry_shape_names: Option<Vec<String>>,
    minimum_severity: &str,
    sort_results: bool,
) -> PyResult<ValidationOptions> {
    Ok(ValidationOptions {
        minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
        sort_results,
        entry_shape_names: entry_shape_names.unwrap_or_default(),
        ..ValidationOptions::default()
    })
}

fn conformance_options(entry_shape_names: Option<Vec<String>>) -> ConformanceOptions {
    ConformanceOptions {
        entry_shape_names: entry_shape_names.unwrap_or_default(),
    }
}

/// Index key for an IRI written either bare or in angle brackets. Focus terms
/// that are blank nodes or literals are not bracketed, so they key on their
/// rendered form unchanged.
fn unbracket(value: &str) -> &str {
    value
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'))
        .unwrap_or(value)
}

/// The IRI of the shape `statement` heads, or `None` when that shape came from
/// a blank node and so has no name to report.
fn statement_shape_iri(schema: &Schema, statement: usize) -> Option<String> {
    let shape = schema.statements.get(statement)?.shape;
    schema.name_of(shape).map(str::to_string)
}

/// Resolve a strict single-evidence lookup. Ambiguity is an error rather than a
/// silent first-match, so a caller that assumed one evaluation per focus hears
/// about the statement that broke the assumption.
fn exactly_one<T>(
    kind: &str,
    focus: &str,
    statement: Option<usize>,
    mut matches: Vec<(usize, T)>,
) -> PyResult<T> {
    match matches.len() {
        1 => Ok(matches.pop().expect("length checked").1),
        0 => Err(py_value_error(match statement {
            Some(id) => format!("no {kind} for focus {focus} under statement {id}"),
            None => format!("no {kind} for focus {focus}"),
        })),
        count => {
            let ids = matches
                .iter()
                .map(|(id, _)| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(py_value_error(format!(
                "focus {focus} has {count} {kind}s (statements {ids}); \
                 pass statement= to choose one"
            )))
        }
    }
}

/// Complete statement-oriented coverage for one evidence validation run.
#[pyclass(name = "EvidenceRun")]
pub struct EvidenceValidationOutcome {
    #[pyo3(get)]
    conforms: bool,
    statements: Vec<Py<PyStatementEvaluation>>,
    /// Every selected focus in statement order — the flat view the indexes address.
    foci: Vec<Py<FocusEvidence>>,
    /// Focus key → positions in `foci`, so a per-focus projection is one hash
    /// lookup rather than a scan over every statement.
    focus_index: HashMap<String, Vec<usize>>,
    /// Shape IRI → positions in `foci`, the same trick for the shape-scoped
    /// projections.
    shape_index: HashMap<String, Vec<usize>>,
    /// Named shapes this run has statements for, in statement order.
    covered_shapes: Vec<String>,
    /// Every shape the schema names, so an IRI that names no shape at all can be
    /// told apart from one this run simply has no statements for.
    known_shapes: HashSet<String>,
    /// Lowered raw constraint id → authored property-shape source id. Kept on
    /// the run so `ShapeMap.from_run(run)` preserves property boundaries even
    /// when the originating EvidenceSession is not supplied.
    source_owners: HashMap<u32, u32>,
    json: String,
}

impl EvidenceValidationOutcome {
    fn evaluations_for(&self, focus: &str) -> impl Iterator<Item = &Py<FocusEvidence>> {
        self.focus_index
            .get(unbracket(focus.trim()))
            .into_iter()
            .flatten()
            .map(|index| &self.foci[*index])
    }

    /// A misspelled shape IRI is an error; a real shape this run has no
    /// statements for is an empty projection. Only the schema can tell them
    /// apart, which is why the run retains the names it knows.
    fn evaluations_for_shape(
        &self,
        shape_iri: &str,
    ) -> PyResult<impl Iterator<Item = &Py<FocusEvidence>>> {
        let key = unbracket(shape_iri.trim());
        if !self.known_shapes.contains(key) {
            return Err(py_value_error(format!(
                "no shape named <{key}> in the schema"
            )));
        }
        Ok(self
            .shape_index
            .get(key)
            .into_iter()
            .flatten()
            .map(|index| &self.foci[*index]))
    }
}

#[pymethods]
impl EvidenceValidationOutcome {
    fn _binding_source_ids(&self) -> HashMap<u32, u32> {
        self.source_owners.clone()
    }

    #[getter]
    fn statements(&self, py: Python<'_>) -> Vec<Py<PyStatementEvaluation>> {
        self.statements
            .iter()
            .map(|statement| statement.clone_ref(py))
            .collect()
    }

    /// Every evaluation of `focus`, one per statement that selected it, in
    /// statement order. `focus` is an IRI with or without angle brackets, or a
    /// blank node / literal in its rendered form. Empty when no statement
    /// selected `focus` — an unselected focus is not an error.
    fn results_for(&self, py: Python<'_>, focus: &str) -> Vec<Py<FocusEvidence>> {
        self.evaluations_for(focus)
            .map(|value| value.clone_ref(py))
            .collect()
    }

    /// The failing evaluations of `focus`, in statement order — `results_for`
    /// restricted to one polarity. Empty when `focus` failed nothing.
    fn failures_for(&self, py: Python<'_>, focus: &str) -> Vec<Py<FocusWitness>> {
        self.evaluations_for(focus)
            .filter_map(|value| value.borrow(py).failure.as_ref().map(|f| f.clone_ref(py)))
            .collect()
    }

    /// The passing evaluations of `focus`, in statement order — the dual of
    /// `failures_for`. Empty when `focus` satisfied nothing.
    fn satisfactions_for(&self, py: Python<'_>, focus: &str) -> Vec<Py<FocusSatisfaction>> {
        self.evaluations_for(focus)
            .filter_map(|value| {
                value
                    .borrow(py)
                    .satisfaction
                    .as_ref()
                    .map(|s| s.clone_ref(py))
            })
            .collect()
    }

    /// The single failure of `focus`, or the one under authored statement
    /// `statement`. Raises `ValueError` when there is no such failure, and when
    /// `focus` failed more than one statement and `statement` does not pick one
    /// out — an ambiguous match is never resolved silently.
    #[pyo3(signature = (focus, statement=None))]
    fn failure_for(
        &self,
        py: Python<'_>,
        focus: &str,
        statement: Option<usize>,
    ) -> PyResult<Py<FocusWitness>> {
        let matches = self
            .failures_for(py, focus)
            .into_iter()
            .map(|value| {
                let id = value.borrow(py).statement;
                (id, value)
            })
            .filter(|(id, _)| statement.is_none_or(|wanted| wanted == *id))
            .collect();
        exactly_one("failure", focus, statement, matches)
    }

    /// The named shapes this run has statements for, in statement order and
    /// without duplicates — the coverage the shape-scoped projections address.
    ///
    /// A shape rooted at a blank node has no IRI to report and appears only in
    /// `statements`, so this is a list of the *nameable* coverage, not a count
    /// of every statement. An IRI absent from this list but present in the
    /// schema projects empty rather than raising; see `failures_for_shape`.
    fn covered_shapes(&self) -> Vec<String> {
        self.covered_shapes.clone()
    }

    /// Every evaluation made under `shape_iri`, in statement order — the
    /// shape-scoped counterpart of `results_for`. Raises `ValueError` if the
    /// schema names no such shape; returns `[]` for a shape this run holds no
    /// statements for, which `covered_shapes()` lets you check up front.
    fn results_for_shape(
        &self,
        py: Python<'_>,
        shape_iri: &str,
    ) -> PyResult<Vec<Py<FocusEvidence>>> {
        Ok(self
            .evaluations_for_shape(shape_iri)?
            .map(|value| value.clone_ref(py))
            .collect())
    }

    /// The failing evaluations made under `shape_iri`, in statement order.
    /// Raises and projects like `results_for_shape`.
    fn failures_for_shape(
        &self,
        py: Python<'_>,
        shape_iri: &str,
    ) -> PyResult<Vec<Py<FocusWitness>>> {
        Ok(self
            .evaluations_for_shape(shape_iri)?
            .filter_map(|value| value.borrow(py).failure.as_ref().map(|f| f.clone_ref(py)))
            .collect())
    }

    /// The passing evaluations made under `shape_iri`, in statement order — the
    /// dual of `failures_for_shape`.
    fn satisfactions_for_shape(
        &self,
        py: Python<'_>,
        shape_iri: &str,
    ) -> PyResult<Vec<Py<FocusSatisfaction>>> {
        Ok(self
            .evaluations_for_shape(shape_iri)?
            .filter_map(|value| {
                value
                    .borrow(py)
                    .satisfaction
                    .as_ref()
                    .map(|s| s.clone_ref(py))
            })
            .collect())
    }

    /// The single satisfaction of `focus`, or the one under authored statement
    /// `statement` — the dual of `failure_for`, and equally strict about an
    /// ambiguous match.
    #[pyo3(signature = (focus, statement=None))]
    fn satisfaction_for(
        &self,
        py: Python<'_>,
        focus: &str,
        statement: Option<usize>,
    ) -> PyResult<Py<FocusSatisfaction>> {
        let matches = self
            .satisfactions_for(py, focus)
            .into_iter()
            .map(|value| {
                let id = value.borrow(py).statement;
                (id, value)
            })
            .filter(|(id, _)| statement.is_none_or(|wanted| wanted == *id))
            .collect();
        exactly_one("satisfaction", focus, statement, matches)
    }

    fn to_json(&self) -> String {
        self.json.clone()
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(py
            .import("json")?
            .call_method1("loads", (&self.json,))?
            .unbind())
    }

    /// Compact encoding of this run: evidence nodes and RDF terms hash-consed
    /// into shared tables, and optionally without the constraint catalog.
    /// Lossless — `shifty.expand_evidence` restores the full run.
    #[pyo3(signature = (include_catalog=true))]
    fn to_compact_json(&self, include_catalog: bool) -> PyResult<String> {
        let value: serde_json::Value = serde_json::from_str(&self.json)
            .map_err(|error| py_value_error(format!("cannot read evidence: {error}")))?;
        serde_json::to_string(&shifty_engine::compact_value(value, include_catalog))
            .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))
    }

    #[pyo3(signature = (include_catalog=true))]
    fn to_compact_dict(&self, py: Python<'_>, include_catalog: bool) -> PyResult<Py<PyAny>> {
        let compact = self.to_compact_json(include_catalog)?;
        Ok(py
            .import("json")?
            .call_method1("loads", (compact,))?
            .unbind())
    }

    fn __bool__(&self) -> bool {
        self.conforms
    }

    fn __repr__(&self) -> String {
        format!(
            "EvidenceRun(conforms={}, statements={})",
            self.conforms,
            self.statements.len()
        )
    }
}

/// Restore a run compacted by `EvidenceRun.to_compact_json`.
///
/// `catalog` supplies the constraint catalog for an encoding written with
/// `include_catalog=False`; it is the `"constraints"` value of the original
/// run. Returns the full run as JSON text.
#[pyfunction]
#[pyo3(name = "expand_evidence_json", signature = (compact, catalog=None))]
pub fn expand_evidence_json(compact: &str, catalog: Option<&str>) -> PyResult<String> {
    let value: serde_json::Value = serde_json::from_str(compact)
        .map_err(|error| py_value_error(format!("cannot read compact evidence: {error}")))?;
    let catalog = match catalog {
        Some(text) => serde_json::from_str(text)
            .map_err(|error| py_value_error(format!("cannot read catalog: {error}")))?,
        None => value.get("constraints").cloned().ok_or_else(|| {
            py_value_error("compact evidence omits its constraint catalog; pass catalog=".into())
        })?,
    };
    let expanded = shifty_engine::expand_value(&value, catalog)
        .map_err(|error| py_value_error(error.to_string()))?;
    serde_json::to_string(&expanded)
        .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))
}

/// Prepared evidence validation over one immutable shapes/data snapshot.
/// Normalization, inference, indexing, and SPARQL preparation are retained
/// across calls to `validate()`.
#[pyclass(unsendable, name = "EvidenceSession")]
pub struct EvidenceSession {
    prepared: PreparedEvidenceValidator,
    raw_schema: Arc<Schema>,
    normalized_schema: Arc<Schema>,
    data: Arc<Graph>,
    /// The data graph before inference. `revalidate` patches *this* when it
    /// re-runs the rules, so a deletion stops supporting what it derived.
    base_data: Arc<Graph>,
    /// Retained so `revalidate` can re-prepare a patched graph.
    /// Whether a data graph was supplied separately from the shapes graph;
    /// inference and preparation take a different entry point either way.
    has_data_graph: bool,
    run_infer: bool,
    shapes: shifty_parse::Loaded,
    graph_mode: shifty_engine::ValidationGraphMode,
    diagnostics: Vec<String>,
}

#[pymethods]
impl EvidenceSession {
    #[new]
    #[pyo3(signature = (
        shapes=None,
        shapes_path=None,
        shapes_format="auto",
        data=None,
        data_path=None,
        data_format="auto",
        run_infer=true,
        graph_mode="union",
        base=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        shapes: Option<PyBackedBytes>,
        shapes_path: Option<String>,
        shapes_format: &str,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        run_infer: bool,
        graph_mode: &str,
        base: Option<String>,
    ) -> PyResult<Self> {
        let shapes_spec =
            InputSpec::new(shapes, shapes_path, shapes_format, "shapes").map_err(py_value_error)?;
        let data_spec = match (data, data_path) {
            (None, None) => None,
            (data, path) => {
                Some(InputSpec::new(data, path, data_format, "data").map_err(py_value_error)?)
            }
        };
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let shapes_loaded = shapes_spec.load(base.as_deref()).map_err(py_value_error)?;
        let parsed = shifty_parse::parse_loaded(&shapes_loaded);
        parsed
            .require_valid()
            .map_err(|error| py_value_error(error.to_string()))?;
        let diagnostics = parsed.diagnostics.iter().map(ToString::to_string).collect();
        let raw_schema = parsed.schema;
        let data_loaded = data_spec
            .map(|spec| spec.load(base.as_deref()))
            .transpose()
            .map_err(py_value_error)?;
        let has_data_graph = data_loaded.is_some();
        let base_data = data_loaded
            .as_ref()
            .map_or(&shapes_loaded.graph, |loaded| &loaded.graph);

        let evaluated = if run_infer && !raw_schema.rules.is_empty() {
            let inference = match data_loaded.as_ref() {
                Some(_) => {
                    shifty_engine::infer_graphs(base_data, &shapes_loaded.graph, &raw_schema)
                }
                None => shifty_engine::infer(&shapes_loaded.graph, &raw_schema),
            }
            .map_err(|error| py_value_error(format!("non-stratifiable schema: {error}")))?;
            inference.graph
        } else {
            base_data.clone()
        };

        let prepared = if has_data_graph {
            PreparedEvidenceValidator::with_graphs(
                &evaluated,
                &shapes_loaded.graph,
                &raw_schema,
                mode,
            )
        } else {
            PreparedEvidenceValidator::new(&evaluated, &raw_schema)
        }
        .map_err(|error| py_value_error(format!("non-stratifiable schema: {error}")))?;
        let normalized_schema = Arc::new(prepared.schema().clone());
        let base_data = Arc::new(base_data.clone());

        Ok(Self {
            prepared,
            raw_schema: Arc::new(raw_schema),
            normalized_schema,
            data: Arc::new(evaluated),
            base_data,
            has_data_graph,
            run_infer,
            shapes: shapes_loaded,
            graph_mode: mode,
            diagnostics,
        })
    }

    #[getter]
    fn diagnostics(&self) -> Vec<String> {
        self.diagnostics.clone()
    }

    #[pyo3(signature = (entry_shape_names=None, minimum_severity="info", sort_results=true))]
    fn validate(
        &self,
        py: Python<'_>,
        entry_shape_names: Option<Vec<String>>,
        minimum_severity: &str,
        sort_results: bool,
    ) -> PyResult<EvidenceValidationOutcome> {
        let options = validation_options(entry_shape_names, minimum_severity, sort_results)?;
        let outcome = self.prepared.validate(&options);
        self.build_run(py, outcome, &self.normalized_schema, &self.data)
    }

    /// Validate `G ⊕ ΔG`: the run `validate()` would produce over this
    /// session's graph with `delta` applied. Pure — the session keeps its own
    /// snapshot, so a run taken before the edit stays valid and comparable.
    ///
    /// Unlike `validate()`, this cannot reuse the prepared snapshot: a patched
    /// graph needs its own normalization, indexing, and SPARQL preparation. It
    /// still skips file I/O, parsing, and schema lowering.
    ///
    /// `infer` re-runs SHACL-AF rules over the patched graph, so an added
    /// triple can fire a rule and a deleted one stops supporting what it
    /// derived. It defaults to whatever the session was built with, which keeps
    /// the before and after runs on the same baseline. Passing `False` patches
    /// the already-inferred graph instead and leaves the rules alone — cheaper,
    /// and sound only if the edit fires none of them.
    #[pyo3(signature = (
        delta,
        infer=None,
        entry_shape_names=None,
        minimum_severity="info",
        sort_results=true
    ))]
    fn revalidate(
        &self,
        py: Python<'_>,
        delta: &RepairDelta,
        infer: Option<bool>,
        entry_shape_names: Option<Vec<String>>,
        minimum_severity: &str,
        sort_results: bool,
    ) -> PyResult<EvidenceValidationOutcome> {
        let options = validation_options(entry_shape_names, minimum_severity, sort_results)?;
        let run_infer = infer.unwrap_or(self.run_infer);

        // With inference on, patch the graph the rules read. Patching the
        // already-derived graph would strand triples that the deletion should
        // have invalidated, since inference only ever adds.
        let source = if run_infer {
            &self.base_data
        } else {
            &self.data
        };
        let patched = engine_apply(source, &delta.inner);
        let evaluated = if run_infer && !self.raw_schema.rules.is_empty() {
            if self.has_data_graph {
                shifty_engine::infer_graphs(&patched, &self.shapes.graph, &self.raw_schema)
            } else {
                shifty_engine::infer(&patched, &self.raw_schema)
            }
            .map_err(|error| py_value_error(format!("non-stratifiable schema: {error}")))?
            .graph
        } else {
            patched
        };

        let prepared = if self.has_data_graph {
            PreparedEvidenceValidator::with_graphs(
                &evaluated,
                &self.shapes.graph,
                &self.raw_schema,
                self.graph_mode,
            )
        } else {
            PreparedEvidenceValidator::new(&evaluated, &self.raw_schema)
        }
        .map_err(|error| py_value_error(format!("non-stratifiable schema: {error}")))?;

        let normalized_schema = Arc::new(prepared.schema().clone());
        let outcome = prepared.validate(&options);
        self.build_run(py, outcome, &normalized_schema, &Arc::new(evaluated))
    }

    /// Decide every selected pair without materializing any evidence — the
    /// cheapest of the four entry points, and the baseline the others are
    /// measured against.
    ///
    /// `minimum_severity` is not honored: with no failure evidence there is no
    /// per-constraint severity to weigh, so any failing pair makes `conforms`
    /// false. Only `entry_shape_names` applies.
    #[pyo3(signature = (entry_shape_names=None))]
    fn validate_conformance(
        &self,
        entry_shape_names: Option<Vec<String>>,
    ) -> PyResult<PyConformanceRun> {
        let options = conformance_options(entry_shape_names);
        Ok(conformance_to_py(
            self.prepared.validate_conformance(&options),
        ))
    }

    /// The same pass as `validate_conformance`, also returning a handle for each
    /// pair that failed.
    ///
    /// Paying only a term clone per failing pair, this plus `explain` on each
    /// result is far cheaper than `validate` when failures are a small share of
    /// selected pairs — which is the usual case.
    #[pyo3(signature = (entry_shape_names=None))]
    fn find_failures(
        &self,
        py: Python<'_>,
        entry_shape_names: Option<Vec<String>>,
    ) -> PyResult<(PyConformanceRun, Vec<Py<PySelectedPair>>)> {
        let options = conformance_options(entry_shape_names);
        let (run, failures) = self.prepared.find_failures(&options);
        let pairs = failures
            .into_iter()
            .map(|pair| Py::new(py, PySelectedPair { inner: pair }))
            .collect::<PyResult<Vec<_>>>()?;
        Ok((conformance_to_py(run), pairs))
    }

    /// Materialize evidence for one pair, as a run holding just that pair.
    ///
    /// Every projection works on the result, so it reads like a slice of a full
    /// run: one `StatementEvaluation` per authored statement that normalizes to
    /// the pair's, each carrying the single focus.
    ///
    /// Target selection is *not* re-run — the pair is taken as already
    /// selected, which is the point. Pairs should come from `find_failures` or
    /// an earlier run over this snapshot.
    ///
    /// The returned run carries **no constraint catalog**: it is fixed per
    /// snapshot rather than per pair, so take it once from `constraints()`.
    /// That only affects serialization; the `constraint` objects on statements
    /// and evidence are present either way.
    fn explain(
        &self,
        py: Python<'_>,
        pair: &PySelectedPair,
    ) -> PyResult<EvidenceValidationOutcome> {
        self.explain_run(py, self.prepared.explain(&pair.inner))
    }

    /// `explain` without the authored-statement progress view, keeping the
    /// satisfaction trace or failure witness and the authored identities.
    fn explain_canonical(
        &self,
        py: Python<'_>,
        pair: &PySelectedPair,
    ) -> PyResult<EvidenceValidationOutcome> {
        self.explain_run(py, self.prepared.explain_canonical(&pair.inner))
    }

    /// The source and normalized constraint catalogs for this snapshot, as a
    /// dict.
    ///
    /// Fixed for the snapshot, so a caller explaining pairs one at a time takes
    /// this once instead of paying for it per pair. It is also the `catalog`
    /// argument of `shifty.expand_evidence`, which is what makes
    /// `to_compact_json(include_catalog=False)` usable: the catalog travels once,
    /// out of band.
    fn constraints(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let json = serde_json::to_string(&self.prepared.constraints())
            .map_err(|error| py_value_error(format!("cannot serialize constraints: {error}")))?;
        Ok(py.import("json")?.call_method1("loads", (json,))?.unbind())
    }

    /// Evidence for one focus against any normalized constraint in the run's
    /// catalog, including constraints below a statement's top-level shape.
    fn _evidence_for(&self, focus: &str, constraint_id: u32) -> PyResult<PyEvidenceNode> {
        self.evidence_for_impl(focus, constraint_id)
    }

    #[pyo3(signature = (name_path=None))]
    fn _binding_names(&self, name_path: Option<&str>) -> PyResult<HashMap<u32, Vec<String>>> {
        self.binding_names_impl(name_path)
    }

    fn _binding_source_ids(&self) -> HashMap<u32, u32> {
        self.binding_source_ids_impl()
    }

    fn _binding_values(&self, focus: &str, constraint_id: u32) -> PyResult<Vec<String>> {
        self.binding_values_impl(focus, constraint_id)
    }

    fn _shape_name_of(&self, constraint_id: u32) -> Option<String> {
        self.shape_name_of_impl(constraint_id)
    }

    fn _resolve_path(
        &self,
        nodes: Vec<String>,
        path: &str,
    ) -> PyResult<HashMap<String, Vec<String>>> {
        self.resolve_path_impl(nodes, path)
    }

    fn __repr__(&self) -> String {
        format!(
            "EvidenceSession(statements={}, triples={})",
            self.raw_schema.statements.len(),
            self.data.len()
        )
    }
}

fn conformance_to_py(run: shifty_engine::ConformanceRun) -> PyConformanceRun {
    PyConformanceRun {
        conforms: run.conforms,
        selected_pairs: run.selected_pairs,
        passed: run.passed,
        failed: run.failed,
    }
}

impl EvidenceSession {
    /// Wrap the statements `explain` produced as a run. `conforms` is derived
    /// from the evidence rather than assumed: explaining a *passing* pair is
    /// well-defined, so this is not always false.
    fn explain_run(
        &self,
        py: Python<'_>,
        statements: Vec<shifty_engine::StatementEvaluation>,
    ) -> PyResult<EvidenceValidationOutcome> {
        let conforms = statements.iter().all(|statement| {
            statement
                .selected_foci
                .iter()
                .all(|focus| focus.status() == shifty_engine::EvaluationStatus::Pass)
        });
        // The catalog is fixed per snapshot; `constraints()` serves it once.
        let run = shifty_engine::EvidenceRun {
            conforms,
            constraints: shifty_engine::ConstraintCatalog {
                source: Vec::new(),
                normalized: Vec::new(),
            },
            statements,
        };
        let json = serde_json::to_string(&run)
            .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))?;
        self.build_outcome(
            py,
            conforms,
            run.statements,
            json,
            &self.normalized_schema,
            &self.data,
        )
    }

    /// Turn one engine run into the Python object graph, including the focus
    /// and shape indexes. `normalized_schema` and `data` come from whichever
    /// prepared validator produced `outcome`, so a revalidated run reports
    /// arena ids and reads repair candidates from its own patched graph.
    fn build_run(
        &self,
        py: Python<'_>,
        outcome: shifty_engine::EvidenceRun,
        normalized_schema: &Arc<Schema>,
        data: &Arc<Graph>,
    ) -> PyResult<EvidenceValidationOutcome> {
        let json = serde_json::to_string(&outcome)
            .map_err(|error| py_value_error(format!("cannot serialize evidence: {error}")))?;
        self.build_outcome(
            py,
            outcome.conforms,
            outcome.statements,
            json,
            normalized_schema,
            data,
        )
    }

    /// The object graph itself, over whatever set of statements the caller has —
    /// a whole run from `validate`, or the one pair `explain` materialized.
    fn build_outcome(
        &self,
        py: Python<'_>,
        conforms: bool,
        outcome_statements: Vec<shifty_engine::StatementEvaluation>,
        json: String,
        normalized_schema: &Arc<Schema>,
        data: &Arc<Graph>,
    ) -> PyResult<EvidenceValidationOutcome> {
        let mut statements = Vec::with_capacity(outcome_statements.len());
        let mut foci: Vec<Py<FocusEvidence>> = Vec::new();
        let mut focus_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut shape_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut covered_shapes: Vec<String> = Vec::new();
        let known_shapes: HashSet<String> =
            self.raw_schema.names.values().flatten().cloned().collect();

        for statement in outcome_statements {
            let normalized_statement_id = statement
                .normalized_statement_id
                .ok_or_else(|| py_value_error("statement has no normalized identity".into()))?;
            let raw_statement = self
                .raw_schema
                .statements
                .get(statement.source_statement_id)
                .ok_or_else(|| py_value_error("raw statement is out of bounds".into()))?;
            let target = shifty_algebra::render::selector_to_string_in(
                &raw_statement.selector,
                &self.raw_schema.arena,
            );
            let normalized_constraint_id = statement
                .normalized_constraint_id
                .ok_or_else(|| py_value_error("statement has no normalized constraint".into()))?;
            let constraint =
                constraint_to_py(py, &normalized_schema.arena, normalized_constraint_id)?;
            let constraint_kind = constraint_kind_to_py(statement.constraint_kind);
            let shape_iri = self
                .raw_schema
                .name_of(raw_statement.shape)
                .map(str::to_string);
            let mut selected_foci = Vec::with_capacity(statement.selected_foci.len());
            let first_focus = foci.len();

            for result in statement.selected_foci {
                let focus = result.focus.to_string();
                let progress = result
                    .progress
                    .map(|progress| {
                        let children = progress
                            .evaluated_children
                            .into_iter()
                            .map(|child| {
                                Py::new(
                                    py,
                                    PyChildEvaluation {
                                        source_constraint_ref: child.source_constraint_ref.0,
                                        normalized_constraint_ref: child
                                            .normalized_constraint_ref
                                            .map(|id| id.0),
                                        status: match child.status {
                                            shifty_engine::EvaluationStatus::Pass => "pass",
                                            shifty_engine::EvaluationStatus::Fail => "fail",
                                        }
                                        .to_string(),
                                        constraint_kind: constraint_kind_to_py(
                                            child.evidence_summary.constraint_kind,
                                        ),
                                    },
                                )
                            })
                            .collect::<PyResult<Vec<_>>>()?;
                        Py::new(
                            py,
                            PyEvaluationProgress {
                                evaluated_children: children,
                            },
                        )
                    })
                    .transpose()?;

                let (status, satisfaction, failure) = match result.evidence {
                    IrEvidence::Satisfaction(trace) => {
                        let value = Py::new(
                            py,
                            FocusSatisfaction {
                                focus: focus.clone(),
                                statement: statement.source_statement_id,
                                statement_id: normalized_statement_id,
                                constraint_id: normalized_constraint_id.0,
                                constraint_kind,
                                constraint: constraint.clone_ref(py),
                                target: target.clone(),
                                inner: IrSat {
                                    focus: result.focus,
                                    statement: normalized_statement_id,
                                    trace,
                                },
                                selector_schema: Arc::clone(&self.raw_schema),
                            },
                        )?;
                        ("pass".to_string(), Some(value), None)
                    }
                    IrEvidence::Failure(failure) => {
                        let value = Py::new(
                            py,
                            FocusWitness {
                                focus: focus.clone(),
                                statement: statement.source_statement_id,
                                statement_id: normalized_statement_id,
                                constraint_id: normalized_constraint_id.0,
                                constraint_kind,
                                constraint: constraint.clone_ref(py),
                                target: target.clone(),
                                inner: IrFocus {
                                    focus: result.focus,
                                    statement: normalized_statement_id,
                                    failure,
                                },
                                schema: Arc::clone(normalized_schema),
                                selector_schema: Arc::clone(&self.raw_schema),
                                data: Arc::clone(data),
                            },
                        )?;
                        ("fail".to_string(), None, Some(value))
                    }
                };
                let key = unbracket(&focus).to_string();
                let evaluation = Py::new(
                    py,
                    FocusEvidence {
                        focus,
                        status,
                        satisfaction,
                        failure,
                        progress,
                    },
                )?;
                focus_index.entry(key).or_default().push(foci.len());
                foci.push(evaluation.clone_ref(py));
                selected_foci.push(evaluation);
            }

            // A statement covers its shape even when the selector chose nothing,
            // so record coverage on the statement rather than on its foci.
            if let Some(iri) = shape_iri {
                if !shape_index.contains_key(&iri) {
                    covered_shapes.push(iri.clone());
                }
                shape_index
                    .entry(iri)
                    .or_default()
                    .extend(first_focus..foci.len());
            }

            statements.push(Py::new(
                py,
                PyStatementEvaluation {
                    source_statement_id: statement.source_statement_id,
                    normalized_statement_id: statement.normalized_statement_id,
                    source_constraint_id: statement.source_constraint_id.0,
                    normalized_constraint_id: statement.normalized_constraint_id.map(|id| id.0),
                    constraint_kind,
                    constraint,
                    target,
                    selected_foci,
                    selector_schema: Arc::clone(&self.raw_schema),
                },
            )?);
        }

        Ok(EvidenceValidationOutcome {
            conforms,
            statements,
            foci,
            focus_index,
            shape_index,
            covered_shapes,
            known_shapes,
            source_owners: self.binding_source_ids_impl(),
            json,
        })
    }
}

impl EvidenceSession {
    /// Evidence for `focus` against one *normalized* constraint id — any
    /// constraint in the run's catalog, not just a statement's top-level shape.
    ///
    /// A failing conjunction's failure evidence carries only the failing
    /// children; the run's `EvaluationProgress` says which children passed
    /// without materializing why. This is the drill-down for those elided
    /// passes: give it the focus (N-Triples syntax, as `FocusEvaluation.focus`
    /// renders it) and a child's `normalized_constraint_ref`, and it returns
    /// the same tagged dict a run's `evidence` entries use
    /// (`{"status": "pass"|"fail", "evidence": {...}}`).
    ///
    /// No target selection is involved: the pair is taken as given, and a focus
    /// no statement selects still yields well-defined evidence.
    fn evidence_for_impl(&self, focus: &str, constraint_id: u32) -> PyResult<PyEvidenceNode> {
        let term = parse_term(focus).map_err(py_value_error)?;
        let evidence = self
            .prepared
            .explain_constraint(&term, ShapeId(constraint_id))
            .ok_or_else(|| {
                py_value_error(format!(
                    "constraint id {constraint_id} is not in the normalized schema"
                ))
            })?;
        evidence_nodes(&evidence)
            .into_iter()
            .next()
            .ok_or_else(|| py_value_error("constraint evaluation returned no evidence".to_string()))
    }

    /// The authored property-shape owner for each raw constraint nested in
    /// that property shape. Lowering expands one property shape into several
    /// internal constraints (for example a universal datatype check plus a
    /// `minCount`); this table restores that boundary for shape-map consumers.
    fn binding_source_ids_impl(&self) -> HashMap<u32, u32> {
        let property_sources: Vec<ShapeId> = self
            .raw_schema
            .sources
            .iter()
            .filter_map(|(id, source)| {
                shifty_parse::graph::term_to_node(source)
                    .filter(|node| {
                        self.shapes
                            .object(node, shifty_parse::vocab::SH_PATH)
                            .is_some()
                    })
                    .map(|_| *id)
            })
            .collect();
        let source_ids: HashSet<ShapeId> = self.raw_schema.sources.keys().copied().collect();
        let mut owners = HashMap::new();

        for owner in property_sources {
            let mut pending = vec![owner];
            let mut seen = HashSet::new();
            while let Some(id) = pending.pop() {
                if !seen.insert(id) {
                    continue;
                }
                // A nested authored shape starts a distinct binding. Do not
                // let its internals become siblings of the enclosing property.
                if id != owner && source_ids.contains(&id) {
                    continue;
                }
                owners.entry(id.0).or_insert(owner.0);
                match self.raw_schema.arena.get(id) {
                    Shape::Annotated { shape, .. } | Shape::Not(shape) => pending.push(*shape),
                    Shape::And(children) | Shape::Or(children) => {
                        pending.extend(children.iter().copied())
                    }
                    Shape::Count { qualifier, .. } => pending.push(*qualifier),
                    _ => {}
                }
            }
        }
        owners
    }

    /// For every *raw* (source) constraint with shapes-graph provenance, and
    /// every lowered child of an authored property shape, the values
    /// `name_path` reaches from that originating property/source node,
    /// evaluated over the shapes graph. `name_path=None` means `sh:name`.
    /// Constraints with no source-node provenance, or where `name_path`
    /// resolves to nothing, are omitted. Literal values render as their bare
    /// lexical form; IRIs/blank nodes render as `<…>`/`_:…`.
    fn binding_names_impl(&self, name_path: Option<&str>) -> PyResult<HashMap<u32, Vec<String>>> {
        let expr = name_path.unwrap_or("sh:name");
        let path = shifty_parse::parse_property_path(expr, &self.shapes)
            .map_err(|e| py_value_error(format!("invalid name_path: {e}")))?;
        let mut out = HashMap::new();
        let owners = self.binding_source_ids_impl();
        for id in 0..self.raw_schema.arena.len() as u32 {
            let owner = ShapeId(*owners.get(&id).unwrap_or(&id));
            let Some(source) = self.raw_schema.sources.get(&owner) else {
                continue;
            };
            let mut matches: Vec<String> =
                shifty_engine::path::succ(&self.shapes.graph, source, &path)
                    .into_iter()
                    .map(|t| term_text(&t))
                    .collect();
            if matches.is_empty() {
                continue;
            }
            matches.sort();
            out.insert(id, matches);
        }
        Ok(out)
    }

    /// Recover values for a property constraint that lowered to `Top`.
    ///
    /// An unbounded `sh:qualifiedValueShape` is vacuous as a validation
    /// constraint, so the algebra intentionally simplifies it to `Top`. A
    /// shape map is also an extraction view, however, and still needs the
    /// qualifying values. Use the retained authored property shape for this
    /// narrow case: evaluate its `sh:path`, then keep candidates satisfying
    /// each qualified value shape.
    fn binding_values_impl(&self, focus: &str, constraint_id: u32) -> PyResult<Vec<String>> {
        let focus = parse_term(focus).map_err(py_value_error)?;
        let mut current = ShapeId(constraint_id);
        let mut seen = HashSet::new();
        let property = loop {
            if !seen.insert(current) {
                return Ok(Vec::new());
            }
            if let Some(source) = self.raw_schema.sources.get(&current)
                && let Some(node) = shifty_parse::graph::term_to_node(source)
                && self
                    .shapes
                    .object(&node, shifty_parse::vocab::SH_PATH)
                    .is_some()
            {
                break node;
            }
            match self.raw_schema.arena.get(current) {
                shifty_algebra::Shape::Annotated { shape, .. } => current = *shape,
                _ => return Ok(Vec::new()),
            }
        };

        let Some(path_term) = self.shapes.object(&property, shifty_parse::vocab::SH_PATH) else {
            return Ok(Vec::new());
        };
        let path =
            shifty_parse::path::parse_path(&self.shapes, &path_term).map_err(py_value_error)?;

        let qualifying: Vec<ShapeId> = self
            .shapes
            .objects(&property, shifty_parse::vocab::SH_QUALIFIED_VALUE_SHAPE)
            .into_iter()
            .filter_map(|term| {
                shifty_parse::graph::term_to_node(&term)?;
                let raw = self
                    .raw_schema
                    .sources
                    .iter()
                    .find_map(|(id, source)| (source == &term).then_some(*id))?;
                Some(raw)
            })
            .collect();

        let union_graph;
        let graph: &Graph = match self.graph_mode {
            shifty_engine::ValidationGraphMode::Data => self.data.as_ref(),
            shifty_engine::ValidationGraphMode::Union
            | shifty_engine::ValidationGraphMode::UnionAll => {
                union_graph = graph_union(&self.data, &self.shapes.graph);
                &union_graph
            }
        };
        let mut values: Vec<String> = shifty_engine::path::succ(graph, &focus, &path)
            .into_iter()
            .filter(|value| {
                qualifying
                    .iter()
                    .all(|shape| self.prepared.raw_constraint_holds(value, *shape) == Some(true))
            })
            // Shape-map values are parsed back into typed Python terms, so
            // retain the full N-Triples spelling rather than the display form
            // `term_text` uses for plain literals.
            .map(|value| value.to_string())
            .collect();
        values.sort();
        values.dedup();
        Ok(values)
    }

    /// The raw schema's shape name for `constraint_id` — the IRI of the
    /// named (non-blank) RDF node it was lowered from, when it has one.
    fn shape_name_of_impl(&self, constraint_id: u32) -> Option<String> {
        self.raw_schema
            .name_of(ShapeId(constraint_id))
            .map(str::to_string)
    }

    /// Batch-evaluate `path` (a SPARQL 1.1 property path, same grammar as
    /// `name_path`) from each of `nodes` (N-Triples spellings) over the
    /// session's evaluation graph — the data graph, unioned with the shapes
    /// graph to match this session's own `graph_mode` (`union`/`union_all`;
    /// `data` mode reads the data graph alone). Returns each input node's
    /// N-Triples spelling mapped to the N-Triples spellings it reaches.
    fn resolve_path_impl(
        &self,
        nodes: Vec<String>,
        path: &str,
    ) -> PyResult<HashMap<String, Vec<String>>> {
        let parsed = shifty_parse::parse_property_path(path, &self.shapes)
            .map_err(|e| py_value_error(format!("invalid path: {e}")))?;
        let union_graph;
        let graph: &Graph = match self.graph_mode {
            shifty_engine::ValidationGraphMode::Data => self.data.as_ref(),
            shifty_engine::ValidationGraphMode::Union
            | shifty_engine::ValidationGraphMode::UnionAll => {
                union_graph = graph_union(&self.data, &self.shapes.graph);
                &union_graph
            }
        };
        let mut out = HashMap::with_capacity(nodes.len());
        for node in nodes {
            let term = parse_term(&node).map_err(py_value_error)?;
            let mut matches: Vec<String> = shifty_engine::path::succ(graph, &term, &parsed)
                .into_iter()
                .map(|t| t.to_string())
                .collect();
            matches.sort();
            out.insert(node, matches);
        }
        Ok(out)
    }
}

/// The repair space for one violation: an AND/OR/Repeat tree with typed holes.
#[pyclass(name = "RepairTree")]
pub struct RepairTree {
    inner: IrTree,
    origins: BTreeMap<NodeId, Vec<IrEvidenceOrigin>>,
    schema: Arc<Schema>,
    data: Arc<Graph>,
}

/// The evidence occurrence that justified one repair-tree node.
#[pyclass(get_all, eq, frozen, name = "RepairOrigin")]
#[derive(Clone, PartialEq, Eq)]
pub struct RepairOrigin {
    /// Authored statement id, absent for direct node/sub-shape repairs.
    pub statement_id: Option<usize>,
    pub path: Vec<usize>,
    pub constraint_id: u32,
    pub node: Option<String>,
    pub status: String,
    pub evidence_kind: PyEvidenceKind,
    /// Compatibility spelling of `evidence_kind` in snake case.
    pub kind: String,
}

impl From<&IrEvidenceOrigin> for RepairOrigin {
    fn from(origin: &IrEvidenceOrigin) -> Self {
        let evidence_kind = PyEvidenceKind::from(origin.kind);
        Self {
            statement_id: origin.statement,
            path: origin.path.clone(),
            constraint_id: origin.constraint_id.0,
            node: origin.node.as_ref().map(ToString::to_string),
            status: evidence_kind.status_str().to_string(),
            evidence_kind,
            kind: evidence_kind.as_str().to_string(),
        }
    }
}

#[pymethods]
impl RepairOrigin {
    fn __repr__(&self) -> String {
        format!(
            "RepairOrigin(statement={:?}, path={:?}, kind={:?})",
            self.statement_id, self.path, self.kind
        )
    }
}

#[pymethods]
impl RepairTree {
    /// Stable id of the root repair operator.
    #[getter]
    fn root_id(&self) -> u32 {
        self.inner.id().0
    }

    /// Evidence occurrences justifying `node_id`, or the root when omitted.
    /// A joint synthetic node can have several origins; an ordinary node has
    /// one. Unknown ids return an empty list.
    #[pyo3(signature = (node_id=None))]
    fn origins(&self, node_id: Option<u32>) -> Vec<RepairOrigin> {
        let node = NodeId(node_id.unwrap_or_else(|| self.inner.id().0));
        self.origins
            .get(&node)
            .into_iter()
            .flatten()
            .map(RepairOrigin::from)
            .collect()
    }

    /// True if no data repair is possible in scope (opaque SPARQL / identity /
    /// coinductive).
    #[getter]
    fn is_blocked(&self) -> bool {
        self.inner.is_blocked()
    }

    /// The tree rendered as indented text.
    fn explain(&self) -> String {
        let mut out = Vec::new();
        render_tree(&self.inner, &self.schema.arena, 0, &mut out);
        out.join("\n")
    }

    /// Every static hole in the tree, with its constraint. (Per-instance holes a
    /// `Repeat` unrolls to appear only after `instantiate`, as `open_holes`.)
    fn holes(&self, py: Python<'_>) -> PyResult<Vec<Py<Hole>>> {
        let mut pairs = Vec::new();
        collect_hole_constraints(&self.inner, &mut pairs);
        pairs
            .into_iter()
            .map(|(h, c)| {
                Py::new(
                    py,
                    Hole {
                        id: h.0,
                        constraint: constraint_str(&c, &self.schema.arena),
                        inner: c,
                        schema: Arc::clone(&self.schema),
                        data: Arc::clone(&self.data),
                    },
                )
            })
            .collect()
    }

    /// The decision points: every `Any` (branches) and `Repeat` (min/max) node.
    fn choices(&self) -> Vec<Choice> {
        let mut out = Vec::new();
        collect_choices(&self.inner, &mut out);
        out
    }

    /// Fold a driver's [`RepairPlan`] into concrete edits, reporting what is still
    /// open. A pure operation — never validates, never chooses.
    fn instantiate(&self, plan: &RepairPlan) -> Instantiated {
        let inst = instantiate(&self.inner, &plan.inner);
        let open_holes = inst.open_holes.into_iter().map(|(h, c)| (h.0, c)).collect();
        let open_choices = inst.open_choices.into_iter().map(|n| n.0).collect();
        Instantiated {
            delta: inst.delta,
            open_holes,
            open_choices,
            schema: Arc::clone(&self.schema),
            data: Arc::clone(&self.data),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RepairTree(root_id={}, blocked={})",
            self.inner.id().0,
            self.inner.is_blocked()
        )
    }
}

fn collect_hole_constraints(tree: &IrTree, out: &mut Vec<(IrHole, HoleConstraint)>) {
    match tree {
        IrTree::Edits { holes, .. } => {
            for (h, c) in holes {
                if !out.iter().any(|(eh, _)| eh == h) {
                    out.push((*h, c.clone()));
                }
            }
        }
        IrTree::All { children, .. } | IrTree::Any { children, .. } => {
            for c in children {
                collect_hole_constraints(c, out);
            }
        }
        IrTree::Repeat { body, .. } => collect_hole_constraints(body, out),
        IrTree::Noop(_) | IrTree::Blocked(..) => {}
    }
}

fn collect_choices(tree: &IrTree, out: &mut Vec<Choice>) {
    match tree {
        IrTree::Any { id, children } => {
            out.push(Choice {
                node_id: id.0,
                kind: ChoiceKind::Any,
                branches: Some(children.len()),
                min: None,
                max: None,
            });
            for c in children {
                collect_choices(c, out);
            }
        }
        IrTree::Repeat { id, body, min, max } => {
            out.push(Choice {
                node_id: id.0,
                kind: ChoiceKind::Repeat,
                branches: None,
                min: Some(*min),
                max: *max,
            });
            collect_choices(body, out);
        }
        IrTree::All { children, .. } => {
            for c in children {
                collect_choices(c, out);
            }
        }
        IrTree::Edits { .. } | IrTree::Noop(_) | IrTree::Blocked(..) => {}
    }
}

/// A typed hole a driver must bind. `candidates()` enumerates reuse-first options
/// from the data graph.
#[pyclass(name = "Hole")]
pub struct Hole {
    #[pyo3(get)]
    id: u32,
    /// The constraint, fully rendered (e.g. `typed value`, `instance of <C>`,
    /// `instance of <A> or instance of <B>`) — every sub-shape inlined, no `@id`.
    #[pyo3(get)]
    constraint: String,
    inner: HoleConstraint,
    schema: Arc<Schema>,
    data: Arc<Graph>,
}

#[pymethods]
impl Hole {
    /// Up to `limit` candidate terms (N-Triples syntax) satisfying the
    /// constraint, drawn reuse-first from the data graph. Bind one straight back
    /// via `RepairPlan.bind(hole.id, value)`.
    #[pyo3(signature = (limit=64))]
    fn candidates(&self, py: Python<'_>, limit: usize) -> Vec<String> {
        py.allow_threads(|| {
            engine_candidates(&self.inner, &self.data, limit)
                .iter()
                .map(|t| t.to_string())
                .collect()
        })
    }

    /// The sub-shape id for a single `conforms to` hole (`None` for a multi-shape
    /// `ConformsToAll` or a non-conformance hole — use `conforms_to_shapes` to
    /// cover both). Feed it to [`RepairSession.repair_node_against`].
    #[getter]
    fn conforms_to(&self) -> Option<u32> {
        match &self.inner {
            HoleConstraint::ConformsTo(s) => Some(s.0),
            _ => None,
        }
    }

    /// Every sub-shape id the bound value must conform to: one for `ConformsTo`,
    /// all of them for `ConformsToAll`, empty otherwise. The complete set a driver
    /// must build the value against (each via `RepairSession.repair_node_against`).
    #[getter]
    fn conforms_to_shapes(&self) -> Vec<u32> {
        match &self.inner {
            HoleConstraint::ConformsTo(s) => vec![s.0],
            HoleConstraint::ConformsToAll(ss) => ss.iter().map(|s| s.0).collect(),
            _ => Vec::new(),
        }
    }

    /// Each conform-to sub-shape as `(id, definition)`: its arena id plus a
    /// fully-expanded, human-readable definition. Empty for non-conformance holes.
    /// Lets a driver read *and* recursively build every obligation on the value.
    fn sub_shapes(&self) -> Vec<(u32, String)> {
        self.conforms_to_shapes()
            .into_iter()
            .map(|id| {
                (
                    id,
                    shifty_algebra::render::describe_shape(&self.schema.arena, ShapeId(id)),
                )
            })
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("Hole(id={}, constraint={:?})", self.id, self.constraint)
    }
}

/// The kind of decision point in a [`RepairTree`] — the enumerated discriminant
/// of [`Choice`]. `Any` is a disjunction (pick one branch); `Repeat` a bounded
/// repetition (pick a count).
#[pyclass(eq, eq_int, hash, frozen, name = "ChoiceKind")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ChoiceKind {
    Any,
    Repeat,
}

/// An `Any`/`Repeat` decision point in a [`RepairTree`].
#[pyclass(get_all, name = "Choice")]
#[derive(Clone)]
pub struct Choice {
    pub node_id: u32,
    /// Which kind of decision point (see [`ChoiceKind`]).
    pub kind: ChoiceKind,
    /// Number of branches, for an `Any`.
    pub branches: Option<usize>,
    /// Minimum count, for a `Repeat`.
    pub min: Option<u64>,
    /// Maximum count (`None` = unbounded), for a `Repeat`.
    pub max: Option<u64>,
}

#[pymethods]
impl Choice {
    fn __repr__(&self) -> String {
        format!("Choice(node_id={}, kind={:?})", self.node_id, self.kind)
    }
}

/// A driver's choices over a [`RepairTree`]: `Any` branches, `Repeat` counts, and
/// hole bindings. Serializable and position-stable (keyed by node/hole id).
#[pyclass(name = "RepairPlan")]
#[derive(Default)]
pub struct RepairPlan {
    inner: Plan,
}

#[pymethods]
impl RepairPlan {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// Take child `branch_index` at the `Any` node `node_id`.
    fn choose(&mut self, node_id: u32, branch_index: usize) {
        self.inner.branch.insert(NodeId(node_id), branch_index);
    }

    /// Materialize `n` instances at the `Repeat` node `node_id`.
    fn count(&mut self, node_id: u32, n: u64) {
        self.inner.count.insert(NodeId(node_id), n);
    }

    /// Bind hole `hole_id` to `value` (N-Triples term syntax).
    fn bind(&mut self, hole_id: u32, value: &str) -> PyResult<()> {
        let term = parse_term(value).map_err(py_value_error)?;
        self.inner.binding.insert(IrHole(hole_id), term);
        Ok(())
    }

    /// Drop any choice/binding for an id (hole binding and node choice alike).
    fn clear(&mut self, id: u32) {
        self.inner.branch.remove(&NodeId(id));
        self.inner.count.remove(&NodeId(id));
        self.inner.binding.remove(&IrHole(id));
    }

    fn __repr__(&self) -> String {
        format!(
            "RepairPlan(branches={}, counts={}, bindings={})",
            self.inner.branch.len(),
            self.inner.count.len(),
            self.inner.binding.len()
        )
    }
}

/// The result of folding a plan into a tree: the `ΔG` plus what is still open.
#[pyclass(name = "Instantiated")]
pub struct Instantiated {
    delta: shifty_repair::GraphDelta,
    open_holes: Vec<(u32, HoleConstraint)>,
    open_choices: Vec<u32>,
    schema: Arc<Schema>,
    data: Arc<Graph>,
}

#[pymethods]
impl Instantiated {
    /// The concrete graph delta resolved so far.
    #[getter]
    fn delta(&self) -> RepairDelta {
        RepairDelta {
            inner: self.delta.clone(),
        }
    }

    /// Holes still needing a binding, as `Hole` objects (with live `candidates`).
    #[getter]
    fn open_holes(&self, py: Python<'_>) -> PyResult<Vec<Py<Hole>>> {
        self.open_holes
            .iter()
            .map(|(id, c)| {
                Py::new(
                    py,
                    Hole {
                        id: *id,
                        constraint: constraint_str(c, &self.schema.arena),
                        inner: c.clone(),
                        schema: Arc::clone(&self.schema),
                        data: Arc::clone(&self.data),
                    },
                )
            })
            .collect()
    }

    /// `Any`/`Repeat` node ids still needing a choice.
    #[getter]
    fn open_choices(&self) -> Vec<u32> {
        self.open_choices.clone()
    }

    /// True when nothing is open: the plan fully determines the delta.
    #[getter]
    fn is_complete(&self) -> bool {
        self.open_holes.is_empty() && self.open_choices.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "Instantiated(add={}, delete={}, open_holes={}, open_choices={})",
            self.delta.add.len(),
            self.delta.delete.len(),
            self.open_holes.len(),
            self.open_choices.len()
        )
    }
}

/// A set of triple additions and deletions — the `ΔG` a driver gates and applies.
#[pyclass(name = "RepairDelta")]
#[derive(Clone)]
pub struct RepairDelta {
    inner: shifty_repair::GraphDelta,
}

#[pymethods]
impl RepairDelta {
    /// Build a delta directly from triples in N-Triples syntax — for a
    /// driver that authors a *subgraph* patch by hand (e.g. a connection point
    /// with its type assertion) rather than binding a single hole. `add` and
    /// `delete` are each whole N-Triples documents (possibly empty). The result
    /// gates and applies through the same path as a synthesized delta, so the
    /// gate still rejects a patch that doesn't make sound progress.
    #[staticmethod]
    #[pyo3(signature = (add="", delete=""))]
    fn from_ntriples(add: &str, delete: &str) -> PyResult<Self> {
        let parse = |doc: &str| -> PyResult<Vec<oxrdf::Triple>> {
            if doc.trim().is_empty() {
                return Ok(Vec::new());
            }
            let loaded = shifty_parse::load_ntriples(doc.as_bytes())
                .map_err(|e| py_value_error(format!("cannot parse N-Triples: {e}")))?;
            Ok(loaded.graph.iter().map(|t| t.into_owned()).collect())
        };
        Ok(RepairDelta {
            inner: shifty_repair::GraphDelta {
                add: parse(add)?,
                delete: parse(delete)?,
            },
        })
    }

    /// Triples to add, as `(subject, predicate, object)` N-Triples-string tuples.
    #[getter]
    fn add(&self) -> Vec<(String, String, String)> {
        self.inner.add.iter().map(triple_strs).collect()
    }

    /// Triples to delete, as `(subject, predicate, object)` tuples.
    #[getter]
    fn delete(&self) -> Vec<(String, String, String)> {
        self.inner.delete.iter().map(triple_strs).collect()
    }

    #[getter]
    fn is_empty(&self) -> bool {
        self.inner.add.is_empty() && self.inner.delete.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "RepairDelta(add={}, delete={})",
            self.inner.add.len(),
            self.inner.delete.len()
        )
    }
}

fn triple_strs(t: &oxrdf::Triple) -> (String, String, String) {
    (
        t.subject.to_string(),
        t.predicate.to_string(),
        t.object.to_string(),
    )
}

/// The gate's verdict on a `ΔG`: which violations it fixes, introduces, or leaves.
#[pyclass(name = "RepairOutcome")]
pub struct RepairOutcome {
    #[pyo3(get)]
    is_sound: bool,
    #[pyo3(get)]
    is_progress: bool,
    fixed: Vec<Py<Violation>>,
    introduced: Vec<Py<Violation>>,
    remaining: Vec<Py<Violation>>,
}

#[pymethods]
impl RepairOutcome {
    /// Pre-existing violations this `ΔG` removes.
    #[getter]
    fn fixed(&self, py: Python<'_>) -> Vec<Py<Violation>> {
        self.fixed.iter().map(|v| v.clone_ref(py)).collect()
    }

    /// New violations this `ΔG` would cause (empty ⟺ sound).
    #[getter]
    fn introduced(&self, py: Python<'_>) -> Vec<Py<Violation>> {
        self.introduced.iter().map(|v| v.clone_ref(py)).collect()
    }

    /// Pre-existing violations left unaddressed.
    #[getter]
    fn remaining(&self, py: Python<'_>) -> Vec<Py<Violation>> {
        self.remaining.iter().map(|v| v.clone_ref(py)).collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "RepairOutcome(sound={}, progress={}, fixed={}, introduced={}, remaining={})",
            self.is_sound,
            self.is_progress,
            self.fixed.len(),
            self.introduced.len(),
            self.remaining.len()
        )
    }
}

/// Register the repair classes on the `_shifty` module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(expand_evidence_json, m)?)?;
    m.add_class::<EvidenceSession>()?;
    m.add_class::<EvidenceValidationOutcome>()?;
    m.add_class::<PyStatementEvaluation>()?;
    m.add_class::<FocusEvidence>()?;
    m.add_class::<PyEvaluationProgress>()?;
    m.add_class::<PyChildEvaluation>()?;
    m.add_class::<PyEvidenceNode>()?;
    m.add_class::<PyPathSupport>()?;
    m.add_class::<PyMissingObligation>()?;
    m.add_class::<PyConformanceRun>()?;
    m.add_class::<PySelectedPair>()?;
    m.add_class::<RepairSession>()?;
    m.add_class::<FocusWitness>()?;
    m.add_class::<FocusSatisfaction>()?;
    m.add_class::<Target>()?;
    m.add_class::<TargetKind>()?;
    m.add_class::<PyEvidenceKind>()?;
    m.add_class::<WitnessAtom>()?;
    m.add_class::<WitnessKind>()?;
    m.add_class::<SatAtom>()?;
    m.add_class::<SatKind>()?;
    m.add_class::<RepairTree>()?;
    m.add_class::<RepairOrigin>()?;
    m.add_class::<Hole>()?;
    m.add_class::<Choice>()?;
    m.add_class::<ChoiceKind>()?;
    m.add_class::<RepairPlan>()?;
    m.add_class::<Instantiated>()?;
    m.add_class::<RepairDelta>()?;
    m.add_class::<RepairOutcome>()?;
    Ok(())
}
