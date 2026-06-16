//! Python bindings for the symbolic-repair API (`docs/06-repair.md`).
//!
//! These expose the repair *primitives* — witness → synthesize → enumerate
//! candidates → instantiate → gate → apply — as plain Python objects, so an
//! external driver can inspect the horizon of violations per focus node, list the
//! holes and their options, make its own choices, and gate before committing. The
//! library computes and gates; **it decides nothing**. No canned repair loop is
//! shipped here — the driver is yours to write in Python.

use crate::{InputSpec, Violation, graph_to_ntriples, py_value_error, violation_to_py};
use oxrdf::{Graph, Term};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use shifty_algebra::{Schema, Selector, ShapeId};
use shifty_engine::{
    FocusSat as IrSat, FocusWitness as IrFocus, SatTrace, Witness, apply as engine_apply,
    candidates as engine_candidates, gate as engine_gate, satisfy_shape, shape_id_for_iri,
    synthesize, witness_node, witness_shape, witness_violations,
};
use shifty_repair::{
    Edit, EditOp, Hole as IrHole, HoleConstraint, NodeId, Plan, RepairTree as IrTree, Slot,
    instantiate,
};
use std::sync::Arc;

// ── shared rendering (ports of the CLI renderers; kept local to the binding) ────

fn path_str(p: &shifty_algebra::Path) -> String {
    shifty_algebra::render::path_to_string(p)
}

fn constraint_str(c: &HoleConstraint) -> String {
    match c {
        HoleConstraint::AnyNode => "any node".to_string(),
        HoleConstraint::Fresh => "fresh node".to_string(),
        HoleConstraint::Const(t) => format!("= {t}"),
        HoleConstraint::Typed(vt) => shifty_algebra::render::value_type_to_string(vt),
        HoleConstraint::Kind(_) => "nodeKind".to_string(),
        HoleConstraint::OneOf(v) => format!("one of {} value(s)", v.len()),
        HoleConstraint::ConformsTo(s) => format!("conforms to @{}", s.0),
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
        SatTrace::NotHeld { inner_fails, .. } => {
            out.push(format!("{pad}NotHeld — make the inner shape hold:"));
            render_witness(inner_fails, indent + 2, out);
        }
        SatTrace::Blocked { reason, .. } => out.push(format!("{pad}Blocked: {reason:?}")),
        SatTrace::Coinductive { .. } => out.push(format!("{pad}Coinductive (gfp back-edge)")),
    }
}

fn render_tree(t: &IrTree, indent: usize, out: &mut Vec<String>) {
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
                out.push(format!("{pad}  ?{} : {}", h.0, constraint_str(c)));
            }
        }
        IrTree::All { children, .. } => {
            out.push(format!("{pad}All — do all:"));
            for c in children {
                render_tree(c, indent + 2, out);
            }
        }
        IrTree::Any { children, .. } => {
            out.push(format!("{pad}Any — choose one:"));
            for c in children {
                render_tree(c, indent + 2, out);
            }
        }
        IrTree::Repeat { body, min, max, .. } => {
            let hi = max.map_or_else(|| "∞".to_string(), |m| m.to_string());
            out.push(format!("{pad}Repeat [{min}..{hi}]:"));
            render_tree(body, indent + 2, out);
        }
    }
}

// ── flat witness summary ────────────────────────────────────────────────────────

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

fn witness_leaves(w: &Witness, out: &mut Vec<WitnessAtom>) {
    match w {
        Witness::Atom {
            node,
            reached_by,
            produced_by,
            ..
        } => out.push(WitnessAtom {
            kind: WitnessKind::Atom,
            path: Some(path_str(reached_by)),
            value: Some(node.to_string()),
            detail: if produced_by.is_some() {
                "value-type test failed (edge is cuttable)".into()
            } else {
                "value-type test failed on the focus".into()
            },
        }),
        Witness::Relational {
            kind,
            node,
            offending,
            ..
        } => out.push(WitnessAtom {
            kind: WitnessKind::Relational,
            path: None,
            value: Some(node.to_string()),
            detail: format!("{kind:?}: {} offending pair(s)", offending.len()),
        }),
        Witness::Closed {
            node, offenders, ..
        } => out.push(WitnessAtom {
            kind: WitnessKind::Closed,
            path: None,
            value: Some(node.to_string()),
            detail: format!("{} disallowed triple(s)", offenders.len()),
        }),
        Witness::CountLow {
            node,
            path,
            have,
            min,
            ..
        } => out.push(WitnessAtom {
            kind: WitnessKind::CountLow,
            path: Some(path_str(path)),
            value: Some(node.to_string()),
            detail: format!("have {have}, need {min}"),
        }),
        Witness::CountHigh {
            node,
            path,
            matched,
            max,
            per_value,
            ..
        } => {
            out.push(WitnessAtom {
                kind: WitnessKind::CountHigh,
                path: Some(path_str(path)),
                value: Some(node.to_string()),
                detail: format!("{} match(es), max {max}", matched.len()),
            });
            for (_, sub) in per_value {
                witness_leaves(sub, out);
            }
        }
        Witness::Not { node, .. } => out.push(WitnessAtom {
            kind: WitnessKind::Not,
            path: None,
            value: Some(node.to_string()),
            detail: "a shape holds that must be falsified".into(),
        }),
        Witness::Opaque { node, .. } => out.push(WitnessAtom {
            kind: WitnessKind::Opaque,
            path: None,
            value: Some(node.to_string()),
            detail: "opaque SPARQL — no algebraic witness".into(),
        }),
        Witness::All { failed, .. } => {
            for f in failed {
                witness_leaves(f, out);
            }
        }
        Witness::Any { branches, .. } => {
            for b in branches {
                witness_leaves(b, out);
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
            for (v, _) in matches {
                out.push(SatAtom {
                    kind: SatKind::Match,
                    path: Some(path_str(path)),
                    value: Some(v.to_string()),
                    detail: format!("matched value (count {bounds})"),
                });
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
            path: None,
            value: Some(node.to_string()),
            detail: "negation holds (the inner shape fails)".into(),
        }),
        SatTrace::Blocked { node, reason, .. } => out.push(SatAtom {
            kind: SatKind::Blocked,
            path: None,
            value: Some(node.to_string()),
            detail: format!("holds, no enumerable values ({reason:?})"),
        }),
        SatTrace::Coinductive { node, .. } => out.push(SatAtom {
            kind: SatKind::Coinductive,
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
    data: Arc<Graph>,
    diagnostics: Vec<String>,
}

impl RepairSession {
    fn from_parts(schema: Arc<Schema>, data: Arc<Graph>, diagnostics: Vec<String>) -> Self {
        Self {
            schema,
            data,
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
}

#[pymethods]
impl RepairSession {
    #[new]
    #[pyo3(signature = (
        shapes=None,
        shapes_path=None,
        shapes_format="turtle",
        data=None,
        data_path=None,
        data_format="turtle",
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
            let diagnostics = parse_out
                .diagnostics
                .iter()
                .map(ToString::to_string)
                .collect();
            let schema = parse_out.schema;

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

            Ok(RepairSession::from_parts(
                Arc::new(schema),
                Arc::new(eval),
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
            .allow_threads(|| witness_violations(&self.data, &self.schema))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        raw.into_iter()
            .map(|fw| {
                let target = shifty_algebra::render::selector_to_string_in(
                    &self.schema.statements[fw.statement].selector,
                    &self.schema.arena,
                );
                Py::new(
                    py,
                    FocusWitness {
                        focus: fw.focus.to_string(),
                        statement: fw.statement,
                        target,
                        inner: fw,
                        schema: Arc::clone(&self.schema),
                        data: Arc::clone(&self.data),
                    },
                )
            })
            .collect()
    }

    /// The violation horizon for a single shape: one [`FocusWitness`] per failing
    /// `(focus, statement)` whose statement targets `shape_iri` (matched against
    /// the schema's shape IRIs; angle brackets optional). The shape-scoped
    /// counterpart of `witnesses()`; its satisfaction-side dual is
    /// `satisfactions_for`. Raises if no shape is named `shape_iri`.
    fn witnesses_for(&self, py: Python<'_>, shape_iri: &str) -> PyResult<Vec<Py<FocusWitness>>> {
        let shape = self.resolve_shape(shape_iri)?;
        let raw = py
            .allow_threads(|| witness_shape(&self.data, &self.schema, shape))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        raw.into_iter()
            .map(|fw| {
                let target = shifty_algebra::render::selector_to_string_in(
                    &self.schema.statements[fw.statement].selector,
                    &self.schema.arena,
                );
                Py::new(
                    py,
                    FocusWitness {
                        focus: fw.focus.to_string(),
                        statement: fw.statement,
                        target,
                        inner: fw,
                        schema: Arc::clone(&self.schema),
                        data: Arc::clone(&self.data),
                    },
                )
            })
            .collect()
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
            .allow_threads(|| satisfy_shape(&self.data, &self.schema, shape))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        raw.into_iter()
            .map(|fs| {
                let target = shifty_algebra::render::selector_to_string_in(
                    &self.schema.statements[fs.statement].selector,
                    &self.schema.arena,
                );
                Py::new(
                    py,
                    FocusSatisfaction {
                        focus: fs.focus.to_string(),
                        statement: fs.statement,
                        target,
                        inner: fs,
                        schema: Arc::clone(&self.schema),
                    },
                )
            })
            .collect()
    }

    /// Re-validate `G ⊕ ΔG` and diff the violation set against `G`'s — the gate.
    /// Decides and applies nothing; returns a [`RepairOutcome`].
    fn gate(&self, py: Python<'_>, delta: &RepairDelta) -> PyResult<RepairOutcome> {
        let outcome = py
            .allow_threads(|| engine_gate(&self.data, &self.schema, &delta.inner))
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
            .allow_threads(|| witness_node(&self.data, &self.schema, &term, ShapeId(shape_id)))
            .map_err(|e| py_value_error(format!("non-stratifiable schema: {e}")))?;
        Ok(fw.map(|fw| RepairTree {
            inner: synthesize(&self.schema.arena, &fw),
            data: Arc::clone(&self.data),
        }))
    }

    /// A new session over `G ⊕ ΔG` (same schema, no re-inference) so a driver can
    /// accept a repair and re-witness from the patched graph.
    fn advance(&self, py: Python<'_>, delta: &RepairDelta) -> Self {
        let next = py.allow_threads(|| engine_apply(&self.data, &delta.inner));
        RepairSession::from_parts(
            Arc::clone(&self.schema),
            Arc::new(next),
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

/// Why one focus node failed one statement, plus its repair tree.
#[pyclass(name = "FocusWitness")]
pub struct FocusWitness {
    #[pyo3(get)]
    focus: String,
    #[pyo3(get)]
    statement: usize,
    /// The statement's target selector, rendered (e.g. `class(ex:Person)`). See
    /// `selector` for the structured form.
    #[pyo3(get)]
    target: String,
    inner: IrFocus,
    schema: Arc<Schema>,
    data: Arc<Graph>,
}

#[pymethods]
impl FocusWitness {
    /// The target selector as a structured [`Target`] (its `kind`, the class /
    /// predicate / node it picks out, …) — the inspectable form of `.target`.
    #[getter]
    fn selector(&self) -> Target {
        build_target(
            &self.schema.statements[self.statement].selector,
            &self.schema,
        )
    }

    /// The failing leaves, flattened (AND/OR structure dropped; see `explain`).
    fn summary(&self) -> Vec<WitnessAtom> {
        let mut out = Vec::new();
        witness_leaves(&self.inner.failure, &mut out);
        out
    }

    /// The full witness tree, rendered as indented text.
    fn explain(&self) -> String {
        let mut out = Vec::new();
        render_witness(&self.inner.failure, 0, &mut out);
        out.join("\n")
    }

    /// Synthesize the repair space (`RepairTree`) for this violation.
    fn repair_tree(&self, py: Python<'_>) -> RepairTree {
        let tree = py.allow_threads(|| synthesize(&self.schema.arena, &self.inner));
        RepairTree {
            inner: tree,
            data: Arc::clone(&self.data),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "FocusWitness(focus={:?}, statement={})",
            self.focus, self.statement
        )
    }
}

/// Why one focus node *satisfies* a statement: the satisfaction-side dual of
/// [`FocusWitness`]. Carries why the node conforms, including the values matched
/// along each checked path. Yielded by [`RepairSession::satisfactions_for`].
#[pyclass(name = "FocusSatisfaction")]
pub struct FocusSatisfaction {
    #[pyo3(get)]
    focus: String,
    #[pyo3(get)]
    statement: usize,
    /// The statement's target selector, rendered (e.g. `class(ex:Person)`). See
    /// `selector` for the structured form.
    #[pyo3(get)]
    target: String,
    inner: IrSat,
    schema: Arc<Schema>,
}

#[pymethods]
impl FocusSatisfaction {
    /// The target selector as a structured [`Target`] — the inspectable form of
    /// `.target`, identical to the witness side for the same statement.
    #[getter]
    fn selector(&self) -> Target {
        build_target(
            &self.schema.statements[self.statement].selector,
            &self.schema,
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

    fn __repr__(&self) -> String {
        format!(
            "FocusSatisfaction(focus={:?}, statement={})",
            self.focus, self.statement
        )
    }
}

/// The repair space for one violation: an AND/OR/Repeat tree with typed holes.
#[pyclass(name = "RepairTree")]
pub struct RepairTree {
    inner: IrTree,
    data: Arc<Graph>,
}

#[pymethods]
impl RepairTree {
    /// True if no data repair is possible in scope (opaque SPARQL / identity /
    /// coinductive).
    #[getter]
    fn is_blocked(&self) -> bool {
        self.inner.is_blocked()
    }

    /// The tree rendered as indented text.
    fn explain(&self) -> String {
        let mut out = Vec::new();
        render_tree(&self.inner, 0, &mut out);
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
                        constraint: constraint_str(&c),
                        inner: c,
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
            data: Arc::clone(&self.data),
        }
    }

    fn __repr__(&self) -> String {
        format!("RepairTree(blocked={})", self.inner.is_blocked())
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
    /// The constraint, rendered (e.g. `typed value`, `one of 2 value(s)`).
    #[pyo3(get)]
    constraint: String,
    inner: HoleConstraint,
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

    /// The sub-shape id for a `conforms to @N` hole (else `None`). Feed it to
    /// [`RepairSession.repair_node_against`] to build the value out recursively.
    #[getter]
    fn conforms_to(&self) -> Option<u32> {
        match &self.inner {
            HoleConstraint::ConformsTo(s) => Some(s.0),
            _ => None,
        }
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
                        constraint: constraint_str(c),
                        inner: c.clone(),
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
    m.add_class::<RepairSession>()?;
    m.add_class::<FocusWitness>()?;
    m.add_class::<FocusSatisfaction>()?;
    m.add_class::<Target>()?;
    m.add_class::<TargetKind>()?;
    m.add_class::<WitnessAtom>()?;
    m.add_class::<WitnessKind>()?;
    m.add_class::<SatAtom>()?;
    m.add_class::<SatKind>()?;
    m.add_class::<RepairTree>()?;
    m.add_class::<Hole>()?;
    m.add_class::<Choice>()?;
    m.add_class::<ChoiceKind>()?;
    m.add_class::<RepairPlan>()?;
    m.add_class::<Instantiated>()?;
    m.add_class::<RepairDelta>()?;
    m.add_class::<RepairOutcome>()?;
    Ok(())
}
