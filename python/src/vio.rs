//! Violation generation for Python, behind `shifty.experimental.vio`.
//!
//! The expansion, sampling, composition, and verification all run in
//! `shifty_experimental::vio`. This module mirrors its `Universe` and
//! `Case`, handing injections to the Python wrapper as plain tuples that it
//! turns into dataclasses. Filters run in Rust, during the streamed
//! expansion; a Python callable filter is called with the injection's tuple.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;

use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use shifty_experimental::vio::{
    Case, Error, ExpansionLimits, Injection, Operation, OperationKind, Options, Triple, Universe,
    Weights,
};

use crate::repair::RepairDelta;
use crate::{InputSpec, py_value_error};

fn py_error(error: Error) -> PyErr {
    py_value_error(error.to_string())
}

/// `(kind, node, polarity, deletes, adds, detail)`
type PyOperation = (
    &'static str,
    String,
    &'static str,
    Vec<Triple>,
    Vec<Triple>,
    String,
);

/// `(focus, statement, shape, key, operations)`
type PyInjection = (String, usize, Option<String>, String, Vec<PyOperation>);

fn operation(op: &Operation) -> PyOperation {
    (
        op.kind.as_str(),
        op.node.clone(),
        op.polarity.as_str(),
        op.deletes.clone(),
        op.adds.clone(),
        op.detail.clone(),
    )
}

fn injection(e: &Injection) -> PyInjection {
    (
        e.focus.clone(),
        e.statement,
        e.shape.clone(),
        e.key().to_owned(),
        e.operations.iter().map(operation).collect(),
    )
}

/// A universe, or a filtered or reweighted view of one.
#[pyclass(unsendable, name = "VioUniverse", module = "shifty._shifty")]
pub struct VioUniverse {
    inner: Universe,
    /// The first exception a Python filter raised. A Rust filter returns a
    /// bool, so the exception waits here until the call that ran it ends.
    raised: Rc<RefCell<Option<PyErr>>>,
}

impl VioUniverse {
    fn view(&self, inner: Universe) -> Self {
        Self {
            inner,
            raised: Rc::clone(&self.raised),
        }
    }

    /// Raise what a Python filter raised during the last call, if anything.
    fn check(&self) -> PyResult<()> {
        match self.raised.borrow_mut().take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

#[pymethods]
impl VioUniverse {
    #[new]
    #[pyo3(signature = (
        data=None,
        data_path=None,
        data_format="auto",
        shapes=None,
        shapes_path=None,
        shapes_format="auto",
        materialize=false,
        infer=true,
        include_external_foci=false,
        canonicalize_blank_nodes=true,
        graph_mode="union",
        on_unsupported="error",
        base=None,
        limits=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        shapes: Option<PyBackedBytes>,
        shapes_path: Option<String>,
        shapes_format: &str,
        materialize: bool,
        infer: bool,
        include_external_foci: bool,
        canonicalize_blank_nodes: bool,
        graph_mode: &str,
        on_unsupported: &str,
        base: Option<String>,
        limits: Option<(usize, usize, usize, usize, usize, usize, usize)>,
    ) -> PyResult<Self> {
        let data = InputSpec::new(data, data_path, data_format, "data")
            .and_then(|spec| spec.load(base.as_deref()))
            .map_err(py_value_error)?;
        let shapes = InputSpec::new(shapes, shapes_path, shapes_format, "shapes")
            .and_then(|spec| spec.load(base.as_deref()))
            .map_err(py_value_error)?;
        let limits = match limits {
            Some((
                max_alternatives,
                max_values,
                max_candidates,
                max_combinations,
                max_depth,
                max_per_target,
                max_injections,
            )) => ExpansionLimits {
                max_alternatives,
                max_values,
                max_candidates,
                max_combinations,
                max_depth,
                max_per_target,
                max_injections,
            },
            None => ExpansionLimits::default(),
        };
        let options = Options {
            materialize,
            infer,
            include_external_foci,
            canonicalize_blank_nodes,
            graph_mode: crate::parse_mode(graph_mode).map_err(py_value_error)?,
            engine: crate::engine_options(on_unsupported).map_err(py_value_error)?,
            limits,
        };
        let inner = Universe::new(shapes, data.graph, options).map_err(py_error)?;
        Ok(Self {
            inner,
            raised: Rc::new(RefCell::new(None)),
        })
    }

    /// The number of injections. Expands and caches the universe.
    fn __len__(&self) -> PyResult<usize> {
        let n = self.inner.len();
        self.check()?;
        Ok(n)
    }

    /// The injections of this view, in expansion order. Expands and caches
    /// the universe.
    fn injections(&self) -> PyResult<Vec<PyInjection>> {
        let rows = self.inner.iter().map(injection).collect();
        self.check()?;
        Ok(rows)
    }

    /// The view of the injections that break one of `shapes` (bare IRIs),
    /// edit one of `predicates` (N-Triples IRIs), break one of `foci`
    /// (N-Triples terms), and for which `keep(row)` is true. `None` means
    /// no restriction.
    #[pyo3(signature = (shapes=None, predicates=None, foci=None, keep=None))]
    fn filtered(
        &self,
        shapes: Option<HashSet<String>>,
        predicates: Option<HashSet<String>>,
        foci: Option<HashSet<String>>,
        keep: Option<Py<PyAny>>,
    ) -> Self {
        let raised = Rc::clone(&self.raised);
        let inner = self.inner.filter(move |e| {
            if shapes
                .as_ref()
                .is_some_and(|s| !e.shape.as_ref().is_some_and(|x| s.contains(x)))
                || foci.as_ref().is_some_and(|f| !f.contains(&e.focus))
                || predicates
                    .as_ref()
                    .is_some_and(|p| !e.predicates().iter().any(|x| p.contains(x)))
            {
                return false;
            }
            let Some(keep) = &keep else {
                return true;
            };
            if raised.borrow().is_some() {
                return false;
            }
            Python::attach(|py| {
                match keep
                    .bind(py)
                    .call1((injection(e),))
                    .and_then(|r| r.is_truthy())
                {
                    Ok(b) => b,
                    Err(err) => {
                        *raised.borrow_mut() = Some(err);
                        false
                    }
                }
            })
        });
        self.view(inner)
    }

    /// The same injections, drawn by these weights instead.
    fn weighted(
        &self,
        predicates: BTreeMap<String, f64>,
        kinds: BTreeMap<String, f64>,
    ) -> PyResult<Self> {
        let kinds = kinds
            .into_iter()
            .map(|(name, w)| {
                OperationKind::parse(&name)
                    .map(|k| (k, w))
                    .ok_or_else(|| py_value_error(format!("unknown operation kind {name:?}")))
            })
            .collect::<PyResult<_>>()?;
        let inner = self
            .inner
            .weighted(Weights { predicates, kinds })
            .map_err(py_error)?;
        Ok(self.view(inner))
    }

    /// One case per `(k, seed)`, from one pass over the expansion.
    fn draws(&self, requests: Vec<(usize, u64)>) -> PyResult<Vec<VioCase>> {
        let cases = self.inner.draws(&requests);
        self.check()?;
        Ok(cases
            .map_err(py_error)?
            .into_iter()
            .map(|case| VioCase { case })
            .collect())
    }

    /// The reference graph as sorted N-Triples.
    fn reference_ntriples(&self) -> String {
        let mut out = String::new();
        for t in self.inner.reference() {
            out.push_str(&shifty_experimental::vio::nt::line(t));
            out.push('\n');
        }
        out
    }

    /// `(focus, statement)` pairs that fail in the reference graph.
    fn baseline_failures(&self) -> Vec<(String, usize)> {
        self.inner.baseline_failures().iter().cloned().collect()
    }
}

/// One drawn case.
#[pyclass(unsendable, name = "VioCase", module = "shifty._shifty")]
pub struct VioCase {
    case: Case,
}

#[pymethods]
impl VioCase {
    #[getter]
    fn k(&self) -> usize {
        self.case.k
    }

    #[getter]
    fn seed(&self) -> u64 {
        self.case.seed
    }

    /// The injections, in admission order.
    #[getter]
    fn injections(&self) -> Vec<PyInjection> {
        self.case.injections.iter().map(injection).collect()
    }

    /// `(focus, statement, shape, injection indices)` per failing pair.
    #[getter]
    fn failures(&self) -> Vec<(String, usize, Option<String>, Vec<usize>)> {
        self.case
            .failures
            .iter()
            .map(|f| {
                (
                    f.focus.clone(),
                    f.statement,
                    f.shape.clone(),
                    f.injections.clone(),
                )
            })
            .collect()
    }

    #[getter]
    fn delta(&self) -> RepairDelta {
        self.case.delta().clone().into()
    }

    #[getter]
    fn repair(&self) -> RepairDelta {
        self.case.repair().into()
    }

    fn graph_ntriples(&self) -> String {
        self.case.graph_ntriples()
    }

    fn apply_ntriples(&self, delta: &RepairDelta) -> String {
        self.case.apply_ntriples(&delta.inner)
    }

    fn repair_sparql(&self) -> String {
        self.case.repair_sparql()
    }

    fn to_json(&self) -> String {
        self.case.to_json().to_string()
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<VioUniverse>()?;
    m.add_class::<VioCase>()?;
    Ok(())
}
