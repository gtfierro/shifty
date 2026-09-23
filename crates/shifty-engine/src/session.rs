//! Evaluation over one immutable asserted-data snapshot.

use crate::compiled::CompiledShapes;
use crate::context;
use crate::evidence::{
    ConformanceOptions, ConformanceRun, PreparedEvidenceValidator, SelectedPair,
};
use crate::frozen::FrozenIndexedDataset;
use crate::gate::RepairOutcome;
use crate::validate::{
    EngineOptions, UnsupportedPolicy, ValidationGraphMode, ValidationOptions, ValidationOutcome,
};
use crate::witness::{EvidenceRun, StatementEvaluation};
use oxrdf::{BlankNode, Graph, NamedOrBlankNode, Term, Triple};
use shifty_algebra::Severity;
use shifty_parse::{DiagLevel, Diagnostic};
use shifty_repair::GraphDelta;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

#[expect(
    clippy::large_enum_variant,
    reason = "the documented public API consumes an owned Graph"
)]
pub enum SessionData {
    Separate(Graph),
    Embedded,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SessionOptions {
    pub graph_mode: ValidationGraphMode,
    pub inference: bool,
    pub engine: EngineOptions,
}

#[derive(Debug, Clone)]
pub struct FindingOptions {
    pub entry_shape_names: Vec<String>,
    pub minimum_severity: Severity,
    pub sort_results: bool,
}

impl Default for FindingOptions {
    fn default() -> Self {
        Self {
            entry_shape_names: Vec::new(),
            minimum_severity: Severity::Info,
            sort_results: true,
        }
    }
}

impl FindingOptions {
    fn validation(&self, engine: EngineOptions) -> ValidationOptions {
        ValidationOptions {
            entry_shape_names: self.entry_shape_names.clone(),
            minimum_severity: self.minimum_severity.clone(),
            sort_results: self.sort_results,
            engine,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EvidenceOptions {
    pub findings: FindingOptions,
    pub include_progress: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionDiagnostic {
    pub message: String,
}

#[derive(Debug)]
pub enum SessionError {
    EmptyShapes,
    UnsupportedFeatures(Vec<Diagnostic>),
    StrictInference(Vec<ExecutionDiagnostic>),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyShapes => write!(f, "separate input requires a nonempty shapes graph"),
            Self::UnsupportedFeatures(diagnostics) => write!(
                f,
                "unsupported shapes features: {}",
                diagnostics
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Self::StrictInference(diagnostics) => write!(
                f,
                "strict inference failed: {}",
                diagnostics
                    .iter()
                    .map(|diagnostic| diagnostic.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        }
    }
}

impl std::error::Error for SessionError {}

/// Only `explain` and `explain_canonical` can fail: every other evaluation on a
/// constructed session is total, so those return their value directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvaluationError {
    ForeignPair,
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignPair => write!(f, "selected pair belongs to another session"),
        }
    }
}

impl std::error::Error for EvaluationError {}

pub struct EvaluationSession {
    compiled: CompiledShapes,
    asserted: Arc<Graph>,
    evaluated: OnceCell<Arc<Graph>>,
    separate: bool,
    options: SessionOptions,
    inferred: Vec<Triple>,
    external_blank_nodes: HashMap<BlankNode, BlankNode>,
    diagnostics: Vec<ExecutionDiagnostic>,
    inference_dataset: RefCell<Option<FrozenIndexedDataset>>,
    prepared: OnceCell<PreparedEvidenceValidator>,
    snapshot: Arc<()>,
}

impl CompiledShapes {
    pub fn session(
        &self,
        data: SessionData,
        options: SessionOptions,
    ) -> Result<EvaluationSession, SessionError> {
        let (asserted, separate) = match data {
            SessionData::Separate(data) => {
                if self.source().graph.is_empty() {
                    return Err(SessionError::EmptyShapes);
                }
                (data, true)
            }
            SessionData::Embedded => (self.source().graph.clone(), false),
        };
        self.session_from_asserted(asserted, separate, options)
    }

    fn session_from_asserted(
        &self,
        asserted: Graph,
        separate: bool,
        options: SessionOptions,
    ) -> Result<EvaluationSession, SessionError> {
        if options.engine.unsupported == UnsupportedPolicy::Error {
            let unsupported: Vec<_> = self
                .diagnostics()
                .iter()
                .filter(|diagnostic| diagnostic.level == DiagLevel::Unsupported)
                .cloned()
                .collect();
            if !unsupported.is_empty() {
                return Err(SessionError::UnsupportedFeatures(unsupported));
            }
        }
        let asserted = Arc::new(asserted);
        // No executable rule can change this snapshot. In particular, avoid
        // assembling a shapes/data union or encoding the source just to hand
        // the inference driver an empty schedule.
        let (evaluated, inferred, diagnostics, inference_dataset, external_blank_nodes) =
            if options.inference && !self.inner.rules.is_empty() {
                let run = crate::infer::infer_with_compiled_functions(
                    &asserted,
                    &self.inner.normalized,
                    &options.engine,
                    &self.inner.functions,
                    &self.inner.rules,
                    (self.source_storage(), separate, self.parsed_queries()),
                );
                let diagnostics: Vec<_> = run
                    .diagnostics
                    .into_iter()
                    .map(|message| ExecutionDiagnostic { message })
                    .collect();
                if options.engine.unsupported == UnsupportedPolicy::Error && !diagnostics.is_empty()
                {
                    return Err(SessionError::StrictInference(diagnostics));
                }
                let external_blank_nodes = run.dataset.external_blank_node_map();
                let mut inference_dataset = Some(run.dataset);
                if separate
                    && options.graph_mode == ValidationGraphMode::Data
                    && let Some(dataset) = &mut inference_dataset
                {
                    dataset.select_data_view();
                }
                (
                    None,
                    run.inferred,
                    diagnostics,
                    inference_dataset,
                    external_blank_nodes,
                )
            } else {
                (
                    Some(Arc::clone(&asserted)),
                    Vec::new(),
                    Vec::new(),
                    None,
                    HashMap::new(),
                )
            };
        let evaluated_cell = OnceCell::new();
        if let Some(graph) = evaluated {
            evaluated_cell.set(graph).expect("new evaluated graph cell");
        }
        Ok(EvaluationSession {
            compiled: self.clone(),
            asserted,
            evaluated: evaluated_cell,
            separate,
            options,
            inferred,
            external_blank_nodes,
            diagnostics,
            inference_dataset: RefCell::new(inference_dataset),
            prepared: OnceCell::new(),
            snapshot: Arc::new(()),
        })
    }
}

impl EvaluationSession {
    #[cfg(test)]
    pub(crate) fn has_prepared_dataset(&self) -> bool {
        self.prepared.get().is_some()
    }

    fn prepared(&self) -> &PreparedEvidenceValidator {
        self.prepared.get_or_init(|| {
            let frozen = self
                .inference_dataset
                .borrow_mut()
                .take()
                .unwrap_or_else(|| {
                    context::frozen(
                        self.evaluated.get().expect("dataset or evaluated graph"),
                        self.compiled.source_storage(),
                        self.separate,
                        self.options.graph_mode,
                    )
                });
            PreparedEvidenceValidator::from_compiled(
                self.evaluated.get().cloned(),
                &self.compiled,
                frozen,
                true,
                self.options.engine.unsupported,
                if self.separate && self.options.graph_mode != ValidationGraphMode::UnionAll {
                    crate::focus::FocusScope::Data
                } else {
                    crate::focus::FocusScope::Default
                },
                self.separate && self.options.graph_mode == ValidationGraphMode::UnionAll,
            )
        })
    }

    fn evaluated_graph(&self) -> &Arc<Graph> {
        self.evaluated.get_or_init(|| {
            if let Some(dataset) = self.inference_dataset.borrow().as_ref() {
                return Arc::new(dataset.data_graph_projection());
            }
            self.prepared().data_graph_shared()
        })
    }

    /// Borrow the prepared evidence view for existing shape-map projections.
    pub fn prepared_evidence(&self) -> &PreparedEvidenceValidator {
        self.prepared()
    }

    pub fn validate(&self, options: &FindingOptions) -> ValidationOutcome {
        self.prepared().validate_findings(
            self.compiled.physical_plan(),
            &options.validation(self.options.engine),
        )
    }

    pub fn report(&self, options: &FindingOptions) -> crate::report::ValidationReport {
        self.prepared().report(
            self.compiled.source(),
            &options.validation(self.options.engine),
        )
    }

    pub fn property_witnesses(
        &self,
        key_path: Option<&shifty_algebra::Path>,
        options: &FindingOptions,
    ) -> Vec<crate::report::PropertyWitness> {
        self.prepared().property_witnesses(
            self.compiled.source(),
            key_path,
            &options.validation(self.options.engine),
        )
    }

    pub fn evidence(&self, options: &EvidenceOptions) -> EvidenceRun {
        let validation = options.findings.validation(self.options.engine);
        if options.include_progress {
            self.prepared().validate(&validation)
        } else {
            self.prepared().validate_canonical(&validation)
        }
    }

    pub fn conformance(&self, options: &ConformanceOptions) -> ConformanceRun {
        self.prepared().validate_conformance(options)
    }

    pub fn find_failures(
        &self,
        options: &ConformanceOptions,
    ) -> (ConformanceRun, Vec<SelectedPair>) {
        let (run, mut pairs) = self.prepared().find_failures(options);
        for pair in &mut pairs {
            pair.snapshot = Some(Arc::clone(&self.snapshot));
        }
        (run, pairs)
    }

    pub fn explain(
        &self,
        pair: &SelectedPair,
    ) -> Result<Vec<StatementEvaluation>, EvaluationError> {
        self.check_pair(pair)?;
        Ok(self.prepared().explain(pair))
    }

    pub fn explain_canonical(
        &self,
        pair: &SelectedPair,
    ) -> Result<Vec<StatementEvaluation>, EvaluationError> {
        self.check_pair(pair)?;
        Ok(self.prepared().explain_canonical(pair))
    }

    fn check_pair(&self, pair: &SelectedPair) -> Result<(), EvaluationError> {
        if !pair
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| Arc::ptr_eq(snapshot, &self.snapshot))
        {
            return Err(EvaluationError::ForeignPair);
        }
        Ok(())
    }

    pub fn with_delta(&self, delta: &GraphDelta) -> Result<Self, SessionError> {
        let patched = crate::gate::apply(&self.asserted, delta);
        self.compiled
            .session_from_asserted(patched, self.separate, self.options)
    }

    /// Revalidate a delta with a different inference setting. Enabling rules
    /// patches asserted data and derives again; disabling them patches the
    /// already evaluated graph so previously inferred triples remain present.
    pub fn with_delta_and_inference(
        &self,
        delta: &GraphDelta,
        inference: bool,
    ) -> Result<Self, SessionError> {
        if inference == self.options.inference {
            return self.with_delta(delta);
        }
        let baseline = if inference {
            self.asserted.as_ref()
        } else {
            self.evaluated_graph()
        };
        let patched = crate::gate::apply(baseline, delta);
        let mut options = self.options;
        options.inference = inference;
        self.compiled
            .session_from_asserted(patched, self.separate, options)
    }

    pub fn data(&self) -> &Graph {
        self.evaluated_graph()
    }

    /// Share the evaluated data graph without copying it.
    pub fn data_shared(&self) -> Arc<Graph> {
        Arc::clone(self.evaluated_graph())
    }

    /// Share the asserted data graph used by `with_delta`.
    pub fn asserted_shared(&self) -> Arc<Graph> {
        Arc::clone(&self.asserted)
    }

    pub fn inferred(&self) -> &[Triple] {
        &self.inferred
    }

    /// Inferred triples with input data blank-node labels restored and shapes
    /// nodes kept distinct, for adding the delta to a caller-owned data graph.
    pub fn inferred_for_write_back(&self) -> Vec<Triple> {
        self.inferred
            .iter()
            .map(|triple| {
                let subject = match &triple.subject {
                    NamedOrBlankNode::BlankNode(node) => NamedOrBlankNode::BlankNode(
                        self.external_blank_nodes
                            .get(node)
                            .cloned()
                            .unwrap_or_else(|| node.clone()),
                    ),
                    named => named.clone(),
                };
                let object = match &triple.object {
                    Term::BlankNode(node) => Term::BlankNode(
                        self.external_blank_nodes
                            .get(node)
                            .cloned()
                            .unwrap_or_else(|| node.clone()),
                    ),
                    term => term.clone(),
                };
                Triple::new(subject, triple.predicate.clone(), object)
            })
            .collect()
    }

    pub fn diagnostics(&self) -> &[ExecutionDiagnostic] {
        &self.diagnostics
    }

    pub fn gate(&self, delta: &GraphDelta) -> Result<RepairOutcome, SessionError> {
        let baseline = self.validate(&FindingOptions::default());
        let candidate = self.with_delta(delta)?;
        let patched = candidate.validate(&FindingOptions::default());
        Ok(crate::gate::diff(baseline.violations, patched.violations))
    }
}
