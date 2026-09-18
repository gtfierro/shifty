//! Evaluation over one immutable asserted-data snapshot.

use crate::compiled::CompiledShapes;
use crate::context;
use crate::evidence::{
    ConformanceOptions, ConformanceRun, PreparedEvidenceValidator, SelectedPair,
};
use crate::gate::RepairOutcome;
use crate::validate::{
    EngineOptions, NonStratifiable, UnsupportedPolicy, ValidationGraphMode, ValidationOptions,
    ValidationOutcome,
};
use crate::witness::{EvidenceRun, StatementEvaluation};
use oxrdf::{Graph, Triple};
use shifty_algebra::Severity;
use shifty_parse::{DiagLevel, Diagnostic};
use shifty_repair::GraphDelta;
use std::cell::OnceCell;
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
    Inference(NonStratifiable),
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
            Self::Inference(error) => write!(f, "{error}"),
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
    evaluated: Arc<Graph>,
    separate: bool,
    options: SessionOptions,
    inferred: Vec<Triple>,
    diagnostics: Vec<ExecutionDiagnostic>,
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
        let (evaluated, inferred, diagnostics) = if options.inference {
            let context = if separate {
                crate::validate::graph_union(&asserted, &self.source().graph)
            } else {
                asserted.as_ref().clone()
            };
            let outcome = crate::infer::infer_with_compiled_functions(
                &asserted,
                context,
                &self.inner.normalized,
                &options.engine,
                &self.inner.functions,
                Some(&self.inner.rules),
                Some(&self.source().graph),
            )
            .map_err(SessionError::Inference)?;
            let diagnostics: Vec<_> = outcome
                .diagnostics
                .into_iter()
                .map(|message| ExecutionDiagnostic { message })
                .collect();
            if options.engine.unsupported == UnsupportedPolicy::Error && !diagnostics.is_empty() {
                return Err(SessionError::StrictInference(diagnostics));
            }
            (Arc::new(outcome.graph), outcome.inferred, diagnostics)
        } else {
            (Arc::clone(&asserted), Vec::new(), Vec::new())
        };
        Ok(EvaluationSession {
            compiled: self.clone(),
            asserted,
            evaluated,
            separate,
            options,
            inferred,
            diagnostics,
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
            let shapes = &self.compiled.source().graph;
            let focus = context::focus_graph(
                &self.evaluated,
                shapes,
                self.separate,
                self.options.graph_mode,
            );
            let frozen = context::frozen(
                &self.evaluated,
                shapes,
                self.separate,
                self.options.graph_mode,
            );
            PreparedEvidenceValidator::from_compiled(
                focus,
                &self.compiled,
                frozen,
                true,
                self.options.engine.unsupported,
            )
        })
    }

    /// Borrow the prepared evidence view for existing shape-map projections.
    pub fn prepared_evidence(&self) -> &PreparedEvidenceValidator {
        self.prepared()
    }

    pub fn validate(&self, options: &FindingOptions) -> Result<ValidationOutcome, EvaluationError> {
        Ok(self.prepared().validate_findings(
            self.compiled.physical_plan(),
            &options.validation(self.options.engine),
        ))
    }

    pub fn report(
        &self,
        options: &FindingOptions,
    ) -> Result<crate::report::ValidationReport, EvaluationError> {
        Ok(self.prepared().report(
            self.compiled.source(),
            &options.validation(self.options.engine),
        ))
    }

    pub fn property_witnesses(
        &self,
        key_path: Option<&shifty_algebra::Path>,
        options: &FindingOptions,
    ) -> Result<Vec<crate::report::PropertyWitness>, EvaluationError> {
        Ok(self.prepared().property_witnesses(
            self.compiled.source(),
            key_path,
            &options.validation(self.options.engine),
        ))
    }

    pub fn evidence(&self, options: &EvidenceOptions) -> Result<EvidenceRun, EvaluationError> {
        let validation = options.findings.validation(self.options.engine);
        Ok(if options.include_progress {
            self.prepared().validate(&validation)
        } else {
            self.prepared().validate_canonical(&validation)
        })
    }

    pub fn conformance(
        &self,
        options: &ConformanceOptions,
    ) -> Result<ConformanceRun, EvaluationError> {
        Ok(self.prepared().validate_conformance(options))
    }

    pub fn find_failures(
        &self,
        options: &ConformanceOptions,
    ) -> Result<(ConformanceRun, Vec<SelectedPair>), EvaluationError> {
        let (run, mut pairs) = self.prepared().find_failures(options);
        for pair in &mut pairs {
            pair.snapshot = Some(Arc::clone(&self.snapshot));
        }
        Ok((run, pairs))
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

    pub fn data(&self) -> &Graph {
        &self.evaluated
    }

    /// Share the evaluated data graph without copying it.
    pub fn data_shared(&self) -> Arc<Graph> {
        Arc::clone(&self.evaluated)
    }

    /// Share the asserted data graph used by `with_delta`.
    pub fn asserted_shared(&self) -> Arc<Graph> {
        Arc::clone(&self.asserted)
    }

    pub fn inferred(&self) -> &[Triple] {
        &self.inferred
    }

    pub fn diagnostics(&self) -> &[ExecutionDiagnostic] {
        &self.diagnostics
    }

    pub fn gate(&self, delta: &GraphDelta) -> Result<RepairOutcome, SessionError> {
        let baseline = self
            .validate(&FindingOptions::default())
            .expect("valid session");
        let candidate = self.with_delta(delta)?;
        let patched = candidate
            .validate(&FindingOptions::default())
            .expect("valid session");
        Ok(crate::gate::diff(baseline.violations, patched.violations))
    }
}
