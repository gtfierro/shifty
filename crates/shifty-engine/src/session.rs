//! Evaluation over one immutable asserted-data snapshot.

use crate::compiled::CompiledShapes;
use crate::context;
use crate::evidence::{
    ConformanceOptions, ConformanceRun, PreparedEvidenceValidator, SelectedPair,
};
use crate::frozen::FrozenIndexedDataset;
use crate::gate::RepairOutcome;
use crate::sparql::SparqlDiagnostic;
use crate::validate::{
    EngineOptions, Reason, UnsupportedPolicy, ValidationGraphMode, ValidationOptions,
    ValidationOutcome, Violation,
};
use crate::witness::{EvidenceRun, StatementEvaluation};
use oxrdf::{BlankNode, Graph, NamedOrBlankNode, Term, Triple};
use shifty_algebra::Severity;
use shifty_parse::{DiagLevel, Diagnostic};
use shifty_repair::GraphDelta;
use std::cell::{OnceCell, RefCell};
use std::collections::{HashMap, HashSet};
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
    public_identity: OnceCell<PublicIdentity>,
    public_data: OnceCell<Arc<Graph>>,
    public_inferred: OnceCell<Vec<Triple>>,
    inference_ran: bool,
    diagnostics: Vec<ExecutionDiagnostic>,
    inference_dataset: RefCell<Option<FrozenIndexedDataset>>,
    prepared: OnceCell<PreparedEvidenceValidator>,
    snapshot: Arc<()>,
}

/// The two inputs may spell different blank nodes with the same label. The
/// evaluator uses an alias for the data node; public results keep the data
/// label and give the shapes node a fresh one. This map is built from aliases
/// captured while the data was encoded, without another graph scan.
struct PublicIdentity {
    labels: HashMap<BlankNode, BlankNode>,
    internal_labels: HashMap<BlankNode, BlankNode>,
    data_aliases: HashMap<BlankNode, BlankNode>,
    source_collisions: HashSet<BlankNode>,
}

#[derive(PartialEq, Eq, Hash)]
enum ScopedTermKey {
    Other(Term),
    DataBlank(BlankNode),
    ShapesBlank(BlankNode),
}

impl PublicIdentity {
    fn from_dataset(dataset: &FrozenIndexedDataset) -> Self {
        let data_aliases = dataset.data_blank_node_aliases();
        let source_collisions = data_aliases.values().cloned().collect();
        let labels = dataset.external_blank_node_map();
        let internal_labels = labels.iter().map(|(a, b)| (b.clone(), a.clone())).collect();
        Self {
            labels,
            internal_labels,
            data_aliases,
            source_collisions,
        }
    }

    fn term(&self, term: &Term) -> Term {
        match term {
            Term::BlankNode(node) => Term::BlankNode(
                self.labels
                    .get(node)
                    .cloned()
                    .unwrap_or_else(|| node.clone()),
            ),
            _ => term.clone(),
        }
    }

    fn internal_term(&self, term: &Term) -> Term {
        match term {
            Term::BlankNode(node) => Term::BlankNode(
                self.internal_labels
                    .get(node)
                    .cloned()
                    .unwrap_or_else(|| node.clone()),
            ),
            _ => term.clone(),
        }
    }

    fn triple(&self, triple: &Triple) -> Triple {
        let subject = match &triple.subject {
            NamedOrBlankNode::BlankNode(node) => NamedOrBlankNode::BlankNode(
                self.labels
                    .get(node)
                    .cloned()
                    .unwrap_or_else(|| node.clone()),
            ),
            named => named.clone(),
        };
        Triple::new(subject, triple.predicate.clone(), self.term(&triple.object))
    }

    fn key(&self, term: &Term) -> ScopedTermKey {
        match term {
            Term::BlankNode(node) if self.data_aliases.contains_key(node) => {
                ScopedTermKey::DataBlank(self.data_aliases[node].clone())
            }
            Term::BlankNode(node) if self.source_collisions.contains(node) => {
                ScopedTermKey::ShapesBlank(node.clone())
            }
            _ => ScopedTermKey::Other(term.clone()),
        }
    }

    fn diagnostic(&self, diagnostic: &mut SparqlDiagnostic) {
        for (_, term) in &mut diagnostic.bindings {
            *term = self.term(term);
        }
        for row in &mut diagnostic.results {
            for (_, term) in row {
                *term = self.term(term);
            }
        }
    }

    fn reason(&self, reason: &mut Reason) {
        reason.value = self.term(&reason.value);
        if let Some(diagnostic) = &mut reason.sparql_diagnostic {
            self.diagnostic(diagnostic);
        }
        for child in &mut reason.sub_reasons {
            self.reason(child);
        }
    }

    fn violation(&self, violation: &mut Violation) {
        violation.focus = self.term(&violation.focus);
        for reason in &mut violation.reasons {
            self.reason(reason);
        }
    }

    fn validation(&self, outcome: &mut ValidationOutcome) {
        for violation in &mut outcome.violations {
            self.violation(violation);
        }
    }

    fn report(&self, report: &mut crate::report::ValidationReport) {
        for result in &mut report.results {
            result.focus = self.term(&result.focus);
            result.path = result.path.as_ref().map(|term| self.term(term));
            result.value = result.value.as_ref().map(|term| self.term(term));
            result.source_shape = self.term(&result.source_shape);
            for message in &mut result.messages {
                *message = self.term(message);
            }
            if let Some(diagnostic) = &mut result.sparql_diagnostic {
                self.diagnostic(diagnostic);
            }
        }
    }

    fn remap_evidence<T>(&self, value: T) -> T
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        if self.labels.is_empty() {
            return value;
        }
        // Evidence is an opt-in, already-serializable tree. Apply the same
        // alias table to every RDF term in that tree only on the collision
        // path; the indexed graph itself is never scanned or copied here.
        let mut encoded = serde_json::to_value(value).expect("evidence serializes");
        fn rewrite(value: &mut serde_json::Value, labels: &HashMap<String, String>) {
            match value {
                serde_json::Value::Object(object) => {
                    if object.get("type").and_then(serde_json::Value::as_str) == Some("bnode")
                        && let Some(serde_json::Value::String(label)) = object.get_mut("value")
                        && let Some(mapped) = labels.get(label)
                    {
                        *label = mapped.clone();
                        return;
                    }
                    for child in object.values_mut() {
                        rewrite(child, labels);
                    }
                }
                serde_json::Value::Array(items) => {
                    for child in items {
                        rewrite(child, labels);
                    }
                }
                _ => {}
            }
        }
        let labels = self
            .labels
            .iter()
            .map(|(from, to)| (from.as_str().to_owned(), to.as_str().to_owned()))
            .collect();
        rewrite(&mut encoded, &labels);
        serde_json::from_value(encoded).expect("rewritten evidence deserializes")
    }
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
        let (evaluated, inferred, diagnostics, inference_dataset, public_identity) =
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
                let public_identity = Some(PublicIdentity::from_dataset(&run.dataset));
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
                    public_identity,
                )
            } else {
                (
                    Some(Arc::clone(&asserted)),
                    Vec::new(),
                    Vec::new(),
                    None,
                    None,
                )
            };
        let evaluated_cell = OnceCell::new();
        if let Some(graph) = evaluated {
            evaluated_cell.set(graph).expect("new evaluated graph cell");
        }
        let identity_cell = OnceCell::new();
        if let Some(identity) = public_identity {
            identity_cell.set(identity).ok();
        }
        Ok(EvaluationSession {
            compiled: self.clone(),
            asserted,
            evaluated: evaluated_cell,
            separate,
            options,
            inferred,
            public_identity: identity_cell,
            public_data: OnceCell::new(),
            public_inferred: OnceCell::new(),
            inference_ran: options.inference && !self.inner.rules.is_empty(),
            diagnostics,
            inference_dataset: RefCell::new(inference_dataset),
            prepared: OnceCell::new(),
            snapshot: Arc::new(()),
        })
    }
}

impl EvaluationSession {
    /// Resolve a public RDF term to the indexed identity used by evidence
    /// constraints. This is a constant-time lookup and leaves other terms alone.
    pub fn internal_term(&self, term: &Term) -> Term {
        self.identity().internal_term(term)
    }

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
            self.public_identity
                .get_or_init(|| PublicIdentity::from_dataset(&frozen));
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

    fn identity(&self) -> &PublicIdentity {
        if self.public_identity.get().is_none() {
            // Preparation constructs the indexed dataset and records its alias
            // map even when no rule inference ran.
            self.prepared();
        }
        self.public_identity
            .get()
            .expect("prepared session has blank-node identity")
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

    fn validate_internal(&self, options: &FindingOptions) -> ValidationOutcome {
        self.prepared().validate_findings(
            self.compiled.physical_plan(),
            &options.validation(self.options.engine),
        )
    }

    pub fn validate(&self, options: &FindingOptions) -> ValidationOutcome {
        let mut outcome = self.validate_internal(options);
        self.identity().validation(&mut outcome);
        outcome
    }

    pub fn report(&self, options: &FindingOptions) -> crate::report::ValidationReport {
        let mut report = self.prepared().report(
            self.compiled.source(),
            &options.validation(self.options.engine),
        );
        self.identity().report(&mut report);
        report
    }

    pub fn property_witnesses(
        &self,
        key_path: Option<&shifty_algebra::Path>,
        options: &FindingOptions,
    ) -> Vec<crate::report::PropertyWitness> {
        let mut witnesses = self.prepared().property_witnesses(
            self.compiled.source(),
            key_path,
            &options.validation(self.options.engine),
        );
        let identity = self.identity();
        for witness in &mut witnesses {
            witness.focus = identity.term(&witness.focus);
            witness.shape = identity.term(&witness.shape);
            witness.key = identity.term(&witness.key);
            for value in &mut witness.values {
                *value = identity.term(value);
            }
        }
        witnesses
    }

    pub fn evidence(&self, options: &EvidenceOptions) -> EvidenceRun {
        let validation = options.findings.validation(self.options.engine);
        self.evidence_with_validation(&validation, options.include_progress)
    }

    pub fn evidence_with_validation(
        &self,
        validation: &ValidationOptions,
        include_progress: bool,
    ) -> EvidenceRun {
        let run = if include_progress {
            self.prepared().validate(validation)
        } else {
            self.prepared().validate_canonical(validation)
        };
        self.identity().remap_evidence(run)
    }

    pub fn conformance(&self, options: &ConformanceOptions) -> ConformanceRun {
        self.prepared().validate_conformance(options)
    }

    pub fn find_failures(
        &self,
        options: &ConformanceOptions,
    ) -> (ConformanceRun, Vec<SelectedPair>) {
        let (run, mut pairs) = self.prepared().find_failures(options);
        let identity = self.identity();
        for pair in &mut pairs {
            if !identity.labels.is_empty() {
                pair.public_focus = Some(identity.term(pair.focus()));
            }
            pair.snapshot = Some(Arc::clone(&self.snapshot));
        }
        (run, pairs)
    }

    pub fn explain(
        &self,
        pair: &SelectedPair,
    ) -> Result<Vec<StatementEvaluation>, EvaluationError> {
        self.check_pair(pair)?;
        Ok(self
            .identity()
            .remap_evidence(self.prepared().explain(pair)))
    }

    pub fn explain_canonical(
        &self,
        pair: &SelectedPair,
    ) -> Result<Vec<StatementEvaluation>, EvaluationError> {
        self.check_pair(pair)?;
        Ok(self
            .identity()
            .remap_evidence(self.prepared().explain_canonical(pair)))
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
        self.public_data()
    }

    /// Share the evaluated data graph without copying it.
    pub fn data_shared(&self) -> Arc<Graph> {
        Arc::clone(self.public_data())
    }

    /// A source graph with only colliding blank nodes relabeled for combining
    /// with the public data graph. No copy is made when scopes do not collide.
    pub fn public_shapes_graph_if_relabelled(&self) -> Option<Graph> {
        let identity = self.identity();
        if identity.labels.is_empty() {
            return None;
        }
        let mut graph = Graph::new();
        for triple in self.compiled.source().graph.iter() {
            graph.insert(&identity.triple(&triple.into_owned()));
        }
        Some(graph)
    }

    /// Relabel shape constants only when a split graph has a blank-node
    /// collision; otherwise preserve the caller's shared schema allocation.
    pub fn public_schema(
        &self,
        schema: Arc<shifty_algebra::Schema>,
    ) -> Arc<shifty_algebra::Schema> {
        if self.identity().labels.is_empty() {
            schema
        } else {
            Arc::new(self.identity().remap_evidence(schema.as_ref().clone()))
        }
    }

    fn public_data(&self) -> &Arc<Graph> {
        if !self.inference_ran || self.identity().labels.is_empty() {
            return self.evaluated_graph();
        }
        self.public_data.get_or_init(|| {
            let identity = self.identity();
            if let Some(dataset) = self.inference_dataset.borrow().as_ref() {
                return Arc::new(dataset.data_graph_projection_mapped(&identity.labels));
            }
            Arc::new(
                self.prepared()
                    .data_graph_projection_mapped(&identity.labels),
            )
        })
    }

    /// Share the asserted data graph used by `with_delta`.
    pub fn asserted_shared(&self) -> Arc<Graph> {
        Arc::clone(&self.asserted)
    }

    pub fn inferred(&self) -> &[Triple] {
        if self.identity().labels.is_empty() {
            return &self.inferred;
        }
        self.public_inferred.get_or_init(|| {
            self.inferred
                .iter()
                .map(|triple| self.identity().triple(triple))
                .collect()
        })
    }

    /// Count inferred triples without materializing their public RDF terms.
    pub fn inferred_count(&self) -> usize {
        self.inferred.len()
    }

    /// Inferred triples with input data blank-node labels restored and shapes
    /// nodes kept distinct, for adding the delta to a caller-owned data graph.
    pub fn inferred_for_write_back(&self) -> Vec<Triple> {
        self.inferred().to_vec()
    }

    pub fn diagnostics(&self) -> &[ExecutionDiagnostic] {
        &self.diagnostics
    }

    pub fn gate(&self, delta: &GraphDelta) -> Result<RepairOutcome, SessionError> {
        let baseline = self.validate_internal(&FindingOptions::default());
        let additions: HashSet<_> = delta.add.iter().collect();
        let changed = additions
            .iter()
            .any(|triple| !self.asserted.contains(*triple))
            || delta
                .delete
                .iter()
                .any(|triple| !additions.contains(triple) && self.asserted.contains(triple));
        if !changed {
            let mut remaining = baseline.violations;
            for violation in &mut remaining {
                self.identity().violation(violation);
            }
            return Ok(RepairOutcome {
                remaining,
                ..RepairOutcome::default()
            });
        }
        let candidate = self.with_delta(delta)?;
        let patched = candidate.validate_internal(&FindingOptions::default());
        let baseline_keys: HashSet<_> = baseline
            .violations
            .iter()
            .map(|violation| (self.identity().key(&violation.focus), violation.statement))
            .collect();
        let patched_keys: HashSet<_> = patched
            .violations
            .iter()
            .map(|violation| {
                (
                    candidate.identity().key(&violation.focus),
                    violation.statement,
                )
            })
            .collect();
        let mut outcome = RepairOutcome::default();
        for mut violation in baseline.violations {
            if !patched_keys.contains(&(self.identity().key(&violation.focus), violation.statement))
            {
                self.identity().violation(&mut violation);
                outcome.fixed.push(violation);
            }
        }
        for mut violation in patched.violations {
            let old = baseline_keys.contains(&(
                candidate.identity().key(&violation.focus),
                violation.statement,
            ));
            candidate.identity().violation(&mut violation);
            if old {
                outcome.remaining.push(violation);
            } else {
                outcome.introduced.push(violation);
            }
        }
        Ok(outcome)
    }
}
