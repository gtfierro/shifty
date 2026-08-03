//! Unified evidence-carrying validation.
//!
//! Each selected `(authored statement, focus)` pair produces exactly one
//! [`Evidence`](crate::Evidence) polarity. The logical algebra evaluator remains
//! the oracle; this driver only owns preparation, target enumeration, and
//! provenance fan-out.

use crate::frozen::FrozenIndexedDataset;
use crate::sparql::SparqlExecutor;
use crate::validate::{
    NonStratifiable, ShapeEvaluator, ValidationGraphMode, ValidationOptions,
    entry_shape_name_selected, focus_nodes_with_evaluator, graph_union,
    prefetch_sparql_constraints, uses_shapes_graph,
};
use crate::witness::{
    ChildEvaluation, ConstraintCatalog, ConstraintRecord, EvaluationProgress, EvaluationStatus,
    Evidence, EvidenceRun, EvidenceSummary, FocusEvaluation, StatementEvaluation, Witness,
    materialize_evidence,
};
use oxrdf::Graph;
use shifty_algebra::{ConstraintKind, Schema, Severity, Shape, ShapeArena, ShapeId};
use shifty_opt::{analyze, normalize_with_mapping};
use std::collections::HashSet;

/// A prepared, immutable evidence-validation snapshot.
///
/// Parsing and inference happen before construction. Normalization,
/// stratification, dataset indexing, and SPARQL-executor construction happen
/// once here; each [`validate`](Self::validate) call creates one reusable
/// [`ShapeEvaluator`] over those retained resources.
pub struct PreparedEvidenceValidator {
    data: Graph,
    raw_schema: Schema,
    schema: Schema,
    raw_by_normalized: Vec<Vec<usize>>,
    shape_map: Vec<Option<ShapeId>>,
    sparql: SparqlExecutor,
}

/// Conformance-only totals from one prepared run.
///
/// Counts are over normalized `(statement, focus)` pairs — the pairs an
/// [`EvidenceRun`] materializes evidence for, before authored statements that
/// normalize together fan the same evidence back out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConformanceRun {
    pub conforms: bool,
    pub selected_pairs: usize,
    pub passed: usize,
    pub failed: usize,
}

impl PreparedEvidenceValidator {
    /// Prepare embedded data/shapes validation over one graph.
    pub fn new(data: &Graph, schema: &Schema) -> Result<Self, NonStratifiable> {
        let uses_shapes = uses_shapes_graph(&schema.arena);
        let frozen = if uses_shapes {
            FrozenIndexedDataset::from_graphs(data, data)
        } else {
            FrozenIndexedDataset::from_graph(data)
        };
        Self::from_frozen(data.clone(), schema, frozen, uses_shapes)
    }

    /// Prepare separate focus and evaluation graphs.
    pub fn with_context(
        data: &Graph,
        context: &Graph,
        schema: &Schema,
    ) -> Result<Self, NonStratifiable> {
        let uses_shapes = uses_shapes_graph(&schema.arena);
        let frozen = if uses_shapes {
            FrozenIndexedDataset::from_graphs(context, context)
        } else {
            FrozenIndexedDataset::from_graph(context)
        };
        Self::from_frozen(data.clone(), schema, frozen, uses_shapes)
    }

    /// Prepare split data/shapes validation in the requested graph mode.
    pub fn with_graphs(
        data: &Graph,
        shapes: &Graph,
        schema: &Schema,
        mode: ValidationGraphMode,
    ) -> Result<Self, NonStratifiable> {
        let uses_shapes = uses_shapes_graph(&schema.arena);
        match mode {
            ValidationGraphMode::Data => {
                let frozen = if uses_shapes {
                    FrozenIndexedDataset::from_graphs(data, shapes)
                } else {
                    FrozenIndexedDataset::from_graph(data)
                };
                Self::from_frozen(data.clone(), schema, frozen, uses_shapes)
            }
            ValidationGraphMode::Union => {
                let frozen = if uses_shapes {
                    FrozenIndexedDataset::from_graph_union_with_shapes(data, shapes)
                } else {
                    FrozenIndexedDataset::from_graph_union(data, shapes)
                };
                Self::from_frozen(data.clone(), schema, frozen, uses_shapes)
            }
            ValidationGraphMode::UnionAll => {
                let union = graph_union(data, shapes);
                let frozen = if uses_shapes {
                    FrozenIndexedDataset::from_graphs(&union, &union)
                } else {
                    FrozenIndexedDataset::from_graph(&union)
                };
                Self::from_frozen(union, schema, frozen, uses_shapes)
            }
        }
    }

    fn from_frozen(
        data: Graph,
        raw_schema: &Schema,
        frozen: FrozenIndexedDataset,
        has_shapes_graph: bool,
    ) -> Result<Self, NonStratifiable> {
        let normalized = normalize_with_mapping(raw_schema);
        let stratification = analyze(&normalized.schema.arena);
        if !stratification.stratifiable {
            let components = stratification
                .strata
                .iter()
                .filter(|stratum| !stratum.stratifiable)
                .map(|stratum| stratum.shapes.clone())
                .collect();
            return Err(NonStratifiable { components });
        }

        let mut raw_by_normalized = vec![Vec::new(); normalized.schema.statements.len()];
        for (raw, normalized_id) in normalized.statement_map.iter().copied().enumerate() {
            raw_by_normalized[normalized_id].push(raw);
        }

        Ok(Self {
            data,
            raw_schema: raw_schema.clone(),
            schema: normalized.schema,
            raw_by_normalized,
            shape_map: normalized.shape_map,
            sparql: SparqlExecutor::from_frozen(frozen, has_shapes_graph),
        })
    }

    /// Validate the prepared snapshot for conformance only.
    ///
    /// Preparation, target selection, and the evaluator are exactly those of
    /// [`validate`](Self::validate); each selected `(statement, focus)` pair is
    /// instead decided by one short-circuiting satisfaction test. No evidence is
    /// materialized and no authored-statement progress is computed, so the
    /// difference against [`validate`](Self::validate) on the same snapshot is
    /// the cost of evidence tracing alone.
    ///
    /// `options.minimum_severity` is not honored: without failure evidence there
    /// is no per-constraint severity to weigh, so every failing pair makes
    /// `conforms` false — the default `Severity::Info` threshold. Only
    /// `entry_shape_names` applies; `sort_results` is irrelevant to counts.
    pub fn validate_conformance(&self, options: &ValidationOptions) -> ConformanceRun {
        let backend = self
            .sparql
            .frozen()
            .expect("prepared evidence validator always owns a frozen dataset");
        let mut evaluator = ShapeEvaluator::new(backend, &self.schema.arena, &self.sparql);
        let mut run = ConformanceRun {
            conforms: true,
            selected_pairs: 0,
            passed: 0,
            failed: 0,
        };

        for statement in &self.schema.statements {
            if !entry_shape_name_selected(
                &options.entry_shape_names,
                self.schema.names.get(&statement.shape).map(String::as_str),
            ) {
                continue;
            }

            let foci = focus_nodes_with_evaluator(&self.data, &statement.selector, &mut evaluator);
            prefetch_sparql_constraints(&self.schema.arena, statement.shape, &foci, &self.sparql);

            for focus in foci {
                run.selected_pairs += 1;
                if evaluator.holds(&focus, statement.shape) {
                    run.passed += 1;
                } else {
                    run.failed += 1;
                    run.conforms = false;
                }
            }
        }

        run
    }

    /// Validate the prepared snapshot and return its complete coverage horizon.
    pub fn validate(&self, options: &ValidationOptions) -> EvidenceRun {
        let backend = self
            .sparql
            .frozen()
            .expect("prepared evidence validator always owns a frozen dataset");
        let mut evaluator = ShapeEvaluator::new(backend, &self.schema.arena, &self.sparql);
        let mut statements: Vec<Option<StatementEvaluation>> =
            vec![None; self.raw_schema.statements.len()];
        let mut conforms = true;
        // Attribution is opt-in: without profiling this loop pays nothing, so
        // the evidence-overhead benchmark measures materialization, not timers.
        let profiling = crate::profile::is_enabled();

        for (statement_id, statement) in self.schema.statements.iter().enumerate() {
            if !entry_shape_name_selected(
                &options.entry_shape_names,
                self.schema.names.get(&statement.shape).map(String::as_str),
            ) {
                continue;
            }

            let label = profiling.then(|| {
                self.schema
                    .names
                    .get(&statement.shape)
                    .cloned()
                    .unwrap_or_else(|| format!("@{}", statement.shape.0))
            });
            let selection_start = profiling.then(web_time::Instant::now);
            let foci = focus_nodes_with_evaluator(&self.data, &statement.selector, &mut evaluator);
            prefetch_sparql_constraints(&self.schema.arena, statement.shape, &foci, &self.sparql);
            if let (Some(start), Some(label)) = (selection_start, label.as_deref()) {
                crate::profile::record_shape_work(
                    &format!("select:{label}"),
                    start.elapsed().as_micros() as u64,
                    0,
                );
            }

            for focus in foci {
                let pair_start = profiling
                    .then(|| (web_time::Instant::now(), crate::profile::evidence_visits()));
                let evidence = materialize_evidence(&mut evaluator, &focus, statement.shape);
                if let (Some((start, visits_before)), Some(label)) =
                    (pair_start, label.as_deref())
                {
                    crate::profile::record_shape_work(
                        label,
                        start.elapsed().as_micros() as u64,
                        crate::profile::evidence_visits().saturating_sub(visits_before),
                    );
                }
                if let Evidence::Failure(failure) = &evidence
                    && failure_meets_threshold(
                        &self.schema.arena,
                        statement.shape,
                        failure,
                        &options.minimum_severity,
                    )
                {
                    conforms = false;
                }

                for &raw_statement in &self.raw_by_normalized[statement_id] {
                    let raw = &self.raw_schema.statements[raw_statement];
                    let progress = source_progress(
                        &mut evaluator,
                        &self.raw_schema.arena,
                        raw.shape,
                        &self.shape_map,
                        &focus,
                    );
                    statements[raw_statement]
                        .get_or_insert_with(|| {
                            statement_evaluation(
                                &self.raw_schema,
                                &self.schema,
                                raw_statement,
                                statement_id,
                            )
                        })
                        .selected_foci
                        .push(FocusEvaluation {
                            focus: focus.clone(),
                            evidence: evidence.clone(),
                            progress,
                        });
                }
            }

            // Source statements remain visible even when target selection is empty.
            for &raw_statement in &self.raw_by_normalized[statement_id] {
                statements[raw_statement].get_or_insert_with(|| {
                    statement_evaluation(
                        &self.raw_schema,
                        &self.schema,
                        raw_statement,
                        statement_id,
                    )
                });
            }
        }

        let mut statements: Vec<StatementEvaluation> = statements.into_iter().flatten().collect();
        if options.sort_results {
            statements.sort_by_key(|statement| statement.source_statement_id);
            for statement in &mut statements {
                statement
                    .selected_foci
                    .sort_by(|left, right| left.focus.to_string().cmp(&right.focus.to_string()));
            }
        }

        // TODO(physical evidence): execute planned FocusSource/path operators
        // here while carrying normalized logical ShapeIds through physical plan
        // nodes, so target/path work is reused without losing evidence identity.
        EvidenceRun {
            conforms,
            constraints: ConstraintCatalog {
                source: constraint_catalog(&self.raw_schema.arena),
                normalized: constraint_catalog(&self.schema.arena),
            },
            statements,
        }
    }

    /// The normalized schema whose ids appear in returned evidence.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn constraint_catalog(arena: &ShapeArena) -> Vec<ConstraintRecord> {
    (0..arena.len())
        .map(|index| {
            let id = ShapeId(index as u32);
            ConstraintRecord {
                id,
                constraint_kind: ConstraintKind::of(arena, id),
                constraint: arena.get(id).clone(),
            }
        })
        .collect()
}

fn statement_evaluation(
    raw_schema: &Schema,
    normalized_schema: &Schema,
    source_statement_id: usize,
    normalized_statement_id: usize,
) -> StatementEvaluation {
    let source = &raw_schema.statements[source_statement_id];
    let normalized = &normalized_schema.statements[normalized_statement_id];
    StatementEvaluation {
        source_statement_id,
        normalized_statement_id: Some(normalized_statement_id),
        source_constraint_id: source.shape,
        normalized_constraint_id: Some(normalized.shape),
        constraint_kind: ConstraintKind::of(&normalized_schema.arena, normalized.shape),
        constraint: normalized_schema.arena.get(normalized.shape).clone(),
        selector: source.selector.clone(),
        selected_foci: Vec::new(),
    }
}

fn source_children(arena: &ShapeArena, mut id: ShapeId) -> Vec<ShapeId> {
    while let Shape::Annotated { shape, .. } = arena.get(id) {
        id = *shape;
    }
    match arena.get(id) {
        Shape::And(children) | Shape::Or(children) => children.clone(),
        Shape::Not(child) => vec![*child],
        _ => Vec::new(),
    }
}

fn source_progress(
    evaluator: &mut ShapeEvaluator<'_>,
    raw_arena: &ShapeArena,
    raw_shape: ShapeId,
    shape_map: &[Option<ShapeId>],
    focus: &oxrdf::Term,
) -> Option<EvaluationProgress> {
    let children = source_children(raw_arena, raw_shape);
    if children.is_empty() {
        return None;
    }
    let evaluated_children = children
        .into_iter()
        .map(|source_constraint_ref| {
            let normalized_constraint_ref = shape_map
                .get(source_constraint_ref.0 as usize)
                .copied()
                .flatten();
            let status = normalized_constraint_ref
                .map(|id| {
                    if evaluator.holds(focus, id) {
                        EvaluationStatus::Pass
                    } else {
                        EvaluationStatus::Fail
                    }
                })
                .unwrap_or(EvaluationStatus::Pass);
            let summary_id = normalized_constraint_ref.unwrap_or(source_constraint_ref);
            let summary_kind = normalized_constraint_ref.map_or_else(
                || ConstraintKind::of(raw_arena, source_constraint_ref),
                |id| ConstraintKind::of(evaluator.arena(), id),
            );
            ChildEvaluation {
                source_constraint_ref,
                normalized_constraint_ref,
                status,
                evidence_summary: EvidenceSummary {
                    constraint_id: summary_id,
                    constraint_kind: summary_kind,
                    status,
                },
            }
        })
        .collect();
    Some(EvaluationProgress { evaluated_children })
}

fn witness_shape_ids(witness: &Witness, out: &mut HashSet<ShapeId>) {
    let shape = match witness {
        Witness::Atom { shape, .. }
        | Witness::Relational { shape, .. }
        | Witness::Closed { shape, .. }
        | Witness::Not { shape, .. }
        | Witness::All { shape, .. }
        | Witness::Any { shape, .. }
        | Witness::CountLow { shape, .. }
        | Witness::CountHigh { shape, .. }
        | Witness::Opaque { shape, .. } => *shape,
    };
    out.insert(shape);
    match witness {
        Witness::All { failed, .. } => failed
            .iter()
            .for_each(|child| witness_shape_ids(child, out)),
        Witness::Any { branches, .. } => branches
            .iter()
            .for_each(|child| witness_shape_ids(child, out)),
        Witness::CountHigh { per_value, .. } => per_value
            .iter()
            .for_each(|(_, child)| witness_shape_ids(child, out)),
        _ => {}
    }
}

fn failure_meets_threshold(
    arena: &ShapeArena,
    root: ShapeId,
    witness: &Witness,
    minimum: &Severity,
) -> bool {
    let mut failed = HashSet::new();
    witness_shape_ids(witness, &mut failed);
    let mut active = HashSet::new();
    failure_shape_meets(
        arena,
        root,
        &Severity::Violation,
        minimum,
        &failed,
        &mut active,
    )
}

fn failure_shape_meets(
    arena: &ShapeArena,
    id: ShapeId,
    inherited: &Severity,
    minimum: &Severity,
    failed: &HashSet<ShapeId>,
    active: &mut HashSet<ShapeId>,
) -> bool {
    if !active.insert(id) {
        return false;
    }
    let result = match arena.get(id) {
        Shape::Annotated {
            severity, shape, ..
        } => failure_shape_meets(arena, *shape, severity, minimum, failed, active),
        Shape::And(children) => children
            .iter()
            .any(|child| failure_shape_meets(arena, *child, inherited, minimum, failed, active)),
        Shape::Or(children) => {
            (failed.contains(&id) && inherited.meets(minimum))
                || children.iter().any(|child| {
                    failure_shape_meets(arena, *child, inherited, minimum, failed, active)
                })
        }
        Shape::Count { qualifier, .. } => {
            (failed.contains(&id) && inherited.meets(minimum))
                || failure_shape_meets(arena, *qualifier, inherited, minimum, failed, active)
        }
        Shape::Not(_) => failed.contains(&id) && inherited.meets(minimum),
        _ => failed.contains(&id) && inherited.meets(minimum),
    };
    active.remove(&id);
    result
}

pub fn validate_with_evidence(
    data: &Graph,
    schema: &Schema,
) -> Result<EvidenceRun, NonStratifiable> {
    validate_with_evidence_and_options(data, schema, &ValidationOptions::default())
}

pub fn validate_with_evidence_and_options(
    data: &Graph,
    schema: &Schema,
    options: &ValidationOptions,
) -> Result<EvidenceRun, NonStratifiable> {
    Ok(PreparedEvidenceValidator::new(data, schema)?.validate(options))
}

pub fn validate_with_context_and_evidence(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
) -> Result<EvidenceRun, NonStratifiable> {
    validate_with_context_and_evidence_and_options(
        data,
        context,
        schema,
        &ValidationOptions::default(),
    )
}

pub fn validate_with_context_and_evidence_and_options(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    options: &ValidationOptions,
) -> Result<EvidenceRun, NonStratifiable> {
    Ok(PreparedEvidenceValidator::with_context(data, context, schema)?.validate(options))
}

pub fn validate_graphs_with_evidence(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
) -> Result<EvidenceRun, NonStratifiable> {
    validate_graphs_with_evidence_and_mode_and_options(
        data,
        shapes,
        schema,
        ValidationGraphMode::default(),
        &ValidationOptions::default(),
    )
}

pub fn validate_graphs_with_evidence_and_mode(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
    mode: ValidationGraphMode,
) -> Result<EvidenceRun, NonStratifiable> {
    validate_graphs_with_evidence_and_mode_and_options(
        data,
        shapes,
        schema,
        mode,
        &ValidationOptions::default(),
    )
}

pub fn validate_graphs_with_evidence_and_mode_and_options(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
    mode: ValidationGraphMode,
    options: &ValidationOptions,
) -> Result<EvidenceRun, NonStratifiable> {
    Ok(PreparedEvidenceValidator::with_graphs(data, shapes, schema, mode)?.validate(options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PathSupport, SatTrace};
    use oxrdf::Triple;
    use shifty_parse::{load_turtle, parse_turtle};

    const PREFIXES: &str = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://ex/> .
    "#;

    fn run(ttl: &str) -> (Graph, Schema, EvidenceRun) {
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let outcome = validate_with_evidence(&loaded.graph, &parsed.schema).unwrap();
        (loaded.graph, parsed.schema, outcome)
    }

    fn foci(outcome: &EvidenceRun) -> Vec<&FocusEvaluation> {
        outcome
            .statements
            .iter()
            .flat_map(|statement| &statement.selected_foci)
            .collect()
    }

    fn traces(trace: &SatTrace, pred: &impl Fn(&SatTrace) -> bool) -> bool {
        if pred(trace) {
            return true;
        }
        match trace {
            SatTrace::AllHeld { children, .. } => children.iter().any(|t| traces(t, pred)),
            SatTrace::AnyHeld { satisfied, .. } => satisfied.iter().any(|t| traces(t, pred)),
            SatTrace::CountHeld { matches, .. } => matches.iter().any(|(_, _, t)| traces(t, pred)),
            SatTrace::ForAllHeld { values, .. } => values.iter().any(|(_, _, t)| traces(t, pred)),
            _ => false,
        }
    }

    fn support_edges(support: &PathSupport, out: &mut Vec<Triple>) {
        match support {
            PathSupport::Edge(edge) => out.push(edge.clone()),
            PathSupport::Chain(parts) | PathSupport::Alt(parts) => {
                parts.iter().for_each(|part| support_edges(part, out));
            }
            PathSupport::Empty => {}
        }
    }

    #[test]
    fn conformance_only_run_agrees_with_evidence_run() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetClass ex:T ;
               sh:property [ sh:path ex:p ; sh:minCount 1 ] .
             ex:T2 a sh:NodeShape ; sh:targetClass ex:T ;
               sh:property [ sh:path ex:p ; sh:minCount 1 ] .
             ex:good a ex:T ; ex:p ex:value .
             ex:bad a ex:T .
             ex:unselected ex:p ex:value ."
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let prepared =
            PreparedEvidenceValidator::new(&loaded.graph, &parsed.schema).unwrap();
        let options = ValidationOptions::default();
        let conformance = prepared.validate_conformance(&options);
        let evidence = prepared.validate(&options);

        assert_eq!(conformance.conforms, evidence.conforms);
        assert_eq!(conformance.selected_pairs, 2);
        assert_eq!(conformance.passed, 1);
        assert_eq!(conformance.failed, 1);
        // Two authored statements normalize together, so the evidence run fans
        // the same two evaluated pairs back out over both of them.
        assert_eq!(foci(&evidence).len(), 4);
    }

    #[test]
    fn selected_pairs_are_partitioned_and_unselected_pairs_are_absent() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetClass ex:T ;
               sh:property [ sh:path ex:p ; sh:minCount 1 ] .
             ex:good a ex:T ; ex:p ex:value .
             ex:bad a ex:T .
             ex:unselected ex:p ex:value ."
        );
        let (_, _, outcome) = run(&ttl);
        let results = foci(&outcome);
        assert!(!outcome.conforms);
        assert_eq!(results.len(), 2);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r.evidence, Evidence::Satisfaction(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r.evidence, Evidence::Failure(_)))
                .count(),
            1
        );
        assert!(
            results
                .iter()
                .all(|r| !r.focus.to_string().contains("unselected"))
        );
    }

    #[test]
    fn raw_statements_survive_normalized_deduplication() {
        let ttl = format!(
            "{PREFIXES}
             ex:S1 a sh:NodeShape ; sh:targetNode ex:x ; sh:nodeKind sh:IRI .
             ex:S2 a sh:NodeShape ; sh:targetNode ex:x ; sh:nodeKind sh:IRI ."
        );
        let (_, raw, outcome) = run(&ttl);
        assert_eq!(raw.statements.len(), 2);
        assert_eq!(outcome.statements.len(), 2);
        assert_eq!(outcome.statements[0].source_statement_id, 0);
        assert_eq!(outcome.statements[1].source_statement_id, 1);
        assert_eq!(
            outcome.statements[0].normalized_statement_id,
            outcome.statements[1].normalized_statement_id
        );
        assert_eq!(
            outcome.statements[0].normalized_constraint_id,
            outcome.statements[1].normalized_constraint_id
        );
    }

    #[test]
    fn selector_matching_nothing_has_empty_statement_row() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetClass ex:Missing ; sh:nodeKind sh:IRI .
             ex:x a ex:Other ."
        );
        let (_, _, outcome) = run(&ttl);
        assert!(outcome.conforms);
        assert_eq!(outcome.statements.len(), 1);
        assert!(outcome.statements[0].selected_foci.is_empty());
    }

    #[test]
    fn negation_crosses_to_the_opposite_evidence_polarity() {
        let passing = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:not [ sh:class ex:C ] .
             ex:x a ex:Other ."
        );
        let (_, _, pass) = run(&passing);
        let pass_foci = foci(&pass);
        let Evidence::Satisfaction(trace) = &pass_foci[0].evidence else {
            panic!("expected satisfaction")
        };
        assert!(traces(trace, &|t| matches!(t, SatTrace::NotHeld { .. })));

        let failing = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:not [ sh:class ex:C ] .
             ex:x a ex:C ."
        );
        let (_, _, fail) = run(&failing);
        let fail_foci = foci(&fail);
        assert!(matches!(
            fail_foci[0].evidence,
            Evidence::Failure(Witness::Not { .. }) | Evidence::Failure(Witness::All { .. })
        ));
        fn contains_not(w: &Witness) -> bool {
            match w {
                Witness::Not { .. } => true,
                Witness::All { failed, .. } => failed.iter().any(contains_not),
                Witness::Any { branches, .. } => branches.iter().any(contains_not),
                _ => false,
            }
        }
        let Evidence::Failure(witness) = &fail_foci[0].evidence else {
            unreachable!()
        };
        assert!(contains_not(witness));
    }

    #[test]
    fn sequence_path_satisfaction_carries_only_real_graph_edges() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:property [ sh:path (ex:p ex:q) ; sh:minCount 1 ] .
             ex:x ex:p ex:y . ex:y ex:q ex:z ."
        );
        let (graph, _, outcome) = run(&ttl);
        let results = foci(&outcome);
        let Evidence::Satisfaction(trace) = &results[0].evidence else {
            panic!("expected satisfaction")
        };
        let mut edges = Vec::new();
        fn gather(trace: &SatTrace, edges: &mut Vec<Triple>) {
            match trace {
                SatTrace::CountHeld { matches, .. } => {
                    for (_, support, child) in matches {
                        support_edges(support, edges);
                        gather(child, edges);
                    }
                }
                SatTrace::ForAllHeld { values, .. } => {
                    for (_, support, child) in values {
                        support_edges(support, edges);
                        gather(child, edges);
                    }
                }
                SatTrace::AllHeld { children, .. } => {
                    children.iter().for_each(|child| gather(child, edges))
                }
                SatTrace::AnyHeld { satisfied, .. } => {
                    satisfied.iter().for_each(|child| gather(child, edges))
                }
                _ => {}
            }
        }
        gather(trace, &mut edges);
        assert_eq!(edges.len(), 2);
        assert!(edges.iter().all(|edge| graph.contains(edge.as_ref())));
    }

    #[test]
    fn count_evidence_retains_matches_bounds_qualifiers_and_support() {
        let low_ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:property [ sh:path ex:p ; sh:minCount 2 ] ;
               sh:property [ sh:path ex:p ; sh:class ex:C ] .
             ex:x ex:p ex:one . ex:one a ex:C ."
        );
        let (_, _, low) = run(&low_ttl);
        let low_foci = foci(&low);
        let Evidence::Failure(low_witness) = &low_foci[0].evidence else {
            panic!("expected low-count failure")
        };
        fn find_low(witness: &Witness) -> Option<(u64, u64, ShapeId, &[ShapeId])> {
            match witness {
                Witness::CountLow {
                    have,
                    min,
                    qualifier,
                    sibling_qualifiers,
                    ..
                } => Some((*have, *min, *qualifier, sibling_qualifiers)),
                Witness::All { failed, .. } => failed.iter().find_map(find_low),
                Witness::Any { branches, .. } => branches.iter().find_map(find_low),
                _ => None,
            }
        }
        let (have, min, qualifier, _siblings) = find_low(low_witness).expect("CountLow");
        assert_eq!((have, min), (1, 2));
        assert_ne!(qualifier, ShapeId(u32::MAX));

        let high_ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:property [ sh:path ex:p ; sh:maxCount 1 ] .
             ex:x ex:p ex:one, ex:two ."
        );
        let (graph, _, high) = run(&high_ttl);
        let high_foci = foci(&high);
        let Evidence::Failure(high_witness) = &high_foci[0].evidence else {
            panic!("expected high-count failure")
        };
        fn find_high(witness: &Witness) -> Option<(&[(oxrdf::Term, PathSupport)], u64)> {
            match witness {
                Witness::CountHigh { matched, max, .. } => Some((matched, *max)),
                Witness::All { failed, .. } => failed.iter().find_map(find_high),
                Witness::Any { branches, .. } => branches.iter().find_map(find_high),
                _ => None,
            }
        }
        let (matched, max) = find_high(high_witness).expect("CountHigh");
        assert_eq!(max, 1);
        assert_eq!(matched.len(), 2);
        let excess = high_foci[0]
            .evidence
            .walk()
            .into_iter()
            .find_map(|node| match node {
                crate::EvidenceNodeRef::Failure(Witness::CountHigh { excess_values, .. }) => {
                    Some(excess_values)
                }
                _ => None,
            })
            .expect("CountHigh excess values");
        assert_eq!(excess.len(), 1);
        let mut edges = Vec::new();
        matched
            .iter()
            .for_each(|(_, support)| support_edges(support, &mut edges));
        assert_eq!(edges.len(), 2);
        assert!(edges.iter().all(|edge| graph.contains(edge.as_ref())));
    }

    #[test]
    fn failed_conjunction_progress_retains_successful_siblings() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:nodeKind sh:IRI ;
               sh:property [ sh:path ex:p ; sh:minCount 1 ] ."
        );
        let (_, _, outcome) = run(&ttl);
        let results = foci(&outcome);
        let focus = results[0];
        assert_eq!(focus.status(), EvaluationStatus::Fail);
        let progress = focus.progress.as_ref().expect("conjunction progress");
        assert_eq!(progress.evaluated_children.len(), 2);
        assert_eq!(
            progress
                .evaluated_children
                .iter()
                .map(|child| child.status)
                .collect::<Vec<_>>(),
            vec![EvaluationStatus::Pass, EvaluationStatus::Fail]
        );
        let Evidence::Failure(Witness::All { failed, .. }) = &focus.evidence else {
            panic!("expected compact conjunction failure")
        };
        assert_eq!(failed.len(), 1);
    }

    #[test]
    fn qualified_count_low_partitions_matches_and_rejected_candidates() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:property [
                 sh:path ex:p ;
                 sh:qualifiedValueShape [ sh:class ex:C ] ;
                 sh:qualifiedMinCount 2
               ] .
             ex:x ex:p ex:good, ex:near . ex:good a ex:C ."
        );
        let (graph, _, outcome) = run(&ttl);
        let results = foci(&outcome);
        let evidence = &results[0].evidence;
        let low = evidence.walk().into_iter().find_map(|node| match node {
            crate::EvidenceNodeRef::Failure(value @ Witness::CountLow { .. }) => Some(value),
            _ => None,
        });
        let Some(Witness::CountLow {
            qualifying_matches,
            rejected_candidates,
            have,
            min,
            ..
        }) = low
        else {
            panic!("expected CountLow")
        };
        assert_eq!((*have, *min), (1, 2));
        assert_eq!(qualifying_matches.len(), 1);
        assert_eq!(rejected_candidates.len(), 1);
        assert_eq!(evidence.missing_obligations()[0].missing, 1);
        assert!(
            evidence
                .supporting_triples()
                .iter()
                .all(|triple| graph.contains(triple.as_ref()))
        );
        assert_eq!(
            evidence.to_json().unwrap(),
            evidence.to_json().unwrap(),
            "serialization must be deterministic"
        );
    }

    #[test]
    fn positive_recursion_is_coinductive_and_negative_recursion_is_rejected() {
        let positive = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:property [ sh:path ex:p ; sh:node ex:S ] .
             ex:x ex:p ex:x ."
        );
        let (_, _, outcome) = run(&positive);
        let results = foci(&outcome);
        let Evidence::Satisfaction(trace) = &results[0].evidence else {
            panic!("expected coinductive satisfaction")
        };
        assert!(
            traces(trace, &|t| matches!(t, SatTrace::Coinductive { .. })),
            "trace did not retain recursion: {trace:?}"
        );
        let positive_loaded = load_turtle(positive.as_bytes(), None).unwrap();
        let positive_parsed = parse_turtle(positive.as_bytes(), None).unwrap();
        let ordinary = crate::validate(
            &positive_loaded.graph,
            &shifty_opt::normalize(&positive_parsed.schema),
        )
        .unwrap();
        assert_eq!(outcome.conforms, ordinary.conforms);

        let negative = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:not [ sh:path ex:p ; sh:qualifiedValueShape ex:S ;
                        sh:qualifiedMinCount 1 ] .
             ex:x ex:p ex:x ."
        );
        let parsed = parse_turtle(negative.as_bytes(), None).unwrap();
        let loaded = load_turtle(negative.as_bytes(), None).unwrap();
        assert!(validate_with_evidence(&loaded.graph, &parsed.schema).is_err());
    }

    #[test]
    fn outcome_serializes_and_conforms_matches_ordinary_validation() {
        let ttl = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:property [ sh:path ex:p ; sh:minCount 1 ] ."
        );
        let (graph, raw, outcome) = run(&ttl);
        let normalized = shifty_opt::normalize(&raw);
        let ordinary = crate::validate(&graph, &normalized).unwrap();
        assert_eq!(outcome.conforms, ordinary.conforms);
        let encoded = serde_json::to_string(&outcome).unwrap();
        let decoded: EvidenceRun = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, outcome);

        let sparql = format!(
            "{PREFIXES}
             ex:S a sh:NodeShape ; sh:targetNode ex:x ;
               sh:sparql [ sh:select \"SELECT $this WHERE {{ FILTER NOT EXISTS {{ $this <http://ex/p> ?value }} }}\" ] ."
        );
        let (graph, raw, evidence) = run(&sparql);
        let ordinary = crate::validate(&graph, &shifty_opt::normalize(&raw)).unwrap();
        assert_eq!(evidence.conforms, ordinary.conforms);
        assert!(matches!(foci(&evidence)[0].evidence, Evidence::Failure(_)));
    }
}
