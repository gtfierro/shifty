use crate::compiled_runtime::analysis::{
    AnalysisBudget, AnalysisContext, AnalysisPhase, AnalysisState, AnalyzerRegistry,
};
use crate::compiled_runtime::opt::{build_default_plan, ExecutionPlan, PlanRewriterRegistry};
use crate::compiled_runtime::path;
use crate::compiled_runtime::program::{CompiledProgram, KERNEL_VERSION, PROGRAM_SCHEMA_VERSION};
use crate::compiled_runtime::report::{compact_from_graph, graph_to_turtle, CompactReport};
use crate::{Source, ValidationReportOptions, Validator};
use oxigraph::model::{GraphName, GraphNameRef, NamedNode, Quad};
use oxigraph::store::Store;
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct KernelOptions {
    pub enable_inference: bool,
    pub follow_bnodes: bool,
    pub analysis_budget: AnalysisBudget,
    pub optimization_level: u8,
}

impl Default for KernelOptions {
    fn default() -> Self {
        Self {
            enable_inference: true,
            follow_bnodes: false,
            analysis_budget: AnalysisBudget::default(),
            optimization_level: 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KernelReport {
    pub compact: CompactReport,
    pub report_turtle: String,
    pub stage_timings_ms: BTreeMap<String, u128>,
    pub analysis_state: AnalysisState,
    pub execution_plan: ExecutionPlan,
}

pub fn run(
    store: &Store,
    data_graph: Option<GraphNameRef<'_>>,
    program: &CompiledProgram,
    opts: &KernelOptions,
) -> Result<KernelReport, String> {
    ensure_compatible(program)?;

    let mut stage_timings_ms = BTreeMap::new();

    let shape_graph_iri =
        path::term_as_named_node(program, program.static_hints.shape_graph_iri_term)?;
    let data_graph_iri = resolve_data_graph_iri(program, data_graph)?;
    let data_graph_ref = GraphNameRef::NamedNode(data_graph_iri.as_ref());

    let analysis_ctx = AnalysisContext {
        program,
        store,
        data_graph: Some(data_graph_ref),
    };
    let analyzer_registry = AnalyzerRegistry::with_defaults();
    let mut analysis_state = AnalysisState::default();

    let started = Instant::now();
    analyzer_registry.run_phase(
        AnalysisPhase::Static,
        opts.analysis_budget.max_static_analyzers,
        &analysis_ctx,
        &mut analysis_state,
    )?;
    stage_timings_ms.insert("shape_analysis".to_string(), started.elapsed().as_millis());

    let started = Instant::now();
    analyzer_registry.run_phase(
        AnalysisPhase::Runtime,
        opts.analysis_budget.max_runtime_analyzers,
        &analysis_ctx,
        &mut analysis_state,
    )?;
    stage_timings_ms.insert("data_analysis".to_string(), started.elapsed().as_millis());

    let started = Instant::now();
    let mut execution_plan = build_default_plan(program);
    let rewriters = PlanRewriterRegistry::default();
    rewriters.rewrite(&mut execution_plan, &analysis_state)?;
    stage_timings_ms.insert("plan_build".to_string(), started.elapsed().as_millis());

    let started = Instant::now();
    let shape_quads = build_shape_quads(program, &shape_graph_iri)?;
    stage_timings_ms.insert("load_program".to_string(), started.elapsed().as_millis());

    let started = Instant::now();
    let data_quads = collect_data_quads(store, data_graph_ref, &data_graph_iri)?;
    let validator = Validator::builder()
        .with_shapes_source(Source::Quads {
            graph: shape_graph_iri.to_string(),
            quads: shape_quads,
        })
        .with_data_source(Source::Quads {
            graph: data_graph_iri.to_string(),
            quads: data_quads,
        })
        .with_do_imports(false)
        .build()
        .map_err(|err| format!("failed to build compiled runtime validator: {err}"))?;

    if opts.enable_inference {
        validator
            .run_inference()
            .map_err(|err| format!("compiled runtime inference failed: {err}"))?;
    }

    let report = validator.validate();
    stage_timings_ms.insert("validate".to_string(), started.elapsed().as_millis());

    let started = Instant::now();
    let report_graph = report.to_graph_with_options(ValidationReportOptions {
        follow_bnodes: opts.follow_bnodes,
    });
    let report_turtle = report
        .to_turtle_with_options(ValidationReportOptions {
            follow_bnodes: opts.follow_bnodes,
        })
        .map_err(|err| format!("failed to serialize validation report: {err}"))?;

    let compact = compact_from_graph(&report_graph, program);
    stage_timings_ms.insert("report".to_string(), started.elapsed().as_millis());

    let _ = graph_to_turtle(&report_graph);

    Ok(KernelReport {
        compact,
        report_turtle,
        stage_timings_ms,
        analysis_state,
        execution_plan,
    })
}

fn ensure_compatible(program: &CompiledProgram) -> Result<(), String> {
    if program.header.schema_version != PROGRAM_SCHEMA_VERSION {
        return Err(format!(
            "compiled program schema {} is incompatible with kernel schema {}",
            program.header.schema_version, PROGRAM_SCHEMA_VERSION
        ));
    }

    if program.header.min_kernel_version > KERNEL_VERSION {
        return Err(format!(
            "compiled program requires kernel version {}, but runtime is {}",
            program.header.min_kernel_version, KERNEL_VERSION
        ));
    }

    Ok(())
}

fn resolve_data_graph_iri(
    program: &CompiledProgram,
    data_graph: Option<GraphNameRef<'_>>,
) -> Result<NamedNode, String> {
    match data_graph {
        Some(GraphNameRef::NamedNode(node)) => Ok(node.into_owned()),
        Some(GraphNameRef::BlankNode(_)) => Err(
            "compiled runtime expects a named data graph; blank graph names are unsupported"
                .to_string(),
        ),
        Some(GraphNameRef::DefaultGraph) => program
            .static_hints
            .default_data_graph_iri_term
            .ok_or_else(|| "compiled program does not define a default data graph IRI".to_string())
            .and_then(|term_id| path::term_as_named_node(program, term_id)),
        None => program
            .static_hints
            .default_data_graph_iri_term
            .ok_or_else(|| "compiled program does not define a default data graph IRI".to_string())
            .and_then(|term_id| path::term_as_named_node(program, term_id)),
    }
}

fn build_shape_quads(
    program: &CompiledProgram,
    shape_graph_iri: &NamedNode,
) -> Result<Vec<Quad>, String> {
    let mut quads = Vec::with_capacity(program.shape_graph_triples.len());
    for triple in &program.shape_graph_triples {
        let subject = path::term_as_subject(program, triple.subject)?;
        let predicate = path::term_as_named_node(program, triple.predicate)?;
        let object = path::term(program, triple.object)?;
        quads.push(Quad::new(
            subject,
            predicate,
            object,
            GraphName::NamedNode(shape_graph_iri.clone()),
        ));
    }
    Ok(quads)
}

fn collect_data_quads(
    store: &Store,
    data_graph_ref: GraphNameRef<'_>,
    data_graph_iri: &NamedNode,
) -> Result<Vec<Quad>, String> {
    let mut quads = Vec::new();
    for quad in store.quads_for_pattern(None, None, None, Some(data_graph_ref)) {
        let quad = quad.map_err(|err| format!("failed to read runtime data quad: {err}"))?;
        quads.push(Quad::new(
            quad.subject,
            quad.predicate,
            quad.object,
            GraphName::NamedNode(data_graph_iri.clone()),
        ));
    }
    Ok(quads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiled_runtime::program::{
        ComponentRow, ProgramHeader, ShapeRow, StaticHints, TripleRow, KERNEL_VERSION,
        PROGRAM_SCHEMA_VERSION,
    };
    use oxigraph::model::{NamedNode, Term};

    fn minimal_program() -> CompiledProgram {
        CompiledProgram {
            header: ProgramHeader {
                schema_version: PROGRAM_SCHEMA_VERSION,
                min_kernel_version: KERNEL_VERSION,
                program_hash: [0u8; 32],
                feature_bits: 0,
            },
            terms: vec![Term::NamedNode(
                NamedNode::new("http://example.com/shapes").expect("valid iri"),
            )],
            shapes: Vec::<ShapeRow>::new(),
            components: Vec::<ComponentRow>::new(),
            paths: Vec::new(),
            targets: Vec::new(),
            rules: Vec::new(),
            shape_graph_triples: Vec::<TripleRow>::new(),
            static_hints: StaticHints {
                shape_graph_iri_term: 0,
                default_data_graph_iri_term: Some(0),
                shape_id_to_term: Vec::new(),
                component_id_to_iri_term: Vec::new(),
                path_id_to_term: Vec::new(),
            },
            ext: BTreeMap::new(),
        }
    }

    #[test]
    fn rejects_schema_mismatch() {
        let mut program = minimal_program();
        program.header.schema_version = PROGRAM_SCHEMA_VERSION + 1;
        let err = ensure_compatible(&program).expect_err("schema mismatch should fail");
        assert!(err.contains("schema"));
    }

    #[test]
    fn rejects_kernel_version_mismatch() {
        let mut program = minimal_program();
        program.header.min_kernel_version = KERNEL_VERSION + 1;
        let err = ensure_compatible(&program).expect_err("kernel mismatch should fail");
        assert!(err.contains("kernel version"));
    }
}
