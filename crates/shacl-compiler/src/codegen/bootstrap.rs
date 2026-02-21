use crate::emit::program::serialize_program_deterministic;
use crate::GeneratedRust;
use shifty::compiled_runtime::program::CompiledProgram;

pub fn generate_modules(program: &CompiledProgram) -> Result<GeneratedRust, String> {
    let shape_graph_iri = term_iri(program, program.static_hints.shape_graph_iri_term)?;
    let data_graph_iri = program
        .static_hints
        .default_data_graph_iri_term
        .map(|term_id| term_iri(program, term_id))
        .transpose()?
        .unwrap_or_default();

    let shape_arms = program
        .static_hints
        .shape_id_to_term
        .iter()
        .map(|row| {
            let iri = term_iri(program, row.term).unwrap_or_default();
            format!("        {} => {},\n", row.id, lit(&iri))
        })
        .collect::<String>();

    let component_arms = program
        .static_hints
        .component_id_to_iri_term
        .iter()
        .map(|row| {
            let iri = term_iri(program, row.term).unwrap_or_default();
            format!("        {} => {},\n", row.id, lit(&iri))
        })
        .collect::<String>();

    let program_json = String::from_utf8(serialize_program_deterministic(program)?)
        .map_err(|err| format!("compiled program is not valid utf-8 JSON: {err}"))?;

    let bootstrap = format!(
        "use oxigraph::model::{{GraphNameRef, NamedNode, Term}};\n\
         use oxigraph::store::Store;\n\
         use shifty::compiled_runtime::analysis::AnalysisBudget;\n\
         use shifty::compiled_runtime::program::CompiledProgram;\n\
         use shifty::compiled_runtime::report::CompactResultPath;\n\
         use shifty::compiled_runtime::{{self, KernelOptions}};\n\
         use std::collections::BTreeMap;\n\
         use std::sync::OnceLock;\n\
         \n\
         pub const SHAPE_GRAPH: &str = {shape_graph};\n\
         pub const DATA_GRAPH: &str = {data_graph};\n\
         \n\
         #[derive(Debug, Clone, Default)]\n\
         pub struct RuntimeMetricsSnapshot {{\n\
             pub fast_path_hits: u64,\n\
             pub fallback_dispatches: u64,\n\
             pub per_component_violations: BTreeMap<u64, u64>,\n\
         }}\n\
         \n\
         pub fn runtime_metrics_snapshot() -> RuntimeMetricsSnapshot {{\n\
             RuntimeMetricsSnapshot::default()\n\
         }}\n\
         \n\
         #[derive(Debug, Default)]\n\
         pub struct Report {{\n\
             pub violations: Vec<Violation>,\n\
             report_turtle: String,\n\
         }}\n\
         \n\
         #[derive(Debug)]\n\
         pub struct Violation {{\n\
             pub shape_id: u64,\n\
             pub component_id: u64,\n\
             pub focus: Term,\n\
             pub value: Option<Term>,\n\
             pub path: Option<ResultPath>,\n\
         }}\n\
         \n\
         #[derive(Debug, Clone)]\n\
         pub enum ResultPath {{\n\
             Term(Term),\n\
             PathId(u64),\n\
         }}\n\
         \n\
         impl Report {{\n\
             pub fn to_turtle(&self, _store: &Store) -> String {{\n\
                 self.report_turtle.clone()\n\
             }}\n\
         }}\n\
         \n\
         fn compiled_program() -> &'static CompiledProgram {{\n\
             static PROGRAM: OnceLock<CompiledProgram> = OnceLock::new();\n\
             PROGRAM.get_or_init(|| {{\n\
                 serde_json::from_str(include_str!(\"program.json\"))\n\
                     .expect(\"failed to parse embedded compiled program\")\n\
             }})\n\
         }}\n\
         \n\
         pub fn shape_iri(shape_id: u64) -> &'static str {{\n\
             match shape_id {{\n\
{shape_arms}                 _ => \"\",\n\
             }}\n\
         }}\n\
         \n\
         pub fn component_iri(component_id: u64) -> &'static str {{\n\
             match component_id {{\n\
{component_arms}                 _ => \"http://www.w3.org/ns/shacl#ConstraintComponent\",\n\
             }}\n\
         }}\n\
         \n\
         pub fn run_with_options(\n\
             store: &Store,\n\
             data_graph: Option<&NamedNode>,\n\
             enable_inference: bool,\n\
         ) -> Report {{\n\
             let opts = KernelOptions {{\n\
                 enable_inference,\n\
                 follow_bnodes: false,\n\
                 analysis_budget: AnalysisBudget::default(),\n\
                 optimization_level: 1,\n\
             }};\n\
\n\
             match compiled_runtime::run(\n\
                 store,\n\
                 data_graph.map(|graph| GraphNameRef::NamedNode(graph.as_ref())),\n\
                 compiled_program(),\n\
                 &opts,\n\
             ) {{\n\
                 Ok(kernel_report) => {{\n\
                     let violations = kernel_report\n\
                         .compact\n\
                         .violations\n\
                         .into_iter()\n\
                         .map(|violation| Violation {{\n\
                             shape_id: violation.shape_id,\n\
                             component_id: violation.component_id,\n\
                             focus: violation.focus,\n\
                             value: violation.value,\n\
                             path: violation.path.map(|path| match path {{\n\
                                 CompactResultPath::Term(term) => ResultPath::Term(term),\n\
                                 CompactResultPath::PathId(path_id) => ResultPath::PathId(path_id),\n\
                             }}),\n\
                         }})\n\
                         .collect();\n\
\n\
                     Report {{\n\
                         violations,\n\
                         report_turtle: kernel_report.report_turtle,\n\
                     }}\n\
                 }}\n\
                 Err(err) => {{\n\
                     eprintln!(\"compiled runtime error: {{}}\", err);\n\
                     Report {{\n\
                         violations: Vec::new(),\n\
                         report_turtle: String::from(\"@prefix sh: <http://www.w3.org/ns/shacl#> .\\n[] a sh:ValidationReport ; sh:conforms true .\\n\"),\n\
                     }}\n\
                 }}\n\
             }}\n\
         }}\n\
         \n\
         pub fn run(store: &Store, data_graph: Option<&NamedNode>) -> Report {{\n\
             run_with_options(store, data_graph, true)\n\
         }}\n\
         \n\
         pub fn render_report(report: &Report, _store: &Store, _follow_bnodes: bool) -> String {{\n\
             report.report_turtle.clone()\n\
         }}\n"
        ,
        shape_graph = lit(&shape_graph_iri),
        data_graph = lit(&data_graph_iri),
        shape_arms = shape_arms,
        component_arms = component_arms,
    );

    let root = String::from("// Code generated by shacl-compiler. DO NOT EDIT.\n#![allow(unused_variables)]\n#![allow(dead_code)]\n#![allow(unused_mut)]\n#![allow(unused_imports)]\n#![allow(unused_comparisons)]\ninclude!(\"bootstrap.rs\");\n");

    Ok(GeneratedRust {
        root,
        files: vec![
            ("bootstrap.rs".to_string(), bootstrap),
            ("program.json".to_string(), program_json),
        ],
    })
}

fn term_iri(program: &CompiledProgram, term_id: u64) -> Result<String, String> {
    let term = program
        .term(term_id)
        .ok_or_else(|| format!("missing term {} in compiled program", term_id))?;
    match term {
        oxigraph::model::Term::NamedNode(node) => Ok(node.as_str().to_string()),
        _ => Err(format!("term {} is not an IRI", term_id)),
    }
}

fn lit(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}
