#![allow(clippy::too_many_arguments)]

use std::fs;
use std::path::{Path, PathBuf};

use ::shifty::shacl_ir::ShapeIR;
use ::shifty::trace::TraceEvent;
use ::shifty::{InferenceConfig, Source, ValidationReportOptions, Validator};
use ontoenv::config::Config;
use oxigraph::model::{
    BlankNode, Graph, GraphName, Literal, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Quad,
    Term, TermRef,
};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyTuple};
use pyo3::wrap_pyfunction;
use serde_json::json;
use std::sync::Once;
use tempfile::tempdir;

fn map_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyValueError::new_err(err.to_string())
}

const PY_SHAPES_GRAPH_IRI: &str = "urn:shifty:python:shapes";
const PY_DATA_GRAPH_IRI: &str = "urn:shifty:python:data";
const XSD_STRING_IRI: &str = "http://www.w3.org/2001/XMLSchema#string";

struct RdflibTerms {
    graph: Py<PyAny>,
    uri_ref: Py<PyAny>,
    bnode: Py<PyAny>,
    literal: Py<PyAny>,
}

fn rdflib_terms(py: Python<'_>) -> PyResult<RdflibTerms> {
    let rdflib = PyModule::import(py, "rdflib")?;
    let term = PyModule::import(py, "rdflib.term")?;
    Ok(RdflibTerms {
        graph: rdflib.getattr("Graph")?.unbind(),
        uri_ref: term.getattr("URIRef")?.unbind(),
        bnode: term.getattr("BNode")?.unbind(),
        literal: term.getattr("Literal")?.unbind(),
    })
}

fn py_term_as_string(term: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(term.str()?.to_string())
}

fn py_term_to_named_or_blank(
    py: Python<'_>,
    term: &Bound<'_, PyAny>,
    terms: &RdflibTerms,
    role: &str,
) -> PyResult<NamedOrBlankNode> {
    if term.is_instance(terms.uri_ref.bind(py))? {
        let iri = py_term_as_string(term)?;
        let node = NamedNode::new(iri).map_err(map_err)?;
        return Ok(NamedOrBlankNode::NamedNode(node));
    }
    if term.is_instance(terms.bnode.bind(py))? {
        let id = py_term_as_string(term)?;
        let node = BlankNode::new(id).map_err(map_err)?;
        return Ok(NamedOrBlankNode::BlankNode(node));
    }
    Err(PyValueError::new_err(format!(
        "Expected URIRef or BNode for {}",
        role
    )))
}

fn py_term_to_predicate(
    py: Python<'_>,
    term: &Bound<'_, PyAny>,
    terms: &RdflibTerms,
) -> PyResult<NamedNode> {
    if !term.is_instance(terms.uri_ref.bind(py))? {
        return Err(PyValueError::new_err(
            "Predicate must be an rdflib.term.URIRef",
        ));
    }
    NamedNode::new(py_term_as_string(term)?).map_err(map_err)
}

fn py_term_to_object(
    py: Python<'_>,
    term: &Bound<'_, PyAny>,
    terms: &RdflibTerms,
) -> PyResult<Term> {
    if term.is_instance(terms.uri_ref.bind(py))? {
        let iri = py_term_as_string(term)?;
        return Ok(Term::NamedNode(NamedNode::new(iri).map_err(map_err)?));
    }

    if term.is_instance(terms.bnode.bind(py))? {
        let id = py_term_as_string(term)?;
        return Ok(Term::BlankNode(BlankNode::new(id).map_err(map_err)?));
    }

    if term.is_instance(terms.literal.bind(py))? {
        let lexical = py_term_as_string(term)?;
        let language_obj = term.getattr("language")?;
        if !language_obj.is_none() {
            let language = py_term_as_string(&language_obj)?;
            let literal =
                Literal::new_language_tagged_literal(lexical, language).map_err(map_err)?;
            return Ok(Term::Literal(literal));
        }

        let datatype_obj = term.getattr("datatype")?;
        if !datatype_obj.is_none() {
            let datatype = NamedNode::new(py_term_as_string(&datatype_obj)?).map_err(map_err)?;
            return Ok(Term::Literal(Literal::new_typed_literal(lexical, datatype)));
        }

        return Ok(Term::Literal(Literal::new_simple_literal(lexical)));
    }

    Err(PyValueError::new_err(
        "Object must be an rdflib.term.URIRef, BNode, or Literal",
    ))
}

fn graph_to_quads(
    py: Python<'_>,
    graph: &Bound<'_, PyAny>,
    graph_iri: &str,
) -> PyResult<Vec<Quad>> {
    let terms = rdflib_terms(py)?;
    let graph_name = NamedNode::new(graph_iri).map_err(map_err)?;
    let mut quads = Vec::new();

    for item in graph.try_iter()? {
        let triple = item?;
        let triple = triple.cast::<PyTuple>()?;
        if triple.len() != 3 {
            return Err(PyValueError::new_err(
                "Expected graph iteration to yield 3-tuples",
            ));
        }
        let subject_py = triple.get_item(0)?;
        let predicate_py = triple.get_item(1)?;
        let object_py = triple.get_item(2)?;

        let subject = py_term_to_named_or_blank(py, &subject_py, &terms, "subject")?;
        let predicate = py_term_to_predicate(py, &predicate_py, &terms)?;
        let object = py_term_to_object(py, &object_py, &terms)?;
        quads.push(Quad::new(
            subject,
            predicate,
            object,
            GraphName::NamedNode(graph_name.clone()),
        ));
    }

    Ok(quads)
}

fn build_env_config(root: &Path) -> PyResult<Config> {
    let root_path = root.to_path_buf();
    Config::builder()
        .root(root_path.clone())
        .locations(vec![root_path])
        .offline(false)
        .temporary(true)
        .build()
        .map_err(map_err)
}

fn build_validator_from_graphs(
    py: Python<'_>,
    shapes_graph: Option<&Bound<'_, PyAny>>,
    data_graph: &Bound<'_, PyAny>,
    enable_af: bool,
    enable_rules: bool,
    skip_invalid_rules: bool,
    warnings_are_errors: bool,
    do_imports: bool,
) -> PyResult<Validator> {
    let temp_dir = tempdir().map_err(map_err)?;
    let data_quads = graph_to_quads(py, data_graph, PY_DATA_GRAPH_IRI)?;
    let config = build_env_config(temp_dir.path())?;
    let mut builder = Validator::builder();
    if let Some(shapes_graph) = shapes_graph {
        let shapes_quads = graph_to_quads(py, shapes_graph, PY_SHAPES_GRAPH_IRI)?;
        builder = builder.with_shapes_source(Source::Quads {
            graph: PY_SHAPES_GRAPH_IRI.to_string(),
            quads: shapes_quads,
        });
    }

    let validator = builder
        .with_data_source(Source::Quads {
            graph: PY_DATA_GRAPH_IRI.to_string(),
            quads: data_quads,
        })
        .with_env_config(config)
        .with_af_enabled(enable_af)
        .with_rules_enabled(enable_rules)
        .with_skip_invalid_rules(skip_invalid_rules)
        .with_warnings_are_errors(warnings_are_errors)
        .with_do_imports(do_imports)
        .build()
        .map_err(map_err)?;

    Ok(validator)
}

fn build_validator_from_ir(
    py: Python<'_>,
    shape_ir: &ShapeIR,
    data_graph: &Bound<'_, PyAny>,
    enable_af: bool,
    enable_rules: bool,
    skip_invalid_rules: bool,
    warnings_are_errors: bool,
    do_imports: bool,
) -> PyResult<Validator> {
    let temp_dir = tempdir().map_err(map_err)?;
    let data_quads = graph_to_quads(py, data_graph, PY_DATA_GRAPH_IRI)?;
    let config = build_env_config(temp_dir.path())?;

    Validator::builder()
        .with_shape_ir(shape_ir.clone())
        .with_data_source(Source::Quads {
            graph: PY_DATA_GRAPH_IRI.to_string(),
            quads: data_quads,
        })
        .with_env_config(config)
        .with_af_enabled(enable_af)
        .with_rules_enabled(enable_rules)
        .with_skip_invalid_rules(skip_invalid_rules)
        .with_warnings_are_errors(warnings_are_errors)
        .with_do_imports(do_imports)
        .build()
        .map_err(map_err)
}

fn build_inference_config(
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    trace: Option<bool>,
) -> PyResult<InferenceConfig> {
    let mut config = InferenceConfig::default();

    if let Some(min) = min_iterations {
        if min == 0 {
            return Err(PyValueError::new_err("min_iterations must be at least 1"));
        }
        config.min_iterations = min;
    }

    if let Some(max) = max_iterations {
        if max == 0 {
            return Err(PyValueError::new_err("max_iterations must be at least 1"));
        }
        config.max_iterations = max;
    }

    if config.max_iterations < config.min_iterations {
        return Err(PyValueError::new_err(
            "max_iterations must be greater than or equal to min_iterations",
        ));
    }

    if let Some(run_until) = run_until_converged {
        config.run_until_converged = run_until;
    }

    if let Some(blank_policy) = error_on_blank_nodes {
        config.error_on_blank_nodes = blank_policy;
    }

    if let Some(trace) = trace {
        config.trace = trace;
    }

    Ok(config)
}

fn resolve_alias<T>(
    primary: Option<T>,
    alias: Option<T>,
    primary_name: &str,
    alias_name: &str,
) -> PyResult<Option<T>>
where
    T: Clone + PartialEq,
{
    match (primary, alias) {
        (Some(primary_value), Some(alias_value)) => {
            if primary_value == alias_value {
                Ok(Some(primary_value))
            } else {
                Err(PyValueError::new_err(format!(
                    "Received conflicting values for {} and {}",
                    primary_name, alias_name
                )))
            }
        }
        (Some(primary_value), None) => Ok(Some(primary_value)),
        (None, Some(alias_value)) => Ok(Some(alias_value)),
        (None, None) => Ok(None),
    }
}

fn merge_option<T>(
    target: &mut Option<T>,
    incoming: Option<T>,
    target_name: &str,
    source_name: &str,
) -> PyResult<()>
where
    T: PartialEq + Copy,
{
    if let Some(value) = incoming {
        if let Some(existing) = target {
            if *existing != value {
                return Err(PyValueError::new_err(format!(
                    "Received conflicting values for {} and {}",
                    target_name, source_name
                )));
            }
        } else {
            *target = Some(value);
        }
    }
    Ok(())
}

fn trace_event_to_json(event: &TraceEvent) -> serde_json::Value {
    match event {
        TraceEvent::EnterNodeShape(id) => json!({
            "type": "EnterNodeShape",
            "node_shape_id": id.0,
        }),
        TraceEvent::EnterPropertyShape(id) => json!({
            "type": "EnterPropertyShape",
            "property_shape_id": id.0,
        }),
        TraceEvent::ComponentPassed {
            component,
            focus,
            value,
        } => json!({
            "type": "ComponentPassed",
            "component_id": component.0,
            "focus": focus.to_string(),
            "value": value.as_ref().map(|t| t.to_string()),
        }),
        TraceEvent::ComponentFailed {
            component,
            focus,
            value,
            message,
        } => json!({
            "type": "ComponentFailed",
            "component_id": component.0,
            "focus": focus.to_string(),
            "value": value.as_ref().map(|t| t.to_string()),
            "message": message,
        }),
        TraceEvent::SparqlQuery { label } => json!({
            "type": "SparqlQuery",
            "label": label,
        }),
        TraceEvent::RuleApplied { rule, inserted } => json!({
            "type": "RuleApplied",
            "rule_id": rule.0,
            "inserted": inserted,
        }),
    }
}

fn trace_events_to_py(py: Python<'_>, events: &[TraceEvent]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for event in events {
        let dict = PyDict::new(py);
        match event {
            TraceEvent::EnterNodeShape(id) => {
                dict.set_item("type", "EnterNodeShape")?;
                dict.set_item("node_shape_id", id.0)?;
            }
            TraceEvent::EnterPropertyShape(id) => {
                dict.set_item("type", "EnterPropertyShape")?;
                dict.set_item("property_shape_id", id.0)?;
            }
            TraceEvent::ComponentPassed {
                component,
                focus,
                value,
            } => {
                dict.set_item("type", "ComponentPassed")?;
                dict.set_item("component_id", component.0)?;
                dict.set_item("focus", focus.to_string())?;
                dict.set_item("value", value.as_ref().map(|t| t.to_string()))?;
            }
            TraceEvent::ComponentFailed {
                component,
                focus,
                value,
                message,
            } => {
                dict.set_item("type", "ComponentFailed")?;
                dict.set_item("component_id", component.0)?;
                dict.set_item("focus", focus.to_string())?;
                dict.set_item("value", value.as_ref().map(|t| t.to_string()))?;
                dict.set_item("message", message)?;
            }
            TraceEvent::SparqlQuery { label } => {
                dict.set_item("type", "SparqlQuery")?;
                dict.set_item("label", label)?;
            }
            TraceEvent::RuleApplied { rule, inserted } => {
                dict.set_item("type", "RuleApplied")?;
                dict.set_item("rule_id", rule.0)?;
                dict.set_item("inserted", inserted)?;
            }
        }
        list.append(dict)?;
    }
    Ok(list.into())
}

fn write_trace_outputs(
    events: &[TraceEvent],
    trace_file: Option<&Path>,
    trace_jsonl: Option<&Path>,
) -> PyResult<()> {
    if let Some(path) = trace_file {
        let mut lines = String::new();
        for ev in events {
            lines.push_str(&format!("{:?}\n", ev));
        }
        fs::write(path, lines).map_err(map_err)?;
    }

    if let Some(path) = trace_jsonl {
        let mut buf = String::new();
        for ev in events {
            let value = trace_event_to_json(ev);
            let line = serde_json::to_string(&value).map_err(map_err)?;
            buf.push_str(&line);
            buf.push('\n');
        }
        fs::write(path, buf).map_err(map_err)?;
    }

    Ok(())
}

fn resolve_run_until(
    run_until_converged: Option<bool>,
    no_converge: Option<bool>,
    run_until_name: &str,
    no_converge_name: &str,
) -> PyResult<Option<bool>> {
    if let Some(flag) = no_converge {
        let inferred_value = Some(!flag);
        if let Some(explicit) = run_until_converged {
            if explicit == flag {
                return Err(PyValueError::new_err(format!(
                    "Received conflicting values for {} and {}",
                    run_until_name, no_converge_name
                )));
            }
            Ok(Some(explicit))
        } else {
            Ok(inferred_value)
        }
    } else {
        Ok(run_until_converged)
    }
}

fn empty_graph(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let terms = rdflib_terms(py)?;
    let graph = terms.graph.bind(py).call0()?;
    Ok(graph.into())
}

fn named_or_blank_ref_to_py(
    py: Python<'_>,
    terms: &RdflibTerms,
    node: NamedOrBlankNodeRef<'_>,
) -> PyResult<Py<PyAny>> {
    match node {
        NamedOrBlankNodeRef::NamedNode(node) => {
            Ok(terms.uri_ref.bind(py).call1((node.as_str(),))?.into())
        }
        NamedOrBlankNodeRef::BlankNode(node) => {
            Ok(terms.bnode.bind(py).call1((node.as_str(),))?.into())
        }
    }
}

fn term_ref_to_py(py: Python<'_>, terms: &RdflibTerms, term: TermRef<'_>) -> PyResult<Py<PyAny>> {
    match term {
        TermRef::NamedNode(node) => Ok(terms.uri_ref.bind(py).call1((node.as_str(),))?.into()),
        TermRef::BlankNode(node) => Ok(terms.bnode.bind(py).call1((node.as_str(),))?.into()),
        TermRef::Literal(literal) => {
            let kwargs = PyDict::new(py);
            if let Some(language) = literal.language() {
                kwargs.set_item("lang", language)?;
            } else if literal.datatype().as_str() != XSD_STRING_IRI {
                let datatype = terms
                    .uri_ref
                    .bind(py)
                    .call1((literal.datatype().as_str(),))?;
                kwargs.set_item("datatype", datatype)?;
            }
            Ok(terms
                .literal
                .bind(py)
                .call((literal.value(),), Some(&kwargs))?
                .into())
        }
    }
}

fn append_quad_triple_to_graph(
    py: Python<'_>,
    terms: &RdflibTerms,
    graph: &Bound<'_, PyAny>,
    quad: &Quad,
) -> PyResult<()> {
    let subject = named_or_blank_ref_to_py(py, terms, quad.subject.as_ref())?;
    let predicate = terms
        .uri_ref
        .bind(py)
        .call1((quad.predicate.as_str(),))?
        .into();
    let object = term_ref_to_py(py, terms, quad.object.as_ref())?;
    let triple = PyTuple::new(py, &[subject, predicate, object])?;
    graph.call_method1("add", (triple,))?;
    Ok(())
}

fn graph_from_quads(py: Python<'_>, quads: &[Quad]) -> PyResult<Py<PyAny>> {
    if quads.is_empty() {
        return empty_graph(py);
    }

    let terms = rdflib_terms(py)?;
    let graph = terms.graph.bind(py).call0()?;
    for quad in quads {
        append_quad_triple_to_graph(py, &terms, &graph, quad)?;
    }
    Ok(graph.into())
}

fn graph_from_oxigraph_graph(py: Python<'_>, ox_graph: &Graph) -> PyResult<Py<PyAny>> {
    let terms = rdflib_terms(py)?;
    let graph = terms.graph.bind(py).call0()?;
    for triple in ox_graph.iter() {
        let subject = named_or_blank_ref_to_py(py, &terms, triple.subject)?;
        let predicate = terms
            .uri_ref
            .bind(py)
            .call1((triple.predicate.as_str(),))?
            .into();
        let object = term_ref_to_py(py, &terms, triple.object)?;
        let tuple = PyTuple::new(py, &[subject, predicate, object])?;
        graph.call_method1("add", (tuple,))?;
    }
    Ok(graph.into())
}

fn execute_infer(
    py: Python<'_>,
    validator: Validator,
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    debug: Option<bool>,
    graphviz: bool,
    heatmap: bool,
    heatmap_all: bool,
    trace_events: bool,
    trace_file: Option<PathBuf>,
    trace_jsonl: Option<PathBuf>,
    return_inference_outcome: bool,
    union: bool,
) -> PyResult<Py<PyAny>> {
    let should_collect_traces =
        trace_events || trace_file.is_some() || trace_jsonl.is_some() || heatmap;
    let trace_buf = if should_collect_traces {
        Some(validator.context().trace_events())
    } else {
        None
    };

    let config = build_inference_config(
        min_iterations,
        max_iterations,
        run_until_converged,
        error_on_blank_nodes,
        debug,
    )?;
    let outcome = validator
        .run_inference_with_config(config)
        .map_err(map_err)?;

    let output_graph = if union {
        let all_data_quads = validator.data_graph_quads().map_err(map_err)?;
        graph_from_quads(py, &all_data_quads)?
    } else {
        graph_from_quads(py, &outcome.inferred_quads)?
    };

    let diagnostics = PyDict::new(py);

    if graphviz {
        let dot = validator.to_graphviz().map_err(map_err)?;
        diagnostics.set_item("graphviz", dot)?;
    }

    if heatmap {
        let _report = validator.validate();
        let heatmap_dot = validator
            .to_graphviz_heatmap(heatmap_all)
            .map_err(map_err)?;
        diagnostics.set_item("heatmap", heatmap_dot)?;
    }

    if let Some(buf) = trace_buf {
        let events = buf
            .lock()
            .ok()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        write_trace_outputs(&events, trace_file.as_deref(), trace_jsonl.as_deref())?;
        if trace_events {
            let py_events = trace_events_to_py(py, &events)?;
            diagnostics.set_item("trace_events", py_events)?;
        }
    }

    if return_inference_outcome {
        let stats = PyDict::new(py);
        stats.set_item("iterations_executed", outcome.iterations_executed)?;
        stats.set_item("triples_added", outcome.triples_added)?;
        stats.set_item("converged", outcome.converged)?;
        diagnostics.set_item("inference_outcome", stats)?;
    }

    if diagnostics.is_empty() {
        Ok(output_graph.clone_ref(py))
    } else {
        let graph_obj = output_graph.clone_ref(py).into_py_any(py)?;
        let diag_obj = diagnostics.into_py_any(py)?;
        let tuple = PyTuple::new(py, &[graph_obj, diag_obj])?;
        Ok(tuple.into_py_any(py)?)
    }
}

struct PreparedInferenceSettings {
    run_inference: bool,
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    debug: Option<bool>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_inference_settings(
    run_inference: bool,
    inference: Option<&Bound<'_, PyAny>>,
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    no_converge: Option<bool>,
    inference_min_iterations: Option<usize>,
    inference_max_iterations: Option<usize>,
    inference_no_converge: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    inference_error_on_blank_nodes: Option<bool>,
    debug: Option<bool>,
    inference_debug: Option<bool>,
) -> PyResult<PreparedInferenceSettings> {
    let mut min_iterations = resolve_alias(
        min_iterations,
        inference_min_iterations,
        "min_iterations",
        "inference_min_iterations",
    )?;
    let mut max_iterations = resolve_alias(
        max_iterations,
        inference_max_iterations,
        "max_iterations",
        "inference_max_iterations",
    )?;
    let mut run_until_converged = run_until_converged;
    let mut no_converge = no_converge;
    let mut inference_no_converge = inference_no_converge;
    let mut error_on_blank_nodes = resolve_alias(
        error_on_blank_nodes,
        inference_error_on_blank_nodes,
        "error_on_blank_nodes",
        "inference_error_on_blank_nodes",
    )?;
    let mut debug = resolve_alias(debug, inference_debug, "debug", "inference_debug")?;
    let mut should_run_inference = run_inference;

    if let Some(obj) = inference {
        if obj.is_none() {
            // No additional options provided
        } else if obj.is_instance_of::<PyBool>() {
            let flag = obj.extract::<bool>()?;
            if should_run_inference && !flag {
                return Err(PyValueError::new_err(
                    "Received conflicting values for run_inference and inference flag",
                ));
            }
            should_run_inference = flag;
        } else if let Ok(dict) = obj.cast::<PyDict>() {
            let mut run_toggle: Option<bool> = None;
            for (key, value) in dict.iter() {
                let key_name: String = key
                    .extract()
                    .map_err(|_| PyValueError::new_err("Inference option keys must be strings"))?;
                if value.is_none() {
                    continue;
                }
                let source_name = format!("inference['{}']", key_name);
                match key_name.as_str() {
                    "run" | "enabled" | "run_inference" => {
                        let flag = value.extract::<bool>()?;
                        run_toggle = match run_toggle {
                            Some(existing) if existing != flag => {
                                return Err(PyValueError::new_err(
                                    "Received conflicting values inside inference['run']",
                                ));
                            }
                            _ => Some(flag),
                        };
                    }
                    "min_iterations" | "inference_min_iterations" => {
                        let parsed = value.extract::<usize>()?;
                        merge_option(
                            &mut min_iterations,
                            Some(parsed),
                            "min_iterations",
                            &source_name,
                        )?;
                    }
                    "max_iterations" | "inference_max_iterations" => {
                        let parsed = value.extract::<usize>()?;
                        merge_option(
                            &mut max_iterations,
                            Some(parsed),
                            "max_iterations",
                            &source_name,
                        )?;
                    }
                    "run_until_converged" => {
                        let parsed = value.extract::<bool>()?;
                        merge_option(
                            &mut run_until_converged,
                            Some(parsed),
                            "run_until_converged",
                            &source_name,
                        )?;
                    }
                    "no_converge" => {
                        let parsed = value.extract::<bool>()?;
                        merge_option(&mut no_converge, Some(parsed), "no_converge", &source_name)?;
                    }
                    "inference_no_converge" => {
                        let parsed = value.extract::<bool>()?;
                        merge_option(
                            &mut inference_no_converge,
                            Some(parsed),
                            "inference_no_converge",
                            &source_name,
                        )?;
                    }
                    "error_on_blank_nodes" | "inference_error_on_blank_nodes" => {
                        let parsed = value.extract::<bool>()?;
                        merge_option(
                            &mut error_on_blank_nodes,
                            Some(parsed),
                            "error_on_blank_nodes",
                            &source_name,
                        )?;
                    }
                    "debug" | "inference_debug" => {
                        let parsed = value.extract::<bool>()?;
                        merge_option(&mut debug, Some(parsed), "debug", &source_name)?;
                    }
                    other => {
                        return Err(PyValueError::new_err(format!(
                            "Unknown inference option '{}'",
                            other
                        )));
                    }
                }
            }

            let desired_run = run_toggle.unwrap_or(true);
            if should_run_inference && !desired_run {
                return Err(PyValueError::new_err(
                    "Received conflicting values for run_inference and inference['run']",
                ));
            }
            should_run_inference = desired_run;
        } else {
            return Err(PyValueError::new_err(
                "inference must be a bool, dict, or None",
            ));
        }
    }

    let run_until_converged = resolve_run_until(
        run_until_converged,
        no_converge,
        "run_until_converged",
        "no_converge",
    )?;
    let run_until_converged = resolve_run_until(
        run_until_converged,
        inference_no_converge,
        "run_until_converged",
        "inference_no_converge",
    )?;

    Ok(PreparedInferenceSettings {
        run_inference: should_run_inference,
        min_iterations,
        max_iterations,
        run_until_converged,
        error_on_blank_nodes,
        debug,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_validate(
    py: Python<'_>,
    validator: Validator,
    settings: PreparedInferenceSettings,
    follow_bnodes: bool,
    graphviz: bool,
    heatmap: bool,
    heatmap_all: bool,
    trace_events: bool,
    trace_file: Option<PathBuf>,
    trace_jsonl: Option<PathBuf>,
    return_inference_outcome: bool,
) -> PyResult<Py<PyAny>> {
    let should_collect_traces =
        trace_events || trace_file.is_some() || trace_jsonl.is_some() || heatmap;
    let trace_buf = if should_collect_traces {
        Some(validator.context().trace_events())
    } else {
        None
    };

    let (report, inference_outcome) = if settings.run_inference {
        let config = build_inference_config(
            settings.min_iterations,
            settings.max_iterations,
            settings.run_until_converged,
            settings.error_on_blank_nodes,
            settings.debug,
        )?;
        match validator.validate_with_inference(config) {
            Ok((outcome, report)) => (report, Some(outcome)),
            Err(err) => return Err(map_err(err)),
        }
    } else {
        (validator.validate(), None)
    };

    let conforms = report.conforms();
    let report_turtle = report
        .to_turtle_with_options(ValidationReportOptions { follow_bnodes })
        .map_err(map_err)?;
    let report_graph_ox = report.to_graph_with_options(ValidationReportOptions { follow_bnodes });
    let report_graph = graph_from_oxigraph_graph(py, &report_graph_ox)?;
    let diagnostics = PyDict::new(py);

    if graphviz {
        let dot = validator.to_graphviz().map_err(map_err)?;
        diagnostics.set_item("graphviz", dot)?;
    }

    if heatmap {
        let dot = validator
            .to_graphviz_heatmap(heatmap_all)
            .map_err(map_err)?;
        diagnostics.set_item("heatmap", dot)?;
    }

    if let Some(buf) = trace_buf {
        let events = buf
            .lock()
            .ok()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        write_trace_outputs(&events, trace_file.as_deref(), trace_jsonl.as_deref())?;
        if trace_events {
            let py_events = trace_events_to_py(py, &events)?;
            diagnostics.set_item("trace_events", py_events)?;
        }
    }

    if return_inference_outcome {
        if let Some(outcome) = inference_outcome {
            let stats = PyDict::new(py);
            stats.set_item("iterations_executed", outcome.iterations_executed)?;
            stats.set_item("triples_added", outcome.triples_added)?;
            stats.set_item("converged", outcome.converged)?;
            diagnostics.set_item("inference_outcome", stats)?;
        }
    }

    if diagnostics.is_empty() {
        let items: Vec<Py<PyAny>> = vec![
            conforms.into_py_any(py)?,
            report_graph.clone_ref(py).into_py_any(py)?,
            report_turtle.into_py_any(py)?,
        ];
        let tuple = PyTuple::new(py, &items)?;
        Ok(tuple.into_py_any(py)?)
    } else {
        let items: Vec<Py<PyAny>> = vec![
            conforms.into_py_any(py)?,
            report_graph.clone_ref(py).into_py_any(py)?,
            report_turtle.into_py_any(py)?,
            diagnostics.into_py_any(py)?,
        ];
        let tuple = PyTuple::new(py, &items)?;
        Ok(tuple.into_py_any(py)?)
    }
}

/// Cache of ShapeIR artifacts that allows repeated inference/validation
/// without rebuilding validators from scratch.
#[pyclass(name = "CompiledShapeGraph")]
struct PyCompiledShapeGraph {
    shape_ir: ShapeIR,
}

#[pymethods]
impl PyCompiledShapeGraph {
    /// Run SHACL rule inference using the cached ShapeIR.
    #[pyo3(signature=(data_graph, *, min_iterations=None, max_iterations=None, run_until_converged=None, no_converge=None, error_on_blank_nodes=None, enable_af=true, enable_rules=true, debug=None, skip_invalid_rules=false, warnings_are_errors=false, do_imports=true, graphviz=false, heatmap=false, heatmap_all=false, trace_events=false, trace_file=None, trace_jsonl=None, return_inference_outcome=false, union=false))]
    fn infer(
        &self,
        py: Python<'_>,
        data_graph: &Bound<'_, PyAny>,
        min_iterations: Option<usize>,
        max_iterations: Option<usize>,
        run_until_converged: Option<bool>,
        no_converge: Option<bool>,
        error_on_blank_nodes: Option<bool>,
        enable_af: bool,
        enable_rules: bool,
        debug: Option<bool>,
        skip_invalid_rules: Option<bool>,
        warnings_are_errors: Option<bool>,
        do_imports: Option<bool>,
        graphviz: bool,
        heatmap: bool,
        heatmap_all: bool,
        trace_events: bool,
        trace_file: Option<PathBuf>,
        trace_jsonl: Option<PathBuf>,
        return_inference_outcome: bool,
        union: bool,
    ) -> PyResult<Py<PyAny>> {
        let validator = build_validator_from_ir(
            py,
            &self.shape_ir,
            data_graph,
            enable_af,
            enable_rules,
            skip_invalid_rules.unwrap_or(false),
            warnings_are_errors.unwrap_or(false),
            do_imports.unwrap_or(true),
        )?;
        let run_until_converged = resolve_run_until(
            run_until_converged,
            no_converge,
            "run_until_converged",
            "no_converge",
        )?;
        execute_infer(
            py,
            validator,
            min_iterations,
            max_iterations,
            run_until_converged,
            error_on_blank_nodes,
            debug,
            graphviz,
            heatmap,
            heatmap_all,
            trace_events,
            trace_file,
            trace_jsonl,
            return_inference_outcome,
            union,
        )
    }

    /// Validate data using the cached ShapeIR and optionally run inference.
    #[pyo3(signature=(data_graph, *, run_inference=false, inference=None, min_iterations=None, max_iterations=None, run_until_converged=None, no_converge=None, inference_min_iterations=None, inference_max_iterations=None, inference_no_converge=None, error_on_blank_nodes=None, inference_error_on_blank_nodes=None, enable_af=true, enable_rules=true, debug=None, inference_debug=None, skip_invalid_rules=false, warnings_are_errors=false, do_imports=true, follow_bnodes=false, graphviz=false, heatmap=false, heatmap_all=false, trace_events=false, trace_file=None, trace_jsonl=None, return_inference_outcome=false))]
    fn validate(
        &self,
        py: Python<'_>,
        data_graph: &Bound<'_, PyAny>,
        run_inference: bool,
        inference: Option<&Bound<'_, PyAny>>,
        min_iterations: Option<usize>,
        max_iterations: Option<usize>,
        run_until_converged: Option<bool>,
        no_converge: Option<bool>,
        inference_min_iterations: Option<usize>,
        inference_max_iterations: Option<usize>,
        inference_no_converge: Option<bool>,
        error_on_blank_nodes: Option<bool>,
        inference_error_on_blank_nodes: Option<bool>,
        enable_af: bool,
        enable_rules: bool,
        debug: Option<bool>,
        inference_debug: Option<bool>,
        skip_invalid_rules: Option<bool>,
        warnings_are_errors: Option<bool>,
        do_imports: Option<bool>,
        follow_bnodes: bool,
        graphviz: bool,
        heatmap: bool,
        heatmap_all: bool,
        trace_events: bool,
        trace_file: Option<PathBuf>,
        trace_jsonl: Option<PathBuf>,
        return_inference_outcome: bool,
    ) -> PyResult<Py<PyAny>> {
        let validator = build_validator_from_ir(
            py,
            &self.shape_ir,
            data_graph,
            enable_af,
            enable_rules,
            skip_invalid_rules.unwrap_or(false),
            warnings_are_errors.unwrap_or(false),
            do_imports.unwrap_or(true),
        )?;
        let settings = prepare_inference_settings(
            run_inference,
            inference,
            min_iterations,
            max_iterations,
            run_until_converged,
            no_converge,
            inference_min_iterations,
            inference_max_iterations,
            inference_no_converge,
            error_on_blank_nodes,
            inference_error_on_blank_nodes,
            debug,
            inference_debug,
        )?;
        execute_validate(
            py,
            validator,
            settings,
            follow_bnodes,
            graphviz,
            heatmap,
            heatmap_all,
            trace_events,
            trace_file,
            trace_jsonl,
            return_inference_outcome,
        )
    }
}

/// Build and cache the ShapeIR for a SHACL shapes graph.
#[pyfunction(signature=(shapes_graph, *, enable_af=true, enable_rules=true, skip_invalid_rules=false, warnings_are_errors=false, do_imports=true))]
fn generate_ir(
    py: Python<'_>,
    shapes_graph: &Bound<'_, PyAny>,
    enable_af: bool,
    enable_rules: bool,
    skip_invalid_rules: bool,
    warnings_are_errors: bool,
    do_imports: bool,
) -> PyResult<PyCompiledShapeGraph> {
    let temp_dir = tempdir().map_err(map_err)?;
    let shapes_quads = graph_to_quads(py, shapes_graph, PY_SHAPES_GRAPH_IRI)?;
    let config = build_env_config(temp_dir.path())?;
    let validator = Validator::builder()
        .with_shapes_source(Source::Quads {
            graph: PY_SHAPES_GRAPH_IRI.to_string(),
            quads: shapes_quads,
        })
        .with_data_source(Source::Empty)
        .with_env_config(config)
        .with_af_enabled(enable_af)
        .with_rules_enabled(enable_rules)
        .with_skip_invalid_rules(skip_invalid_rules)
        .with_warnings_are_errors(warnings_are_errors)
        .with_do_imports(do_imports)
        .build()
        .map_err(map_err)?;

    let mut shape_ir = validator.context().shape_ir().clone();
    shape_ir.data_graph = None;
    Ok(PyCompiledShapeGraph { shape_ir })
}

/// Run SHACL rules to infer additional triples, optionally with diagnostics.
/// If `shapes_graph` is omitted, the data graph is used as both data and shapes input.
#[pyfunction(signature = (data_graph, shapes_graph=None, *, min_iterations=None, max_iterations=None, run_until_converged=None, no_converge=None, error_on_blank_nodes=None, enable_af=true, enable_rules=true, debug=None, skip_invalid_rules=false, warnings_are_errors=false, do_imports=true, graphviz=false, heatmap=false, heatmap_all=false, trace_events=false, trace_file=None, trace_jsonl=None, return_inference_outcome=false, union=false))]
fn infer(
    py: Python<'_>,
    data_graph: &Bound<'_, PyAny>,
    shapes_graph: Option<&Bound<'_, PyAny>>,
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    no_converge: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    enable_af: bool,
    enable_rules: bool,
    debug: Option<bool>,
    skip_invalid_rules: Option<bool>,
    warnings_are_errors: Option<bool>,
    do_imports: Option<bool>,
    graphviz: bool,
    heatmap: bool,
    heatmap_all: bool,
    trace_events: bool,
    trace_file: Option<PathBuf>,
    trace_jsonl: Option<PathBuf>,
    return_inference_outcome: bool,
    union: bool,
) -> PyResult<Py<PyAny>> {
    let validator = build_validator_from_graphs(
        py,
        shapes_graph,
        data_graph,
        enable_af,
        enable_rules,
        skip_invalid_rules.unwrap_or(false),
        warnings_are_errors.unwrap_or(false),
        do_imports.unwrap_or(true),
    )?;
    let run_until_converged = resolve_run_until(
        run_until_converged,
        no_converge,
        "run_until_converged",
        "no_converge",
    )?;
    execute_infer(
        py,
        validator,
        min_iterations,
        max_iterations,
        run_until_converged,
        error_on_blank_nodes,
        debug,
        graphviz,
        heatmap,
        heatmap_all,
        trace_events,
        trace_file,
        trace_jsonl,
        return_inference_outcome,
        union,
    )
}

/// Validate data against SHACL shapes, with optional inference and diagnostics.
/// If `shapes_graph` is omitted, the data graph is used as both data and shapes input.
#[pyfunction(signature = (data_graph, shapes_graph=None, *, run_inference=false, inference=None, min_iterations=None, max_iterations=None, run_until_converged=None, no_converge=None, inference_min_iterations=None, inference_max_iterations=None, inference_no_converge=None, error_on_blank_nodes=None, inference_error_on_blank_nodes=None, enable_af=true, enable_rules=true, debug=None, inference_debug=None, skip_invalid_rules=false, warnings_are_errors=false, do_imports=true, follow_bnodes=false, graphviz=false, heatmap=false, heatmap_all=false, trace_events=false, trace_file=None, trace_jsonl=None, return_inference_outcome=false))]
fn validate(
    py: Python<'_>,
    data_graph: &Bound<'_, PyAny>,
    shapes_graph: Option<&Bound<'_, PyAny>>,
    run_inference: bool,
    inference: Option<&Bound<'_, PyAny>>,
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    no_converge: Option<bool>,
    inference_min_iterations: Option<usize>,
    inference_max_iterations: Option<usize>,
    inference_no_converge: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    inference_error_on_blank_nodes: Option<bool>,
    enable_af: bool,
    enable_rules: bool,
    debug: Option<bool>,
    inference_debug: Option<bool>,
    skip_invalid_rules: Option<bool>,
    warnings_are_errors: Option<bool>,
    do_imports: Option<bool>,
    follow_bnodes: bool,
    graphviz: bool,
    heatmap: bool,
    heatmap_all: bool,
    trace_events: bool,
    trace_file: Option<PathBuf>,
    trace_jsonl: Option<PathBuf>,
    return_inference_outcome: bool,
) -> PyResult<Py<PyAny>> {
    let validator = build_validator_from_graphs(
        py,
        shapes_graph,
        data_graph,
        enable_af,
        enable_rules,
        skip_invalid_rules.unwrap_or(false),
        warnings_are_errors.unwrap_or(false),
        do_imports.unwrap_or(true),
    )?;
    let settings = prepare_inference_settings(
        run_inference,
        inference,
        min_iterations,
        max_iterations,
        run_until_converged,
        no_converge,
        inference_min_iterations,
        inference_max_iterations,
        inference_no_converge,
        error_on_blank_nodes,
        inference_error_on_blank_nodes,
        debug,
        inference_debug,
    )?;

    execute_validate(
        py,
        validator,
        settings,
        follow_bnodes,
        graphviz,
        heatmap,
        heatmap_all,
        trace_events,
        trace_file,
        trace_jsonl,
        return_inference_outcome,
    )
}

fn init_logging_once() {
    static LOGGER_INIT: Once = Once::new();
    LOGGER_INIT.call_once(|| {
        let _ = env_logger::try_init();
    });
}

#[pyfunction]
fn init_logging() -> PyResult<()> {
    init_logging_once();
    Ok(())
}

/// Python module definition.
#[pymodule]
fn shifty(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_logging_once();

    m.add_class::<PyCompiledShapeGraph>()?;
    m.add_function(wrap_pyfunction!(generate_ir, m)?)?;
    m.add_function(wrap_pyfunction!(infer, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    Ok(())
}
