#![allow(clippy::too_many_arguments)]

use std::fs;
use std::path::{Path, PathBuf};

use ontoenv::config::Config;
use oxigraph::model::Quad;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict};
use pyo3::wrap_pyfunction;
use shacl::{InferenceConfig, Source, Validator};
use tempfile::tempdir;

fn map_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn write_graph_to_file(
    py: Python<'_>,
    graph: &Bound<'_, PyAny>,
    dir: &Path,
    filename: &str,
) -> PyResult<PathBuf> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("format", "turtle")?;
    let serialized = graph.call_method("serialize", (), Some(&kwargs))?;
    let turtle = match serialized.extract::<String>() {
        Ok(s) => s,
        Err(_) => {
            let bytes: Vec<u8> = serialized.extract()?;
            String::from_utf8(bytes).map_err(map_err)?
        }
    };

    let path = dir.join(filename);
    fs::write(&path, turtle).map_err(map_err)?;
    Ok(path)
}

fn build_env_config(root: &Path) -> PyResult<Config> {
    Config::builder()
        .root(root.to_path_buf())
        .offline(false)
        .no_search(false)
        .temporary(true)
        .build()
        .map_err(map_err)
}

fn build_validator_from_graphs(
    py: Python<'_>,
    shapes_graph: &Bound<'_, PyAny>,
    data_graph: &Bound<'_, PyAny>,
    enable_af: bool,
    enable_rules: bool,
    skip_invalid_rules: bool,
) -> PyResult<Validator> {
    let temp_dir = tempdir().map_err(map_err)?;
    let shapes_path = write_graph_to_file(py, shapes_graph, temp_dir.path(), "shapes.ttl")?;
    let data_path = write_graph_to_file(py, data_graph, temp_dir.path(), "data.ttl")?;

    let config = build_env_config(temp_dir.path())?;

    let validator = Validator::builder()
        .with_shapes_source(Source::File(shapes_path))
        .with_data_source(Source::File(data_path))
        .with_env_config(config)
        .with_af_enabled(enable_af)
        .with_rules_enabled(enable_rules)
        .with_skip_invalid_rules(skip_invalid_rules)
        .build()
        .map_err(map_err)?;

    Ok(validator)
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

fn quads_to_ntriples(quads: &[Quad]) -> String {
    let mut buffer = String::new();
    for quad in quads {
        buffer.push_str(&quad.subject.to_string());
        buffer.push(' ');
        buffer.push_str(&quad.predicate.to_string());
        buffer.push(' ');
        buffer.push_str(&quad.object.to_string());
        buffer.push_str(" .\n");
    }
    buffer
}

fn empty_graph(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let rdflib = PyModule::import(py, "rdflib")?;
    let graph = rdflib.getattr("Graph")?.call0()?;
    Ok(graph.into())
}

fn graph_from_data(py: Python<'_>, data: &str, format: &str) -> PyResult<Py<PyAny>> {
    if data.trim().is_empty() {
        return empty_graph(py);
    }

    let rdflib = PyModule::import(py, "rdflib")?;
    let graph = rdflib.getattr("Graph")?.call0()?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("data", data)?;
    kwargs.set_item("format", format)?;
    graph.call_method("parse", (), Some(&kwargs))?;
    Ok(graph.into())
}

#[pyfunction(signature = (data_graph, shapes_graph, *, min_iterations=None, max_iterations=None, run_until_converged=None, no_converge=None, error_on_blank_nodes=None, enable_af=true, enable_rules=true, debug=None, skip_invalid_rules=true))]
fn infer(
    py: Python<'_>,
    data_graph: &Bound<'_, PyAny>,
    shapes_graph: &Bound<'_, PyAny>,
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    run_until_converged: Option<bool>,
    no_converge: Option<bool>,
    error_on_blank_nodes: Option<bool>,
    enable_af: bool,
    enable_rules: bool,
    debug: Option<bool>,
    skip_invalid_rules: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let validator = build_validator_from_graphs(
        py,
        shapes_graph,
        data_graph,
        enable_af,
        enable_rules,
        skip_invalid_rules.unwrap_or(false),
    )?;
    let run_until_converged = resolve_run_until(
        run_until_converged,
        no_converge,
        "run_until_converged",
        "no_converge",
    )?;
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

    let data = quads_to_ntriples(&outcome.inferred_quads);
    graph_from_data(py, &data, "nt")
}

#[pyfunction(signature = (data_graph, shapes_graph, *, run_inference=false, inference=None, min_iterations=None, max_iterations=None, run_until_converged=None, no_converge=None, inference_min_iterations=None, inference_max_iterations=None, inference_no_converge=None, error_on_blank_nodes=None, inference_error_on_blank_nodes=None, enable_af=true, enable_rules=true, debug=None, inference_debug=None, skip_invalid_rules=None))]
fn validate(
    py: Python<'_>,
    data_graph: &Bound<'_, PyAny>,
    shapes_graph: &Bound<'_, PyAny>,
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
) -> PyResult<(bool, Py<PyAny>, String)> {
    let validator = build_validator_from_graphs(
        py,
        shapes_graph,
        data_graph,
        enable_af,
        enable_rules,
        skip_invalid_rules.unwrap_or(false),
    )?;

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
            // no-op
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

    if should_run_inference {
        let config = build_inference_config(
            min_iterations,
            max_iterations,
            run_until_converged,
            error_on_blank_nodes,
            debug,
        )?;
        validator
            .run_inference_with_config(config)
            .map_err(map_err)?;
    }

    let report = validator.validate();
    let conforms = report.conforms();
    let report_turtle = report.to_turtle().map_err(map_err)?;
    let report_graph = graph_from_data(py, &report_turtle, "turtle")?;
    Ok((conforms, report_graph, report_turtle))
}

/// Python module definition.
#[pymodule]
fn shacl_rs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(infer, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    Ok(())
}
