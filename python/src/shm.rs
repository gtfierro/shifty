//! Shapeshift for Python: format, check, and export `.shm` text.
//!
//! The composition API lives in `shifty/shapeshift.py`; it renders text and
//! hands it here, so formatting, name resolution, and the SHACL it raises
//! are exactly the CLI's.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use shapeshift::Session;
use shapeshift::macros::diagnostic::{Diagnostic, Level};

pyo3::create_exception!(
    _shifty,
    ShapeshiftError,
    PyValueError,
    "Shapeshift text that does not parse or build. The message carries the diagnostics."
);

/// One diagnostic, flattened for Python:
/// `(level, message, file, line, column, hint, rendered)`.
type PyDiagnostic = (
    String,
    String,
    Option<String>,
    Option<u32>,
    Option<u32>,
    Option<String>,
    String,
);

/// A diagnostic as the CLI prints it, with the offending line from `source`
/// when it is about `label` (and from disk otherwise).
fn render(d: &Diagnostic, label: &str, source: &str) -> String {
    let word = match d.level {
        Level::Error => "error",
        Level::Warning => "warning",
    };
    let mut out = format!("{word}: {}\n", d.message);
    if let Some(loc) = &d.location {
        out.push_str(&format!("  --> {}:{}:{}\n", loc.file, loc.line, loc.column));
        let text = if loc.file == label {
            Some(source.to_string())
        } else {
            std::fs::read_to_string(&loc.file).ok()
        };
        if let Some(line) = text
            .as_deref()
            .and_then(|t| t.lines().nth(loc.line.saturating_sub(1) as usize))
        {
            let n = loc.line.to_string();
            let pad = " ".repeat(n.len());
            let caret = " ".repeat(loc.column.saturating_sub(1) as usize);
            out.push_str(&format!("  {n} | {line}\n  {pad} | {caret}^\n"));
        }
    }
    for note in &d.notes {
        out.push_str(&format!("  {note}\n"));
    }
    if let Some(h) = &d.hint {
        out.push_str(&format!("  hint: {h}\n"));
    }
    out
}

fn render_all(ds: &[Diagnostic], label: &str, source: &str) -> String {
    ds.iter().map(|d| render(d, label, source)).collect()
}

fn flatten(d: &Diagnostic, label: &str, source: &str) -> PyDiagnostic {
    let level = match d.level {
        Level::Error => "error",
        Level::Warning => "warning",
    };
    (
        level.to_string(),
        d.message.clone(),
        d.location.as_ref().map(|l| l.file.clone()),
        d.location.as_ref().map(|l| l.line),
        d.location.as_ref().map(|l| l.column),
        d.hint.clone(),
        render(d, label, source),
    )
}

/// A session holding `source` as the module `label`, resolving relative
/// imports against `directory`.
fn session(
    source: &str,
    label: &str,
    directory: Option<&Path>,
    catalog: Option<BTreeMap<String, PathBuf>>,
) -> (Session, Vec<Diagnostic>) {
    let mut s = Session::new(label);
    if let Some(dir) = directory {
        s.dir = Some(std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf()));
    }
    if let Some(c) = catalog {
        s.imports.catalog = c;
    }
    let parsed = shapeshift::parse::parse(source, label);
    s.module = parsed.module;
    (s, parsed.diagnostics)
}

/// Canonical `.shm` text for `source`.
#[pyfunction]
#[pyo3(signature = (source, label = "module"))]
fn _shapeshift_format(source: &str, label: &str) -> PyResult<String> {
    shapeshift::parse::format(source, label)
        .map_err(|ds| ShapeshiftError::new_err(render_all(&ds, label, source)))
}

/// Every diagnostic `source` has: syntax, imports, names, and the build.
#[pyfunction]
#[pyo3(signature = (source, label = "module", directory = None, catalog = None))]
fn _shapeshift_check(
    source: &str,
    label: &str,
    directory: Option<PathBuf>,
    catalog: Option<BTreeMap<String, PathBuf>>,
) -> Vec<PyDiagnostic> {
    let (s, mut diags) = session(source, label, directory.as_deref(), catalog);
    if !diags.iter().any(Diagnostic::is_error) {
        diags.extend(s.compile().diagnostics);
    }
    diags.iter().map(|d| flatten(d, label, source)).collect()
}

/// `source` raised to SHACL Turtle, and the raiser's warnings.
#[pyfunction]
#[pyo3(signature = (source, label = "module", directory = None, catalog = None, core = false))]
fn _shapeshift_export(
    source: &str,
    label: &str,
    directory: Option<PathBuf>,
    catalog: Option<BTreeMap<String, PathBuf>>,
    core: bool,
) -> PyResult<(String, Vec<String>)> {
    let (s, diags) = session(source, label, directory.as_deref(), catalog);
    if diags.iter().any(Diagnostic::is_error) {
        return Err(ShapeshiftError::new_err(render_all(&diags, label, source)));
    }
    let compiled = s.compile();
    if compiled.has_errors() {
        return Err(ShapeshiftError::new_err(render_all(
            &compiled.diagnostics,
            label,
            source,
        )));
    }
    s.export(core)
        .map_err(|e| ShapeshiftError::new_err(e.to_string()))
}

/// Words with a fixed meaning, which cannot name a shape, path, or label.
#[pyfunction]
fn _shapeshift_keywords() -> Vec<&'static str> {
    shapeshift::parse::KEYWORDS.to_vec()
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ShapeshiftError", m.py().get_type::<ShapeshiftError>())?;
    m.add_function(wrap_pyfunction!(_shapeshift_keywords, m)?)?;
    m.add_function(wrap_pyfunction!(_shapeshift_format, m)?)?;
    m.add_function(wrap_pyfunction!(_shapeshift_check, m)?)?;
    m.add_function(wrap_pyfunction!(_shapeshift_export, m)?)?;
    Ok(())
}
