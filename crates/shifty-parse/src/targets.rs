//! Reusable SPARQL target resolution shared by lowering and source reporting.
use crate::{
    Loaded,
    lower::{canonical_sparql_query, validate_shacl_prebinding},
    vocab,
};
use oxrdf::{NamedOrBlankNode, Term, Variable};
use shifty_algebra::SparqlTarget;
use spargebra::Query;
use std::collections::HashSet;

/// `None` denotes an unsupported target implementation. An empty vector is a
/// supported target type whose required parameters were not supplied.
pub fn sparql_targets(
    graph: &Loaded,
    instance: &NamedOrBlankNode,
) -> Result<Option<Vec<SparqlTarget>>, String> {
    let mut definitions = Vec::new();
    if graph.object(instance, vocab::SH_SELECT).is_some() {
        definitions.push(instance.clone());
    } else {
        for kind in graph.objects(instance, vocab::RDF_TYPE) {
            let definition = match kind {
                Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
                Term::BlankNode(n) => NamedOrBlankNode::BlankNode(n),
                _ => continue,
            };
            if graph.is_instance_of(&definition, vocab::SH_SPARQL_TARGET_TYPE) {
                definitions.push(definition);
            }
        }
    }
    if definitions.is_empty() {
        return Ok(None);
    }
    let mut targets = Vec::new();
    for definition in definitions {
        let Some(Term::Literal(raw)) = graph.object(&definition, vocab::SH_SELECT) else {
            return Err(format!(
                "SPARQL target {definition} must have a literal sh:select"
            ));
        };
        let (query, canonical) = canonical_sparql_query(graph, &definition, raw.value())?;
        if !matches!(query, Query::Select { .. }) {
            return Err(format!("SPARQL target {definition} requires SELECT"));
        }
        let mut bindings = Vec::new();
        let mut names = HashSet::new();
        let mut missing = false;
        for declaration in graph.objects(&definition, vocab::SH_PARAMETER) {
            let node = match declaration {
                Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
                Term::BlankNode(n) => NamedOrBlankNode::BlankNode(n),
                _ => return Err("target parameter must be a resource".into()),
            };
            let Some(Term::NamedNode(path)) = graph.object(&node, vocab::SH_PATH) else {
                return Err("target parameter sh:path must be an IRI".into());
            };
            let name = path
                .as_str()
                .rsplit(['#', '/'])
                .next()
                .unwrap_or(path.as_str())
                .to_string();
            Variable::new(&name)
                .map_err(|error| format!("invalid target parameter variable: {error}"))?;
            if matches!(
                name.as_str(),
                "this" | "shapesGraph" | "currentShape" | "value" | "PATH"
            ) || !names.insert(name.clone())
            {
                return Err(format!(
                    "target parameter variable ?{name} is reserved or duplicated"
                ));
            }
            let values = graph.objects(instance, path.as_ref());
            if values.len() > 1 {
                return Err(format!(
                    "target {instance} parameter {path} has multiple values"
                ));
            }
            match values.into_iter().next() {
                Some(Term::BlankNode(_)) => {
                    return Err(format!(
                        "target {instance} parameter {path} must not be a blank node"
                    ));
                }
                Some(value) => bindings.push((name, value)),
                None => {
                    let optional = matches!(graph.object(&node,vocab::SH_OPTIONAL), Some(Term::Literal(value)) if matches!(value.value(), "true" | "1"));
                    missing |= !optional;
                }
            }
        }
        // Omitted optional parameters remain potentially pre-bound variables.
        if !names.is_empty() {
            validate_shacl_prebinding(&query, names.iter().map(String::as_str))?;
        }
        if !missing {
            bindings.sort_by(|a, b| a.0.cmp(&b.0));
            targets.push(SparqlTarget {
                query: canonical,
                extra_bindings: bindings,
            });
        }
    }
    Ok(Some(targets))
}
