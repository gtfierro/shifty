//! Discover and canonicalize document-owned SHACL function definitions.

use crate::graph::Loaded;
use crate::lower::canonical_sparql_query;
use crate::vocab;
use oxrdf::{NamedOrBlankNode, Term};
use shifty_algebra::FunctionDef;
use spargebra::Query;
use spargebra::SparqlParser;
use spargebra::algebra::GraphPattern;

pub fn collect_functions(shapes: &Loaded) -> Vec<FunctionDef> {
    let mut out = Vec::new();
    for func in shapes
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, vocab::SH_SPARQL_FUNCTION)
        .map(|subject| subject.into_owned())
        .collect::<Vec<_>>()
    {
        let NamedOrBlankNode::NamedNode(iri) = &func else {
            continue;
        };
        let raw = match shapes
            .object(&func, vocab::SH_SELECT)
            .or_else(|| shapes.object(&func, vocab::SH_ASK))
        {
            Some(Term::Literal(query)) => query.value().to_string(),
            _ => continue,
        };
        let Ok((_, query)) = canonical_sparql_query(shapes, &func, &raw) else {
            continue;
        };
        out.push(FunctionDef {
            iri: iri.clone(),
            params: function_param_names(shapes, &func),
            reads_graph: query_reads_graph(&query),
            query,
        });
    }
    out
}

fn function_param_names(shapes: &Loaded, func: &NamedOrBlankNode) -> Vec<String> {
    let mut params: Vec<(i64, String)> = shapes
        .objects(func, vocab::SH_PARAMETER)
        .iter()
        .filter_map(|parameter| {
            let node = match parameter {
                Term::NamedNode(node) => NamedOrBlankNode::NamedNode(node.clone()),
                Term::BlankNode(node) => NamedOrBlankNode::BlankNode(node.clone()),
                Term::Literal(_) => return None,
            };
            let order = match shapes.object(&node, vocab::SH_ORDER) {
                Some(Term::Literal(value)) => value.value().parse::<i64>().unwrap_or(0),
                _ => 0,
            };
            let name = match shapes.object(&node, vocab::SH_NAME) {
                Some(Term::Literal(value)) => value.value().to_string(),
                _ => match shapes.object(&node, vocab::SH_PATH) {
                    Some(Term::NamedNode(path)) => local_name(path.as_str()).to_string(),
                    _ => return None,
                },
            };
            Some((order, name))
        })
        .collect();
    params.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    params.into_iter().map(|(_, name)| name).collect()
}

fn local_name(iri: &str) -> &str {
    iri.rsplit(['#', '/']).next().unwrap_or(iri)
}

fn query_reads_graph(query: &str) -> bool {
    let Ok(parsed) = SparqlParser::new().parse_query(query) else {
        return true;
    };
    fn walk(pattern: &GraphPattern) -> bool {
        use GraphPattern::*;
        match pattern {
            Bgp { patterns } => !patterns.is_empty(),
            Path { .. } | Graph { .. } | Service { .. } => true,
            Join { left, right }
            | Union { left, right }
            | Minus { left, right }
            | Lateral { left, right } => walk(left) || walk(right),
            LeftJoin { left, right, .. } => walk(left) || walk(right),
            Filter { inner, .. }
            | Extend { inner, .. }
            | Project { inner, .. }
            | Distinct { inner }
            | Reduced { inner }
            | Slice { inner, .. }
            | OrderBy { inner, .. }
            | Group { inner, .. } => walk(inner),
            _ => false,
        }
    }
    match &parsed {
        Query::Select { pattern, .. } | Query::Ask { pattern, .. } => walk(pattern),
        _ => true,
    }
}
