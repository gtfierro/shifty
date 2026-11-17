//! Shared evaluation of lowered SHACL-AF node expressions.
use crate::path::{PathBackend, succ};
use oxrdf::{NamedNode, Term};
use shifty_algebra::{NodeExpr, ShapeId};
use std::collections::HashSet;

pub(crate) fn evaluate(
    backend: &dyn PathBackend,
    focus: &Term,
    expr: &NodeExpr,
    holds: &mut impl FnMut(&Term, ShapeId) -> bool,
    function: &mut impl FnMut(&NamedNode, &[HashSet<Term>]) -> HashSet<Term>,
) -> HashSet<Term> {
    match expr {
        NodeExpr::This => [focus.clone()].into(),
        NodeExpr::Constant(term) => [term.clone()].into(),
        NodeExpr::Path(path) => succ(backend, focus, path),
        NodeExpr::PathFrom { input, path } => evaluate(backend, focus, input, holds, function)
            .iter()
            .flat_map(|node| succ(backend, node, path))
            .collect(),
        NodeExpr::Filter { input, shape } => evaluate(backend, focus, input, holds, function)
            .into_iter()
            .filter(|node| holds(node, *shape))
            .collect(),
        NodeExpr::Intersection(expressions) => {
            let mut iter = expressions.iter();
            let Some(first) = iter.next() else {
                return HashSet::new();
            };
            let mut result = evaluate(backend, focus, first, holds, function);
            for expr in iter {
                let values = evaluate(backend, focus, expr, holds, function);
                result.retain(|node| values.contains(node));
            }
            result
        }
        NodeExpr::Union(expressions) => expressions
            .iter()
            .flat_map(|expr| evaluate(backend, focus, expr, holds, function))
            .collect(),
        NodeExpr::Function { iri, args } => {
            let values: Vec<_> = args
                .iter()
                .map(|arg| evaluate(backend, focus, arg, holds, function))
                .collect();
            function(iri, &values)
        }
    }
}
