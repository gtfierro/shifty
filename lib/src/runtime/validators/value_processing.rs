use crate::context::Context;
use crate::runtime::ComponentValidationResult;
use oxigraph::model::Term;
use rayon::prelude::*;

pub(crate) fn parallel_value_node_checks<F>(
    value_nodes: Vec<Term>,
    context_template: &Context,
    check: F,
) -> Vec<ComponentValidationResult>
where
    F: Fn(Term, Context) -> Option<ComponentValidationResult> + Sync + Send,
{
    if value_nodes.is_empty() {
        return Vec::new();
    }

    let template = context_template.clone();

    value_nodes
        .into_par_iter()
        .filter_map(move |value_node| {
            let ctx = template.clone();
            check(value_node, ctx)
        })
        .collect()
}
