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

    value_nodes
        .into_par_iter()
        .filter_map(move |value_node| {
            let ctx = context_template.clone_for_parallel_value_check(&value_node);
            check(value_node, ctx)
        })
        .collect()
}
