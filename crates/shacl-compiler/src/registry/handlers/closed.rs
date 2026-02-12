use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission};

pub struct ClosedHandler;

impl ComponentCodegen for ClosedHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Closed
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let (closed, ignored) = match params {
            ComponentParams::Closed {
                closed,
                ignored_properties,
            } => (*closed, ignored_properties),
            _ => return Err("closed params mismatch".to_string()),
        };
        if !closed {
            return Ok(NodeEmission::default());
        }
        let mut emission = NodeEmission::default();
        let mut ignored_exprs = Vec::new();
        for term_id in ignored {
            ignored_exprs.push((ctx.term_iri)(*term_id)?);
        }
        emission.lines.push(format!(
            "        let mut allowed: std::collections::HashSet<String> = allowed_predicates_{}();\n            let ignored: Vec<&str> = vec![{}];\n            for ig in ignored {{ allowed.insert(ig.to_string()); }}\n            let cache_key = ({}, graph_cache_key(graph));\n            let violations_for_focus: Vec<(NamedNode, Term)> = CLOSED_WORLD_VIOLATION_CACHE.with(|cell| {{\n                let mut cache = cell.borrow_mut();\n                let per_focus = cache.entry(cache_key).or_insert_with(|| {{\n                    let targets = collect_targets_node_{}(store, graph);\n                    collect_closed_world_violations_for_targets(store, graph, &targets, &allowed)\n                }});\n                per_focus.get(&focus).cloned().unwrap_or_default()\n            }});\n            for (predicate, value) in violations_for_focus {{\n                report.record({}, {}, &focus, Some(&value), Some(ResultPath::Term(Term::NamedNode(predicate.clone()))));\n            }}",
            ctx.shape_id,
            ignored_exprs
                .into_iter()
                .map(|s| format!("\"{}\"", s))
                .collect::<Vec<_>>()
                .join(", "),
            ctx.shape_id,
            ctx.shape_id,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
